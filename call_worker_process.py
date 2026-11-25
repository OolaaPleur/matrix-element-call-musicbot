import asyncio
import inspect
import json
import logging
import os
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional


logger = logging.getLogger(__name__)

_WORKER_PROBLEM_WORDS = ("error", "fatal", "fail", "warn", "timeout", "exception")
_TERMINAL_EVENTS = {"play_ended", "play_stopped", "play_failed"}
# Only events someone waits for go into the queue (pongs alone are ~8600 a day).
_QUEUED_EVENTS = _TERMINAL_EVENTS | {"joined", "error"}

EventHandler = Callable[[dict[str, Any]], Optional[Awaitable[None]]]


class CallWorkerProcess:
    def __init__(
        self,
        project_root: Path,
        *,
        max_restart_attempts: int = 3,
        heartbeat_interval: float = 10.0,
        stop_timeout_restart_threshold: int = 2,
        env_overrides: Optional[dict[str, str]] = None,
    ):
        self._worker_dir = project_root / "call_worker"
        self._script_path = self._worker_dir / "src" / "join_call.js"

        self._process: Optional[asyncio.subprocess.Process] = None
        self._room_id: Optional[str] = None
        self._desired_room_id: Optional[str] = None

        self._stdout_task: Optional[asyncio.Task] = None
        self._stderr_task: Optional[asyncio.Task] = None
        self._monitor_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None

        self._events: "asyncio.Queue[dict[str, Any]]" = asyncio.Queue()
        self._write_lock = asyncio.Lock()
        self._lifecycle_lock = asyncio.Lock()

        self._event_handler: Optional[EventHandler] = None
        self._pong_event = asyncio.Event()

        self._intentional_stop = False
        self._restart_attempts = 0
        self._max_restart_attempts = max(0, int(max_restart_attempts))
        self._heartbeat_interval = max(1.0, float(heartbeat_interval))
        self._stop_timeout_restart_threshold = max(1, int(stop_timeout_restart_threshold))
        self._env_overrides = dict(env_overrides or {})
        self._state = "idle"
        self._last_pong_ts: Optional[float] = None
        self._consecutive_stop_timeouts = 0

    @property
    def running(self) -> bool:
        return self._process is not None and self._process.returncode is None

    @property
    def room_id(self) -> Optional[str]:
        return self._room_id

    @property
    def available(self) -> bool:
        return self._script_path.exists()

    @property
    def state(self) -> str:
        return self._state

    @property
    def restart_attempts(self) -> int:
        return self._restart_attempts

    @property
    def max_restart_attempts(self) -> int:
        return self._max_restart_attempts

    @property
    def desired_room_id(self) -> Optional[str]:
        return self._desired_room_id

    @property
    def last_pong_ts(self) -> Optional[float]:
        return self._last_pong_ts

    @property
    def consecutive_stop_timeouts(self) -> int:
        return self._consecutive_stop_timeouts

    @property
    def stop_timeout_restart_threshold(self) -> int:
        return self._stop_timeout_restart_threshold

    def set_event_handler(self, handler: Optional[EventHandler]):
        self._event_handler = handler

    async def start(self, room_id: str):
        if not self.available:
            raise RuntimeError(f"Call worker script not found: {self._script_path}")

        async with self._lifecycle_lock:
            self._desired_room_id = room_id
            self._intentional_stop = False

            if self.running and self._room_id == room_id:
                return

            if self.running:
                await self._stop_locked(clear_desired=False)

            await self._spawn_locked(room_id)
            try:
                joined = await self._wait_for_event("joined", timeout=35)
            except BaseException:
                # Don't keep a half-started worker: `running` would stay true and
                # the next play would be sent to a worker that never reached LiveKit.
                await self._stop_locked(clear_desired=True)
                raise
            self._state = "joined"
            self._restart_attempts = 0
            logger.info("Call worker joined room %s (%s)", room_id, joined.get("mode", "unknown"))

            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def play(self, file_path: str, title: Optional[str] = None):
        if not self.running:
            raise RuntimeError("Call worker is not running")
        payload = {"type": "play", "file": file_path}
        if title:
            payload["title"] = title
        await self._send_command(payload)

    async def play_stream(self, stream_url: str, title: Optional[str] = None):
        if not self.running:
            raise RuntimeError("Call worker is not running")
        payload = {"type": "play", "url": stream_url}
        if title:
            payload["title"] = title
        await self._send_command(payload)

    async def pause_playback(self):
        if self.running:
            await self._send_command({"type": "pause"})

    async def resume_playback(self):
        if self.running:
            await self._send_command({"type": "resume"})

    async def stop_playback(self, wait_for_terminal: bool = False, timeout: float = 8.0) -> bool:
        if not self.running:
            return True
        # Nothing playing: the worker sends no terminal event, so waiting would
        # time out and count towards a worker restart.
        was_playing = self._state == "playing"
        await self._send_command({"type": "stop"})
        if not wait_for_terminal or not was_playing:
            return True

        terminal_seen = True
        if wait_for_terminal:
            try:
                await self.wait_for_playback_terminal(timeout=timeout)
            except RuntimeError:
                pass  # play_failed: playback ended all the same
            except asyncio.TimeoutError:
                terminal_seen = False
                self._consecutive_stop_timeouts += 1
                logger.warning(
                    "Timed out waiting for playback terminal event after stop "
                    "(%s/%s)",
                    self._consecutive_stop_timeouts,
                    self._stop_timeout_restart_threshold,
                )
                await self._dispatch_event(
                    {
                        "event": "worker_stop_timeout",
                        "count": self._consecutive_stop_timeouts,
                        "threshold": self._stop_timeout_restart_threshold,
                    }
                )
                if self._consecutive_stop_timeouts >= self._stop_timeout_restart_threshold:
                    self._consecutive_stop_timeouts = 0
                    await self._recover_worker("consecutive_stop_timeouts")

        return terminal_seen

    async def set_audio_settings(self, normalize_audio: bool, fade_in_ms: int, volume_percent: int = 100):
        if not self.running:
            return
        payload = {
            "type": "set_audio",
            "normalize_audio": bool(normalize_audio),
            "fade_in_ms": int(max(0, fade_in_ms)),
            "volume_percent": int(max(0, volume_percent)),
        }
        await self._send_command(payload)

    async def stop(self):
        async with self._lifecycle_lock:
            self._desired_room_id = None
            await self._stop_locked(clear_desired=True)

    async def wait_for_playback_terminal(
        self, timeout: Optional[float] = None, expected_source: Optional[str] = None
    ) -> dict[str, Any]:
        """Next play_ended/play_stopped (returned) or play_failed (raised).

        With expected_source, events of another source are skipped: after a skip the
        old track's play_stopped arrives after the new watcher started. Only
        play_failed is a playback error; other worker errors don't fail the track.
        """
        async def _next() -> dict[str, Any]:
            while True:
                event = await self._events.get()
                event_name = event.get("event")
                if event_name not in _TERMINAL_EVENTS:
                    continue
                source = event.get("source") or event.get("file") or event.get("url")
                if expected_source and source and source != expected_source:
                    logger.info("Skipping %s of another track", event_name)
                    continue
                if event_name == "play_failed":
                    raise RuntimeError(str(event.get("message", "Playback failed")))
                return event

        if timeout is None:
            return await _next()
        return await asyncio.wait_for(_next(), timeout=timeout)

    async def _spawn_locked(self, room_id: str):
        # Events of the previous process (e.g. a non-fatal "error" nobody consumed)
        # would otherwise fail the wait for the new one's "joined".
        self._drain_events()
        env = os.environ.copy()
        env["MATRIX_ROOM_ID"] = room_id
        env.update(self._env_overrides)

        self._process = await asyncio.create_subprocess_exec(
            "node",
            str(self._script_path),
            cwd=str(self._worker_dir),
            env=env,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        self._room_id = room_id
        self._state = "joining"
        self._consecutive_stop_timeouts = 0

        self._stdout_task = asyncio.create_task(self._pump_stdout(self._process.stdout))
        self._stderr_task = asyncio.create_task(self._pump_stderr(self._process.stderr))
        self._monitor_task = asyncio.create_task(self._monitor_process(self._process))

    async def _stop_locked(self, clear_desired: bool):
        self._intentional_stop = True
        self._state = "stopping"
        self._cancel_background_tasks()

        if not self.running:
            self._finalize_stopped(clear_desired=clear_desired)
            return

        proc = self._process
        if proc is None:
            self._finalize_stopped(clear_desired=clear_desired)
            return

        try:
            await self._send_command({"type": "shutdown"})
            await asyncio.wait_for(proc.wait(), timeout=8)
        except (asyncio.TimeoutError, BrokenPipeError):
            logger.warning("Call worker did not exit gracefully, terminating")
            proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), timeout=4)
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()

        self._finalize_stopped(clear_desired=clear_desired)

    def _finalize_stopped(self, clear_desired: bool):
        self._process = None
        self._room_id = None
        if clear_desired:
            self._desired_room_id = None
        self._state = "idle"
        self._drain_events()

    async def _monitor_process(self, proc: asyncio.subprocess.Process):
        return_code = await proc.wait()
        async with self._lifecycle_lock:
            if self._process is not proc:
                return

            if self._intentional_stop:
                return

            self._state = "error"
            logger.error("Call worker exited unexpectedly with code %s", return_code)
            await self._dispatch_event({"event": "worker_exited", "code": return_code})

            if not self._desired_room_id:
                self._finalize_stopped(clear_desired=True)
                return

            if self._restart_attempts >= self._max_restart_attempts:
                await self._dispatch_event({"event": "worker_restart_failed", "reason": "max_restarts"})
                self._finalize_stopped(clear_desired=False)
                return

            self._restart_attempts += 1
            backoff = min(8, 2 ** self._restart_attempts)
            await self._dispatch_event({"event": "worker_restart_attempt", "attempt": self._restart_attempts, "backoff": backoff})

            self._cancel_background_tasks()
            self._process = None
            self._room_id = None

            await asyncio.sleep(backoff)

            try:
                room_id = self._desired_room_id
                if not room_id:
                    self._finalize_stopped(clear_desired=True)
                    return

                self._intentional_stop = False
                await self._spawn_locked(room_id)
                await self._wait_for_event("joined", timeout=35)
                self._state = "joined"
                self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
                await self._dispatch_event({"event": "worker_restarted", "attempt": self._restart_attempts})
            except Exception as exc:
                await self._dispatch_event({"event": "worker_restart_failed", "reason": str(exc)})
                self._state = "error"

    async def _recover_worker(self, reason: str):
        async with self._lifecycle_lock:
            if not self.running:
                return

            room_id = self._desired_room_id or self._room_id
            if not room_id:
                return

            logger.warning("Recovering call worker in room %s (reason=%s)", room_id, reason)
            await self._dispatch_event({"event": "worker_recovering", "reason": reason})

            await self._stop_locked(clear_desired=False)
            self._intentional_stop = False

            try:
                await self._spawn_locked(room_id)
                await self._wait_for_event("joined", timeout=35)
                self._state = "joined"
                self._restart_attempts = 0
                self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
                await self._dispatch_event({"event": "worker_recovered", "reason": reason})
            except Exception as exc:
                self._state = "error"
                await self._dispatch_event({"event": "worker_restart_failed", "reason": str(exc)})

    async def _heartbeat_loop(self):
        try:
            while self.running:
                self._pong_event.clear()
                await self._send_command({"type": "ping"})
                try:
                    await asyncio.wait_for(self._pong_event.wait(), timeout=6)
                except asyncio.TimeoutError:
                    logger.warning("Call worker heartbeat timeout")
                    await self._dispatch_event({"event": "worker_heartbeat_timeout"})
                    if self._process and self._process.returncode is None:
                        self._process.terminate()
                    return

                await asyncio.sleep(self._heartbeat_interval)
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.error("Heartbeat loop error: %s", exc)

    async def _send_command(self, payload: dict[str, Any]):
        if not self.running:
            raise RuntimeError("Call worker is not running")

        proc = self._process
        if proc is None or proc.stdin is None:
            raise RuntimeError("Call worker stdin unavailable")

        message = json.dumps(payload, ensure_ascii=True) + "\n"
        async with self._write_lock:
            proc.stdin.write(message.encode("utf-8"))
            await proc.stdin.drain()

    async def _wait_for_event(self, expected_event: str, timeout: float) -> dict[str, Any]:
        async def _next() -> dict[str, Any]:
            while True:
                event = await self._events.get()
                event_name = event.get("event")
                if event_name == "error":
                    raise RuntimeError(str(event.get("message", "Call worker error")))
                if event_name == expected_event:
                    return event

        return await asyncio.wait_for(_next(), timeout=timeout)

    async def _pump_stdout(self, stream: Optional[asyncio.StreamReader]):
        if stream is None:
            return
        try:
            while True:
                line = await stream.readline()
                if not line:
                    break
                text = line.decode("utf-8", errors="replace").rstrip()
                if not text:
                    continue

                try:
                    event = json.loads(text)
                except json.JSONDecodeError:
                    logger.info("[call-worker] %s", text)
                    continue

                if isinstance(event, dict):
                    await self._dispatch_event(event)
        except asyncio.CancelledError:
            pass

    async def _pump_stderr(self, stream: Optional[asyncio.StreamReader]):
        if stream is None:
            return
        try:
            while True:
                line = await stream.readline()
                if not line:
                    break
                text = line.decode("utf-8", errors="replace").rstrip()
                if not text:
                    continue
                # The worker logs everything to call_worker/call_worker.log too; keep only
                # problems at WARNING here, not heartbeats and progress lines.
                lowered = text.lower()
                if any(word in lowered for word in _WORKER_PROBLEM_WORDS):
                    logger.warning("[call-worker:stderr] %s", text)
                else:
                    logger.debug("[call-worker:stderr] %s", text)
        except asyncio.CancelledError:
            pass

    async def _dispatch_event(self, event: dict[str, Any]):
        event_name = event.get("event")
        if event_name == "pong":
            self._pong_event.set()
            self._last_pong_ts = asyncio.get_running_loop().time()
        elif event_name == "joined":
            self._state = "joined"
        elif event_name == "play_started":
            self._state = "playing"
            self._consecutive_stop_timeouts = 0
        elif event_name in {"play_stopped", "play_ended", "play_failed"}:
            if self.running:
                self._state = "joined"
            self._consecutive_stop_timeouts = 0
        # "error" doesn't change the state: most are non-fatal (membership manager,
        # a bad command) and the worker keeps playing; a fatal one ends the process,
        # which _monitor_process handles.
        elif event_name == "left":
            self._state = "idle"

        if event_name in _QUEUED_EVENTS:
            await self._events.put(event)

        if self._event_handler:
            try:
                result = self._event_handler(event)
                if inspect.isawaitable(result):
                    await result
            except Exception as exc:
                logger.error("Worker event handler failed: %s", exc)

        if event_name not in {"joined", "left", "pong"}:
            logger.info("[call-worker-event] %s", event)

    def _cancel_background_tasks(self):
        for task in (self._stdout_task, self._stderr_task, self._heartbeat_task):
            if task and not task.done():
                task.cancel()
        self._stdout_task = None
        self._stderr_task = None
        self._heartbeat_task = None

    def _drain_events(self):
        while True:
            try:
                self._events.get_nowait()
            except asyncio.QueueEmpty:
                break

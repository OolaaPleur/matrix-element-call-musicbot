import asyncio
import json
import os
from collections import defaultdict, deque
from dataclasses import dataclass
import logging
from pathlib import Path
import random
import re
import shutil
import subprocess
import time
from typing import Awaitable, Callable, Optional
import uuid
from urllib.parse import urlparse

from nio import AsyncClient, InviteMemberEvent, MatrixRoom, MegolmEvent, RoomMessageText, UnknownEvent
from matrix_bot_common.cross_signing import ensure_cross_signing

from i18n import Translations
from config import Config
from audio_queue import AudioQueue
from call_worker_process import CallWorkerProcess
from saved_queues import SavedQueueStore
from spotify import SpotifyError, is_spotify_link, resolve_spotify


logger = logging.getLogger(__name__)

EVT_TRACK_STARTED  = "dev.elementcall.musicbot.track_started"
EVT_TRACK_FINISHED = "dev.elementcall.musicbot.track_finished"
BOT_KIND = "matrix-element-call-musicbot"


def detect_source(url: str) -> str:
    host = (urlparse(url).netloc or "").lower()
    if host == "music.youtube.com":
        return "youtube_music"
    if host in {"youtube.com", "www.youtube.com", "youtu.be", "m.youtube.com"}:
        return "youtube"
    return "other"


_NOISE_RE = re.compile(
    r"\s*[\(\[][^)\]]*"
    r"(?:official|video|audio|lyrics?|hd|4k|"
    r"remaster(?:ed)?|live|music\s*video|mv|"
    r"visualizer|explicit|clean|extended)"
    r"[^)\]]*[\)\]]\s*",
    re.IGNORECASE,
)
_TOPIC_SUFFIX_RE = re.compile(r"\s*-\s*Topic\s*$", re.IGNORECASE)
# Per-device call membership: _@localpart:server_deviceId_m.call
_PER_DEVICE_CALL_STATE_KEY_RE = re.compile(r"^_(@[^:]+:[^_]+)_")
_DATA_DIR = Path(__file__).resolve().parent / "data"
SEARCH_RESULTS = 5
SEARCH_RESULTS_TTL = 300
EMIT_YIELD_MAX_SECONDS = 15  # scrobbler events wait at most this long for chat replies
LOADER_POLL_SECONDS = 0.5  # how soon the playlist loader notices the queue ran dry
QUEUE_PENDING_SHOWN = 10  # not yet loaded playlist tracks listed by `queue`  # seconds `play <n>` refers to the last search


def _clean_title(title: str) -> str:
    prev = None
    cur = title
    while prev != cur:
        prev = cur
        cur = _NOISE_RE.sub(" ", cur).strip()
    cur = _TOPIC_SUFFIX_RE.sub("", cur).strip()
    return cur.strip(" -")


def extract_metadata(yt_info: dict) -> tuple[str, str, str, str]:
    """Returns (artist, track, album, quality) where quality is 'high' or 'low'."""
    yt_artist = (yt_info.get("artist") or "").strip()
    yt_track  = (yt_info.get("track")  or "").strip()
    yt_album  = (yt_info.get("album")  or "").strip()
    if yt_artist and yt_track:
        return yt_artist, yt_track, yt_album, "high"

    title = (yt_info.get("title") or "").strip()
    if " - " in title:
        left, right = title.split(" - ", 1)
        artist = _TOPIC_SUFFIX_RE.sub("", left).strip()
        track = _clean_title(right)
        if artist and track:
            return artist, track, yt_album, "low"

    channel = (yt_info.get("channel") or yt_info.get("uploader") or "").strip()
    cleaned = _clean_title(title)
    if channel and cleaned:
        channel = _TOPIC_SUFFIX_RE.sub("", channel).strip()
        return channel, cleaned, yt_album, "low"

    return "", "", "", "low"


def display_title(track: Optional[dict]) -> str:
    """'Artist - Track' for chat messages (same metadata the scrobbler gets), else the raw title."""
    if not isinstance(track, dict):
        return ""
    artist, track_name, _album, _quality = extract_metadata(track)
    if artist and track_name:
        return f"{artist} - {track_name}"
    return str(track.get("title") or "")


@dataclass(slots=True)
class OutboundMessage:
    room_id: str
    text: str
    html_body: Optional[str]
    priority: str


class IntegratedBot:
    """Matrix music bot core (commands, queue, call join, audio publish)."""

    def __init__(self, config: Config):
        self.config = config
        self._prefix = config.COMMAND_PREFIX
        _store_dir = Path(__file__).resolve().parent / "data" / "crypto_store"
        _store_dir.mkdir(parents=True, exist_ok=True)
        self.client = AsyncClient(config.MATRIX_HOMESERVER, config.MATRIX_USER_ID, store_path=str(_store_dir) + "/")
        self.client.access_token = config.MATRIX_ACCESS_TOKEN
        self.first_sync_done = False

        self.audio_queue = AudioQueue(
            config.AUDIO_DIR,
            config.AUTO_ADVANCE_BUFFER,
            config.PREROLL_SILENCE,
            cache_mode=config.AUDIO_CACHE_MODE,
            cache_max_bytes=config.AUDIO_CACHE_MAX_BYTES,
            cache_delete_after_playback=config.AUDIO_CACHE_DELETE_AFTER_PLAYBACK,
            cache_delete_on_shutdown=config.AUDIO_CACHE_DELETE_ON_SHUTDOWN,
            search_mode=config.SEARCH_MODE,
            search_timeout_seconds=config.SEARCH_TIMEOUT_SECONDS,
            extractor_retries=config.EXTRACTOR_RETRIES,
            download_format=config.AUDIO_DOWNLOAD_FORMAT,
            audio_quality=config.AUDIO_QUALITY,
            cookies_file=config.YTDLP_COOKIES_FILE,
            proxy=config.PROXY,
        )

        self._auto_advance_task: Optional[asyncio.Task] = None
        self._advance_watchdog_task: Optional[asyncio.Task] = None
        self._worker_playback_task: Optional[asyncio.Task] = None
        self._background_load_task: Optional[asyncio.Task] = None
        # Sources the background loader hasn't downloaded yet (shared with `shuffle`)
        self._pending_sources: Optional[deque] = None
        # Title/duration of the not yet loaded playlist tracks, by source URL (for `queue`).
        self._pending_info: dict[str, dict] = {}
        self._stream_prefetch_task: Optional[asyncio.Task] = None
        self._current_room_id: Optional[str] = None
        self._current_track_started_at: Optional[float] = None
        # Pause state: loop time and wall time when paused (None = playing)
        self._paused_at: Optional[float] = None
        self._paused_wall: Optional[float] = None
        self._playback_generation = 0
        self._playback_lock = asyncio.Lock()
        self._play_request_lock = asyncio.Lock()
        self._last_skip_at = 0.0
        self._skip_cooldown_seconds = config.SKIP_COOLDOWN_SECONDS
        self._restart_failed_notified = False
        self._startup_warnings: list[str] = []
        self._tool_versions: dict[str, str] = {}
        self._play_history_by_room: dict[str, deque[dict]] = defaultdict(
            lambda: deque(maxlen=max(1, self.config.HISTORY_LIMIT))
        )
        self._message_queue: asyncio.PriorityQueue[tuple[int, int, OutboundMessage]] = asyncio.PriorityQueue()
        self._message_dispatch_task: Optional[asyncio.Task] = None
        self._message_seq = 0
        self._noisy_cooldown_until: dict[tuple[str, str], float] = {}
        self._message_priority_map = {"critical": 0, "normal": 1, "noisy": 2}
        self._pending_megolm: dict[str, list[tuple[MatrixRoom, MegolmEvent]]] = defaultdict(list)
        # room_id -> {call.member state_key: user_id} for active memberships. Keyed by
        # state_key because one user has one per-device entry per device; the user
        # is in the call while any of them is active.
        self._call_memberships: dict[str, dict[str, str]] = {}
        self._active_play: dict | None = None
        # Scrobbler events go out in order through one background task, so playback
        # never waits on Matrix (nio retries 429s and timeouts indefinitely).
        self._emit_queue: asyncio.Queue[tuple[str, str, dict, str]] = asyncio.Queue()
        self._emit_task: Optional[asyncio.Task] = None
        self._command_tasks: set[asyncio.Task] = set()
        # room_id -> (monotonic time, result URLs) of the last `search`, for `play <n>`.
        self._search_results: dict[str, tuple[float, list[str]]] = {}
        self.call_worker = CallWorkerProcess(
            Path(__file__).resolve().parent,
            max_restart_attempts=config.WORKER_MAX_RESTART_ATTEMPTS,
            heartbeat_interval=config.WORKER_HEARTBEAT_INTERVAL,
            stop_timeout_restart_threshold=config.WORKER_STOP_TIMEOUT_RESTART_THRESHOLD,
            env_overrides={
                "MATRIX_HOMESERVER": config.MATRIX_HOMESERVER or "",
                "MATRIX_USER_ID": config.MATRIX_USER_ID or "",
                "MATRIX_ACCESS_TOKEN": config.MATRIX_ACCESS_TOKEN or "",
                "NORMALIZE_AUDIO": "true" if config.NORMALIZE_AUDIO else "false",
                "FADE_IN_MS": str(config.FADE_IN_MS),
                "VOLUME_PERCENT": str(config.VOLUME_PERCENT),
                "WORKER_LOG_MAX_BYTES": str(config.WORKER_LOG_MAX_BYTES),
                "WORKER_LOG_BACKUPS": str(config.WORKER_LOG_BACKUPS),
                "WORKER_MEMBERSHIP_MODE": config.WORKER_MEMBERSHIP_MODE,
                "WORKER_AUDIO_CHANNELS": "2" if config.WORKER_STEREO else "1",
                "WORKER_AUDIO_BITRATE_KBPS": str(config.WORKER_AUDIO_BITRATE_KBPS),
                "WORKER_E2EE": "true" if config.WORKER_E2EE else "false",
                "WORKER_DEVICE_FILE": str(_DATA_DIR / "worker_device.json"),
                "WORKER_CRYPTO_STORE_FILE": str(_DATA_DIR / "worker_crypto_store.v8"),
                "WORKER_CROSS_SIGNING_KEYS_FILE": str(_DATA_DIR / "cross_signing_keys.json"),
            },
        )
        if config.WORKER_E2EE:
            self.call_worker.join_timeout = 120.0
        self.call_worker.set_event_handler(self._on_call_worker_event)
        self.saved_queues = SavedQueueStore(config.SAVED_QUEUES_FILE)
        self.translations = Translations(Path(__file__).resolve().parent / "locales", Path(__file__).resolve().parent / "locales" / "custom")
        if config.BOT_LANGUAGE not in self.translations.catalogs:
            logger.warning("bot.language=%r has no locale file, using English", config.BOT_LANGUAGE)
        self._room_languages_file = _DATA_DIR / "room_languages.json"
        self._room_languages = self._load_room_languages()

        self.client.add_event_callback(self.on_message, RoomMessageText)
        self.client.add_event_callback(self.on_invite, InviteMemberEvent)
        self.client.add_event_callback(self._on_megolm_event, MegolmEvent)
        self.client.add_event_callback(self._on_call_member_state, UnknownEvent)

        self._command_handlers: dict[str, Callable[[MatrixRoom, str], Awaitable[None]]] = {}
        self._register_command_handlers()

    def _cancel_auto_advance(self):
        if self._auto_advance_task and not self._auto_advance_task.done():
            self._auto_advance_task.cancel()
            logger.info("Cancelled auto-advance timer")
        self._auto_advance_task = None

    async def _on_call_member_state(self, room: MatrixRoom, event):
        if event.source.get("type") != "org.matrix.msc3401.call.member":
            return
        content = event.source.get("content", {})
        state_key = event.source.get("state_key", "")
        if not state_key:
            return
        room_id = room.room_id

        per_device_match = _PER_DEVICE_CALL_STATE_KEY_RE.match(state_key)
        if per_device_match:
            # Per-device format: state_key = _@user:server_deviceId_m.call
            user_id = per_device_match.group(1)
            active = content.get("application") == "m.call" and bool(content)
        else:
            # Legacy format: state_key = @user:server, content has memberships list
            user_id = state_key
            memberships = content.get("memberships", [])
            active = any(m.get("application") == "m.call" for m in memberships)

        if not user_id:
            return
        logger.info("call_member state room=%s user=%s active=%s", room_id, user_id, active)
        self._set_call_membership(room_id, state_key, user_id, active)

    def _set_call_membership(self, room_id: str, state_key: str, user_id: str, active: bool):
        memberships = self._call_memberships.setdefault(room_id, {})
        if active:
            memberships[state_key] = user_id
        else:
            # Only this device's entry: an old empty entry of another device must not
            # remove a user who is still in the call (seen 2026-11-15).
            memberships.pop(state_key, None)

    @staticmethod
    def _log_emit(kind: str, resp, play_id: str, details: str):
        """One log line per scrobbler event sent, keyed by play_id like the scrobbler's own log."""
        if getattr(resp, "event_id", None):
            logger.info("Emitted %s play_id=%s %s", kind, play_id, details)
        else:
            logger.warning("Emit %s play_id=%s failed: %s (%s)", kind, play_id, resp, details)

    def get_call_participants(self, room_id: str) -> list[str]:
        participants = set(self._call_memberships.get(room_id, {}).values())
        return [u for u in participants if u != self.client.user_id]

    async def _seed_call_participants_from_state(self):
        """Populate _call_memberships from current room state after initial sync.

        matrix-nio only fires UnknownEvent callbacks for timeline events, not for
        the state snapshot delivered in the initial sync. This method queries the
        current state directly so a bot restart doesn't lose participant tracking.
        """
        for room_id in list(self.client.rooms):
            try:
                resp = await self.client.room_get_state(room_id)
            except Exception as exc:
                logger.warning("_seed_call_participants: room_get_state failed for %s: %s", room_id, exc)
                continue
            if not hasattr(resp, "events"):
                continue
            for evt in resp.events:
                if evt.get("type") != "org.matrix.msc3401.call.member":
                    continue
                content = evt.get("content", {})
                state_key = evt.get("state_key", "")
                if not state_key:
                    continue
                per_device_match = _PER_DEVICE_CALL_STATE_KEY_RE.match(state_key)
                if per_device_match:
                    user_id = per_device_match.group(1)
                    active = content.get("application") == "m.call" and bool(content)
                else:
                    user_id = state_key
                    memberships = content.get("memberships", [])
                    active = any(m.get("application") == "m.call" for m in memberships)
                if not user_id:
                    continue
                self._set_call_membership(room_id, state_key, user_id, active)
                if active:
                    logger.info("seed call_participants: room=%s user=%s active=True", room_id, user_id)

    def _played_seconds(self) -> int:
        """Seconds the active play has been audible (a pause in progress doesn't count)."""
        if not self._active_play:
            return 0
        now = self._paused_wall if self._paused_wall is not None else time.time()
        return max(0, int(now - self._active_play["started_at"]))

    async def _emit_track_finished(self, call_room_id: str, reason: str, played_s: Optional[int] = None):
        if self._active_play is None or self._active_play.get("finished"):
            return
        self._active_play["finished"] = True
        if played_s is None:
            played_s = self._played_seconds()

        current = set(self.get_call_participants(call_room_id))
        eligible = list(self._active_play["started_participants"] & current)
        play_id = self._active_play["play_id"]
        self._queue_emit(
            call_room_id,
            EVT_TRACK_FINISHED,
            {
                "play_id": play_id,
                "played_s": int(played_s),
                "reason": reason,
                "finished_at": int(time.time()),
                "eligible_participants": eligible,
            },
            f"reason={reason} played_s={int(played_s)} eligible={eligible}",
        )
        self._active_play = None

    def _start_play_tracking(self, room_id: str, track: dict):
        """New active play for the scrobbler; announces it if the metadata is usable."""
        artist, track_name, album, quality = extract_metadata(track)
        play_id = str(uuid.uuid4())
        participants = self.get_call_participants(room_id)
        started_at = int(time.time())
        duration_s = int(track.get("duration") or 0)
        self._active_play = {
            "play_id": play_id,
            "room_id": room_id,
            "started_at": started_at,
            "started_participants": set(participants),
            "duration_s": duration_s,
            "finished": False,
        }
        if not (artist and track_name):
            logger.info(
                "Skipping track_started emit: no artist/track metadata (play_id=%s title=%r channel=%r uploader=%r artist=%r track=%r)",
                play_id,
                track.get("title"), track.get("channel"), track.get("uploader"),
                track.get("artist"), track.get("track"),
            )
            return
        self._queue_emit(
            room_id,
            EVT_TRACK_STARTED,
            {
                "play_id": play_id,
                "source": detect_source(track.get("source_url", "")),
                "source_url": track.get("source_url", ""),
                "artist": artist,
                "track": track_name,
                "album": album,
                "duration_s": duration_s,
                "metadata_quality": quality,
                "started_at": started_at,
                "call_participants": participants,
                "emitter": {"user_id": self.client.user_id, "kind": BOT_KIND},
            },
            f"track={artist} - {track_name} quality={quality} participants={participants}",
        )

    def _queue_emit(self, room_id: str, event_type: str, content: dict, details: str):
        if not self.config.SCROBBLER_EVENTS:
            return
        self._emit_queue.put_nowait((room_id, event_type, content, details))

    async def _emit_loop(self):
        while True:
            room_id, event_type, content, details = await self._emit_queue.get()
            kind = event_type.rsplit(".", 1)[-1]
            # Chat replies first: both share the account's send rate limit, and a
            # scrobbler event can wait a few seconds while a command's reply can't.
            try:
                await asyncio.wait_for(self._message_queue.join(), timeout=EMIT_YIELD_MAX_SECONDS)
            except asyncio.TimeoutError:
                pass
            try:
                # Unverified devices: nio counts only devices verified in nio itself,
                # cross-signing doesn't count; refusing them would drop the event.
                resp = await asyncio.wait_for(
                    self.client.room_send(
                        room_id=room_id,
                        message_type=event_type,
                        content=content,
                        ignore_unverified_devices=True,
                    ),
                    timeout=120,
                )
                self._log_emit(kind, resp, content.get("play_id"), details)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Emit %s play_id=%s failed: %r (%s)", kind, content.get("play_id"), exc, details)
            finally:
                self._emit_queue.task_done()

    def _start_message_dispatcher(self):
        if self._message_dispatch_task and not self._message_dispatch_task.done():
            return
        self._message_dispatch_task = asyncio.create_task(self._message_dispatch_loop())

    async def _stop_message_dispatcher(self):
        task = self._message_dispatch_task
        if not task:
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        self._message_dispatch_task = None

    def _should_emit_message(self, text: str, priority: str) -> bool:
        if priority == "critical":
            return True
        if not self.config.QUIET_MODE:
            return True
        if priority == "noisy":
            return False
        return True

    async def _queue_message(self, room_id: str, text: str, *, html_body: Optional[str], priority: str):
        if not self._should_emit_message(text, priority):
            return
        if priority == "noisy":
            key = (room_id, text)
            now = asyncio.get_running_loop().time()
            cool_until = self._noisy_cooldown_until.get(key)
            if cool_until is not None and now < cool_until:
                return
            self._noisy_cooldown_until[key] = now + 2.5
        self._message_seq += 1
        msg = OutboundMessage(room_id=room_id, text=text, html_body=html_body, priority=priority)
        await self._message_queue.put((self._message_priority_map[priority], self._message_seq, msg))

    async def _message_dispatch_loop(self):
        try:
            while True:
                _, _, msg = await self._message_queue.get()
                try:
                    await self._send_message_now(msg.room_id, msg.text, html_body=msg.html_body)
                except Exception as exc:
                    logger.error("Error sending message: %s", exc)
                finally:
                    self._message_queue.task_done()
        except asyncio.CancelledError:
            raise

    async def _send_message_now(self, room_id: str, text: str, *, html_body: Optional[str] = None):
        content = {"msgtype": "m.text", "body": text}
        if self.config.RICH_FORMATTING and html_body:
            content["format"] = "org.matrix.custom.html"
            content["formatted_body"] = html_body
        await self.client.room_send(room_id, message_type="m.room.message", content=content, ignore_unverified_devices=True)

    def _ensure_advance_watchdog(self):
        if self._advance_watchdog_task and not self._advance_watchdog_task.done():
            return
        self._advance_watchdog_task = asyncio.create_task(self._advance_watchdog_loop())

    def _cancel_advance_watchdog(self):
        if self._advance_watchdog_task and not self._advance_watchdog_task.done():
            self._advance_watchdog_task.cancel()
            logger.info("Cancelled advance watchdog")
        self._advance_watchdog_task = None

    async def _advance_watchdog_loop(self):
        try:
            while True:
                await asyncio.sleep(4.0)

                if self.audio_queue.loop_mode:
                    continue
                if not self.audio_queue.current or not self.audio_queue.queue:
                    continue
                if self._current_track_started_at is None or self._paused_at is not None:
                    continue

                duration = self.audio_queue.current.get("duration")
                if duration is None:
                    continue

                elapsed = asyncio.get_running_loop().time() - self._current_track_started_at
                threshold = float(duration) + float(self.audio_queue.auto_advance_buffer) + 6.0
                if elapsed < threshold:
                    continue

                room_id = self._current_room_id
                if not room_id:
                    continue
                if not self._is_joined_in_room_call(room_id):
                    continue

                generation = self._playback_generation
                async with self._playback_lock:
                    # A skip or the timer may have started another track while we
                    # waited for the lock: that one hasn't overrun.
                    if generation != self._playback_generation:
                        continue
                    if not (self.audio_queue.current and self.audio_queue.queue):
                        continue
                    logger.warning(
                        "Advance watchdog forcing next track after %.2fs (threshold %.2fs)",
                        elapsed,
                        threshold,
                    )
                    await self._advance_queue(room_id, force_next=True)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("Advance watchdog error: %s", exc)

    @staticmethod
    def _format_duration(seconds: Optional[float]) -> str:
        # Clock format: the same in every language, nothing to translate.
        if seconds is None:
            return "?"
        total = int(round(max(0.0, seconds)))
        hours, rem = divmod(total, 3600)
        minutes, secs = divmod(rem, 60)
        if hours > 0:
            return f"{hours}:{minutes:02d}:{secs:02d}"
        return f"{minutes}:{secs:02d}"

    @classmethod
    def _format_track_line(cls, prefix: str, track: dict) -> str:
        duration = track.get("duration")
        duration_str = f" [{cls._format_duration(duration)}]" if duration is not None else ""
        return f"{prefix}{display_title(track)}{duration_str}"

    @staticmethod
    def _format_bytes(num_bytes: int) -> str:
        size = float(max(0, int(num_bytes)))
        units = ["B", "KB", "MB", "GB", "TB"]
        unit_index = 0
        while size >= 1024.0 and unit_index < len(units) - 1:
            size /= 1024.0
            unit_index += 1
        if unit_index == 0:
            return f"{int(size)} {units[unit_index]}"
        return f"{size:.1f} {units[unit_index]}"

    def _next_track_title(self) -> Optional[str]:
        if not self.audio_queue.queue:
            return None
        return display_title(self.audio_queue.queue[0])

    @staticmethod
    def _sum_known_durations(tracks: list[dict]) -> Optional[float]:
        total = 0.0
        for track in tracks:
            duration = track.get("duration") if isinstance(track, dict) else None
            if duration is None:
                return None
            total += float(duration)
        return total

    def _current_track_remaining_seconds(self) -> Optional[float]:
        if not self.audio_queue.current:
            return None

        duration = self.audio_queue.current.get("duration")
        if duration is None:
            return None

        if self._current_track_started_at is None:
            return None

        now = self._paused_at if self._paused_at is not None else asyncio.get_running_loop().time()
        elapsed = max(0.0, now - self._current_track_started_at)
        return max(0.0, float(duration) - elapsed)

    def _load_room_languages(self) -> dict[str, str]:
        try:
            data = json.loads(self._room_languages_file.read_text(encoding="utf-8"))
            return {str(k): str(v) for k, v in data.items()} if isinstance(data, dict) else {}
        except FileNotFoundError:
            return {}
        except (OSError, ValueError) as exc:
            logger.warning("Could not read %s: %s", self._room_languages_file, exc)
            return {}

    def _save_room_languages(self):
        tmp = self._room_languages_file.with_suffix(".tmp")
        tmp.parent.mkdir(parents=True, exist_ok=True)
        tmp.write_text(json.dumps(self._room_languages, indent=2, sort_keys=True), encoding="utf-8")
        os.replace(tmp, self._room_languages_file)

    def _room_language(self, room_id: Optional[str]) -> str:
        language = self._room_languages.get(room_id or "") or self.config.BOT_LANGUAGE
        return language if language in self.translations.catalogs else "en"

    def _t(self, room_id: Optional[str], key: str, /, **values) -> str:
        """A chat message in the room's language (see i18n.py, locales/)."""
        return self.translations.text(self._room_language(room_id), key, **values)

    def _language_label(self, code: str) -> str:
        name = self.translations.names.get(code, code)
        return f"{name} ({code})" if name != code else code

    def _help_text(self, room_id: str) -> str:
        return self._t(room_id, "help", prefix=self._prefix, results=SEARCH_RESULTS)

    def _safe_config_text(self) -> str:
        lines = [
            "⚙️ Active config",
            f"Config file: {self.config.config_file}",
            f"Audio dir: {self.config.AUDIO_DIR}",
            f"Saved queues: {self.config.SAVED_QUEUES_FILE}",
            f"Pre-roll silence: {self.config.PREROLL_SILENCE:.1f}s",
            f"Auto-advance buffer: {self.config.AUTO_ADVANCE_BUFFER:.1f}s",
            f"Normalize audio: {'On' if self.config.NORMALIZE_AUDIO else 'Off'}",
            f"Fade-in: {self.config.FADE_IN_MS}ms",
            f"Volume: {self.config.VOLUME_PERCENT}%",
            f"Scrobbler events: {'On' if self.config.SCROBBLER_EVENTS else 'Off'}",
            f"Cache mode: {self.config.AUDIO_CACHE_MODE}",
            f"Cache max size: {self._format_bytes(self.config.AUDIO_CACHE_MAX_BYTES)}",
            f"Delete after playback: {'On' if self.config.AUDIO_CACHE_DELETE_AFTER_PLAYBACK else 'Off'}",
            f"Delete on shutdown: {'On' if self.config.AUDIO_CACHE_DELETE_ON_SHUTDOWN else 'Off'}",
            f"Search mode: {self.config.SEARCH_MODE}",
            f"Search timeout: {self.config.SEARCH_TIMEOUT_SECONDS:.1f}s",
            f"Extractor retries: {self.config.EXTRACTOR_RETRIES}",
            f"Download format: {self.config.AUDIO_DOWNLOAD_FORMAT}",
            f"Audio quality: {self.config.AUDIO_QUALITY}",
            f"Stream first when idle: {'On' if self.config.STREAM_FIRST_IDLE else 'Off'}",
            f"Stream prefetch current: {'On' if self.config.STREAM_PREFETCH_CURRENT else 'Off'}",
            (
                "Stream retry to file on fail: "
                f"{'On' if self.config.STREAM_RETRY_TO_FILE_ON_FAIL else 'Off'}"
            ),
            f"Skip cooldown: {self.config.SKIP_COOLDOWN_SECONDS:.1f}s",
            f"Worker restarts: {self.config.WORKER_MAX_RESTART_ATTEMPTS}",
            f"Worker heartbeat: {self.config.WORKER_HEARTBEAT_INTERVAL:.1f}s",
            f"Stop-timeout recovery: {self.config.WORKER_STOP_TIMEOUT_RESTART_THRESHOLD}",
            f"Worker membership mode: {self.config.WORKER_MEMBERSHIP_MODE}",
            f"Call audio: {'stereo' if self.config.WORKER_STEREO else 'mono'}, "
            f"{f'{self.config.WORKER_AUDIO_BITRATE_KBPS} kbps' if self.config.WORKER_AUDIO_BITRATE_KBPS else 'default bitrate'}",
            f"Playlist max tracks/request: {self.config.PLAYLIST_MAX_TRACKS_PER_REQUEST}",
            f"Playlist background concurrency: {self.config.PLAYLIST_BACKGROUND_LOAD_CONCURRENCY}",
            f"History limit: {self.config.HISTORY_LIMIT}",
            (
                f"Auto-accept invites: trusted: {', '.join(self.config.AUTO_ACCEPT_INVITES_FROM)}"
                if self.config.AUTO_ACCEPT_INVITES_FROM
                else f"Auto-accept invites: {'On' if self.config.AUTO_ACCEPT_INVITES else 'Off'}"
            ),
            f"Progress messages: {'On' if self.config.SHOW_PROGRESS_MESSAGES else 'Off'}",
            f"Quiet mode: {'On' if self.config.QUIET_MODE else 'Off'}",
            f"Log file: {self.config.LOG_FILE}",
            f"Clean log: {self.config.CLEAN_LOG_FILE if self.config.CLEAN_LOG_ENABLED else 'Off'}",
        ]
        if self.config.AUDIO_CACHE_MAX_BYTES_CLAMPED:
            lines.append(
                "Cache max was below 200MB, clamped to minimum 200MB for size_lru stability"
            )
        return "\n".join(lines)

    def _default_config_text(self) -> str:
        return Config.defaults_text()

    async def _apply_audio_settings_to_worker(self):
        if not self.call_worker.running:
            return
        await self.call_worker.set_audio_settings(
            normalize_audio=self.config.NORMALIZE_AUDIO,
            fade_in_ms=self.config.FADE_IN_MS,
            volume_percent=self.config.VOLUME_PERCENT,
        )

    @staticmethod
    def _track_source_ref(track: Optional[dict]) -> Optional[str]:
        if not isinstance(track, dict):
            return None
        active_source = track.get("active_source")
        if isinstance(active_source, str) and active_source:
            return active_source
        file_path = track.get("file")
        if isinstance(file_path, str) and file_path:
            return file_path
        stream_url = track.get("stream_url")
        if isinstance(stream_url, str) and stream_url:
            return stream_url
        return None

    async def _play_track_in_worker(self, track: dict, *, pre_stop: bool = True) -> tuple[int, Optional[str]]:
        self._paused_at = None
        self._paused_wall = None
        should_wait_stop = pre_stop and self.call_worker.state == "playing"
        if should_wait_stop:
            await self.call_worker.stop_playback(wait_for_terminal=True, timeout=3.0)
        file_path = track.get("file")
        stream_url = track.get("stream_url")
        title = track.get("title")
        selected_source: Optional[str] = None
        if isinstance(file_path, str) and file_path:
            await self.call_worker.play(file_path, title)
            selected_source = file_path
        elif isinstance(stream_url, str) and stream_url:
            await self.call_worker.play_stream(stream_url, title)
            selected_source = stream_url
        else:
            raise RuntimeError("Track has neither file nor stream source")
        track["active_source"] = selected_source
        self._playback_generation += 1
        generation = self._playback_generation
        return generation, self._track_source_ref(track)

    def _cancel_stream_prefetch(self):
        if self._stream_prefetch_task and not self._stream_prefetch_task.done():
            self._stream_prefetch_task.cancel()
        self._stream_prefetch_task = None

    async def _prefetch_current_track_file(self, room_id: str, source_url: str, expected_generation: int):
        try:
            success, result = await self.audio_queue.download_audio(source_url)
        except asyncio.CancelledError:
            return
        if not success or not isinstance(result, dict):
            return

        replay_needed = False
        async with self._playback_lock:
            if expected_generation != self._playback_generation:
                return
            current = self.audio_queue.current
            if not isinstance(current, dict):
                return
            if current.get("source_url") != source_url:
                return
            current["file"] = result.get("file")
            current["non_cacheable"] = bool(result.get("non_cacheable"))
            # Only if this track's playback really is over (its watcher is gone). The
            # worker state alone says "joined" for ~1 s after a stream play starts,
            # and a cached file arrives faster than that: it would replay the track
            # with a bogus "finished" and a second play_id.
            watcher = self._worker_playback_task
            replay_needed = (
                self.audio_queue.loop_mode
                and (watcher is None or watcher.done())
                and self.call_worker.state != "playing"
            )
            if replay_needed:
                try:
                    await self._advance_queue(room_id, force_next=False, pre_stop=False)
                except Exception:
                    logger.exception("Failed to resume loop playback after stream prefetch")

    async def _retry_stream_track_as_file(self, room_id: str, track: dict) -> bool:
        source_url = track.get("source_url") if isinstance(track, dict) else None
        if not isinstance(source_url, str) or not source_url:
            return False
        success, result = await self.audio_queue.download_audio(source_url)
        if not success or not isinstance(result, dict):
            return False

        track["file"] = result.get("file")
        track["non_cacheable"] = bool(result.get("non_cacheable"))
        try:
            generation, source_ref = await self._play_track_in_worker(track, pre_stop=False)
        except Exception:
            return False

        self._current_track_started_at = asyncio.get_running_loop().time()
        self._worker_playback_task = asyncio.create_task(
            self._wait_for_worker_playback(room_id, generation, source_ref)
        )
        self._arm_auto_advance(
            track.get("duration"),
            room_id,
            expected_generation=generation,
            expected_source=source_ref,
        )
        return True

    def _should_stream_first_idle(self) -> bool:
        if not self.config.STREAM_FIRST_IDLE:
            return False
        if self.audio_queue.current is not None:
            return False
        if self.audio_queue.queue:
            return False
        if self.call_worker.state == "playing":
            return False
        return True

    async def _try_stream_first_idle_play(self, room_id: str, args: str) -> bool:
        if not self._should_stream_first_idle():
            return False

        join_task = asyncio.create_task(self._join_call_for_room(room_id))
        resolve_task = asyncio.create_task(self.audio_queue.resolve_stream_source(args))
        join_ok = False
        try:
            join_ok, resolved = await asyncio.gather(join_task, resolve_task)
        except Exception as exc:
            logger.warning("Stream-first resolve failed, falling back to file download: %s", exc)
            return False

        if not join_ok:
            return False
        if not isinstance(resolved, tuple) or len(resolved) != 2:
            return False
        success, result = resolved
        if not success:
            return False
        if not isinstance(result, dict):
            return False

        source_url = result.get("source_url")
        if isinstance(source_url, str) and self.audio_queue.has_source(source_url):
            await self.send_message(
                room_id,
                self._t(room_id, "track_already_playing_queued", prefix=self._prefix),
            )
            return True

        title = result.get("title") or "track"
        duration = result.get("duration")

        track = {
            "title": title,
            "duration": duration,
            "source_url": source_url,
            "stream_url": result.get("stream_url"),
            "non_cacheable": True,
            "artist":   result.get("artist",   ""),
            "track":    result.get("track",    ""),
            "album":    result.get("album",    ""),
            "channel":  result.get("channel",  ""),
            "uploader": result.get("uploader", ""),
        }

        previous_current = self.audio_queue.current
        previous_room_id = self._current_room_id
        previous_started_at = self._current_track_started_at
        self._cancel_stream_prefetch()
        generation = 0
        source_ref = None
        try:
            async with self._playback_lock:
                self.audio_queue.current = track
                self._current_room_id = room_id
                generation, source_ref = await self._play_track_in_worker(track, pre_stop=False)
                self._current_track_started_at = asyncio.get_running_loop().time()
                self._push_play_history(room_id, track)
                self._worker_playback_task = asyncio.create_task(
                    self._wait_for_worker_playback(room_id, generation, source_ref)
                )
                self._arm_auto_advance(
                    track.get("duration"),
                    room_id,
                    expected_generation=generation,
                    expected_source=source_ref,
                )
        except Exception as exc:
            logger.warning("Stream-first playback start failed, falling back to file download: %s", exc)
            async with self._playback_lock:
                if self.audio_queue.current is track:
                    self.audio_queue.current = previous_current
                    self._current_room_id = previous_room_id
                    self._current_track_started_at = previous_started_at
            return False

        await self.send_message(room_id, self._t(room_id, "now_playing", title=display_title(track) or title))

        self._start_play_tracking(room_id, track)

        if self.config.STREAM_PREFETCH_CURRENT and isinstance(source_url, str) and source_url:
            self._stream_prefetch_task = asyncio.create_task(
                self._prefetch_current_track_file(room_id, source_url, generation)
            )
        return True

    def _arm_auto_advance(
        self,
        duration: Optional[float],
        room_id: str,
        *,
        expected_generation: Optional[int] = None,
        expected_source: Optional[str] = None,
    ):
        if self.audio_queue.loop_mode:
            logger.info("Loop mode enabled - auto-advance disabled")
            return
        if duration is None:
            logger.warning("No duration available - auto-advance disabled for this track")
            return
        self._cancel_auto_advance()
        generation = self._playback_generation if expected_generation is None else expected_generation
        self._auto_advance_task = asyncio.create_task(
            self._auto_advance_timer(float(duration), room_id, generation, expected_source)
        )

    def _schedule_resync(self, room_id: str):
        """Replay the current track on a new worker process, under the playback lock.

        The restart/recovery event can arrive in the middle of a track change (a
        stop timeout inside _advance_queue recovers the worker): replaying right
        away would start the track twice. If a track was started after the event,
        the new worker is already playing it.
        """
        generation = self._playback_generation

        async def resync():
            async with self._playback_lock:
                if generation != self._playback_generation:
                    logger.info("Skipping resync: a track was started after the worker came back")
                    return
                await self._sync_current_track_to_worker(room_id, announce=False)

        task = asyncio.create_task(resync())
        self._command_tasks.add(task)
        task.add_done_callback(self._command_tasks.discard)

    async def _sync_current_track_to_worker(self, room_id: str, announce: bool = True) -> bool:
        if not self.audio_queue.current:
            return False
        if self._current_room_id != room_id:
            return False
        if not self._is_joined_in_room_call(room_id):
            return False

        try:
            self._cancel_worker_playback_wait()
            # The track restarts from the beginning: close the interrupted play (it
            # isn't a finished listen) and start a new one for the scrobbler. Before
            # replaying, which clears the pause state the played time depends on.
            await self._emit_track_finished(room_id, "stopped")
            await self.call_worker.stop_playback(wait_for_terminal=False)
            generation, source_ref = await self._play_track_in_worker(self.audio_queue.current, pre_stop=False)
            self._current_track_started_at = asyncio.get_running_loop().time()
            self._start_play_tracking(room_id, self.audio_queue.current)
            self._worker_playback_task = asyncio.create_task(
                self._wait_for_worker_playback(room_id, generation, source_ref)
            )
            self._arm_auto_advance(
                self.audio_queue.current.get("duration"),
                room_id,
                expected_generation=generation,
                expected_source=source_ref,
            )
            if announce:
                await self.send_message(room_id, self._t(room_id, "synced_current_track_call", title=self.audio_queue.current['title']))
            return True
        except Exception as exc:
            self._current_track_started_at = None
            logger.error("Failed to sync current track after join/recovery: %s", exc)
            if announce:
                await self.send_message(room_id, self._t(room_id, "joined_call_but_could", exc=exc))
            return False

    def _push_play_history(self, room_id: str, track: dict):
        if not room_id or not isinstance(track, dict):
            return
        title = track.get("title")
        if not isinstance(title, str) or not title:
            return
        self._play_history_by_room[room_id].appendleft(
            {
                "title": title,
                "duration": track.get("duration"),
                "at": int(time.time()),
            }
        )

    async def _join_call_for_room(self, room_id: str, *, announce_if_already_joined: bool = False) -> bool:
        if self._is_joined_in_room_call(room_id):
            if announce_if_already_joined:
                await self.send_message(room_id, self._t(room_id, "already_joined_room_call"))
            return True

        if not self.call_worker.available:
            await self.send_message(
                room_id,
                self._t(room_id, "call_worker_not_found"),
            )
            return False

        try:
            await self.call_worker.start(room_id)
        except Exception as exc:
            await self.send_message(room_id, self._t(room_id, "failed_join_element_call", exc=exc))
            return False

        await self.send_message(room_id, self._t(room_id, "joined_element_call"))
        await self._apply_audio_settings_to_worker()

        async with self._playback_lock:
            if self.audio_queue.current and self._current_room_id == room_id:
                await self._sync_current_track_to_worker(room_id, announce=True)

        return True

    def _is_joined_in_room_call(self, room_id: str) -> bool:
        return self.call_worker.running and self.call_worker.room_id == room_id

    def _active_call_room_id(self, command_room_id: str) -> str:
        """Return the room the call worker is in, falling back to the command room."""
        return self.call_worker.room_id or command_room_id

    async def _require_joined_in_room_call(self, room_id: str, command_name: str) -> bool:
        if self.call_worker.running:
            return True
        await self.send_message(
            room_id,
            self._t(room_id, "join_call_first_with", prefix=self._prefix, command_name=command_name),
        )
        return False

    def _cancel_worker_playback_wait(self):
        if self._worker_playback_task is asyncio.current_task():
            # Called from the watcher itself (play_ended -> _advance_queue): just forget it.
            self._worker_playback_task = None
            return
        if self._worker_playback_task and not self._worker_playback_task.done():
            self._worker_playback_task.cancel()
            logger.info("Cancelled worker playback watcher")
        self._worker_playback_task = None

    def _background_load_alive(self) -> bool:
        """A playlist/saved queue is still being loaded: the queue isn't really empty."""
        return self._background_load_task is not None and not self._background_load_task.done()

    def _cancel_background_load(self):
        if self._background_load_task and not self._background_load_task.done():
            self._background_load_task.cancel()
            logger.info("Cancelled background queue loader")
        self._background_load_task = None
        self._pending_sources = None
        self._pending_info = {}
        # The current track's file prefetch is left alone: play/load only add to
        # the queue, and without the file a loop replay or a failed stream would
        # need a new download. stop/leave/shutdown cancel it themselves.

    def _is_retryable_stream_failure(self, track) -> bool:
        """True if `track` was playing from a stream URL and can still fall back to a file."""
        return (
            self.config.STREAM_RETRY_TO_FILE_ON_FAIL
            and isinstance(track, dict)
            and isinstance(track.get("stream_url"), str)
            and track.get("active_source") == track.get("stream_url")
            and isinstance(track.get("source_url"), str)
            and bool(track.get("source_url"))
            and not track.get("stream_file_retry_done")
        )

    async def _finish_failed_playback(self, room_id: str):
        """Drop the failed track and move on. Caller holds _playback_lock."""
        self._current_track_started_at = None

        await self._emit_track_finished(room_id, "error")

        if self.audio_queue.queue:
            await self._advance_queue(room_id, force_next=True, pre_stop=False)
            return

        if self.audio_queue.current is not None:
            self.audio_queue.current = None

    async def _recover_failed_stream(self, room_id: str, expected_generation: int, track: dict) -> bool:
        """A stream URL failed after ffmpeg had started (e.g. YouTube 403): play the file instead.

        The stream-first path starts a background download of the same track, so wait
        for it first; download here only if it isn't there. Returns False if the track
        still could not be played (the caller then drops it as before).
        """
        logger.warning("Stream playback failed, retrying from file: %s", track.get("title"))
        await self.send_message(room_id, self._t(room_id, "stream_failed_retrying_from"), priority="critical")

        prefetch = self._stream_prefetch_task
        if prefetch is not None and not prefetch.done():
            try:
                await asyncio.wait_for(asyncio.shield(prefetch), timeout=120)
            except asyncio.TimeoutError:
                logger.warning("Stream prefetch still running after 120s, downloading again")
            except Exception:
                logger.exception("Stream prefetch failed, downloading again")

        ok = False
        async with self._playback_lock:
            if expected_generation != self._playback_generation or self.audio_queue.current is not track:
                logger.info("Ignoring stream fallback (track changed while waiting)")
                return True
            try:
                file_path = track.get("file")
                if isinstance(file_path, str) and file_path and Path(file_path).is_file():
                    generation, source_ref = await self._play_track_in_worker(track, pre_stop=False)
                    self._current_track_started_at = asyncio.get_running_loop().time()
                    self._worker_playback_task = asyncio.create_task(
                        self._wait_for_worker_playback(room_id, generation, source_ref)
                    )
                    self._arm_auto_advance(
                        track.get("duration"),
                        room_id,
                        expected_generation=generation,
                        expected_source=source_ref,
                    )
                    ok = True
                else:
                    ok = await self._retry_stream_track_as_file(room_id, track)
            except Exception:
                logger.exception("Stream fallback to file failed")
                ok = False

        if ok:
            await self.send_message(room_id, self._t(room_id, "now_playing", title=display_title(track)))
        else:
            await self.send_message(room_id, self._t(room_id, "stream_failed_file_fallback"))
        return ok

    async def _wait_for_worker_playback(
        self,
        room_id: str,
        expected_generation: int,
        expected_source: Optional[str],
    ):
        try:
            event = await self.call_worker.wait_for_playback_terminal(expected_source=expected_source)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(f"Call worker playback error: {exc}")
            if "InvalidState - failed to capture frame" not in str(exc):
                await self.send_message(room_id, self._t(room_id, "playback_worker_error", exc=exc))
            stream_retry_track: Optional[dict] = None
            async with self._playback_lock:
                if expected_generation != self._playback_generation:
                    logger.info("Ignoring playback recovery (generation changed)")
                    return
                current = self.audio_queue.current
                current_source = self._track_source_ref(current)
                if expected_source and current_source and current_source != expected_source:
                    logger.info("Ignoring playback recovery (track changed)")
                    return

                self._cancel_auto_advance()
                if self._is_retryable_stream_failure(current):
                    current["stream_file_retry_done"] = True
                    stream_retry_track = current
                else:
                    await self._finish_failed_playback(room_id)
                    return

            # Outside the lock: the stream prefetch needs it to store the file.
            if await self._recover_failed_stream(room_id, expected_generation, stream_retry_track):
                return
            async with self._playback_lock:
                if (
                    expected_generation == self._playback_generation
                    and self.audio_queue.current is stream_retry_track
                ):
                    await self._finish_failed_playback(room_id)
            return

        if expected_generation != self._playback_generation:
            logger.info("Ignoring stale worker terminal event (generation changed)")
            return

        event_source = event.get("source") or event.get("file") or event.get("url")
        current_source = self._track_source_ref(self.audio_queue.current)
        if expected_source and current_source and event_source and event_source != expected_source:
            logger.info("Ignoring stale worker terminal event (track changed)")
            return

        event_name = event.get("event")
        if event_name != "play_ended":
            return

        await self._emit_track_finished(room_id, "finished")

        async with self._playback_lock:
            await self._advance_queue(room_id, force_next=not self.audio_queue.loop_mode)

    async def _download_first_available_source(self, sources: list[str]) -> tuple[Optional[dict], int, int]:
        first_item: Optional[dict] = None
        first_index = -1
        failures = 0
        for index, source_url in enumerate(sources):
            success, result = await self.audio_queue.download_audio(source_url)
            if success and isinstance(result, dict):
                first_item = result
                first_index = index
                break
            failures += 1
        return first_item, first_index, failures

    @staticmethod
    def _parse_int_arg(raw: str) -> Optional[int]:
        try:
            return int(raw)
        except ValueError:
            return None

    async def _load_remaining_tracks(
        self,
        room_id: str,
        collection_name: str,
        remaining_sources: list[str],
        *,
        dedupe_existing: bool,
        source_label: str,
        info: Optional[dict[str, dict]] = None,
    ):
        if not remaining_sources:
            return

        failures = 0
        pending = deque(remaining_sources)
        self._pending_sources = pending
        self._pending_info = dict(info or {})

        while pending:
            # Wait until the queue has no pre-downloaded tracks before fetching next.
            # The next source stays in `pending` meanwhile, so `queue` still lists it.
            while len(self.audio_queue.queue) >= 1:
                try:
                    await asyncio.sleep(LOADER_POLL_SECONDS)
                except asyncio.CancelledError:
                    return
            if not pending:
                break
            source_url = pending.popleft()

            if dedupe_existing and self.audio_queue.has_source(source_url):
                continue

            try:
                ok, result = await self.audio_queue.download_audio(source_url)
            except asyncio.CancelledError:
                return

            if not ok or not isinstance(result, dict):
                failures += 1
                continue

            async with self._playback_lock:
                src = result.get("source_url")
                if dedupe_existing and isinstance(src, str) and src and self.audio_queue.has_source(src):
                    continue
                self.audio_queue.add_to_queue(
                    result["file"],
                    result.get("title"),
                    result.get("duration"),
                    source_url=src,
                    non_cacheable=bool(result.get("non_cacheable")),
                    artist=result.get("artist", ""),
                    track=result.get("track", ""),
                    album=result.get("album", ""),
                    channel=result.get("channel", ""),
                    uploader=result.get("uploader", ""),
                )
                if self.audio_queue.current is None:
                    logger.info("Background loader: playback was idle, starting the newly loaded track")
                    await self._advance_queue(room_id, force_next=True)

        if self._pending_sources is pending:
            self._pending_sources = None
            self._pending_info = {}

        if failures:
            await self.send_message(room_id, self._t(room_id, "failed_load_from", failures=failures, source_label=source_label, collection_name=collection_name))

    async def _on_call_worker_event(self, event: dict):
        event_name = event.get("event")
        room_id = event.get("roomId") or self._current_room_id or self.call_worker.room_id or self.call_worker.desired_room_id
        if not room_id:
            return

        if event_name == "compatibility_notice":
            message = event.get("message")
            if isinstance(message, str) and message:
                await self.send_message(room_id, self._t(room_id, "worker_notice", message=message))
            return

        if event_name == "e2ee_unavailable":
            logger.warning("Call audio E2EE unavailable, playing unencrypted: %s", event.get("reason"))
            await self.send_message(room_id, self._t(room_id, "e2ee_unavailable"))
            return

        if event_name == "worker_restart_attempt":
            self._restart_failed_notified = False
            attempt = event.get("attempt")
            backoff = event.get("backoff")
            await self.send_message(
                room_id,
                self._t(room_id, "call_worker_disconnected_reconnecting", attempt=attempt, backoff=backoff),
                priority="critical",
            )
            return

        if event_name == "worker_restarted":
            self._restart_failed_notified = False
            await self.send_message(room_id, self._t(room_id, "call_worker_reconnected"), priority="critical")
            self._schedule_resync(room_id)
            return

        if event_name == "worker_restart_failed":
            if not self._restart_failed_notified:
                self._restart_failed_notified = True
                await self.send_message(room_id, self._t(room_id, "call_worker_could_not", prefix=self._prefix), priority="critical")
            return

        if event_name == "worker_heartbeat_timeout":
            logger.warning("Call worker heartbeat timeout detected")
            return

        if event_name == "worker_stop_timeout":
            count = event.get("count")
            threshold = event.get("threshold")
            logger.warning("Call worker stop timeout (%s/%s)", count, threshold)
            return

        if event_name == "worker_recovering":
            await self.send_message(room_id, self._t(room_id, "playback_backend_stalled_recovering"), priority="critical")
            return

        if event_name == "worker_recovered":
            await self.send_message(room_id, self._t(room_id, "call_worker_recovered"), priority="critical")
            self._schedule_resync(room_id)
            return

    async def _auto_advance_timer(
        self,
        duration: float,
        room_id: str,
        expected_generation: int,
        expected_source: Optional[str],
    ):
        try:
            total_wait = duration + self.audio_queue.auto_advance_buffer
            logger.info(
                f"Auto-advance timer set for {total_wait:.2f}s "
                f"(track: {duration:.2f}s + buffer: {self.audio_queue.auto_advance_buffer:.2f}s)"
            )
            await asyncio.sleep(total_wait)
            # Under the lock, like every other track change: otherwise a skip running
            # at the same moment starts a track and this starts another on top of it.
            async with self._playback_lock:
                if expected_generation != self._playback_generation:
                    logger.info("Skipping stale auto-advance timer (generation changed)")
                    return
                current_source = self._track_source_ref(self.audio_queue.current)
                if expected_source and current_source and current_source != expected_source:
                    logger.info("Skipping stale auto-advance timer (track changed)")
                    return
                await self._advance_queue(room_id, from_timer=True)
        except asyncio.CancelledError:
            logger.info("Auto-advance timer cancelled")
            raise
        except Exception as exc:
            logger.error(f"Error in auto-advance timer: {exc}")

    async def _advance_queue(
        self,
        room_id: str,
        from_timer: bool = False,
        force_next: bool = False,
        pre_stop: bool = True,
    ):
        await self._emit_track_finished(room_id, "finished")

        previous_current = self.audio_queue.current
        if from_timer:
            self._auto_advance_task = None
        else:
            self._cancel_auto_advance()
        self._cancel_worker_playback_wait()

        if force_next:
            next_track = self.audio_queue.get_next()
        else:
            next_track = self.audio_queue.get_current_or_next()

        if isinstance(previous_current, dict) and previous_current is not next_track:
            self.audio_queue.maybe_delete_track_file(previous_current, trigger="after_playback")

        if not next_track:
            self._current_track_started_at = None
            if self._background_load_task is not None and not self._background_load_task.done():
                # The background loader is still fetching the next track; it starts playback itself
                await self.send_message(room_id, self._t(room_id, "loading_next_track"))
            else:
                await self.send_message(room_id, self._t(room_id, "queue_empty"))
            return

        self._current_room_id = room_id
        loop_indicator = " 🔁" if self.audio_queue.loop_mode else ""

        if not self._is_joined_in_room_call(room_id):
            logger.info("Playback paused: not joined in room call")
            self._current_track_started_at = None
            await self.send_message(
                room_id,
                self._t(room_id, "playback_paused_because_bot", prefix=self._prefix),
            )
            return

        try:
            generation, current_source = await self._play_track_in_worker(next_track, pre_stop=pre_stop)
            self._current_track_started_at = asyncio.get_running_loop().time()
            self._push_play_history(room_id, next_track)
            await self.send_message(room_id, self._t(room_id, "now_playing_loop", title=display_title(next_track), loop_indicator=loop_indicator))

            self._start_play_tracking(room_id, next_track)

            self._worker_playback_task = asyncio.create_task(
                self._wait_for_worker_playback(room_id, generation, current_source)
            )
        except Exception as exc:
            logger.error(f"Failed to send track to call worker: {exc}")
            can_retry_stream = (
                self.config.STREAM_RETRY_TO_FILE_ON_FAIL
                and isinstance(next_track, dict)
                and not next_track.get("file")
                and isinstance(next_track.get("stream_url"), str)
            )
            if can_retry_stream:
                await self.send_message(room_id, self._t(room_id, "stream_failed_retrying_from"), priority="critical")
                fallback_ok = await self._retry_stream_track_as_file(room_id, next_track)
                if fallback_ok:
                    await self.send_message(room_id, self._t(room_id, "now_playing", title=display_title(next_track)))
                    return
            self._current_track_started_at = None
            await self.send_message(room_id, self._t(room_id, "failed_play_call", exc=exc))
            return

        self._arm_auto_advance(
            next_track.get("duration"),
            room_id,
            expected_generation=generation,
            expected_source=current_source,
        )

    async def _on_megolm_event(self, room: MatrixRoom, event: MegolmEvent):
        pending = self._pending_megolm[event.session_id]
        pending.append((room, event))
        try:
            await self.client.request_room_key(event)
        except Exception as exc:
            logger.debug("request_room_key failed for %s: %s", room.room_id, exc)
        if len(pending) == 1:
            asyncio.create_task(self._retry_megolm_decrypt(event.session_id))

    async def _retry_megolm_decrypt(self, session_id: str):
        for delay in (3, 8, 20, 60):
            await asyncio.sleep(delay)
            if session_id not in self._pending_megolm:
                return
            room, probe = self._pending_megolm[session_id][0]
            try:
                self.client.decrypt_event(probe)
            except Exception:
                continue
            pending = self._pending_megolm.pop(session_id)
            for r, ev in pending:
                try:
                    decrypted = self.client.decrypt_event(ev)
                    if isinstance(decrypted, RoomMessageText):
                        await self.on_message(r, decrypted)
                except Exception as exc:
                    logger.debug("E2EE retry: failed to decrypt %s: %s", ev.event_id, exc)
            return
        self._pending_megolm.pop(session_id, None)
        logger.debug("E2EE: key for session %s never arrived, giving up", session_id)

    async def _restore_e2ee_login(self) -> Optional[str]:
        """Load the crypto store before the first sync, so it can decrypt. Returns the device ID."""
        device_id_file = _DATA_DIR / "device_id"
        try:
            stored = device_id_file.read_text(encoding="utf-8").strip() if device_id_file.exists() else ""
        except OSError:
            stored = ""

        # A 5xx (homeserver restarting) comes back as an error response, not an
        # exception; without the crypto store the bot couldn't read encrypted rooms
        # until the next restart. Retry, then trust the stored device id.
        device_id = user_id = None
        for delay in (2, 4, 8, 16, 0):
            try:
                response = await self.client.whoami()
                device_id = getattr(response, "device_id", None)
                user_id = getattr(response, "user_id", None)
            except Exception as exc:
                response = exc
            if device_id and user_id:
                break
            logger.warning("E2EE setup: whoami failed (%s)%s", response, f", retrying in {delay}s" if delay else "")
            if delay:
                await asyncio.sleep(delay)
        if device_id and user_id:
            logger.info("E2EE: whoami ok user_id=%s device_id=%s", user_id, device_id)
        elif stored:
            device_id, user_id = stored, self.config.MATRIX_USER_ID
            logger.warning("E2EE: using the stored device_id %s", device_id)
        else:
            logger.warning("E2EE setup: no device_id, skipping E2EE")
            return None

        # data/device_id is read by scripts/rotate_access_token.py.
        try:
            if stored and stored != device_id:
                logger.warning("E2EE: device_id changed from %s to %s — crypto store may be stale", stored, device_id)
            if stored != device_id:
                device_id_file.write_text(device_id, encoding="utf-8")
        except OSError as exc:
            logger.warning("E2EE: could not persist device_id (%s), continuing", exc)

        try:
            self.client.restore_login(user_id, device_id, self.config.MATRIX_ACCESS_TOKEN)
        except Exception as exc:
            logger.warning("E2EE: restore_login failed (%s), skipping E2EE", exc)
            return None
        if self.client.should_upload_keys:
            try:
                upload = await self.client.keys_upload()
                logger.info("E2EE: keys_upload one_time_key_counts=%s", getattr(upload, "one_time_key_counts", upload))
            except Exception as exc:
                logger.warning("E2EE: keys_upload failed: %s", exc)
        self.call_worker._env_overrides["MATRIX_DEVICE_ID"] = device_id
        return device_id

    async def _finish_e2ee_setup(self, device_id: str):
        """After the first sync: device keys, cross-signing, room keys for joined rooms."""
        try:
            await self.client.keys_query()
        except Exception as exc:
            logger.debug("E2EE: keys_query skipped: %s", exc)

        try:
            await ensure_cross_signing(
                homeserver=self.config.MATRIX_HOMESERVER,
                token=self.config.MATRIX_ACCESS_TOKEN,
                user_id=self.client.user_id,
                device_id=device_id,
                data_dir=_DATA_DIR,
            )
        except Exception as exc:
            logger.warning("E2EE: cross-signing setup failed: %s", exc)

        for room_id, room_obj in self.client.rooms.items():
            if room_obj.encrypted:
                try:
                    await self.client.share_group_session(room_id, ignore_unverified_devices=True)
                    logger.info("E2EE: shared group session for room %s", room_id)
                except Exception as exc:
                    logger.warning("E2EE: share_group_session failed for %s: %s", room_id, exc)
        logger.info("E2EE setup complete device_id=%s", device_id)

    async def on_invite(self, room: MatrixRoom, event: InviteMemberEvent):
        trusted = self.config.AUTO_ACCEPT_INVITES_FROM
        if trusted:
            if event.sender not in trusted:
                logger.info(
                    "Ignoring invite from %s to %s (not in auto_accept_invites_from)",
                    event.sender, room.room_id,
                )
                return
        elif not self.config.AUTO_ACCEPT_INVITES:
            logger.info("Invite received for %s but auto-accept is disabled", room.room_id)
            return

        logger.info("Invited to %s by %s", room.display_name, event.sender)
        await self.client.join(room.room_id)
        joined_room = self.client.rooms.get(room.room_id)
        if joined_room and joined_room.encrypted:
            try:
                await self.client.share_group_session(room.room_id, ignore_unverified_devices=True)
                logger.info("E2EE: shared group session for newly joined room %s", room.room_id)
            except Exception as exc:
                logger.warning("E2EE: share_group_session on invite failed for %s: %s", room.room_id, exc)
        await self.send_message(
            room.room_id,
            self._t(room.room_id, "bot_ready", prefix=self._prefix),
        )

    def _register_command_handlers(self):
        commands = [
            "help",
            "config",
            "defaults",
            "join",
            "leave",
            "play",
            "playnext",
            "search",
            "remove",
            "lang",
            "queue",
            "nowplaying",
            "diag",
            "audio",
            "normalize",
            "fadein",
            "volume",
            "history",
            "save",
            "queues",
            "deletequeue",
            "renamequeue",
            "load",
            "skip",
            "loop",
            "shuffle",
            "pause",
            "resume",
            "stop",
            "status",
        ]
        for command_name in commands:
            self._command_handlers[command_name] = (
                lambda room, args, cmd=command_name: self._handle_command_internal(room, cmd, args)
            )

    async def handle_command(self, room: MatrixRoom, message: str):
        rest = message.strip()[len(self._prefix):].strip()
        parts = rest.split(maxsplit=1)
        if not parts:
            return
        command = parts[0].lower()
        args = parts[1] if len(parts) > 1 else ""

        aliases = {
            "h":    "help",
            "j":    "join",
            "lv":   "leave",
            "p":    "play",
            "pn":   "playnext",
            "next": "playnext",
            "sr":   "search",
            "find": "search",
            "rm":   "remove",
            "language": "lang",
            "q":    "queue",
            "np":   "nowplaying",
            "d":    "diag",
            "a":    "audio",
            "norm": "normalize",
            "fi":   "fadein",
            "v":    "volume",
            "hist": "history",
            "s":    "skip",
            "lp":   "loop",
            "ps":   "pause",
            "unpause": "resume",
            "rs":   "resume",
            "sh":   "shuffle",
            "random": "shuffle",
            "rnd":  "shuffle",
            "x":    "stop",
            "st":   "status",
            "sv":   "save",
            "ld":   "load",
            "qs":   "queues",
            "dq":   "deletequeue",
            "rq":   "renamequeue",
            "cfg":  "config",
            "df":   "defaults",
        }
        command = aliases.get(command, command)
        handler = self._command_handlers.get(command)
        if handler is None:
            await self.send_message(room.room_id, self._t(room.room_id, "unknown_command_help", prefix=self._prefix), priority="normal")
            return
        if command == "load":
            # Replaces the queue: one at a time with `play` (which takes this lock itself).
            async with self._play_request_lock:
                await handler(room, args)
            return
        await handler(room, args)

    async def _handle_command_internal(self, room: MatrixRoom, command: str, args: str):

        if command == "help":
            await self.send_message(room.room_id, self._help_text(room.room_id))
            return

        if command == "config":
            await self.send_message(room.room_id, self._safe_config_text())
            return

        if command == "defaults":
            await self.send_message(room.room_id, self._default_config_text())
            return

        if command == "join":
            target = args.strip() if args and args.strip().startswith("!") and ":" in args else None
            if target:
                if self._is_joined_in_room_call(target):
                    await self.send_message(room.room_id, self._t(room.room_id, "already_joined_call", target=target))
                elif not self.call_worker.available:
                    await self.send_message(room.room_id, self._t(room.room_id, "call_worker_missing"))
                else:
                    try:
                        await self.call_worker.start(target)
                        await self.send_message(room.room_id, self._t(room.room_id, "joined_call_in", target=target))
                        await self._apply_audio_settings_to_worker()
                    except Exception as exc:
                        await self.send_message(room.room_id, self._t(room.room_id, "failed_join_call", target=target, exc=exc))
            else:
                await self._join_call_for_room(room.room_id, announce_if_already_joined=True)
            return

        if command == "leave":
            if not self.call_worker.running:
                await self.send_message(room.room_id, self._t(room.room_id, "not_currently_call"))
                return

            # The auto-advance timer would otherwise fire after the track's end,
            # pop the next track and post "not joined in this room call".
            self._cancel_auto_advance()
            self._cancel_worker_playback_wait()
            self._cancel_background_load()
            self._cancel_stream_prefetch()
            # Under the lock: a track change in progress would otherwise start the
            # next track after we left (or in the new worker).
            async with self._playback_lock:
                self._cancel_auto_advance()
                self._cancel_worker_playback_wait()
                _call_room = self.call_worker.room_id or room.room_id
                await self._emit_track_finished(_call_room, "stopped")
                self._current_track_started_at = None
                await self.call_worker.stop()
            await self.send_message(room.room_id, self._t(room.room_id, "left_element_call"))
            return

        if command == "lang":
            r = room.room_id
            code = args.strip().lower()
            available = ", ".join(self._language_label(c) for c in self.translations.languages)
            if not code:
                await self.send_message(r, self._t(
                    r, "lang_current", language=self._language_label(self._room_language(r)),
                    available=available, prefix=self._prefix, default=self.config.BOT_LANGUAGE,
                ))
                return
            if code == "default":
                self._room_languages.pop(r, None)
                self._save_room_languages()
                await self.send_message(r, self._t(r, "lang_reset", language=self._language_label(self._room_language(r))))
                return
            if code not in self.translations.catalogs:
                await self.send_message(r, self._t(r, "lang_unknown", code=code, available=available))
                return
            self._room_languages[r] = code
            self._save_room_languages()
            await self.send_message(r, self._t(r, "lang_set", language=self._language_label(code)))
            return

        if command == "search":
            if not args:
                await self.send_message(room.room_id, self._t(room.room_id, "usage_search_query", prefix=self._prefix))
                return
            ok, results = await self.audio_queue.search(args, SEARCH_RESULTS)
            if not ok:
                await self.send_message(room.room_id, self._t(room.room_id, "search_failed", results=results))
                return
            if not results:
                await self.send_message(room.room_id, self._t(room.room_id, "no_results_found"))
                return
            self._search_results[room.room_id] = (time.monotonic(), [r["url"] for r in results])
            lines = [f"🔎 {args}", ""]
            for idx, r in enumerate(results, 1):
                duration = f" [{self._format_duration(r['duration'])}]" if r.get("duration") else ""
                channel = f" · {r['channel']}" if r.get("channel") else ""
                lines.append(f"{idx}. {r['title']}{duration}{channel}")
            lines += ["", self._t(room.room_id, "search_pick_hint", prefix=self._prefix, minutes=SEARCH_RESULTS_TTL // 60)]
            await self.send_message(room.room_id, "\n".join(lines))
            return

        if command == "remove":
            try:
                position = int(args.strip())
            except ValueError:
                await self.send_message(room.room_id, self._t(room.room_id, "usage_remove_position_from", prefix=self._prefix))
                return
            async with self._playback_lock:
                if not 1 <= position <= len(self.audio_queue.queue):
                    await self.send_message(room.room_id, self._t(room.room_id, "no_track_at_position", position=position))
                    return
                removed = self.audio_queue.queue[position - 1]
                del self.audio_queue.queue[position - 1]
                self.audio_queue.maybe_delete_track_file(removed, trigger="after_playback")
            await self.send_message(room.room_id, self._t(room.room_id, "removed", title=display_title(removed) or removed.get('title')))
            return

        if command in ("play", "playnext"):
            # playnext: the track goes to the front of the queue instead of the end.
            front = command == "playnext"
            if not args:
                await self.send_message(room.room_id, self._t(room.room_id, "usage_audio_url_query", prefix=self._prefix, command=command))
                return
            if args.strip().isdigit() and room.room_id in self._search_results:
                searched_at, urls = self._search_results[room.room_id]
                index = int(args.strip())
                if time.monotonic() - searched_at <= SEARCH_RESULTS_TTL and 1 <= index <= len(urls):
                    args = urls[index - 1]

            call_room_id = self._active_call_room_id(room.room_id)
            async with self._play_request_lock:
                self._cancel_background_load()

                playlist_ok = False
                playlist_info: dict | None = None

                # Spotify audio is DRM-protected: read title/artists from Spotify,
                # then search YouTube like a plain text query.
                if is_spotify_link(args):
                    try:
                        spotify_item = await resolve_spotify(args)
                    except SpotifyError as exc:
                        await self.send_message(room.room_id, self._t(room.room_id, "spotify_error", exc=exc))
                        return
                    if spotify_item.kind == "track":
                        spotify_track = spotify_item.tracks[0]
                        await self.send_message(
                            room.room_id,
                            self._t(room.room_id, "spotify_searching_youtube", display=spotify_track.display),
                            priority="noisy",
                        )
                        args = spotify_track.query
                    else:
                        playlist_ok = True
                        playlist_info = {
                            "is_playlist": True,
                            "title": spotify_item.name,
                            "entries": [t.query for t in spotify_item.tracks],
                        }
                elif self.audio_queue.looks_like_url(args) and not self.audio_queue.is_single_video_url(
                    self.audio_queue.normalize_media_url(args)
                ):
                    # A plain video link can't be a playlist: skip the yt-dlp probe.
                    resolved_ok, resolved = await self.audio_queue.resolve_playlist_entries(args)
                    if resolved_ok and isinstance(resolved, dict) and resolved.get("is_playlist"):
                        playlist_ok = True
                        playlist_info = resolved

                if playlist_ok and front:
                    await self.send_message(room.room_id, self._t(room.room_id, "playnext_takes_single_track", prefix=self._prefix))
                    return

                if not playlist_ok:
                    streamed = await self._try_stream_first_idle_play(call_room_id, args)
                    if streamed:
                        return

                if not await self._join_call_for_room(call_room_id):
                    return

                if playlist_ok and isinstance(playlist_info, dict):
                    entries = playlist_info.get("entries")
                    if not isinstance(entries, list) or not entries:
                        await self.send_message(room.room_id, self._t(room.room_id, "could_not_find_playable"))
                        return

                    playlist_name_raw = playlist_info.get("title")
                    playlist_name = playlist_name_raw if isinstance(playlist_name_raw, str) and playlist_name_raw else "playlist"

                    unique_sources: list[str] = []
                    seen_sources: set[str] = set()
                    for source in entries:
                        if not isinstance(source, str) or not source:
                            continue
                        if source in seen_sources:
                            continue
                        seen_sources.add(source)
                        unique_sources.append(source)

                    over_limit_skipped = 0
                    max_tracks = self.config.PLAYLIST_MAX_TRACKS_PER_REQUEST
                    if len(unique_sources) > max_tracks:
                        over_limit_skipped = len(unique_sources) - max_tracks
                        unique_sources = unique_sources[:max_tracks]

                    filtered_sources = [src for src in unique_sources if not self.audio_queue.has_source(src)]
                    skipped_existing = len(unique_sources) - len(filtered_sources)
                    if not filtered_sources:
                        await self.send_message(
                            room.room_id,
                            self._t(room.room_id, "all_tracks_from_are", playlist_name=playlist_name),
                        )
                        return

                    await self.send_message(room.room_id, self._t(room.room_id, "loading_playlist", playlist_name=playlist_name))

                    first_item, first_index, failures = await self._download_first_available_source(filtered_sources)

                    if first_item is None:
                        await self.send_message(room.room_id, self._t(room.room_id, "could_not_load_any", name=playlist_name))
                        return

                    async with self._playback_lock:
                        had_current = self.audio_queue.current is not None or self.call_worker.state == "playing"
                        self.audio_queue.add_to_queue(
                            first_item["file"],
                            first_item.get("title"),
                            first_item.get("duration"),
                            source_url=first_item.get("source_url"),
                            non_cacheable=bool(first_item.get("non_cacheable")),
                            artist=first_item.get("artist", ""),
                            track=first_item.get("track", ""),
                            album=first_item.get("album", ""),
                            channel=first_item.get("channel", ""),
                            uploader=first_item.get("uploader", ""),
                        )
                        if not had_current:
                            await self._advance_queue(call_room_id, force_next=True)

                    loaded_count = 1
                    remaining_sources = [src for idx, src in enumerate(filtered_sources) if idx != first_index]
                    if remaining_sources:
                        self._background_load_task = asyncio.create_task(
                            self._load_remaining_tracks(
                                call_room_id,
                                playlist_name,
                                remaining_sources,
                                dedupe_existing=True,
                                source_label="playlist",
                                info=playlist_info.get("info"),
                            )
                        )

                    total_tracks = loaded_count + len(remaining_sources)
                    r = room.room_id
                    summary = [self._t(r, "playlist_loaded", name=playlist_name, count=total_tracks)]
                    if skipped_existing:
                        summary.append(self._t(r, "playlist_skipped_queued", count=skipped_existing))
                    if over_limit_skipped:
                        summary.append(self._t(
                            r, "playlist_skipped_limit",
                            limit=self.config.PLAYLIST_MAX_TRACKS_PER_REQUEST, count=over_limit_skipped,
                        ))
                    if failures:
                        summary.append(self._t(r, "playlist_failed", count=failures))
                    await self.send_message(room.room_id, "\n".join(summary))
                    return

                if self.config.SHOW_PROGRESS_MESSAGES:
                    if not self.audio_queue.looks_like_url(args):
                        asyncio.create_task(self.send_message(room.room_id, self._t(room.room_id, "searching", args=args), priority="noisy"))
                    asyncio.create_task(self.send_message(room.room_id, self._t(room.room_id, "downloading"), priority="noisy"))

                success, result = await self.audio_queue.download_audio(args)
                if not success:
                    await self.send_message(room.room_id, self._t(room.room_id, "download_failed", result=result))
                    return

                if not isinstance(result, dict):
                    await self.send_message(room.room_id, self._t(room.room_id, "download_failed_invalid_download"))
                    return

                audio_file = result["file"]
                duration = result["duration"]
                title = result["title"]
                source_url = result.get("source_url")

                if isinstance(source_url, str) and self.audio_queue.has_source(source_url):
                    await self.send_message(
                        room.room_id,
                        self._t(room.room_id, "track_already_playing_queued", prefix=self._prefix),
                    )
                    return

                queued_message = None
                should_advance = False
                async with self._playback_lock:
                    had_current = self.audio_queue.current is not None or self.call_worker.state == "playing"
                    self.audio_queue.add_to_queue(
                        audio_file,
                        title,
                        duration,
                        source_url=source_url,
                        non_cacheable=bool(result.get("non_cacheable")),
                        artist=result.get("artist", ""),
                        track=result.get("track", ""),
                        album=result.get("album", ""),
                        channel=result.get("channel", ""),
                        uploader=result.get("uploader", ""),
                    )
                    if front and had_current:
                        self.audio_queue.queue.appendleft(self.audio_queue.queue.pop())
                    added = self.audio_queue.queue[0] if front and had_current else self.audio_queue.queue[-1]
                    position = 1 if front else len(self.audio_queue.queue)

                    if had_current:
                        eta_known = True
                        eta_seconds = 0.0

                        current_remaining = self._current_track_remaining_seconds()
                        if current_remaining is None:
                            eta_known = False
                        else:
                            eta_seconds = current_remaining

                        # Sum durations of tracks ahead of the newly queued one.
                        for queued_item in list(self.audio_queue.queue)[: position - 1]:
                            item_duration = queued_item.get("duration") if isinstance(queued_item, dict) else None
                            if eta_known and item_duration is not None:
                                eta_seconds += float(item_duration)
                            else:
                                eta_known = False

                        r = room.room_id
                        lines = [
                            self._t(r, "queued_next" if front else "queued", title=display_title(added) or title),
                            self._t(r, "queued_position", position=position),
                            self._t(r, "starts_in", eta=self._format_duration(eta_seconds if eta_known else None)),
                        ]
                        next_title = self._next_track_title()
                        if next_title:
                            lines.append(self._t(r, "up_next", title=next_title))
                        queued_message = "\n".join(lines)
                    else:
                        should_advance = True
                if queued_message:
                    await self.send_message(room.room_id, queued_message)
                if should_advance:
                    await self._advance_queue(call_room_id, force_next=True)
            return

        if command == "queue":
            if not self.audio_queue.queue and not self.audio_queue.current and not self._background_load_alive():
                await self.send_message(room.room_id, self._t(room.room_id, "queue_empty"))
                return

            lines = [self._t(room.room_id, "queue_header"), ""]
            eta_known = True
            eta_seconds = 0.0

            if self.audio_queue.current:
                loop_indicator = " " + self._t(room.room_id, "looping") if self.audio_queue.loop_mode else ""
                lines.append(self._format_track_line(self._t(room.room_id, "now_prefix") + " ", self.audio_queue.current) + loop_indicator)
                lines.append("")
                current_remaining = self._current_track_remaining_seconds()
                if current_remaining is None:
                    eta_known = False
                else:
                    eta_seconds = current_remaining

            for idx, item in enumerate(self.audio_queue.queue, 1):
                eta_label = self._t(room.room_id, "eta_in", eta=self._format_duration(eta_seconds if eta_known else None))
                lines.append(self._format_track_line(f"{idx}. ", item) + f" ({eta_label})")

                item_duration = item.get("duration")
                if eta_known and item_duration is not None:
                    eta_seconds += float(item_duration)
                else:
                    eta_known = False

            tracks_for_total = []
            if self.audio_queue.current:
                tracks_for_total.append(self.audio_queue.current)
            tracks_for_total.extend(list(self.audio_queue.queue))
            total_duration = self._sum_known_durations(tracks_for_total)
            if total_duration is not None and self.audio_queue.current:
                current_duration = self.audio_queue.current.get("duration")
                current_remaining = self._current_track_remaining_seconds()
                if current_duration is None or current_remaining is None:
                    total_duration = None
                else:
                    total_duration = max(0.0, total_duration - float(current_duration) + current_remaining)
            total_label = self._format_duration(total_duration)
            lines.append("")
            lines.append(self._t(room.room_id, "queue_total", total=total_label))

            pending = self._pending_sources
            if pending and self._background_load_alive():
                not_loaded = list(pending)
                lines.append("")
                lines.append(self._t(room.room_id, "queue_pending_header", count=len(not_loaded)))
                for source in not_loaded[:QUEUE_PENDING_SHOWN]:
                    meta = self._pending_info.get(source) or {"title": source}
                    lines.append(self._format_track_line("• ", meta))
                if len(not_loaded) > QUEUE_PENDING_SHOWN:
                    lines.append(self._t(room.room_id, "queue_pending_more", count=len(not_loaded) - QUEUE_PENDING_SHOWN))

            await self.send_message(room.room_id, "\n".join(lines))
            return

        if command == "nowplaying":
            if not self.audio_queue.current:
                await self.send_message(room.room_id, self._t(room.room_id, "nothing_currently_playing"))
                return

            line = self._format_track_line("▶️ " + self._t(room.room_id, "now_prefix") + " ", self.audio_queue.current)
            next_title = self._next_track_title()
            if next_title:
                line += "\n" + self._t(room.room_id, "up_next", title=next_title)
            await self.send_message(room.room_id, line)
            return

        if command == "diag":
            now = asyncio.get_running_loop().time()
            last_pong = self.call_worker.last_pong_ts
            pong_age = f"{(now - last_pong):.1f}s ago" if last_pong is not None else "never"
            lines = [
                "🛠️ Diagnostics",
                f"Worker state: {self.call_worker.state}",
                f"Worker running: {'yes' if self.call_worker.running else 'no'}",
                f"Room joined: {self.call_worker.room_id or 'none'}",
                f"Desired room: {self.call_worker.desired_room_id or 'none'}",
                f"Heartbeat pong: {pong_age}",
                f"Restart attempts: {self.call_worker.restart_attempts}/{self.call_worker.max_restart_attempts}",
                (
                    "Stop timeouts: "
                    f"{self.call_worker.consecutive_stop_timeouts}/{self.call_worker.stop_timeout_restart_threshold}"
                ),
                f"Queue size: {len(self.audio_queue.queue)}",
                "Startup checks: " + ("ok" if not self._startup_warnings else f"{len(self._startup_warnings)} warning(s)"),
            ]
            if self._tool_versions:
                tools = ", ".join(f"{k}={v}" for k, v in sorted(self._tool_versions.items()))
                lines.append(f"Tool versions: {tools}")
            if self._startup_warnings:
                lines.extend(f"- {w}" for w in self._startup_warnings[:3])
            await self.send_message(room.room_id, "\n".join(lines))
            return

        if command == "audio":
            r = room.room_id
            onoff = lambda value: self._t(r, "on" if value else "off")  # noqa: E731
            await self.send_message(r, self._t(
                r, "audio_settings",
                normalize=onoff(self.config.NORMALIZE_AUDIO),
                fade_in_ms=self.config.FADE_IN_MS,
                volume=self.config.VOLUME_PERCENT,
                cache_mode=self.config.AUDIO_CACHE_MODE,
                cache_max=self._format_bytes(self.config.AUDIO_CACHE_MAX_BYTES),
                delete_after=onoff(self.config.AUDIO_CACHE_DELETE_AFTER_PLAYBACK),
                delete_on_shutdown=onoff(self.config.AUDIO_CACHE_DELETE_ON_SHUTDOWN),
                download_format=self.config.AUDIO_DOWNLOAD_FORMAT,
                stream_first=onoff(self.config.STREAM_FIRST_IDLE),
                stream_prefetch=onoff(self.config.STREAM_PREFETCH_CURRENT),
            ))
            return

        if command == "normalize":
            value = args.strip().lower()
            if value not in {"on", "off"}:
                await self.send_message(room.room_id, self._t(room.room_id, "usage_normalize_on_off", prefix=self._prefix))
                return
            self.config.NORMALIZE_AUDIO = value == "on"
            await self._apply_audio_settings_to_worker()
            await self.send_message(room.room_id, self._t(
                room.room_id, "normalize_set", state=self._t(room.room_id, "on" if self.config.NORMALIZE_AUDIO else "off")
            ))
            return

        if command == "fadein":
            raw = args.strip()
            if not raw:
                await self.send_message(room.room_id, self._t(room.room_id, "usage_fadein_milliseconds", prefix=self._prefix))
                return
            ms = self._parse_int_arg(raw)
            if ms is None:
                await self.send_message(room.room_id, self._t(room.room_id, "fade_must_integer_number"))
                return
            if ms < 0 or ms > 5000:
                await self.send_message(room.room_id, self._t(room.room_id, "fade_must_between_ms"))
                return
            self.config.FADE_IN_MS = ms
            await self._apply_audio_settings_to_worker()
            await self.send_message(room.room_id, self._t(room.room_id, "fade_set_ms", fade_in_ms=self.config.FADE_IN_MS))
            return

        if command == "volume":
            raw = args.strip()
            if not raw:
                await self.send_message(room.room_id, self._t(room.room_id, "usage_volume", prefix=self._prefix))
                return
            value = self._parse_int_arg(raw)
            if value is None:
                await self.send_message(room.room_id, self._t(room.room_id, "volume_must_integer_between"))
                return
            if value < 0 or value > 200:
                await self.send_message(room.room_id, self._t(room.room_id, "volume_must_between"))
                return
            self.config.VOLUME_PERCENT = value
            await self._apply_audio_settings_to_worker()
            if self.call_worker.running:
                await self.send_message(room.room_id, self._t(room.room_id, "volume_set_applies_immediately", volume_percent=self.config.VOLUME_PERCENT))
            else:
                await self.send_message(room.room_id, self._t(room.room_id, "volume_set", volume_percent=self.config.VOLUME_PERCENT))
            return

        if command == "history":
            history = list(self._play_history_by_room.get(room.room_id, []))
            if not history:
                await self.send_message(room.room_id, self._t(room.room_id, "no_playback_history_yet"))
                return

            lines = [self._t(room.room_id, "history_header")]
            for idx, item in enumerate(history, 1):
                title = item.get("title", "?")
                duration = self._format_duration(item.get("duration"))
                at_ts = item.get("at")
                at_label = time.strftime("%H:%M:%S", time.localtime(at_ts)) if isinstance(at_ts, int) else "?"
                lines.append(self._t(room.room_id, "history_line", index=idx, title=title, duration=duration, time=at_label))
            await self.send_message(room.room_id, "\n".join(lines))
            return

        if command == "save":
            tokens = args.strip().split()
            force = False
            if "--force" in tokens:
                force = True
                tokens = [t for t in tokens if t != "--force"]
            name = " ".join(tokens).strip()
            if not name:
                await self.send_message(room.room_id, self._t(room.room_id, "usage_save_name_force", prefix=self._prefix))
                return

            if self.saved_queues.has_queue(room.room_id, name) and not force:
                await self.send_message(
                    room.room_id,
                    self._t(room.room_id, "saved_queue_already_exists", name=name, prefix=self._prefix),
                )
                return

            snapshot = []
            skipped = 0

            if self.audio_queue.current:
                source_url = self.audio_queue.current.get("source_url")
                if source_url:
                    snapshot.append(
                        {
                            "source_url": source_url,
                            "title": self.audio_queue.current.get("title"),
                            "duration": self.audio_queue.current.get("duration"),
                        }
                    )
                else:
                    skipped += 1

            for item in self.audio_queue.queue:
                source_url = item.get("source_url")
                if source_url:
                    snapshot.append(
                        {
                            "source_url": source_url,
                            "title": item.get("title"),
                            "duration": item.get("duration"),
                        }
                    )
                else:
                    skipped += 1

            if not snapshot:
                await self.send_message(room.room_id, self._t(room.room_id, "nothing_saveable_queue_right"))
                return

            self.saved_queues.save_queue(room.room_id, name, snapshot)
            msg = self._t(room.room_id, "queue_saved", name=name, count=len(snapshot))
            if skipped:
                msg += "\n" + self._t(room.room_id, "queue_saved_skipped", count=skipped)
            await self.send_message(room.room_id, msg)
            return

        if command == "queues":
            names = self.saved_queues.list_names(room.room_id)
            if not names:
                await self.send_message(room.room_id, self._t(room.room_id, "no_saved_queues_room"))
                return

            entries = []
            for name in names:
                tracks = self.saved_queues.load_queue(room.room_id, name) or []
                total = self._sum_known_durations(tracks)
                entries.append(self._t(
                    room.room_id, "saved_queue_line", name=name, count=len(tracks), total=self._format_duration(total)
                ))

            await self.send_message(room.room_id, self._t(room.room_id, "saved_queues_header") + "\n" + "\n".join(entries))
            return

        if command == "deletequeue":
            name = args.strip()
            if not name:
                await self.send_message(room.room_id, self._t(room.room_id, "usage_deletequeue_name", prefix=self._prefix))
                return
            deleted = self.saved_queues.delete_queue(room.room_id, name)
            if not deleted:
                await self.send_message(room.room_id, self._t(room.room_id, "saved_queue_not_found", name=name))
                return
            await self.send_message(room.room_id, self._t(room.room_id, "deleted_saved_queue", name=name))
            return

        if command == "renamequeue":
            parts = args.strip().split(maxsplit=1)
            if len(parts) != 2:
                await self.send_message(room.room_id, self._t(room.room_id, "usage_renamequeue_old_new", prefix=self._prefix))
                return

            old_name, new_name = parts[0].strip(), parts[1].strip()
            if not old_name or not new_name:
                await self.send_message(room.room_id, self._t(room.room_id, "usage_renamequeue_old_new", prefix=self._prefix))
                return

            ok, reason = self.saved_queues.rename_queue(room.room_id, old_name, new_name)
            if not ok:
                if reason == "missing_old":
                    await self.send_message(room.room_id, self._t(room.room_id, "saved_queue_not_found", name=old_name))
                    return
                if reason == "new_exists":
                    await self.send_message(room.room_id, self._t(room.room_id, "saved_queue_named_already", new_name=new_name))
                    return
                await self.send_message(room.room_id, self._t(room.room_id, "failed_rename_saved_queue"))
                return

            await self.send_message(room.room_id, self._t(room.room_id, "renamed", old_name=old_name, new_name=new_name))
            return

        if command == "load":
            name = args.strip()
            if not name:
                await self.send_message(room.room_id, self._t(room.room_id, "usage_load_name", prefix=self._prefix))
                return

            tracks = self.saved_queues.load_queue(room.room_id, name)
            if tracks is None:
                await self.send_message(room.room_id, self._t(room.room_id, "saved_queue_not_found", name=name))
                return

            if not tracks:
                await self.send_message(room.room_id, self._t(room.room_id, "saved_queue_empty", name=name))
                return

            if not await self._join_call_for_room(room.room_id):
                return

            self._cancel_background_load()
            await self.send_message(room.room_id, self._t(room.room_id, "loading_saved_queue", name=name))

            sources: list[str] = []
            failures = 0

            for track in tracks:
                source_url = track.get("source_url") if isinstance(track, dict) else None
                if not source_url:
                    failures += 1
                    continue
                sources.append(source_url)

            first_item, first_index, download_failures = await self._download_first_available_source(sources)
            failures += download_failures

            if first_item is None:
                await self.send_message(room.room_id, self._t(room.room_id, "could_not_load_any", name=name))
                return

            async with self._playback_lock:
                # The track being replaced was stopped, not played to the end.
                await self._emit_track_finished(self._active_call_room_id(room.room_id), "stopped")
                prior_tracks = []
                if self.audio_queue.current:
                    prior_tracks.append(self.audio_queue.current)
                prior_tracks.extend(list(self.audio_queue.queue))
                self._cancel_auto_advance()
                self._cancel_worker_playback_wait()
                await self.call_worker.stop_playback(wait_for_terminal=True)
                self.audio_queue.clear_queue()
                self.audio_queue.current = None
                for track in prior_tracks:
                    self.audio_queue.maybe_delete_track_file(track, trigger="after_playback")

                self.audio_queue.add_to_queue(
                    first_item["file"],
                    first_item.get("title"),
                    first_item.get("duration"),
                    source_url=first_item.get("source_url"),
                    non_cacheable=bool(first_item.get("non_cacheable")),
                    artist=first_item.get("artist", ""),
                    track=first_item.get("track", ""),
                    album=first_item.get("album", ""),
                    channel=first_item.get("channel", ""),
                    uploader=first_item.get("uploader", ""),
                )

                await self._advance_queue(room.room_id, force_next=True)

            remaining_sources = [src for idx, src in enumerate(sources) if idx != first_index]
            if remaining_sources:
                self._background_load_task = asyncio.create_task(
                    self._load_remaining_tracks(
                        room.room_id,
                        name,
                        remaining_sources,
                        dedupe_existing=False,
                        source_label="saved queue",
                    )
                )
                await self.send_message(room.room_id, self._t(room.room_id, "more_tracks_queued", count=len(remaining_sources)))
            elif failures:
                await self.send_message(room.room_id, self._t(room.room_id, "failed_count", failures=failures))
            return

        if command == "skip":
            if not await self._require_joined_in_room_call(room.room_id, f"{self._prefix} skip"):
                return

            now = time.monotonic()
            if now - self._last_skip_at < self._skip_cooldown_seconds:
                await self.send_message(room.room_id, self._t(room.room_id, "skip_already_progress"))
                return

            queue_empty = False
            async with self._playback_lock:
                if not self.audio_queue.current and not self.audio_queue.queue:
                    queue_empty = True
                    still_loading = self._background_load_alive()
                else:
                    self._last_skip_at = now
                    asyncio.create_task(self.send_message(room.room_id, self._t(room.room_id, "skipping"), priority="noisy"))
                    _call_room = self.call_worker.room_id or room.room_id
                    await self._emit_track_finished(_call_room, "skipped")
                    self._cancel_worker_playback_wait()
                    await self.call_worker.stop_playback(wait_for_terminal=False)
                    await self._advance_queue(room.room_id, force_next=True, pre_stop=False)
            if queue_empty:
                # Skipped again while the next playlist track is still loading.
                key = "loading_next_track" if still_loading else "queue_empty"
                await self.send_message(room.room_id, self._t(room.room_id, key))
            return

        if command in ("pause", "resume"):
            if not await self._require_joined_in_room_call(room.room_id, f"{self._prefix} {command}"):
                return
            loop = asyncio.get_running_loop()
            async with self._playback_lock:
                paused = self._paused_at is not None
                if command == "pause" and not paused:
                    if not self.audio_queue.current or self._current_track_started_at is None:
                        reply = self._t(room.room_id, "nothing_playing")
                    else:
                        self._cancel_auto_advance()
                        self._paused_at = loop.time()
                        self._paused_wall = time.time()
                        await self.call_worker.pause_playback()
                        reply = self._t(room.room_id, "paused", prefix=self._prefix)
                elif not paused:
                    reply = self._t(room.room_id, "not_paused")
                else:
                    # Shift the start times by the pause length so ETA, auto-advance
                    # and the scrobbler's played time ignore the pause
                    self._current_track_started_at += loop.time() - self._paused_at
                    if self._active_play and self._paused_wall is not None:
                        self._active_play["started_at"] += time.time() - self._paused_wall
                    self._paused_at = None
                    self._paused_wall = None
                    await self.call_worker.resume_playback()
                    current = self.audio_queue.current
                    duration = current.get("duration") if current else None
                    if duration is not None:
                        elapsed = loop.time() - self._current_track_started_at
                        self._arm_auto_advance(
                            max(0.0, float(duration) - elapsed),
                            self._current_room_id or room.room_id,
                            expected_source=self._track_source_ref(current),
                        )
                    reply = self._t(room.room_id, "resumed")
            await self.send_message(room.room_id, reply)
            return

        if command == "shuffle":
            async with self._playback_lock:
                pending = self._pending_sources
                loader_alive = self._background_load_task is not None and not self._background_load_task.done()
                if pending is not None and loader_alive:
                    pool = list(pending)
                    keep = []
                    for item in self.audio_queue.queue:
                        src = item.get("source_url")
                        if src:
                            pool.append(src)
                        else:
                            keep.append(item)
                    random.shuffle(pool)
                    self.audio_queue.queue.clear()
                    self.audio_queue.queue.extend(keep)
                    pending.clear()
                    pending.extend(pool)
                    total = len(pool) + len(keep)
                else:
                    queued = list(self.audio_queue.queue)
                    random.shuffle(queued)
                    self.audio_queue.queue.clear()
                    self.audio_queue.queue.extend(queued)
                    total = len(queued)
            if total < 2:
                await self.send_message(room.room_id, self._t(room.room_id, "nothing_shuffle_fewer_than"))
            else:
                await self.send_message(room.room_id, self._t(room.room_id, "shuffled_upcoming_tracks", count=total))
            return

        if command == "loop":
            if not await self._require_joined_in_room_call(room.room_id, f"{self._prefix} loop"):
                return

            loop_status = self.audio_queue.toggle_loop()
            if loop_status:
                self._cancel_auto_advance()
                await self.send_message(
                    room.room_id,
                    self._t(room.room_id, "loop_on"),
                )
            else:
                await self.send_message(room.room_id, self._t(room.room_id, "loop_off"))
                current = self.audio_queue.current
                if current and current.get("duration") is not None and self._current_room_id:
                    self._arm_auto_advance(
                        current["duration"],
                        self._current_room_id,
                        expected_generation=self._playback_generation,
                        expected_source=self._track_source_ref(current),
                    )
            return

        if command == "stop":
            if not await self._require_joined_in_room_call(room.room_id, f"{self._prefix} stop"):
                return

            self._cancel_auto_advance()
            self._cancel_worker_playback_wait()
            self._cancel_background_load()
            self._cancel_stream_prefetch()
            # Under the lock: otherwise a track change in progress (pre-stop wait,
            # file fallback download) plays the next track after "Stopped".
            async with self._playback_lock:
                self._cancel_auto_advance()
                self._cancel_worker_playback_wait()
                _call_room = self.call_worker.room_id or room.room_id
                await self._emit_track_finished(_call_room, "stopped")
                tracks_to_cleanup = []
                if self.audio_queue.current:
                    tracks_to_cleanup.append(self.audio_queue.current)
                tracks_to_cleanup.extend(list(self.audio_queue.queue))
                self.audio_queue.clear_queue()
                self.audio_queue.current = None
                self._current_room_id = None
                self._current_track_started_at = None
                self._paused_at = None
                self._paused_wall = None
                for track in tracks_to_cleanup:
                    self.audio_queue.maybe_delete_track_file(track, trigger="after_playback")
                await self.call_worker.stop_playback(wait_for_terminal=True)
            await self.send_message(room.room_id, self._t(room.room_id, "stopped_cleared_queue"))
            return

        if command == "status":
            r = room.room_id
            onoff = lambda value: self._t(r, "on" if value else "off")  # noqa: E731
            timer_active = bool(self._auto_advance_task and not self._auto_advance_task.done())
            lines = [
                self._t(r, "status_online"),
                self._t(r, "status_worker", state=self.call_worker.state),
                self._t(r, "status_queue", count=len(self.audio_queue.queue)),
                self._t(r, "status_loop", state=onoff(self.audio_queue.loop_mode)),
                self._t(r, "status_timer", state=self._t(r, "active" if timer_active else "inactive")),
                self._t(r, "status_preroll", seconds=self.audio_queue.preroll_silence),
                self._t(r, "status_normalize", state=onoff(self.config.NORMALIZE_AUDIO)),
                self._t(r, "status_fade_in", fade_in_ms=self.config.FADE_IN_MS),
                self._t(r, "status_volume", volume=self.config.VOLUME_PERCENT),
                self._t(r, "status_language", language=self._language_label(self._room_language(r))),
            ]
            if self.call_worker.running and self.call_worker.room_id:
                lines.insert(2, self._t(r, "status_room", room=self.call_worker.room_id))
            if self.audio_queue.current:
                lines.insert(2, self._t(r, "status_current", title=self.audio_queue.current['title']))
                next_title = self._next_track_title()
                if next_title:
                    lines.insert(3, self._t(r, "up_next", title=next_title))
            await self.send_message(room.room_id, "\n".join(lines))
            return

        return

    async def send_message(
        self,
        room_id: str,
        text: str,
        *,
        html_body: Optional[str] = None,
        priority: str = "normal",
    ):
        try:
            resolved_priority = priority if priority in self._message_priority_map else "normal"
            if priority == "normal":
                if text.startswith("❌") or text.startswith("⚠️"):
                    resolved_priority = "critical"
            await self._queue_message(room_id, text, html_body=html_body, priority=resolved_priority)
        except Exception as exc:
            logger.error(f"Error sending message: {exc}")

    async def on_message(self, room: MatrixRoom, event: RoomMessageText):
        if event.sender == self.config.MATRIX_USER_ID or not self.first_sync_done:
            return

        if event.body.strip().startswith(self._prefix):
            # Own task: a `play` that downloads for a while must not hold up the sync
            # loop (and with it `skip`/`stop`), and an exception must not end it.
            task = asyncio.create_task(self._run_command(room, event.body))
            self._command_tasks.add(task)
            task.add_done_callback(self._command_tasks.discard)

    async def _run_command(self, room: MatrixRoom, body: str):
        try:
            await self.handle_command(room, body)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("Command failed: %r", body[:200])
            await self.send_message(room.room_id, self._t(room.room_id, "command_failed", exc=exc))

    async def start(self):
        logger.info("=" * 60)
        logger.info("Music Bot Core Starting")
        logger.info(f"Auto-advance buffer: {self.config.AUTO_ADVANCE_BUFFER}s")
        logger.info(f"Pre-roll silence: {self.config.PREROLL_SILENCE}s")
        logger.info(f"Normalize audio: {self.config.NORMALIZE_AUDIO}")
        logger.info(f"Fade-in: {self.config.FADE_IN_MS}ms")
        logger.info(f"Volume: {self.config.VOLUME_PERCENT}%")
        logger.info("Voice backend: call worker enabled")
        self._run_startup_checks()
        logger.info("=" * 60)

        device_id = await self._restore_e2ee_login()
        await self.client.sync(timeout=30000, full_state=True)
        self.first_sync_done = True
        await self._seed_call_participants_from_state()
        if device_id:
            await self._finish_e2ee_setup(device_id)
        self._start_message_dispatcher()
        self._emit_task = asyncio.create_task(self._emit_loop())
        self._ensure_advance_watchdog()
        logger.info("Bot ready")

        try:
            backoff = 5
            while True:
                started = time.monotonic()
                try:
                    # No sleep between syncs: the long poll (timeout) already waits for
                    # events. A 5 s loop_sleep_time used to delay every command by up to 5 s.
                    await self.client.sync_forever(timeout=30000, full_state=False)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    # sync_forever never returns normally: reset the backoff after a
                    # healthy stretch, or occasional crashes add up to 5-minute gaps.
                    if time.monotonic() - started > 300:
                        backoff = 5
                    logger.warning("sync_forever crashed (%s), retrying in %ds", exc, backoff)
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 300)
        finally:
            # Nothing may start a track (or a worker) while we shut down.
            self.call_worker.begin_shutdown()
            self._cancel_auto_advance()
            self._cancel_advance_watchdog()
            self._cancel_worker_playback_wait()
            self._cancel_background_load()
            self._cancel_stream_prefetch()
            if self._active_play is not None:
                # The track playing now ends here: close it for the scrobbler.
                await self._emit_track_finished(self._active_play["room_id"], "stopped")
            if self._emit_task:
                # Send what's still queued (e.g. the track_finished just above).
                try:
                    await asyncio.wait_for(self._emit_queue.join(), timeout=10)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    logger.warning("Shutdown: %d scrobbler event(s) not sent", self._emit_queue.qsize())
                self._emit_task.cancel()
            self.audio_queue.cleanup_on_shutdown()
            await self._stop_message_dispatcher()
            await self.call_worker.stop()
            await self.client.close()

    def _run_startup_checks(self):
        warnings: list[str] = []
        self._tool_versions = self._collect_tool_versions()

        if not self.call_worker.available:
            warnings.append("Call worker script not found (call features disabled)")

        if not shutil.which("ffmpeg"):
            warnings.append("ffmpeg not found in PATH")

        if not shutil.which("yt-dlp") and not shutil.which("youtube-dlp"):
            warnings.append("yt-dlp/youtube-dlp not found in PATH")

        try:
            self.audio_queue.audio_dir.mkdir(parents=True, exist_ok=True)
            probe = self.audio_queue.audio_dir / ".write_test"
            probe.write_text("ok", encoding="utf-8")
            probe.unlink(missing_ok=True)
        except Exception as exc:
            warnings.append(f"Audio directory is not writable ({self.audio_queue.audio_dir}): {exc}")

        if warnings:
            for warning in warnings:
                logger.warning("Startup check: %s", warning)
        else:
            logger.info("Startup checks passed")
        self._startup_warnings = warnings

    def _collect_tool_versions(self) -> dict[str, str]:
        result: dict[str, str] = {}

        def capture(name: str, cmd: list[str]):
            try:
                output = subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True, timeout=3).strip()
            except Exception:
                return
            if not output:
                return
            first = output.splitlines()[0].strip()
            if first:
                result[name] = first

        capture("python", ["python", "--version"])
        capture("node", ["node", "--version"])
        capture("ffmpeg", ["ffmpeg", "-version"])
        if shutil.which("yt-dlp"):
            capture("yt-dlp", ["yt-dlp", "--version"])
        elif shutil.which("youtube-dlp"):
            capture("youtube-dlp", ["youtube-dlp", "--version"])

        return result

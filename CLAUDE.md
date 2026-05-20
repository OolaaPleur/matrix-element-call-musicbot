# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

A Matrix music bot with Discord-style chat commands (`!m play`, `!m skip`, `!m queue`, etc.) that joins Element Call and streams audio directly into the call via LiveKit. Two-component architecture: a Python bot process and a Node.js call worker subprocess.

**Command prefix**: default `!m`, configurable via `bot.command_prefix` in `config.toml`. Format is `<prefix> <command> [args]` (e.g. `!m play some song`). The prefix was made configurable so the scrobbler bot's `!fm` commands are ignored cleanly.

## Running

**Direct (development):**
```bash
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
npm ci --prefix call_worker
cp config/config.example.toml config/config.toml
# The active config is config.toml at the ROOT of the project, NOT config/config.toml.
# Always edit the root-level config.toml — config/config.toml is just the example template.
python3 main.py
```

**Docker:**
```bash
docker compose up -d --build
docker compose logs -f musicbot
```

Requirements: Python 3.11+, Node.js 22+, ffmpeg, yt-dlp (all in PATH). No test suite exists.

**yt-dlp JS runtime**: yt-dlp 2026.03+ requires a JavaScript runtime for YouTube format extraction. Node.js is already installed for the call worker, so `~/.config/yt-dlp/config` contains `--js-runtimes node` to wire this up system-wide.

## Architecture

### Python bot (`bot.py` — `IntegratedBot`)
- Connects to Matrix via `matrix-nio` (`AsyncClient`), handles E2EE with `SqliteStore`
- Listens for messages starting with the configured prefix (`!m` by default) and dispatches to handlers in `_handle_command_internal`; command keys are bare words (`"play"`, `"skip"`, etc.) and aliases map short forms (`"p"` → `"play"`)
- Manages an in-memory `AudioQueue` (current track + deque of upcoming tracks)
- Drives the Node.js call worker via `CallWorkerProcess`
- Uses a priority `asyncio.PriorityQueue` for outbound messages: `critical=0`, `normal=1`, `noisy=2`
- `QUIET_MODE=true` (default) suppresses `noisy` messages; `critical` always sends
- Emits `dev.elementcall.musicbot.track_started` / `dev.elementcall.musicbot.track_finished` custom room events for the companion scrobbler bot; `_active_play` tracks the in-flight play state; `_emit_track_finished` has a double-emit guard

### Node.js call worker (`call_worker/src/join_call.js`)
- Spawned as a subprocess per `!join` call; stdin/stdout carry newline-delimited JSON commands/events
- Joins Matrix room, joins MatrixRTC session via `matrix-js-sdk`, connects to LiveKit SFU
- Pipes audio through `ffmpeg` (spawned as a child process) → `AudioFrame` → LiveKit `AudioSource`
- Supports `play` (file path or stream URL), `stop`, `set_audio`, `ping/pong`, `shutdown`
- Emits events: `joined`, `play_started`, `play_ended`, `play_stopped`, `pong`, `error`, `left`
- Audio at 48 kHz mono PCM s16le; FRAME_MS=20ms; volume ramping per-sample to avoid clicks

### Python↔Node bridge (`call_worker_process.py` — `CallWorkerProcess`)
- Spawns and monitors the Node.js subprocess
- Reads events from stdout, pumps stderr to Python logger
- Exposes `play()`, `play_stream()`, `stop_playback()`, `set_audio_settings()`, `stop()`
- Auto-restarts on unexpected crash (up to `max_restart_attempts`, exponential backoff)
- Heartbeat ping/pong every `heartbeat_interval` seconds; kills worker on timeout

### Audio pipeline (`audio_queue.py`)
- Downloads audio via `yt-dlp` (supports URLs and search queries)
- Cache modes: `size_lru` (LRU eviction at `cache_max_bytes`, min 200 MB), `never_delete`, `always_delete`
- Stream-first idle: when queue is empty, resolves a stream URL and plays immediately without downloading
- Stream prefetch: after stream-first play, downloads the file in background for loop support
- Result dicts include `artist`, `track`, `album`, `channel` from yt-dlp (empty string when absent); consumed by `extract_metadata()` in `bot.py` for scrobbler events
- `normalize_media_url()` prepends `https://` when the user omits the scheme (e.g. `youtube.com/watch?v=...` → `https://youtube.com/watch?v=...`), so bare-domain URLs are treated as URLs not search queries

### E2EE + Cross-signing
- Matrix room E2EE (chat messages, commands) is **fully supported** via `matrix-nio`. Call audio (LiveKit) is **not** SFrame-encrypted — see the call worker section for why.
- E2EE via `matrix-nio`'s `SqliteStore` in `data/crypto_store/`
- On startup, `cross_signing.py:ensure_cross_signing()` uploads/re-uploads master/self-signing/user-signing Ed25519 keypairs
- Private keys stored in `data/cross_signing_keys.json` (chmod 600, gitignored)
- Megolm retry: undecryptable events are retried at 3/8/20/60s intervals after requesting room keys
- Device verification: use `/verify <device_id> <ed25519_fingerprint>` in Element as `@youruser:matrix.org`
  - `device_id`: `cat data/device_id`
  - fingerprint: query the server — `curl -s "https://<homeserver>/_matrix/client/v3/keys/query" -H "Authorization: Bearer <token>" -H "Content-Type: application/json" -d '{"device_keys":{"@yourbotname:matrix.org":["<device_id>"]}}' | python3 -m json.tool`
  - Or use `python scripts/show_cross_signing_fingerprint.py` for the cross-signing key

### Configuration (`config.py` — `Config`)
- Reads from `config.toml` (or path from `CONFIG_FILE` env var) plus env variable overrides
- Every config key has a matching env variable (e.g., `NORMALIZE_AUDIO`, `WORKER_MEMBERSHIP_MODE`)
- Also reads `.env` files (key=value) if present in project root
- See `config/config.example.toml` for all options with comments

## Key Behavioral Details

**Playback generation counter** (`_playback_generation`): incremented each time a new track starts; used to discard stale auto-advance timers and worker terminal events from previous tracks.

**Auto-advance**: timer set to `duration + auto_advance_buffer` seconds. Also has a watchdog task that force-advances if the timer fires but the next track hasn't started within `duration + buffer + 6s`.

**Membership mode** (`worker.membership_mode`):
- `matrix2_auto` (default): tries MatrixRTC sticky events, falls back to legacy automatically
- `matrix2`: sticky events only, fails hard if unsupported
- `legacy`: always uses `/sfu/get` endpoint; required for some homeservers; may need PL50

**Saved queues** (`saved_queues.py`): persisted to `data/saved_queues.json`; stored as `{room_id: {name: [{source_url, title, duration}]}}`. Only tracks with a `source_url` can be saved.

## Data Files (gitignored)
- `data/cross_signing_keys.json` — bot's cross-signing private keys (chmod 600)
- `data/crypto_store/` — matrix-nio E2EE SQLite stores (one per device_id)
- `data/device_id` — persisted Matrix device ID
- `data/saved_queues.json` — saved queue presets
- `logs/` — rotating log files; `musicbot.clean.log` filters MatrixRTC noise
- `call_worker/call_worker.log` — Node.js worker log

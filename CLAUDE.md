# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

A Matrix music bot with Discord-style chat commands (`!m play`, `!m skip`, `!m queue`, etc.) that joins Element Call and streams audio directly into the call via LiveKit. Two-component architecture: a Python bot process and a Node.js call worker subprocess.

**Command prefix**: default `!m`, configurable via `bot.command_prefix` in `config.toml`. Format is `<prefix> <command> [args]` (e.g. `!m play some song`). The prefix was made configurable so the scrobbler bot's `!fm` commands are ignored cleanly.

## Running

**Direct (development):**
```bash
python3 -m venv venv && source venv/bin/activate
# ../matrix-element-call-common must be cloned as a sibling directory first
pip install -r requirements.txt
bun install --cwd call_worker --frozen-lockfile
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
- Each command runs in its own task (`_run_command`, since 2026-11-25): a long `play` download no longer holds up the nio sync loop (so `skip`/`stop` get through), and an exception replies "❌ Command failed" instead of crashing `sync_forever`. `play` and `load` are serialized by `_play_request_lock`; queue changes take `_playback_lock`
- E2EE start-up: `_restore_e2ee_login()` (whoami retried 5×, then the stored `data/device_id`; `restore_login`, key upload if needed) runs **before** the first sync so it can decrypt; `_finish_e2ee_setup()` (keys_query, cross-signing, room keys) after it
- Volume is logarithmic since 2026-11-25 (`volumePercentToGain` in `join_call.js`: gain = (p/100)² up to 100%, linear above, 200% = +6 dB); live `volume_percent` raised 80 → 90 to sound the same.
- Call audio: `[worker] stereo` (2 channels, ffmpeg `-ac 2`) and `audio_bitrate_kbps` (Opus `maxBitrate` via `TrackPublishOptions({audioEncoding})`; 0 = LiveKit default); DTX is always off (it thins out quiet passages). Live config: stereo, 128 kbps since 2026-11-25.
- Chat messages are translated (2026-11-25): `i18n.py` + `locales/en.toml` (complete reference) / `locales/ru.toml`; custom or overriding files in `locales/custom/<code>.toml` (ships `en.toml`/`ru.toml` with every key commented out, for editing in place; regenerate them by commenting out `locales/*.toml` when keys are added). Not in `data/`: that would need a `.gitignore` exception next to secrets. Code calls `self._t(room_id, key, **values)`; language = `data/room_languages.json` (set by `lang <code>|default`) or `[bot] language`. Plural tables `{one, few, many|other}` keyed by `count`; broken/missing translations fall back to English with a warning. `config`/`defaults`/`diag` stay English (technical). Durations are clock format (`3:05`). Keep the leading ❌/⚠️ in translations: `send_message` derives urgency from it.
- Commands added 2026-11-25: `search` (top 5, one flat yt-dlp run; `play <n>`/`playnext <n>` pick one for 5 min, per room), `playnext` (single track to the queue front), `remove <n>`.
- Manages an in-memory `AudioQueue` (current track + deque of upcoming tracks)
- Drives the Node.js call worker via `CallWorkerProcess`
- Uses a priority `asyncio.PriorityQueue` for outbound messages: `critical=0`, `normal=1`, `noisy=2`
- `QUIET_MODE=true` (default) suppresses `noisy` messages; `critical` always sends
- Emits `dev.elementcall.musicbot.track_started` / `dev.elementcall.musicbot.track_finished` custom room events for the companion scrobbler bot; `_active_play` tracks the in-flight play state (`_start_play_tracking`); `_emit_track_finished` has a double-emit guard and computes played time without pauses (`_played_seconds`)
- Those events go through `_emit_queue` → one `_emit_loop` task (in order, 120 s timeout, `ignore_unverified_devices=True`). Before 2026-11-25 they were sent inline under `_playback_lock` with `ignore_unverified_devices=False`: nio retries 429/timeouts forever, so a slow send stalled playback (2026-11-15 16:51, ~4 min, rescued by the advance watchdog), and a room-key re-share to a new device (nio counts only nio-verified devices, cross-signing doesn't count) raised and dropped the event

### Node.js call worker (`call_worker/src/join_call.js`)
- Spawned as a subprocess per `!join` call; stdin/stdout carry newline-delimited JSON commands/events
- Joins Matrix room, joins MatrixRTC session via `matrix-js-sdk`, connects to LiveKit SFU
- Pipes audio through `ffmpeg` (spawned as a child process) → `AudioFrame` → LiveKit `AudioSource`
- Supports `play` (file path or stream URL), `stop`, `set_audio`, `ping/pong`, `shutdown`
- Emits events: `joined`, `play_started`, `play_ended`, `play_stopped`, `play_failed` (ffmpeg/playback error of the current track), `pong`, `error` (anything else), `left`. Only `play_failed` makes the bot drop or retry the track
- Audio at 48 kHz mono PCM s16le; FRAME_MS=20ms; volume ramping per-sample to avoid clicks

### Python↔Node bridge (`call_worker_process.py` — `CallWorkerProcess`)
- Spawns and monitors the Node.js subprocess
- Reads events from stdout, pumps stderr to Python logger: only lines with error/fail/warn/timeout/… at WARNING, the rest (heartbeats, progress) at DEBUG — all of it is in `call_worker/call_worker.log` anyway
- Exposes `play()`, `play_stream()`, `stop_playback()`, `set_audio_settings()`, `stop()`
- Auto-restarts on unexpected crash (up to `max_restart_attempts`, exponential backoff)
- Heartbeat ping/pong every `heartbeat_interval` seconds; kills worker on timeout
- Failed startup (2026-11-14, after a matrix.org `livekit-jwt` 503): `start()` stops the spawned worker if `joined` never arrives, and `join_call.js`'s fatal handler leaves the MatrixRTC session and calls `process.exit(1)`. Before, `exitCode = 1` alone left the worker alive (the Matrix client keeps the event loop busy), in the call but without LiveKit; `running` stayed true, so every later `play` went to that dead worker and nothing played.

### Audio pipeline (`audio_queue.py`)
- Downloads audio via `yt-dlp` (supports URLs and search queries)
- Cache modes: `size_lru` (LRU eviction at `cache_max_bytes`, min 200 MB), `never_delete`, `always_delete`
- Stream-first idle: when queue is empty, resolves a stream URL and plays immediately without downloading. One `yt-dlp --dump-single-json -f bestaudio/best` gives both metadata and the stream URL (was two parallel yt-dlp runs); the metadata is kept in `_info_cache` by source URL, so the background download doesn't extract again
- Cache files are named `<md5(source_url)[:8]>.<format>`; after a restart an existing file is reused (`Using cached file from disk`) instead of downloaded again (only with `preroll_silence = 0`)
- yt-dlp runs per `download_audio()`: a URL never seen and not on disk = **one** run that downloads and prints the metadata (`--dump-json --no-simulate`, log "metadata in the same run"); a search query = metadata + download (the metadata says whether the file is cached); a URL on disk after a restart = metadata only. Playlists and saved queues load one track ahead, so this is one yt-dlp run per track as playback reaches it
- `normalize_media_url()` gives one spelling per YouTube video (`youtu.be/…`, `/shorts|live|embed/ID`, `m.`/`music.` hosts → `https://www.youtube.com/watch?v=…`, only `v`/`list`/`index` kept, so share links with `?si=` hit the cache); other spellings are remembered as aliases in `_info_cache`. The combined download+metadata yt-dlp run is used only for such plain video URLs (`is_single_video_url`); other URLs get a metadata run first, since their cache file name isn't known upfront. Pre-roll is written to `<name>.preroll.<ext>` and `os.replace`d. Downloads are locked per query and, once resolved, per `source_url` (search query + its URL can't write the same temp file); a cancelled yt-dlp is killed (`_communicate`) and `<hash>_temp.*` is always removed. `watch?v=X&list=…` queues the playlist from X; `embed/videoseries?list=` is a playlist; an empty search says "No results found"; a plain video URL skips the playlist probe. Every metadata and download run has `--playlist-items 1` (a channel/tab URL isn't reduced by `--no-playlist`: without it yt-dlp extracted every video of the tab, >100 s); a channel URL in `resolve_playlist_entries` (flat entries are its tabs) queues the videos of its first tab
- Stream prefetch: after stream-first play, downloads the file in background for loop support
- Stream failure mid-playback (e.g. YouTube answers ffmpeg's request with 403 after the worker started; since 2026-11-15): `_wait_for_worker_playback` → `_recover_failed_stream` waits for the prefetch, plays the file (or downloads it), once per track (`stream_file_retry_done`). Before, only a synchronous send failure fell back to a file; a 403 from the running ffmpeg just dropped the track and nothing played. `download_audio` is serialized per query/URL, because the prefetch and the fallback downloading the same track at once wrote the same `<hash>_temp.*` file and both failed. Tested by putting a fake `ffmpeg` that exits 8 on `googlevideo.com/videoplayback` URLs first in the unit's `PATH`.
- Result dicts include `artist`, `track`, `album`, `channel` from yt-dlp (empty string when absent); consumed by `extract_metadata()` in `bot.py` for scrobbler events
- **Spotify links** (`spotify.py`): yt-dlp can't play Spotify (DRM), so `play` resolves the link first — reads the public embed page `open.spotify.com/embed/<kind>/<id>` (`__NEXT_DATA__` JSON, no API key) and turns each track into a YouTube search query `<first artist> - <title>` (Spotify-only `- … Remaster` suffixes dropped). Track → plain query (stream-first path as usual); album/playlist/artist (top 10) → fed into the existing playlist loader as query strings. Embed pages list at most 100 tracks. Podcasts/audiobooks are refused. If Spotify changes the embed page layout, resolving fails with "nothing found at this link".
- `normalize_media_url()` prepends `https://` when the user omits the scheme (e.g. `youtube.com/watch?v=...` → `https://youtube.com/watch?v=...`), so bare-domain URLs are treated as URLs not search queries

### E2EE + Cross-signing
- Matrix room E2EE (chat messages, commands) is **fully supported** via `matrix-nio`. Call audio (LiveKit) is **not** SFrame-encrypted — see the call worker section for why.
- The worker connects to LiveKit with `autoSubscribe: false` (since 2026-11-15; was `true`): it only publishes music and never receives other participants' tracks. Humans' audio stays E2EE between their Element clients; the bot's unencrypted track carries only the music.
- E2EE via `matrix-nio`'s `SqliteStore` in `data/crypto_store/`
- On startup, `matrix_bot_common.cross_signing:ensure_cross_signing()` uploads/re-uploads master/self-signing/user-signing Ed25519 keypairs (from the shared [matrix-element-call-common](https://github.com/OolaaPleur/matrix-element-call-common) package)
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

**Playback generation counter** (`_playback_generation`): incremented each time a new track starts; used to discard stale auto-advance timers and worker terminal events from previous tracks. The worker watcher also passes `expected_source`, so the old track's `play_stopped` after a skip doesn't end the new one (2026-11-25 logs: 68 such stale events per 107 tracks). The auto-advance timer and the prefetch loop-replay take `_playback_lock`; `_cancel_worker_playback_wait` never cancels the calling task. `stop_playback` doesn't wait when nothing is playing (the worker sends no terminal event then). Only terminal/`joined`/`error` events are queued in `CallWorkerProcess._events` (pongs aren't). A replay after rejoin/worker restart ends the old play as `stopped` and starts a new `play_id`. `main.py` cancels the main task on SIGTERM/SIGINT, so `start()`'s `finally` runs and cancels timers/watchers, closes the playing track as `stopped` (`_active_play["room_id"]`) and waits up to 10 s for `_emit_queue` to drain. `stop`/`leave`/the join-time resync run under `_playback_lock`; worker restart/recovery replays the track via `_schedule_resync` (a task under the lock, skipped if a track was started after the event: recovery can fire inside `_advance_queue`). The advance watchdog re-checks `_playback_generation` under the lock. The loop replay after a stream prefetch requires the watcher to be done. `_cancel_background_load` no longer cancels the current track's file prefetch (only stop/leave/shutdown do). `CallWorkerProcess` drains old events before every spawn; an `error` event no longer sets state `error`.

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

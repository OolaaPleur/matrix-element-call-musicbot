# Matrix Element Call MusicBot

Discord-style music bot UX for Matrix: chat commands (`!m play`, `!m queue`, `!m skip`, etc.) plus real playback in Element Call.

Based on [SultanAlburaq/matrix-element-call-musicbot](https://github.com/SultanAlburaq/matrix-element-call-musicbot); this fork adds Matrix room E2EE, cross-signing, scrobbler events, an updated call worker and more.

Related projects:
- [matrix-element-call-scrobbler](https://github.com/OolaaPleur/matrix-element-call-scrobbler) — scrobbles what this bot plays to Last.fm for everyone in the call
- [matrix-element-call-common](https://github.com/OolaaPleur/matrix-element-call-common) — shared cross-signing code

## Features

- Discord-style chat command workflow in Matrix rooms
- Joins Element Call and plays audio directly in call
- URL or search-based playback with queue and ETA
- Playlists supported
- Saved queue presets (`save`, `load`, `rename`, `delete`)
- Playback history and now-playing visibility
- Audio controls (volume, fade-in, normalization)
- Configurable download format (`wav`, `mp3`, `ogg`, `m4a`, `opus`)
- Built-in diagnostics and runtime status commands
- Quiet-mode messaging defaults for less chat noise during playback
- Fast search defaults for quicker `!play` query resolution
- Full Matrix E2EE support — works in encrypted rooms, commands and messages are end-to-end encrypted

## Configurable Command Prefix

The default prefix is `!m` — all commands are `!m play`, `!m skip`, `!m help`, etc.
Shorthands work too: `!m p`, `!m s`, `!m h`.

To change the prefix, set in `config.toml`:
```toml
[bot]
command_prefix = "!m"   # change to anything, e.g. "!music" or "!dj"
```

## Performance Defaults

- `ui.quiet_mode = true` by default (suppresses non-critical chatter)
- `audio.search_mode = "fast"` by default (faster query resolution)
- `audio.stream_first_idle = true` by default (instant playback start when idle)

## E2EE

Matrix room E2EE is **fully supported** — the bot decrypts encrypted commands and encrypts its own messages. Cross-signing is bootstrapped automatically on startup so the bot user shows as verified in Element.

The bot does not subscribe to other participants' tracks (`autoSubscribe: false`), so it never receives anyone's voice.

### Call audio E2EE (optional)

With `[worker] e2ee = true` the call worker encrypts the music like Element Call does (LiveKit SFrame, per-participant keys exchanged over Olm). It needs:

1. **A device of its own.** The keys travel as Olm-encrypted to-device messages, and the bot's own device belongs to matrix-nio, so the worker logs in as a second device of the same account. Create it once (asks for the bot's password, writes `data/worker_device.json`, mode 600):

   ```bash
   systemctl --user stop matrix-musicbot      # the script refuses while the bot runs
   scripts/create_worker_device.py            # --replace for a new one
   ```

   With Docker (the file goes into the `musicbot_data` volume):

   ```bash
   docker compose stop musicbot
   docker compose run --rm musicbot python3 scripts/create_worker_device.py
   docker compose up -d
   ```

   Its Olm identity is kept in `data/worker_crypto_store.v8` (matrix-js-sdk rust crypto on [fake-indexeddb](https://github.com/dumbmatter/fakeIndexedDB), snapshotted to disk; no native modules). The worker cross-signs it with `data/cross_signing_keys.json` on start.

2. **A fix in `@livekit/rtc-node`.** Released versions (1.2.x) can't take a key from the application ([livekit/node-sdks#768](https://github.com/livekit/node-sdks/pull/768)). The Docker image applies it at build time; on the host, apply it after every `bun install`:

   ```bash
   python3 call_worker/patches/apply-pr768.py
   ```

   The script patches the installed `dist/` files, is idempotent, and fails loudly on an rtc-node version it doesn't know. Drop it once a release contains #768.

The worker also moves its own frame cryptors to each new key index by hand (`FrameCryptor.setKeyIndex`), working around [livekit/rust-sdks#1676](https://github.com/livekit/rust-sdks/issues/1676) until a release ships [#1677](https://github.com/livekit/rust-sdks/pull/1677). `WORKER_E2EE_SET_KEY_INDEX=false` turns that off.

If the device file or the rtc-node fix is missing, the device's token is rejected, or the crypto store can't be read, the worker logs why (`e2ee OFF, playing unencrypted: …`), says so in the room, and plays unencrypted (an unreadable store needs `create_worker_device.py --replace`). A homeserver that is merely unreachable doesn't switch E2EE off: the worker start fails and is retried.

## Current Limitations

- Call audio E2EE needs a patched `@livekit/rtc-node` until livekit/node-sdks#768 is released (see above)
- Single active playback session at a time (no sharding/multi-room playback yet)

## Companion Scrobbler Bot

With `[bot] scrobbler_events = true` this bot emits custom Matrix room events (`dev.elementcall.musicbot.track_started` / `dev.elementcall.musicbot.track_finished`) that a companion scrobbler bot can listen for to scrobble played tracks to Last.fm. They are off by default: each track costs two extra sends against the homeserver's rate limit.

Companion scrobbler: **https://github.com/OolaaPleur/matrix-element-call-scrobbler**

Both bots must share the same room to exchange events. The command prefix is configurable (`bot.command_prefix`) so their commands don't collide — by default the music bot uses `!m` and the scrobbler uses `!fm`.

## Invite Security

By default `auto_accept_invites = false` — the bot ignores all room invites.

To allow only specific users to invite the bot:

```toml
[bot]
auto_accept_invites = true
auto_accept_invites_from = ["@youruser:matrix.org"]
```

When `auto_accept_invites_from` is set, only those users can trigger auto-join; all other senders are silently ignored regardless of `auto_accept_invites`.

## Cross-Signing and User Verification

The bot automatically bootstraps Matrix cross-signing on startup (master key, self-signing key, user-signing key). This removes the "encrypted by device not verified by its owner" warning next to bot messages.

### Manual user verification from Element

To get a green verified shield on the bot user in Element:

1. Run the fingerprint script from the project root:
   ```
   python scripts/show_cross_signing_fingerprint.py
   ```
2. In Element, open the bot user profile → **Verify User** → **Verify manually**
3. Compare the master key shown in Element with the `Master key (grouped)` line from the script output
4. Confirm in Element

Add `--check-server` to also verify the local key matches what is currently uploaded (requires the bot to be running with a live token):
```
python scripts/show_cross_signing_fingerprint.py --check-server
```

Private keys are stored in `data/cross_signing_keys.json` (chmod 600, gitignored). If this file is deleted, the bot will initiate a new cross-signing reset on next startup.

## Troubleshooting

### `UnsupportedStickyEventsEndpointError` when joining call

If you see:

- `Membership manager error: ... UnsupportedStickyEventsEndpointError: Server does not support the sticky events`

your homeserver does not support sticky events yet. The worker will fall back and post:

- `Server lacks sticky events; fell back to legacy compatibility mode. Require PL50 (Moderator).`

What this means:

- PL50 is not always required.
- If your homeserver supports sticky events (`matrix2`/`matrix2_auto`), PL50 is typically not needed.
- On legacy mode, normal rooms may require PL50, while Video rooms can work without PL50.

How to force compatibility mode manually:

```toml
[worker]
membership_mode = "legacy"
```

Long-term fix:

- Upgrade your homeserver/call stack to support MatrixRTC sticky events, then use `matrix2_auto`.

## Demo

<p align="center">
  <img src="assets/play.gif" width="700"/>
</p>

<p align="center">
  <img src="assets/commands.gif" width="700"/>
</p>

<p align="center">
  <img src="assets/customplaylists.gif" width="700"/>
</p>


## Commands

### Playback

- `!m help` (`h`) — show this help
- `!m join` (`j`) — join Element Call in this room
- `!m leave` (`lv`) — leave current Element Call
- `!m play` (`p`) `<url-or-query>` — add track and auto-join call if needed
  - Spotify links (track, album, playlist, artist; `open.spotify.com/...`, `spotify:track:...`, `spotify.link/...`) are looked up on YouTube by artist + title — Spotify audio itself is DRM-protected. Albums/playlists: first 100 tracks; podcasts are not supported.
- `!m queue` (`q`) — show queue with ETA
- `!m nowplaying` (`np`) — show current track
- `!m skip` (`s`) — skip current track
- `!m pause` (`ps`) — pause; run it again (or `!m resume`, `rs`) to continue from the same spot
- `!m stop` (`x`) — stop playback and clear queue
- `!m loop` (`lp`) — toggle loop mode
- `!m shuffle` (`sh`, `random`) — shuffle the upcoming tracks (also the not-yet-downloaded rest of a playlist or saved queue)
- `!m history` (`hist`) — show recent playback history

### Saved Queues

- `!m save` (`sv`) `<name> [--force]` — save current+upcoming queue
- `!m load` (`ld`) `<name>` — load a saved queue
- `!m queues` (`qs`) — list saved queues
- `!m deletequeue` (`dq`) `<name>` — delete a saved queue
- `!m renamequeue` (`rq`) `<old> <new>` — rename a saved queue

### Audio & Info

- `!m audio` (`a`) — show current audio settings
- `!m normalize` (`norm`) `on|off` — toggle normalization
- `!m fadein` (`fi`) `<ms>` — set fade-in (0–5000)
- `!m volume` (`v`) `<0-200>` — set playback volume percent (logarithmic up to 100: 50% = −12 dB, about half as loud; linear above, 200% = +6 dB)
- `!m lang` `[code|default]` — show or set the language of the bot's messages in this room
- `!m status` (`st`) — show bot status
- `!m diag` (`d`) — show diagnostics
- `!m config` (`cfg`) — show active config
- `!m defaults` (`df`) — show default config values

## Docker

`docker compose up -d --build` builds the image from this checkout. CI also publishes it as `ghcr.io/oolaapleur/matrix-element-call-musicbot` (`latest`, version tag, commit SHA); to use that instead, remove `build: .` from `docker-compose.yml` and run `docker compose pull`. Bot data (crypto stores, saved queues, room languages) lives in the `musicbot_data` volume.
The bot runs as uid 10001 in the container, so `config/config.toml` must be readable by it (e.g. `chmod 644`, or `chown 10001` with `chmod 600` since it holds the access token).

### Linux

```bash
# 1) Download and extract release source
# 2) Enter extracted folder
cd musicbot

mkdir -p config
cp config/config.example.toml config/config.toml
# Edit config/config.toml

docker compose up -d --build
```

### Windows (PowerShell)

```powershell
# 1) Download and extract release source
# 2) Enter extracted folder
Set-Location .\musicbot

New-Item -ItemType Directory -Path .\config -Force | Out-Null
Copy-Item .\config\config.example.toml .\config\config.toml
# Edit .\config\config.toml

docker compose up -d --build
```

## Run Without Docker (Raw)

Use this only if you want to run directly on the host.

### Linux

```bash
# Clone this repo and the shared library as siblings
git clone https://github.com/OolaaPleur/matrix-element-call-musicbot
git clone https://github.com/OolaaPleur/matrix-element-call-common
cd matrix-element-call-musicbot

python3 -m venv venv
source venv/bin/activate

pip install -r requirements.txt   # resolves ../matrix-element-call-common automatically
bun install --cwd call_worker --frozen-lockfile
python3 call_worker/patches/apply-pr768.py   # rtc-node fix for call audio E2EE, see below

cp config/config.example.toml config.toml
# Edit config.toml

python3 main.py
```

Requires: Python 3.11+, Node.js 22+, Bun (installs the call worker's dependencies), ffmpeg, yt-dlp.

> [!NOTE]
> `requirements.txt` references `../matrix-element-call-common` (the shared library). Both repos must be cloned into the same parent directory for this to resolve correctly.

### Windows (PowerShell)

```powershell
# Clone this repo and the shared library as siblings
git clone https://github.com/OolaaPleur/matrix-element-call-musicbot
git clone https://github.com/OolaaPleur/matrix-element-call-common
Set-Location .\matrix-element-call-musicbot

py -3 -m venv venv
.\venv\Scripts\Activate.ps1

pip install -r requirements.txt
bun install --cwd call_worker --frozen-lockfile
python3 call_worker/patches/apply-pr768.py   # rtc-node fix for call audio E2EE, see below

Copy-Item .\config\config.example.toml .\config.toml
# Edit .\config.toml

python .\main.py
```

Requires: Python 3.11+, Node.js 22+, Bun (installs the call worker's dependencies), ffmpeg, yt-dlp.(available in PATH).

## Basic Verification

After start, check:

```bash
docker compose ps
docker logs --tail=100 musicbot
```

Then in Matrix room:

- `!m help`
- `!m join`
- `!m play never gonna give you up`

## Attribution

Forked from [SultanAlburaq/matrix-element-call-musicbot](https://github.com/SultanAlburaq/matrix-element-call-musicbot).

Shared Matrix bot utilities (cross-signing, E2EE helpers): [matrix-element-call-common](https://github.com/OolaaPleur/matrix-element-call-common).

## Languages

The bot's messages come from `locales/<code>.toml` (`en` is the complete reference, `ru` ships too). `[bot] language` in `config.toml` is the default; `!m lang <code>` sets the language of one room (`!m lang default` goes back to the config default).

To change some messages, edit `locales/custom/en.toml` or `locales/custom/ru.toml`: they list every message commented out; uncomment a line, change the text, restart. Your own translations go into `locales/custom/<code>.toml` too: a new language (copy `locales/en.toml`, set `language_name`, translate), or a few keys overriding a shipped one. Keys you leave out, and translations with a placeholder the bot doesn't know, fall back to English (with a warning in the log). Plural forms are tables keyed by `count`: `{ one = "…", other = "…" }`, Russian `{ one, few, many }`.

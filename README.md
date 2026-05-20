# Matrix Element Call MusicBot

Discord-style music bot UX for Matrix: chat commands (`!m play`, `!m queue`, `!m skip`, etc.) plus real playback in Element Call.

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

> [!NOTE]
> The call audio stream is **not** SFrame/E2EE encrypted. The LiveKit Rust SDK (`@livekit/rtc-node`) and browser JS SDK (`livekit-client`) have incompatible key derivation, causing `"maximum ratchet attempts exceeded"` on every audio frame. Since the bot streams public music this has no practical impact. Matrix room messages remain E2EE encrypted regardless.

## Current Limitations

- Call audio is not SFrame/E2EE encrypted (LiveKit SDK incompatibility — see above)
- Single active playback session at a time (no sharding/multi-room playback yet)

## Companion Scrobbler Bot

This bot emits custom Matrix room events (`dev.elementcall.musicbot.track_started` / `dev.elementcall.musicbot.track_finished`) that a companion scrobbler bot can listen for to scrobble played tracks to Last.fm.

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
- `!m queue` (`q`) — show queue with ETA
- `!m nowplaying` (`np`) — show current track
- `!m skip` (`s`) — skip current track
- `!m stop` (`x`) — stop playback and clear queue
- `!m loop` (`lp`) — toggle loop mode
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
- `!m volume` (`v`) `<0-200>` — set playback volume percent
- `!m status` (`st`) — show bot status
- `!m diag` (`d`) — show diagnostics
- `!m config` (`cfg`) — show active config
- `!m defaults` (`df`) — show default config values

## Docker

Build from the source of this repository to get all fork features.

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
npm ci --prefix call_worker

cp config/config.example.toml config.toml
# Edit config.toml

python3 main.py
```

Requires: Python 3.11+, Node.js 22+, ffmpeg, yt-dlp.

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
npm ci --prefix call_worker

Copy-Item .\config\config.example.toml .\config.toml
# Edit .\config.toml

python .\main.py
```

Requires: Python 3.11+, Node.js 22+, ffmpeg, yt-dlp (available in PATH).

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

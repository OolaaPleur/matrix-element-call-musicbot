# Running without Docker

[← README](../README.md)

Use this only if you want to run directly on the host. Linux only; on Windows use Docker Desktop.

Requires: Python 3.11+, Node.js 22+, Bun (installs the call worker's dependencies), ffmpeg, yt-dlp.

```bash
# Clone this repo and the shared library as siblings
git clone https://github.com/OolaaPleur/matrix-element-call-musicbot
git clone https://github.com/OolaaPleur/matrix-element-call-common
cd matrix-element-call-musicbot

python3 -m venv venv
source venv/bin/activate

pip install -r requirements.txt   # resolves ../matrix-element-call-common automatically
bun install --cwd call_worker --frozen-lockfile
python3 call_worker/patches/apply-pr768.py   # rtc-node fix for call audio E2EE, see docs/e2ee.md

cp config/config.example.toml config.toml
nano config.toml                      # settings (optional, the defaults work)
                                      # e.g. [bot] scrobbler_events = true for the companion scrobbler
python3 scripts/setup.py              # login, cross-signing keys, call worker device

python3 main.py
```

> [!NOTE]
> `requirements.txt` references `../matrix-element-call-common` (the shared library). Both repos must be cloned into the same parent directory.

## systemd user service

To keep it running, `~/.config/systemd/user/matrix-musicbot.service` (the name `rotate_access_token.py --restart` restarts):

```ini
[Unit]
Description=Matrix Element Call musicbot
After=network-online.target
Wants=network-online.target

[Service]
WorkingDirectory=%h/matrix-element-call-musicbot
ExecStart=%h/matrix-element-call-musicbot/venv/bin/python main.py
Environment=PYTHONUNBUFFERED=1
Restart=on-failure
RestartSec=30

[Install]
WantedBy=default.target
```

```bash
systemctl --user enable --now matrix-musicbot
loginctl enable-linger "$USER"   # keep it running while you're logged out
```

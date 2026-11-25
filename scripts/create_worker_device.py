#!/usr/bin/env python3
"""Create the call worker's own Matrix device (needed for call audio E2EE).

Media keys travel as Olm-encrypted to-device messages, so the Node call worker
needs a device with its own Olm identity; the bot's device belongs to nio.
Logs in once with the password (new device "musicbot call worker") and writes
data/worker_device.json (0600): user_id, device_id, access_token and a random
key that encrypts the worker's crypto store. Prints neither the token nor the key.
The worker cross-signs the device with data/cross_signing_keys.json on its
first start. Stop the bot first: a running worker would write the old
device's crypto store back after this script removed it.

    scripts/create_worker_device.py            # once
    scripts/create_worker_device.py --replace  # new device; logs the old one out
"""
import argparse, getpass, json, os, secrets, sys, tomllib, urllib.error, urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CONFIG = ROOT / "config.toml"
DATA = ROOT / "data"
DEVICE_FILE = DATA / "worker_device.json"
STORE_FILE = DATA / "worker_crypto_store.v8"
DISPLAY_NAME = "musicbot call worker"

parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
parser.add_argument("--replace", action="store_true", help="replace an existing worker device")
parser.add_argument("--force", action="store_true", help="run even though the bot seems to be running")
args = parser.parse_args()


def bot_processes():
    """PIDs of this checkout's bot (main.py) or call worker (join_call.js)."""
    pids = []
    for proc in Path("/proc").glob("[0-9]*"):
        try:
            cmdline = (proc / "cmdline").read_bytes().split(b"\0")
            cwd = Path(os.readlink(proc / "cwd"))
        except OSError:
            continue
        if cwd != ROOT and ROOT not in cwd.parents:
            continue
        if any(arg.endswith((b"main.py", b"join_call.js")) for arg in cmdline):
            pids.append(int(proc.name))
    return pids


running = bot_processes()
if running and not args.force:
    sys.exit(f"the bot seems to be running (pid {', '.join(map(str, running))}); stop it first "
             "(e.g. systemctl --user stop matrix-musicbot) or pass --force")

raw = tomllib.loads(CONFIG.read_text())
cfg = raw.get("matrix", raw)
hs, user_id = cfg["homeserver"].rstrip("/"), cfg["user_id"]


def call(method, path, body=None, token=None, ok_codes=()):
    req = urllib.request.Request(hs + path, method=method,
                                 data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Content-Type": "application/json"})
    if token:
        req.add_header("Authorization", "Bearer " + token)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        if e.code in ok_codes:
            return None
        sys.exit(f"{method} {path} failed: HTTP {e.code} {e.read().decode()[:300]}")


old = None
if DEVICE_FILE.exists():
    if not args.replace:
        sys.exit(f"{DEVICE_FILE} exists (device {json.loads(DEVICE_FILE.read_text())['device_id']}); "
                 "use --replace for a new one")
    old = json.loads(DEVICE_FILE.read_text())

print(f"{user_id}: creating a new device '{DISPLAY_NAME}'")
password = getpass.getpass("Bot password: ")
resp = call("POST", "/_matrix/client/v3/login", {
    "type": "m.login.password",
    "identifier": {"type": "m.id.user", "user": user_id},
    "password": password,
    "initial_device_display_name": DISPLAY_NAME,
})
del password
if resp.get("user_id") != user_id:
    sys.exit(f"logged in as {resp.get('user_id')}, expected {user_id}")

DATA.mkdir(mode=0o700, exist_ok=True)
data = json.dumps({
    "user_id": user_id,
    "device_id": resp["device_id"],
    "access_token": resp["access_token"],
    "store_key": secrets.token_hex(32),
}, indent=2).encode()
tmp = DEVICE_FILE.with_suffix(".tmp")
fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
try:
    os.write(fd, data)
    os.fsync(fd)
finally:
    os.close(fd)
os.replace(tmp, DEVICE_FILE)
# The old store holds the old device's Olm identity: useless for the new one.
for stale in [STORE_FILE, *DATA.glob(STORE_FILE.name + ".*")]:
    stale.unlink(missing_ok=True)
print(f"worker device {resp['device_id']} written to {DEVICE_FILE.relative_to(ROOT)}")

if old:
    # Logging out deletes the old device (and its keys) on the server.
    call("POST", "/_matrix/client/v3/logout", {}, token=old["access_token"], ok_codes=(401,))
    print(f"old worker device {old['device_id']} logged out")

print("Set [worker] e2ee = true in config.toml and restart the bot.")

#!/usr/bin/env python3
"""Reissue the bot's Matrix access token without creating a new device.

Logs in with the password and the SAME device_id (data/device_id), so the
homeserver invalidates the old token while the device and its E2EE keys stay.
Writes the new token into config.toml (never prints it) and restarts the bot.
"""
import getpass, json, re, subprocess, sys, tomllib, urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CONFIG = ROOT / "config.toml"
SERVICE = "matrix-musicbot.service"

cfg = tomllib.loads(CONFIG.read_text())["matrix"] if "matrix" in tomllib.loads(CONFIG.read_text()) else tomllib.loads(CONFIG.read_text())
hs, user_id = cfg["homeserver"].rstrip("/"), cfg["user_id"]
device_id = (ROOT / "data" / "device_id").read_text().strip()

def call(method, path, body=None, token=None):
    req = urllib.request.Request(hs + path, method=method,
                                 data=json.dumps(body).encode() if body else None,
                                 headers={"Content-Type": "application/json"})
    if token:
        req.add_header("Authorization", "Bearer " + token)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        sys.exit(f"{method} {path} failed: HTTP {e.code} {e.read().decode()[:300]}")

print(f"{user_id}, device {device_id}")
password = getpass.getpass("Bot password: ")
resp = call("POST", "/_matrix/client/v3/login", {
    "type": "m.login.password",
    "identifier": {"type": "m.id.user", "user": user_id},
    "password": password,
    "device_id": device_id,
})
token = resp["access_token"]
if resp.get("device_id") != device_id:
    sys.exit(f"Server returned another device ({resp.get('device_id')}); config NOT changed. "
             "Log that device out in Element.")

text = CONFIG.read_text()
new, n = re.subn(r'(?m)^(access_token\s*=\s*)"[^"]*"', lambda m: m.group(1) + json.dumps(token), text)
if n != 1:
    sys.exit("access_token line not found; config NOT changed")
CONFIG.write_text(new)
CONFIG.chmod(0o600)
print("config.toml updated (mode 600)")

who = call("GET", "/_matrix/client/v3/account/whoami", token=token)
print(f"whoami: {who.get('user_id')} device {who.get('device_id')}")
subprocess.run(["systemctl", "--user", "restart", SERVICE], check=True)
print(f"{SERVICE} restarted")

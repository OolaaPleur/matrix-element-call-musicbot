#!/usr/bin/env python3
"""Get an access token for the bot's Matrix account (first setup).

Logs in once with the bot's password, which creates the bot's device on the
homeserver, and writes homeserver, user_id and access_token into the [matrix]
section of the config (CONFIG_FILE, else config.toml, else config/config.toml;
mode 600). If there is no config or it can't be written (read-only in the
container), prints the lines to paste instead. Only the Python standard
library is needed, so it runs on the host before anything else is installed:

    python3 scripts/create_access_token.py

or inside the image (no Python on the host, e.g. Windows):

    docker compose run --rm musicbot python3 scripts/create_access_token.py

Don't use the token Element shows (Settings > Help & About): it belongs to
that Element session, and on homeservers with OAuth login (matrix.org) it
expires within minutes.

To get a new token for an existing install later, use rotate_access_token.py
instead: it keeps the bot's device and its E2EE keys.
"""
import getpass
import json
import os
import re
import sys
import urllib.error
import urllib.request
from pathlib import Path

DEVICE_NAME = "musicbot"
ROOT = Path(__file__).resolve().parent.parent


def request(method, url, body=None):
    req = urllib.request.Request(
        url,
        method=method,
        data=json.dumps(body).encode() if body is not None else None,
        # Cloudflare (matrix.org) rejects urllib's default User-Agent.
        headers={"Content-Type": "application/json", "Accept": "application/json",
                 "User-Agent": "matrix-element-call-musicbot"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def discover_homeserver(server_name):
    """Client API base URL for a server name, via .well-known (matrix.org ->
    https://matrix-client.matrix.org); falls back to https://<server_name>."""
    try:
        data = request("GET", f"https://{server_name}/.well-known/matrix/client")
        base = data["m.homeserver"]["base_url"]
        if base:
            return base.rstrip("/")
    except (urllib.error.URLError, OSError, ValueError, KeyError, TypeError):
        pass
    return f"https://{server_name}"


def find_config():
    """The config the bot will read: CONFIG_FILE (Docker), else config.toml
    (raw install), else config/config.toml (Docker, run on the host)."""
    env = os.environ.get("CONFIG_FILE")
    if env:
        path = Path(env)
        return path if path.is_absolute() else ROOT / path
    for path in (ROOT / "config.toml", ROOT / "config" / "config.toml"):
        if path.is_file():
            return path
    return None


def write_config(path, values):
    """Set the keys in the [matrix] section; False if that isn't possible."""
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return False
    section = re.search(r"(?ms)^\[matrix\][ \t]*\n(.*?)(?=^\[|\Z)", text)
    if not section:
        return False
    body = section.group(1)
    for key, value in values.items():
        body, n = re.subn(rf'(?m)^({key}\s*=\s*)"[^"\n]*"',
                          lambda m: m.group(1) + json.dumps(value), body, count=1)
        if n != 1:
            return False
    try:
        path.write_text(text[:section.start(1)] + body + text[section.end(1):], encoding="utf-8")
        path.chmod(0o600)
    except OSError:
        return False
    return True


def main():
    user_id = input("Bot's Matrix user ID (e.g. @musicbot:matrix.org): ").strip()
    if not user_id.startswith("@") or ":" not in user_id:
        sys.exit("A full user ID is needed: @name:server")
    homeserver = discover_homeserver(user_id.split(":", 1)[1])
    print(f"Homeserver: {homeserver}")

    try:
        flows = request("GET", f"{homeserver}/_matrix/client/v3/login").get("flows", [])
    except (urllib.error.URLError, OSError, ValueError) as exc:
        sys.exit(f"Can't reach {homeserver}: {exc}")
    if not any(flow.get("type") == "m.login.password" for flow in flows):
        sys.exit("This homeserver doesn't allow password login for bots "
                 "(no m.login.password); ask its admin for a token.")

    password = getpass.getpass("Bot's password (not shown): ")
    try:
        resp = request("POST", f"{homeserver}/_matrix/client/v3/login", {
            "type": "m.login.password",
            "identifier": {"type": "m.id.user", "user": user_id},
            "password": password,
            "initial_device_display_name": DEVICE_NAME,
        })
    except urllib.error.HTTPError as exc:
        try:
            error = json.load(exc).get("error", "")
        except ValueError:
            error = ""
        sys.exit(f"Login failed: HTTP {exc.code} {error}".rstrip())
    except (urllib.error.URLError, OSError) as exc:
        sys.exit(f"Login failed: {exc}")

    print(f"\nLogged in as {resp['user_id']}, new device {resp['device_id']}.")
    values = {"homeserver": homeserver, "user_id": resp["user_id"],
              "access_token": resp["access_token"]}
    config = find_config()
    if config and write_config(config, values):
        print(f"Wrote homeserver, user_id and access_token into {config} (mode 600).")
        return
    where = f"{config} can't be updated" if config else "No config.toml found"
    print(f"{where}; put these lines into its [matrix] section "
          "(the token is a password: keep it secret, don't paste it anywhere else):\n")
    for key, value in values.items():
        print(f"{key} = {json.dumps(value)}")


if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, EOFError):
        sys.exit("\nCancelled")

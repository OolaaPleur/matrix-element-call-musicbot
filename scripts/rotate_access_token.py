#!/usr/bin/env python3
"""Reissue the bot's Matrix access token without creating a new device.

Logs in with the password and the SAME device ID, so the homeserver
invalidates the old token while the device and its E2EE keys stay. Saves the
new token in data/credentials.json and never prints it. Restart the bot
afterwards; with --restart it restarts the systemd user unit matrix-musicbot.

    scripts/rotate_access_token.py [--restart]
    docker compose run --rm musicbot python3 scripts/rotate_access_token.py   # then: docker compose restart
"""
import argparse
import getpass
import subprocess
import sys

import _matrix

SERVICE = "matrix-musicbot.service"


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--restart", action="store_true", help=f"restart {SERVICE} afterwards")
    args = parser.parse_args()

    login = _matrix.bot_login()
    device_file = _matrix.DATA / "device_id"
    device_id = login.get("device_id") or (device_file.read_text().strip() if device_file.exists() else "")
    if not (login.get("homeserver") and login.get("user_id") and device_id):
        sys.exit("No login in data/credentials.json: run scripts/setup.py")
    homeserver, user_id = login["homeserver"], login["user_id"]

    print(f"{user_id}, device {device_id}")
    try:
        resp = _matrix.login(homeserver, user_id, getpass.getpass("Bot password: "), device_id=device_id)
    except _matrix.MatrixError as exc:
        sys.exit(f"Login failed: {exc}")
    if resp.get("device_id") != device_id:
        sys.exit(f"Server returned another device ({resp.get('device_id')}); nothing changed. "
                 "Log that device out in Element.")
    token = resp["access_token"]

    stored = _matrix.bot_config.load_credentials()
    stored.update({"device_id": device_id, "access_token": token})
    _matrix.bot_config.save_credentials(stored)
    print("data/credentials.json updated")

    who = _matrix.request("GET", f"{homeserver}/_matrix/client/v3/account/whoami", token=token)
    print(f"whoami: {who.get('user_id')} device {who.get('device_id')}")
    if args.restart:
        subprocess.run(["systemctl", "--user", "restart", SERVICE], check=True)
        print(f"{SERVICE} restarted")
    else:
        print("Restart the bot to use the new token.")


if __name__ == "__main__":
    main()

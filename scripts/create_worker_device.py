#!/usr/bin/env python3
"""Create the call worker's own Matrix device (needed for call audio E2EE).

scripts/setup.py does this as part of the first setup; run this script to
create the device later or replace it.

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

Docker (writes into the data volume; stop the bot first, the check below
can't see another container):

    docker compose stop musicbot
    docker compose run --rm musicbot python3 scripts/create_worker_device.py
"""
import argparse
import getpass
import json
import secrets
import sys

import _matrix

DEVICE_FILE = _matrix.DATA / "worker_device.json"
STORE_FILE = _matrix.DATA / "worker_crypto_store.v8"
DISPLAY_NAME = "musicbot call worker"


def create(homeserver, user_id, password, replace=False):
    """Log the worker device in and write data/worker_device.json; returns its device ID."""
    old = None
    if DEVICE_FILE.exists():
        if not replace:
            raise SystemExit(f"{DEVICE_FILE} exists (device {json.loads(DEVICE_FILE.read_text())['device_id']}); "
                             "use --replace for a new one")
        old = json.loads(DEVICE_FILE.read_text())
    try:
        resp = _matrix.login(homeserver, user_id, password, device_name=DISPLAY_NAME)
    except _matrix.MatrixError as exc:
        raise SystemExit(f"Worker device login failed: {exc}")
    if resp.get("user_id") != user_id:
        raise SystemExit(f"logged in as {resp.get('user_id')}, expected {user_id}")
    _matrix.write_private(DEVICE_FILE, {
        "user_id": user_id,
        "device_id": resp["device_id"],
        "access_token": resp["access_token"],
        "store_key": secrets.token_hex(32),
    })
    # The old store holds the old device's Olm identity: useless for the new one.
    for stale in [STORE_FILE, *_matrix.DATA.glob(STORE_FILE.name + ".*")]:
        stale.unlink(missing_ok=True)
    if old:
        # Logging out deletes the old device (and its keys) on the server.
        try:
            _matrix.request("POST", f"{homeserver}/_matrix/client/v3/logout", {}, token=old["access_token"])
            print(f"Old worker device {old['device_id']} logged out")
        except _matrix.MatrixError as exc:
            if exc.status != 401:
                print(f"Couldn't log out the old worker device {old['device_id']}: {exc}")
    return resp["device_id"]


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--replace", action="store_true", help="replace an existing worker device")
    parser.add_argument("--force", action="store_true", help="run even though the bot seems to be running")
    args = parser.parse_args()

    running = _matrix.bot_processes()
    if running and not args.force:
        sys.exit(f"the bot seems to be running (pid {', '.join(map(str, running))}); stop it first "
                 "or pass --force")
    login = _matrix.bot_login()
    if not login.get("homeserver") or not login.get("user_id"):
        sys.exit("The bot isn't set up yet: run scripts/setup.py")
    print(f"{login['user_id']}: creating a new device '{DISPLAY_NAME}'")
    device_id = create(login["homeserver"], login["user_id"], getpass.getpass("Bot password: "), args.replace)
    print(f"Worker device {device_id} written to data/{DEVICE_FILE.name}. Restart the bot.")


if __name__ == "__main__":
    main()

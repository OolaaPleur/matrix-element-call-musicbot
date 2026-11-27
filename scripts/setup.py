#!/usr/bin/env python3
"""First-time setup of the bot's Matrix account, in one run.

1. Logs the bot in with its password (that login is the bot's device) and
   saves the login in data/credentials.json (mode 600). The config needs no
   secrets.
2. Cross-signing: if the account already has keys in secret storage (set up
   in Element with "Set up recovery"), asks for the recovery key and restores
   them, so the bot signs its device on first start. A fresh account has
   nothing to restore; the bot creates its keys on first start.
3. Creates the call worker's own device for encrypted call audio.

Safe to run again: done steps are skipped, so a failed step can be retried.

    docker compose run --rm musicbot python3 scripts/setup.py
    python3 scripts/setup.py            # raw install, in the bot's venv

Later changes have their own scripts: rotate_access_token.py (new token, same
device), create_worker_device.py --replace, and
`python3 -m matrix_bot_common.restore_keys`.
"""
import getpass
import sys

import _matrix
import create_worker_device

BOT_DEVICE_NAME = "musicbot"


def ask(prompt):
    value = input(prompt).strip()
    if not value:
        sys.exit("Cancelled")
    return value


class Password:
    """Asks for the bot's password once, only when a step needs it."""

    def __init__(self):
        self.value = None

    def __call__(self):
        if self.value is None:
            self.value = getpass.getpass("Bot's password (not shown): ")
        return self.value


def step_login(password):
    login = _matrix.bot_login()
    if login.get("homeserver") and login.get("user_id") and login.get("access_token"):
        try:
            _matrix.request("GET", f"{login['homeserver']}/_matrix/client/v3/account/whoami",
                            token=login["access_token"])
            print(f"1. Login: already done ({login['user_id']}"
                  f"{', device ' + login['device_id'] if login.get('device_id') else ''}).")
            return login
        except _matrix.MatrixError as exc:
            if exc.status != 401:
                sys.exit(f"Checking the saved login failed: {exc}")
            print("1. Login: the saved token no longer works (device logged out?), logging in again.")
        except OSError as exc:
            sys.exit(f"Can't reach {login['homeserver']}: {exc}")
    user_id = login.get("user_id") or ask("Bot's Matrix user ID (e.g. @musicbot:matrix.org): ")
    if not user_id.startswith("@") or ":" not in user_id:
        sys.exit("A full user ID is needed: @name:server")
    homeserver = login.get("homeserver") or _matrix.discover_homeserver(user_id.split(":", 1)[1])
    print(f"Homeserver: {homeserver}")
    try:
        if not _matrix.password_login_allowed(homeserver):
            sys.exit("This homeserver doesn't allow password login for bots (no m.login.password).")
        resp = _matrix.login(homeserver, user_id, password(), device_name=BOT_DEVICE_NAME)
    except _matrix.MatrixError as exc:
        sys.exit(f"Login failed: {exc}")
    except OSError as exc:
        sys.exit(f"Can't reach {homeserver}: {exc}")
    login = {"homeserver": homeserver, "user_id": resp["user_id"],
             "device_id": resp["device_id"], "access_token": resp["access_token"]}
    _matrix.bot_config.save_credentials(login)
    print(f"1. Login: done, device {resp['device_id']}, saved in data/credentials.json.")
    return login


def step_cross_signing(login):
    if (_matrix.DATA / "cross_signing_keys.json").exists():
        print("2. Cross-signing keys: already there.")
        return True
    try:
        from matrix_bot_common import restore_keys
    except ImportError as exc:
        print(f"2. Cross-signing keys: matrix_bot_common is missing ({exc}); install requirements.txt.")
        return False
    print("2. Cross-signing keys: checking the account's secret storage...")
    ask_key = lambda: getpass.getpass(  # noqa: E731
        "Recovery key from Element (not shown; Enter to skip): ")
    while True:
        try:
            result = restore_keys.restore(login["homeserver"], login["access_token"], _matrix.DATA, ask_key)
            break
        except restore_keys.Abort as exc:
            print(f"   {exc}")
            if "recovery key" not in str(exc).lower():
                return False
    if result == "restored":
        print("   Restored: the bot signs its device on first start.")
    elif result == "no_secret_storage":
        print("   The account has no keys yet: the bot creates them on first start.")
    else:
        print("   Skipped. Without the keys the bot can't sign its device; see the README "
              "(Cross-Signing) to restore them later or to replace them.")
    return True


def step_worker(login, password):
    if create_worker_device.DEVICE_FILE.exists():
        print("3. Call worker device: already there.")
        return
    device_id = create_worker_device.create(login["homeserver"], login["user_id"], password())
    print(f"3. Call worker device: done, device {device_id}.")


def main():
    running = _matrix.bot_processes()
    if running:
        sys.exit(f"The bot seems to be running (pid {', '.join(map(str, running))}); stop it first.")
    password = Password()
    login = step_login(password)
    keys_ok = step_cross_signing(login)
    step_worker(login, password)
    if not keys_ok:
        sys.exit("\nSetup isn't complete: fix the problem above and run scripts/setup.py again.")
    print("\nSetup done. Start the bot: `docker compose up -d` (or `python3 main.py`).")


if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, EOFError):
        sys.exit("\nCancelled")

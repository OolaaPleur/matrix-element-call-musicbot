#!/usr/bin/env python3
"""First-time setup of the bot's Matrix account, in one run.

1. Logs the bot in with its password (that login is the bot's device) and
   saves the login in data/credentials.json (mode 600). The config needs no
   secrets.
2. Asks for your Matrix user ID: the bot accepts room invites from it
   (Enter to skip; more users go into [bot] auto_accept_invites_from).
3. Cross-signing: if the account already has keys in secret storage (set up
   in Element with "Set up recovery"), asks for the recovery key and restores
   them, so the bot signs its device on first start. A fresh account has
   nothing to restore; the bot creates its keys on first start.
4. Creates the call worker's own device for encrypted call audio.

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
INDENT = "   "


def say(text):
    print(INDENT + text)


def done(text):
    print(INDENT + "✓ " + text)


def ask(prompt):
    value = input(INDENT + prompt).strip()
    if not value:
        sys.exit("Cancelled")
    return value


class Password:
    """Asks for the bot's password once, only when a step needs it."""

    def __init__(self):
        self.value = None

    def __call__(self):
        if self.value is None:
            self.value = getpass.getpass(INDENT + "Bot's password (not shown): ")
        return self.value


def step_login(password):
    print("1. Login")
    login = _matrix.bot_login()
    if login.get("homeserver") and login.get("user_id") and login.get("access_token"):
        try:
            _matrix.request("GET", f"{login['homeserver']}/_matrix/client/v3/account/whoami",
                            token=login["access_token"])
            done(f"Already done: {login['user_id']}"
                 f"{', device ' + login['device_id'] if login.get('device_id') else ''}")
            return login
        except _matrix.MatrixError as exc:
            if exc.status != 401:
                sys.exit(f"Checking the saved login failed: {exc}")
            say("The saved token no longer works (device logged out?), logging in again.")
        except OSError as exc:
            sys.exit(f"Can't reach {login['homeserver']}: {exc}")
    user_id = login.get("user_id") or ask("Bot's Matrix user ID (e.g. @musicbot:matrix.org): ")
    if not user_id.startswith("@") or ":" not in user_id:
        sys.exit("A full user ID is needed: @name:server")
    homeserver = login.get("homeserver") or _matrix.discover_homeserver(user_id.split(":", 1)[1])
    say(f"Homeserver: {homeserver}")
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
    stored = _matrix.bot_config.load_credentials()
    stored.update(login)
    _matrix.bot_config.save_credentials(stored)
    done(f"Device {resp['device_id']}, saved in data/credentials.json")
    return login


def step_owner():
    print("2. Room invites")
    stored = _matrix.bot_config.load_credentials()
    if "owners" in stored:
        done(f"Already done: accepted from {', '.join(stored['owners']) or 'nobody (skipped)'}")
        return
    owner = input(INDENT + "Your Matrix user ID; the bot joins rooms you invite it to "
                  "(Enter to skip; more users: [bot] auto_accept_invites_from in the config): ").strip()
    if owner and (not owner.startswith("@") or ":" not in owner):
        sys.exit(f"A full user ID is needed: @name:server (got {owner!r})")
    stored["owners"] = [owner] if owner else []
    _matrix.bot_config.save_credentials(stored)
    if owner:
        done(f"Accepted from {owner}")
    else:
        done("Skipped: the bot joins no room by itself; set [bot] auto_accept_invites_from "
             "(or auto_accept_invites = true) in the config")


def step_cross_signing(login):
    print("3. Cross-signing keys")
    if (_matrix.DATA / "cross_signing_keys.json").exists():
        done("Already there")
        return True
    try:
        from matrix_bot_common import restore_keys
    except ImportError as exc:
        say(f"matrix_bot_common is missing ({exc}); install requirements.txt.")
        return False
    ask_key = lambda: getpass.getpass(INDENT + "Recovery key from Element (not shown; Enter to skip): ")  # noqa: E731
    while True:
        try:
            result = restore_keys.restore(login["homeserver"], login["access_token"], _matrix.DATA,
                                          ask_key, out=lambda *_: None)
            break
        except restore_keys.Abort as exc:
            say(str(exc))
            if "recovery key" not in str(exc).lower():
                return False
    if result == "restored":
        say("master, self-signing, user-signing: match the server")
        done("Restored: the bot signs its device on first start")
    elif result == "fresh":
        done("The account has no keys yet: the bot creates them on first start")
    elif result == "no_secret_storage":
        done("The account has keys but no recovery set up, so they can't be restored. The bot runs "
             "with an unsigned device until you replace them (README, Cross-Signing)")
    else:
        done("Skipped: the bot can't sign its device without the keys; restore or replace them later "
             "(README, Cross-Signing)")
    return True


def step_worker(login, password):
    print("4. Call worker device")
    if create_worker_device.DEVICE_FILE.exists():
        done("Already there")
        return
    device_id = create_worker_device.create(login["homeserver"], login["user_id"], password())
    done(f"Device {device_id}")


def main():
    running = _matrix.bot_processes()
    if running:
        sys.exit(f"The bot seems to be running (pid {', '.join(map(str, running))}); stop it first.")
    password = Password()
    login = step_login(password)
    step_owner()
    keys_ok = step_cross_signing(login)
    step_worker(login, password)
    if not keys_ok:
        sys.exit("\nSetup isn't complete: fix the problem above and run scripts/setup.py again.")
    print("\nSetup done. Start the bot: docker compose up -d (or python3 main.py)")


if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, EOFError):
        sys.exit("\nCancelled")

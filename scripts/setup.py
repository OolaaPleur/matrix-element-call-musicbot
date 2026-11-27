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
When everything is done, it offers to change the answers: another bot
account, other users for room invites, the cross-signing keys restored again,
a new call worker device. Stop the bot first.

    docker compose run --rm musicbot python3 scripts/setup.py
    python3 scripts/setup.py            # raw install, in the bot's venv

A new token for the same device: rotate_access_token.py.
"""
import datetime
import getpass
import sys
import tomllib

import _matrix
import create_worker_device

BOT_DEVICE_NAME = "musicbot"
INDENT = "   "


def say(text):
    print(INDENT + text)


WORKED = False  # set when a step did something on this run


def done(text):
    global WORKED
    WORKED = True
    print(INDENT + "✓ " + text)


def already(text):
    print(INDENT + "✓ " + text)


def yes(prompt):
    return input(INDENT + prompt + " [y/N]: ").strip().lower() in ("y", "yes")


def user_ids(text):
    ids = [part for part in text.replace(",", " ").split() if part]
    for value in ids:
        if not value.startswith("@") or ":" not in value:
            sys.exit(f"A full user ID is needed: @name:server (got {value!r})")
    return ids


def config_overrides(section, keys):
    """Keys of the config's [section] that override what setup stores."""
    path = _matrix.config_path()
    if not path:
        return []
    table = tomllib.loads(path.read_text(encoding="utf-8")).get(section, {})
    return [key for key in keys if key in table and (table[key] or isinstance(table[key], list))]


# What belongs to the bot's account, moved aside when it changes.
ACCOUNT_FILES = ["crypto_store", "device_id", "cross_signing_keys.json", "cross_signing_keys.json.stale-*",
                 "worker_device.json", "worker_crypto_store.v8*"]


def move_account_files():
    target = _matrix.DATA / f"previous-account-{datetime.datetime.now():%Y%m%d-%H%M%S}"
    moved = [path for pattern in ACCOUNT_FILES for path in _matrix.DATA.glob(pattern)]
    if moved:
        target.mkdir()
        for path in moved:
            path.rename(target / path.name)
        say(f"The old account's keys and devices are in data/{target.name}/")


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


def step_login(password, change=False):
    print("1. Login")
    login = _matrix.bot_login()
    if login.get("homeserver") and login.get("user_id") and login.get("access_token"):
        try:
            _matrix.request("GET", f"{login['homeserver']}/_matrix/client/v3/account/whoami",
                            token=login["access_token"])
            already(f"{'Now' if change else 'Already done'}: {login['user_id']}"
                    f"{', device ' + login['device_id'] if login.get('device_id') else ''}")
            if not change:
                return login
            new_id = input(INDENT + "Enter to keep, or another account's user ID: ").strip()
            if not new_id or new_id == login["user_id"]:
                return login
            user_ids(new_id)
            say("The bot's keys and devices belong to its account: they are moved aside, and the next")
            say("steps set up the new account. Rooms, saved queues and languages stay.")
            if not yes(f"Switch the bot to {new_id}?"):
                return login
            move_account_files()
            stored = _matrix.bot_config.load_credentials()
            for key in ("homeserver", "user_id", "device_id", "access_token"):
                stored.pop(key, None)
            _matrix.bot_config.save_credentials(stored)
            login = {"user_id": new_id}
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


def step_owner(change=False):
    print("2. Room invites")
    stored = _matrix.bot_config.load_credentials()
    if "owners" in stored:
        already(f"{'Now' if change else 'Already done'}: accepted from "
                f"{', '.join(stored['owners']) or 'nobody (skipped)'}")
        if not change:
            return
        if config_overrides("bot", ["auto_accept_invites_from"]):
            say("[bot] auto_accept_invites_from in the config overrides this; change it there.")
            return
        answer = input(INDENT + "Enter to keep, - for nobody, or user IDs (space-separated): ").strip()
        if not answer:
            return
        stored["owners"] = [] if answer == "-" else user_ids(answer)
        _matrix.bot_config.save_credentials(stored)
        done(f"Accepted from {', '.join(stored['owners']) or 'nobody'}")
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


def step_cross_signing(login, change=False):
    print("3. Cross-signing keys")
    if (_matrix.DATA / "cross_signing_keys.json").exists():
        already("Already there")
        if not change or not yes("Restore them again with the recovery key (e.g. after a reset in Element)?"):
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
        already("The account has keys but no recovery set up, so they can't be restored. The bot runs "
             "with an unsigned device until you replace them (README, Cross-Signing)")
    else:
        already("Skipped: the bot can't sign its device without the keys; restore or replace them later "
             "(README, Cross-Signing)")
    return True


def step_worker(login, password, change=False):
    print("4. Call worker device")
    replace = False
    if create_worker_device.DEVICE_FILE.exists():
        already("Already there")
        if not change or not yes("Replace it with a new device (logs the old one out)?"):
            return
        replace = True
    device_id = create_worker_device.create(login["homeserver"], login["user_id"], password(), replace)
    done(f"Device {device_id}")


def main():
    running = _matrix.bot_processes()
    if running:
        sys.exit(f"The bot seems to be running (pid {', '.join(map(str, running))}); stop it first.")
    password = Password()
    change = False
    while True:
        login = step_login(password, change)
        step_owner(change)
        keys_ok = step_cross_signing(login, change)
        step_worker(login, password, change)
        if not keys_ok:
            sys.exit("\nSetup isn't complete: fix the problem above and run scripts/setup.py again.")
        if WORKED or change:
            break
        print("\nEverything is set up.")
        if not yes("Change something?"):
            return
        print()
        change = True
    print("\nSetup done. Start the bot: docker compose up -d (or python3 main.py)"
          if not change else "\nDone. Start the bot again: docker compose up -d (or python3 main.py)")


if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, EOFError):
        sys.exit("\nCancelled")

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
import json
import os
import sys
import tomllib

import _matrix
import create_worker_device

BOT_DEVICE_NAME = "musicbot"
INDENT = "   "


def say(text):
    print(INDENT + text)


WORKED = False  # set when a step did something on this run
KEYS_SKIPPED = False  # the recovery key was skipped: setup isn't complete


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


def owners_overridden():
    """Same rule as config.py: any auto_accept_invites_from in the config or the
    environment replaces the owners from setup."""
    if "AUTO_ACCEPT_INVITES_FROM" in os.environ:
        return "The AUTO_ACCEPT_INVITES_FROM environment variable"
    path = _matrix.config_path()
    if path and "auto_accept_invites_from" in tomllib.loads(path.read_text(encoding="utf-8")).get("bot", {}):
        return "[bot] auto_accept_invites_from in the config"
    return None


# What belongs to the bot's account, moved aside when it changes.
ACCOUNT_FILES = ["crypto_store", "device_id", "cross_signing_keys.json", "cross_signing_keys.json.stale-*",
                 "worker_device.json", "worker_crypto_store.v8*"]


def log_out_old_devices(login):
    """Best effort: the old account's bot and worker devices, so they don't linger as
    unverified sessions."""
    tokens = [("bot", login.get("homeserver"), login.get("access_token"))]
    worker_file = _matrix.DATA / "worker_device.json"
    if worker_file.exists():
        try:
            tokens.append(("call worker", login.get("homeserver"),
                           json.loads(worker_file.read_text())["access_token"]))
        except (OSError, ValueError, KeyError):
            pass
    for name, homeserver, token in tokens:
        if not (homeserver and token):
            continue
        try:
            _matrix.request("POST", f"{homeserver}/_matrix/client/v3/logout", {}, token=token)
            say(f"Old {name} device logged out")
        except (_matrix.MatrixError, OSError) as exc:
            say(f"Couldn't log the old {name} device out ({exc}); sign it out in Element")


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


def password_login(user_id, password, homeserver=None):
    """Log in and return the login; exits on failure with nothing saved."""
    if not user_id.startswith("@") or ":" not in user_id:
        sys.exit("A full user ID is needed: @name:server")
    homeserver = homeserver or _matrix.discover_homeserver(user_id.split(":", 1)[1])
    say(f"Homeserver: {homeserver}")
    try:
        if not _matrix.password_login_allowed(homeserver):
            sys.exit("This homeserver doesn't allow password login for bots (no m.login.password).")
        resp = _matrix.login(homeserver, user_id, password(), device_name=BOT_DEVICE_NAME)
    except _matrix.MatrixError as exc:
        sys.exit(f"Login failed: {exc}")
    except OSError as exc:
        sys.exit(f"Can't reach {homeserver}: {exc}")
    return {"homeserver": homeserver, "user_id": resp["user_id"],
            "device_id": resp["device_id"], "access_token": resp["access_token"]}


def save_login(login):
    stored = _matrix.bot_config.load_credentials()
    stored.update(login)
    _matrix.bot_config.save_credentials(stored)
    done(f"Device {login['device_id']}, saved in data/credentials.json")


def switch_account(login, password):
    """Change mode: another account for the bot. Logs in first, so a failed login
    changes nothing."""
    new_id = input(INDENT + "Enter to keep, or another account's user ID: ").strip()
    if not new_id or new_id == login["user_id"]:
        return login
    user_ids(new_id)
    say("The bot's keys and devices belong to its account: the old ones are moved aside and logged")
    say("out, and the next steps set up the new account. Rooms, saved queues and languages stay.")
    if not yes(f"Switch the bot to {new_id}?"):
        return login
    new_login = password_login(new_id, password)
    log_out_old_devices(login)
    move_account_files()
    save_login(new_login)
    return new_login


def step_login(password, change=False):
    print("1. Login")
    login = _matrix.bot_login()
    if login.get("homeserver") and login.get("user_id") and login.get("access_token"):
        try:
            _matrix.request("GET", f"{login['homeserver']}/_matrix/client/v3/account/whoami",
                            token=login["access_token"])
        except _matrix.MatrixError as exc:
            if exc.status != 401:
                sys.exit(f"Checking the saved login failed: {exc}")
            say("The saved token no longer works (device logged out?), logging in again.")
        except OSError as exc:
            sys.exit(f"Can't reach {login['homeserver']}: {exc}")
        else:
            already(f"{'Now' if change else 'Already done'}: {login['user_id']}"
                    f"{', device ' + login['device_id'] if login.get('device_id') else ''}")
            return switch_account(login, password) if change else login
    user_id = login.get("user_id") or ask("Bot's Matrix user ID (e.g. @musicbot:matrix.org): ")
    login = password_login(user_id, password, login.get("homeserver"))
    save_login(login)
    return login


def step_owner(change=False):
    print("2. Room invites")
    stored = _matrix.bot_config.load_credentials()
    if "owners" in stored:
        already(f"{'Now' if change else 'Already done'}: accepted from "
                f"{', '.join(stored['owners']) or 'nobody (skipped)'}")
        if not change:
            return
        overridden = owners_overridden()
        if overridden:
            say(f"{overridden} replaces this; change it there.")
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
    global KEYS_SKIPPED
    had_keys = (_matrix.DATA / "cross_signing_keys.json").exists()
    if had_keys:
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
    elif had_keys:
        already("Skipped: the keys stay as they were")
    else:
        KEYS_SKIPPED = True
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
        print("\nSet up, except the cross-signing keys (skipped)." if KEYS_SKIPPED
              else "\nEverything is set up.")
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

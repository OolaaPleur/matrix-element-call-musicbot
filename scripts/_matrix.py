"""Small Matrix client helpers shared by the setup scripts (standard library only)."""
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
sys.path.insert(0, str(ROOT))
import config as bot_config  # noqa: E402  (load_credentials / save_credentials)

# Cloudflare in front of some homeservers (matrix.org) rejects urllib's default User-Agent.
USER_AGENT = "matrix-element-call-musicbot"


class MatrixError(Exception):
    def __init__(self, status, errcode, message):
        super().__init__(f"HTTP {status} {errcode or ''} {message or ''}".strip())
        self.status, self.errcode = status, errcode


def request(method, url, body=None, token=None, timeout=30):
    headers = {"Content-Type": "application/json", "Accept": "application/json", "User-Agent": USER_AGENT}
    if token:
        headers["Authorization"] = "Bearer " + token
    req = urllib.request.Request(url, method=method, headers=headers,
                                 data=json.dumps(body).encode() if body is not None else None)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.load(resp)
    except urllib.error.HTTPError as exc:
        try:
            err = json.load(exc)
        except ValueError:
            err = {}
        raise MatrixError(exc.code, err.get("errcode"), err.get("error")) from None


def discover_homeserver(server_name):
    """Client API base URL for a server name via .well-known (matrix.org ->
    https://matrix-client.matrix.org); falls back to https://<server_name>."""
    try:
        base = request("GET", f"https://{server_name}/.well-known/matrix/client")["m.homeserver"]["base_url"]
        if base:
            return base.rstrip("/")
    except (MatrixError, urllib.error.URLError, OSError, ValueError, KeyError, TypeError):
        pass
    return f"https://{server_name}"


def password_login_allowed(homeserver):
    flows = request("GET", f"{homeserver}/_matrix/client/v3/login").get("flows", [])
    return any(flow.get("type") == "m.login.password" for flow in flows)


def login(homeserver, user_id, password, device_name=None, device_id=None):
    body = {"type": "m.login.password", "identifier": {"type": "m.id.user", "user": user_id},
            "password": password}
    if device_name:
        body["initial_device_display_name"] = device_name
    if device_id:
        body["device_id"] = device_id
    return request("POST", f"{homeserver}/_matrix/client/v3/login", body)


def config_path():
    """The config the bot reads: CONFIG_FILE, else config.toml, else config/config.toml."""
    env = os.environ.get("CONFIG_FILE")
    candidates = [Path(env)] if env else [Path("config.toml"), Path("config") / "config.toml"]
    for path in candidates:
        path = path if path.is_absolute() else ROOT / path
        if path.is_file():
            return path
    return None


def bot_login():
    """homeserver, user_id, access_token, device_id of the bot, from data/credentials.json."""
    result = dict(bot_config.load_credentials())
    if "homeserver" in result:
        result["homeserver"] = result["homeserver"].rstrip("/")
    return result


def bot_processes():
    """PIDs of this checkout's bot (main.py) or call worker (join_call.js) on
    this machine; a bot in another container isn't visible."""
    pids = []
    for proc in Path("/proc").glob("[0-9]*"):
        try:
            cmdline = (proc / "cmdline").read_bytes().split(b"\0")
            cwd = Path(os.readlink(proc / "cwd"))
        except OSError:
            continue
        if proc.name == str(os.getpid()) or (cwd != ROOT and ROOT not in cwd.parents):
            continue
        if any(arg.endswith((b"main.py", b"join_call.js")) for arg in cmdline):
            pids.append(int(proc.name))
    return pids


def write_private(path, data):
    """Write JSON readable by the owner only; the file never exists with wider permissions."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as handle:
        json.dump(data, handle, indent=2)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)

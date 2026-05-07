import base64
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import aiohttp
from nacl.exceptions import BadSignatureError
from nacl.signing import SigningKey, VerifyKey

logger = logging.getLogger(__name__)

_PENDING_FILE = "cross_signing_pending.json"
_KEYS_FILE = "cross_signing_keys.json"
_UIA_TYPE = "org.matrix.cross_signing_reset"


def _b64(b: bytes) -> str:
    return base64.b64encode(b).rstrip(b"=").decode()


def _unb64(s: str) -> bytes:
    return base64.b64decode(s + "=" * (-len(s) % 4))


def _canonical_json(obj: dict) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":")).encode()


def _sign_obj(sk: SigningKey, obj: dict) -> str:
    return _b64(sk.sign(_canonical_json(obj)).signature)


@dataclass
class CrossSigningKeys:
    master_sk: bytes
    self_signing_sk: bytes
    user_signing_sk: bytes
    master_pub: str
    self_signing_pub: str
    user_signing_pub: str


def generate_keys(user_id: str) -> tuple["CrossSigningKeys", dict]:
    """Generate three Ed25519 keypairs and return (local_keys, upload_payload)."""
    master_sk = SigningKey.generate()
    ss_sk = SigningKey.generate()
    us_sk = SigningKey.generate()

    master_pub = _b64(bytes(master_sk.verify_key))
    ss_pub = _b64(bytes(ss_sk.verify_key))
    us_pub = _b64(bytes(us_sk.verify_key))

    master_obj = {
        "user_id": user_id,
        "usage": ["master"],
        "keys": {f"ed25519:{master_pub}": master_pub},
    }
    ss_obj = {
        "user_id": user_id,
        "usage": ["self_signing"],
        "keys": {f"ed25519:{ss_pub}": ss_pub},
    }
    us_obj = {
        "user_id": user_id,
        "usage": ["user_signing"],
        "keys": {f"ed25519:{us_pub}": us_pub},
    }

    ss_obj["signatures"] = {user_id: {f"ed25519:{master_pub}": _sign_obj(master_sk, ss_obj)}}
    us_obj["signatures"] = {user_id: {f"ed25519:{master_pub}": _sign_obj(master_sk, us_obj)}}

    local = CrossSigningKeys(
        master_sk=bytes(master_sk),
        self_signing_sk=bytes(ss_sk),
        user_signing_sk=bytes(us_sk),
        master_pub=master_pub,
        self_signing_pub=ss_pub,
        user_signing_pub=us_pub,
    )
    payload = {"master_key": master_obj, "self_signing_key": ss_obj, "user_signing_key": us_obj}
    return local, payload


def load_local_keys(data_dir: Path) -> Optional[CrossSigningKeys]:
    path = data_dir / _KEYS_FILE
    if not path.exists():
        return None
    try:
        d = json.loads(path.read_text())
        return CrossSigningKeys(
            master_sk=_unb64(d["master_sk"]),
            self_signing_sk=_unb64(d["self_signing_sk"]),
            user_signing_sk=_unb64(d["user_signing_sk"]),
            master_pub=d["master_pub"],
            self_signing_pub=d["self_signing_pub"],
            user_signing_pub=d["user_signing_pub"],
        )
    except Exception as exc:
        logger.error("Cross-signing: failed to load local keys: %s", exc)
        return None


def save_local_keys(data_dir: Path, keys: CrossSigningKeys) -> None:
    path = data_dir / _KEYS_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "master_sk": _b64(keys.master_sk),
        "self_signing_sk": _b64(keys.self_signing_sk),
        "user_signing_sk": _b64(keys.user_signing_sk),
        "master_pub": keys.master_pub,
        "self_signing_pub": keys.self_signing_pub,
        "user_signing_pub": keys.user_signing_pub,
    }, indent=2))
    path.chmod(0o600)
    logger.info("Cross-signing: private keys saved to %s", path)


def load_pending(data_dir: Path) -> Optional[dict]:
    path = data_dir / _PENDING_FILE
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def save_pending(data_dir: Path, session_id: str, payload: dict, keys: CrossSigningKeys) -> None:
    path = data_dir / _PENDING_FILE
    path.write_text(json.dumps({
        "session_id": session_id,
        "payload": payload,
        "master_sk": _b64(keys.master_sk),
        "self_signing_sk": _b64(keys.self_signing_sk),
        "user_signing_sk": _b64(keys.user_signing_sk),
        "master_pub": keys.master_pub,
        "self_signing_pub": keys.self_signing_pub,
        "user_signing_pub": keys.user_signing_pub,
    }, indent=2))
    path.chmod(0o600)


def clear_pending(data_dir: Path) -> None:
    (data_dir / _PENDING_FILE).unlink(missing_ok=True)


def _keys_from_pending(p: dict) -> CrossSigningKeys:
    return CrossSigningKeys(
        master_sk=_unb64(p["master_sk"]),
        self_signing_sk=_unb64(p["self_signing_sk"]),
        user_signing_sk=_unb64(p["user_signing_sk"]),
        master_pub=p["master_pub"],
        self_signing_pub=p["self_signing_pub"],
        user_signing_pub=p["user_signing_pub"],
    )


async def _query_server(homeserver: str, token: str, user_id: str) -> dict:
    async with aiohttp.ClientSession() as s:
        async with s.post(
            f"{homeserver}/_matrix/client/v3/keys/query",
            json={"device_keys": {user_id: []}},
            headers={"Authorization": f"Bearer {token}"},
        ) as r:
            return await r.json()


async def _upload_signing_keys(
    homeserver: str, token: str, payload: dict, auth: Optional[dict] = None
) -> tuple[bool, Optional[str], Optional[str]]:
    """Returns (success, session_id, approval_url)."""
    body = dict(payload)
    if auth:
        body["auth"] = auth
    async with aiohttp.ClientSession() as s:
        async with s.post(
            f"{homeserver}/_matrix/client/v3/keys/device_signing/upload",
            json=body,
            headers={"Authorization": f"Bearer {token}"},
        ) as r:
            data = await r.json()
            if r.status == 200:
                return True, None, None
            if r.status == 401:
                session_id = data.get("session")
                url = None
                for v in data.get("params", {}).values():
                    if isinstance(v, dict) and "url" in v:
                        url = v["url"]
                        break
                if not url:
                    url = data.get("msg", "")
                return False, session_id, url
            logger.error("Cross-signing upload unexpected response %s: %s", r.status, data)
            return False, None, None


def _verify_device_sig(server_data: dict, user_id: str, device_id: str, ss_pub: str) -> bool:
    device = server_data.get("device_keys", {}).get(user_id, {}).get(device_id)
    if not device:
        return False
    sig_key_id = f"ed25519:{ss_pub}"
    sig_b64 = device.get("signatures", {}).get(user_id, {}).get(sig_key_id)
    if not sig_b64:
        return False
    to_verify = {k: v for k, v in device.items() if k not in ("signatures", "unsigned")}
    try:
        VerifyKey(_unb64(ss_pub)).verify(_canonical_json(to_verify), _unb64(sig_b64))
        return True
    except (BadSignatureError, Exception):
        return False


async def _sign_device(
    homeserver: str,
    token: str,
    user_id: str,
    device_id: str,
    ss_sk_bytes: bytes,
    server_data: dict,
) -> bool:
    ss_sk = SigningKey(ss_sk_bytes)
    ss_pub = _b64(bytes(ss_sk.verify_key))

    device = server_data.get("device_keys", {}).get(user_id, {}).get(device_id)
    if not device:
        logger.error("Cross-signing: device %s not found in server keys query", device_id)
        return False

    to_sign = {k: v for k, v in device.items() if k not in ("signatures", "unsigned")}
    new_sig = _sign_obj(ss_sk, to_sign)

    existing = device.get("signatures", {}).get(user_id, {})
    signed_device = {
        **device,
        "signatures": {user_id: {**existing, f"ed25519:{ss_pub}": new_sig}},
    }

    async with aiohttp.ClientSession() as s:
        async with s.post(
            f"{homeserver}/_matrix/client/v3/keys/signatures/upload",
            json={user_id: {device_id: signed_device}},
            headers={"Authorization": f"Bearer {token}"},
        ) as r:
            data = await r.json()
            if r.status == 200 and not data.get("failures"):
                return True
            logger.error("Cross-signing: signature upload failed %s: %s", r.status, data)
            return False


async def ensure_cross_signing(
    homeserver: str,
    token: str,
    user_id: str,
    device_id: str,
    data_dir: Path,
) -> None:
    """
    Called once after keys_upload during bot startup.
    Ensures cross-signing keys exist and current device is validly signed.
    """
    data_dir.mkdir(parents=True, exist_ok=True)

    # --- Handle pending approval from a previous startup ---
    pending = load_pending(data_dir)
    if pending:
        logger.info("Cross-signing: pending approval found — attempting UIA completion")
        pending_keys = _keys_from_pending(pending)
        auth = {"type": _UIA_TYPE, "session": pending["session_id"]}
        ok, _, _ = await _upload_signing_keys(homeserver, token, pending["payload"], auth=auth)
        if ok:
            logger.info("Cross-signing: approval accepted, keys uploaded")
            save_local_keys(data_dir, pending_keys)
            clear_pending(data_dir)
            server_data = await _query_server(homeserver, token, user_id)
            if await _sign_device(homeserver, token, user_id, device_id, pending_keys.self_signing_sk, server_data):
                logger.info("Cross-signing: device %s signed successfully", device_id)
            return
        else:
            logger.warning(
                "Cross-signing: approval not yet confirmed. Please visit the URL below and restart bot.\n"
                "  → https://account.matrix.org/account/?action=org.matrix.cross_signing_reset\n"
                "  Log in as the BOT account: %s",
                user_id,
            )
            return

    # --- Query server ---
    try:
        server_data = await _query_server(homeserver, token, user_id)
    except Exception as exc:
        logger.warning("Cross-signing: server query failed: %s", exc)
        return

    mk = server_data.get("master_keys", {})
    ssk = server_data.get("self_signing_keys", {})
    usk = server_data.get("user_signing_keys", {})
    server_has_keys = user_id in mk and user_id in ssk and user_id in usk

    local_keys = load_local_keys(data_dir)

    # --- No keys on server: generate fresh ---
    if not server_has_keys:
        logger.info("Cross-signing: no keys on server, generating new keypairs and initiating upload")
        new_keys, payload = generate_keys(user_id)
        ok, session_id, approval_url = await _upload_signing_keys(homeserver, token, payload)
        if ok:
            save_local_keys(data_dir, new_keys)
            server_data = await _query_server(homeserver, token, user_id)
            if await _sign_device(homeserver, token, user_id, device_id, new_keys.self_signing_sk, server_data):
                logger.info("Cross-signing: fully set up, device %s signed", device_id)
        elif session_id is not None:
            save_pending(data_dir, session_id, payload, new_keys)
            logger.warning(
                "Cross-signing: server requires account approval.\n"
                "  → Visit: https://account.matrix.org/account/?action=org.matrix.cross_signing_reset\n"
                "  Log in as the BOT account: %s\n"
                "  Then restart the bot to complete setup.",
                user_id,
            )
        else:
            logger.error("Cross-signing: upload failed (no session returned), skipping")
        return

    # --- Keys on server, no local private keys ---
    if local_keys is None:
        ss_pub_list = list(ssk.get(user_id, {}).get("keys", {}).values())
        ss_pub = ss_pub_list[0] if ss_pub_list else None
        if ss_pub and _verify_device_sig(server_data, user_id, device_id, ss_pub):
            logger.info(
                "Cross-signing: keys on server, no local private keys, "
                "but device %s is already validly signed. Cross-signing functional.",
                device_id,
            )
            return

        # Signature invalid or missing — initiate reset to upload fresh keypairs
        logger.info(
            "Cross-signing: server has stale/invalid cross-signing keys "
            "(no local private keys, device %s not validly signed). Initiating reset.",
            device_id,
        )
        new_keys, payload = generate_keys(user_id)
        ok, session_id, _ = await _upload_signing_keys(homeserver, token, payload)
        if ok:
            save_local_keys(data_dir, new_keys)
            server_data = await _query_server(homeserver, token, user_id)
            if await _sign_device(homeserver, token, user_id, device_id, new_keys.self_signing_sk, server_data):
                logger.info("Cross-signing: fully set up, device %s signed", device_id)
        elif session_id is not None:
            save_pending(data_dir, session_id, payload, new_keys)
            logger.warning(
                "Cross-signing: server requires account approval to reset.\n"
                "  → Visit: https://account.matrix.org/account/?action=org.matrix.cross_signing_reset\n"
                "  Log in as the BOT account: %s\n"
                "  Then restart the bot to complete setup.",
                user_id,
            )
        else:
            logger.error("Cross-signing: reset upload failed unexpectedly, skipping")
        return

    # --- Keys on server, have local private keys ---
    if _verify_device_sig(server_data, user_id, device_id, local_keys.self_signing_pub):
        logger.info("Cross-signing: device %s already validly signed, nothing to do", device_id)
        return

    logger.info("Cross-signing: signing device %s", device_id)
    if await _sign_device(homeserver, token, user_id, device_id, local_keys.self_signing_sk, server_data):
        logger.info("Cross-signing: device %s signed and signature uploaded successfully", device_id)
    else:
        logger.error("Cross-signing: failed to sign device %s", device_id)

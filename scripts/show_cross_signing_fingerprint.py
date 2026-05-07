#!/usr/bin/env python3
"""Print bot's cross-signing master key fingerprint for manual verification in Element."""
import argparse
import asyncio
import base64
import json
import sys
from pathlib import Path

try:
    from nacl.signing import SigningKey
except ImportError:
    print("ERROR: PyNaCl not installed. Run: pip install pynacl", file=sys.stderr)
    sys.exit(1)

KEYS_PATH = Path(__file__).resolve().parent.parent / "data" / "cross_signing_keys.json"
CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "config.toml"


def _unb64(s: str) -> bytes:
    return base64.b64decode(s + "=" * (-len(s) % 4))


def _load_keys() -> dict:
    if not KEYS_PATH.exists():
        print(f"ERROR: {KEYS_PATH} not found. Has cross-signing been bootstrapped?", file=sys.stderr)
        sys.exit(1)
    return json.loads(KEYS_PATH.read_text())


def _read_config() -> tuple[str, str, str]:
    """Return (homeserver, user_id, access_token) from config.toml."""
    try:
        import tomllib  # Python 3.11+
    except ImportError:
        try:
            import tomli as tomllib
        except ImportError:
            return ("(install tomllib/tomli to read config)", "(unknown)", "(unknown)")
    try:
        data = tomllib.loads(CONFIG_PATH.read_text())
        m = data.get("matrix", {})
        return m.get("homeserver", ""), m.get("user_id", ""), m.get("access_token", "")
    except Exception as e:
        return ("", f"(config read error: {e})", "")


async def _check_server(homeserver: str, token: str, user_id: str, expected_pub: str) -> None:
    try:
        import aiohttp
    except ImportError:
        print("  aiohttp not available, skipping server check", file=sys.stderr)
        return

    async with aiohttp.ClientSession() as s:
        async with s.post(
            f"{homeserver}/_matrix/client/v3/keys/query",
            json={"device_keys": {user_id: []}},
            headers={"Authorization": f"Bearer {token}"},
        ) as r:
            status = r.status
            data = await r.json()

    if status == 401:
        print(f"  Skipped: access token rejected (M_UNKNOWN_TOKEN / soft_logout).")
        print("  Start the bot first, then copy the live token from config to check.")
        return
    if status != 200:
        print(f"  WARNING: server returned {status}: {data}")
        return

    mk = data.get("master_keys", {}).get(user_id)
    if not mk:
        print("  WARNING: no master key found on server for this user_id")
        return

    server_pub_values = list(mk.get("keys", {}).values())
    if not server_pub_values:
        print("  WARNING: master key on server has no 'keys' entries")
        return

    server_pub = server_pub_values[0]
    if server_pub == expected_pub:
        print("  ✓ Local key matches server")
    else:
        print(f"  WARNING: mismatch! Server has: {server_pub}")
        print("  This means the bot's local keys are stale. Re-run bootstrap.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Show bot cross-signing master key fingerprint")
    parser.add_argument("--check-server", action="store_true", help="Verify key matches server")
    args = parser.parse_args()

    keys = _load_keys()
    master_sk_bytes = _unb64(keys["master_sk"])
    sk = SigningKey(master_sk_bytes)
    master_pub_bytes = bytes(sk.verify_key)

    pub_b64 = base64.b64encode(master_pub_bytes).rstrip(b"=").decode("ascii")
    pub_hex_colons = ":".join(f"{b:02X}" for b in master_pub_bytes)
    grouped = " ".join(pub_b64[i:i+4] for i in range(0, len(pub_b64), 4))

    homeserver, user_id, token = _read_config()
    print(f"Bot user_id:              {user_id}")
    print(f"Master key (base64):      {pub_b64}")
    print(f"Master key (grouped):     {grouped}")
    print(f"Master key (hex):         {pub_hex_colons}")

    # Sanity-check: master_pub stored in file should match what we derived from private key
    stored_pub = keys.get("master_pub", "")
    if stored_pub and stored_pub != pub_b64:
        print(f"\nWARNING: stored master_pub field ({stored_pub}) differs from derived public key!")
        print("Keys file may be corrupt.")
    else:
        print("\n✓ Derived public key matches stored master_pub field")

    if args.check_server:
        if not homeserver or not token or not user_id:
            print("\nSkipping server check: config not fully readable")
        else:
            print("\nChecking server...")
            asyncio.run(_check_server(homeserver, token, user_id, pub_b64))


if __name__ == "__main__":
    main()

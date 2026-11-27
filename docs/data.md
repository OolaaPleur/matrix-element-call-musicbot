# Data, backups and the access token

[← README](../README.md)

Everything the bot keeps lives in `data/` (the `musicbot_data` volume with Docker). Back it up; the crypto files can't be recreated without side effects:

| File | What it is | If it's lost |
|---|---|---|
| `crypto_store/` | The bot device's E2EE keys (matrix-nio) | The bot can't decrypt older messages; log in again as a new device |
| `credentials.json` | The bot's login (homeserver, user ID, device, access token), from `setup.py` | Run `setup.py` again: a new device, see `crypto_store/` |
| `cross_signing_keys.json` | Private cross-signing keys | New devices stay unsigned until you restore the keys (recovery key) or replace them; see [e2ee.md](e2ee.md#cross-signing-keys) |
| `device_id` | The bot's device ID | Used by `rotate_access_token.py` and as a fallback at start |
| `worker_device.json`, `worker_crypto_store.v8` | The call worker's device and its E2EE keys | No call audio E2EE until `create_worker_device.py --replace` |
| `saved_queues.json`, `room_languages.json` | Saved queues, per-room languages | Gone, nothing else breaks |

Downloaded tracks go to `cache/audio` (`paths.audio_dir`; the `musicbot_cache` volume with Docker). They are kept across restarts and re-downloadable, so leave them out of backups.

Back up while the bot is stopped: `crypto_store/` is an SQLite database, a copy taken mid-write may not open. Restoring one device's crypto store into a second running copy of the bot breaks E2EE for both: never run two copies with the same access token.

## Rotating the access token

`scripts/rotate_access_token.py` logs in with the password and the same device ID, so the old token stops working while the device and its keys stay. It saves the new token in `data/credentials.json` (never prints it); restart the bot afterwards (`--restart` restarts the `matrix-musicbot` systemd user unit).

With Docker:

```bash
docker compose run --rm musicbot python3 scripts/rotate_access_token.py
docker compose restart
```

# Encryption details

[← README](../README.md)

## Room messages

Matrix room E2EE is fully supported: the bot (matrix-nio) decrypts encrypted commands and encrypts its own messages. It signs its own device with the account's cross-signing keys (`data/cross_signing_keys.json`) on every start, so its messages don't show "encrypted by a device not verified by its owner".

The bot does not subscribe to other participants' tracks (`autoSubscribe: false`), so it never receives anyone's voice.

## Cross-signing keys

`scripts/setup.py` restores the keys with the account's recovery key, or leaves them to the bot on a fresh account (see [Cross-Signing](../README.md#cross-signing)).

To restore them later (skipped in setup, lost `data/`), run this in the bot's directory and restart the bot:

```bash
python3 -m matrix_bot_common.restore_keys
docker compose exec musicbot python3 -m matrix_bot_common.restore_keys   # Docker
```

**No recovery key?** The bot can replace the account's keys, but only when asked to: start it once with `CROSS_SIGNING_ALLOW_RESET=1` (Docker: add it under `environment:` in `docker-compose.yml`, remove it afterwards). On homeservers with OAuth login (matrix.org) the log then shows a link: open it, log in as the bot, approve the reset, and restart the bot. Everyone who verified the bot has to verify it again.

**Verifying without Element access to the bot's account:** `python3 scripts/show_cross_signing_fingerprint.py` (Docker: `docker compose exec musicbot python3 scripts/show_cross_signing_fingerprint.py`) prints the bot's master key to compare with the one Element shows (**Verify manually**, where your Element version offers it).

## Call audio E2EE

The call worker can encrypt the music like Element Call does (LiveKit SFrame, per-participant keys exchanged over Olm). `[worker] e2ee`:

- `"auto"` (default): on once the worker device exists, silently off before that;
- `true`: on, and says so in the room when it can't be;
- `false`: off.

It needs two things.

### 1. A device of its own

The keys travel as Olm-encrypted to-device messages, and the bot's own device belongs to matrix-nio, so the worker logs in as a second device of the same account. `scripts/setup.py` creates it (`data/worker_device.json`, mode 600). To create it later or replace it, stop the bot and run:

```bash
systemctl --user stop matrix-musicbot      # the script refuses while the bot runs
scripts/create_worker_device.py            # --replace for a new one
```

With Docker:

```bash
docker compose stop musicbot
docker compose run --rm musicbot python3 scripts/create_worker_device.py
docker compose up -d
```

Its Olm identity is kept in `data/worker_crypto_store.v8` (matrix-js-sdk rust crypto on [fake-indexeddb](https://github.com/dumbmatter/fakeIndexedDB), snapshotted to disk; no native modules). The worker cross-signs it with `data/cross_signing_keys.json` on start.

### 2. A fix in `@livekit/rtc-node`

Released versions (1.2.x) can't take a key from the application ([livekit/node-sdks#768](https://github.com/livekit/node-sdks/pull/768)). The Docker image applies the fix at build time; on the host, apply it after every `bun install`:

```bash
python3 call_worker/patches/apply-pr768.py
```

The script patches the installed `dist/` files, is idempotent, and fails loudly on an rtc-node version it doesn't know. Drop it once a release contains #768.

The worker also moves its own frame cryptors to each new key index by hand (`FrameCryptor.setKeyIndex`), working around [livekit/rust-sdks#1676](https://github.com/livekit/rust-sdks/issues/1676) until a release ships [#1677](https://github.com/livekit/rust-sdks/pull/1677). `WORKER_E2EE_SET_KEY_INDEX=false` turns that off.

### "e2ee OFF, playing unencrypted"

If E2EE is on but can't work, the worker logs why, says so in the room (with `"auto"`, a missing worker device just means E2EE stays off, without a notice) and plays unencrypted:

| Reason | Fix |
|---|---|
| no `data/worker_device.json` | run `scripts/create_worker_device.py` |
| rtc-node not patched | `python3 call_worker/patches/apply-pr768.py` after `bun install` |
| device token rejected (401) or belongs to another device | `scripts/create_worker_device.py --replace` |
| crypto store unreadable (`worker_crypto_store.v8.unreadable` exists) | `scripts/create_worker_device.py --replace` |

A homeserver that is merely unreachable doesn't switch E2EE off: the worker start fails and is retried.

#!/usr/bin/env python3
"""Apply livekit/node-sdks PR #768 (commit 593b012f) to an installed @livekit/rtc-node 1.2.2.

Patches the shipped build output (dist/e2ee.cjs + type declarations); the
shipped src/*.ts is not used at runtime and is left alone. Idempotent:
a file that already carries the change is left alone. Exits non-zero if any
expected snippet is missing (i.e. a different rtc-node version).
Usage: apply-pr768.py [path/to/node_modules/@livekit/rtc-node]
"""
import pathlib
import sys

root = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else
                    pathlib.Path(__file__).resolve().parent.parent / "node_modules/@livekit/rtc-node")

CJS = [
    ("  setKey(participantIdentity, keyIndex) {",
     "  setKey(participantIdentity, key, keyIndex) {"),
    ("        value: new import_rtc_ffi_bindings.SetKeyRequest({\n          keyIndex,\n          participantIdentity\n        })",
     "        value: new import_rtc_ffi_bindings.SetKeyRequest({\n          keyIndex,\n          participantIdentity,\n          key\n        })"),
    ("  constructor(roomHandle, participantIdentity, keyIndex, enabled) {\n    this.roomHandle = BigInt(0);\n    this.roomHandle = roomHandle;\n    this.participantIdentity = participantIdentity;\n",
     "  constructor(roomHandle, participantIdentity, trackSid, keyIndex, enabled) {\n    this.roomHandle = BigInt(0);\n    this.roomHandle = roomHandle;\n    this.participantIdentity = participantIdentity;\n    this.trackSid = trackSid;\n"),
    ("        value: new import_rtc_ffi_bindings.FrameCryptorSetEnabledRequest({\n          participantIdentity: this.participantIdentity,\n",
     "        value: new import_rtc_ffi_bindings.FrameCryptorSetEnabledRequest({\n          participantIdentity: this.participantIdentity,\n          trackSid: this.trackSid,\n"),
    ("        value: new import_rtc_ffi_bindings.FrameCryptorSetKeyIndexRequest({\n          participantIdentity: this.participantIdentity,\n",
     "        value: new import_rtc_ffi_bindings.FrameCryptorSetKeyIndexRequest({\n          participantIdentity: this.participantIdentity,\n          trackSid: this.trackSid,\n"),
    ("        cryptor.participantIdentity,\n        cryptor.keyIndex,",
     "        cryptor.participantIdentity,\n        cryptor.trackSid,\n        cryptor.keyIndex,"),
]
DTS = [
    ("setKey(participantIdentity: string, keyIndex: number): void;",
     "setKey(participantIdentity: string, key: Uint8Array, keyIndex: number): void;"),
    ("constructor(roomHandle: bigint, participantIdentity: string, keyIndex: number, enabled: boolean);",
     "constructor(roomHandle: bigint, participantIdentity: string, trackSid: string, keyIndex: number, enabled: boolean);"),
]
TS = [
    ("  setKey(participantIdentity: string, keyIndex: number) {",
     "  setKey(participantIdentity: string, key: Uint8Array, keyIndex: number) {"),
    ("          participantIdentity: participantIdentity,\n        }),",
     "          participantIdentity: participantIdentity,\n          key: key,\n        }),"),
]

def patch(path, pairs, optional=False):
    if not path.exists():
        if optional:
            return True
        print(f"missing {path}"); return False
    text = path.read_text()
    ok = True
    for old, new in pairs:
        if new in text:
            continue
        if text.count(old) != 1:
            print(f"{path.name}: snippet not found exactly once: {old[:60]!r}"); ok = False; continue
        text = text.replace(old, new)
    if ok:
        path.write_text(text)
        print(f"patched {path.relative_to(root)}")
    return ok

dist = root / "dist"
results = [patch(dist / "e2ee.cjs", CJS), patch(dist / "e2ee.d.ts", DTS), patch(dist / "e2ee.d.cts", DTS)]
sys.exit(0 if all(results) else 1)

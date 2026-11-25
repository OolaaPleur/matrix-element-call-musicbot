// The worker's rust crypto keeps its device identity across processes.
//   node test/crypto-store.test.cjs        (no network: crypto init only)
"use strict";

const assert = require("assert");
const crypto = require("crypto");
const fs = require("fs");
const os = require("os");
const path = require("path");
const { execFileSync } = require("child_process");

const dir = fs.mkdtempSync(path.join(os.tmpdir(), "crypto-store-test-"));
const file = path.join(dir, "store.v8");
const storeKey = crypto.randomBytes(32).toString("hex");

if (process.argv[2] === "--child") {
    const { PersistentCryptoStore } = require("../src/crypto-store");
    const { createClient, MemoryStore } = require("matrix-js-sdk");
    const { logger } = require("matrix-js-sdk/lib/logger");
    logger.setLevel("ERROR");
    (async () => {
        const [, , , childFile, key, deviceId] = process.argv;
        const store = new PersistentCryptoStore(childFile, (l) => console.error(l), deviceId);
        let restored;
        try {
            restored = await store.load();
        } catch (e) {
            console.log(JSON.stringify({ error: e.constructor.name }));
            process.exit(0);
        }
        const client = createClient({
            baseUrl: "http://127.0.0.1:9", userId: "@t:example.org", deviceId,
            accessToken: "x", store: new MemoryStore(),
        });
        await client.initRustCrypto({ useIndexedDB: true, cryptoDatabasePrefix: "worker", storageKey: Buffer.from(key, "hex") });
        const keys = await client.getCrypto().getOwnDeviceKeys();
        await store.flush();
        console.log(JSON.stringify({ restored, ed25519: keys.ed25519 }));
        process.exit(0);
    })().catch((e) => { console.error(e); process.exit(1); });
    return;
}

const run = (deviceId = "TESTDEVICE") =>
    JSON.parse(execFileSync(process.execPath, [__filename, "--child", file, storeKey, deviceId], { encoding: "utf-8", stdio: ["ignore", "pipe", "ignore"] }).trim().split("\n").pop());
try {
    const first = run();
    assert.strictEqual(first.restored, 0);
    assert.ok(fs.existsSync(file), "snapshot written");
    assert.strictEqual(fs.statSync(file).mode & 0o777, 0o600);
    const second = run();
    assert.ok(second.restored >= 1, "databases restored");
    assert.strictEqual(second.ed25519, first.ed25519, "same device identity after restart");
    console.log(`ok   identity kept across restarts (${second.restored} databases, ${fs.statSync(file).size} bytes)`);

    const other = run("OTHERDEVICE");
    assert.strictEqual(other.restored, 0, "another device's snapshot is not restored");
    assert.notStrictEqual(other.ed25519, first.ed25519);
    assert.ok(fs.existsSync(`${file}.device-TESTDEVICE`), "old snapshot kept aside");
    console.log("ok   snapshot of another device moved aside, fresh store");

    fs.writeFileSync(file, "garbage");
    assert.strictEqual(run("OTHERDEVICE").error, "UnreadableCryptoStoreError");
    assert.ok(fs.existsSync(`${file}.unreadable`) && !fs.existsSync(file), "moved to .unreadable");
    assert.strictEqual(run("OTHERDEVICE").error, "UnreadableCryptoStoreError", "keeps refusing");
    console.log("ok   unreadable snapshot moved aside, store refused until the device is replaced");
} finally {
    fs.rmSync(dir, { recursive: true, force: true });
}

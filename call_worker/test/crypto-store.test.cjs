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
        const [, , , childFile, key] = process.argv;
        const store = new PersistentCryptoStore(childFile, (l) => console.error(l));
        const restored = await store.load();
        const client = createClient({
            baseUrl: "http://127.0.0.1:9", userId: "@t:example.org", deviceId: "TESTDEVICE",
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

const run = () => JSON.parse(execFileSync(process.execPath, [__filename, "--child", file, storeKey], { encoding: "utf-8" }).trim().split("\n").pop());
try {
    const first = run();
    assert.strictEqual(first.restored, 0);
    assert.ok(fs.existsSync(file), "snapshot written");
    assert.strictEqual(fs.statSync(file).mode & 0o777, 0o600);
    const second = run();
    assert.ok(second.restored >= 1, "databases restored");
    assert.strictEqual(second.ed25519, first.ed25519, "same device identity after restart");
    console.log(`ok   identity kept across restarts (${second.restored} databases, ${fs.statSync(file).size} bytes)`);
} finally {
    fs.rmSync(dir, { recursive: true, force: true });
}

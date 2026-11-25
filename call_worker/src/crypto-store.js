// Persistent IndexedDB for matrix-js-sdk's rust crypto, without native modules.
//
// The worker's Matrix device keeps its Olm identity across runs, so its crypto
// store must survive restarts. matrix-sdk-crypto-wasm persists into IndexedDB;
// Node has none, so we use fake-indexeddb (pure JS, in memory) and snapshot all
// its databases to one file: loaded before initRustCrypto, written shortly after
// every committed readwrite transaction and on shutdown.
//
// The rust store encrypts its values with the storageKey we pass to
// initRustCrypto; the snapshot file is additionally written 0600.
//
// The snapshot records the device it belongs to. A snapshot of another device
// (the device was replaced) is moved aside and a fresh store started. An
// unreadable one is moved to <file>.unreadable and load() fails until the device
// is replaced: a fresh Olm account under the same device id would clash with
// the keys the server already has for it.

"use strict";

const fs = require("fs");
const path = require("path");
const v8 = require("v8");

require("fake-indexeddb/auto");

const SNAPSHOT_FORMAT = 1;
const SAVE_DELAY_MS = 1000;

function requestToPromise(request) {
    return new Promise((resolve, reject) => {
        request.onsuccess = () => resolve(request.result);
        request.onerror = () => reject(request.error);
    });
}

function transactionDone(tx) {
    return new Promise((resolve, reject) => {
        tx.oncomplete = () => resolve();
        tx.onabort = () => reject(tx.error || new Error("transaction aborted"));
        tx.onerror = () => reject(tx.error);
    });
}

async function dumpDatabase(name) {
    const db = await requestToPromise(indexedDB.open(name));
    try {
        const storeNames = Array.from(db.objectStoreNames);
        const stores = [];
        if (storeNames.length) {
            const tx = db.transaction(storeNames, "readonly");
            const done = transactionDone(tx);
            const reads = storeNames.map(async (storeName) => {
                const store = tx.objectStore(storeName);
                const indexes = Array.from(store.indexNames).map((indexName) => {
                    const index = store.index(indexName);
                    return { name: indexName, keyPath: index.keyPath, unique: index.unique, multiEntry: index.multiEntry };
                });
                // Both lists are in key order, so they pair up.
                const [keys, values] = await Promise.all([
                    requestToPromise(store.getAllKeys()),
                    requestToPromise(store.getAll()),
                ]);
                stores.push({
                    name: storeName,
                    keyPath: store.keyPath,
                    autoIncrement: store.autoIncrement,
                    indexes,
                    records: keys.map((key, i) => [key, values[i]]),
                });
            });
            await Promise.all([...reads, done]);
        }
        return { name, version: db.version, stores };
    } finally {
        db.close();
    }
}

async function restoreDatabase({ name, version, stores }) {
    const request = indexedDB.open(name, version);
    request.onupgradeneeded = () => {
        const db = request.result;
        for (const s of stores) {
            const store = db.createObjectStore(s.name, { keyPath: s.keyPath, autoIncrement: s.autoIncrement });
            for (const ix of s.indexes) {
                store.createIndex(ix.name, ix.keyPath, { unique: ix.unique, multiEntry: ix.multiEntry });
            }
            for (const [key, value] of s.records) {
                if (s.keyPath === null) store.put(value, key);
                else store.put(value);
            }
        }
    };
    const db = await requestToPromise(request);
    db.close();
}

class UnreadableCryptoStoreError extends Error {}

class PersistentCryptoStore {
    constructor(file, logLine, deviceId) {
        this.file = file;
        this.logLine = logLine;
        this.deviceId = deviceId;
        this.timer = null;
        this.saving = null;
        this.dirty = false;
        this.closed = false;
    }

    /** Load the snapshot (if any) and start watching for writes. Returns the number of databases restored. */
    async load() {
        const unreadable = `${this.file}.unreadable`;
        if (fs.existsSync(unreadable)) {
            throw new UnreadableCryptoStoreError(`${unreadable} exists (an earlier snapshot could not be read)`);
        }
        let restored = 0;
        const snapshot = this.readSnapshot(unreadable);
        if (snapshot && snapshot.deviceId !== this.deviceId) {
            const aside = `${this.file}.device-${snapshot.deviceId}`;
            fs.renameSync(this.file, aside);
            this.logLine(`crypto store: snapshot is for device ${snapshot.deviceId}, not ${this.deviceId}; moved to ${aside}, starting fresh`);
        } else if (snapshot) {
            for (const database of snapshot.databases) {
                await restoreDatabase(database);
                restored += 1;
            }
        }
        this.watchWrites();
        return restored;
    }

    readSnapshot(unreadable) {
        if (!fs.existsSync(this.file)) return null;
        let snapshot;
        try {
            snapshot = v8.deserialize(fs.readFileSync(this.file));
        } catch (err) {
            snapshot = { error: err instanceof Error ? err.message : String(err) };
        }
        if (snapshot?.format !== SNAPSHOT_FORMAT || !Array.isArray(snapshot.databases) || !snapshot.deviceId) {
            fs.renameSync(this.file, unreadable);
            throw new UnreadableCryptoStoreError(
                `cannot read crypto store snapshot ${this.file} (${snapshot?.error || "unknown format"}); moved to ${unreadable}`,
            );
        }
        return snapshot;
    }

    watchWrites() {
        const store = this;
        const original = IDBDatabase.prototype.transaction;
        IDBDatabase.prototype.transaction = function transaction(storeNames, mode, options) {
            const tx = original.call(this, storeNames, mode, options);
            if (mode === "readwrite") {
                tx.addEventListener("complete", () => store.scheduleSave());
            }
            return tx;
        };
    }

    scheduleSave() {
        if (this.closed) return;
        this.dirty = true;
        if (this.timer) return;
        this.timer = setTimeout(() => {
            this.timer = null;
            void this.save().catch((err) => {
                this.logLine(`crypto store: snapshot FAILED ${err instanceof Error ? err.message : String(err)}`);
            });
        }, SAVE_DELAY_MS);
    }

    async save() {
        // One snapshot at a time; a write during a snapshot schedules another.
        while (this.saving) await this.saving;
        if (!this.dirty) return;
        this.dirty = false;
        this.saving = this.writeSnapshot().finally(() => {
            this.saving = null;
        });
        await this.saving;
    }

    async writeSnapshot() {
        const infos = await indexedDB.databases();
        const databases = [];
        for (const { name } of infos) databases.push(await dumpDatabase(name));
        const data = v8.serialize({ format: SNAPSHOT_FORMAT, deviceId: this.deviceId, savedAt: Date.now(), databases });

        const dir = path.dirname(this.file);
        fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
        const tmp = `${this.file}.tmp`;
        const fd = fs.openSync(tmp, "w", 0o600);
        try {
            fs.writeSync(fd, data);
            fs.fsyncSync(fd);
        } finally {
            fs.closeSync(fd);
        }
        fs.renameSync(tmp, this.file);
        const dirFd = fs.openSync(dir, "r");
        try {
            fs.fsyncSync(dirFd);
        } finally {
            fs.closeSync(dirFd);
        }
    }

    /** Write pending changes now (shutdown). */
    async flush() {
        if (this.timer) {
            clearTimeout(this.timer);
            this.timer = null;
        }
        do {
            await this.save();
        } while (this.dirty);
        this.closed = true;
    }
}

module.exports = { PersistentCryptoStore, UnreadableCryptoStoreError };

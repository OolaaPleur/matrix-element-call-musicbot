// LiveKit media E2EE for the call worker, keyed by matrix-js-sdk's MatrixRTC
// encryption manager (per-participant keys sent over Olm-encrypted to-device
// messages, as Element Call does).
//
// Needs @livekit/rtc-node with livekit/node-sdks#768 (KeyProvider.setKey takes
// the key); see patches/apply-pr768.py.

const crypto = require("crypto");
const fs = require("fs");
const { EncryptionType, FrameCryptor, KeyProvider } = require("@livekit/rtc-node");
// rtc-node does not re-export this enum; the FFI bindings package does.
const { KeyDerivationFunction } = require("@livekit/rtc-ffi-bindings");

// Element Call's livekit-client settings (MatrixKeyProvider):
// HKDF, salt "LKFrameEncryptionKey", ratchetWindowSize 10, keyringSize 256.
// libwebrtc's key ring holds at most 255 keys (livekit/rust-sdks#1517: a larger
// key_ring_size silently fails to allocate the ring), so we use 255. js-sdk
// counts key indices modulo 256, so index 255 cannot be stored; we skip it
// with a log line instead of handing it to the native side.
const KEY_RING_SIZE = 255;
const RATCHET_WINDOW_SIZE = 10;
const RATCHET_SALT = new TextEncoder().encode("LKFrameEncryptionKey");

function e2eeRoomOptions() {
    return {
        encryptionType: EncryptionType.GCM,
        keyProviderOptions: {
            ratchetSalt: RATCHET_SALT,
            ratchetWindowSize: RATCHET_WINDOW_SIZE,
            failureTolerance: -1,
            keyRingSize: KEY_RING_SIZE,
            keyDerivationFunction: KeyDerivationFunction.HKDF,
        },
    };
}

/**
 * True if the installed rtc-node has livekit/node-sdks#768: KeyProvider.setKey
 * takes the key itself, and FrameCryptor carries the track SID (setKeyIndex
 * needs it). Stock 1.2.x has neither; patches/apply-pr768.py adds both.
 */
function rtcNodeSupportsKeyedE2ee() {
    return KeyProvider?.prototype?.setKey?.length >= 3 && FrameCryptor?.length >= 5;
}

function keyFingerprint(key) {
    return crypto.createHash("sha256").update(key).digest("hex").slice(0, 8);
}

/**
 * Collects keys from MatrixRTCSessionEvent.EncryptionKeyChanged and hands them
 * to the LiveKit key provider once the room is connected (keys usually arrive
 * before that, because the MatrixRTC session is joined first).
 */
class MediaKeyBridge {
    constructor({ logLine, emit, ownIdentity, setKeyIndexWorkaround }) {
        this.logLine = logLine;
        this.emit = emit;
        this.ownIdentity = ownIdentity;
        this.setKeyIndexWorkaround = setKeyIndexWorkaround;
        this.room = null;
        this.pending = [];
        this.ownKeyIndex = null;
    }

    onEncryptionKeyChanged(key, index, membership, backendIdentity) {
        const own = backendIdentity === this.ownIdentity;
        const who = `${membership?.userId || "?"}:${membership?.deviceId || "?"}`;
        this.logLine(
            `e2ee key received own=${own} index=${index} identity=${backendIdentity} member=${who} ` +
                `len=${key.length} sha256=${keyFingerprint(key)}`,
        );
        if (!this.room) {
            this.pending.push({ key, index, backendIdentity, own });
            return;
        }
        this.apply(key, index, backendIdentity, own);
    }

    attachRoom(room) {
        this.room = room;
        const localIdentity = room.localParticipant?.identity;
        if (localIdentity && localIdentity !== this.ownIdentity) {
            this.logLine(
                `e2ee WARNING livekit identity ${localIdentity} != MatrixRTC backend identity ${this.ownIdentity}; ` +
                    "our own key will not be used for sending",
            );
        }
        const queued = this.pending;
        this.pending = [];
        this.logLine(`e2ee room attached, applying ${queued.length} queued key(s)`);
        for (const k of queued) {
            this.apply(k.key, k.index, k.backendIdentity, k.own);
        }
    }

    apply(key, index, identity, own) {
        if (index < 0 || index >= KEY_RING_SIZE) {
            this.logLine(`e2ee SKIP key index=${index} identity=${identity}: outside key ring (0..${KEY_RING_SIZE - 1})`);
            return;
        }
        const keyProvider = this.room.e2eeManager?.keyProvider;
        if (!keyProvider) {
            this.logLine("e2ee ERROR room has no e2eeManager; connected without encryption options?");
            return;
        }
        try {
            keyProvider.setKey(identity, key, index);
        } catch (err) {
            this.logLine(`e2ee ERROR setKey(${identity}, index=${index}) failed: ${err instanceof Error ? err.message : err}`);
            return;
        }
        this.logLine(`e2ee setKey ok own=${own} identity=${identity} index=${index}`);
        this.emit({ event: "e2ee_key_set", own, index, identity });
        if (own) {
            this.ownKeyIndex = index;
            if (this.setKeyIndexWorkaround) {
                this.moveOwnCryptors(index);
            }
        }
    }

    // Workaround for livekit/rust-sdks#1676 without #1677: point our own frame
    // cryptors at the new index by hand. Needs #768 (track SID in the request).
    moveOwnCryptors(index) {
        for (const cryptor of this.ownCryptors()) {
            try {
                cryptor.setKeyIndex(index);
                this.logLine(`e2ee workaround: cryptor track=${cryptor.trackSid} -> keyIndex=${index}`);
            } catch (err) {
                this.logLine(`e2ee workaround setKeyIndex failed: ${err instanceof Error ? err.message : err}`);
            }
        }
    }

    ownCryptors() {
        const manager = this.room?.e2eeManager;
        if (!manager) return [];
        return manager.frameCryptors().filter((c) => c.participantIdentity === this.ownIdentity);
    }

    // Called after a track is published (new cryptor) and periodically.
    logCryptors(reason) {
        const manager = this.room?.e2eeManager;
        if (!manager) return;
        let cryptors;
        try {
            cryptors = manager.frameCryptors();
        } catch (err) {
            this.logLine(`e2ee frameCryptors failed: ${err instanceof Error ? err.message : err}`);
            return;
        }
        const desc = cryptors
            .map((c) => `${c.participantIdentity === this.ownIdentity ? "OWN" : c.participantIdentity}` +
                `/${c.trackSid}/idx=${c.keyIndex}/${c.enabled ? "on" : "off"}`)
            .join(" ");
        this.logLine(`e2ee cryptors (${reason}) last_own_key_index=${this.ownKeyIndex}: ${desc || "none"}`);
    }
}

function canonicalJson(value) {
    if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
    if (value && typeof value === "object") {
        return `{${Object.keys(value).sort().map((k) => `${JSON.stringify(k)}:${canonicalJson(value[k])}`).join(",")}}`;
    }
    return JSON.stringify(value);
}

function unb64(s) {
    return Buffer.from(s.replace(/-/g, "+").replace(/_/g, "/"), "base64");
}

function b64(buf) {
    return Buffer.from(buf).toString("base64").replace(/=+$/, "");
}

/**
 * Sign the worker device with the account's self-signing key, so it is not an
 * "unverified device of a verified user" for the people in the call.
 * keysFile: the bot's cross_signing_keys.json (read only).
 */
async function crossSignOwnDevice({ homeserver, accessToken, userId, deviceId, keysFile, logLine }) {
    const keys = JSON.parse(fs.readFileSync(keysFile, "utf-8"));
    const seed = unb64(keys.self_signing_sk);
    const privateKey = crypto.createPrivateKey({
        key: Buffer.concat([Buffer.from("302e020100300506032b657004220420", "hex"), seed]),
        format: "der",
        type: "pkcs8",
    });
    const pub = b64(crypto.createPublicKey(privateKey).export({ format: "der", type: "spki" }).subarray(12));
    if (pub !== keys.self_signing_pub) {
        throw new Error("self_signing_sk does not match self_signing_pub in the keys file");
    }

    const headers = { Authorization: `Bearer ${accessToken}`, "Content-Type": "application/json" };
    let query = null;
    let device = null;
    for (let attempt = 0; attempt < 20 && !device; attempt += 1) {
        const r = await fetch(`${homeserver}/_matrix/client/v3/keys/query`, {
            method: "POST",
            headers,
            body: JSON.stringify({ device_keys: { [userId]: [deviceId] } }),
        });
        query = await r.json();
        device = query?.device_keys?.[userId]?.[deviceId];
        if (!device) await new Promise((res) => setTimeout(res, 1000));
    }
    if (!device) throw new Error(`device ${deviceId} has no uploaded keys after 20 s`);

    const serverSsk = Object.values(query?.self_signing_keys?.[userId]?.keys || {})[0];
    if (serverSsk !== pub) {
        throw new Error("the account's self-signing key on the server differs from the keys file; not signing");
    }
    const sigId = `ed25519:${pub}`;
    if (device.signatures?.[userId]?.[sigId]) {
        logLine(`cross-signing: device ${deviceId} already signed`);
        return;
    }
    const { signatures, unsigned, ...toSign } = device;
    const sig = b64(crypto.sign(null, Buffer.from(canonicalJson(toSign), "utf-8"), privateKey));
    const body = { [userId]: { [deviceId]: { ...toSign, signatures: { [userId]: { ...(signatures?.[userId] || {}), [sigId]: sig } } } } };
    const r = await fetch(`${homeserver}/_matrix/client/v3/keys/signatures/upload`, {
        method: "POST",
        headers,
        body: JSON.stringify(body),
    });
    const res = await r.json();
    if (!r.ok || (res.failures && Object.keys(res.failures).length)) {
        throw new Error(`signature upload failed: ${r.status} ${JSON.stringify(res)}`);
    }
    logLine(`cross-signing: device ${deviceId} signed with self-signing key ${pub.slice(0, 8)}…`);
}

module.exports = {
    MediaKeyBridge,
    e2eeRoomOptions,
    crossSignOwnDevice,
    rtcNodeSupportsKeyedE2ee,
    canonicalJson,
    KEY_RING_SIZE,
};

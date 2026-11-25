// Offline check of the #768 patch: build the E2EE FFI requests and encode them
// with FfiRequest.toBinary(), which throws on a missing proto2 `required` field.
// Run from call_worker/: node test/pr768-encode.test.cjs
const path = require("path");
const dist = path.dirname(require.resolve("@livekit/rtc-node"));
const { FfiClient } = require(path.join(dist, "ffi_client.cjs"));
const { KeyProvider, FrameCryptor } = require(path.join(dist, "e2ee.cjs"));
const { FfiRequest } = require("@livekit/rtc-ffi-bindings");

const captured = [];
Object.defineProperty(FfiClient, "instance", { get: () => ({ request: (r) => { captured.push(r); return {}; } }) });

const checks = [
  ["KeyProvider.setKey", () => new KeyProvider(1n, {}).setKey("@bot:example.org:DEV", new Uint8Array(32).fill(7), 3)],
  ["FrameCryptor.setKeyIndex", () => new FrameCryptor(1n, "@bot:example.org:DEV", "TR_x", 0, true).setKeyIndex(2)],
  ["FrameCryptor.setEnabled", () => new FrameCryptor(1n, "@bot:example.org:DEV", "TR_x", 0, true).setEnabled(true)],
];
let failed = 0;
for (const [name, fn] of checks) {
  captured.length = 0;
  try {
    fn();
    const bytes = new FfiRequest(captured[0]).toBinary();
    console.log(`ok   ${name} (${bytes.length} bytes)`);
  } catch (e) {
    failed += 1;
    console.log(`FAIL ${name}: ${e.message}`);
  }
}
process.exit(failed ? 1 : 0);

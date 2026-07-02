import { createHash } from "node:crypto";
import type { HasherPort } from "./types.js";

/** Default hasher factory for Node/Tauri-sidecar environments — `node:crypto`
 * is a Node builtin (not an npm dependency), so this keeps the package at
 * zero runtime deps while still shipping a working default. Browser callers
 * without `node:crypto` must pass their own `hasherFactory` (e.g. a
 * streaming WASM sha256 adapter) — importing this module at all requires a
 * Node-compatible resolver, so keep it a separate entrypoint from the
 * pure-algorithm `./index.js` rather than re-exporting it there. */
export function nodeSha256HasherFactory(): HasherPort {
  const h = createHash("sha256");
  return {
    update(chunk: Uint8Array) {
      h.update(chunk);
    },
    digestHex() {
      // .copy() (Node >=13.1) lets us read an intermediate digest without
      // ending the underlying stream, even though this package's contract
      // only calls digestHex() once at the very end — defensive, zero cost.
      return h.copy().digest("hex");
    },
  };
}

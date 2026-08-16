# @papercusp/resumable-download

Generic, domain-free resumable HTTP downloader with streaming SHA-256
verification. Pure algorithm over three injected ports — zero coupling to
`fs`, a specific `fetch` implementation, or a crypto library — so it runs
the same in a Node sidecar, a Tauri webview, or a browser.

## Why

Large-asset downloads (model weights, in Papercusp's case) need three things
a plain `fetch().then(writeFile)` doesn't give you:

- **Resume** — an interrupted multi-GB download shouldn't restart from
  byte 0. This uses HTTP `Range` requests, resuming from whatever the sink
  reports it already durably holds.
- **Verified** — a byte-perfect checksum against a known-good manifest, so
  a truncated or corrupted download is never silently treated as complete.
  Verification is *streaming* (checked incrementally as bytes arrive), not
  a second pass over the whole file after the fact.
- **Retried, not restarted** — a transient failure (network blip, 5xx)
  backs off and resumes from the last durably-written byte. A checksum
  *mismatch* on a fully-downloaded file is different: that data really is
  wrong, so it truncates and retries the *whole* download from scratch
  (bounded by `mismatchRetries`, default 1).

## Usage

```ts
import { downloadResumable, nodeSha256HasherFactory } from "@papercusp/resumable-download";
import type { DownloadSink } from "@papercusp/resumable-download";

// Implement DownloadSink over whatever storage you have — a plain fs file
// handle, an IndexedDB blob, a Tauri fs-plugin path, etc.
const sink: DownloadSink = myFsFileSink("/path/to/model.gguf.part");

const result = await downloadResumable(
  { url: manifestEntry.url, sha256: manifestEntry.sha256, size: manifestEntry.size },
  {
    sink,
    hasherFactory: nodeSha256HasherFactory, // Node builtin; swap for a WASM
                                             // hasher in a browser context.
    onProgress: ({ bytesDownloaded, totalBytes }) => {
      updateProgressBar(bytesDownloaded, totalBytes);
    },
  },
);
// result.bytesWritten, result.sha256, result.attempts
```

On a checksum mismatch past `mismatchRetries`, `downloadResumable` throws
`ChecksumMismatchError` — treat that as "delete the manifest entry's local
state and surface an error", not a bug in this package.

## Ports

- `DownloadSink` — `bytesWritten()` / `append(chunk)` / `truncate()` /
  optional `finalize()`. The only storage contract; this package never
  touches a filesystem.
- `FetchLike` — matches `globalThis.fetch`'s shape closely enough that the
  default (no `fetchImpl` given) just works in Node ≥18 and any browser;
  inject your own for retry/proxy wrapping or tests.
- `HasherPort` — `update(chunk)` / `digestHex()`, because Web Crypto's
  `subtle.digest` is one-shot (needs the whole buffer in memory) and this
  package hashes incrementally as bytes stream in. `nodeSha256HasherFactory`
  ships a `node:crypto`-backed default (a Node builtin, so this package
  stays at zero npm runtime dependencies).

## Known limitation (v0.1)

Streaming verification only covers bytes downloaded in the *current*
process's attempts. If a caller resumes a partial download **across a
process restart** with a `sha256` set, this package can't re-derive the
hash of bytes it didn't itself stream — it skips verification for that
attempt rather than either lying about a hash or re-reading the whole
partial file (which the write-only `DownloadSink` port deliberately can't
do). A caller that needs verified resume across restarts should either (a)
persist an incremental hash checkpoint alongside the partial in a smarter
sink, or (b) resume without `sha256` and verify the sink's own file hash
out-of-band once complete.

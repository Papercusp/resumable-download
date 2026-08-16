/**
 * Ports (all injected — this package has zero coupling to fs, fetch, or a
 * crypto library; the caller supplies adapters for its runtime).
 */

/** Where downloaded bytes land. Implementations: a fs file, an IndexedDB
 * blob, an in-memory buffer (tests), a Tauri fs-plugin file, etc. */
export interface DownloadSink {
  /** Bytes already durably written at the current destination (the resume
   * offset). Return 0 for a fresh/absent destination. */
  bytesWritten(): Promise<number>;
  /** Append a chunk at the current write offset (i.e. after whatever
   * bytesWritten() reported). Must be durable enough to survive a crash
   * between calls — the caller trusts bytesWritten() as truth on resume. */
  append(chunk: Uint8Array): Promise<void>;
  /** Discard everything written so far and reset the resume offset to 0.
   * Called when a completed download fails checksum verification, or when
   * a resume's declared total size no longer matches the manifest (stale
   * partial from a different asset at the same URL). */
  truncate(): Promise<void>;
  /** Optional: called once, after the download completes AND passes
   * checksum verification (or verification is skipped because the
   * manifest carries no sha256). Use to fsync / rename a .part file into
   * place / mark a DB row complete. */
  finalize?(): Promise<void>;
}

/** A minimal fetch-shaped port so this package never imports a concrete
 * HTTP client. `globalThis.fetch` satisfies this in both Node >=18 and any
 * browser/webview; pass a custom one for retry/proxy wrapping or tests. */
export interface FetchLike {
  (
    url: string,
    init?: { headers?: Record<string, string>; signal?: AbortSignal },
  ): Promise<FetchLikeResponse>;
}

export interface FetchLikeResponse {
  status: number;
  headers: { get(name: string): string | null };
  /** A Web ReadableStream<Uint8Array> (browser/undici fetch) or an
   * AsyncIterable<Uint8Array> (Node's `Readable` also satisfies this via
   * Symbol.asyncIterator). Either is accepted. */
  body: AsyncIterable<Uint8Array> | ReadableStream<Uint8Array> | null;
}

/** An incremental hasher — Web Crypto's `subtle.digest` is one-shot only
 * (needs the whole buffer in memory), so streaming verification needs this
 * port instead. A Node adapter is one line: `crypto.createHash('sha256')`
 * already satisfies this shape (`update`/`digest('hex')` — see
 * `nodeSha256HasherFactory` in `./node-hasher.ts` for the exact adapter). */
export interface HasherPort {
  update(chunk: Uint8Array): void;
  /** Hex-encoded digest of everything fed via update() so far. */
  digestHex(): string;
}

export interface DownloadManifest {
  url: string;
  /** Lowercase hex SHA-256 of the complete file. When omitted, the
   * downloader still resumes byte-range correctly but skips verification
   * (finalize() is called on HTTP-200-complete alone). */
  sha256?: string;
  /** Expected total byte size, when known. Used to (a) detect a stale
   * partial — if bytesWritten() already exceeds `size`, the sink is
   * truncated and the download restarts from scratch — and (b) compute a
   * percentage for onProgress. Not required for correctness otherwise: the
   * downloader also trusts the server's Content-Range/Content-Length. */
  size?: number;
}

export interface ProgressInfo {
  bytesDownloaded: number;
  /** Best-known total: manifest.size, else the server's declared total
   * (Content-Range's `/total` or plain Content-Length + resume offset),
   * else undefined if genuinely unknown. */
  totalBytes?: number;
}

export interface DownloadOptions {
  sink: DownloadSink;
  /** Default: `globalThis.fetch`. */
  fetchImpl?: FetchLike;
  /** Default: a lazy `node:crypto` sha256 adapter (Node builtin, not an
   * npm dependency — see `./node-hasher.ts`). Required in environments
   * without `node:crypto` (browsers) if manifest.sha256 is set. */
  hasherFactory?: () => HasherPort;
  onProgress?: (info: ProgressInfo) => void;
  /** Extra request headers merged into every attempt (auth tokens, UA). */
  headers?: Record<string, string>;
  /** Attempts for a TRANSIENT failure (network error, 5xx, a body-stream
   * error) before giving up. A checksum mismatch always gets exactly one
   * retry-from-scratch regardless of this count (see `mismatchRetries`). */
  maxRetries?: number;
  /** How many times a completed-but-checksum-mismatched download is
   * retried from scratch (full re-download) before giving up. Default 1 —
   * a single silent corruption (flaky network, cache poisoning) is worth
   * one clean retry; anything past that is almost always a bad manifest. */
  mismatchRetries?: number;
  /** attempt is 1-based. Default: exponential backoff with jitter, capped
   * at 30s (1s, 2s, 4s, 8s, 16s, 30s, 30s, …). */
  backoffMs?: (attempt: number) => number;
  signal?: AbortSignal;
}

export interface DownloadResult {
  bytesWritten: number;
  sha256?: string;
  /** How many full network attempts this call made (>1 means it recovered
   * from at least one transient failure or checksum mismatch). */
  attempts: number;
}

export class ChecksumMismatchError extends Error {
  constructor(
    public readonly expected: string,
    public readonly actual: string,
  ) {
    super(`checksum mismatch: expected ${expected}, got ${actual}`);
    this.name = "ChecksumMismatchError";
  }
}

export class DownloadAbortedError extends Error {
  constructor() {
    super("download aborted");
    this.name = "DownloadAbortedError";
  }
}

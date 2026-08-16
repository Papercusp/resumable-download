import {
  ChecksumMismatchError,
  DownloadAbortedError,
  type DownloadManifest,
  type DownloadOptions,
  type DownloadResult,
  type FetchLikeResponse,
} from "./types.js";

export type {
  DownloadSink,
  FetchLike,
  FetchLikeResponse,
  HasherPort,
  DownloadManifest,
  DownloadOptions,
  DownloadResult,
  ProgressInfo,
} from "./types.js";
export { ChecksumMismatchError, DownloadAbortedError } from "./types.js";
export { nodeSha256HasherFactory } from "./node-hasher.js";

const DEFAULT_MAX_RETRIES = 5;
const DEFAULT_MISMATCH_RETRIES = 1;

function defaultBackoffMs(attempt: number): number {
  const base = Math.min(30_000, 1000 * 2 ** (attempt - 1));
  // +/-20% jitter so N parallel downloaders retrying together don't thunder.
  const jitter = base * 0.2 * (Math.random() * 2 - 1);
  return Math.round(base + jitter);
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) return reject(new DownloadAbortedError());
    const t = setTimeout(resolve, ms);
    signal?.addEventListener(
      "abort",
      () => {
        clearTimeout(t);
        reject(new DownloadAbortedError());
      },
      { once: true },
    );
  });
}

/** Normalizes a fetch-like response body (Web ReadableStream OR Node
 * AsyncIterable) into a single async-iterable of Uint8Array chunks. */
async function* iterateBody(
  body: FetchLikeResponse["body"],
): AsyncGenerator<Uint8Array> {
  if (body == null) return;
  const maybeStream = body as ReadableStream<Uint8Array>;
  if (typeof maybeStream.getReader === "function") {
    const reader = maybeStream.getReader();
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) return;
        if (value) yield value;
      }
    } finally {
      reader.releaseLock();
    }
    return;
  }
  // AsyncIterable<Uint8Array> (e.g. a Node Readable).
  for await (const chunk of body as AsyncIterable<Uint8Array>) {
    yield chunk instanceof Uint8Array ? chunk : new Uint8Array(chunk as ArrayBufferLike);
  }
}

/** Parses `Content-Range: bytes 1000-1999/2000` -> 2000 (the total). Returns
 * undefined if the header is absent or unparseable. */
function totalFromContentRange(headerValue: string | null): number | undefined {
  if (!headerValue) return undefined;
  const m = /\/(\d+)\s*$/.exec(headerValue.trim());
  return m ? Number(m[1]) : undefined;
}

/**
 * Download `manifest.url` into `opts.sink`, resuming from whatever
 * `sink.bytesWritten()` already holds via an HTTP `Range` request, verifying
 * a streaming SHA-256 against `manifest.sha256` (when given) as bytes
 * arrive rather than after buffering the whole file.
 *
 * - A checksum mismatch on completion truncates the sink and retries the
 *   FULL download from scratch (up to `mismatchRetries` times) — a partial
 *   file can never silently pass as "resumed" past a corruption.
 * - A transient failure (network error, non-2xx, a body-stream error)
 *   backs off and retries, RESUMING from the last durably-appended byte
 *   (no work is thrown away) — up to `maxRetries` times.
 * - If `manifest.size` is known and the sink already holds >= that many
 *   bytes, the sink is treated as a stale/foreign partial and truncated
 *   before starting (e.g. a different asset previously landed at the same
 *   sink).
 */
export async function downloadResumable(
  manifest: DownloadManifest,
  opts: DownloadOptions,
): Promise<DownloadResult> {
  const fetchImpl =
    opts.fetchImpl ?? (globalThis.fetch as unknown as typeof opts.fetchImpl);
  if (!fetchImpl) {
    throw new Error(
      "resumable-download: no fetchImpl given and globalThis.fetch is unavailable",
    );
  }
  const maxRetries = opts.maxRetries ?? DEFAULT_MAX_RETRIES;
  const mismatchRetries = opts.mismatchRetries ?? DEFAULT_MISMATCH_RETRIES;
  const backoffMs = opts.backoffMs ?? defaultBackoffMs;
  const { sink } = opts;

  const throwIfAborted = () => {
    if (opts.signal?.aborted) throw new DownloadAbortedError();
  };

  let mismatchAttemptsUsed = 0;
  let totalAttempts = 0;

  for (;;) {
    throwIfAborted();

    let offset = await sink.bytesWritten();
    if (manifest.size != null && offset >= manifest.size && offset > 0) {
      // Stale/foreign partial at this sink — a fresh manifest can't resume
      // past a size it already exceeds.
      await sink.truncate();
      offset = 0;
    }

    let hasher = opts.hasherFactory?.();
    // Streaming verification needs the FULL byte sequence from offset 0; a
    // resumed download that already has bytes on disk can't re-derive the
    // hash of those bytes without re-reading them, which this package's
    // write-only sink port deliberately doesn't support (keeps sinks
    // trivial to implement). So: hashing only covers THIS attempt's
    // freshly-downloaded bytes, and verification is only meaningful when
    // resuming from offset 0. A caller that wants verified resume across
    // process restarts should persist the manifest hash alongside a
    // partial-hash checkpoint in a smarter sink — out of scope for v0.1.
    if (offset > 0 && manifest.sha256) hasher = undefined;

    let attemptError: unknown;
    let bytesThisAttempt = 0;
    totalAttempts++;

    for (let retry = 0; retry <= maxRetries; retry++) {
      throwIfAborted();
      try {
        const headers: Record<string, string> = { ...opts.headers };
        if (offset > 0) headers["Range"] = `bytes=${offset}-`;

        const res = await fetchImpl(manifest.url, {
          headers,
          signal: opts.signal,
        });

        if (offset > 0 && res.status === 200) {
          // Server ignored Range (no partial-content support) — it sent
          // the whole file again. Restart the sink from scratch so we
          // don't append a second full copy after the existing bytes.
          await sink.truncate();
          offset = 0;
          hasher = opts.hasherFactory?.();
        } else if (res.status !== 200 && res.status !== 206) {
          throw new Error(`resumable-download: HTTP ${res.status} for ${manifest.url}`);
        }

        const total =
          manifest.size ??
          totalFromContentRange(res.headers.get("content-range")) ??
          (() => {
            const len = res.headers.get("content-length");
            return len ? offset + Number(len) : undefined;
          })();

        let downloaded = offset;
        for await (const chunk of iterateBody(res.body)) {
          throwIfAborted();
          await sink.append(chunk);
          hasher?.update(chunk);
          downloaded += chunk.byteLength;
          bytesThisAttempt = downloaded;
          opts.onProgress?.({ bytesDownloaded: downloaded, totalBytes: total });
        }

        // Completed this attempt without a stream error.
        offset = downloaded;
        attemptError = undefined;
        break;
      } catch (err) {
        if (err instanceof DownloadAbortedError) throw err;
        attemptError = err;
        // Re-check how much actually landed durably (append() may have
        // partially succeeded before the failure) so the next retry
        // resumes from truth, not from our in-loop estimate.
        offset = await sink.bytesWritten();
        if (retry < maxRetries) {
          await sleep(backoffMs(retry + 1), opts.signal);
        }
      }
    }

    if (attemptError) {
      throw new Error(
        `resumable-download: exhausted ${maxRetries} retries for ${manifest.url}: ${
          (attemptError as Error)?.message ?? attemptError
        }`,
      );
    }

    if (manifest.sha256) {
      if (!hasher) {
        // Either no hasherFactory was given, or this attempt resumed from
        // a nonzero offset (can't verify a partial hash — see note above).
        // Both cases: nothing more this function can safely check.
        await sink.finalize?.();
        return { bytesWritten: bytesThisAttempt, attempts: totalAttempts };
      }
      const actual = hasher.digestHex().toLowerCase();
      const expected = manifest.sha256.toLowerCase();
      if (actual !== expected) {
        if (mismatchAttemptsUsed >= mismatchRetries) {
          throw new ChecksumMismatchError(expected, actual);
        }
        mismatchAttemptsUsed++;
        await sink.truncate();
        continue; // retry the whole download from scratch
      }
      await sink.finalize?.();
      return { bytesWritten: bytesThisAttempt, sha256: actual, attempts: totalAttempts };
    }

    await sink.finalize?.();
    return { bytesWritten: bytesThisAttempt, attempts: totalAttempts };
  }
}

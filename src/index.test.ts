import { createHash } from "node:crypto";
import { describe, expect, it, vi } from "vitest";
import { downloadResumable, ChecksumMismatchError, nodeSha256HasherFactory } from "./index.js";
import type { DownloadSink, FetchLike, HasherPort } from "./types.js";

function sha256Hex(bytes: Uint8Array): string {
  return createHash("sha256").update(bytes).digest("hex");
}

/** A trivial in-memory sink — good enough to exercise the resume/truncate
 * contract without touching a real filesystem. */
class MemorySink implements DownloadSink {
  private buf: Uint8Array = new Uint8Array(0);
  finalized = false;

  async bytesWritten(): Promise<number> {
    return this.buf.byteLength;
  }
  async append(chunk: Uint8Array): Promise<void> {
    const next = new Uint8Array(this.buf.byteLength + chunk.byteLength);
    next.set(this.buf, 0);
    next.set(chunk, this.buf.byteLength);
    this.buf = next;
  }
  async truncate(): Promise<void> {
    this.buf = new Uint8Array(0);
  }
  async finalize(): Promise<void> {
    this.finalized = true;
  }
  contents(): Uint8Array {
    return this.buf;
  }
}

function chunk(str: string): Uint8Array {
  return new TextEncoder().encode(str);
}

function bodyFromChunks(chunks: Uint8Array[]): ReadableStream<Uint8Array> {
  let i = 0;
  return new ReadableStream({
    pull(controller) {
      if (i < chunks.length) {
        controller.enqueue(chunks[i++]);
      } else {
        controller.close();
      }
    },
  });
}

describe("downloadResumable", () => {
  it("downloads fully and verifies a matching checksum", async () => {
    const payload = chunk("hello resumable world");
    const expected = sha256Hex(payload);
    const sink = new MemorySink();
    const fetchImpl: FetchLike = vi.fn(async () => ({
      status: 200,
      headers: { get: (n: string) => (n === "content-length" ? String(payload.byteLength) : null) },
      body: bodyFromChunks([payload.slice(0, 5), payload.slice(5)]),
    }));

    const result = await downloadResumable(
      { url: "https://example.test/model.bin", sha256: expected, size: payload.byteLength },
      { sink, fetchImpl, hasherFactory: nodeSha256HasherFactory },
    );

    expect(result.sha256).toBe(expected);
    expect(result.bytesWritten).toBe(payload.byteLength);
    expect(sink.finalized).toBe(true);
    expect(new TextDecoder().decode(sink.contents())).toBe("hello resumable world");
  });

  it("resumes from the sink's existing offset via a Range request", async () => {
    const full = chunk("0123456789");
    const sink = new MemorySink();
    await sink.append(full.slice(0, 4)); // pretend 4 bytes already landed

    const fetchImpl: FetchLike = vi.fn(async (_url, init) => {
      expect(init?.headers?.["Range"]).toBe("bytes=4-");
      return {
        status: 206,
        headers: { get: (n: string) => (n === "content-range" ? "bytes 4-9/10" : null) },
        body: bodyFromChunks([full.slice(4)]),
      };
    });

    const result = await downloadResumable(
      { url: "https://example.test/model.bin", size: 10 },
      { sink, fetchImpl },
    );

    expect(result.bytesWritten).toBe(10);
    expect(new TextDecoder().decode(sink.contents())).toBe("0123456789");
    expect(fetchImpl).toHaveBeenCalledTimes(1);
  });

  it("retries a transient failure and resumes from the last durably-written byte", async () => {
    const full = chunk("abcdefghij");
    const sink = new MemorySink();
    let call = 0;
    const fetchImpl: FetchLike = vi.fn(async (_url, init) => {
      call++;
      if (call === 1) {
        // First attempt: writes 4 bytes then the stream errors.
        return {
          status: 200,
          headers: { get: () => null },
          body: (async function* () {
            yield full.slice(0, 4);
            throw new Error("simulated network drop");
          })(),
        };
      }
      // Second attempt resumes from byte 4.
      expect(init?.headers?.["Range"]).toBe("bytes=4-");
      return {
        status: 206,
        headers: { get: () => null },
        body: bodyFromChunks([full.slice(4)]),
      };
    });

    const result = await downloadResumable(
      { url: "https://example.test/model.bin" },
      { sink, fetchImpl, backoffMs: () => 0 },
    );

    expect(result.bytesWritten).toBe(10);
    expect(new TextDecoder().decode(sink.contents())).toBe("abcdefghij");
    expect(call).toBe(2);
  });

  it("on checksum mismatch, truncates and retries the full download from scratch once", async () => {
    const good = chunk("correct-bytes");
    const bad = chunk("wrong-bytes!!");
    const expected = sha256Hex(good);
    const sink = new MemorySink();
    let call = 0;
    const fetchImpl: FetchLike = vi.fn(async () => {
      call++;
      return {
        status: 200,
        headers: { get: () => null },
        body: bodyFromChunks([call === 1 ? bad : good]),
      };
    });

    const result = await downloadResumable(
      { url: "https://example.test/model.bin", sha256: expected },
      { sink, fetchImpl, hasherFactory: nodeSha256HasherFactory, backoffMs: () => 0 },
    );

    expect(result.sha256).toBe(expected);
    expect(call).toBe(2);
    expect(new TextDecoder().decode(sink.contents())).toBe("correct-bytes");
  });

  it("throws ChecksumMismatchError once mismatchRetries is exhausted", async () => {
    const bad = chunk("always-wrong");
    const sink = new MemorySink();
    const fetchImpl: FetchLike = vi.fn(async () => ({
      status: 200,
      headers: { get: () => null },
      body: bodyFromChunks([bad]),
    }));

    await expect(
      downloadResumable(
        { url: "https://example.test/model.bin", sha256: "0".repeat(64) },
        { sink, fetchImpl, hasherFactory: nodeSha256HasherFactory, mismatchRetries: 1, backoffMs: () => 0 },
      ),
    ).rejects.toBeInstanceOf(ChecksumMismatchError);
  });

  it("truncates a stale partial that already exceeds the manifest's declared size", async () => {
    const sink = new MemorySink();
    await sink.append(new Uint8Array(20)); // stale 20-byte partial
    const fresh = chunk("fresh");
    const fetchImpl: FetchLike = vi.fn(async (_url, init) => {
      expect(init?.headers?.["Range"]).toBeUndefined(); // restarted from 0
      return { status: 200, headers: { get: () => null }, body: bodyFromChunks([fresh]) };
    });

    const result = await downloadResumable(
      { url: "https://example.test/model.bin", size: 5 },
      { sink, fetchImpl },
    );

    expect(result.bytesWritten).toBe(5);
    expect(new TextDecoder().decode(sink.contents())).toBe("fresh");
  });

  it("aborts cleanly via signal without retrying", async () => {
    const sink = new MemorySink();
    const controller = new AbortController();
    const fetchImpl: FetchLike = vi.fn(async () => {
      controller.abort();
      throw new DOMException("aborted", "AbortError");
    });

    await expect(
      downloadResumable(
        { url: "https://example.test/model.bin" },
        { sink, fetchImpl, signal: controller.signal, backoffMs: () => 0 },
      ),
    ).rejects.toThrow();
  });
});

describe("nodeSha256HasherFactory", () => {
  it("matches node:crypto's one-shot digest for the same bytes", () => {
    const hasher: HasherPort = nodeSha256HasherFactory();
    const data = chunk("streamed in two pieces");
    hasher.update(data.slice(0, 10));
    hasher.update(data.slice(10));
    expect(hasher.digestHex()).toBe(sha256Hex(data));
  });
});

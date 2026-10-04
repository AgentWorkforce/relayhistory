/**
 * Reads a JSON request body without buffering more than a fixed number of bytes, so an
 * oversized upload is refused as it streams in rather than after it is held in memory.
 */

/**
 * The body bound for `POST /v1/ingest` and `POST /v1/sessions/:sessionId/turns`: 100 MiB,
 * the request limit the hosted Worker's platform (Cloudflare) enforces. Their batches are
 * bounded by count (1,000 records or turns), not bytes, and a single local transcript
 * record may be 16 MiB before JSON escaping, so no smaller bound is safe to impose
 * without rejecting uploads the hosted service accepts. A self-hosted server still holds
 * at most this much of one request in memory.
 */
export const MAX_JSON_BODY_BYTES = 100 * 1024 * 1024;

/** Why a body could not be read: absent, not UTF-8 JSON, or over the limit. */
export type BoundedJsonFailure = "missing" | "malformed" | "too_large";

/** The default failure: callers answer `too_large` with 413 and the rest with 400. */
export class BoundedJsonError extends Error {
  readonly failure: BoundedJsonFailure;
  constructor(failure: BoundedJsonFailure) {
    super(`Request body is ${failure.replace("_", " ")}`);
    this.failure = failure;
  }
}

export interface BoundedJsonOptions {
  maxBytes: number;
  /** The error thrown for each failure. Defaults to `BoundedJsonError`. */
  error?(failure: BoundedJsonFailure): Error;
  /** Counts the bytes received so far, including when reading fails. */
  read?: { bytes: number };
}

export async function readBoundedJson(
  request: Request,
  {
    maxBytes,
    error = (failure) => new BoundedJsonError(failure),
    read = { bytes: 0 },
  }: BoundedJsonOptions,
): Promise<unknown> {
  const reader = request.body?.getReader();
  if (!reader) throw error("missing");
  const decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: false });
  let text = "";
  try {
    while (true) {
      const chunk = await reader.read();
      if (chunk.done) break;
      read.bytes += chunk.value.byteLength;
      if (read.bytes > maxBytes) {
        await reader.cancel();
        break;
      }
      text += decoder.decode(chunk.value, { stream: true });
    }
    if (read.bytes <= maxBytes) {
      text += decoder.decode();
      return JSON.parse(text);
    }
  } catch {
    throw error("malformed");
  } finally {
    reader.releaseLock();
  }
  throw error("too_large");
}

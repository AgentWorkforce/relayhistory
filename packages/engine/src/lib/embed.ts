export const DEFAULT_EMBEDDING_MODEL = "text-embedding-3-small";
export const DEFAULT_EMBEDDING_DIM = 1536;
export const DEFAULT_EMBEDDING_API_URL = "https://api.openai.com/v1/embeddings";

/** Log line the neighborhood-memory spec can point at when ingest skips embedding. */
export const EMBEDDING_SKIP_LOG_PREFIX = "ingest embedding skipped:";

export type EmbeddingSkipReason =
  | "provider_not_configured"
  | "empty_content"
  | "provider_error"
  | "dimension_mismatch";

export interface EmbeddingProvider {
  readonly model: string;
  readonly dim: number;
  embed(text: string): Promise<number[]>;
  /** Optional. One HTTP request for many post-scrub texts. Ingest uses this
   *  when present so a 1,000-record batch is not 1,000 serial round-trips. */
  embedMany?(texts: string[]): Promise<number[][]>;
}

/**
 * OpenAI accepts up to 2,048 inputs per embeddings request. 64 keeps each
 * payload small enough for Worker memory/timeout given long prompt records,
 * so a max ingest (1,000 records) is 16 HTTP calls instead of 1,000 serial
 * ones. Stay on the ingest path (spec §4.4 / §9 step 2 writes embeddings on
 * ingest); do not fire-and-forget.
 */
export const EMBEDDING_BATCH_SIZE = 64;

/**
 * Two in-flight batch requests overlap provider RTT without bursting the
 * Worker's subrequest budget. 1,000 records → 16 batches → 8 waves.
 */
export const EMBEDDING_BATCH_CONCURRENCY = 2;

/**
 * Longest single input sent to the provider, in characters.
 *
 * `text-embedding-3-small` accepts 8,192 tokens, and the API rejects the **entire
 * request** when any one input exceeds that. Because `embedEventsBatched` marks every
 * entry of a failed batch as `provider_error`, one oversized record does not lose its
 * own embedding — it loses all 64 in its batch.
 *
 * That is not an edge case on real data: 16,591 of 357,935 production events (4.6%)
 * are over the limit, which puts a batch of 64 at ~95% odds of containing at least
 * one. Unbounded, a full backfill would embed roughly 5% of the store while every
 * signal stayed green — rows written, per-event skip reasons logged, ingest 200.
 *
 * 24,000 characters is ~8,000 tokens at the ~3-chars/token rate code and JSON actually
 * tokenize at (prose is nearer 4), which keeps headroom under the cap. Embedding the
 * first 24k characters of a long record is worth far more than embedding none of it,
 * and far more than costing 63 neighbours their vectors.
 */
export const MAX_EMBEDDING_INPUT_CHARS = 24_000;

/**
 * Total characters allowed across one request's inputs.
 *
 * The provider caps a request at 300,000 tokens over all inputs, which is separate from
 * the 8,192-token per-input cap. With inputs bounded at
 * [`MAX_EMBEDDING_INPUT_CHARS`], a full batch of 64 would be ~1.5M characters — roughly
 * 400-500k tokens, well past the request limit — so batches of large records failed
 * wholesale while every individual input was legal.
 *
 * 150,000 characters is ~40-50k tokens, comfortably inside the limit with room for the
 * tokenizer to be less generous than the estimate. Typical records are ~3.4k characters,
 * so an ordinary batch still fills to the 64-input cap and nothing gets slower; only
 * batches of genuinely large records split earlier, which is exactly the case that was
 * failing.
 */
export const MAX_EMBEDDING_BATCH_CHARS = 150_000;

/**
 * Bound one input to what the model will accept.
 *
 * Applied at the provider boundary rather than at the call sites, so every path in —
 * `embed`, `embedMany`, and anything added later — is bounded by construction.
 */
export function boundEmbeddingInput(text: string): string {
  return text.length > MAX_EMBEDDING_INPUT_CHARS
    ? text.slice(0, MAX_EMBEDDING_INPUT_CHARS)
    : text;
}

export interface EmbeddingEnv {
  OPENAI_API_KEY?: string;
  EMBEDDING_API_KEY?: string;
  EMBEDDING_API_URL?: string;
  EMBEDDING_MODEL?: string;
}

export interface EventEmbedding {
  embedding: number[] | null;
  embeddingModel: string | null;
  embeddingDim: number | null;
  contentHash: string | null;
  generatedAt: Date | null;
  embeddingSkipReason: EmbeddingSkipReason | null;
}

export function embeddingProviderFromEnv(
  env: EmbeddingEnv,
): EmbeddingProvider | null {
  const apiKey = env.EMBEDDING_API_KEY?.trim() || env.OPENAI_API_KEY?.trim();
  if (!apiKey) {
    return null;
  }
  return new OpenAiEmbeddingProvider({
    apiKey,
    model: env.EMBEDDING_MODEL?.trim() || DEFAULT_EMBEDDING_MODEL,
    apiUrl: env.EMBEDDING_API_URL?.trim() || DEFAULT_EMBEDDING_API_URL,
  });
}

export class OpenAiEmbeddingProvider implements EmbeddingProvider {
  readonly model: string;
  readonly dim: number;
  readonly apiUrl: string;
  #apiKey: string;

  constructor(opts: {
    apiKey: string;
    model?: string;
    apiUrl?: string;
    dim?: number;
  }) {
    this.#apiKey = opts.apiKey;
    this.model = opts.model ?? DEFAULT_EMBEDDING_MODEL;
    this.apiUrl = opts.apiUrl ?? DEFAULT_EMBEDDING_API_URL;
    this.dim = opts.dim ?? DEFAULT_EMBEDDING_DIM;
  }

  async embed(text: string): Promise<number[]> {
    const [embedding] = await this.embedMany([text]);
    if (!Array.isArray(embedding) || embedding.length === 0) {
      throw new Error("embedding provider returned no vector");
    }
    return embedding;
  }

  async embedMany(texts: string[]): Promise<number[][]> {
    if (texts.length === 0) {
      return [];
    }
    const response = await fetch(this.apiUrl, {
      method: "POST",
      headers: {
        authorization: `Bearer ${this.#apiKey}`,
        "content-type": "application/json",
      },
      body: JSON.stringify({
        model: this.model,
        // Bound every input: one over-length text 400s the whole request, taking the
        // other 63 embeddings in this batch down with it.
        input: texts.map(boundEmbeddingInput),
      }),
    });
    if (!response.ok) {
      throw new Error(`embedding provider http ${response.status}`);
    }
    const body = (await response.json()) as {
      data?: Array<{ embedding?: number[]; index?: number }>;
    };
    const rows = [...(body.data ?? [])].sort(
      (left, right) => (left.index ?? 0) - (right.index ?? 0),
    );
    if (rows.length !== texts.length) {
      throw new Error("embedding provider returned wrong batch size");
    }
    return rows.map((row, index) => {
      const embedding = row.embedding;
      if (!Array.isArray(embedding) || embedding.length === 0) {
        throw new Error(`embedding provider returned no vector at ${index}`);
      }
      return embedding;
    });
  }
}

export async function sha256Hex(value: string): Promise<string> {
  const digest = await crypto.subtle.digest(
    "SHA-256",
    new TextEncoder().encode(value),
  );
  return Array.from(new Uint8Array(digest))
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
}

export function logEmbeddingSkip(
  reason: EmbeddingSkipReason,
  eventId?: string,
): void {
  const suffix = eventId ? ` eventId=${eventId}` : "";
  console.info(`${EMBEDDING_SKIP_LOG_PREFIX} ${reason}${suffix}`);
}

export async function resolveEventEmbedding(
  content: string | null,
  provider: EmbeddingProvider | null | undefined,
  eventId?: string,
): Promise<EventEmbedding> {
  const results = await resolveEventEmbeddings(
    [{ content, eventId }],
    provider,
  );
  const result = results[0];
  if (!result) {
    throw new Error("resolveEventEmbeddings returned no result");
  }
  return result;
}

export async function resolveEventEmbeddings(
  items: ReadonlyArray<{ content: string | null; eventId?: string }>,
  provider: EmbeddingProvider | null | undefined,
): Promise<EventEmbedding[]> {
  const results: EventEmbedding[] = new Array(items.length);
  const pending: Array<{
    index: number;
    content: string;
    contentHash: string;
    eventId?: string;
  }> = [];

  for (const [index, item] of items.entries()) {
    if (!item.content) {
      logEmbeddingSkip("empty_content", item.eventId);
      results[index] = skipped("empty_content", null);
      continue;
    }
    const contentHash = await sha256Hex(item.content);
    if (!provider) {
      logEmbeddingSkip("provider_not_configured", item.eventId);
      results[index] = skipped("provider_not_configured", contentHash);
      continue;
    }
    pending.push({
      index,
      content: item.content,
      contentHash,
      eventId: item.eventId,
    });
  }

  if (pending.length === 0 || !provider) {
    return results;
  }

  // Split on BOTH the input count and the total characters in the request. The count
  // alone is not enough: the API caps a request at 300,000 tokens across all inputs, so
  // 64 large records blow the request limit even when every single input is legal on its
  // own. Measured in production on 2026-09-03, before this bound: rows that failed
  // averaged 83,677 characters against 3,360 for rows that succeeded, and 610 of 762
  // failures were over the per-input cap — one big record poisoning its whole batch.
  const batches: Array<(typeof pending)[number][]> = [];
  let current: (typeof pending)[number][] = [];
  let currentChars = 0;
  for (const item of pending) {
    const chars = Math.min(item.content.length, MAX_EMBEDDING_INPUT_CHARS);
    const wouldOverflow =
      current.length >= EMBEDDING_BATCH_SIZE ||
      (current.length > 0 && currentChars + chars > MAX_EMBEDDING_BATCH_CHARS);
    if (wouldOverflow) {
      batches.push(current);
      current = [];
      currentChars = 0;
    }
    current.push(item);
    currentChars += chars;
  }
  if (current.length > 0) {
    batches.push(current);
  }

  await runWithConcurrency(
    batches,
    EMBEDDING_BATCH_CONCURRENCY,
    async (batch) => {
      try {
        const vectors = await embedTexts(
          provider,
          batch.map((entry) => entry.content),
        );
        if (vectors.length !== batch.length) {
          throw new Error("embedding provider returned wrong batch size");
        }
        for (const [offset, entry] of batch.entries()) {
          const embedding = vectors[offset];
          if (!Array.isArray(embedding) || embedding.length === 0) {
            logEmbeddingSkip("provider_error", entry.eventId);
            results[entry.index] = skipped("provider_error", entry.contentHash);
            continue;
          }
          if (embedding.length !== provider.dim) {
            logEmbeddingSkip("dimension_mismatch", entry.eventId);
            results[entry.index] = skipped(
              "dimension_mismatch",
              entry.contentHash,
            );
            continue;
          }
          results[entry.index] = {
            embedding,
            embeddingModel: provider.model,
            embeddingDim: provider.dim,
            contentHash: entry.contentHash,
            generatedAt: new Date(),
            embeddingSkipReason: null,
          };
        }
      } catch {
        for (const entry of batch) {
          logEmbeddingSkip("provider_error", entry.eventId);
          results[entry.index] = skipped("provider_error", entry.contentHash);
        }
      }
    },
  );

  return results;
}

async function embedTexts(
  provider: EmbeddingProvider,
  texts: string[],
): Promise<number[][]> {
  if (texts.length === 0) {
    return [];
  }
  if (typeof provider.embedMany === "function") {
    return provider.embedMany(texts);
  }
  return runWithConcurrency(texts, EMBEDDING_BATCH_CONCURRENCY, (text) =>
    provider.embed(text),
  );
}

async function runWithConcurrency<T, R>(
  items: readonly T[],
  concurrency: number,
  worker: (item: T) => Promise<R>,
): Promise<R[]> {
  if (items.length === 0) {
    return [];
  }
  const results: R[] = new Array(items.length);
  let next = 0;
  const run = async () => {
    while (true) {
      const index = next;
      next += 1;
      if (index >= items.length) {
        return;
      }
      results[index] = await worker(items[index]);
    }
  };
  const pool = Math.min(Math.max(1, concurrency), items.length);
  await Promise.all(Array.from({ length: pool }, () => run()));
  return results;
}

function skipped(
  reason: EmbeddingSkipReason,
  contentHash: string | null,
): EventEmbedding {
  return {
    embedding: null,
    embeddingModel: null,
    embeddingDim: null,
    contentHash,
    generatedAt: null,
    embeddingSkipReason: reason,
  };
}

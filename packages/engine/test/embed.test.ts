import { afterEach, describe, expect, it, vi } from "vitest";
import {
  DEFAULT_EMBEDDING_API_URL,
  DEFAULT_EMBEDDING_MODEL,
  EMBEDDING_BATCH_SIZE,
  EMBEDDING_SKIP_LOG_PREFIX,
  MAX_EMBEDDING_BATCH_CHARS,
  MAX_EMBEDDING_INPUT_CHARS,
  OpenAiEmbeddingProvider,
  boundEmbeddingInput,
  embeddingProviderFromEnv,
  resolveEventEmbedding,
  resolveEventEmbeddings,
} from "../src/lib/embed.js";

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("embeddingProviderFromEnv", () => {
  it("returns null when no embedding key is configured", () => {
    expect(embeddingProviderFromEnv({})).toBeNull();
    expect(embeddingProviderFromEnv({ OPENAI_API_KEY: "  " })).toBeNull();
  });

  it("builds a provider from EMBEDDING_API_KEY or OPENAI_API_KEY", () => {
    const fromEmbeddingKey = embeddingProviderFromEnv({
      EMBEDDING_API_KEY: "embed-key",
      EMBEDDING_MODEL: "text-embedding-3-small",
    });
    expect(fromEmbeddingKey).toBeInstanceOf(OpenAiEmbeddingProvider);
    expect(fromEmbeddingKey?.model).toBe(DEFAULT_EMBEDDING_MODEL);

    const fromOpenAi = embeddingProviderFromEnv({
      OPENAI_API_KEY: "openai-key",
    });
    expect(fromOpenAi).toBeInstanceOf(OpenAiEmbeddingProvider);
  });
});

describe("OpenAiEmbeddingProvider", () => {
  it("posts scrubbed input to the embeddings API", async () => {
    const fetchMock = vi.fn(async () => {
      return new Response(
        JSON.stringify({
          data: [{ embedding: Array.from({ length: 1536 }, () => 0.1) }],
        }),
        { headers: { "Content-Type": "application/json" } },
      );
    });
    vi.stubGlobal("fetch", fetchMock);

    const provider = new OpenAiEmbeddingProvider({ apiKey: "test-key" });
    const vector = await provider.embed("Task: Build embeddings");

    expect(vector).toHaveLength(1536);
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, init] = fetchMock.mock.calls[0] as unknown as [
      string,
      RequestInit,
    ];
    expect(url).toBe(DEFAULT_EMBEDDING_API_URL);
    expect(init.method).toBe("POST");
    expect(init.headers).toMatchObject({
      authorization: "Bearer test-key",
    });
    expect(JSON.parse(String(init.body))).toEqual({
      model: DEFAULT_EMBEDDING_MODEL,
      input: ["Task: Build embeddings"],
      dimensions: 1536,
    });
  });

  it("posts a batch of inputs in one request", async () => {
    const fetchMock = vi.fn(async () => {
      return new Response(
        JSON.stringify({
          data: [
            { index: 1, embedding: Array.from({ length: 1536 }, () => 0.2) },
            { index: 0, embedding: Array.from({ length: 1536 }, () => 0.1) },
          ],
        }),
        { headers: { "Content-Type": "application/json" } },
      );
    });
    vi.stubGlobal("fetch", fetchMock);

    const provider = new OpenAiEmbeddingProvider({ apiKey: "test-key" });
    const vectors = await provider.embedMany(["first", "second"]);

    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [, init] = fetchMock.mock.calls[0] as unknown as [
      string,
      RequestInit,
    ];
    expect(JSON.parse(String(init.body))).toEqual({
      model: DEFAULT_EMBEDDING_MODEL,
      input: ["first", "second"],
      dimensions: 1536,
    });
    expect(vectors[0]?.[0]).toBeCloseTo(0.1);
    expect(vectors[1]?.[0]).toBeCloseTo(0.2);
  });
});

describe("requested dimensions", () => {
  function stubVectors(length: number) {
    const fetchMock = vi.fn(async (_url: string, init: any) => {
      const { input } = JSON.parse(String(init.body)) as { input: string[] };
      return new Response(
        JSON.stringify({
          data: input.map((_, index) => ({
            index,
            embedding: Array.from({ length }, () => 0.1),
          })),
        }),
        { headers: { "Content-Type": "application/json" } },
      );
    });
    vi.stubGlobal("fetch", fetchMock);
    return fetchMock;
  }
  const body = (fetchMock: ReturnType<typeof stubVectors>) =>
    JSON.parse(String((fetchMock.mock.calls[0] as any[])[1].body));

  it("asks a v3 model for the column's 1536 dimensions and stores the result", async () => {
    const fetchMock = stubVectors(1536);
    const provider = embeddingProviderFromEnv({
      EMBEDDING_API_KEY: "k",
      EMBEDDING_MODEL: "text-embedding-3-large",
    })!;
    const [result] = await resolveEventEmbeddings(
      [{ content: "hello" }],
      provider,
    );
    expect(body(fetchMock)).toEqual({
      model: "text-embedding-3-large",
      input: ["hello"],
      dimensions: 1536,
    });
    expect(result).toMatchObject({
      embeddingModel: "text-embedding-3-large",
      embeddingDim: 1536,
      embeddingSkipReason: null,
    });
  });

  it("sends no dimensions to a model that does not take them", async () => {
    const fetchMock = stubVectors(1536);
    await new OpenAiEmbeddingProvider({
      apiKey: "k",
      model: "text-embedding-ada-002",
    }).embed("hello");
    expect(body(fetchMock)).toEqual({
      model: "text-embedding-ada-002",
      input: ["hello"],
    });
  });
});

describe("injected providers", () => {
  it("one failed embed skips only that input", async () => {
    vi.spyOn(console, "info").mockImplementation(() => {});
    const provider = {
      model: "custom",
      dim: 3,
      embed: async (text: string) => {
        if (text === "bad") throw new Error("provider down");
        return [1, 2, 3];
      },
    };
    const results = await resolveEventEmbeddings(
      [{ content: "good" }, { content: "bad" }, { content: "also good" }],
      provider,
    );
    expect(results.map((result) => result.embeddingSkipReason)).toEqual([
      null,
      "provider_error",
      null,
    ]);
    expect(results[0]!.embedding).toEqual([1, 2, 3]);
  });

  it("receives every input bounded to the provider limit", async () => {
    const seen: string[][] = [];
    const provider = {
      model: "custom",
      dim: 3,
      embed: async () => [1, 2, 3],
      embedMany: async (texts: string[]) => {
        seen.push(texts);
        return texts.map(() => [1, 2, 3]);
      },
    };
    const long = "x".repeat(MAX_EMBEDDING_INPUT_CHARS + 500);
    const [result] = await resolveEventEmbeddings(
      [{ content: long }],
      provider,
    );
    expect(seen).toEqual([[long.slice(0, MAX_EMBEDDING_INPUT_CHARS)]]);
    // The hash still identifies the whole stored content.
    expect(result!.contentHash).toBe(
      (await resolveEventEmbeddings([{ content: long }], null))[0]!.contentHash,
    );
  });
});

describe("resolveEventEmbedding", () => {
  it("records provider_not_configured without failing", async () => {
    const info = vi.spyOn(console, "info").mockImplementation(() => {});
    const result = await resolveEventEmbedding("scrubbed text", null, "evt-1");
    expect(result.embedding).toBeNull();
    expect(result.embeddingSkipReason).toBe("provider_not_configured");
    expect(result.contentHash).toHaveLength(64);
    expect(info).toHaveBeenCalledWith(
      `${EMBEDDING_SKIP_LOG_PREFIX} provider_not_configured eventId=evt-1`,
    );
  });
});

/**
 * The provider must never hand the API an input longer than the model accepts.
 *
 * `text-embedding-3-small` caps at 8,192 tokens and rejects the WHOLE request when any
 * single input is over — and the batched path marks every entry of a failed batch
 * `provider_error`. So one oversized record costs 64 embeddings, not one.
 *
 * Measured on production data: 16,591 of 357,935 events (4.6%) exceed the limit, which
 * puts a batch of 64 at ~95% odds of containing one. Without this bound a full backfill
 * embeds ~5% of the store while every signal stays green.
 */
describe("input bounding", () => {
  const vector = () => Array.from({ length: 1536 }, () => 0.1);

  function stubFetch(capture: { body?: any }) {
    const fetchMock = vi.fn(async (_url: string, init: any) => {
      capture.body = JSON.parse(init.body);
      const inputs = capture.body.input as string[];
      // Model the real API: reject the entire request if ANY input is over the cap.
      // Without this the test would pass against a provider that never truncates.
      if (inputs.some((text) => text.length > MAX_EMBEDDING_INPUT_CHARS)) {
        return new Response("input too long", { status: 400 });
      }
      return new Response(
        JSON.stringify({
          data: inputs.map((_text, index) => ({ index, embedding: vector() })),
        }),
        { status: 200, headers: { "content-type": "application/json" } },
      );
    });
    vi.stubGlobal("fetch", fetchMock);
    return fetchMock;
  }

  it("truncates an over-length input instead of letting the request 400", async () => {
    const capture: { body?: any } = {};
    stubFetch(capture);
    const provider = new OpenAiEmbeddingProvider({ apiKey: "k" });

    const embedding = await provider.embed(
      "x".repeat(MAX_EMBEDDING_INPUT_CHARS * 3),
    );

    expect(embedding).toHaveLength(1536);
    expect(capture.body.input[0].length).toBe(MAX_EMBEDDING_INPUT_CHARS);
  });

  it("leaves an input at or under the cap byte-for-byte unchanged", async () => {
    const capture: { body?: any } = {};
    stubFetch(capture);
    const provider = new OpenAiEmbeddingProvider({ apiKey: "k" });
    const text = "a genuine prompt that is well under the limit";

    await provider.embed(text);

    expect(capture.body.input[0]).toBe(text);
    expect(boundEmbeddingInput(text)).toBe(text);
  });

  /**
   * The regression this exists for. One oversized record in a batch used to take every
   * other embedding in that batch with it — and it failed silently, as a per-event
   * `provider_error` on all 64.
   */
  it("one oversized record does not cost its whole batch their vectors", async () => {
    const capture: { body?: any } = {};
    stubFetch(capture);
    const provider = new OpenAiEmbeddingProvider({ apiKey: "k" });

    const texts = Array.from({ length: EMBEDDING_BATCH_SIZE }, (_unused, i) =>
      i === 17 ? "y".repeat(MAX_EMBEDDING_INPUT_CHARS * 10) : `record ${i}`,
    );

    const embeddings = await provider.embedMany(texts);

    // All 64 come back, including the 63 that were never the problem.
    expect(embeddings).toHaveLength(EMBEDDING_BATCH_SIZE);
    expect(embeddings.every((e) => e.length === 1536)).toBe(true);
    // The neighbours were not truncated as collateral.
    expect(capture.body.input[0]).toBe("record 0");
    expect(capture.body.input[17].length).toBe(MAX_EMBEDDING_INPUT_CHARS);
  });
});

/**
 * The request-level bound, which the per-input bound does not cover.
 *
 * The provider caps a request at 300,000 tokens across ALL inputs, separately from the
 * 8,192-token per-input cap. Bounding only each input still let 64 large records add up
 * to ~400-500k tokens and fail the whole request — and because a failed batch marks every
 * entry `provider_error`, small records batched alongside them died too.
 *
 * Production numbers before this bound: failed rows averaged 83,677 characters against
 * 3,360 for rows that embedded, 762 failures against 3,989 successes.
 */
describe("request-level batching", () => {
  const vector = () => Array.from({ length: 1536 }, () => 0.1);

  function stubFetch(requests: string[][]) {
    vi.stubGlobal(
      "fetch",
      vi.fn(async (_url: string, init: any) => {
        const inputs = JSON.parse(init.body).input as string[];
        requests.push(inputs);
        const chars = inputs.reduce((sum, text) => sum + text.length, 0);
        // Model the real request-level cap. Without this the test passes against an
        // implementation that only bounds each input.
        if (chars > MAX_EMBEDDING_BATCH_CHARS) {
          return new Response("max tokens per request exceeded", {
            status: 400,
          });
        }
        return new Response(
          JSON.stringify({
            data: inputs.map((_t, index) => ({ index, embedding: vector() })),
          }),
          { status: 200, headers: { "content-type": "application/json" } },
        );
      }),
    );
  }

  it("splits on total characters, not just input count", async () => {
    const requests: string[][] = [];
    stubFetch(requests);
    const provider = new OpenAiEmbeddingProvider({ apiKey: "k" });

    // 20 records that are individually legal but collectively over the request cap.
    const events = Array.from({ length: 20 }, (_u, i) => ({
      eventId: `e${i}`,
      content: "z".repeat(MAX_EMBEDDING_INPUT_CHARS),
    }));

    const results = await resolveEventEmbeddings(events, provider);

    expect(requests.length).toBeGreaterThan(1);
    for (const inputs of requests) {
      const chars = inputs.reduce((sum, text) => sum + text.length, 0);
      expect(chars).toBeLessThanOrEqual(MAX_EMBEDDING_BATCH_CHARS);
    }
    // Every record gets a vector; none is collateral damage.
    expect(results.filter((r) => r.embedding !== null)).toHaveLength(20);
    expect(
      results.some((r) => r.embeddingSkipReason === "provider_error"),
    ).toBe(false);
  });

  it("still fills a normal batch to the input cap", async () => {
    const requests: string[][] = [];
    stubFetch(requests);
    const provider = new OpenAiEmbeddingProvider({ apiKey: "k" });

    // Ordinary records (~3.4k chars in production) must not batch more slowly.
    const events = Array.from({ length: 64 }, (_u, i) => ({
      eventId: `e${i}`,
      content: "a".repeat(1_000),
    }));

    await resolveEventEmbeddings(events, provider);

    expect(requests).toHaveLength(1);
    expect(requests[0]).toHaveLength(64);
  });
});

import { deliveryRecordDigest } from "ai-hist";
import { describe, expect, it } from "vitest";
import { parseEndpoint } from "../src/config.js";
import { createLogger, silentLogger } from "../src/log.js";
import { deliveryRecord, selected } from "../src/records.js";
import { consumerName, cutBatches, upload } from "../src/uploader.js";
import {
  ACCOUNT,
  LIMITS,
  MemoryFeed,
  change,
  config,
  fakeServer,
  json,
  noSleep,
  receiptFor,
} from "./support.js";

function server(
  answer: (body: any, attempt: number) => Response = (body) =>
    json(receiptFor(body)),
) {
  let posts = 0;
  return fakeServer(({ url, method, body }) => {
    if (url.endsWith("/v1/delivery/limits")) return json(LIMITS);
    if (method === "POST" && url.endsWith("/v1/delivery/batches"))
      return answer(body, ++posts);
    return json({ error: { code: "not_found" } }, 404);
  });
}

describe("selection", () => {
  const selection = config().selection;
  it("admits named sessions, whole sources and nothing else", () => {
    expect(selected(change(1), selection)).toBe(true);
    expect(selected(change(2, { sessionId: "other" }), selection)).toBe(false);
    expect(
      selected(change(3, { sessionId: "other" }), {
        ...selection,
        sources: ["claude"],
      }),
    ).toBe(true);
    expect(
      selected(
        change(4, { sourceName: "codex", sessionId: "picked" }),
        selection,
      ),
    ).toBe(false);
  });

  it("never sends an excluded session, even under a whole-source selection", () => {
    const excluding = {
      ...selection,
      all_sources: true,
      excluded_sessions: [{ source: "claude", session_id: "private" }],
    };
    expect(selected(change(1, { sessionId: "private" }), excluding)).toBe(
      false,
    );
    expect(
      selected(change(2, { sessionId: "private", op: "delete" }), excluding),
    ).toBe(false);
    const relationship = change(3, {
      kind: "relationship",
      sessionId: "picked",
      columns: { parent_session_id: "picked", child_session_id: "private" },
    });
    expect(selected(relationship, excluding)).toBe(false);
  });

  it("a relationship deletion follows its parent and names no child", () => {
    const withRelationships = {
      ...selection,
      kinds: [...selection.kinds, "relationship" as const],
      excluded_sessions: [{ source: "claude", session_id: "private" }],
    };
    const deletion = change(1, {
      kind: "relationship",
      op: "delete",
      sessionId: "picked",
    });
    expect(selected(deletion, withRelationships)).toBe(true);
    const record = deliveryRecord(deletion, "00000000000000aa");
    expect(record.payload).toBeNull();
    expect(JSON.stringify(record)).not.toContain("private");
    expect(
      selected({ ...deletion, sessionId: "other" }, withRelationships),
    ).toBe(false);
  });

  it("forwards a deletion only when the selection can attribute it", () => {
    const withHistory = {
      ...selection,
      kinds: [...selection.kinds, "history" as const],
    };
    const unattributed = change(1, {
      kind: "history",
      sessionId: "",
      op: "delete",
    });
    expect(selected(unattributed, withHistory)).toBe(false);
    expect(
      selected(unattributed, { ...withHistory, sources: ["claude"] }),
    ).toBe(true);
    expect(selected(change(2, { op: "delete" }), selection)).toBe(true);
  });

  it("admits only the selected kinds", () => {
    expect(selected(change(1, { kind: "tool_call" }), selection)).toBe(false);
    expect(
      selected(change(2, { kind: "tool_call" }), {
        ...selection,
        kinds: ["tool_call"],
      }),
    ).toBe(true);
  });
});

describe("delivery records", () => {
  it("carry export identities and null tombstone payloads", () => {
    const upsert = deliveryRecord(change(7), "00000000000000aa");
    expect(upsert).toMatchObject({
      schema_version: 1,
      revision: 7,
      operation: "upsert",
      session_id: "picked",
    });
    expect(upsert.record_id).toMatch(/^[0-9a-f]{64}$/);
    const tombstone = deliveryRecord(
      change(9, { op: "delete", recordKey: "m-7", key: change(7).key }),
      "00000000000000aa",
    );
    expect(tombstone.record_id).toBe(upsert.record_id);
    expect(tombstone.revision_id).not.toBe(upsert.revision_id);
    expect(tombstone.payload).toBeNull();
    expect(
      deliveryRecord(change(1, { sessionId: "" }), "00000000000000aa")
        .session_id,
    ).toBeNull();
  });
});

describe("consumerName", () => {
  it("isolates endpoints, accounts and selections and never embeds the token", () => {
    const base = config();
    const name = consumerName(base);
    expect(name).toMatch(/^relayhistory-upload:[0-9a-f]{32}$/);
    expect(name).not.toContain(base.token);
    expect(
      consumerName({
        ...base,
        endpoint: parseEndpoint("https://other.example.com"),
      }),
    ).not.toBe(name);
    expect(
      consumerName({ ...base, accountId: `relayhistory:${"b".repeat(64)}` }),
    ).not.toBe(name);
    expect(
      consumerName({
        ...base,
        selection: {
          ...base.selection,
          sessions: [
            ...base.selection.sessions,
            { source: "claude", session_id: "more" },
          ],
        },
      }),
    ).not.toBe(name);
    const rotated = { ...base, token: "rth_st_rotated" };
    expect(consumerName(rotated)).toBe(name);
    expect(
      consumerName({
        ...base,
        selection: { ...base.selection, kinds: ["session_event", "session"] },
      }),
    ).toBe(name);
    expect(
      consumerName({
        ...base,
        endpoint: parseEndpoint("https://history.example.com/"),
      }),
    ).toBe(name);
  });
});

describe("cutBatches", () => {
  const records = Array.from({ length: 5 }, (_, i) =>
    deliveryRecord(change(i + 1), "00000000000000aa"),
  );
  it("respects the record bound and is deterministic", () => {
    const first = cutBatches(config(), "c", "00000000000000aa", records, {
      maxRecords: 2,
      maxBytes: 1_048_576,
    });
    expect(first.map((b) => b.records.length)).toEqual([2, 2, 1]);
    const again = cutBatches(config(), "c", "00000000000000aa", records, {
      maxRecords: 2,
      maxBytes: 1_048_576,
    });
    expect(again.map((b) => b.batch_id)).toEqual(first.map((b) => b.batch_id));
    expect(new Set(first.map((b) => b.batch_id)).size).toBe(3);
  });

  it("respects the byte bound and refuses a record that cannot fit", () => {
    const one = JSON.stringify({
      protocolVersion: 1,
      batch: cutBatches(
        config(),
        "c",
        "00000000000000aa",
        records.slice(0, 1),
        { maxRecords: 10, maxBytes: 1_048_576 },
      )[0],
    }).length;
    const batches = cutBatches(config(), "c", "00000000000000aa", records, {
      maxRecords: 10,
      maxBytes: one + 10,
    });
    expect(batches.every((b) => b.records.length === 1)).toBe(true);
    for (const batch of batches)
      expect(
        JSON.stringify({ protocolVersion: 1, batch }).length,
      ).toBeLessThanOrEqual(one + 10);
    expect(() =>
      cutBatches(config(), "c", "00000000000000aa", records, {
        maxRecords: 10,
        maxBytes: 600,
      }),
    ).toThrow(/exceeds/);
  });
});

describe("upload", () => {
  it("sends selected changes and commits only after the durable receipt", async () => {
    const feed = new MemoryFeed([
      change(1),
      change(2, { sessionId: "other" }),
      change(3),
      change(4, { kind: "session", sessionId: "picked" }),
    ]);
    const fake = server();
    const summary = await upload({
      config: config(),
      log: silentLogger,
      feed,
      fetch: fake.fetch,
      sleep: noSleep,
    });
    const posts = fake.requests.filter((r) => r.method === "POST");
    expect(fake.requests.map((r) => r.url)).toEqual([
      "https://history.example.com/v1/delivery/limits",
      "https://history.example.com/v1/delivery/batches",
    ]);
    expect(posts).toHaveLength(1);
    expect(posts[0].headers.get("authorization")).toBe(
      "Bearer rth_st_secret-token-value",
    );
    expect(posts[0].body.batch.records.map((r: any) => r.revision)).toEqual([
      1, 3, 4,
    ]);
    expect(posts[0].body.batch).toMatchObject({
      account_id: ACCOUNT,
      origin_id: feed.epoch,
      mapping_version: "relayhistory-delivery-v1",
      instance_id: "laptop",
    });
    expect(summary).toMatchObject({
      scanned: 4,
      selected: 3,
      accepted: 3,
      quarantined: 0,
      originId: feed.epoch,
    });
    expect(feed.commits).toEqual([
      {
        consumer: summary.consumer,
        position: { epoch: feed.epoch, revision: 4 },
      },
    ]);

    // Drained: a second run sends nothing.
    const again = await upload({
      config: config(),
      log: silentLogger,
      feed,
      fetch: fake.fetch,
      sleep: noSleep,
    });
    expect(again.scanned).toBe(0);
    expect(fake.requests.filter((r) => r.method === "POST")).toHaveLength(1);
  });

  it("advances past a page with nothing selected without contacting the server", async () => {
    const feed = new MemoryFeed([change(1, { sessionId: "other" })]);
    const fake = server();
    await upload({
      config: config(),
      log: silentLogger,
      feed,
      fetch: fake.fetch,
      sleep: noSleep,
    });
    expect(fake.requests).toHaveLength(0);
    expect(feed.cursors.get(consumerName(config()))).toBe(1);
  });

  it("retries transient failures with backoff, honoring Retry-After, then commits", async () => {
    const feed = new MemoryFeed([change(1)]);
    const waits: number[] = [];
    const fake = server((body, attempt) =>
      attempt === 1
        ? json({ error: { code: "delivery_unavailable" } }, 503)
        : attempt === 2
          ? json({ error: { code: "rate_limited" } }, 429, {
              "retry-after": "2",
            })
          : json(receiptFor(body)),
    );
    await upload({
      config: config(),
      log: silentLogger,
      feed,
      fetch: fake.fetch,
      sleep: async (ms) => void waits.push(ms),
      random: () => 0.5,
    });
    expect(waits).toHaveLength(2);
    expect(waits[1]).toBeGreaterThanOrEqual(1_500);
    const posts = fake.requests.filter((r) => r.method === "POST");
    expect(new Set(posts.map((p) => p.body.batch.batch_id)).size).toBe(1);
    expect(feed.commits).toHaveLength(1);
  });

  it.each([
    [
      "an interrupted body",
      () =>
        new Response(
          new ReadableStream({
            start(controller) {
              controller.enqueue(
                new TextEncoder().encode('{"protocolVersion":1,'),
              );
              controller.error(new Error("socket hang up"));
            },
          }),
          { status: 200 },
        ),
    ],
    [
      "a truncated body",
      () => new Response('{"protocolVersion":1,"receiptId"', { status: 200 }),
    ],
    ["an empty body", () => new Response("", { status: 200 })],
  ])(
    "treats %s on a success as lost and resends the same batch",
    async (_, broken) => {
      const feed = new MemoryFeed([change(1)]);
      const fake = server((body, attempt) =>
        attempt === 1 ? broken() : json(receiptFor(body)),
      );
      await upload({
        config: config(),
        log: silentLogger,
        feed,
        fetch: fake.fetch,
        sleep: noSleep,
      });
      const posts = fake.requests.filter((r) => r.method === "POST");
      expect(posts).toHaveLength(2);
      expect(posts[1].body.batch.batch_id).toBe(posts[0].body.batch.batch_id);
      expect(feed.commits).toHaveLength(1);
    },
  );

  it("refuses a well-formed but empty JSON success instead of resending it", async () => {
    const feed = new MemoryFeed([change(1)]);
    const fake = server(() => json(null));
    await expect(
      upload({
        config: config(),
        log: silentLogger,
        feed,
        fetch: fake.fetch,
        sleep: noSleep,
      }),
    ).rejects.toMatchObject({ failure: "invalid_response" });
    expect(fake.requests.filter((r) => r.method === "POST")).toHaveLength(1);
    expect(feed.commits).toHaveLength(0);
  });

  it("retries a 408 like any other transient failure", async () => {
    const feed = new MemoryFeed([change(1)]);
    const fake = server((body, attempt) =>
      attempt === 1
        ? json({ error: { code: "timeout" } }, 408)
        : json(receiptFor(body)),
    );
    await upload({
      config: config(),
      log: silentLogger,
      feed,
      fetch: fake.fetch,
      sleep: noSleep,
    });
    expect(feed.commits).toHaveLength(1);
  });

  it("an idle run reports the stored cursor, never an uncommitted read position", async () => {
    const feed = new MemoryFeed([change(1)]);
    const fake = server();
    await upload({
      config: config(),
      log: silentLogger,
      feed,
      fetch: fake.fetch,
      sleep: noSleep,
    });
    // Unrelated evidence of an unselected kind moves the head; the kind-filtered page is
    // empty but positioned at the head.
    feed.changes.push(change(2, { kind: "tool_call" }));
    const idle = await upload({
      config: config(),
      log: silentLogger,
      feed,
      fetch: fake.fetch,
      sleep: noSleep,
    });
    const consumer = consumerName(config());
    expect(idle.scanned).toBe(0);
    expect(idle.cursor).toEqual({
      epoch: feed.epoch,
      revision: feed.cursors.get(consumer),
    });
    expect(feed.cursors.get(consumer)).toBe(2);
    expect(fake.requests.filter((r) => r.method === "POST")).toHaveLength(1);
  });

  it("reports no cursor when there is no local feed to commit", async () => {
    const feed = new MemoryFeed([]);
    const idle = await upload({
      config: config(),
      log: silentLogger,
      feed,
      fetch: server().fetch,
      sleep: noSleep,
    });
    expect(idle.cursor).toBeNull();
    expect(feed.commits).toHaveLength(0);
  });

  it("a dry run reports how far it read and commits nothing", async () => {
    const feed = new MemoryFeed([change(1), change(2, { kind: "tool_call" })]);
    const dry = await upload({
      config: config(),
      log: silentLogger,
      feed,
      fetch: server().fetch,
      dryRun: true,
    });
    expect(dry.cursor).toEqual({ epoch: feed.epoch, revision: 2 });
    expect(feed.commits).toHaveLength(0);
  });

  it("keeps the cursor when retries run out, and the next run resends the same batch", async () => {
    const feed = new MemoryFeed([change(1), change(2)]);
    const down = server(() =>
      json({ error: { code: "delivery_unavailable" } }, 503),
    );
    await expect(
      upload({
        config: config(),
        log: silentLogger,
        feed,
        fetch: down.fetch,
        sleep: noSleep,
        retry: { attempts: 3, baseMs: 1, maxMs: 1 },
      }),
    ).rejects.toMatchObject({ failure: "transient" });
    expect(feed.commits).toHaveLength(0);
    const failedBatch = down.requests.find((r) => r.method === "POST")!.body
      .batch.batch_id;
    const up = server();
    await upload({
      config: config(),
      log: silentLogger,
      feed,
      fetch: up.fetch,
      sleep: noSleep,
    });
    expect(
      up.requests.find((r) => r.method === "POST")!.body.batch.batch_id,
    ).toBe(failedBatch);
    expect(feed.commits).toHaveLength(1);
  });

  it.each([
    [
      "wrong batch",
      (body: any) => ({ ...receiptFor(body), batchId: "someone-else" }),
    ],
    [
      "missing revision",
      (body: any) => ({ ...receiptFor(body), acceptedRevisionIds: [] }),
    ],
    [
      "not durable",
      (body: any) => ({ ...receiptFor(body), acceptanceLevel: "indexed" }),
    ],
    [
      "unsupported records",
      (body: any) => ({
        ...receiptFor(body),
        unsupportedRevisionIds: [body.batch.records[0].revision_id],
      }),
    ],
  ])(
    "refuses a stale or partial receipt (%s) without committing",
    async (_, receipt) => {
      const feed = new MemoryFeed([change(1)]);
      const fake = server((body) => json(receipt(body)));
      await expect(
        upload({
          config: config(),
          log: silentLogger,
          feed,
          fetch: fake.fetch,
          sleep: noSleep,
        }),
      ).rejects.toMatchObject({ failure: "invalid_response" });
      expect(feed.commits).toHaveLength(0);
    },
  );

  it.each([
    [401, "invalid_token", "authentication_required"],
    [403, "delivery_account_mismatch", "permission_denied"],
    [403, "forbidden", "permission_denied"],
    [413, "delivery_too_large", "invalid_payload"],
    [422, "unsupported_mapping", "mapping_version_mismatch"],
  ])(
    "stops on %i %s without retrying or committing",
    async (status, code, failure) => {
      const feed = new MemoryFeed([change(1)]);
      const fake = server(() =>
        json({ error: { code, message: "x" } }, status),
      );
      const lines: string[] = [];
      await expect(
        upload({
          config: config(),
          log: createLogger((l) => lines.push(l)),
          feed,
          fetch: fake.fetch,
          sleep: noSleep,
        }),
      ).rejects.toMatchObject({ failure });
      expect(fake.requests.filter((r) => r.method === "POST")).toHaveLength(1);
      expect(feed.commits).toHaveLength(0);
      expect(lines.join("")).not.toContain("secret-token-value");
    },
  );

  it("quarantines only receiver-proven conflicts and delivers the rest", async () => {
    const feed = new MemoryFeed([change(1), change(2)]);
    const fake = server((body, attempt) => {
      if (attempt > 1) return json(receiptFor(body));
      const record = body.batch.records[0];
      const conflict = {
        type: "record_revision",
        originId: record.origin_id,
        recordId: record.record_id,
        submittedRevisionId: record.revision_id,
        submittedRevision: record.revision,
        submittedDigest: deliveryRecordDigest(record),
        currentRevisionId: record.revision_id,
        currentRevision: record.revision,
        currentDigest: "f".repeat(64),
      };
      return json(
        {
          error: {
            code: "delivery_conflict",
            message: "x",
            conflict,
            conflicts: [conflict],
            conflictCount: 1,
          },
        },
        409,
      );
    });
    const summary = await upload({
      config: config(),
      log: silentLogger,
      feed,
      fetch: fake.fetch,
      sleep: noSleep,
    });
    const posts = fake.requests.filter((r) => r.method === "POST");
    expect(posts).toHaveLength(2);
    expect(posts[1].body.batch.records.map((r: any) => r.revision)).toEqual([
      2,
    ]);
    expect(posts[1].body.batch.batch_id).not.toBe(posts[0].body.batch.batch_id);
    expect(summary).toMatchObject({ quarantined: 1, accepted: 1 });
    expect(feed.commits).toHaveLength(1);
  });

  it("refuses a conflict that does not name what was sent", async () => {
    const feed = new MemoryFeed([change(1)]);
    const fake = server((body) => {
      const record = body.batch.records[0];
      const conflict = {
        type: "record_revision",
        originId: record.origin_id,
        recordId: "b".repeat(64),
        submittedRevisionId: record.revision_id,
        submittedRevision: record.revision,
        submittedDigest: "c".repeat(64),
        currentRevisionId: record.revision_id,
        currentRevision: record.revision,
        currentDigest: "d".repeat(64),
      };
      return json(
        {
          error: {
            code: "delivery_conflict",
            message: "x",
            conflict,
            conflicts: [conflict],
            conflictCount: 1,
          },
        },
        409,
      );
    });
    await expect(
      upload({
        config: config(),
        log: silentLogger,
        feed,
        fetch: fake.fetch,
        sleep: noSleep,
      }),
    ).rejects.toMatchObject({ failure: "delivery_conflict" });
    expect(feed.commits).toHaveLength(0);
  });

  it("splits pages into batches within the server's advertised limit", async () => {
    const feed = new MemoryFeed(
      Array.from({ length: 7 }, (_, i) => change(i + 1)),
    );
    const fake = fakeServer(({ url, body }) =>
      url.endsWith("/limits")
        ? json({ maxRecords: 3, maxRequestBytes: 2_097_152 })
        : json(receiptFor(body)),
    );
    await upload({
      config: config(),
      log: silentLogger,
      feed,
      fetch: fake.fetch,
      sleep: noSleep,
    });
    expect(
      fake.requests
        .filter((r) => r.method === "POST")
        .map((r) => r.body.batch.records.length),
    ).toEqual([3, 3, 1]);
  });

  it("a dry run reports what it would send without contacting the server or moving the cursor", async () => {
    const feed = new MemoryFeed([
      change(1),
      change(2, { sessionId: "other" }),
      change(3),
    ]);
    const fake = server();
    const summary = await upload({
      config: config(),
      log: silentLogger,
      feed,
      fetch: fake.fetch,
      dryRun: true,
    });
    expect(fake.requests).toHaveLength(0);
    expect(feed.commits).toHaveLength(0);
    expect(summary).toMatchObject({
      scanned: 3,
      selected: 2,
      sessions: { "claude/picked": 2 },
    });
  });

  it("stops when the local store is replaced mid-run", async () => {
    const feed = new MemoryFeed(
      Array.from({ length: 3 }, (_, i) => change(i + 1)),
    );
    const original = feed.getChangesPage;
    let pages = 0;
    feed.getChangesPage = async (options) => {
      const page = await original({ ...options, limit: 1 });
      if (++pages === 2) {
        feed.epoch = "00000000000000bb";
        return {
          ...page,
          head: { ...page.head, epoch: feed.epoch },
          position: { ...page.position, epoch: feed.epoch },
        };
      }
      return page;
    };
    const fake = server();
    await expect(
      upload({
        config: config(),
        log: silentLogger,
        feed,
        fetch: fake.fetch,
        sleep: noSleep,
      }),
    ).rejects.toThrow(/replaced/);
    expect(feed.commits).toHaveLength(1);
  });
});

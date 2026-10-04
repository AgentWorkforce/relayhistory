import { afterEach, beforeEach, describe, expect, it } from "vitest";
import type { AuthContext } from "../src/env.js";
import { acceptDelivery, deliveryAccount } from "../src/lib/delivery.js";
import {
  MAX_DELIVERY_RECORDS,
  type HistoryExportRecord,
} from "../src/lib/delivery-contracts.js";
import { getSessionEvents, listSessions } from "../src/lib/recall.js";
import {
  migrationStatements,
  readMigrations,
  rolloutDeliveryProjection,
  type Migration,
} from "../src/migrate/index.js";
import {
  createFreshDatabase,
  type FreshDatabase,
} from "./support/fresh-database.js";

/**
 * Migration 0023: the delivery projection counts usage once per request, dedupes one
 * logical record across origins, skips control rows, and fills the typed columns that
 * rollups, recall and session analysis read. Everything runs the real migration SQL.
 */
const migrations: Migration[] = readMigrations();
const auth = {
  orgId: "org",
  workspaceId: "w",
  userId: "u",
  tokenSubject: "u",
  scopes: ["rth:read"],
  claims: {},
} as AuthContext;
let client: FreshDatabase;
let db: any;

/** Runs the 0029 delivery projection rollout to completion, as a deploy does. */
async function rollout(database: FreshDatabase) {
  const outcome = await rolloutDeliveryProjection(
    async (sql) => (await database.query(sql)).rows,
  );
  if (!outcome.complete) throw new Error("delivery rollout did not complete");
}

async function run(input = migrations) {
  await client.transaction(async (query) => {
    for (const statement of migrationStatements(input)) await query(statement);
  });
  // A deploy runs the 0029 rollout after its migrations; the projections go live there.
  if (input.some((m) => m.name.startsWith("0029_"))) await rollout(client);
}

interface Put {
  id: string;
  kind?: string;
  payload?: Record<string, unknown> | null;
  origin?: string;
  session?: string;
  source?: string;
  revision?: number;
  receivedAt?: string;
}
/** The same upsert the delivery function performs, with an explicit delivery time. */
async function put({
  id,
  kind = "session_event",
  payload = {},
  origin = "origin-a",
  session = "s",
  source = "claude",
  revision = 1,
  receivedAt = "2026-09-01T00:00:00Z",
}: Put) {
  await client.query(
    `INSERT INTO sessions.delivery_records
      (org_id, workspace_id, origin_id, record_id, revision_id, revision, digest, kind, source, session_id, operation, payload, user_id, received_at)
     VALUES ('org', 'w', $1, $2, $2 || ':' || $3::text, $3::bigint, 'digest', $4, $5, $6, $7, $8::jsonb, 'u', $9::timestamptz)
     ON CONFLICT (org_id, workspace_id, origin_id, record_id) DO UPDATE SET
       revision = excluded.revision, kind = excluded.kind, source = excluded.source,
       session_id = excluded.session_id, operation = excluded.operation, payload = excluded.payload,
       received_at = excluded.received_at`,
    [
      origin,
      id,
      revision,
      kind,
      source,
      session,
      payload === null ? "delete" : "upsert",
      payload === null ? null : JSON.stringify(payload),
      receivedAt,
    ],
  );
}

async function projected(where = "true") {
  return (
    await client.query<Record<string, any>>(
      `SELECT delivery_record_id AS id, machine_id, kind, content, project_id, subagent_id, provider,
              tool_name, tool_status, tool_calls, files_touched, request_key,
              input_tokens::int AS input, output_tokens::int AS output, reasoning_tokens::int AS reasoning,
              cache_read_tokens::int AS cache_read, cache_create_tokens::int AS cache_create,
              cache_create_5m_tokens::int AS cache_5m, cache_create_1h_tokens::int AS cache_1h,
              cost_usd_micros::int AS cost, extract(epoch FROM ts)::float8 AS seconds
         FROM sessions.convergence_events WHERE ${where} ORDER BY delivery_record_id`,
    )
  ).rows;
}

async function totals() {
  return (
    await client.query<Record<string, number>>(
      `SELECT coalesce(sum(input_tokens), 0)::int AS input, coalesce(sum(output_tokens), 0)::int AS output,
              coalesce(sum(cache_read_tokens), 0)::int AS cache_read,
              coalesce(sum(cache_create_tokens), 0)::int AS cache_create,
              coalesce(sum(reasoning_tokens), 0)::int AS reasoning
         FROM sessions.convergence_events`,
    )
  ).rows[0];
}

const claudeUsage = JSON.stringify({
  cache_creation: {
    ephemeral_1h_input_tokens: 4773,
    ephemeral_5m_input_tokens: 0,
  },
  cache_creation_input_tokens: 4773,
  cache_read_input_tokens: 11496,
  input_tokens: 3,
  output_tokens: 43,
});
function block(
  id: string,
  extra: Record<string, unknown> = {},
): Put & { payload: Record<string, unknown> } {
  return {
    id,
    payload: {
      role: "assistant",
      kind: "text",
      text: `block ${id}`,
      message_id: `uuid-${id}`,
      request_id: "req_1",
      provider_message_id: "msg_1",
      provider: "anthropic",
      model: "claude-opus",
      token_json: claudeUsage,
      ts_ms: 1_000_000,
      ...extra,
    },
  };
}

/**
 * One backfilled session as a client drains it: a catalog row, a prompt, 20
 * Claude requests copied onto three content blocks each (one naming a tool use),
 * their 20 tool calls, and 18 file edits. 100 records, 99 of them activity.
 */
function backfillSession(session: string): HistoryExportRecord[] {
  const entry = (
    kind: HistoryExportRecord["kind"],
    id: string,
    payload: Record<string, unknown>,
  ): HistoryExportRecord => ({
    schema_version: 1,
    origin_id: "origin-a",
    record_id: `${session}:${id}`,
    revision_id: `${session}:${id}:1`,
    revision: 1,
    kind,
    source: "claude",
    session_id: session,
    operation: "upsert",
    payload,
  });
  const records = [
    entry("session", "session", {
      source: "claude",
      session_id: session,
      cwd: "/work/repo",
      project_key: "github.com/example/repo",
      first_activity_ms: 1_000_000,
    }),
    entry("history", "prompt", {
      source: "claude",
      session_id: session,
      prompt: "backfill this history",
      timestamp_ms: 1_000_000,
    }),
  ];
  for (let request = 0; request < 20; request++) {
    for (let piece = 0; piece < 3; piece++)
      records.push(
        entry("session_event", `r${request}b${piece}`, {
          ...block(`${session}-${request}-${piece}`).payload,
          request_id: `req_${request}`,
          provider_message_id: `msg_${request}`,
          ts_ms: 1_000_000 + request * 1_000 + piece,
          ...(piece === 2 ? { tool_use_id: `toolu_${request}` } : {}),
        }),
      );
    records.push(
      entry("tool_call", `t${request}`, {
        name: "Edit",
        target: `src/file-${request}.ts`,
        tool_use_id: `toolu_${request}`,
        args_json: JSON.stringify({ file_path: `src/file-${request}.ts` }),
        is_error: 0,
      }),
    );
    if (request < 18)
      records.push(
        entry("file_edit", `e${request}`, {
          file_path: `src/file-${request}.ts`,
          tool_use_id: `toolu_${request}`,
        }),
      );
  }
  return records;
}

beforeEach(async () => {
  client = await createFreshDatabase();
  await run();
  db = client.db;
});
afterEach(async () => {
  await client.close();
});

describe("delivery projection v2", () => {
  it("counts a Claude request copied onto every content block exactly once, in any arrival order", async () => {
    // Arrival order is deliberately not record-id order.
    await put(block("r3"));
    await put(block("r1"));
    await put(block("r2"));
    const rows = await projected();
    expect(rows.map((row) => [row.id, row.input, row.output])).toEqual([
      ["r1", 3, 43],
      ["r2", 0, 0],
      ["r3", 0, 0],
    ]);
    expect(rows[0]).toMatchObject({
      request_key: "request-id:req_1",
      cache_read: 11496,
      cache_create: 4773,
      provider: "anthropic",
    });
    expect(await totals()).toEqual({
      input: 3,
      output: 43,
      cache_read: 11496,
      cache_create: 4773,
      reasoning: 0,
    });

    const sessions = await listSessions(db, auth, {});
    expect(sessions.sessions).toHaveLength(1);
    expect(sessions.sessions[0]).toMatchObject({
      totalInputTokens: 3,
      totalOutputTokens: 43,
      // The 5m/1h split survives, and no reported cost reads back as unknown.
      totalCacheCreate5mTokens: 0,
      totalCacheCreate1hTokens: 4773,
      totalCostUsdMicros: null,
    });
    expect(rows[0]).toMatchObject({ cache_5m: 0, cache_1h: 4773, cost: null });
    expect(rows[1]).toMatchObject({
      cache_5m: null,
      cache_1h: null,
      cost: null,
    });

    // Removing the carrier moves the one measurement to the next canonical row.
    await put({ id: "r1", payload: null, revision: 2 });
    expect(
      (await projected()).map((row) => [row.id, row.input, row.output]),
    ).toEqual([
      ["r2", 3, 43],
      ["r3", 0, 0],
    ]);
    // A later revision that gains a different request id leaves the old group intact.
    await put({
      ...block("r3", { request_id: "req_2" }),
      revision: 2,
    });
    expect(
      (await projected()).map((row) => [row.id, row.request_key, row.output]),
    ).toEqual([
      ["r2", "request-id:req_1", 43],
      ["r3", "request-id:req_2", 43],
    ]);
  });

  it("refuses disagreeing copies and Claude requests keyed only by block uuid", async () => {
    await put(block("a1"));
    await put(
      block("a2", {
        token_json: JSON.stringify({ input_tokens: 9, output_tokens: 1 }),
      }),
    );
    // No request id, provider message id or span: Claude splits a request across uuids.
    await put(
      block("b1", {
        request_id: null,
        provider_message_id: null,
        message_id: "uuid-b1",
      }),
    );
    expect(await totals()).toMatchObject({ input: 0, output: 0 });
    expect((await projected("delivery_record_id = 'b1'"))[0].request_key).toBe(
      "record-id:uuid-b1",
    );
  });

  it("normalizes Codex, Grok and Muse usage to cache-exclusive input", async () => {
    await put({
      id: "codex-1",
      source: "codex",
      session: "codex",
      payload: {
        role: "assistant",
        message_id: "m1",
        request_span: "3",
        token_json: JSON.stringify({
          input_tokens: 1000,
          cached_input_tokens: 400,
          output_tokens: 50,
          reasoning_output_tokens: 20,
          total_tokens: 1050,
        }),
      },
    });
    await put({
      id: "grok-1",
      source: "grok",
      session: "grok",
      payload: {
        role: "assistant",
        message_id: "g1",
        request_span: "1",
        token_json: JSON.stringify({
          context_total_tokens: 18432,
          usage: {
            inputTokens: 1000,
            outputTokens: 100,
            reasoningTokens: 20,
            cachedReadTokens: 400,
          },
        }),
      },
    });
    await put({
      id: "grok-2",
      source: "grok",
      session: "grok",
      payload: {
        role: "assistant",
        message_id: "g2",
        request_span: "2",
        token_json: JSON.stringify({ context_total_tokens: 9210 }),
      },
    });
    await put({
      id: "codex-bad",
      source: "codex",
      session: "codex",
      payload: {
        role: "assistant",
        message_id: "m2",
        request_span: "4",
        token_json: JSON.stringify({ input_tokens: -1, output_tokens: 5 }),
      },
    });
    const rows = await projected();
    expect(
      rows.map((row) => [
        row.id,
        row.input,
        row.output,
        row.reasoning,
        row.cache_read,
      ]),
    ).toEqual([
      ["codex-1", 600, 50, 20, 400],
      ["codex-bad", 0, 0, 0, 0],
      ["grok-1", 600, 100, 20, 400],
      ["grok-2", 0, 0, 0, 0],
    ]);
    // Grok usage is only the nested `usage` counters: top-level counters beside an
    // empty breakdown report no usage rather than a fabricated zero.
    const grok = async (token: object) =>
      (
        await client.query<{ usage: unknown }>(
          "SELECT sessions.delivery_normalized_usage('grok', $1) AS usage",
          [JSON.stringify(token)],
        )
      ).rows[0]!.usage;
    expect(
      await grok({ output_tokens: 5, total_tokens: 18432, usage: {} }),
    ).toBeNull();
    expect(await grok({ usage: { totalTokens: 10 } })).toMatchObject({
      input: 0,
      output: 0,
    });
  });

  it("dedupes one record delivered by two origins, newest write winning, tombstones included", async () => {
    await put({ ...block("x"), origin: "origin-a" });
    await put({
      ...block("x", { text: "rebuilt copy" }),
      origin: "origin-b",
      receivedAt: "2026-09-02T00:00:00Z",
    });
    let rows = await projected();
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({ content: "rebuilt copy", output: 43 });
    const machineB = rows[0].machine_id;
    expect((await totals()).output).toBe(43);

    // A later write wins even when its received_at is older: received_at is the
    // accepting transaction's start, and a writer that waited on the projection lock
    // carries an older stamp than the write it follows.
    await put({
      ...block("x", { text: "later write, older stamp" }),
      origin: "origin-a",
      revision: 2,
      receivedAt: "2026-09-01T12:00:00Z",
    });
    rows = await projected();
    expect(rows).toHaveLength(1);
    expect(rows[0].content).toBe("later write, older stamp");
    expect(rows[0].machine_id).not.toBe(machineB);
    expect((await totals()).output).toBe(43);

    // The newest statement about the record is a delete: nothing is projected, even
    // though origin-a still holds a live copy with a newer stamp.
    await put({
      id: "x",
      payload: null,
      origin: "origin-b",
      revision: 2,
      receivedAt: "2026-08-01T00:00:00Z",
    });
    expect(await projected()).toEqual([]);

    // A later live write from any origin brings it back, once.
    await put({
      ...block("x", { text: "revived" }),
      origin: "origin-a",
      revision: 3,
      receivedAt: "2026-09-04T00:00:00Z",
    });
    rows = await projected();
    expect(rows.map((row) => [row.content, row.output])).toEqual([
      ["revived", 43],
    ]);
    expect((await getSessionEvents(db, auth, "s", {})).events).toHaveLength(1);
  });

  it("uses the canonical owner/repo project key so ?project= matches the ingest path", async () => {
    await put({
      id: "remote",
      payload: {
        role: "user",
        text: "fix it",
        project: "relayhistory",
        cwd: "/Users/x/relayhistory",
        project_key: "github.com/AgentWorkforce/relayhistory",
        project_key_method: "remote",
        ts_ms: 1000,
      },
    });
    await put({
      id: "path",
      session: "s2",
      payload: {
        role: "user",
        text: "local",
        project: "scratch",
        project_key: "/Users/x/scratch",
        project_key_method: "path",
        ts_ms: 1000,
      },
    });
    await put({
      id: "slug",
      session: "s3",
      payload: {
        role: "user",
        text: "slug",
        project_key: "AgentWorkforce/cloud",
        project_key_method: "git_remote",
        ts_ms: 1000,
      },
    });
    expect((await projected()).map((row) => [row.id, row.project_id])).toEqual([
      ["path", "scratch"],
      ["remote", "AgentWorkforce/relayhistory"],
      ["slug", "AgentWorkforce/cloud"],
    ]);
    const filtered = await listSessions(db, auth, {
      projectId: "AgentWorkforce/relayhistory",
    });
    expect(filtered.sessions.map((s: any) => s.sessionId)).toEqual(["s"]);
    // Cloud also asserts `listProjects` here; project rollups are hosted-only, not engine scope.
  });

  it("fills tool, file and subagent columns and takes a missing tool time from its parent event", async () => {
    // The tool call arrives before the event that issued it.
    await put({
      id: "tool",
      kind: "tool_call",
      payload: {
        name: "Read",
        target: "fixture.ts",
        tool_use_id: "toolu_1",
        args_json: JSON.stringify({ path: "fixture.ts" }),
        is_error: 1,
        ts_ms: null,
      },
      receivedAt: "2026-09-10T00:00:00Z",
    });
    expect((await projected("kind = 'tool_call'"))[0].seconds).toBe(
      Date.parse("2026-09-10T00:00:00Z") / 1000,
    );
    await put({
      id: "edit",
      kind: "file_edit",
      payload: {
        file_path: "src/app.ts",
        tool_name: "Edit",
        tool_use_id: "toolu_1",
      },
    });
    await put({
      id: "use",
      payload: {
        role: "assistant",
        kind: "tool_use",
        tool_use_id: "toolu_1",
        agent_id: "root-agent",
        subagent_session_id: null,
        ts_ms: 5_000,
      },
    });
    await put({
      id: "notice",
      payload: {
        role: "tool_result",
        kind: "tool_result",
        text: "subagent finished",
        subagent_session_id: "child-session",
        agent_id: "agent-1",
        ts_ms: 6_000,
      },
    });
    const rows = await projected();
    const byId = Object.fromEntries(rows.map((row) => [row.id, row]));
    expect(byId.tool).toMatchObject({
      tool_name: "Read",
      tool_status: "error",
      seconds: 5,
      tool_calls: [
        {
          toolUseId: "toolu_1",
          name: "Read",
          target: "fixture.ts",
          args: { path: "fixture.ts" },
          isError: true,
          status: "error",
        },
      ],
    });
    expect(byId.edit).toMatchObject({
      files_touched: ["src/app.ts"],
      tool_name: null,
      seconds: 5,
    });
    expect(byId.notice.subagent_id).toBe("child-session");
    // A root reply can carry its own agent_id; it is not subagent activity.
    expect(byId.use.subagent_id).toBeNull();

    // Removing the parent returns the tool row to its own delivery time.
    await put({ id: "use", payload: null, revision: 2 });
    expect((await projected("kind = 'tool_call'"))[0].seconds).toBe(
      Date.parse("2026-09-10T00:00:00Z") / 1000,
    );
  });

  it("removes the prior kind's projection when a revision reclassifies a record", async () => {
    await put(block("a"));
    await put(block("b"));
    await put(
      block("c", { request_id: "req_2", provider_message_id: "msg_2" }),
    );
    expect(
      (await projected()).map((row) => [row.id, row.kind, row.output]),
    ).toEqual([
      ["a", "session_event", 43],
      ["b", "session_event", 0],
      ["c", "session_event", 43],
    ]);

    // session_event -> tool_call: only the tool call remains, and the request group
    // it left is re-settled onto its remaining member.
    await put({
      id: "a",
      kind: "tool_call",
      revision: 2,
      payload: { name: "Read", target: "x.ts", ts_ms: 7_000 },
    });
    expect(
      (await projected()).map((row) => [row.id, row.kind, row.output]),
    ).toEqual([
      ["a", "tool_call", 0],
      ["b", "session_event", 43],
      ["c", "session_event", 43],
    ]);

    // session_event -> session (not activity): nothing is projected for it.
    await put({
      id: "c",
      kind: "session",
      revision: 2,
      payload: { session_id: "s", cwd: "/work" },
    });
    expect(
      (await projected()).map((row) => [row.id, row.kind, row.output]),
    ).toEqual([
      ["a", "tool_call", 0],
      ["b", "session_event", 43],
    ]);
    expect((await totals()).output).toBe(43);
  });

  it("retracts another origin's projection when a record arrives under a new kind", async () => {
    await put(block("x"));
    // Another origin reports the same record as a tool call: only that remains.
    await put({
      id: "x",
      origin: "origin-b",
      kind: "tool_call",
      payload: { name: "Read", target: "x.ts", ts_ms: 7_000 },
    });
    expect((await projected()).map((row) => [row.id, row.kind])).toEqual([
      ["x", "tool_call"],
    ]);
    // A third origin reports it as a non-activity kind: nothing is projected.
    await put({
      id: "x",
      origin: "origin-c",
      kind: "session",
      payload: { session_id: "s", cwd: "/work" },
    });
    expect(await projected()).toEqual([]);
    expect(await totals()).toMatchObject({ input: 0, output: 0 });
  });

  it("takes no workspace lock for babysitter evidence, so expiry cannot deadlock a batch", async () => {
    // Expiry row-locks session_lineage/turn_receipt rows before updating them; if
    // their trigger then waited on the workspace lock a delivery batch holds, the
    // two could deadlock. Those kinds are never projected, so they skip the lock.
    await put({
      id: "receipt",
      kind: "turn_receipt",
      payload: { expires_at_ms: 1 },
    });
    await client.exec("BEGIN");
    try {
      // The expiry path: an existing evidence row updated to its tombstone.
      await put({
        id: "receipt",
        kind: "turn_receipt",
        payload: null,
        revision: 2,
      });
      const locks = (
        await client.query<{ count: number }>(
          `SELECT count(*)::int AS count FROM pg_locks
            WHERE locktype = 'advisory' AND pid = pg_backend_pid()`,
        )
      ).rows[0]!.count;
      expect(locks).toBe(0);
    } finally {
      await client.exec("ROLLBACK");
    }
  });

  it("retracts another origin's activity when the record arrives as babysitter evidence", async () => {
    await put(block("ev"));
    await put({
      id: "ev",
      origin: "origin-b",
      kind: "turn_receipt",
      payload: { expires_at_ms: 1 },
    });
    expect(await projected()).toEqual([]);
  });

  it("keeps control rows in delivery_records without projecting them", async () => {
    await put({
      id: "caveat",
      payload: {
        role: "user",
        text: "Caveat: The messages below were generated by the user while running local commands.",
        control_kind: "slash_command_caveat",
      },
    });
    await put({
      id: "meta",
      payload: {
        role: "user",
        text: "<system-reminder>x</system-reminder>",
        is_meta: 1,
      },
    });
    await put({
      id: "prompt",
      payload: { role: "user", text: "real prompt", is_meta: 0 },
    });
    expect((await projected()).map((row) => row.id)).toEqual(["prompt"]);
    expect(
      (
        await client.query(
          "SELECT count(*)::int AS n FROM sessions.delivery_records",
        )
      ).rows,
    ).toEqual([{ n: 3 }]);
  });

  it("accepts and projects a full backfill batch in one transaction", async () => {
    const records = ["s1", "s2", "s3", "s4", "s5"].flatMap(backfillSession);
    expect(records).toHaveLength(MAX_DELIVERY_RECORDS);
    const receipt = await acceptDelivery(db, auth, {
      schema_version: 1,
      origin_id: "origin-a",
      batch_id: "backfill-500",
      job_id: "job",
      generation: 1,
      destination_id: "relayhistory",
      instance_id: "projection-test",
      account_id: await deliveryAccount(auth),
      mapping_version: "relayhistory-delivery-v1",
      records,
    });
    expect(receipt.acceptedRevisionIds).toEqual(
      records.map((item) => item.revision_id),
    );
    expect(receipt.limits.maxRecords).toBe(MAX_DELIVERY_RECORDS);
    const count = async (sql: string) =>
      (await client.query<{ n: number }>(sql)).rows[0]!.n;
    expect(
      await count("SELECT count(*)::int AS n FROM sessions.delivery_records"),
    ).toBe(500);
    // Every record but the five catalog rows is activity.
    expect(
      await count("SELECT count(*)::int AS n FROM sessions.convergence_events"),
    ).toBe(495);
    expect(
      await count("SELECT count(*)::int AS n FROM sessions.session_catalog"),
    ).toBe(5);
    // 100 requests, each counted once despite three copies of its usage.
    expect(await totals()).toMatchObject({ input: 300, output: 4_300 });
    // Tool calls without their own time take their parent event's.
    expect(
      await count(
        `SELECT count(*)::int AS n FROM sessions.convergence_events
          WHERE kind IN ('tool_call', 'file_edit') AND ts = ingested_at`,
      ),
    ).toBe(0);
  });

  it("settles a request copied onto a full batch of blocks, counting it once", async () => {
    const records: HistoryExportRecord[] = Array.from(
      { length: MAX_DELIVERY_RECORDS },
      (_, index) => {
        const id = `b${String(index).padStart(4, "0")}`;
        return {
          schema_version: 1,
          origin_id: "origin-a",
          record_id: id,
          revision_id: `${id}:1`,
          revision: 1,
          kind: "session_event",
          source: "claude",
          session_id: "s",
          operation: "upsert",
          payload: block(id).payload,
        };
      },
    );
    const receipt = await acceptDelivery(db, auth, {
      schema_version: 1,
      origin_id: "origin-a",
      batch_id: "one-request",
      job_id: "job",
      generation: 1,
      destination_id: "relayhistory",
      instance_id: "projection-test",
      account_id: await deliveryAccount(auth),
      mapping_version: "relayhistory-delivery-v1",
      records,
    });
    expect(receipt.acceptedRevisionIds).toHaveLength(MAX_DELIVERY_RECORDS);
    expect(await totals()).toEqual({
      input: 3,
      output: 43,
      cache_read: 11496,
      cache_create: 4773,
      reasoning: 0,
    });
    const carriers = await projected("input_tokens > 0");
    expect(carriers.map((row) => row.id)).toEqual(["b0000"]);
  });

  it("rewrites only the request members whose usage changes (0024)", async () => {
    await put(block("r2"));
    await put(block("r3"));
    await put(block("r4"));
    const versions = async () =>
      Object.fromEntries(
        (
          await client.query<{ id: string; version: string }>(
            `SELECT delivery_record_id AS id, xmin::text AS version
               FROM sessions.convergence_events ORDER BY delivery_record_id`,
          )
        ).rows.map((row) => [row.id, row.version]),
      );
    const before = await versions();
    // A later member changes nothing already projected.
    await put(block("r5"));
    const later = await versions();
    expect([later.r2, later.r3, later.r4]).toEqual([
      before.r2,
      before.r3,
      before.r4,
    ]);
    // A new canonical member moves the usage: only the old carrier is rewritten.
    await put(block("r1"));
    const moved = await versions();
    expect(moved.r2).not.toBe(later.r2);
    expect([moved.r3, moved.r4, moved.r5]).toEqual([
      later.r3,
      later.r4,
      later.r5,
    ]);
    expect(
      (await projected()).map((row) => [row.id, row.input, row.output]),
    ).toEqual([
      ["r1", 3, 43],
      ["r2", 0, 0],
      ["r3", 0, 0],
      ["r4", 0, 0],
      ["r5", 0, 0],
    ]);
  });

  it("reprojects retained pre-0023 rows: duplicates, control rows and usage", async () => {
    await client.close();
    client = await createFreshDatabase();
    await run(migrations.filter((m) => m.name < "0023"));
    // The newer copy is stored first, so the reprojection must order by
    // received_at rather than by row order.
    await put({
      ...block("r1", { text: "newer copy" }),
      origin: "origin-b",
      receivedAt: "2026-09-02T00:00:00Z",
    });
    await put(block("r1", { text: "older copy" }));
    await put(block("r2"));
    // One record retained under two kinds: the newer copy (a tool call) is stored
    // first, so replaying per kind or in row order would let the older kind win.
    await put({
      id: "k",
      origin: "origin-b",
      kind: "tool_call",
      payload: { name: "Read", target: "k.ts", ts_ms: 7_000 },
      receivedAt: "2026-09-03T00:00:00Z",
    });
    await put({
      id: "k",
      payload: { role: "user", text: "older kind" },
      receivedAt: "2026-09-01T00:00:00Z",
    });
    await put({
      id: "ctl",
      payload: {
        role: "user",
        text: "<command-name>/clear</command-name>",
        control_kind: "slash_command_invocation",
      },
    });
    expect(
      (
        await client.query(
          "SELECT count(*)::int AS n FROM sessions.convergence_events",
        )
      ).rows,
    ).toEqual([{ n: 6 }]);
    // A retained catalog record is seeded once by the catalog backfill, in received_at
    // order within its key; the activity reprojection must not re-accept it, which
    // would re-rank catalog winners as if it were a live write (seq >= 2^40).
    await put({
      id: "sess",
      kind: "session",
      payload: { session_id: "s", git_branch: "main" },
    });
    const acceptance = async () =>
      (
        await client.query(
          "SELECT record_id, accepted_seq::text AS seq FROM sessions.delivery_catalog_acceptance ORDER BY record_id",
        )
      ).rows;
    await run();
    expect(await acceptance()).toEqual([{ record_id: "sess", seq: "1" }]);
    await rollout(client);
    expect(await acceptance()).toEqual([{ record_id: "sess", seq: "1" }]);
    const rows = await projected();
    expect(rows.map((row) => [row.id, row.kind, row.content])).toEqual([
      ["k", "tool_call", "Read k.ts"],
      ["r1", "session_event", "newer copy"],
      ["r2", "session_event", "block r2"],
    ]);
    expect(await totals()).toMatchObject({ input: 3, output: 43 });
  });
});

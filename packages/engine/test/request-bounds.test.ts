import { Hono } from "hono";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { HistoryDb } from "../src/db/database.js";
import type { AuthContext, HistoryEnv } from "../src/env.js";
import { MAX_JSON_BODY_BYTES } from "../src/lib/bounded-json.js";
import { deliveryAccount, listDelivery } from "../src/lib/delivery.js";
import { createDeliveryRoutes } from "../src/routes/delivery.js";
import { createIngestRoutes } from "../src/routes/ingest.js";
import { createRecallRoutes } from "../src/routes/recall.js";
import { createTurnRoutes, parseTurnRequest } from "../src/routes/turns.js";
import { createTestDatabase, type TestDatabase } from "./support/database.js";

/**
 * Request bounds and parameter validation at the route boundary: a request the engine
 * cannot or will not serve is answered before it is buffered, written or queried.
 */

// A configured database that fails the test if a request reaches it.
const untouchedDb = new Proxy({} as HistoryDb, {
  get(_target, property) {
    throw new Error(`database accessed: ${String(property)}`);
  },
});

const auth: AuthContext = {
  userId: "user-a",
  orgId: "org-a",
  workspaceId: "workspace-a",
  tokenSubject: "user-a",
  scopes: ["rth:sync", "rth:read"],
  claims: {},
};

function app(routes: Hono<HistoryEnv>) {
  const result = new Hono<HistoryEnv>();
  result.use("*", async (c, next) => {
    c.set("requestId", "req-test");
    c.set("correlationId", "corr-test");
    c.set("auth", auth);
    await next();
  });
  result.route("/", routes);
  return result;
}

const MiB = 1024 * 1024;

/** A body of `total` bytes in 1 MiB chunks that records how much was pulled. */
function countedBody(total: number) {
  const pulled = { bytes: 0 };
  const chunk = new Uint8Array(MiB).fill(0x20);
  const stream = new ReadableStream<Uint8Array>({
    pull(controller) {
      if (pulled.bytes >= total) {
        controller.close();
        return;
      }
      pulled.bytes += chunk.byteLength;
      controller.enqueue(chunk);
    },
  });
  return { stream, pulled };
}

const legacyRoutes = [
  {
    name: "/v1/ingest",
    path: "/ingest",
    routes: () => createIngestRoutes({ database: () => untouchedDb }),
  },
  {
    name: "/v1/sessions/:sessionId/turns",
    path: "/sessions/session-a/turns",
    routes: () => createTurnRoutes({ database: () => untouchedDb }),
  },
];

describe.each(legacyRoutes)("$name request body bound", ({ path, routes }) => {
  it("answers 413 without reading past the limit", async () => {
    const { stream, pulled } = countedBody(MAX_JSON_BODY_BYTES + 8 * MiB);
    const response = await app(routes()).request(
      new Request(`http://localhost${path}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: stream,
        duplex: "half",
      } as RequestInit),
    );
    expect(response.status).toBe(413);
    expect(await response.json()).toEqual({
      error: {
        code: "payload_too_large",
        message: `Request body exceeds ${MAX_JSON_BODY_BYTES} bytes`,
      },
      correlationId: "corr-test",
    });
    expect(pulled.bytes).toBeLessThanOrEqual(MAX_JSON_BODY_BYTES + 2 * MiB);
  });

  it("still parses a body at the limit", async () => {
    const json = JSON.stringify({ pad: "" });
    const body =
      json.slice(0, -2) +
      " ".repeat(MAX_JSON_BODY_BYTES - json.length) +
      json.slice(-2);
    expect(new TextEncoder().encode(body).length).toBe(MAX_JSON_BODY_BYTES);
    const response = await app(routes()).request(path, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body,
    });
    // Parsed, then rejected by the envelope validation, before the database.
    expect(response.status).toBe(400);
    expect(await response.json()).toMatchObject({
      error: { code: "bad_request" },
    });
  });

  it.each([
    ["malformed JSON", "{"],
    ["invalid UTF-8", new Uint8Array([0x7b, 0xff, 0x7d])],
    ["an empty body", ""],
  ])("keeps the invalid JSON answer for %s", async (_name, body) => {
    const response = await app(routes()).request(path, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body,
    });
    expect(response.status).toBe(400);
    expect(await response.text()).toBe(
      JSON.stringify({
        error: {
          code: "bad_request",
          message: "Request body must be valid JSON",
        },
        correlationId: "corr-test",
      }),
    );
  });
});

describe("recall maxContent", () => {
  const recall = () => app(createRecallRoutes({ database: () => untouchedDb }));

  it.each(["0.5", "abc", "0", "-1", "1.5", "1e3", "9007199254740993"])(
    "rejects maxContent=%s before querying",
    async (value) => {
      for (const path of ["/events", "/sessions/session-a/events"]) {
        const response = await recall().request(`${path}?maxContent=${value}`);
        expect(response.status, path).toBe(400);
        expect(await response.json()).toEqual({
          error: {
            code: "bad_request",
            message: "maxContent must be a positive integer",
          },
          correlationId: "corr-test",
        });
      }
    },
  );
});

describe("delivery readback filters", () => {
  const delivery = () =>
    app(createDeliveryRoutes({ database: () => untouchedDb }));

  it("rejects an empty kind like any unknown kind", async () => {
    await expect(
      listDelivery(untouchedDb, auth, { kind: "" }),
    ).rejects.toMatchObject({ status: 400, message: "Unknown evidence kind" });
    const response = await delivery().request("/delivery/records?kind=", {
      headers: {
        "X-RelayHistory-Expected-Account": await deliveryAccount(auth),
      },
    });
    expect(response.status).toBe(400);
    expect(await response.json()).toMatchObject({
      error: { code: "invalid_delivery", message: "Unknown evidence kind" },
    });
  });

  it("rejects a repeated filter, wherever it repeats", async () => {
    for (const query of [
      "kind=history&kind=history",
      "source=claude&limit=1&source=codex",
      "a=1&b=2&c=3&a=4",
    ]) {
      const response = await delivery().request(`/delivery/records?${query}`, {
        headers: {
          "X-RelayHistory-Expected-Account": await deliveryAccount(auth),
        },
      });
      expect(response.status, query).toBe(400);
      expect(await response.json()).toMatchObject({
        error: {
          code: "invalid_delivery",
          message: "Repeated delivery filters are unsupported",
        },
      });
    }
  });
});

describe("turn index range", () => {
  const turn = (turnIndex: number) => ({
    sessionOwner: "owner",
    turnIndex,
    role: "user",
    content: "hello",
    actorName: "owner",
    actorRole: "owner",
    ts: "2026-10-04T00:00:00Z",
  });

  it("rejects an index past PostgreSQL integer in the parser", () => {
    expect(parseTurnRequest([turn(2_147_483_648)])).toBe(
      "turns[0].turnIndex must be at most 2147483647",
    );
    expect(parseTurnRequest([turn(Number.MAX_SAFE_INTEGER)])).toBe(
      "turns[0].turnIndex must be at most 2147483647",
    );
    expect(parseTurnRequest([turn(-1)])).toBe(
      "turns[0].turnIndex must be a non-negative integer",
    );
    expect(parseTurnRequest([turn(2_147_483_647)])).toHaveLength(1);
  });

  describe("against the migrated table", () => {
    let database: TestDatabase;
    beforeAll(async () => {
      database = await createTestDatabase();
    });
    afterAll(async () => {
      await database?.close();
    });

    it("stores the largest index and answers 400 past it", async () => {
      const turns = app(createTurnRoutes({ database: () => database.db }));
      const post = (index: number) =>
        turns.request("/sessions/session-a/turns", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify([turn(index)]),
        });
      const largest = await post(2_147_483_647);
      expect(largest.status).toBe(200);
      expect(await largest.json()).toMatchObject({ accepted: 1 });
      const past = await post(2_147_483_648);
      expect(past.status).toBe(400);
      expect(await past.json()).toMatchObject({
        error: {
          code: "bad_request",
          message: "turns[0].turnIndex must be at most 2147483647",
        },
      });
    });

    it("still truncates with a valid maxContent", async () => {
      await database.query(
        `INSERT INTO sessions.convergence_events
           (org_id, workspace_id, machine_id, user_id, source, lens, session_id,
            event_id, kind, type, ts, content, record)
         VALUES ('org-a', 'workspace-a', 'm', 'user-a', 'claude', 'transcript',
                 'session-a', 'e1', 'session_event', 'message', now(), $1, '{}'::jsonb)`,
        ["x".repeat(200)],
      );
      const recall = app(createRecallRoutes({ database: () => database.db }));
      const response = await recall.request(
        "/sessions/session-a/events?maxContent=100",
      );
      expect(response.status).toBe(200);
      const { events } = (await response.json()) as {
        events: { content: string }[];
      };
      expect(events[0]?.content).toContain("[truncated by maxContent]");
    });
  });
});

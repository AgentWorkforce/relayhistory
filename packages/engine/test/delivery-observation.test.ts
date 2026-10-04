import { Hono } from "hono";
import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  it,
  vi,
} from "vitest";
import type {
  AuthContext,
  DeliveryBatchObservation,
  HistoryEnv,
} from "../src/env.js";
import { deliveryAccount } from "../src/lib/delivery.js";
import type { HistoryExportBatch } from "../src/lib/delivery-contracts.js";
import {
  createDeliveryRoutes,
  deliveryBatchShape,
} from "../src/routes/delivery.js";
import { createTestDatabase, type TestDatabase } from "./support/database.js";

/**
 * The delivery route reports every batch request it finishes to the host's
 * `observeDeliveryBatch` hook; what a host does with it (hosted telemetry) is the
 * host's concern. Requests run against the real migrated database.
 */
let database: TestDatabase;
const observations: DeliveryBatchObservation[] = [];

const auth: AuthContext = {
  orgId: "org-fixture",
  workspaceId: "workspace-fixture",
  userId: "user-fixture",
  tokenSubject: "user-fixture",
  scopes: ["rth:sync"],
  claims: {},
};

function record(
  id: string,
  session: string,
): HistoryExportBatch["records"][number] {
  return {
    schema_version: 1,
    origin_id: "origin-fixture",
    record_id: id,
    revision_id: `${id}-r1`,
    revision: 1,
    kind: "history",
    source: "claude",
    session_id: session,
    operation: "upsert",
    payload: { prompt: "hello" },
  };
}

async function batch(): Promise<HistoryExportBatch> {
  return {
    schema_version: 1,
    origin_id: "origin-fixture",
    batch_id: "batch-1",
    job_id: "job-1",
    generation: 1,
    destination_id: "destination-1",
    instance_id: "instance-1",
    account_id: await deliveryAccount(auth),
    mapping_version: "relayhistory-delivery-v1",
    records: [
      record("record-1", "session-alpha"),
      record("record-2", "session-alpha"),
      record("record-3", "session-beta"),
    ],
  };
}

function app(observe = true): Hono<HistoryEnv> {
  const app = new Hono<HistoryEnv>();
  app.use("*", async (c, next) => {
    c.set("requestId", "req-test");
    c.set("correlationId", "corr-test");
    c.set("auth", auth);
    await next();
  });
  app.route(
    "/",
    createDeliveryRoutes({
      database: () => database.db,
      ...(observe
        ? {
            observeDeliveryBatch: () => (observation) => {
              observations.push(observation);
            },
          }
        : {}),
    }),
  );
  return app;
}

function post(body: string, observe = true) {
  return app(observe).request("/delivery/batches", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body,
  });
}

beforeAll(async () => {
  database = await createTestDatabase();
});
beforeEach(async () => {
  observations.length = 0;
  await database.exec(
    "TRUNCATE delivery_records, delivery_receipts, delivery_origins",
  );
});
afterAll(async () => {
  await database?.close();
});

describe("delivery batch observation", () => {
  it("reports an accepted batch with its bytes and shape", async () => {
    const value = await batch();
    const body = JSON.stringify({ protocolVersion: 1, batch: value });
    const response = await post(body);
    expect(response.status).toBe(200);

    expect(observations).toHaveLength(1);
    const [observation] = observations;
    expect(observation).toEqual({
      auth,
      batch: value,
      bytes: body.length,
      outcome: "accepted",
      durationMs: expect.any(Number),
    });
    expect(observation).not.toHaveProperty("conflictCount");
    expect(deliveryBatchShape(observation.batch)).toEqual({
      records: 3,
      kinds: { history: 3 },
      sessions: 2,
      max_session_records: 2,
    });
  });

  it("reports the delivery error code as the outcome", async () => {
    const response = await post(
      JSON.stringify({
        protocolVersion: 1,
        batch: { ...(await batch()), schema_version: 2 },
      }),
    );
    expect(response.status).toBe(422);
    expect(observations).toHaveLength(1);
    expect(observations[0]).toMatchObject({
      outcome: "unsupported_schema",
      batch: undefined,
    });
    expect(deliveryBatchShape(observations[0].batch)).toMatchObject({
      records: 0,
      sessions: 0,
    });
    expect(
      (await database.query("SELECT count(*)::int AS n FROM delivery_receipts"))
        .rows,
    ).toEqual([{ n: 0 }]);
  });

  it("reports a conflicting batch with its conflict count", async () => {
    const current = await batch();
    expect(
      (await post(JSON.stringify({ protocolVersion: 1, batch: current })))
        .status,
    ).toBe(200);
    observations.length = 0;

    const changed = structuredClone(current);
    changed.batch_id = "batch-2";
    changed.records[0].payload!.prompt = "conflicting";
    changed.records[2].payload!.prompt = "conflicting";
    const response = await post(
      JSON.stringify({ protocolVersion: 1, batch: changed }),
    );
    expect(response.status).toBe(409);
    expect(observations).toEqual([
      expect.objectContaining({
        auth,
        batch: changed,
        outcome: "delivery_conflict",
        conflictCount: 2,
      }),
    ]);
  });

  it("observes nothing without an observer and still accepts", async () => {
    const response = await post(
      JSON.stringify({ protocolVersion: 1, batch: await batch() }),
      false,
    );
    expect(response.status).toBe(200);
    expect(observations).toEqual([]);
  });

  it("shapes a batch by kind and session", async () => {
    expect(deliveryBatchShape(undefined)).toEqual({
      records: 0,
      kinds: {},
      sessions: 0,
      max_session_records: 0,
    });
    const shaped = deliveryBatchShape({
      ...(await batch()),
      records: [
        record("a", "s1"),
        { ...record("b", "s1"), kind: "tool_call" },
        { ...record("c", "s2"), session_id: null },
      ],
    });
    // The sessionless record counts as a record, never as a session.
    expect(shaped).toEqual({
      records: 3,
      kinds: { history: 2, tool_call: 1 },
      sessions: 1,
      max_session_records: 2,
    });
    expect(
      deliveryBatchShape({
        ...(await batch()),
        records: [
          { ...record("a", "s1"), session_id: null },
          { ...record("b", "s1"), session_id: "" },
        ],
      }),
    ).toMatchObject({ records: 2, sessions: 0, max_session_records: 0 });
  });

  it("reports the bytes read even when the body is rejected", async () => {
    const body = '{"protocolVersion": 1, "batch": not-json';
    const response = await post(body);
    expect(response.status).toBe(400);
    expect(observations).toHaveLength(1);
    expect(observations[0]).toMatchObject({
      outcome: "invalid_delivery",
      bytes: body.length,
      batch: undefined,
    });
  });
});

describe("a failing observer", () => {
  function failing(
    observeDeliveryBatch: () => (observation: DeliveryBatchObservation) => void,
  ) {
    const result = new Hono<HistoryEnv>();
    result.use("*", async (c, next) => {
      c.set("correlationId", "corr-test");
      c.set("auth", auth);
      await next();
    });
    result.route(
      "/",
      createDeliveryRoutes({
        database: () => database.db,
        observeDeliveryBatch,
      }),
    );
    return (body: string) =>
      result.request("/delivery/batches", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body,
      });
  }
  const throwingFactory = () => {
    throw new Error("observer factory failed");
  };
  const throwingCallback = () => () => {
    throw new Error("observer callback failed");
  };

  const rejectingFactory = (async () => {
    throw new Error("observer factory rejected");
  }) as unknown as () => (observation: DeliveryBatchObservation) => void;
  const rejectingCallback = () => async () => {
    throw new Error("observer callback rejected");
  };

  it.each([
    ["factory", rejectingFactory],
    ["callback", rejectingCallback],
  ])(
    "contains an async %s rejection without changing the response",
    async (_name, observer) => {
      const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
      const unhandled: unknown[] = [];
      const onUnhandled = (reason: unknown) => unhandled.push(reason);
      process.on("unhandledRejection", onUnhandled);
      try {
        const accepted = await failing(observer)(
          JSON.stringify({ protocolVersion: 1, batch: await batch() }),
        );
        expect(accepted.status).toBe(200);
        expect(await accepted.json()).toMatchObject({
          acceptanceLevel: "durable",
        });
        await new Promise((done) => setTimeout(done, 20));
        expect(unhandled).toEqual([]);
        expect(warn.mock.calls.map((call) => String(call[0]))).toContainEqual(
          expect.stringMatching(
            /^\[delivery\] batch observer (failed|unavailable)$/,
          ),
        );
      } finally {
        process.off("unhandledRejection", onUnhandled);
        warn.mockRestore();
      }
    },
  );

  it.each([
    ["factory", throwingFactory],
    ["callback", throwingCallback],
  ])(
    "does not change the delivery response when its %s throws",
    async (_name, observer) => {
      const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
      try {
        const post = failing(observer);
        const value = await batch();
        const accepted = await post(
          JSON.stringify({ protocolVersion: 1, batch: value }),
        );
        expect(accepted.status).toBe(200);
        expect(await accepted.json()).toMatchObject({
          batchId: "batch-1",
          acceptanceLevel: "durable",
          acceptedRevisionIds: ["record-1-r1", "record-2-r1", "record-3-r1"],
        });
        const rejected = await post(
          JSON.stringify({
            protocolVersion: 1,
            batch: { ...value, schema_version: 2 },
          }),
        );
        expect(rejected.status).toBe(422);
        expect(await rejected.json()).toMatchObject({
          error: { code: "unsupported_schema" },
          correlationId: "corr-test",
        });
        expect(
          warn.mock.calls.map((call) => String(call[0])).join("\n"),
        ).not.toContain("hello");
      } finally {
        warn.mockRestore();
      }
    },
  );
});

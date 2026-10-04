import type { HistoryDb } from "@relayhistory/engine";
import type pg from "pg";
import { describe, expect, it } from "vitest";
import { createServerApp } from "../src/app.js";
import { createLogger, silentLogger, type Logger } from "../src/log.js";

function app(
  options: { accepting: boolean; database: boolean },
  log: Logger = silentLogger,
) {
  const pool = {
    query: async () => {
      if (!options.database) throw new Error("connection refused");
      return { rows: [] };
    },
  } as unknown as pg.Pool;
  return createServerApp({
    db: {} as HistoryDb,
    pool,
    embeddings: null,
    log,
    accepting: () => options.accepting,
  });
}

describe("createServerApp", () => {
  it("serves the engine's liveness and its own readiness", async () => {
    const ready = app({ accepting: true, database: true });
    expect((await ready.request("/health")).status).toBe(200);
    expect(await (await ready.request("/ready")).json()).toEqual({
      ok: true,
      service: "relayhistory",
    });
  });

  it("is not ready while draining or without the database", async () => {
    const draining = await app({ accepting: false, database: true }).request(
      "/ready",
    );
    expect(draining.status).toBe(503);
    expect(await draining.json()).toMatchObject({ reason: "not_accepting" });
    const down = await app({ accepting: true, database: false }).request(
      "/ready",
    );
    expect(down.status).toBe(503);
    expect(await down.json()).toMatchObject({ reason: "database_unavailable" });
  });

  it("puts every /v1 route behind service-local authentication", async () => {
    const response = await app({ accepting: true, database: true }).request(
      "/v1/sessions",
    );
    expect(response.status).toBe(401);
    expect(await response.json()).toMatchObject({
      error: { code: "missing_authorization" },
    });
  });

  it("answers an unhandled failure with the engine's body and logs no driver text", async () => {
    const lines: string[] = [];
    const db = new Proxy(
      {},
      {
        get() {
          throw Object.assign(
            new Error(
              'duplicate key in "secret_table_q7": Key (session_id)=(private-s9) password=hunter2',
            ),
            { code: "XX000" },
          );
        },
      },
    ) as HistoryDb;
    const failing = createServerApp({
      db,
      pool: {} as pg.Pool,
      embeddings: null,
      log: createLogger((line) => lines.push(line)),
      accepting: () => true,
    });
    const response = await failing.request("/v1/sessions", {
      headers: {
        authorization: "Bearer rth_st_example",
        "x-correlation-id": "corr-1",
      },
    });
    expect(response.status).toBe(500);
    expect(await response.json()).toEqual({
      error: { code: "internal_error", message: "Internal server error" },
      correlationId: "corr-1",
    });
    // Exactly the safe fields: no message, detail, query or stack from the driver error.
    const records = lines.map((line) => JSON.parse(line));
    expect(records).toHaveLength(1);
    expect(Object.keys(records[0]).sort()).toEqual(
      [
        "code",
        "correlationId",
        "error",
        "level",
        "message",
        "method",
        "time",
      ].sort(),
    );
    expect(records[0]).toMatchObject({
      message: "request failed",
      error: "Error",
      code: "XX000",
      correlationId: "corr-1",
    });
    const logged = lines.join("");
    for (const detail of [
      "secret_table_q7",
      "private-s9",
      "hunter2",
      "duplicate key",
    ])
      expect(logged).not.toContain(detail);
  });
});

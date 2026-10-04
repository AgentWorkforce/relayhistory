import type { HistoryDb } from "@relayhistory/engine";
import type pg from "pg";
import { describe, expect, it } from "vitest";
import { createServerApp } from "../src/app.js";

function app(options: { accepting: boolean; database: boolean }) {
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
});

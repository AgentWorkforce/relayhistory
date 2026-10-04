import { Hono } from "hono";
import { describe, expect, it } from "vitest";
import type { HistoryDb } from "../src/db/database.js";
import type { HistoryEnv } from "../src/env.js";
import { createIngestRoutes } from "../src/routes/ingest.js";

// A configured database that fails the test if a request reaches it.
const untouchedDb = new Proxy({} as HistoryDb, {
  get(_target, property) {
    throw new Error(`database accessed: ${String(property)}`);
  },
});
const ingestRoutes = createIngestRoutes({ database: () => untouchedDb });

describe("hosted /v1/ingest route contract", () => {
  it("requires rth:sync scope", async () => {
    const app = new Hono<HistoryEnv>();
    app.use("*", async (c, next) => {
      c.set("requestId", "req-test");
      c.set("correlationId", "corr-test");
      c.set("auth", {
        userId: "user-a",
        orgId: "org-a",
        workspaceId: "workspace-a",
        tokenSubject: "user-a",
        scopes: [],
        claims: {},
      });
      await next();
    });
    app.route("/", ingestRoutes);

    const response = await app.request(
      "/ingest",
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          machine: { id: "machine-a" },
          batchId: "batch-a",
          records: [],
        }),
      },
      { DATABASE_URL: "postgres://example" },
    );

    expect(response.status).toBe(403);
    await expect(response.json()).resolves.toMatchObject({
      error: { code: "forbidden", message: "missing required scope: rth:sync" },
    });
  });

  it("validates the heterogeneous batch envelope before touching Neon", async () => {
    const app = new Hono<HistoryEnv>();
    app.use("*", async (c, next) => {
      c.set("requestId", "req-test");
      c.set("correlationId", "corr-test");
      c.set("auth", {
        userId: "user-a",
        orgId: "org-a",
        workspaceId: "workspace-a",
        tokenSubject: "user-a",
        scopes: ["rth:sync"],
        claims: {},
      });
      await next();
    });
    app.route("/", ingestRoutes);

    const response = await app.request(
      "/ingest",
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          machine: { id: "machine-a" },
          batchId: "batch-a",
          records: [{ kind: "finding", source: "trajectories" }],
        }),
      },
      { DATABASE_URL: "postgres://example" },
    );

    expect(response.status).toBe(400);
    await expect(response.json()).resolves.toMatchObject({
      error: {
        code: "bad_request",
        message: "records[0].sessionId is required",
      },
    });
  });

  it("requires commitSha for session_outcome records", async () => {
    const app = new Hono<HistoryEnv>();
    app.use("*", async (c, next) => {
      c.set("requestId", "req-test");
      c.set("correlationId", "corr-test");
      c.set("auth", {
        userId: "user-a",
        orgId: "org-a",
        workspaceId: "workspace-a",
        tokenSubject: "user-a",
        scopes: ["rth:sync"],
        claims: {},
      });
      await next();
    });
    app.route("/", ingestRoutes);

    const response = await app.request(
      "/ingest",
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          machine: { id: "machine-a" },
          batchId: "batch-a",
          records: [
            {
              kind: "session_outcome",
              source: "relayhistory",
              sessionId: "session-a",
            },
          ],
        }),
      },
      { DATABASE_URL: "postgres://example" },
    );

    expect(response.status).toBe(400);
    await expect(response.json()).resolves.toMatchObject({
      error: {
        code: "bad_request",
        message: "records[0].commitSha is required for session_outcome records",
      },
    });
  });
});

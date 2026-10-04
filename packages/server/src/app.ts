/**
 * The self-hosted application: the shared History engine with a node-postgres
 * database, optional embeddings, and `/ready`.
 */
import {
  createHistoryEngine,
  type EmbeddingProvider,
  type HistoryDb,
  type HistoryEnv,
} from "@relayhistory/engine";
import { Hono } from "hono";
import type pg from "pg";
import type { Logger } from "./log.js";

export interface ServerAppOptions {
  db: HistoryDb;
  pool: pg.Pool;
  embeddings: EmbeddingProvider | null;
  log: Logger;
  /** False while starting or draining; `/ready` answers 503 and the balancer stops routing. */
  accepting(): boolean;
}

export function createServerApp(options: ServerAppOptions) {
  const ready = new Hono();
  ready.get("/ready", async (c) => {
    if (!options.accepting())
      return c.json({ ok: false, reason: "not_accepting" }, 503);
    try {
      await options.pool.query("SELECT 1");
    } catch {
      return c.json({ ok: false, reason: "database_unavailable" }, 503);
    }
    return c.json({ ok: true, service: "relayhistory" });
  });

  const app = createHistoryEngine<HistoryEnv>({
    database: () => options.db,
    embeddings: () => options.embeddings,
    rootRoutes: [ready],
  });
  // The engine's default handler prints the raw exception, and PostgreSQL errors carry
  // SQL details and row values. Answer with the engine's error bodies and log only the
  // error's class and code.
  app.onError((error, c) => {
    const correlationId = c.get("correlationId") ?? "";
    if (error.name === "BadRequestError")
      return c.json(
        {
          error: { code: "bad_request", message: error.message },
          correlationId,
        },
        400,
      );
    options.log.error("request failed", {
      method: c.req.method,
      error: error.name,
      code: (error as { code?: unknown }).code ?? "unknown",
      correlationId,
    });
    return c.json(
      {
        error: { code: "internal_error", message: "Internal server error" },
        correlationId,
      },
      500,
    );
  });
  return app;
}

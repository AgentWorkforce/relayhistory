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

export interface ServerAppOptions {
  db: HistoryDb;
  pool: pg.Pool;
  embeddings: EmbeddingProvider | null;
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

  return createHistoryEngine<HistoryEnv>({
    database: () => options.db,
    embeddings: () => options.embeddings,
    rootRoutes: [ready],
  });
}

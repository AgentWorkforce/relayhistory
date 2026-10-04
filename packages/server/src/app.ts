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
import type { Logger } from "./log.js";

export interface ServerAppOptions {
  db: HistoryDb;
  /** Whether the database answers, within a bound (see `databaseReadiness`). */
  databaseReady(): Promise<boolean>;
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
    if (!(await options.databaseReady()))
      return c.json({ ok: false, reason: "database_unavailable" }, 503);
    return c.json({ ok: true, service: "relayhistory" });
  });

  return createHistoryEngine<HistoryEnv>({
    database: () => options.db,
    embeddings: () => options.embeddings,
    rootRoutes: [ready],
    // Failures the engine answers without exposing: log the class and code only, never
    // a message, SQL or row values.
    reportError: (error, c) =>
      options.log.error("request failed", {
        method: c.req.method,
        error: (error as { name?: unknown })?.name ?? "Error",
        code: (error as { code?: unknown })?.code ?? "unknown",
        correlationId: c.get("correlationId") ?? "",
      }),
  });
}

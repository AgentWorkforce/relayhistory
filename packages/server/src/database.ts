/**
 * PostgreSQL over ordinary TCP: one `pg` pool for requests and background jobs, and a
 * dedicated connection for schema preparation.
 */
import { schema, type HistoryDb } from "@relayhistory/engine";
import { applyMigrations } from "@relayhistory/engine/migrations";
import { drizzle } from "drizzle-orm/node-postgres";
import pg from "pg";
import type { Logger } from "./log.js";

export interface Database {
  pool: pg.Pool;
  db: HistoryDb;
  close(): Promise<void>;
}

export function openDatabase(
  url: string,
  options: { max: number; log: Logger },
): Database {
  const pool = new pg.Pool({
    connectionString: url,
    max: options.max,
    application_name: "relayhistory-server",
  });
  // An idle client that loses its connection must not crash the process; the pool
  // replaces it. Driver messages can echo connection details, so only the code leaves.
  pool.on("error", (error: Error & { code?: string }) =>
    options.log.warn("database connection lost", {
      code: error.code ?? "unknown",
    }),
  );
  let closed: Promise<void> | undefined;
  return {
    pool,
    db: drizzle(pool, { schema }),
    close: () => (closed ??= pool.end()),
  };
}

const PROGRESS_INTERVAL_MS = 5_000;

export interface PreparedDatabase {
  migrations: number;
  /** Sessions the rollup backfill rebuilt this run (nonzero only after an upgrade). */
  rolledUpSessions: number;
}

/**
 * Bring the `sessions` schema to the packaged version: every pending migration under
 * the engine's advisory lock and checksum ledger (the migrations install `vector`, and
 * `pg_trgm` when available), then the concurrent indexes and the projection rollouts.
 * Safe to run from several processes at once; an interrupted rollout resumes.
 */
export async function prepareDatabase(
  url: string,
  options: { runtimeRole?: string; log: Logger },
): Promise<PreparedDatabase> {
  const client = new pg.Client({
    connectionString: url,
    application_name: "relayhistory-migrate",
  });
  // A connection lost mid-run is also emitted as an 'error' event; unheard, Node turns
  // it into an uncaught exception that crashes the process with the driver's raw text.
  // The statement in flight rejects with the same failure, which propagates from here
  // to the caller's sanitized logging, so the event itself needs only to be heard.
  client.on("error", () => {});
  try {
    await client.connect();
    // The engine reports fixed progress lines (counters, never SQL or rows). A long
    // upgrade backfill gets a heartbeat at most every few seconds; a quick start, none.
    let lastReport = Date.now();
    const result = await applyMigrations(client, {
      ...(options.runtimeRole ? { runtimeRole: options.runtimeRole } : {}),
      report: (line) => {
        if (Date.now() - lastReport < PROGRESS_INTERVAL_MS) return;
        lastReport = Date.now();
        options.log.info("preparing database", { progress: line });
      },
    });
    return {
      migrations: result.applied.length,
      rolledUpSessions: result.rolledUpSessions,
    };
  } finally {
    // Closing a lost connection must not replace the failure that lost it.
    await client.end().catch(() => {});
  }
}

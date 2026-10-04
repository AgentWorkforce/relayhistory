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
  await client.connect();
  try {
    const result = await applyMigrations(client, {
      ...(options.runtimeRole ? { runtimeRole: options.runtimeRole } : {}),
    });
    return {
      migrations: result.applied.length,
      rolledUpSessions: result.rolledUpSessions,
    };
  } finally {
    await client.end();
  }
}

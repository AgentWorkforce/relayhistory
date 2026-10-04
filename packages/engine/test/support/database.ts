/**
 * A migrated `sessions` database for one test file.
 *
 * By default this is in-process PGlite with pgvector and pg_trgm loaded, so the
 * packaged migrations run byte-for-byte. With `RELAYHISTORY_ENGINE_TEST_DATABASE=postgres`
 * and `DATABASE_URL` set to a server where the role may create databases, each call
 * creates a fresh database on that server instead, so the same suite proves the engine
 * against real PostgreSQL + pgvector over node-postgres.
 */
import { PGlite } from "@electric-sql/pglite";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { vector } from "@electric-sql/pglite-pgvector";
import { drizzle as drizzlePglite } from "drizzle-orm/pglite";
import { drizzle as drizzlePg } from "drizzle-orm/node-postgres";
import pg from "pg";
import type { HistoryDb } from "../../src/db/database.js";
import * as schema from "../../src/db/schema.js";
import { applyMigrations } from "../../src/migrate/index.js";

export interface TestDatabase {
  kind: "pglite" | "postgres";
  db: HistoryDb;
  query<T extends Record<string, unknown> = Record<string, unknown>>(
    text: string,
    params?: unknown[],
  ): Promise<{ rows: T[] }>;
  /** Runs one or more statements without parameters. */
  exec(text: string): Promise<void>;
  close(): Promise<void>;
}

export const testDatabaseKind: "pglite" | "postgres" =
  process.env.RELAYHISTORY_ENGINE_TEST_DATABASE === "postgres"
    ? "postgres"
    : "pglite";

export async function createTestDatabase(): Promise<TestDatabase> {
  return testDatabaseKind === "postgres"
    ? postgresDatabase()
    : pgliteDatabase();
}

async function pgliteDatabase(): Promise<TestDatabase> {
  const client = await PGlite.create({ extensions: { vector, pg_trgm } });
  await client.exec(
    "CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public; CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;",
  );
  await applyMigrations({
    query: async (text) => ({
      rows: (await client.query<Record<string, unknown>>(text)).rows,
    }),
  });
  await client.exec("SET search_path TO sessions, public");
  return {
    kind: "pglite",
    db: drizzlePglite(client, { schema }) as unknown as HistoryDb,
    query: async (text, params) => client.query(text, params) as never,
    exec: async (text) => {
      await client.exec(text);
    },
    close: () => client.close(),
  };
}

async function postgresDatabase(): Promise<TestDatabase> {
  const url = process.env.DATABASE_URL;
  if (!url) throw new Error("DATABASE_URL is required for postgres tests");
  const name = `rh_engine_${process.pid}_${Math.random().toString(36).slice(2, 10)}`;
  const admin = new pg.Client({ connectionString: url });
  await admin.connect();
  await admin.query(`CREATE DATABASE ${name}`);
  const target = new URL(url);
  target.pathname = `/${name}`;
  const migrator = new pg.Client({ connectionString: target.toString() });
  await migrator.connect();
  await migrator.query(
    "CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public",
  );
  await migrator.query(
    "CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public",
  );
  await applyMigrations(migrator);
  await migrator.end();
  const pool = new pg.Pool({
    connectionString: target.toString(),
    max: 4,
    options: "-c search_path=sessions,public",
  });
  // An error on an idle pooled client is a test failure; it surfaces from `close`
  // instead of crashing the worker as an unhandled "error" event.
  const poolErrors: Error[] = [];
  pool.on("error", (error) => poolErrors.push(error));
  return {
    kind: "postgres",
    db: drizzlePg(pool, { schema }),
    query: (text, params) => pool.query(text, params as unknown[]) as never,
    exec: async (text) => {
      await pool.query(text);
    },
    close: async () => {
      await pool.end();
      await dropDatabase(admin, name);
      await admin.end();
      if (poolErrors.length) throw poolErrors[0];
    },
  };
}

/**
 * Drop a test database once its backends have exited. `pool.end()` and `client.end()`
 * resolve when the client side closes, slightly before the server-side backend is gone;
 * a forced drop at that moment terminates backends a pool may still report (57P01).
 */
export async function dropDatabase(admin: pg.Client, name: string) {
  for (let attempt = 0; ; attempt++) {
    const { rows } = await admin.query<{ n: number }>(
      "SELECT count(*)::int AS n FROM pg_stat_activity WHERE datname = $1",
      [name],
    );
    if (rows[0]!.n === 0) break;
    if (attempt >= 100)
      throw new Error(`test database ${name} still has connections`);
    await new Promise((done) => setTimeout(done, 100));
  }
  await admin.query(`DROP DATABASE IF EXISTS ${name}`);
}

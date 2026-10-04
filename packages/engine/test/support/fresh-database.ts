/**
 * An empty database with pgvector and pg_trgm installed and no migrations applied, for
 * suites that drive the migration runner themselves. One connection, so session state
 * such as `SET ROLE` holds across calls. Mode follows `createTestDatabase`.
 */
import { PGlite } from "@electric-sql/pglite";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { vector } from "@electric-sql/pglite-pgvector";
import { drizzle as drizzlePglite } from "drizzle-orm/pglite";
import { drizzle as drizzlePg } from "drizzle-orm/node-postgres";
import pg from "pg";
import type { HistoryDb } from "../../src/db/database.js";
import * as schema from "../../src/db/schema.js";
import type { MigrationClient } from "../../src/migrate/index.js";
import { testDatabaseKind } from "./database.js";

export interface FreshDatabase {
  kind: "pglite" | "postgres";
  query<T extends Record<string, unknown> = Record<string, unknown>>(
    text: string,
    params?: unknown[],
  ): Promise<{ rows: T[] }>;
  /** Runs one or more statements without parameters. */
  exec(text: string): Promise<void>;
  /** Runs `fn` in one transaction, rolled back when it throws. */
  transaction(
    fn: (tx: Pick<FreshDatabase, "query">) => Promise<void>,
  ): Promise<void>;
  /** The connection as `applyMigrations` takes it. */
  client: MigrationClient;
  /** Drizzle over the same connection, with the server's default search path. */
  db: HistoryDb;
  /**
   * Creates a role named `prefix` plus a suffix unique to this database, since roles
   * are cluster-wide on a real server. Dropped on `close`.
   */
  createRole(prefix: string): Promise<string>;
  close(): Promise<void>;
}

export async function createFreshDatabase(): Promise<FreshDatabase> {
  return testDatabaseKind === "postgres"
    ? postgresDatabase()
    : pgliteDatabase();
}

async function pgliteDatabase(): Promise<FreshDatabase> {
  const client = await PGlite.create({ extensions: { vector, pg_trgm } });
  await client.exec(
    "CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public; CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;",
  );
  const query: FreshDatabase["query"] = async (text, params) =>
    client.query(text, params) as never;
  return {
    kind: "pglite",
    query,
    exec: async (text) => {
      await client.exec(text);
    },
    transaction: async (fn) => {
      await client.transaction(async (tx) => {
        await fn({
          query: async (text, params) => tx.query(text, params) as never,
        });
      });
    },
    client: { query: (text) => query(text) },
    db: drizzlePglite(client, { schema }) as unknown as HistoryDb,
    createRole: async (prefix) => {
      await client.exec(`CREATE ROLE ${prefix}`);
      return prefix;
    },
    close: () => client.close(),
  };
}

async function postgresDatabase(): Promise<FreshDatabase> {
  const url = process.env.DATABASE_URL;
  if (!url) throw new Error("DATABASE_URL is required for postgres tests");
  const name = `rh_engine_fresh_${process.pid}_${Math.random().toString(36).slice(2, 10)}`;
  const admin = new pg.Client({ connectionString: url });
  await admin.connect();
  await admin.query(`CREATE DATABASE ${name}`);
  const target = new URL(url);
  target.pathname = `/${name}`;
  const roles: string[] = [];
  const client = new pg.Client({ connectionString: target.toString() });
  await client.connect();
  await client.query(
    "CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public",
  );
  await client.query(
    "CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public",
  );
  const query: FreshDatabase["query"] = (text, params) =>
    client.query(text, params as unknown[]) as never;
  return {
    kind: "postgres",
    query,
    exec: async (text) => {
      await client.query(text);
    },
    transaction: async (fn) => {
      await client.query("BEGIN");
      try {
        await fn({ query });
        await client.query("COMMIT");
      } catch (error) {
        await client.query("ROLLBACK");
        throw error;
      }
    },
    client,
    db: drizzlePg(client, { schema }),
    createRole: async (prefix) => {
      const role = `${prefix}_${name}`;
      await client.query(`CREATE ROLE ${role}`);
      roles.push(role);
      return role;
    },
    close: async () => {
      await client.end();
      await admin.query(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`);
      for (const role of roles)
        await admin.query(`DROP ROLE IF EXISTS ${role}`);
      await admin.end();
    },
  };
}

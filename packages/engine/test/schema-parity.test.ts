/**
 * `src/db/schema.ts` is the engine's typed view of the `sessions` schema the packaged
 * migrations create. Nothing generates one from the other, so this holds them
 * together: every table and column Drizzle declares exists in the migrated database
 * with the same type, nullability and (for SQL defaults) a server-side default. A
 * column Drizzle selects that the database lacks fails every `select()` on its table.
 */
import { is } from "drizzle-orm";
import { PgTable, getTableConfig } from "drizzle-orm/pg-core";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import * as schema from "../src/db/schema.js";
import { createTestDatabase, type TestDatabase } from "./support/database.js";

type DatabaseColumn = {
  table_name: string;
  column_name: string;
  type: string;
  not_null: boolean;
  has_default: boolean;
};

const tables: PgTable[] = Object.values(schema).filter((value) =>
  is(value, PgTable),
);

let database: TestDatabase;
let columns: Map<string, Map<string, DatabaseColumn>>;

beforeAll(async () => {
  database = await createTestDatabase();
  const { rows } = await database.query<DatabaseColumn>(
    `SELECT c.relname AS table_name,
            a.attname AS column_name,
            pg_catalog.format_type(a.atttypid, a.atttypmod) AS type,
            a.attnotnull AS not_null,
            (a.atthasdef OR a.attidentity <> '') AS has_default
       FROM pg_catalog.pg_attribute AS a
       JOIN pg_catalog.pg_class AS c ON c.oid = a.attrelid
       JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
      WHERE n.nspname = 'sessions' AND c.relkind IN ('r', 'p')
        AND a.attnum > 0 AND NOT a.attisdropped`,
  );
  columns = new Map();
  for (const row of rows) {
    if (!columns.has(row.table_name)) columns.set(row.table_name, new Map());
    columns.get(row.table_name)!.set(row.column_name, row);
  }
});
afterAll(async () => {
  await database?.close();
});

/** Drizzle's SQL type spelled the way `format_type` spells it. */
function serverType(sqlType: string): string {
  const type = sqlType.replace(/\s+\(/g, "(");
  const aliases: Record<string, string> = {
    serial: "integer",
    bigserial: "bigint",
    smallserial: "smallint",
    timestamp: "timestamp without time zone",
    varchar: "character varying",
  };
  return aliases[type] ?? type;
}

describe("src/db/schema.ts matches the migrated sessions schema", () => {
  it("declares every table in the sessions schema", () => {
    expect(tables.length).toBeGreaterThan(10);
    for (const table of tables) {
      expect(getTableConfig(table).schema).toBe("sessions");
    }
  });

  it("covers every table the service reads and writes", () => {
    const declared = new Set(tables.map((t) => getTableConfig(t).name));
    for (const table of [
      "convergence_events",
      "conversation_turns",
      "auth_sessions",
      "machines",
      "sync_batches",
      "session_links",
      "session_outcomes",
      "daily_digests",
      "delivery_records",
      "delivery_receipts",
      "session_catalog",
    ]) {
      expect(declared.has(table), `schema.ts has no ${table}`).toBe(true);
      expect(columns.has(table), `migrations create no ${table}`).toBe(true);
    }
  });

  it("keeps every column schema.ts declares, so select() cannot fail on a missing one", () => {
    const missing: string[] = [];
    for (const table of tables) {
      const config = getTableConfig(table);
      const actual = columns.get(config.name);
      for (const column of config.columns) {
        if (!actual?.has(column.name))
          missing.push(`${config.name}.${column.name}`);
      }
    }
    expect(missing).toEqual([]);
    // The columns a hand-written fixture once lacked, failing every recall read.
    const events = columns.get("convergence_events")!;
    expect(events.has("embedding_model")).toBe(true);
    expect(events.get("embedding")?.type).toBe("vector(1536)");
    expect(events.has("code_churn")).toBe(true);
  });

  it("gives every column the database's type", () => {
    const drift: string[] = [];
    for (const table of tables) {
      const config = getTableConfig(table);
      for (const column of config.columns) {
        const actual = columns.get(config.name)?.get(column.name);
        if (!actual) continue;
        const declared = serverType(column.getSQLType());
        if (declared !== actual.type)
          drift.push(
            `${config.name}.${column.name}: schema.ts ${declared}, database ${actual.type}`,
          );
      }
    }
    expect(drift).toEqual([]);
  });

  it("gives every column the database's nullability", () => {
    const drift: string[] = [];
    for (const table of tables) {
      const config = getTableConfig(table);
      const primaryKey = new Set(
        config.primaryKeys.flatMap((key) => key.columns.map((c) => c.name)),
      );
      for (const column of config.columns) {
        const actual = columns.get(config.name)?.get(column.name);
        if (!actual) continue;
        const notNull = column.notNull || primaryKey.has(column.name);
        if (notNull !== actual.not_null)
          drift.push(
            `${config.name}.${column.name}: schema.ts ${notNull ? "NOT NULL" : "nullable"}, database ${actual.not_null ? "NOT NULL" : "nullable"}`,
          );
      }
    }
    expect(drift).toEqual([]);
  });

  it("has a server default wherever schema.ts lets an insert omit a column with DEFAULT", () => {
    const drift: string[] = [];
    for (const table of tables) {
      const config = getTableConfig(table);
      for (const column of config.columns) {
        const actual = columns.get(config.name)?.get(column.name);
        if (!actual) continue;
        // `$defaultFn` values are computed client-side; `.default()` and serial
        // columns are filled by the server.
        if (column.hasDefault && !column.defaultFn && !actual.has_default)
          drift.push(`${config.name}.${column.name}`);
      }
    }
    expect(drift).toEqual([]);
  });
});

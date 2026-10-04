/**
 * The engine's database handle: a Drizzle PostgreSQL database over the `sessions`
 * schema. Neon HTTP (hosted), node-postgres (self-hosted) and PGlite (tests) Drizzle
 * instances all satisfy it. Every query the engine runs is a single statement, so a
 * driver without interactive transactions is sufficient.
 */
import type { PgDatabase, PgQueryResultHKT } from "drizzle-orm/pg-core";
import type * as schema from "./schema.js";

/** Raw `execute` results expose `rows`, as every supported driver's result does. */
export interface HistoryQueryResult<T> {
  rows: T[];
}

export interface HistoryQueryResultHKT extends PgQueryResultHKT {
  type: HistoryQueryResult<this["row"]>;
}

export type HistoryDb = PgDatabase<HistoryQueryResultHKT, typeof schema>;

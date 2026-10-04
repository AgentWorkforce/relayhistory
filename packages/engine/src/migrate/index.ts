/**
 * The `sessions` schema's migration runner: one transaction over an advisory lock and
 * a checksum ledger (`sessions.__migrations`), then the hot-table indexes built
 * concurrently outside it. Node-only (reads the SQL files from disk); the hosted
 * service drives the same statements over its Neon transport, and a self-hosted server
 * calls `applyMigrations` with a node-postgres client at startup.
 *
 * The SQL files are the schema of record and are never edited once applied: a changed
 * checksum fails the run.
 */
import { createHash } from "node:crypto";
import { readdirSync, readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { splitSqlStatements } from "./split-sql-statements.js";

export { splitSqlStatements };

/** The packaged migrations, `packages/engine/migrations`. */
export const MIGRATIONS_DIR = resolve(
  dirname(fileURLToPath(import.meta.url)),
  "../../migrations",
);

export interface Migration {
  name: string;
  checksum: string;
  statements: string[];
}

export interface ConcurrentIndex {
  name: string;
  /** Skipped when this extension is not installed. */
  extension?: string;
  definition: string;
}

export type ConcurrentIndexAction =
  "valid" | "created" | "rebuilt" | "waited" | "skipped";

export interface ConcurrentIndexOptions {
  timeoutMs?: number;
  pollMs?: number;
  sleep?: (ms: number) => Promise<void>;
  now?: () => number;
}

// A migration is schema: tables, columns, functions, triggers and indexes on
// empty or small tables. Work that grows with stored history (backfills,
// reprojection, index builds on delivery_records or convergence_events) does
// not belong in this transaction and is bounded out of it by these limits.
// Earlier contents of migrations that were rewritten before reaching production.
// 0022 and 0023 originally built their indexes, switched their triggers on and
// backfilled inside the migration transaction (see 0029). A database that applied
// one of these versions already has that projection live and backfilled, so the
// version counts as applied and 0029 marks its rollout stage complete. Any other
// change to an applied migration still fails.
export const SUPERSEDED_CHECKSUMS: Readonly<Record<string, readonly string[]>> =
  {
    "0022_delivery_session_catalog.sql": [
      "0dd4f317d3eb479a73d04c007b78025494f9e500e2b0e7cc738057c4acaabf73",
      "211df3e420a5aa7cf13c6d25857b8350982bdbdb99a449ae01c062f0e36ae46a",
      "3cfc99672471feb644bd4c830ec2f2f9f6fb81cf24fa62758de1ea6628767729",
      "8ea764d14d559c80287f3b8d59d68c4855967b775c0b490f06024f2992bf39d3",
    ],
    "0023_delivery_projection_v2.sql": [
      "0ddb0df7c791956689869c06631dde7412408cbe21c9bfefe21ec1201122ec56",
      "3acdd386163bb62a22d4e69571f1dcc4825c3f2aaea626c0c07211b463a92ca1",
      "5879cbf00b2b93fc0dc5e37ea0d06753bbf793152c065750000ab8166489cc94",
      "cd19b19833755d5674d92d74dc5d6fe81b67b222b1017f725797a05edfb37945",
      "d40b3ac3399415c839a09645ed852c50bf1754892b11d9d4383e503632e9965c",
      "f429a28f1e9cc5cc11ce54850a06b6419b2bcaaee07d2f96e0ae50b3a9ae628a",
    ],
  };

export const STATEMENT_TIMEOUT = "5min";
export const TRANSACTION_TIMEOUT = "10min";

export function readMigrations(
  directory: string = MIGRATIONS_DIR,
): Migration[] {
  return readdirSync(directory)
    .filter((name) => name.endsWith(".sql"))
    .sort()
    .map((name) => {
      const source = readFileSync(resolve(directory, name), "utf8");
      return {
        name,
        checksum: createHash("sha256").update(source).digest("hex"),
        statements: splitSqlStatements(source),
      };
    });
}

function literal(value: string): string {
  return "'" + value.replaceAll("'", "''") + "'";
}

// One prepared statement per query, all inside ONE transaction. The lock
// serializes local and CI runners; ledger checks occur after acquiring the lock.
// DO is a single SQL statement even though its body executes several commands.
export function migrationStatements(
  migrations: Migration[],
  runtimeRole?: string,
): string[] {
  if (migrations.length === 0) throw new Error("No sessions migrations found");
  return [
    "SELECT pg_advisory_xact_lock(1919249529, 1)",
    "SET LOCAL search_path = pg_catalog, public",
    "CREATE SCHEMA IF NOT EXISTS sessions",
    `CREATE TABLE IF NOT EXISTS sessions.__migrations (
      name text PRIMARY KEY,
      checksum text NOT NULL,
      applied_at timestamptz NOT NULL DEFAULT now()
    )`,
    ...migrations.map(({ name, checksum, statements }, migrationIndex) => {
      let delimiter = "$relayhistory_migration$";
      while (statements.some((statement) => statement.includes(delimiter))) {
        delimiter = delimiter.replace("$", "$_");
      }
      return `DO ${delimiter}
BEGIN
  IF EXISTS (SELECT 1 FROM sessions.__migrations
             WHERE name = ${literal(name)} AND checksum <> ALL (ARRAY[${[checksum, ...(SUPERSEDED_CHECKSUMS[name] ?? [])].map(literal).join(", ")}])) THEN
    RAISE EXCEPTION 'Applied sessions migration checksum changed: %', ${literal(name)};
  END IF;
  IF NOT EXISTS (SELECT 1 FROM sessions.__migrations WHERE name = ${literal(name)}) THEN
    ${statements
      .map(
        (
          statement,
          statementIndex,
        ) => `RAISE NOTICE 'Sessions migration detail ${migrationIndex + 1}/${migrations.length} statement ${statementIndex + 1}/${statements.length}: start';
    EXECUTE ${literal(statement)};
    RAISE NOTICE 'Sessions migration detail ${migrationIndex + 1}/${migrations.length} statement ${statementIndex + 1}/${statements.length}: done';`,
      )
      .join("\n    ")}
    INSERT INTO sessions.__migrations (name, checksum)
    VALUES (${literal(name)}, ${literal(checksum)});
  END IF;
END
${delimiter}`;
    }),
    // Reuse Cloud's existing runtime role; no new roles or public grants. Refresh
    // grants after every run so tables/sequences from future migrations work too.
    ...(runtimeRole
      ? [
          `DO $runtime_grants$
DECLARE relation record;
BEGIN
  EXECUTE format('GRANT USAGE ON SCHEMA sessions TO %I', ${literal(runtimeRole)});
  FOR relation IN SELECT tablename FROM pg_catalog.pg_tables
    WHERE schemaname = 'sessions' AND tablename <> '__migrations'
  LOOP
    EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE sessions.%I TO %I', relation.tablename, ${literal(runtimeRole)});
  END LOOP;
  EXECUTE format('GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA sessions TO %I', ${literal(runtimeRole)});
  EXECUTE format('GRANT EXECUTE ON FUNCTION sessions.enqueue_session_analysis_job(text, text, text, text, text, text, text, integer, text, text, text, jsonb) TO %I', ${literal(runtimeRole)});
  EXECUTE format('GRANT EXECUTE ON FUNCTION sessions.request_session_analysis_v1(text, text, text, text) TO %I', ${literal(runtimeRole)});
  EXECUTE format('GRANT EXECUTE ON FUNCTION sessions.read_session_analysis_v1(text, text) TO %I', ${literal(runtimeRole)});
  EXECUTE format('GRANT EXECUTE ON FUNCTION sessions.collect_session_analysis_request(uuid, uuid, text, text, text, integer, text, text, text, jsonb) TO %I', ${literal(runtimeRole)});
  EXECUTE format('GRANT EXECUTE ON FUNCTION sessions.session_brief_v1(text, text, text, text, text) TO %I', ${literal(runtimeRole)});
  EXECUTE format('GRANT EXECUTE ON FUNCTION sessions.collect_session_analysis_request_v2(uuid, uuid, text, text, text, text, jsonb, text, text, integer, text, jsonb) TO %I', ${literal(runtimeRole)});
  EXECUTE format('GRANT EXECUTE ON FUNCTION sessions.session_brief_v2(text, text, text, text, text, text) TO %I', ${literal(runtimeRole)});
END
$runtime_grants$`,
        ]
      : []),
    "SELECT name, checksum FROM sessions.__migrations ORDER BY name",
  ];
}

/**
 * Indexes on hot tables are not built inside the migration transaction: a plain
 * CREATE INDEX holds a write lock on the table for the whole build, which would
 * stall live delivery for the length of a deploy. They are built here, after the
 * migrations commit, one autocommit statement each, with CREATE INDEX
 * CONCURRENTLY. Every run re-checks them, so the step is idempotent and an
 * interrupted build (which leaves an INVALID index that IF NOT EXISTS would skip
 * forever) is dropped and rebuilt. An entry whose extension is not installed is
 * skipped, like the guarded migration that installs it.
 */
export const CONCURRENT_INDEXES: readonly ConcurrentIndex[] = [
  {
    name: "convergence_events_content_trgm_idx",
    extension: "pg_trgm",
    definition:
      "ON sessions.convergence_events USING gin (content public.gin_trgm_ops)",
  },
  {
    name: "convergence_events_task_title_trgm_idx",
    extension: "pg_trgm",
    definition:
      "ON sessions.convergence_events USING gin (task_title public.gin_trgm_ops) WHERE task_title IS NOT NULL",
  },
];

/**
 * Ensure every CONCURRENT_INDEXES entry exists and is valid. `query(text)` runs one
 * statement outside any transaction and resolves to its rows; single-column
 * results are read from the `v` column. Returns what it did per index.
 *
 * The migration's advisory lock ends at commit and this step runs on its own
 * session, so no lock spans it and two overlapping deploys can both
 * reach it. An INVALID index is therefore dropped only when no backend is
 * building it: while pg_stat_progress_create_index shows a build of exactly this
 * index, or a build on its table not yet registered (index_relid = 0), the step
 * waits, then re-checks, so an index the other runner failed to build is still
 * rebuilt here. A wait past `timeoutMs` throws, failing the deploy rather than
 * reporting success with an unusable index. Two runners racing to create the
 * same missing index lose only a duplicate-name error, which re-enters the loop.
 * Progress rows are visible across sessions of the same role, which every runner
 * is (the migration owner).
 */
export async function ensureConcurrentIndexes(
  query: (text: string) => Promise<Record<string, unknown>[]>,
  indexes: readonly ConcurrentIndex[] = CONCURRENT_INDEXES,
  {
    timeoutMs = 10 * 60_000,
    pollMs = 2_000,
    sleep = (ms: number) => new Promise<void>((done) => setTimeout(done, ms)),
    now = () => Date.now(),
  }: ConcurrentIndexOptions = {},
): Promise<Record<string, ConcurrentIndexAction>> {
  const actions: Record<string, ConcurrentIndexAction> = {};
  for (const { name, extension, definition } of indexes) {
    if (extension) {
      const installed = await query(
        `SELECT true AS v FROM pg_catalog.pg_extension WHERE extname = ${literal(extension)}`,
      );
      if (!installed.length) {
        actions[name] = "skipped";
        continue;
      }
    }
    const deadline = now() + timeoutMs;
    let action: ConcurrentIndexAction | null = null;
    for (;;) {
      const [row] = await query(
        `SELECT CASE
             WHEN i.indisvalid THEN 'valid'
             WHEN EXISTS (
               SELECT 1 FROM pg_catalog.pg_stat_progress_create_index AS p
                WHERE p.index_relid = i.indexrelid
                   OR (p.relid = i.indrelid AND p.index_relid = 0)
             ) THEN 'building'
             ELSE 'invalid' END AS v
           FROM pg_catalog.pg_index AS i
           JOIN pg_catalog.pg_class AS c ON c.oid = i.indexrelid
           JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
          WHERE n.nspname = 'sessions' AND c.relname = ${literal(name)}`,
      );
      const state = (row?.v as string | undefined) ?? "missing";
      if (state === "valid") {
        actions[name] = action ?? "valid";
        break;
      }
      if (state === "building") {
        if (now() >= deadline)
          throw new Error(
            `Timed out waiting for another build of sessions.${name}`,
          );
        action ??= "waited";
        await sleep(pollMs);
        continue;
      }
      if (state === "invalid") {
        await query(`DROP INDEX CONCURRENTLY IF EXISTS sessions.${name}`);
        action = "rebuilt";
        continue;
      }
      try {
        await query(
          `CREATE INDEX CONCURRENTLY IF NOT EXISTS ${name} ${definition}`,
        );
        if (action !== "rebuilt") action = "created";
      } catch (error) {
        // Another runner created the same name between the check and this build.
        if (!duplicateRelation(error)) throw error;
      }
    }
  }
  return actions;
}

function duplicateRelation(error: unknown): boolean {
  const failure = error as { code?: unknown; message?: unknown } | null;
  return (
    failure?.code === "23505" ||
    failure?.code === "42P07" ||
    /already exists|duplicate key/i.test(String(failure?.message ?? ""))
  );
}

/** A node-postgres-compatible client holding one connection. */
export interface MigrationClient {
  query(text: string): Promise<{ rows: Record<string, unknown>[] }>;
  on?(
    event: "notice",
    listener: (notice: { message?: string }) => void,
  ): unknown;
  removeListener?(
    event: "notice",
    listener: (notice: { message?: string }) => void,
  ): unknown;
}

export interface ApplyMigrationsOptions {
  /** Granted runtime access to the `sessions` schema after the run. */
  runtimeRole?: string;
  /** Defaults to `MIGRATIONS_DIR`. */
  directory?: string;
  /** Progress lines: fixed phase and statement counters only, never SQL or rows. */
  report?: (line: string) => void;
  indexes?: ConcurrentIndexOptions;
}

export interface AppliedMigrations {
  /** Every migration in the ledger after the run. */
  applied: { name: string; checksum: string }[];
  indexes: Record<string, ConcurrentIndexAction>;
}

const PROGRESS =
  /^Sessions migration detail [1-9]\d*\/[1-9]\d* statement [1-9]\d*\/[1-9]\d*: (start|done)$/;

/**
 * Apply every pending migration in one transaction on `client`, then ensure the
 * concurrent indexes on the same connection outside any transaction. Safe to run from
 * several processes at once: the advisory lock serializes them and an applied
 * migration is skipped.
 */
export async function applyMigrations(
  client: MigrationClient,
  options: ApplyMigrationsOptions = {},
): Promise<AppliedMigrations> {
  const report = options.report ?? (() => {});
  const statements = migrationStatements(
    readMigrations(options.directory),
    options.runtimeRole,
  );
  const onNotice = (notice: { message?: string }) => {
    // Driver notices may contain SQL or row contents. Only report the fixed, numeric
    // progress markers from the migration wrapper.
    if (typeof notice?.message === "string" && PROGRESS.test(notice.message))
      report(notice.message);
  };
  let transactionStarted = false;
  let ledger: Record<string, unknown>[] = [];
  client.on?.("notice", onNotice);
  try {
    await client.query("BEGIN");
    transactionStarted = true;
    // Fail a blocked advisory or DDL lock, and bound how long the migration may hold
    // its locks, so a stuck run rolls back instead of stalling uploads.
    await client.query("SET LOCAL lock_timeout = '30s'");
    // `transaction_timeout` is PostgreSQL 17+; on an older server each statement is
    // still bounded by `statement_timeout`.
    const [setting] = (
      await client.query(
        "SELECT current_setting('transaction_timeout', true) AS v",
      )
    ).rows;
    if (setting?.v != null)
      await client.query(
        `SET LOCAL transaction_timeout = '${TRANSACTION_TIMEOUT}'`,
      );
    await client.query(`SET LOCAL statement_timeout = '${STATEMENT_TIMEOUT}'`);
    for (const [index, statement] of statements.entries()) {
      report(
        `Sessions migration statement ${index + 1}/${statements.length}: start`,
      );
      ledger = (await client.query(statement)).rows;
      report(
        `Sessions migration statement ${index + 1}/${statements.length}: done`,
      );
    }
    await client.query("COMMIT");
    transactionStarted = false;
  } catch (error) {
    if (transactionStarted) {
      try {
        await client.query("ROLLBACK");
      } catch {
        // Preserve the migration failure; the caller retires the connection.
      }
    }
    throw error;
  } finally {
    client.removeListener?.("notice", onNotice);
  }
  const indexes = await ensureConcurrentIndexes(
    async (text) => (await client.query(text)).rows,
    CONCURRENT_INDEXES,
    options.indexes,
  );
  return {
    applied: ledger.map((row) => ({
      name: String(row.name),
      checksum: String(row.checksum),
    })),
    indexes,
  };
}

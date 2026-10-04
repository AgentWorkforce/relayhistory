/**
 * Online rollouts that complete the schema after migrations: the 0029 activation and
 * backfill of the delivered-session catalog and activity projection, and the 0030
 * session-rollup backfill. Until the first completes, delivered records are stored but
 * not projected for recall; until the second completes, `GET /v1/sessions` reads
 * events instead of rollups.
 *
 * Each batch is its own short transaction, so uploads continue throughout, and the
 * cursors are durable: a rerun resumes where the last run stopped. `query(sql)` runs
 * one statement on a dedicated connection and resolves to its rows.
 *
 * Each rollout holds a session-level advisory lock on that connection for its whole
 * run, so replicas that migrate together take turns: the migration's own lock ends at
 * its commit, and two runners building the same index concurrently deadlock. The one
 * that waits then finds every stage complete.
 */
export type RolloutQuery = (sql: string) => Promise<Record<string, unknown>[]>;

export interface DeliveryProjectionRolloutOptions {
  /** `false` builds indexes in the caller's session, for databases without CONCURRENTLY. */
  concurrently?: boolean;
  batch?: number;
  /** Epoch ms after which the rollout pauses; rerun to resume. */
  deadline?: number;
  report?: (line: string) => void;
}

export interface SessionRollupsRolloutOptions {
  batch?: number;
  deadline?: number;
  report?: (line: string) => void;
}

/** Bounded so a batch never stalls uploads past its own transaction. */
export const SESSION_ROLLUPS_BATCH = 200;

const STAGES = [
  {
    stage: "catalog",
    activate: "activate_delivery_catalog",
    step: "delivery_catalog_backfill_step",
    unit: "keys",
  },
  {
    stage: "activity",
    activate: "activate_delivery_projection_v2",
    step: "delivery_activity_reproject_step",
    unit: "records",
  },
];
// A batch holds its workspace's projection lock until it commits. On a copy of
// production (2026-10-03) 200 catalog keys took a median 250 ms but up to 6 s on
// keys with many contenders; 100 halves that worst case at the same throughput.
export const DELIVERY_PROJECTION_BATCH = 100;
// One batch's ceiling. A batch that cannot finish rolls back alone; nothing an
// upload waits on is held longer than this.
export const ROLLOUT_STEP_TIMEOUT = "2min";

// A batch that meets an upload's row lock yields rather than waits (see 0029).
const LOCK_RETRIES = 30;
const RETRY_DELAY_MS = 1000;

const IDENTIFIER = /^[a-z_][a-z0-9_]*$/;

// Session-level advisory lock keys, beside the migration's (1919249529, 1).
const DELIVERY_PROJECTION_LOCK = "1919249529, 3";
const SESSION_ROLLUPS_LOCK = "1919249529, 4";
const LOCK_POLL_MS = 1000;

const sleep = (ms: number) => new Promise<void>((done) => setTimeout(done, ms));

function requirePositiveBatch(batch: number): void {
  // The step functions return 0 for LIMIT 0 without completing, so the loop would
  // never end.
  if (!Number.isInteger(batch) || batch < 1)
    throw new RangeError("rollout batch must be a positive integer");
}

/**
 * Runs `run` holding the advisory lock `key` on the rollout's connection, or returns
 * `undefined` when `deadline` passes before the lock is free. Waiting polls
 * `pg_try_advisory_lock` instead of blocking in `pg_advisory_lock`: a blocked
 * statement keeps its snapshot, and CREATE INDEX CONCURRENTLY in the holder waits for
 * every older snapshot, so a blocked waiter would deadlock the build it waits on.
 */
async function withRolloutLock<T>(
  query: RolloutQuery,
  key: string,
  deadline: number,
  run: () => Promise<T>,
): Promise<T | undefined> {
  for (;;) {
    const [row] = await query(`SELECT pg_try_advisory_lock(${key}) AS locked`);
    if (row?.locked === true) break;
    if (Date.now() >= deadline) return undefined;
    await sleep(LOCK_POLL_MS);
  }
  let result: T;
  try {
    result = await run();
  } catch (error) {
    // Preserve the rollout failure; a broken connection releases the lock anyway.
    await query(`SELECT pg_advisory_unlock(${key})`).catch(() => {});
    throw error;
  }
  await query(`SELECT pg_advisory_unlock(${key})`);
  return result;
}

/**
 * Runs every pending stage through `query(sql) -> rows`. In production each call
 * is one autocommit statement on a dedicated connection; tests pass PGlite.
 * `concurrently: false` builds indexes in the caller's session for test
 * databases that cannot run CREATE INDEX CONCURRENTLY.
 */
export async function rolloutDeliveryProjection(
  query: RolloutQuery,
  {
    concurrently = true,
    batch = DELIVERY_PROJECTION_BATCH,
    deadline = Number.POSITIVE_INFINITY,
    report = () => {},
  }: DeliveryProjectionRolloutOptions = {},
): Promise<{ complete: true } | { complete: false; stage: string }> {
  requirePositiveBatch(batch);
  const outcome = await withRolloutLock(
    query,
    DELIVERY_PROJECTION_LOCK,
    deadline,
    () => runDeliveryProjection(query, concurrently, batch, deadline, report),
  );
  if (outcome) return outcome;
  const stage = STAGES[0]!.stage;
  report(`Rollout ${stage}: another runner holds the rollout; rerun to resume`);
  return { complete: false, stage };
}

async function runDeliveryProjection(
  query: RolloutQuery,
  concurrently: boolean,
  batch: number,
  deadline: number,
  report: (line: string) => void,
): Promise<{ complete: true } | { complete: false; stage: string }> {
  const paused = (stage: string, done: string) => {
    report(`Rollout ${stage}: paused after ${done}; rerun to resume`);
    return { complete: false as const, stage };
  };
  for (const { stage, activate, step, unit } of STAGES) {
    const [stageState] = await query(
      `SELECT sessions.delivery_rollout_stage_requires_indexes('${stage}') AS requires_indexes`,
    );
    if (stageState?.requires_indexes === false) {
      // Persist the compatibility completion row when an earlier 0022/0023
      // already made the projection live. Do this before reading an index
      // definition that may reference helpers that historical schemas lack.
      await query(`SELECT sessions.${activate}() AS activated`);
      report(`Rollout ${stage}: already complete`);
      continue;
    }
    const indexes = await query(
      `SELECT index_name, definition FROM sessions.delivery_rollout_indexes() WHERE stage = '${stage}' ORDER BY index_name`,
    );
    for (const { index_name: name, definition } of indexes as {
      index_name: string;
      definition: string;
    }[]) {
      if (!IDENTIFIER.test(name)) throw new Error("Unexpected index name");
      const [state] = await query(
        `SELECT i.indisvalid AND i.indisready AS valid FROM pg_index AS i
           JOIN pg_class AS c ON c.oid = i.indexrelid
           JOIN pg_namespace AS n ON n.oid = c.relnamespace
          WHERE n.nspname = 'sessions' AND c.relname = '${name}'`,
      );
      if (state?.valid) continue;
      if (Date.now() >= deadline) return paused(stage, `0 ${unit}`);
      // An interrupted concurrent build leaves an invalid index: rebuild it.
      if (state)
        await query(
          `DROP INDEX ${concurrently ? "CONCURRENTLY " : ""}sessions.${name}`,
        );
      report(`Rollout ${stage}: building index ${name}`);
      await query(
        `CREATE INDEX ${concurrently ? "CONCURRENTLY " : ""}${name} ${definition}`,
      );
      report(`Rollout ${stage}: index ${name} ready`);
    }
    if (Date.now() >= deadline) return paused(stage, `0 ${unit}`);
    const [activated] = await query(
      `SELECT sessions.${activate}() AS activated`,
    );
    if (activated?.activated) report(`Rollout ${stage}: projection live`);
    let total = 0;
    for (;;) {
      if (Date.now() >= deadline) return paused(stage, `${total} ${unit}`);
      let processed: number;
      for (let attempt = 1; ; attempt += 1) {
        await query("BEGIN");
        try {
          await query(
            `SET LOCAL statement_timeout = '${ROLLOUT_STEP_TIMEOUT}'`,
          );
          await query("SET LOCAL lock_timeout = '30s'");
          const [row] = await query(
            `SELECT sessions.${step}(${Number(batch)}) AS processed`,
          );
          processed = Number(row?.processed ?? 0);
          await query("COMMIT");
          break;
        } catch (error) {
          await query("ROLLBACK").catch(() => {});
          // 55P03: an upload holds a row this batch needs. The batch yielded;
          // retry it once the upload has committed.
          if (
            (error as { code?: unknown })?.code !== "55P03" ||
            attempt >= LOCK_RETRIES
          )
            throw error;
          await new Promise((done) => setTimeout(done, RETRY_DELAY_MS));
        }
      }
      total += processed;
      if (processed > 0 && total % (batch * 50) < batch)
        report(`Rollout ${stage}: ${total} ${unit} so far`);
      if (processed < batch) break;
    }
    report(`Rollout ${stage}: complete (${total} ${unit} this run)`);
  }
  return { complete: true };
}

/**
 * Drives session_rollup_backfill_step() to completion through `query(sql) -> rows`.
 * In production each call is one statement on a dedicated connection; tests pass
 * PGlite.
 */
export async function rolloutSessionRollups(
  query: RolloutQuery,
  {
    batch = SESSION_ROLLUPS_BATCH,
    deadline = Number.POSITIVE_INFINITY,
    report = () => {},
  }: SessionRollupsRolloutOptions = {},
): Promise<{ complete: boolean; sessions: number }> {
  requirePositiveBatch(batch);
  const outcome = await withRolloutLock(
    query,
    SESSION_ROLLUPS_LOCK,
    deadline,
    () => runSessionRollups(query, batch, deadline, report),
  );
  if (outcome) return outcome;
  report("Session rollups: another runner holds the backfill; rerun to resume");
  return { complete: false, sessions: 0 };
}

async function runSessionRollups(
  query: RolloutQuery,
  batch: number,
  deadline: number,
  report: (line: string) => void,
): Promise<{ complete: boolean; sessions: number }> {
  let total = 0;
  for (;;) {
    if (Date.now() >= deadline) {
      report(
        `Session rollups: paused after ${total} sessions; rerun to resume`,
      );
      return { complete: false, sessions: total };
    }
    let processed: number;
    for (let attempt = 1; ; attempt += 1) {
      await query("BEGIN");
      try {
        await query(`SET LOCAL statement_timeout = '${ROLLOUT_STEP_TIMEOUT}'`);
        const [row] = await query(
          `SELECT sessions.session_rollup_backfill_step(${Number(batch)}) AS processed`,
        );
        processed = Number(row?.processed ?? 0);
        await query("COMMIT");
        break;
      } catch (error) {
        await query("ROLLBACK").catch(() => {});
        // 55P03: an upload holds a session this batch needs. Retry once it commits.
        if (
          (error as { code?: unknown })?.code !== "55P03" ||
          attempt >= LOCK_RETRIES
        )
          throw error;
        await new Promise((done) => setTimeout(done, RETRY_DELAY_MS));
      }
    }
    total += processed;
    if (processed > 0 && total % (batch * 50) < batch)
      report(`Session rollups: ${total} sessions so far`);
    if (processed < batch) break;
  }
  // A just-filled table has no statistics until autovacuum reaches it, and without
  // them the planner scans every rollup of the org instead of one page.
  if (total > 0) await query("ANALYZE sessions.session_rollups");
  report(`Session rollups: complete (${total} sessions this run)`);
  return { complete: true, sessions: total };
}

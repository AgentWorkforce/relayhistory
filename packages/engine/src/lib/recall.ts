/**
 * Recall: the read path out of `convergence_events`.
 *
 * Until this existed the convergence store was, in practice, write-only. `applyIngest`
 * wrote it and `checkPairWarnings` matched against it, and nothing else in the Worker
 * touched it — so 302,643 events across 57,239 sessions could only be reached with direct
 * SQL against Neon. The capture side had been the whole focus; getting context back out
 * was the missing half.
 *
 * Three questions, three reads:
 *   1. "Replay this session."          → `getSessionEvents`  (ordered transcript)
 *   2. "Which sessions touched X?"     → `listSessions`      (rollup for discovery)
 *   3. "Find events matching X."       → `queryEvents`       (cross-session search)
 *
 * Every read is org-scoped from the auth context, never from a caller-supplied parameter,
 * and every read is bounded and cursor-paged: an agent pulling context for a PR must not
 * be able to ask for 300k rows in one request.
 */
import {
  and,
  asc,
  desc,
  eq,
  getTableColumns,
  gte,
  inArray,
  isNull,
  lte,
  or,
  sql,
  type SQL,
} from "drizzle-orm";
import type { HistoryDb as Db } from "../db/database.js";
import { convergenceEvents, conversationTurns } from "../db/schema.js";
import type { AuthContext } from "../env.js";
import {
  catalogSummaries,
  type SessionCatalogSummary,
} from "./session-catalog.js";

import {
  sessionWorkStatus,
  selectedSessions,
  sessionIdentityKey,
  conversationSource,
  selectedSessionTurns,
} from "./session-work-state.js";

export const DEFAULT_EVENT_LIMIT = 200;
export const MAX_EVENT_LIMIT = 1000;
export const DEFAULT_SESSION_LIMIT = 50;
export const MAX_SESSION_LIMIT = 500;

/**
 * Cap on `content` returned per event, when the caller asks for it to be capped.
 *
 * A replay is read by agents with finite context windows as often as by people. Returning
 * a 256 KB record inline can blow a caller's budget on a single event, so `maxContent`
 * truncates with an explicit marker. It is opt-in: the default returns content whole,
 * because a truncation the caller did not ask for and cannot see is how a replay quietly
 * stops being a replay.
 */
export const CONTENT_TRUNCATION_NOTE = "…[truncated by maxContent]";

export interface RecallEvent {
  eventId: string;
  userId?: string | null;
  sessionId: string;
  source: string;
  lens: string | null;
  kind: string;
  type: string;
  ts: string;
  actorName: string | null;
  actorRole: string | null;
  subagentId: string | null;
  machineId: string;
  projectId: string | null;
  trajectoryId: string | null;
  taskRef: unknown;
  taskTitle: string | null;
  taskDescription: string | null;
  taskStatus: string | null;
  content: string | null;
  contentTruncated: boolean;
  significance: string | null;
  /** Re-expanded from basis points to the 0..1 the source reported. */
  confidence: number | null;
  tags: unknown;
  model: string | null;
  provider: string | null;
  usage: {
    input: number;
    output: number;
    reasoning: number;
    cacheRead: number;
    /** Every cache-write bucket summed. */
    cacheCreate: number;
    /** The 5-minute / 1-hour cache-write split; null when the client did not report it. */
    cacheCreate5m: number | null;
    cacheCreate1h: number | null;
  };
  /** Null when the client sent no cost; never a fabricated zero. */
  costUsdMicros: number | null;
  toolName: string | null;
  toolStatus: string | null;
  toolCalls: unknown;
  filesTouched: unknown;
  durationMs: number | null;
}

export interface SessionSummary {
  /** Latest recorded work signal; metadata refreshes and uploads are excluded. */
  workState?: {
    status: "active" | "idle" | "finished";
    updatedAt: string;
  } | null;
  sessionId: string;
  userIds?: string[];
  source: string;
  projectId: string | null;
  machineIds: string[];
  firstTs: string;
  lastTs: string;
  eventCount: number;
  kinds: string[];
  models: string[];
  /** The most recent non-null task title — the closest thing to a session name. */
  taskTitle: string | null;
  /**
   * A one-line answer to "what was this session?", so a rollup is readable without
   * opening every session. `taskTitle` when the harness recorded one; otherwise the
   * session's opening user turn, which is the closest thing to an intent we have.
   *
   * Null only when neither exists. That is deliberately distinguishable from an empty
   * string: "this session has no recoverable intent" is a real finding, and rendering
   * it as "" would let a caller print a blank line and call it a summary.
   */
  summary: string | null;
  /** Which source `summary` came from, so a reader can judge how much to trust it. */
  summarySource: "task_title" | "first_user_turn" | null;
  taskRefs: unknown[];
  /** Sum over the events that carried a cost; null when none did. */
  totalCostUsdMicros: number | null;
  totalInputTokens: number;
  totalOutputTokens: number;
  totalReasoningTokens: number;
  totalCacheReadTokens: number;
  /** Every cache-write bucket summed. */
  totalCacheCreateTokens: number;
  /**
   * The 5-minute / 1-hour cache-write split, reported only when every event that wrote
   * cache reported a split covering its whole write (usage-accounting's all-or-nothing
   * rule): a partial split would silently price the unsplit tokens at neither rate.
   * An event that wrote no cache is a known zero and never makes the split partial.
   */
  totalCacheCreate5mTokens: number | null;
  totalCacheCreate1hTokens: number | null;
  /**
   * The delivered session catalog (branch, repository, project key, models, agent
   * version, parent/child relationships). Absent when no single catalog row belongs to
   * the workspaces this session's events came from.
   */
  catalog?: SessionCatalogSummary;
}

export interface EventFilters {
  sessionId?: string;
  projectId?: string;
  /**
   * Matches `project_id IS NULL` exactly — the unattributed-project bucket, not "no
   * project filter was given". A caller that wants the null bucket cannot express it as
   * `?project=` (an empty string is indistinguishable from an absent parameter once a
   * route does `query("project") || undefined`), so it needs its own flag rather than a
   * sentinel value overloaded onto `projectId`.
   */
  noProject?: boolean;
  /** Substring match, so `AgentWorkforce/relayhistory-cloud` finds every PR under it. */
  projectContains?: string;
  source?: string;
  kind?: string;
  /**
   * A single id, or several. A single string preserves the original behavior exactly —
   * an exact `task_ref->>'id'` match, falling back to a whole-document substring match
   * so a differently-shaped ref still finds something. Several ids (the epic drill-down
   * of `docs/specs/2026-09-05-reflex-day-view.md` §4, one per `epicKeys` entry) match
   * `task_ref->>'id'` exactly against any of them — no ILIKE fallback, since a fallback
   * substring match across several unrelated ids would blur exactly the boundary the
   * drill-down exists to keep, and stop the `sessionCount` on `/v1/epics` from being
   * reproducible by any single request.
   */
  taskRef?: string | string[];
  /**
   * Matches `task_ref = '{}'::jsonb` exactly — the unattributed bucket (spec §0/§4 of
   * `docs/specs/2026-09-05-reflex-day-view.md`), not "no taskRef filter was given". Kept
   * separate from `taskRef` rather than overloading it with a sentinel, since `{}` is a
   * real, queryable value and not the absence of one.
   */
  noTaskRef?: boolean;
  tag?: string;
  since?: Date;
  until?: Date;
  /** Case-insensitive substring over content and task title. */
  q?: string;
}

export interface PageOptions {
  source?: string;
  limit?: number;
  /** Opaque cursor from a previous page's `nextCursor`. */
  cursor?: string | null;
  order?: "asc" | "desc";
  maxContent?: number;
}

/**
 * An explicitly requested tenant narrowing for a recall read.
 *
 * Legacy recall is organization-wide. Routes opt into the narrower contract only after
 * proving the requested workspace is the workspace carried by the authenticated token;
 * callers of this library must never fill this from an untrusted query parameter.
 */
export interface RecallScope {
  workspaceId?: string;
}

export function clampLimit(
  raw: unknown,
  fallback: number,
  max: number,
): number {
  const parsed =
    typeof raw === "number"
      ? raw
      : typeof raw === "string"
        ? Number(raw)
        : Number.NaN;
  if (!Number.isFinite(parsed)) {
    return fallback;
  }
  return Math.min(Math.max(Math.trunc(parsed), 1), max);
}

/**
 * Legacy cursors are `<iso ts>|<event id>`. Event pages now emit a v2 cursor
 * with the remaining import identity fields to break ties across machines.
 *
 * Offset paging over a table that is being written to while an agent pages through it
 * silently skips and repeats rows. Keying on the same (ts, eventId) pair the query orders
 * by makes a page boundary stable regardless of what arrives mid-read.
 */
export function encodeCursor(ts: Date | string, eventId: string): string {
  const iso =
    ts instanceof Date ? ts.toISOString() : new Date(ts).toISOString();
  return `${iso}|${eventId}`;
}

export function decodeCursor(
  cursor: string | null | undefined,
): { ts: Date; eventId: string } | null {
  if (!cursor) {
    return null;
  }
  const split = cursor.indexOf("|");
  if (split <= 0) {
    return null;
  }
  const ts = new Date(cursor.slice(0, split));
  const eventId = cursor.slice(split + 1);
  if (!Number.isFinite(ts.getTime()) || !eventId) {
    return null;
  }
  return { ts, eventId };
}

/**
 * A row's exact `timestamptz` as ISO-8601 UTC with microseconds.
 *
 * Cursors carry this text and bind it back as `::timestamptz`. A JS `Date` holds only
 * milliseconds, and delivery stamps rows with `now()`, so a cursor built from one sits
 * between rows that share a millisecond and the keyset comparison skips or repeats them.
 */
function exactTs(column: SQL): SQL<string> {
  return sql<string>`to_char(${column} at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')`;
}

const CURSOR_TS = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?Z$/u;

/**
 * The cursor timestamp, or null. Accepts the microsecond text `exactTs` produces and the
 * millisecond `toISOString()` text of earlier cursors; anything else never reaches
 * `::timestamptz`.
 *
 * The date and time fields must survive a parse unchanged: JS rolls an impossible date
 * (February 30, `24:00`) into a real one, where Postgres rejects it. Postgres has no
 * year 0, which JS accepts.
 */
function cursorTs(value: unknown): string | null {
  if (typeof value !== "string" || !CURSOR_TS.test(value)) return null;
  const parsed = new Date(value);
  return Number.isFinite(parsed.getTime()) &&
    parsed.getUTCFullYear() >= 1 &&
    parsed.toISOString().slice(0, 19) === value.slice(0, 19)
    ? value
    : null;
}

type EventCursor = {
  ts: string;
  eventId: string;
  machineId?: string;
  source?: string;
  sessionId?: string;
  kind?: string;
};

function encodeEventCursor(row: {
  cursorTs: string;
  eventId: string;
  machineId: string;
  source: string;
  sessionId: string;
  kind: string;
}): string {
  return `events-v2:${encodeURIComponent(
    JSON.stringify([
      row.cursorTs,
      row.eventId,
      row.machineId,
      row.source,
      row.sessionId,
      row.kind,
    ]),
  )}`;
}

function decodeEventCursor(
  value: string | null | undefined,
): EventCursor | null {
  if (!value?.startsWith("events-v2:")) {
    const legacy = decodeCursor(value);
    return legacy
      ? { ts: legacy.ts.toISOString(), eventId: legacy.eventId }
      : null;
  }
  try {
    const fields: unknown = JSON.parse(decodeURIComponent(value.slice(10)));
    if (
      !Array.isArray(fields) ||
      fields.length !== 6 ||
      fields.some((field) => typeof field !== "string" || !field)
    )
      return null;
    const [stamp, eventId, machineId, source, sessionId, kind] =
      fields as string[];
    const ts = cursorTs(stamp);
    return ts
      ? { ts, eventId: eventId!, machineId, source, sessionId, kind }
      : null;
  } catch {
    return null;
  }
}

/**
 * Normalise a timestamp to ISO-8601 UTC.
 *
 * Not cosmetic. Drizzle hands back a `Date` for a mapped `timestamp` column but a raw
 * driver string for an aggregate like `max(ts)` — and that string arrives in the server's
 * local offset (`2026-09-01 11:00:00+01`). Passing it through meant a session's `firstTs`
 * and `lastTs` disagreed in both format and offset with the `ts` on the events inside it,
 * so a caller comparing the two would place events outside the session that contains them.
 *
 * Throws rather than falling back to the raw string: every value reaching here comes from
 * a `timestamptz` column, so one that will not parse means the shape changed underneath
 * us, and a 500 is better than a transcript with a plausible but wrong clock.
 */
function toIso(value: unknown): string {
  if (value instanceof Date) {
    return value.toISOString();
  }
  const parsed = new Date(String(value));
  if (!Number.isFinite(parsed.getTime())) {
    throw new Error(
      `recall: unparseable timestamp from the database: ${String(value)}`,
    );
  }
  return parsed.toISOString();
}

function num(value: unknown): number {
  const parsed = typeof value === "number" ? value : Number(value ?? 0);
  return Number.isFinite(parsed) ? parsed : 0;
}

/** Like `num`, but an absent value stays null instead of reading as zero. */
function nullableNum(value: unknown): number | null {
  return value === null || value === undefined ? null : num(value);
}

/**
 * Every event in the group either wrote no cache or reported a 5m/1h split that covers
 * its whole cache write. Only then do the split totals describe the session.
 */
const cacheSplitComplete = sql`coalesce(bool_and(
  ${convergenceEvents.cacheCreateTokens} = 0
  or coalesce(${convergenceEvents.cacheCreate5mTokens}, 0) + coalesce(${convergenceEvents.cacheCreate1hTokens}, 0) = ${convergenceEvents.cacheCreateTokens}
    and (${convergenceEvents.cacheCreate5mTokens} is not null or ${convergenceEvents.cacheCreate1hTokens} is not null)
), true)`;

function shapeEvent(
  row: Record<string, any>,
  maxContent?: number,
): RecallEvent {
  const rawContent: string | null = row.content ?? null;
  let content = rawContent;
  let contentTruncated = false;
  if (
    maxContent &&
    maxContent > 0 &&
    typeof rawContent === "string" &&
    rawContent.length > maxContent
  ) {
    content = `${rawContent.slice(0, maxContent)}${CONTENT_TRUNCATION_NOTE}`;
    contentTruncated = true;
  }
  return {
    eventId: row.eventId,
    userId: row.userId ?? null,
    sessionId: row.sessionId,
    source: row.source,
    lens: row.lens ?? null,
    kind: row.kind,
    type: row.type,
    ts: toIso(row.ts),
    actorName: row.actorName ?? null,
    actorRole: row.actorRole ?? null,
    subagentId: row.subagentId ?? null,
    machineId: row.machineId,
    projectId: row.projectId ?? null,
    trajectoryId: row.trajectoryId ?? null,
    taskRef: row.taskRef ?? {},
    taskTitle: row.taskTitle ?? null,
    taskDescription: row.taskDescription ?? null,
    taskStatus: row.taskStatus ?? null,
    content,
    contentTruncated,
    significance: row.significance ?? null,
    // Stored as basis points; hand back the 0..1 the client originally sent.
    confidence:
      row.confidence === null || row.confidence === undefined
        ? null
        : num(row.confidence) / 10_000,
    tags: row.tags ?? [],
    model: row.model ?? null,
    provider: row.provider ?? null,
    usage: {
      input: num(row.inputTokens),
      output: num(row.outputTokens),
      reasoning: num(row.reasoningTokens),
      cacheRead: num(row.cacheReadTokens),
      cacheCreate: num(row.cacheCreateTokens),
      cacheCreate5m: nullableNum(row.cacheCreate5mTokens),
      cacheCreate1h: nullableNum(row.cacheCreate1hTokens),
    },
    costUsdMicros: nullableNum(row.costUsdMicros),
    toolName: row.toolName ?? null,
    toolStatus: row.toolStatus ?? null,
    toolCalls: row.toolCalls ?? [],
    filesTouched: row.filesTouched ?? [],
    durationMs: row.durationMs ?? null,
  };
}

/**
 * Build the WHERE clause. `orgId` comes from the verified auth context and is always
 * applied — a filter argument can never widen the scope, only narrow it.
 */
export function buildWhere(
  auth: AuthContext,
  filters: EventFilters,
  scope: RecallScope = {},
) {
  const clauses = [eq(convergenceEvents.orgId, auth.orgId)];

  if (scope.workspaceId) {
    clauses.push(eq(convergenceEvents.workspaceId, scope.workspaceId));
  }

  if (filters.sessionId) {
    clauses.push(eq(convergenceEvents.sessionId, filters.sessionId));
  }
  if (filters.projectId) {
    clauses.push(eq(convergenceEvents.projectId, filters.projectId));
  }
  if (filters.noProject) {
    clauses.push(isNull(convergenceEvents.projectId));
  }
  if (filters.projectContains) {
    clauses.push(
      sql`${convergenceEvents.projectId} ILIKE ${`%${filters.projectContains}%`}`,
    );
  }
  if (filters.source) {
    clauses.push(eq(convergenceEvents.source, filters.source));
  }
  if (filters.kind) {
    clauses.push(eq(convergenceEvents.kind, filters.kind));
  }
  if (filters.taskRef) {
    const taskRefs = Array.isArray(filters.taskRef)
      ? filters.taskRef
      : [filters.taskRef];
    if (taskRefs.length === 1) {
      // `task_ref` is `{system, id}`. Match the id, which is what a PR reference carries,
      // and fall back to a whole-document text match so a differently-shaped ref still
      // finds its events rather than silently returning nothing.
      clauses.push(
        or(
          sql`${convergenceEvents.taskRef}->>'id' = ${taskRefs[0]}`,
          sql`${convergenceEvents.taskRef}::text ILIKE ${`%${taskRefs[0]}%`}`,
        )!,
      );
    } else {
      // Several explicit ids — the epic drill-down case (spec §4): match any of them
      // exactly, so a `sessionCount` computed by grouping on `(project_id, branch)` in
      // `rollups.ts` can be reproduced by one `/v1/sessions` request carrying every
      // contributing `task_ref->>'id'`, not just the newest.
      clauses.push(
        or(
          ...taskRefs.map(
            (ref) => sql`${convergenceEvents.taskRef}->>'id' = ${ref}`,
          ),
        )!,
      );
    }
  }
  if (filters.noTaskRef) {
    clauses.push(sql`${convergenceEvents.taskRef} = '{}'::jsonb`);
  }
  if (filters.tag) {
    clauses.push(sql`${convergenceEvents.tags} ? ${filters.tag}`);
  }
  if (filters.since) {
    clauses.push(gte(convergenceEvents.ts, filters.since));
  }
  if (filters.until) {
    clauses.push(lte(convergenceEvents.ts, filters.until));
  }
  if (filters.q) {
    const needle = `%${filters.q}%`;
    clauses.push(
      or(
        sql`${convergenceEvents.content} ILIKE ${needle}`,
        sql`${convergenceEvents.taskTitle} ILIKE ${needle}`,
      )!,
    );
  }

  return and(...clauses);
}

export interface EventPage {
  events: RecallEvent[];
  nextCursor: string | null;
}

/**
 * One page of events, ordered for replay (oldest first by default).
 *
 * The full import identity breaks timestamp and event-ID ties. Distinct machines
 * and raw kinds can store the same event ID at the same timestamp, including at
 * a page boundary.
 */
export async function queryEvents(
  db: Db,
  auth: AuthContext,
  filters: EventFilters,
  page: PageOptions = {},
  scope: RecallScope = {},
): Promise<EventPage> {
  const limit = clampLimit(page.limit, DEFAULT_EVENT_LIMIT, MAX_EVENT_LIMIT);
  const ascending = (page.order ?? "asc") === "asc";
  const cursor = decodeEventCursor(page.cursor);

  const where = buildWhere(auth, filters, scope);
  const paged = cursor
    ? and(
        where,
        cursor.machineId !== undefined
          ? ascending
            ? sql`(${convergenceEvents.ts}, ${convergenceEvents.eventId}, ${convergenceEvents.machineId}, ${convergenceEvents.source}, ${convergenceEvents.sessionId}, ${convergenceEvents.kind}) > (${cursor.ts}::timestamptz, ${cursor.eventId}, ${cursor.machineId}, ${cursor.source}, ${cursor.sessionId}, ${cursor.kind})`
            : sql`(${convergenceEvents.ts}, ${convergenceEvents.eventId}, ${convergenceEvents.machineId}, ${convergenceEvents.source}, ${convergenceEvents.sessionId}, ${convergenceEvents.kind}) < (${cursor.ts}::timestamptz, ${cursor.eventId}, ${cursor.machineId}, ${cursor.source}, ${cursor.sessionId}, ${cursor.kind})`
          : ascending
            ? sql`(${convergenceEvents.ts}, ${convergenceEvents.eventId}) > (${cursor.ts}::timestamptz, ${cursor.eventId})`
            : sql`(${convergenceEvents.ts}, ${convergenceEvents.eventId}) < (${cursor.ts}::timestamptz, ${cursor.eventId})`,
      )
    : where;

  const rows = await db
    .select({
      ...getTableColumns(convergenceEvents),
      cursorTs: exactTs(sql`${convergenceEvents.ts}`),
    })
    .from(convergenceEvents)
    .where(paged)
    .orderBy(
      ascending ? asc(convergenceEvents.ts) : desc(convergenceEvents.ts),
      ascending
        ? asc(convergenceEvents.eventId)
        : desc(convergenceEvents.eventId),
      ascending
        ? asc(convergenceEvents.machineId)
        : desc(convergenceEvents.machineId),
      ascending
        ? asc(convergenceEvents.source)
        : desc(convergenceEvents.source),
      ascending
        ? asc(convergenceEvents.sessionId)
        : desc(convergenceEvents.sessionId),
      ascending ? asc(convergenceEvents.kind) : desc(convergenceEvents.kind),
    )
    // One extra row tells us whether another page exists without a second COUNT query.
    .limit(limit + 1);

  const hasMore = rows.length > limit;
  const visible = hasMore ? rows.slice(0, limit) : rows;
  const last = visible[visible.length - 1] as Record<string, any> | undefined;

  return {
    events: visible.map((row) =>
      shapeEvent(row as Record<string, any>, page.maxContent),
    ),
    nextCursor:
      hasMore && last
        ? encodeEventCursor(last as Parameters<typeof encodeEventCursor>[0])
        : null,
  };
}

/** The ordered transcript of one session. */
export async function getSessionEvents(
  db: Db,
  auth: AuthContext,
  sessionId: string,
  page: PageOptions = {},
  scope: RecallScope = {},
): Promise<EventPage> {
  return queryEvents(
    db,
    auth,
    { sessionId, source: page.source },
    { ...page, order: page.order ?? "asc" },
    scope,
  );
}

export interface SessionPage {
  sessions: SessionSummary[];
  nextCursor: string | null;
}

/**
 * Sessions rolled up for discovery — "which session should I replay?".
 *
 * Ordered by most recent activity, because the answer is nearly always a recent session.
 * Paged on `(lastTs, sessionId, source)` for the same stability reason as the event cursor.
 */
/** Longest a derived summary may be before it is truncated on a word boundary. */
export const MAX_SESSION_SUMMARY_CHARS = 240;

/**
 * Collapse a raw turn into one readable line.
 *
 * Turn content is whole prompts — newlines, code fences, indentation. Rendered raw into a
 * rollup it is unreadable and can dominate a digest, so it is flattened and bounded.
 * Truncation cuts on a word boundary and marks itself with an ellipsis: a summary that
 * silently stops mid-word reads like the session ended there.
 */
export function condenseTurn(content: string): string | null {
  const flat = content.replace(/\s+/gu, " ").trim();
  if (!flat) return null;
  if (flat.length <= MAX_SESSION_SUMMARY_CHARS) return flat;
  const cut = flat.slice(0, MAX_SESSION_SUMMARY_CHARS);
  const lastSpace = cut.lastIndexOf(" ");
  // Only honour the word boundary if it keeps most of the budget; a very long token
  // (a URL, a base64 blob) would otherwise collapse the summary to almost nothing.
  const body =
    lastSpace > MAX_SESSION_SUMMARY_CHARS * 0.6 ? cut.slice(0, lastSpace) : cut;
  return `${body.trimEnd()}…`;
}

/**
 * Resolve a session's summary and say where it came from.
 *
 * `summarySource` is not decoration. A `task_title` was named by the harness; a
 * `first_user_turn` was inferred by us from an opening prompt that may be an aside. A
 * reader deciding whether to trust a one-line summary needs to know which one it got,
 * and an agent consuming the rollup can weight them differently.
 */
function hasTaskTitle(taskTitle: unknown): taskTitle is string {
  return typeof taskTitle === "string" && taskTitle.trim() !== "";
}

function summaryFor(
  taskTitle: unknown,
  openingTurn: string | undefined,
): { summary: string | null; summarySource: SessionSummary["summarySource"] } {
  if (hasTaskTitle(taskTitle)) {
    return { summary: condenseTurn(taskTitle), summarySource: "task_title" };
  }
  const condensed = openingTurn ? condenseTurn(openingTurn) : null;
  return condensed
    ? { summary: condensed, summarySource: "first_user_turn" }
    : { summary: null, summarySource: null };
}

/**
 * The opening user turn for each of `sessionIds`, in ONE query.
 *
 * Deliberately not a per-session lookup: a 50-session page would become 50 round trips,
 * and the rollup exists to avoid exactly that. `distinct on` takes the lowest
 * `turn_index` per session, which is the session's opening intent.
 *
 * Org-scoped like every other read here — the session ids come from an already
 * org-filtered rollup, and this re-asserts the constraint rather than trusting them.
 */
async function firstUserTurns(
  db: Db,
  orgId: string,
  identities: Array<{ source: string; sessionId: string }>,
  scope: RecallScope = {},
): Promise<Map<string, string>> {
  const found = new Map<string, string>();
  const indices = new Map<string, number>();
  if (!identities.length) return found;
  // Legacy conversation turns have no workspace column. Scoped reads use only
  // the attributed convergence events below, including opening user prompts.
  const rows = scope.workspaceId
    ? []
    : await db
        .selectDistinctOn([conversationTurns.sessionId, conversationSource], {
          sessionId: conversationTurns.sessionId,
          source: conversationSource,
          content: conversationTurns.content,
          turnIndex: conversationTurns.turnIndex,
        })
        .from(conversationTurns)
        .where(
          and(
            eq(conversationTurns.orgId, orgId),
            eq(conversationTurns.role, "user"),
            selectedSessionTurns(orgId, identities),
          ),
        )
        .orderBy(
          conversationTurns.sessionId,
          conversationSource,
          asc(conversationTurns.turnIndex),
        );
  for (const row of rows) {
    if (!row.content?.trim()) continue;
    for (const identity of identities) {
      if (
        identity.sessionId === row.sessionId &&
        (!row.source || row.source === identity.source)
      ) {
        const key = sessionIdentityKey(identity);
        if (!indices.has(key) || row.turnIndex < indices.get(key)!) {
          found.set(key, row.content);
          indices.set(key, row.turnIndex);
        }
      }
    }
  }
  const missing = identities.filter(
    (identity) => !found.has(sessionIdentityKey(identity)),
  );
  if (missing.length) {
    const openings = await db
      .selectDistinctOn(
        [convergenceEvents.source, convergenceEvents.sessionId],
        {
          source: convergenceEvents.source,
          sessionId: convergenceEvents.sessionId,
          content: convergenceEvents.content,
        },
      )
      .from(convergenceEvents)
      .where(
        and(
          eq(convergenceEvents.orgId, orgId),
          scope.workspaceId
            ? eq(convergenceEvents.workspaceId, scope.workspaceId)
            : undefined,
          selectedSessions(missing),
          or(
            inArray(convergenceEvents.kind, ["history", "prompt", "user"]),
            and(
              eq(convergenceEvents.kind, "session_event"),
              eq(convergenceEvents.actorRole, "user"),
            ),
          ),
          sql`length(trim(coalesce(${convergenceEvents.content}, ''))) > 0`,
        ),
      )
      .orderBy(
        convergenceEvents.source,
        convergenceEvents.sessionId,
        asc(convergenceEvents.ts),
        asc(convergenceEvents.eventId),
      );
    for (const row of openings)
      if (row.content) found.set(sessionIdentityKey(row), row.content);
  }
  return found;
}

function optionalCatalog(catalog: SessionCatalogSummary | undefined): {
  catalog?: SessionCatalogSummary;
} {
  return catalog ? { catalog } : {};
}

function decodeSessionCursor(
  cursor: string | null | undefined,
): { ts: string; eventId: string; source?: string } | null {
  if (!cursor?.startsWith("sessions-v2:")) {
    const legacy = decodeCursor(cursor);
    return legacy
      ? { ts: legacy.ts.toISOString(), eventId: legacy.eventId }
      : null;
  }
  try {
    const value = JSON.parse(cursor.slice("sessions-v2:".length));
    const ts = cursorTs(value.ts);
    return ts &&
      typeof value.sessionId === "string" &&
      value.sessionId &&
      typeof value.source === "string" &&
      value.source
      ? { ts, eventId: value.sessionId, source: value.source }
      : null;
  } catch {
    return null;
  }
}

/**
 * Filters the per-session rollups (migration 0030) can answer: tenancy, session,
 * source and project, which are columns of a rollup row. Every other filter selects
 * events, and a session's totals under it come from the matching events alone, so
 * those requests keep the event aggregate.
 */
function rollupsServe(filters: EventFilters): boolean {
  return (
    !filters.kind &&
    !filters.taskRef &&
    !filters.noTaskRef &&
    !filters.tag &&
    !filters.since &&
    !filters.until &&
    !filters.q
  );
}

type SessionCursor = NonNullable<ReturnType<typeof decodeSessionCursor>>;
type SessionRow = Record<string, any>;

function rollupWhere(
  alias: string,
  auth: AuthContext,
  filters: EventFilters,
  scope: RecallScope,
): SQL {
  const t = sql.identifier(alias);
  const clauses = [sql`${t}.org_id = ${auth.orgId}`];
  if (scope.workspaceId)
    clauses.push(sql`${t}.workspace_id = ${scope.workspaceId}`);
  if (filters.sessionId)
    clauses.push(sql`${t}.session_id = ${filters.sessionId}`);
  if (filters.projectId)
    clauses.push(sql`${t}.project_id = ${filters.projectId}`);
  if (filters.noProject) clauses.push(sql`${t}.project_id is null`);
  if (filters.projectContains)
    clauses.push(sql`${t}.project_id ILIKE ${`%${filters.projectContains}%`}`);
  if (filters.source) clauses.push(sql`${t}.source = ${filters.source}`);
  return sql.join(clauses, sql` and `);
}

/**
 * One page of sessions from `session_rollups`, or null until the rollup backfill
 * has completed.
 *
 * A session can have several rollup rows (one per workspace and project). The page
 * is chosen from each session's newest matching row — the row no other matching
 * row of the same session outranks — so a keyset scan of the recency index visits
 * about one row per session it returns. The page's rows are then merged into one
 * summary per session. The readiness check rides in the same statement.
 */
async function sessionRowsFromRollups(
  db: Db,
  auth: AuthContext,
  filters: EventFilters,
  limit: number,
  cursor: SessionCursor | null,
  scope: RecallScope,
): Promise<SessionRow[] | null> {
  const after = !cursor
    ? sql`true`
    : cursor.source
      ? sql`(r.last_ts, r.session_id, r.source) < (${cursor.ts}::timestamptz, ${cursor.eventId}, ${cursor.source})`
      : sql`(r.last_ts, r.session_id) < (${cursor.ts}::timestamptz, ${cursor.eventId})`;
  const result = await db.execute(sql`
    WITH ready AS (
      SELECT completed_at IS NOT NULL AS ok FROM sessions.session_rollup_rollout
    ),
    -- MATERIALIZED: the page is chosen once, then joined, whatever the planner
    -- estimates for the rollups (a freshly backfilled table has no statistics).
    page AS MATERIALIZED (
      SELECT r.session_id, r.source
        FROM sessions.session_rollups AS r
       WHERE (SELECT ok FROM ready)
         AND ${rollupWhere("r", auth, filters, scope)}
         AND ${after}
         AND NOT EXISTS (
           SELECT 1 FROM sessions.session_rollups AS o
            WHERE ${rollupWhere("o", auth, filters, scope)}
              AND o.session_id = r.session_id
              AND o.source = r.source
              AND (o.last_ts, o.workspace_id, o.project_id IS NULL, coalesce(o.project_id, ''))
                > (r.last_ts, r.workspace_id, r.project_id IS NULL, coalesce(r.project_id, '')))
       ORDER BY r.last_ts DESC, r.session_id DESC, r.source DESC
       LIMIT ${limit + 1}
    )
    SELECT ready.ok AS "rollupsReady", s.*
      FROM ready
      LEFT JOIN LATERAL (
        SELECT r.session_id AS "sessionId",
               r.source AS "source",
               sessions.session_rollup_text_union_agg(r.user_ids) AS "userIds",
               array_agg(DISTINCT r.workspace_id) AS "workspaceIds",
               max(r.project_id) AS "projectId",
               sessions.session_rollup_text_union_agg(r.machine_ids) AS "machineIds",
               min(r.first_ts) AS "firstTs",
               max(r.last_ts) AS "lastTs",
               ${exactTs(sql`max(r.last_ts)`)} AS "cursorTs",
               sum(r.event_count) AS "eventCount",
               sessions.session_rollup_text_union_agg(r.kinds) AS "kinds",
               sessions.session_rollup_text_union_agg(r.models) AS "models",
               (array_agg(r.task_title ORDER BY r.task_title_ts DESC, r.task_title DESC)
                  FILTER (WHERE r.task_title IS NOT NULL))[1] AS "taskTitle",
               sessions.session_rollup_jsonb_union_agg(r.task_refs) AS "taskRefs",
               sum(r.cost_usd_micros) AS "totalCostUsdMicros",
               sum(r.input_tokens) AS "totalInputTokens",
               sum(r.output_tokens) AS "totalOutputTokens",
               sum(r.reasoning_tokens) AS "totalReasoningTokens",
               sum(r.cache_read_tokens) AS "totalCacheReadTokens",
               sum(r.cache_create_tokens) AS "totalCacheCreateTokens",
               CASE WHEN sum(r.cache_split_incomplete_events) = 0
                 THEN sum(r.cache_create_5m_tokens) END AS "totalCacheCreate5mTokens",
               CASE WHEN sum(r.cache_split_incomplete_events) = 0
                 THEN sum(r.cache_create_1h_tokens) END AS "totalCacheCreate1hTokens"
          FROM page AS p
          JOIN sessions.session_rollups AS r
            ON r.session_id = p.session_id
           AND r.source = p.source
           AND ${rollupWhere("r", auth, filters, scope)}
         GROUP BY r.session_id, r.source
      ) AS s ON true
     ORDER BY s."lastTs" DESC, s."sessionId" DESC, s."source" DESC
  `);
  const rows = (
    Array.isArray(result) ? result : (result?.rows ?? [])
  ) as SessionRow[];
  if (!rows[0]?.rollupsReady) return null;
  return rows.filter((row) => row.sessionId != null);
}

/**
 * One page of sessions aggregated from `convergence_events` under the full filter set.
 * Its cost grows with the matching events, so it serves only the filters the rollups
 * cannot answer, and every request until the rollup backfill completes.
 */
async function sessionRowsFromEvents(
  db: Db,
  auth: AuthContext,
  filters: EventFilters,
  limit: number,
  cursor: SessionCursor | null,
  scope: RecallScope,
): Promise<SessionRow[]> {
  const where = buildWhere(auth, filters, scope);
  const lastTs = sql<string>`max(${convergenceEvents.ts})`;
  // The cursor filters the aggregate, so it belongs in HAVING, not WHERE — putting it in
  // WHERE would drop matching events from a session's rollup instead of skipping the
  // session, quietly under-reporting counts and costs on every page after the first.
  const pageHaving = cursor
    ? cursor.source
      ? sql`(max(${convergenceEvents.ts}), ${convergenceEvents.sessionId}, ${convergenceEvents.source}) < (${cursor.ts}::timestamptz, ${cursor.eventId}, ${cursor.source})`
      : sql`(max(${convergenceEvents.ts}), ${convergenceEvents.sessionId}) < (${cursor.ts}::timestamptz, ${cursor.eventId})`
    : sql`true`;
  const pageOrder = [
    sql`max(${convergenceEvents.ts}) desc`,
    desc(convergenceEvents.sessionId),
    desc(convergenceEvents.source),
  ];
  // Two phases in one statement. The page is chosen from max(ts) alone, which reads
  // three narrow columns, and the full rollup (array_agg, jsonb_agg, the title sort)
  // is computed only for the sessions on that page. Aggregating every session of the
  // organization and then discarding all but one page cost seconds once an
  // organization held ~1M events. Both phases apply the same WHERE, so each rollup
  // is exactly what the single-pass query produced.
  const sessionPage = db.$with("session_page").as(
    db
      .select({
        sessionId: convergenceEvents.sessionId,
        source: convergenceEvents.source,
      })
      .from(convergenceEvents)
      .where(where)
      .groupBy(convergenceEvents.sessionId, convergenceEvents.source)
      .having(pageHaving)
      .orderBy(...pageOrder)
      .limit(limit + 1),
  );
  return db
    .with(sessionPage)
    .select({
      sessionId: convergenceEvents.sessionId,
      userIds: sql<
        string[]
      >`array_remove(array_agg(distinct ${convergenceEvents.userId}), null)`,
      source: convergenceEvents.source,
      workspaceIds: sql<
        string[]
      >`array_agg(distinct ${convergenceEvents.workspaceId})`,
      projectId: sql<string | null>`max(${convergenceEvents.projectId})`,
      machineIds: sql<
        string[]
      >`array_agg(distinct ${convergenceEvents.machineId})`,
      firstTs: sql<string>`min(${convergenceEvents.ts})`,
      lastTs,
      cursorTs: exactTs(lastTs),
      eventCount: sql<number>`count(*)`,
      kinds: sql<string[]>`array_agg(distinct ${convergenceEvents.kind})`,
      models: sql<
        string[]
      >`array_remove(array_agg(distinct ${convergenceEvents.model}), null)`,
      // Same (ts, title) precedence as the session_rollups aggregate, so a summary does
      // not change when the rollups take over.
      taskTitle: sql<
        string | null
      >`(array_remove(array_agg(${convergenceEvents.taskTitle} order by ${convergenceEvents.ts} desc, ${convergenceEvents.taskTitle} desc), null))[1]`,
      taskRefs: sql<
        unknown[]
      >`coalesce(jsonb_agg(distinct ${convergenceEvents.taskRef}) filter (where ${convergenceEvents.taskRef} <> '{}'::jsonb), '[]'::jsonb)`,
      // sum() skips NULLs and is NULL when every input is: no row carried a cost.
      totalCostUsdMicros: sql<
        number | null
      >`sum(${convergenceEvents.costUsdMicros})`,
      totalInputTokens: sql<number>`coalesce(sum(${convergenceEvents.inputTokens}), 0)`,
      totalOutputTokens: sql<number>`coalesce(sum(${convergenceEvents.outputTokens}), 0)`,
      totalReasoningTokens: sql<number>`coalesce(sum(${convergenceEvents.reasoningTokens}), 0)`,
      totalCacheReadTokens: sql<number>`coalesce(sum(${convergenceEvents.cacheReadTokens}), 0)`,
      totalCacheCreateTokens: sql<number>`coalesce(sum(${convergenceEvents.cacheCreateTokens}), 0)`,
      totalCacheCreate5mTokens: sql<
        number | null
      >`case when ${cacheSplitComplete} then coalesce(sum(${convergenceEvents.cacheCreate5mTokens}), 0) end`,
      totalCacheCreate1hTokens: sql<
        number | null
      >`case when ${cacheSplitComplete} then coalesce(sum(${convergenceEvents.cacheCreate1hTokens}), 0) end`,
    })
    .from(convergenceEvents)
    .where(
      and(
        where,
        sql`(${convergenceEvents.sessionId}, ${convergenceEvents.source}) in (select ${sessionPage.sessionId}, ${sessionPage.source} from ${sessionPage})`,
      ),
    )
    .groupBy(convergenceEvents.sessionId, convergenceEvents.source)
    .orderBy(...pageOrder);
}

/**
 * Sessions for discovery, newest activity first. Reads the per-session rollups when
 * they can answer the filters, and the event aggregate otherwise; both return the
 * same page for the same request.
 */
export async function listSessions(
  db: Db,
  auth: AuthContext,
  filters: EventFilters,
  page: PageOptions = {},
  scope: RecallScope = {},
): Promise<SessionPage> {
  const limit = clampLimit(
    page.limit,
    DEFAULT_SESSION_LIMIT,
    MAX_SESSION_LIMIT,
  );
  const cursor = decodeSessionCursor(page.cursor);
  const rows =
    (rollupsServe(filters)
      ? await sessionRowsFromRollups(db, auth, filters, limit, cursor, scope)
      : null) ??
    (await sessionRowsFromEvents(db, auth, filters, limit, cursor, scope));
  return completeSessionPage(db, auth, rows, limit, scope);
}

/** `listSessions` through the event aggregate alone, whatever the filters. */
export async function listSessionsFromEvents(
  db: Db,
  auth: AuthContext,
  filters: EventFilters,
  page: PageOptions = {},
  scope: RecallScope = {},
): Promise<SessionPage> {
  const limit = clampLimit(
    page.limit,
    DEFAULT_SESSION_LIMIT,
    MAX_SESSION_LIMIT,
  );
  const rows = await sessionRowsFromEvents(
    db,
    auth,
    filters,
    limit,
    decodeSessionCursor(page.cursor),
    scope,
  );
  return completeSessionPage(db, auth, rows, limit, scope);
}

/** Shapes a page of session rows and attaches its summaries, work states and catalog. */
async function completeSessionPage(
  db: Db,
  auth: AuthContext,
  rows: SessionRow[],
  limit: number,
  scope: RecallScope,
): Promise<SessionPage> {
  const hasMore = rows.length > limit;
  const visible = hasMore ? rows.slice(0, limit) : rows;
  const last = visible[visible.length - 1];

  // Only sessions the harness never titled need the turns lookup. On a page where every
  // session has a taskTitle this costs nothing, and fallback lookups stay bounded to the page.
  // The SAME predicate `summaryFor` uses. `prepareConvergenceEvent` stores a
  // whitespace-only taskTitle (it checks truthiness), so a bare `!row.taskTitle`
  // here would call that session titled and skip its turn lookup, while
  // `summaryFor` trims, rejects it, and returns null — a session with a perfectly
  // good opening turn would silently lose the fallback this feature exists for.
  const untitled = visible
    .filter((row) => !hasTaskTitle(row.taskTitle))
    .map((row) => ({
      source: row.source as string,
      sessionId: row.sessionId as string,
    }));
  // The opening turns, work states and catalog summaries are independent reads of
  // the same page, so they share one round trip of latency instead of three.
  const openingTurnsQuery = firstUserTurns(db, auth.orgId, untitled, scope);
  // Discovery filters select sessions; their current state must consider every
  // work event, including newer messages that do not match the search/filter.
  const statesQuery = visible.length
    ? db
        .selectDistinctOn(
          [convergenceEvents.source, convergenceEvents.sessionId],
          {
            source: convergenceEvents.source,
            sessionId: convergenceEvents.sessionId,
            status: sessionWorkStatus,
            updatedAt: convergenceEvents.ts,
          },
        )
        .from(convergenceEvents)
        .where(
          and(
            eq(convergenceEvents.orgId, auth.orgId),
            scope.workspaceId
              ? eq(convergenceEvents.workspaceId, scope.workspaceId)
              : undefined,
            selectedSessions(
              visible.map((row) => ({
                source: row.source as string,
                sessionId: row.sessionId as string,
              })),
            ),
            sql`${sessionWorkStatus} is not null`,
          ),
        )
        .orderBy(
          convergenceEvents.source,
          convergenceEvents.sessionId,
          desc(convergenceEvents.ts),
          desc(convergenceEvents.eventId),
        )
    : Promise.resolve([]);
  const catalogsQuery = catalogSummaries(
    db,
    auth.orgId,
    visible.map((row) => ({
      source: row.source as string,
      sessionId: row.sessionId as string,
      workspaceIds: ((row.workspaceIds ?? []) as unknown[]).filter(
        (id): id is string => typeof id === "string",
      ),
    })),
  );
  const [openingTurns, states, catalogs] = await Promise.all([
    openingTurnsQuery,
    statesQuery,
    catalogsQuery,
  ]);
  const workStates = new Map(
    states.map((row) => [
      sessionIdentityKey(row),
      {
        status: row.status as "active" | "idle" | "finished",
        updatedAt: toIso(row.updatedAt),
      },
    ]),
  );

  return {
    sessions: visible.map((row) => ({
      sessionId: row.sessionId,
      userIds: (row.userIds ?? []).filter(Boolean).sort(),
      source: row.source,
      projectId: row.projectId ?? null,
      machineIds: (row.machineIds ?? []).filter(Boolean),
      firstTs: toIso(row.firstTs),
      lastTs: toIso(row.lastTs),
      workState:
        workStates.get(
          sessionIdentityKey({ source: row.source, sessionId: row.sessionId }),
        ) ?? null,
      eventCount: num(row.eventCount),
      kinds: (row.kinds ?? []).filter(Boolean),
      models: (row.models ?? []).filter(Boolean),
      taskTitle: row.taskTitle ?? null,
      ...summaryFor(
        row.taskTitle,
        openingTurns.get(
          sessionIdentityKey({ source: row.source, sessionId: row.sessionId }),
        ),
      ),
      taskRefs: row.taskRefs ?? [],
      totalCostUsdMicros: nullableNum(row.totalCostUsdMicros),
      totalInputTokens: num(row.totalInputTokens),
      totalOutputTokens: num(row.totalOutputTokens),
      totalReasoningTokens: num(row.totalReasoningTokens),
      totalCacheReadTokens: num(row.totalCacheReadTokens),
      totalCacheCreateTokens: num(row.totalCacheCreateTokens),
      totalCacheCreate5mTokens: nullableNum(row.totalCacheCreate5mTokens),
      totalCacheCreate1hTokens: nullableNum(row.totalCacheCreate1hTokens),
      ...optionalCatalog(
        catalogs.get(
          sessionIdentityKey({ source: row.source, sessionId: row.sessionId }),
        ),
      ),
    })),
    nextCursor:
      hasMore && last
        ? `sessions-v2:${JSON.stringify({ ts: last.cursorTs, source: last.source, sessionId: last.sessionId })}`
        : null,
  };
}

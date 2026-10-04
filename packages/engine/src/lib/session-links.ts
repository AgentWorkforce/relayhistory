import { and, desc, eq, inArray, isNull, lt, or, sql } from "drizzle-orm";
import type { HistoryDb as Db } from "../db/database.js";
import {
  convergenceEvents,
  sessionLinks,
  sessionOutcomes,
} from "../db/schema.js";
import type { AuthContext } from "../env.js";
import { clampLimit } from "./recall.js";
import { scrubJson, scrubText } from "./scrub.js";

export interface SessionLink {
  linkKind: string;
  linkRef: string;
  linkUrl: string | null;
  linkTs: string | null;
  metadata: unknown;
  confidence: number | null;
}

export interface SessionThread {
  session: {
    source: string;
    sessionId: string;
    orgId: string;
    workspaceId: string;
    firstEventAt: string | null;
    lastEventAt: string | null;
  } | null;
  outcomes: Array<{
    commitSha: string;
    shippedAt: string | null;
    reverted: boolean;
    revertedBySha: string | null;
    revertedAt: string | null;
  }>;
  links: SessionLink[];
  nextCursor: string | null;
}

export interface LinkCursor {
  linkTs: string | null;
  id: string;
}

export function encodeLinkCursor(cursor: LinkCursor): string {
  return btoa(JSON.stringify([cursor.linkTs, cursor.id]));
}

/** Preserve the exact Postgres timestamp and BIGSERIAL, without JS numeric rounding. */
export function decodeLinkCursor(raw: string): LinkCursor {
  try {
    if (!raw || raw.length > 512) throw new Error();
    const tuple: unknown = JSON.parse(atob(raw));
    if (!Array.isArray(tuple) || tuple.length !== 2) throw new Error();
    const [linkTs, id] = tuple;
    if (
      linkTs !== null &&
      (typeof linkTs !== "string" ||
        !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z$/.test(linkTs) ||
        !Number.isFinite(Date.parse(linkTs)) ||
        new Date(linkTs).toISOString().slice(0, 19) !== linkTs.slice(0, 19))
    )
      throw new Error();
    if (
      typeof id !== "string" ||
      !/^[1-9]\d{0,18}$/.test(id) ||
      BigInt(id) > 9223372036854775807n
    )
      throw new Error();
    return { linkTs, id };
  } catch {
    throw new Error("cursor must be a base64 (link_ts, id) tuple");
  }
}

/** SQL lookup for future commit-based producers; never resolve another org's commits. */
export async function sessionsForCommit(
  db: Db,
  auth: AuthContext,
  commitSha: string,
) {
  return db
    .select({
      workspaceId: sessionOutcomes.workspaceId,
      source: sessionOutcomes.source,
      sessionId: sessionOutcomes.sessionId,
    })
    .from(sessionOutcomes)
    .where(
      and(
        eq(sessionOutcomes.orgId, auth.orgId),
        eq(sessionOutcomes.commitSha, commitSha),
      ),
    );
}

/** Every workspace's outcomes for (source, sessionId), bounded per session. */
export async function getSessionOutcomes(
  db: Db,
  auth: AuthContext,
  source: string,
  sessionId: string,
) {
  const rows = await db
    .select()
    .from(sessionOutcomes)
    .where(
      and(
        eq(sessionOutcomes.orgId, auth.orgId),
        eq(sessionOutcomes.source, source),
        eq(sessionOutcomes.sessionId, sessionId),
      ),
    )
    .orderBy(sessionOutcomes.commitSha, sessionOutcomes.workspaceId);
  return rows;
}

export async function getSessionThread(
  db: Db,
  auth: AuthContext,
  source: string,
  sessionId: string,
  options: {
    /** Validated ISO timestamp; retain fractional digits through the SQL binding. */
    since?: string;
    kinds?: string[];
    cursor?: LinkCursor;
    limit?: number;
  } = {},
): Promise<SessionThread> {
  const limit = clampLimit(options.limit, 100, 500);
  const scope = and(
    eq(sessionLinks.orgId, auth.orgId),
    eq(sessionLinks.source, source),
    eq(sessionLinks.sessionId, sessionId),
  );
  const clauses = [scope];
  if (options.since)
    clauses.push(sql`${sessionLinks.linkTs} >= ${options.since}::timestamptz`);
  if (options.kinds?.length)
    clauses.push(inArray(sessionLinks.linkKind, options.kinds));
  const cursor = options.cursor;
  // Explicit NULLS LAST, with a separate null partition so undated links are reachable.
  if (cursor)
    clauses.push(
      cursor.linkTs === null
        ? and(
            isNull(sessionLinks.linkTs),
            lt(sessionLinks.id, BigInt(cursor.id)),
          )
        : or(
            sql`${sessionLinks.linkTs} < ${cursor.linkTs}::timestamptz`,
            and(
              sql`${sessionLinks.linkTs} = ${cursor.linkTs}::timestamptz`,
              lt(sessionLinks.id, BigInt(cursor.id)),
            ),
            isNull(sessionLinks.linkTs),
          ),
    );

  const [rows, outcomes, [envelope], [linkAnchor]] = await Promise.all([
    db
      .select({
        id: sessionLinks.id,
        linkKind: sessionLinks.linkKind,
        linkRef: sessionLinks.linkRef,
        linkUrl: sessionLinks.linkUrl,
        linkTs: sql<
          string | null
        >`to_char(${sessionLinks.linkTs} at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')`,
        metadata: sessionLinks.metadata,
        confidence: sessionLinks.confidence,
      })
      .from(sessionLinks)
      .where(and(...clauses))
      .orderBy(
        sql`${sessionLinks.linkTs} desc nulls last`,
        desc(sessionLinks.id),
      )
      .limit(limit + 1),
    getSessionOutcomes(db, auth, source, sessionId),
    db
      .select({
        workspaceId: sql<string>`(array_agg(${convergenceEvents.workspaceId} order by ${convergenceEvents.ts}, ${convergenceEvents.eventId}))[1]`,
        firstEventAt: sql<string>`min(${convergenceEvents.ts})`,
        lastEventAt: sql<string>`max(${convergenceEvents.ts})`,
      })
      .from(convergenceEvents)
      .where(
        and(
          eq(convergenceEvents.orgId, auth.orgId),
          eq(convergenceEvents.source, source),
          eq(convergenceEvents.sessionId, sessionId),
        ),
      )
      .groupBy(
        convergenceEvents.orgId,
        convergenceEvents.source,
        convergenceEvents.sessionId,
      ),
    // Unfiltered anchor keeps the envelope stable across pages, since and kinds filters.
    db
      .select({ workspaceId: sessionLinks.workspaceId })
      .from(sessionLinks)
      .where(scope)
      .orderBy(sessionLinks.id)
      .limit(1),
  ]);
  const visible = rows.slice(0, limit);
  const last = visible.at(-1);
  const workspaceId =
    envelope?.workspaceId ??
    linkAnchor?.workspaceId ??
    outcomes[0]?.workspaceId;
  return {
    session:
      workspaceId === undefined
        ? null
        : {
            source,
            sessionId,
            orgId: auth.orgId,
            workspaceId,
            firstEventAt: envelope
              ? new Date(envelope.firstEventAt).toISOString()
              : null,
            lastEventAt: envelope
              ? new Date(envelope.lastEventAt).toISOString()
              : null,
          },
    // Outcomes are returned whole on every links page, and only those recorded in the
    // envelope's workspace: a session id is unique only within (workspace, source), so
    // another workspace's outcome for the same id belongs to a different session.
    outcomes: outcomes
      .filter((row) => row.workspaceId === workspaceId)
      .map((row) => ({
        commitSha: row.commitSha,
        shippedAt: row.shippedAt?.toISOString() ?? null,
        reverted: row.reverted,
        revertedBySha: row.revertedBySha,
        revertedAt: row.revertedAt?.toISOString() ?? null,
      })),
    links: visible.map(({ id: _id, confidence, ...link }) => ({
      ...link,
      confidence: confidence === null ? null : confidence / 10000,
    })),
    nextCursor:
      rows.length > limit && last
        ? encodeLinkCursor({ linkTs: last.linkTs, id: last.id.toString() })
        : null,
  };
}

/** Explicit server-side writer for producers; callers cannot supply tenant columns. */
export async function upsertSessionLink(
  db: Db,
  auth: AuthContext,
  input: {
    source: string;
    sessionId: string;
    linkKind: string;
    linkRef: string;
    linkUrl?: string | null;
    linkTs?: string | null;
    metadata?: unknown;
    provenanceLens: string;
    confidence?: number | null;
  },
) {
  const fields = {
    workspaceId: auth.workspaceId ?? "default",
    linkUrl: input.linkUrl ? scrubText(input.linkUrl) : null,
    linkTs: input.linkTs ?? null,
    metadata: input.metadata == null ? null : scrubJson(input.metadata),
    provenanceLens: scrubText(input.provenanceLens),
    confidence:
      typeof input.confidence === "number" &&
      Number.isFinite(input.confidence) &&
      input.confidence >= 0 &&
      input.confidence <= 1
        ? Math.round(input.confidence * 10000)
        : null,
    updatedAt: new Date(),
  };
  await db
    .insert(sessionLinks)
    .values({
      orgId: auth.orgId,
      source: input.source,
      sessionId: input.sessionId,
      linkKind: scrubText(input.linkKind),
      linkRef: scrubText(input.linkRef),
      ...fields,
    })
    .onConflictDoUpdate({
      target: [
        sessionLinks.orgId,
        sessionLinks.source,
        sessionLinks.sessionId,
        sessionLinks.linkKind,
        sessionLinks.linkRef,
      ],
      set: {
        ...fields,
        confidence:
          input.confidence === undefined
            ? sql`coalesce(EXCLUDED.confidence_basis_points, ${sessionLinks.confidence})`
            : fields.confidence,
        metadata:
          input.metadata === undefined
            ? sql`coalesce(EXCLUDED.metadata, ${sessionLinks.metadata})`
            : fields.metadata,
        linkUrl:
          input.linkUrl === undefined
            ? sql`coalesce(EXCLUDED.link_url, ${sessionLinks.linkUrl})`
            : fields.linkUrl,
      },
      // Delayed retries must not move an artifact behind an incremental since window.
      setWhere: sql`${sessionLinks.linkTs} is null or EXCLUDED.link_ts >= ${sessionLinks.linkTs}`,
    });
}

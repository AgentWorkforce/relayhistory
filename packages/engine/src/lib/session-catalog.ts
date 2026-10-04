/**
 * Reads over the delivered session catalog (migration 0022): the `session`,
 * `relationship`, `session_marker` and `commit_link` delivery kinds, projected into
 * typed tables by a trigger on `sessions.delivery_records`.
 *
 * Every one of those tables is keyed by (org, workspace, source, …) because a session
 * id is only unique within a workspace and source. Tenancy always comes from the
 * resolved auth context; nothing here accepts an org or workspace from a request.
 */
import { and, asc, eq, inArray, or, sql } from "drizzle-orm";
import type { HistoryDb as Db } from "../db/database.js";
import {
  sessionCatalog,
  sessionCommitLinks,
  sessionMarkers,
  sessionRelationships,
} from "../db/schema.js";
import type { AuthContext } from "../env.js";
import { sessionIdentityKey } from "./session-work-state.js";

/** The catalog fields shared by the session list and the catalog detail read. */
export interface SessionCatalogEntry {
  title: string | null;
  gitBranch: string | null;
  repoUrl: string | null;
  initialCommit: string | null;
  projectKey: string | null;
  projectKeyMethod: string | null;
  models: string[];
  originator: string | null;
  agentVersion: string | null;
  firstActivityAt: string | null;
  lastActivityAt: string | null;
}

/** What `GET /v1/sessions` attaches to a session whose catalog row is unambiguous. */
export interface SessionCatalogSummary extends SessionCatalogEntry {
  workspaceId: string;
  /** Sessions that recorded this one as their child (subagent, continuation, fork…). */
  parentSessionIds: string[];
  /** Relationship rows naming this session as parent, linked or not. */
  childSessionCount: number;
}

export interface SessionRelationshipView {
  relationshipUid: string;
  parentSessionId: string;
  childSessionId: string | null;
  relationship: string | null;
  identityStatus: string | null;
  childAgentType: string | null;
  childAgentName: string | null;
  childModel: string | null;
  spawnDepth: number | null;
  spawnedAt: string | null;
}

export interface SessionMarkerView {
  markerUid: string;
  kind: string | null;
  subkind: string | null;
  ts: string | null;
  turnId: string | null;
}

export interface SessionCommitLinkView {
  commitSha: string;
  matchMethod: string;
  repo: string | null;
  branch: string | null;
  /** 0..1, re-expanded from stored basis points. */
  confidence: number | null;
  linkedAt: string | null;
}

export interface SessionCatalogDetail {
  sessionId: string;
  source: string;
  workspaceId: string;
  catalog: (SessionCatalogEntry & { firstPrompt: string | null }) | null;
  parents: SessionRelationshipView[];
  children: SessionRelationshipView[];
  markers: SessionMarkerView[];
  commitLinks: SessionCommitLinkView[];
  /** True when any list above was cut at `MAX_CATALOG_ITEMS`. */
  truncated: boolean;
}

/** Bound on each list in the detail read; a parent can spawn many subagents. */
export const MAX_CATALOG_ITEMS = 500;

/** The workspace delivery stores records under for this token. */
export function catalogWorkspace(auth: AuthContext): string {
  return auth.workspaceId ?? "";
}

function iso(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  const parsed = value instanceof Date ? value : new Date(String(value));
  return Number.isFinite(parsed.getTime()) ? parsed.toISOString() : null;
}

function models(value: unknown): string[] {
  return Array.isArray(value)
    ? value.filter(
        (item): item is string => typeof item === "string" && item !== "",
      )
    : [];
}

const catalogColumns = {
  workspaceId: sessionCatalog.workspaceId,
  source: sessionCatalog.source,
  sessionId: sessionCatalog.sessionId,
  title: sessionCatalog.title,
  gitBranch: sessionCatalog.gitBranch,
  repoUrl: sessionCatalog.repoUrl,
  initialCommit: sessionCatalog.initialCommit,
  projectKey: sessionCatalog.projectKey,
  projectKeyMethod: sessionCatalog.projectKeyMethod,
  models: sessionCatalog.models,
  originator: sessionCatalog.originator,
  agentVersion: sessionCatalog.agentVersion,
  firstActivityAt: sessionCatalog.firstActivityAt,
  lastActivityAt: sessionCatalog.lastActivityAt,
};

function shapeEntry(row: Record<string, unknown>): SessionCatalogEntry {
  return {
    title: (row.title as string | null) ?? null,
    gitBranch: (row.gitBranch as string | null) ?? null,
    repoUrl: (row.repoUrl as string | null) ?? null,
    initialCommit: (row.initialCommit as string | null) ?? null,
    projectKey: (row.projectKey as string | null) ?? null,
    projectKeyMethod: (row.projectKeyMethod as string | null) ?? null,
    models: models(row.models),
    originator: (row.originator as string | null) ?? null,
    agentVersion: (row.agentVersion as string | null) ?? null,
    firstActivityAt: iso(row.firstActivityAt),
    lastActivityAt: iso(row.lastActivityAt),
  };
}

const key = (workspaceId: string, source: string, sessionId: string) =>
  JSON.stringify([workspaceId, source, sessionId]);

/**
 * Catalog summaries for one page of rolled-up sessions, in three bounded queries.
 *
 * A rolled-up session may span several workspaces when the read is organization-wide,
 * and the catalog is per workspace. A summary is attached only when exactly one catalog
 * row belongs to the workspaces the session's own events came from; an ambiguous
 * session gets none rather than another workspace's branch or repository.
 */
export async function catalogSummaries(
  db: Db,
  orgId: string,
  sessions: Array<{
    source: string;
    sessionId: string;
    workspaceIds: string[];
  }>,
): Promise<Map<string, SessionCatalogSummary>> {
  const found = new Map<string, SessionCatalogSummary>();
  const candidates = sessions.filter((s) => s.workspaceIds.length > 0);
  if (!candidates.length) return found;
  const rows = await db
    .select(catalogColumns)
    .from(sessionCatalog)
    .where(
      and(
        eq(sessionCatalog.orgId, orgId),
        inArray(sessionCatalog.workspaceId, [
          ...new Set(candidates.flatMap((s) => s.workspaceIds)),
        ]),
        or(
          ...candidates.map((s) =>
            and(
              eq(sessionCatalog.source, s.source),
              eq(sessionCatalog.sessionId, s.sessionId),
            ),
          ),
        ),
      ),
    );

  const chosen = new Map<string, (typeof rows)[number]>();
  for (const session of candidates) {
    const matches = rows.filter(
      (row) =>
        row.source === session.source &&
        row.sessionId === session.sessionId &&
        session.workspaceIds.includes(row.workspaceId),
    );
    if (matches.length === 1)
      chosen.set(sessionIdentityKey(session), matches[0]!);
  }
  if (!chosen.size) return found;

  const identities = [...chosen.values()];
  const parents = await db
    .select({
      workspaceId: sessionRelationships.workspaceId,
      source: sessionRelationships.source,
      sessionId: sessionRelationships.childSessionId,
      parentSessionId: sessionRelationships.parentSessionId,
    })
    .from(sessionRelationships)
    .where(
      and(
        eq(sessionRelationships.orgId, orgId),
        or(
          ...identities.map((row) =>
            and(
              eq(sessionRelationships.workspaceId, row.workspaceId),
              eq(sessionRelationships.source, row.source),
              eq(sessionRelationships.childSessionId, row.sessionId),
            ),
          ),
        ),
      ),
    )
    .orderBy(asc(sessionRelationships.parentSessionId));
  const children = await db
    .select({
      workspaceId: sessionRelationships.workspaceId,
      source: sessionRelationships.source,
      sessionId: sessionRelationships.parentSessionId,
      count: sql<number>`count(*)::int`,
    })
    .from(sessionRelationships)
    .where(
      and(
        eq(sessionRelationships.orgId, orgId),
        or(
          ...identities.map((row) =>
            and(
              eq(sessionRelationships.workspaceId, row.workspaceId),
              eq(sessionRelationships.source, row.source),
              eq(sessionRelationships.parentSessionId, row.sessionId),
            ),
          ),
        ),
      ),
    )
    .groupBy(
      sessionRelationships.workspaceId,
      sessionRelationships.source,
      sessionRelationships.parentSessionId,
    );

  const parentIds = new Map<string, Set<string>>();
  for (const row of parents) {
    const k = key(row.workspaceId, row.source, row.sessionId ?? "");
    if (!parentIds.has(k)) parentIds.set(k, new Set());
    parentIds.get(k)!.add(row.parentSessionId);
  }
  const childCounts = new Map(
    children.map((row) => [
      key(row.workspaceId, row.source, row.sessionId),
      Number(row.count),
    ]),
  );
  for (const [identity, row] of chosen) {
    const k = key(row.workspaceId, row.source, row.sessionId);
    found.set(identity, {
      workspaceId: row.workspaceId,
      ...shapeEntry(row),
      parentSessionIds: [...(parentIds.get(k) ?? [])],
      childSessionCount: childCounts.get(k) ?? 0,
    });
  }
  return found;
}

const relationshipColumns = {
  relationshipUid: sessionRelationships.relationshipUid,
  parentSessionId: sessionRelationships.parentSessionId,
  childSessionId: sessionRelationships.childSessionId,
  relationship: sessionRelationships.relationship,
  identityStatus: sessionRelationships.identityStatus,
  childAgentType: sessionRelationships.childAgentType,
  childAgentName: sessionRelationships.childAgentName,
  childModel: sessionRelationships.childModel,
  spawnDepth: sessionRelationships.spawnDepth,
  spawnedAt: sessionRelationships.spawnedAt,
};

function shapeRelationship(
  row: Record<string, unknown>,
): SessionRelationshipView {
  return {
    relationshipUid: row.relationshipUid as string,
    parentSessionId: row.parentSessionId as string,
    childSessionId: (row.childSessionId as string | null) ?? null,
    relationship: (row.relationship as string | null) ?? null,
    identityStatus: (row.identityStatus as string | null) ?? null,
    childAgentType: (row.childAgentType as string | null) ?? null,
    childAgentName: (row.childAgentName as string | null) ?? null,
    childModel: (row.childModel as string | null) ?? null,
    spawnDepth: (row.spawnDepth as number | null) ?? null,
    spawnedAt: iso(row.spawnedAt),
  };
}

/**
 * The delivered catalog for one natural session key in the token's workspace:
 * catalog fields, parents, children, markers and commit links. Null when the
 * workspace has none of them for this session.
 */
export async function getSessionCatalog(
  db: Db,
  auth: AuthContext,
  source: string,
  sessionId: string,
): Promise<SessionCatalogDetail | null> {
  const orgId = auth.orgId;
  const workspaceId = catalogWorkspace(auth);
  const limit = MAX_CATALOG_ITEMS + 1;
  const [catalogRows, parents, children, markers, commitLinks] =
    await Promise.all([
      db
        .select({ ...catalogColumns, firstPrompt: sessionCatalog.firstPrompt })
        .from(sessionCatalog)
        .where(
          and(
            eq(sessionCatalog.orgId, orgId),
            eq(sessionCatalog.workspaceId, workspaceId),
            eq(sessionCatalog.source, source),
            eq(sessionCatalog.sessionId, sessionId),
          ),
        ),
      db
        .select(relationshipColumns)
        .from(sessionRelationships)
        .where(
          and(
            eq(sessionRelationships.orgId, orgId),
            eq(sessionRelationships.workspaceId, workspaceId),
            eq(sessionRelationships.source, source),
            eq(sessionRelationships.childSessionId, sessionId),
          ),
        )
        .orderBy(
          asc(sessionRelationships.parentSessionId),
          asc(sessionRelationships.relationshipUid),
        )
        .limit(limit),
      db
        .select(relationshipColumns)
        .from(sessionRelationships)
        .where(
          and(
            eq(sessionRelationships.orgId, orgId),
            eq(sessionRelationships.workspaceId, workspaceId),
            eq(sessionRelationships.source, source),
            eq(sessionRelationships.parentSessionId, sessionId),
          ),
        )
        .orderBy(
          sql`${sessionRelationships.spawnedAt} asc nulls last`,
          asc(sessionRelationships.relationshipUid),
        )
        .limit(limit),
      db
        .select({
          markerUid: sessionMarkers.markerUid,
          kind: sessionMarkers.markerKind,
          subkind: sessionMarkers.subkind,
          ts: sessionMarkers.ts,
          turnId: sessionMarkers.turnId,
        })
        .from(sessionMarkers)
        .where(
          and(
            eq(sessionMarkers.orgId, orgId),
            eq(sessionMarkers.workspaceId, workspaceId),
            eq(sessionMarkers.source, source),
            eq(sessionMarkers.sessionId, sessionId),
          ),
        )
        .orderBy(
          sql`${sessionMarkers.ts} asc nulls last`,
          asc(sessionMarkers.markerUid),
        )
        .limit(limit),
      db
        .select({
          commitSha: sessionCommitLinks.commitSha,
          matchMethod: sessionCommitLinks.matchMethod,
          repo: sessionCommitLinks.repo,
          branch: sessionCommitLinks.branch,
          confidence: sessionCommitLinks.confidence,
          linkedAt: sessionCommitLinks.linkedAt,
        })
        .from(sessionCommitLinks)
        .where(
          and(
            eq(sessionCommitLinks.orgId, orgId),
            eq(sessionCommitLinks.workspaceId, workspaceId),
            eq(sessionCommitLinks.source, source),
            eq(sessionCommitLinks.sessionId, sessionId),
          ),
        )
        .orderBy(
          sql`${sessionCommitLinks.linkedAt} asc nulls last`,
          asc(sessionCommitLinks.commitSha),
          asc(sessionCommitLinks.matchMethod),
        )
        .limit(limit),
    ]);

  const catalogRow = catalogRows[0];
  if (
    !catalogRow &&
    !parents.length &&
    !children.length &&
    !markers.length &&
    !commitLinks.length
  )
    return null;

  const truncated = [parents, children, markers, commitLinks].some(
    (list) => list.length > MAX_CATALOG_ITEMS,
  );
  const bounded = <T>(list: T[]) => list.slice(0, MAX_CATALOG_ITEMS);
  return {
    sessionId,
    source,
    workspaceId,
    catalog: catalogRow
      ? { ...shapeEntry(catalogRow), firstPrompt: catalogRow.firstPrompt }
      : null,
    parents: bounded(parents).map(shapeRelationship),
    children: bounded(children).map(shapeRelationship),
    markers: bounded(markers).map((row) => ({
      markerUid: row.markerUid,
      kind: row.kind,
      subkind: row.subkind,
      ts: iso(row.ts),
      turnId: row.turnId,
    })),
    commitLinks: bounded(commitLinks).map((row) => ({
      commitSha: row.commitSha,
      matchMethod: row.matchMethod,
      repo: row.repo,
      branch: row.branch,
      confidence: row.confidence === null ? null : row.confidence / 10000,
      linkedAt: iso(row.linkedAt),
    })),
    truncated,
  };
}

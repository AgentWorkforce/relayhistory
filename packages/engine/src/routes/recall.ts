/**
 * The read API over `convergence_events` — how an agent or a person gets context back out.
 *
 *   GET /v1/sessions                      discover sessions (filterable, rolled up)
 *   GET /v1/sessions/:sessionId/events    replay one session in order
 *   GET /v1/events                        search across sessions
 *
 * All three are `rth:read`. Legacy requests are org-scoped. A request that explicitly
 * supplies its authenticated `workspace` is narrowed to that token workspace and receives
 * a response attestation header; the query can never select a different workspace.
 */
import { Hono } from "hono";
import {
  hostContext,
  type HistoryContext,
  type HistoryEngineDeps,
  type HistoryEnv,
} from "../env.js";
import { getAuth, requireScope } from "../middleware/auth.js";
import {
  DEFAULT_EVENT_LIMIT,
  DEFAULT_SESSION_LIMIT,
  MAX_EVENT_LIMIT,
  MAX_SESSION_LIMIT,
  clampLimit,
  getSessionEvents,
  listSessions,
  queryEvents,
  type EventFilters,
} from "../lib/recall.js";

import { decodeLinkCursor, getSessionThread } from "../lib/session-links.js";
import { getSessionCatalog } from "../lib/session-catalog.js";

const MAX_Q_LENGTH = 500;
export { WORKSPACE_RECALL_HEADER } from "../middleware/workspace-recall.js";
import {
  readWorkspaceRecallScope,
  attestWorkspace,
} from "../middleware/workspace-recall.js";

/**
 * Bound on repeated `?taskRef=` values (spec §4's epic drill-down, one per `epicKeys`
 * entry). Without a cap, `or(...)` over an attacker-supplied `taskRef` list becomes an
 * unbounded OR-list the planner has to evaluate per row — a single request could carry
 * thousands of clauses. An epic realistically rolls in a handful of stale ids, so 50 is
 * generous headroom, not a tight fit.
 */
const MAX_TASK_REFS = 50;

function missingDb(c: any) {
  return c.json(
    {
      error: {
        code: "not_configured",
        message: "DATABASE_URL is required for history recall",
      },
      correlationId: c.get("correlationId") ?? "",
    },
    503,
  );
}

function badRequest(c: any, message: string) {
  return c.json(
    {
      error: { code: "bad_request", message },
      correlationId: c.get("correlationId") ?? "",
    },
    400,
  );
}

/**
 * Parse a timestamp filter. An unparseable value is rejected rather than dropped: silently
 * ignoring a bad `since` returns the whole history where the caller asked for a window,
 * which looks like a successful answer to a different question.
 */
function parseDate(
  raw: string | undefined,
  field: string,
): Date | undefined | Error {
  if (!raw) {
    return undefined;
  }
  const parsed = new Date(raw);
  if (!Number.isFinite(parsed.getTime())) {
    return new Error(`${field} must be an ISO timestamp`);
  }
  return parsed;
}

/** Validate thread bounds without converting away Postgres microseconds. */
function parseThreadSince(raw: string | undefined): string | undefined | Error {
  if (!raw) return undefined;
  if (
    !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-](?:0\d|1[0-5]):[0-5]\d)$/.test(
      raw,
    ) ||
    raw.startsWith("0000-") ||
    !Number.isFinite(Date.parse(raw))
  )
    return new Error(
      "since must be an ISO timestamp with a timezone and at most six fractional digits",
    );
  // Check the local calendar components independently of the offset. Date.parse
  // normalizes impossible dates such as February 30, which Postgres rejects.
  const calendar = new Date(`${raw.slice(0, 19)}Z`);
  if (
    !Number.isFinite(calendar.getTime()) ||
    calendar.toISOString().slice(0, 19) !== raw.slice(0, 19)
  ) {
    return new Error("since must be a valid ISO timestamp");
  }
  return raw;
}

function readFilters(c: any): EventFilters | Error {
  const since = parseDate(c.req.query("since"), "since");
  if (since instanceof Error) {
    return since;
  }
  const until = parseDate(c.req.query("until"), "until");
  if (until instanceof Error) {
    return until;
  }
  const q = c.req.query("q");
  if (q && q.length > MAX_Q_LENGTH) {
    return new Error(`q must be ${MAX_Q_LENGTH} characters or fewer`);
  }

  const project = c.req.query("project");
  const noProject = c.req.query("noProject") === "true";
  // Guessing which one wins is exactly how a caller landed on `?project=` (empty string)
  // to mean "the unattributed bucket" in the first place — that collapses to "no filter"
  // and returns every project in the org. Reject the contradiction instead of resolving
  // it silently.
  if (noProject && project) {
    return new Error("project and noProject are mutually exclusive");
  }

  // `c.req.queries` reads every repeat of `?taskRef=`, not just the first (`c.req.query`
  // would silently drop the rest). A single `?taskRef=x` still comes back as `["x"]`, so
  // the existing one-value behavior is unchanged — see `EventFilters.taskRef`'s comment
  // in `lib/recall.ts` for why one id and several are handled differently downstream.
  const taskRefs = (c.req.queries("taskRef") ?? []).filter(Boolean);
  if (taskRefs.length > MAX_TASK_REFS) {
    return new Error(`taskRef accepts at most ${MAX_TASK_REFS} values`);
  }
  const noTaskRef = c.req.query("noTaskRef") === "true";
  if (noTaskRef && taskRefs.length > 0) {
    return new Error("taskRef and noTaskRef are mutually exclusive");
  }

  return {
    projectId: project || undefined,
    noProject,
    projectContains: c.req.query("projectContains") || undefined,
    source: c.req.query("source") || undefined,
    kind: c.req.query("kind") || undefined,
    taskRef:
      taskRefs.length === 0
        ? undefined
        : taskRefs.length === 1
          ? taskRefs[0]
          : taskRefs,
    noTaskRef,
    tag: c.req.query("tag") || undefined,
    since,
    until,
    q: q || undefined,
  };
}

function readMaxContent(c: any): number | undefined {
  const raw = c.req.query("maxContent");
  if (!raw) {
    return undefined;
  }
  const parsed = Number(raw);
  return Number.isFinite(parsed) && parsed > 0 ? Math.trunc(parsed) : undefined;
}

export function createRecallRoutes<E extends HistoryEnv>(
  deps: HistoryEngineDeps<E>,
): Hono<HistoryEnv> {
  const database = (c: HistoryContext) => deps.database(hostContext<E>(c));
  const recallRoutes = new Hono<HistoryEnv>();

  /**
   * `GET /v1/sessions/:sessionId/events` — the full ordered transcript of one session.
   *
   * This is the endpoint the PR workflow points at: a comment carries a session id, and any
   * agent or person can expand it into the work that produced the change. Ascending by
   * default so the page order is the order things happened.
   */
  recallRoutes.get(
    "/sessions/:sessionId/events",
    requireScope("rth:read"),
    async (c) => {
      const scope = readWorkspaceRecallScope(c);
      if (scope.error) return scope.error;
      const db = database(c);
      if (!db) {
        return missingDb(c);
      }
      const sessionId = c.req.param("sessionId");
      if (!sessionId) {
        return badRequest(c, "sessionId is required");
      }
      const order = c.req.query("order") === "desc" ? "desc" : "asc";
      const page = await getSessionEvents(
        db,
        getAuth(c),
        sessionId,
        {
          limit: clampLimit(
            c.req.query("limit"),
            DEFAULT_EVENT_LIMIT,
            MAX_EVENT_LIMIT,
          ),
          cursor: c.req.query("cursor") ?? null,
          order,
          source: c.req.query("source") || undefined,
          maxContent: readMaxContent(c),
        },
        { workspaceId: scope.workspaceId },
      );

      attestWorkspace(c, scope.workspaceId);
      return c.json({
        sessionId,
        events: page.events,
        nextCursor: page.nextCursor,
        correlationId: c.get("correlationId") ?? "",
      });
    },
  );

  /**
   * `GET /v1/sessions` — which sessions exist, newest activity first.
   *
   * Filters compose: `?project=…&taskRef=…&since=…`. `taskRef` may repeat
   * (`?taskRef=a&taskRef=b`, up to `MAX_TASK_REFS`) to match any of several exact
   * `task_ref->>'id'` values in one request — the epic drill-down of spec §4, where an
   * epic's `sessionCount` (grouped on `(project_id, branch)` in `rollups.ts`) can span
   * more than one contributing id and a single-key request would under-report it. Returns
   * a rollup per session (span, event count, models, cost, the latest task title) so a
   * caller can choose what to replay without pulling any transcripts first.
   */
  recallRoutes.get("/sessions", requireScope("rth:read"), async (c) => {
    const scope = readWorkspaceRecallScope(c);
    if (scope.error) return scope.error;
    const db = database(c);
    if (!db) {
      return missingDb(c);
    }
    const filters = readFilters(c);
    if (filters instanceof Error) {
      return badRequest(c, filters.message);
    }
    const page = await listSessions(
      db,
      getAuth(c),
      filters,
      {
        limit: clampLimit(
          c.req.query("limit"),
          DEFAULT_SESSION_LIMIT,
          MAX_SESSION_LIMIT,
        ),
        cursor: c.req.query("cursor") ?? null,
      },
      { workspaceId: scope.workspaceId },
    );

    // Host enrichment is organization-scoped and cannot attest a workspace.
    if (deps.enrichSessions && !scope.workspaceId) {
      try {
        await deps.enrichSessions(
          hostContext<E>(c),
          db,
          getAuth(c),
          page.sessions,
        );
      } catch {
        console.warn("[recall] session enrichment unavailable");
      }
    }

    attestWorkspace(c, scope.workspaceId);
    return c.json({
      sessions: page.sessions,
      nextCursor: page.nextCursor,
      correlationId: c.get("correlationId") ?? "",
    });
  });

  /**
   * `GET /v1/events` — search across sessions.
   *
   * Newest first here, unlike a replay: a search is asking "what happened recently about X",
   * where a transcript is asking "what happened, in order".
   */
  recallRoutes.get("/events", requireScope("rth:read"), async (c) => {
    const scope = readWorkspaceRecallScope(c);
    if (scope.error) return scope.error;
    const db = database(c);
    if (!db) {
      return missingDb(c);
    }
    const filters = readFilters(c);
    if (filters instanceof Error) {
      return badRequest(c, filters.message);
    }
    const sessionId = c.req.query("session");
    const page = await queryEvents(
      db,
      getAuth(c),
      { ...filters, sessionId: sessionId || undefined },
      {
        limit: clampLimit(
          c.req.query("limit"),
          DEFAULT_EVENT_LIMIT,
          MAX_EVENT_LIMIT,
        ),
        cursor: c.req.query("cursor") ?? null,
        order: c.req.query("order") === "asc" ? "asc" : "desc",
        maxContent: readMaxContent(c),
      },
      { workspaceId: scope.workspaceId },
    );

    attestWorkspace(c, scope.workspaceId);
    return c.json({
      events: page.events,
      nextCursor: page.nextCursor,
      correlationId: c.get("correlationId") ?? "",
    });
  });

  /** Lifecycle links for the natural session key (source, sessionId). */
  recallRoutes.get(
    "/sessions/:sessionId/thread",
    requireScope("rth:read"),
    async (c) => {
      const db = database(c);
      if (!db) return missingDb(c);
      const source = c.req.query("source")?.trim();
      if (!source) return badRequest(c, "source is required");
      const since = parseThreadSince(c.req.query("since"));
      if (since instanceof Error) return badRequest(c, since.message);
      const kinds = c.req
        .query("kinds")
        ?.split(",")
        .map((kind) => kind.trim())
        .filter(Boolean);
      if (kinds && kinds.length > 50)
        return badRequest(c, "kinds accepts at most 50 values");
      let cursor;
      try {
        const raw = c.req.query("cursor");
        cursor = raw === undefined ? undefined : decodeLinkCursor(raw);
      } catch (error) {
        return badRequest(c, (error as Error).message);
      }
      const page = await getSessionThread(
        db,
        getAuth(c),
        source,
        c.req.param("sessionId"),
        {
          since,
          kinds,
          cursor,
          limit: clampLimit(c.req.query("limit"), 100, 500),
        },
      );
      return c.json(page);
    },
  );

  /**
   * `GET /v1/sessions/:sessionId/catalog` — the delivered session catalog for one natural
   * session key: branch, repository, project key, models, agent version, parent and
   * child relationships, markers (compactions, resume/fork points) and commit links.
   *
   * Delivered evidence is stored per workspace, so this read is always narrowed to the
   * authenticated token's workspace (the same boundary `/v1/delivery/records` uses) and
   * attests it. No request parameter can select another workspace or organization.
   */
  recallRoutes.get(
    "/sessions/:sessionId/catalog",
    requireScope("rth:read"),
    async (c) => {
      const db = database(c);
      if (!db) return missingDb(c);
      const source = c.req.query("source")?.trim();
      if (!source) return badRequest(c, "source is required");
      const sessionId = c.req.param("sessionId");
      const auth = getAuth(c);
      const detail = await getSessionCatalog(db, auth, source, sessionId);
      if (auth.workspaceId) attestWorkspace(c, auth.workspaceId);
      if (!detail) {
        return c.json(
          {
            error: {
              code: "not_found",
              message: `No delivered catalog for session ${sessionId}`,
            },
            correlationId: c.get("correlationId") ?? "",
          },
          404,
        );
      }
      return c.json({ ...detail, correlationId: c.get("correlationId") ?? "" });
    },
  );

  return recallRoutes;
}

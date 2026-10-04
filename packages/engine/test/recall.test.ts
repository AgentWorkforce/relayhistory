import { Hono } from "hono";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import type { HistoryDb } from "../src/db/database.js";
import type { AuthContext, HistoryEnv } from "../src/env.js";
import {
  decodeCursor,
  encodeCursor,
  getSessionEvents,
  listSessions,
  MAX_SESSION_SUMMARY_CHARS,
  queryEvents,
} from "../src/lib/recall.js";
import { listConversationTurns } from "../src/lib/turns.js";
import { createRecallRoutes } from "../src/routes/recall.js";
import { createTestDatabase, type TestDatabase } from "./support/database.js";

let database: TestDatabase;
let db: any;

/**
 * Recall is the read path that made the convergence store readable at all — before it,
 * `applyIngest` wrote 302,643 events that only direct SQL could reach. These tests hold
 * the three properties that matter for that: the scope cannot be widened by a caller,
 * paging cannot drop or duplicate a row, and a replay comes back in the order it happened.
 */
describe("recall over convergence_events", () => {
  // One migrated database per file; each test starts from empty recall tables. The
  // session rollups are the production triggers' (0030), live because the rollup
  // rollout completes on a database with no events.
  beforeAll(async () => {
    database = await createTestDatabase();
    db = database.db;
  });

  afterAll(async () => {
    await database?.close();
  });

  // Truncating convergence_events empties session_rollups through its trigger.
  beforeEach(async () => {
    await database.exec(
      "TRUNCATE convergence_events, conversation_turns, session_catalog, session_relationships, session_rollup_dirty",
    );
  });

  describe("session replay", () => {
    it("returns one session's events oldest-first", async () => {
      await seed([
        ev({
          sessionId: "s1",
          eventId: "e3",
          ts: "2026-09-01T10:00:03Z",
          content: "third",
        }),
        ev({
          sessionId: "s1",
          eventId: "e1",
          ts: "2026-09-01T10:00:01Z",
          content: "first",
        }),
        ev({
          sessionId: "s1",
          eventId: "e2",
          ts: "2026-09-01T10:00:02Z",
          content: "second",
        }),
        ev({
          sessionId: "s2",
          eventId: "x1",
          ts: "2026-09-01T10:00:04Z",
          content: "other",
        }),
      ]);

      const page = await getSessionEvents(db, auth(), "s1");

      expect(page.events.map((e) => e.content)).toEqual([
        "first",
        "second",
        "third",
      ]);
      expect(page.nextCursor).toBeNull();
    });

    it("can replay newest-first when asked", async () => {
      await seed([
        ev({
          sessionId: "s1",
          eventId: "e1",
          ts: "2026-09-01T10:00:01Z",
          content: "first",
        }),
        ev({
          sessionId: "s1",
          eventId: "e2",
          ts: "2026-09-01T10:00:02Z",
          content: "second",
        }),
      ]);

      const page = await getSessionEvents(db, auth(), "s1", { order: "desc" });

      expect(page.events.map((e) => e.content)).toEqual(["second", "first"]);
    });

    it("returns an empty page, not an error, for a session that does not exist", async () => {
      const page = await getSessionEvents(db, auth(), "nope");
      expect(page.events).toEqual([]);
      expect(page.nextCursor).toBeNull();
    });
  });

  /**
   * The tiebreak on event_id is the whole point. Events in one session routinely share a
   * timestamp — a prompt and the tool calls it triggers land on the same millisecond — so
   * ordering by `ts` alone leaves their order undefined between calls, and a paged replay
   * can drop or repeat them exactly at the page boundary. That loss would be invisible:
   * every page returns 200 with a plausible number of events.
   */
  describe("paging is stable across identical timestamps", () => {
    it("does not skip same-ID imports from different machines or kinds", async () => {
      await seed([
        ev({
          sessionId: "s1",
          eventId: "same",
          machineId: "m_1",
          kind: "prompt",
          content: "first",
        }),
        ev({
          sessionId: "s1",
          eventId: "same",
          machineId: "m_2",
          kind: "prompt",
          content: "second",
        }),
        ev({
          sessionId: "s1",
          eventId: "same",
          machineId: "m_2",
          kind: "response",
          content: "third",
        }),
      ]);
      const seen: string[] = [];
      let cursor: string | null = null;
      for (let index = 0; index < 3; index++) {
        const page = await getSessionEvents(db, auth(), "s1", {
          limit: 1,
          cursor,
        });
        seen.push(page.events[0]!.content!);
        cursor = page.nextCursor;
      }
      expect(seen).toEqual(["first", "second", "third"]);
      expect(cursor).toBeNull();
    });

    it("walks every event exactly once when all timestamps collide", async () => {
      const sameTs = "2026-09-01T10:00:00Z";
      await seed(
        Array.from({ length: 25 }, (_, i) =>
          ev({
            sessionId: "s1",
            eventId: `e${String(i).padStart(2, "0")}`,
            ts: sameTs,
            content: `c${i}`,
          }),
        ),
      );

      const seen: string[] = [];
      let cursor: string | null = null;
      for (let guard = 0; guard < 20; guard += 1) {
        const page: any = await getSessionEvents(db, auth(), "s1", {
          limit: 7,
          cursor,
        });
        seen.push(...page.events.map((e: any) => e.eventId));
        cursor = page.nextCursor;
        if (!cursor) break;
      }

      expect(seen).toHaveLength(25);
      expect(new Set(seen).size).toBe(25);
    });

    it("does not repeat the boundary row across pages", async () => {
      await seed(
        Array.from({ length: 6 }, (_, i) =>
          ev({
            sessionId: "s1",
            eventId: `e${i}`,
            ts: `2026-09-01T10:00:0${i}Z`,
          }),
        ),
      );

      const first = await getSessionEvents(db, auth(), "s1", { limit: 3 });
      const second = await getSessionEvents(db, auth(), "s1", {
        limit: 3,
        cursor: first.nextCursor,
      });

      const overlap = first.events
        .map((e) => e.eventId)
        .filter((id) => second.events.some((e) => e.eventId === id));
      expect(overlap).toEqual([]);
      expect(second.nextCursor).toBeNull();
    });
  });

  /**
   * Tenancy. `orgId` comes from the verified token; a filter must only ever narrow.
   */
  describe("org scoping", () => {
    it("never returns another org's events, even for an exact session id", async () => {
      await seed([
        ev({
          orgId: "org_a",
          sessionId: "shared",
          eventId: "mine",
          content: "mine",
        }),
        ev({
          orgId: "org_b",
          sessionId: "shared",
          eventId: "theirs",
          content: "theirs",
        }),
      ]);

      const page = await getSessionEvents(db, auth("org_a"), "shared");

      expect(page.events).toHaveLength(1);
      expect(page.events[0]?.content).toBe("mine");
      expect(page.events[0]?.userId).toBe("user_1");
    });

    it("scopes the session rollup too", async () => {
      await seed([
        ev({ orgId: "org_a", sessionId: "a1" }),
        ev({ orgId: "org_b", sessionId: "b1" }),
      ]);

      const page = await listSessions(db, auth("org_a"), {});

      expect(page.sessions.map((s) => s.sessionId)).toEqual(["a1"]);
    });

    it("scopes search and replay to the authenticated workspace when requested", async () => {
      await seed([
        ev({
          workspaceId: "ws_1",
          sessionId: "shared",
          eventId: "mine",
          content: "workspace one context",
        }),
        ev({
          workspaceId: "ws_2",
          sessionId: "shared",
          eventId: "theirs",
          content: "workspace two context",
        }),
      ]);

      const searched = await queryEvents(
        db,
        auth(),
        { q: "context" },
        {},
        { workspaceId: "ws_1" },
      );
      const replayed = await getSessionEvents(
        db,
        auth(),
        "shared",
        { source: "claude" },
        { workspaceId: "ws_1" },
      );

      expect(searched.events.map((event) => event.content)).toEqual([
        "workspace one context",
      ]);
      expect(replayed.events.map((event) => event.content)).toEqual([
        "workspace one context",
      ]);
    });

    it("pages only the requested workspace when same-org events interleave", async () => {
      await seed([
        ev({
          workspaceId: "ws_1",
          eventId: "a1",
          ts: "2026-09-01T10:00:01Z",
          content: "mine one",
        }),
        ev({
          workspaceId: "ws_2",
          eventId: "b1",
          ts: "2026-09-01T10:00:02Z",
          content: "theirs one",
        }),
        ev({
          workspaceId: "ws_1",
          eventId: "a2",
          ts: "2026-09-01T10:00:03Z",
          content: "mine two",
        }),
        ev({
          workspaceId: "ws_2",
          eventId: "b2",
          ts: "2026-09-01T10:00:04Z",
          content: "theirs two",
        }),
      ]);

      const seen: string[] = [];
      let cursor: string | null = null;
      for (let pageNumber = 0; pageNumber < 3; pageNumber += 1) {
        const page = await queryEvents(
          db,
          auth(),
          {},
          { limit: 1, cursor },
          { workspaceId: "ws_1" },
        );
        seen.push(...page.events.map((event) => event.content!));
        cursor = page.nextCursor;
        if (!cursor) break;
      }

      expect(seen).toEqual(["mine one", "mine two"]);
      expect(cursor).toBeNull();
    });

    it("scopes counts, opening summaries and latest work state for colliding session IDs", async () => {
      await seed([
        ev({
          workspaceId: "ws_1",
          sessionId: "shared",
          eventId: "ours",
          content: "Our opening",
          ts: "2026-09-01T10:00:00Z",
        }),
        ev({
          workspaceId: "ws_2",
          sessionId: "shared",
          eventId: "theirs",
          content: "Secret opening",
          ts: "2026-09-01T09:00:00Z",
        }),
        ev({
          workspaceId: "ws_2",
          sessionId: "shared",
          eventId: "theirs-done",
          content: "Secret finish",
          ts: "2026-09-01T11:00:00Z",
        }),
        ev({ workspaceId: "ws_2", sessionId: "other", eventId: "other" }),
      ]);
      await database.exec(
        "UPDATE convergence_events SET task_status = 'completed' WHERE event_id = 'theirs-done'",
      );
      await seedTurns([
        {
          sessionId: "shared",
          turnIndex: 0,
          role: "user",
          content: "Unattributed legacy secret",
        },
      ]);
      const page = await listSessions(
        db,
        auth(),
        {},
        {},
        { workspaceId: "ws_1" },
      );
      expect(page.sessions).toHaveLength(1);
      expect(page.sessions[0]).toMatchObject({
        sessionId: "shared",
        eventCount: 1,
        summary: "Our opening",
        lastTs: "2026-09-01T10:00:00.000Z",
      });
      expect(JSON.stringify(page)).not.toContain("Secret");
      expect(JSON.stringify(page)).not.toContain("Unattributed");
      expect(page.sessions[0]?.workState?.status).not.toBe("finished");
    });

    it("includes only distinct owners from the authorized organization", async () => {
      await seed([
        ev({ sessionId: "shared", userId: "user_1" }),
        ev({ sessionId: "shared", userId: "user_1" }),
        ev({ sessionId: "shared", userId: "user_2" }),
        ev({ orgId: "org_b", sessionId: "shared", userId: "other_org" }),
      ]);

      const page = await listSessions(db, auth(), {});

      expect(page.sessions).toHaveLength(1);
      expect(page.sessions[0]?.userIds).toEqual(["user_1", "user_2"]);
    });
  });

  describe("cross-session search", () => {
    it("filters by project", async () => {
      await seed([
        ev({
          sessionId: "s1",
          eventId: "e1",
          projectId: "~/Projects/relayhistory",
        }),
        ev({ sessionId: "s2", eventId: "e2", projectId: "~/Projects/other" }),
      ]);

      const page = await queryEvents(db, auth(), {
        projectId: "~/Projects/relayhistory",
      });

      expect(page.events.map((e) => e.sessionId)).toEqual(["s1"]);
    });

    it("filters by a PR task ref", async () => {
      await seed([
        ev({
          sessionId: "s1",
          eventId: "e1",
          taskRef: {
            system: "github",
            id: "AgentWorkforce/relayhistory-cloud#25",
          },
        }),
        ev({ sessionId: "s2", eventId: "e2", taskRef: {} }),
      ]);

      const page = await queryEvents(db, auth(), {
        taskRef: "AgentWorkforce/relayhistory-cloud#25",
      });

      expect(page.events.map((e) => e.sessionId)).toEqual(["s1"]);
    });

    /**
     * P1 from the day-view review: an epic's `sessionCount` can span more than one
     * `task_ref` id (see `rollups.test.ts`'s "epic drill-down" tests), so
     * `EventFilters.taskRef` accepts several values, matched against `task_ref->>'id'`
     * exactly — the epic drill-down of spec §4, one per `epicKeys` entry.
     */
    it("matches any of several exact taskRef ids in one request", async () => {
      await seed([
        ev({
          sessionId: "s1",
          eventId: "e1",
          taskRef: { system: "git", id: "a@main" },
        }),
        ev({
          sessionId: "s2",
          eventId: "e2",
          taskRef: { system: "git", id: "b@main" },
        }),
        ev({
          sessionId: "s3",
          eventId: "e3",
          taskRef: { system: "git", id: "c@main" },
        }),
      ]);

      const page = await queryEvents(db, auth(), {
        taskRef: ["a@main", "b@main"],
      });

      expect(page.events.map((e) => e.sessionId).sort()).toEqual(["s1", "s2"]);
    });

    /**
     * A single `taskRef` — however it arrives, as a bare string or a one-element array —
     * must keep the original behavior exactly: an exact id match, falling back to a
     * whole-document substring match. Several values drop that fallback (see
     * `EventFilters.taskRef`'s comment), so this exercises the fallback specifically to
     * confirm it did not disappear along with the multi-value change.
     */
    it("keeps the whole-document ILIKE fallback for exactly one taskRef", async () => {
      await seed([
        ev({
          sessionId: "s1",
          eventId: "e1",
          taskRef: {
            system: "github",
            id: "AgentWorkforce/relayhistory-cloud#25",
          },
        }),
        ev({
          sessionId: "s2",
          eventId: "e2",
          taskRef: { system: "git", id: "unrelated@main" },
        }),
      ]);

      // Not the id itself - a substring of the whole task_ref document, which only the
      // ILIKE fallback (not an exact `->>'id'` match) can find.
      const page = await queryEvents(db, auth(), {
        taskRef: "relayhistory-cloud#25",
      });

      expect(page.events.map((e) => e.sessionId)).toEqual(["s1"]);
    });

    it("filters by tag", async () => {
      await seed([
        ev({ sessionId: "s1", eventId: "e1", tags: ["release", "infra"] }),
        ev({ sessionId: "s2", eventId: "e2", tags: ["infra"] }),
        ev({ sessionId: "s3", eventId: "e3", tags: [] }),
      ]);

      const page = await queryEvents(db, auth(), { tag: "release" });

      expect(page.events.map((e) => e.sessionId)).toEqual(["s1"]);
    });

    it("matches content and task title case-insensitively", async () => {
      await seed([
        ev({
          sessionId: "s1",
          eventId: "e1",
          content: "Fix the QUADRATIC scrub",
        }),
        ev({
          sessionId: "s2",
          eventId: "e2",
          taskTitle: "quadratic regression",
        }),
        ev({ sessionId: "s3", eventId: "e3", content: "unrelated" }),
      ]);

      const page = await queryEvents(db, auth(), { q: "quadratic" });

      expect(page.events.map((e) => e.sessionId).sort()).toEqual(["s1", "s2"]);
    });

    it("bounds a time window from both ends", async () => {
      await seed([
        ev({ sessionId: "s1", eventId: "e1", ts: "2026-08-01T00:00:00Z" }),
        ev({ sessionId: "s2", eventId: "e2", ts: "2026-09-01T00:00:00Z" }),
        ev({ sessionId: "s3", eventId: "e3", ts: "2026-10-01T00:00:00Z" }),
      ]);

      const page = await queryEvents(db, auth(), {
        since: new Date("2026-08-15T00:00:00Z"),
        until: new Date("2026-09-15T00:00:00Z"),
      });

      expect(page.events.map((e) => e.sessionId)).toEqual(["s2"]);
    });
  });

  describe("session discovery rollup", () => {
    it("uses the latest work signal, excludes metadata uploads, and reopens finished sessions", async () => {
      await seed([
        ev({
          sessionId: "finished",
          eventId: "done",
          kind: "task",
          ts: "2026-09-01T10:00:00Z",
        }),
        ev({
          sessionId: "finished",
          eventId: "metadata",
          kind: "session",
          ts: "2026-09-19T12:00:00Z",
        }),
        ev({
          sessionId: "resumed",
          eventId: "done-before",
          kind: "task",
          ts: "2026-09-01T10:00:00Z",
        }),
        ev({
          sessionId: "resumed",
          eventId: "new-prompt",
          kind: "history",
          ts: "2026-09-19T12:00:00Z",
        }),
        ev({ sessionId: "metadata-only", kind: "presence" }),
        ev({ sessionId: "paused", eventId: "pause", kind: "task" }),
        ev({
          sessionId: "other-tenant",
          orgId: "org_other",
          eventId: "other-done",
          kind: "task",
        }),
      ]);
      await database.exec(
        "UPDATE convergence_events SET task_status = 'completed' WHERE event_id IN ('done', 'done-before', 'other-done'); UPDATE convergence_events SET task_status = 'waiting' WHERE event_id = 'pause'",
      );
      const sessions = (await listSessions(db, auth(), {})).sessions;
      const state = (id: string) =>
        sessions.find((s) => s.sessionId === id)?.workState;
      expect(state("finished")).toEqual({
        status: "finished",
        updatedAt: "2026-09-01T10:00:00.000Z",
      });
      expect(state("resumed")).toEqual({
        status: "active",
        updatedAt: "2026-09-19T12:00:00.000Z",
      });
      expect(state("metadata-only")).toBeNull();
      expect(state("paused")?.status).toBe("idle");
      expect(state("other-tenant")).toBeUndefined();
    });
    it("keeps the earliest opening when tagged and legacy turns coexist", async () => {
      await seed([ev({ sessionId: "s", source: "claude" })]);
      await seedTurns([
        {
          sessionId: "s",
          turnIndex: 0,
          role: "user",
          content: "Opening intent",
        },
        {
          sessionId: "s",
          turnIndex: 1,
          role: "user",
          content: "Later untagged turn",
        },
      ]);
      await database.exec(
        `UPDATE conversation_turns SET metadata = '{"nativeCli":"claude"}' WHERE turn_index = 0`,
      );
      expect((await listSessions(db, auth(), {})).sessions[0].summary).toBe(
        "Opening intent",
      );
    });

    it("pages same-ID sessions by source without mixing summaries, states or replay", async () => {
      await seed([
        ev({
          sessionId: "shared",
          source: "claude",
          eventId: "claude",
          content: "Claude opening",
        }),
        ev({
          sessionId: "shared",
          source: "codex",
          eventId: "codex",
          content: "Codex opening",
        }),
      ]);
      await database.exec(
        "UPDATE convergence_events SET task_status = 'completed' WHERE source = 'claude'",
      );
      await seedTurns([
        {
          sessionId: "shared",
          turnIndex: 0,
          role: "user",
          content: "Ambiguous legacy turn",
        },
      ]);
      const first = await listSessions(db, auth(), {}, { limit: 1 });
      const second = await listSessions(
        db,
        auth(),
        {},
        { limit: 1, cursor: first.nextCursor },
      );
      expect(
        first.sessions.map((row) => [
          row.source,
          row.summary,
          row.workState?.status,
        ]),
      ).toEqual([["codex", "Codex opening", "active"]]);
      expect(
        second.sessions.map((row) => [
          row.source,
          row.summary,
          row.workState?.status,
        ]),
      ).toEqual([["claude", "Claude opening", "finished"]]);
      expect(second.nextCursor).toBeNull();
      expect(
        (
          await getSessionEvents(db, auth(), "shared", { source: "claude" })
        ).events.map((row) => row.source),
      ).toEqual(["claude"]);
      expect(
        await listConversationTurns(db, "org_a", "shared", "claude"),
      ).toHaveLength(0);
      await database.exec(
        `UPDATE conversation_turns SET metadata = '{"nativeCli":"codex"}'::jsonb`,
      );
      expect(
        await listConversationTurns(db, "org_a", "shared", "claude"),
      ).toHaveLength(0);
      expect(
        await listConversationTurns(db, "org_a", "shared", "codex"),
      ).toHaveLength(1);
      const all = (await listSessions(db, auth(), {})).sessions;
      expect(all.find((row) => row.source === "claude")?.summary).toBe(
        "Claude opening",
      );
      expect(all.find((row) => row.source === "codex")?.summary).toBe(
        "Ambiguous legacy turn",
      );
    });

    it("keeps source-filtered work state within the matching source", async () => {
      await seed([
        ev({
          sessionId: "shared",
          source: "claude",
          eventId: "claude-open",
          kind: "prompt",
          content: "Claude opening",
          ts: "2026-09-01T09:00:00Z",
        }),
        ev({
          sessionId: "shared",
          source: "codex",
          eventId: "codex-open",
          kind: "prompt",
          content: "Codex opening",
          ts: "2026-09-01T08:00:00Z",
        }),
        ev({
          sessionId: "shared",
          source: "claude",
          eventId: "done",
          kind: "task",
          ts: "2026-09-01T10:00:00Z",
        }),
        ev({
          sessionId: "shared",
          source: "codex",
          eventId: "new",
          kind: "prompt",
          ts: "2026-09-01T11:00:00Z",
        }),
      ]);
      await database.exec(
        "UPDATE convergence_events SET task_status = 'completed' WHERE event_id = 'done'",
      );
      expect(
        (await listSessions(db, auth(), { source: "claude" })).sessions[0]
          .summary,
      ).toBe("Claude opening");
      expect(
        (await listSessions(db, auth(), { source: "claude" })).sessions[0]
          .workState?.status,
      ).toBe("finished");
      expect(
        (await listSessions(db, auth(), { source: "codex" })).sessions[0]
          .workState?.status,
      ).toBe("active");
    });

    it("derives state from the full session even when search only matches its completed task", async () => {
      await seed([
        ev({
          sessionId: "s",
          eventId: "done",
          kind: "task",
          content: "needle",
          ts: "2026-09-01T10:00:00Z",
        }),
        ev({
          sessionId: "s",
          eventId: "resume",
          kind: "prompt",
          content: "continue",
          ts: "2026-09-01T11:00:00Z",
        }),
        ev({
          sessionId: "s",
          orgId: "org_other",
          eventId: "foreign-done",
          kind: "task",
          ts: "2026-09-01T12:00:00Z",
        }),
      ]);
      await database.exec(
        "UPDATE convergence_events SET task_status = 'completed' WHERE kind = 'task'",
      );
      const page = await listSessions(db, auth(), { q: "needle" });
      expect(page.sessions).toHaveLength(1);
      expect(page.sessions[0].eventCount).toBe(1);
      expect(page.sessions[0].workState).toEqual({
        status: "active",
        updatedAt: "2026-09-01T11:00:00.000Z",
      });
    });

    it("ignores generic metadata statuses and retains the opening prompt as the title", async () => {
      await seed([
        ev({
          sessionId: "s",
          eventId: "open",
          kind: "history",
          content: "Build the feature",
          ts: "2026-09-01T09:00:00Z",
        }),
        ev({
          sessionId: "s",
          eventId: "later",
          kind: "history",
          content: "try again",
          ts: "2026-09-01T09:30:00Z",
        }),
        ev({
          sessionId: "s",
          eventId: "done",
          kind: "task",
          ts: "2026-09-01T10:00:00Z",
        }),
        ev({
          sessionId: "s",
          eventId: "metadata",
          kind: "session",
          ts: "2026-09-01T11:00:00Z",
        }),
      ]);
      await database.exec(
        "UPDATE convergence_events SET task_status = 'completed' WHERE event_id = 'done'; UPDATE convergence_events SET task_status = 'active' WHERE event_id = 'metadata'",
      );
      const row = (await listSessions(db, auth(), {})).sessions[0];
      expect(row.workState?.status).toBe("finished");
      expect(row.summary).toBe("Build the feature");
      expect(row.summarySource).toBe("first_user_turn");
      expect(row.taskTitle).toBeNull();
    });

    it("summarises a session without returning its transcript", async () => {
      await seed([
        ev({
          sessionId: "s1",
          eventId: "e1",
          ts: "2026-09-01T10:00:00Z",
          projectId: "~/Projects/relayhistory",
          model: "claude-opus-5",
          inputTokens: 100,
          outputTokens: 10,
          costUsdMicros: 500,
        }),
        ev({
          sessionId: "s1",
          eventId: "e2",
          ts: "2026-09-01T11:00:00Z",
          projectId: "~/Projects/relayhistory",
          model: "claude-opus-5",
          taskTitle: "unwedge the fleet",
          inputTokens: 50,
          outputTokens: 5,
          costUsdMicros: 250,
        }),
      ]);

      const page = await listSessions(db, auth(), {});
      const s1 = page.sessions[0]!;

      expect(s1.sessionId).toBe("s1");
      expect(s1.eventCount).toBe(2);
      expect(s1.firstTs).toBe("2026-09-01T10:00:00.000Z");
      expect(s1.lastTs).toBe("2026-09-01T11:00:00.000Z");
      expect(s1.models).toEqual(["claude-opus-5"]);
      expect(s1.taskTitle).toBe("unwedge the fleet");
      expect(s1.totalCostUsdMicros).toBe(750);
      expect(s1.totalInputTokens).toBe(150);
      expect(s1.totalOutputTokens).toBe(15);
    });

    it("totals every token category and the cache-write split when it covers every write", async () => {
      await seed([
        ev({
          sessionId: "s1",
          eventId: "e1",
          ts: "2026-09-01T10:00:00Z",
          reasoningTokens: 7,
          cacheReadTokens: 1000,
          cacheCreateTokens: 300,
          cache_create_5m_tokens: 100,
          cache_create_1h_tokens: 200,
        }),
        ev({
          sessionId: "s1",
          eventId: "e2",
          ts: "2026-09-01T10:01:00Z",
          reasoningTokens: 3,
          cacheReadTokens: 500,
          cacheCreateTokens: 40,
          cache_create_5m_tokens: 40,
          cache_create_1h_tokens: 0,
        }),
        // Wrote no cache: a known zero, so it cannot make the split partial.
        ev({ sessionId: "s1", eventId: "e3", ts: "2026-09-01T10:02:00Z" }),
      ]);

      const s1 = (await listSessions(db, auth(), {})).sessions[0]!;

      expect(s1.totalReasoningTokens).toBe(10);
      expect(s1.totalCacheReadTokens).toBe(1500);
      expect(s1.totalCacheCreateTokens).toBe(340);
      expect(s1.totalCacheCreate5mTokens).toBe(140);
      expect(s1.totalCacheCreate1hTokens).toBe(200);

      const events = (await getSessionEvents(db, auth(), "s1")).events;
      expect(events[0]!.usage).toEqual({
        input: 0,
        output: 0,
        reasoning: 7,
        cacheRead: 1000,
        cacheCreate: 300,
        cacheCreate5m: 100,
        cacheCreate1h: 200,
      });
      expect(events[2]!.usage.cacheCreate5m).toBeNull();
    });

    it("reports no cache-write split when any cache write went unsplit", async () => {
      await seed([
        ev({
          sessionId: "s1",
          eventId: "e1",
          ts: "2026-09-01T10:00:00Z",
          cacheCreateTokens: 300,
          cache_create_5m_tokens: 100,
          cache_create_1h_tokens: 200,
        }),
        // Unsplit write: summing its absent split as zero would misprice 50 tokens.
        ev({
          sessionId: "s1",
          eventId: "e2",
          ts: "2026-09-01T10:01:00Z",
          cacheCreateTokens: 50,
        }),
        // A split that covers only part of the write is partial too.
        ev({
          sessionId: "s2",
          eventId: "e3",
          ts: "2026-09-01T09:00:00Z",
          cacheCreateTokens: 90,
          cache_create_5m_tokens: 30,
          cache_create_1h_tokens: 0,
        }),
      ]);

      const sessions = (await listSessions(db, auth(), {})).sessions;
      const s1 = sessions.find((s) => s.sessionId === "s1")!;
      const s2 = sessions.find((s) => s.sessionId === "s2")!;

      expect(s1.totalCacheCreateTokens).toBe(350);
      expect(s1.totalCacheCreate5mTokens).toBeNull();
      expect(s1.totalCacheCreate1hTokens).toBeNull();
      expect(s2.totalCacheCreateTokens).toBe(90);
      expect(s2.totalCacheCreate5mTokens).toBeNull();
    });

    it("reports cost as null, not zero, when no event carried one", async () => {
      await seed([
        ev({
          sessionId: "nocost",
          eventId: "e1",
          ts: "2026-09-01T10:00:00Z",
          costUsdMicros: null,
        }),
        ev({
          sessionId: "partial",
          eventId: "e2",
          ts: "2026-09-01T09:00:00Z",
          costUsdMicros: null,
        }),
        ev({
          sessionId: "partial",
          eventId: "e3",
          ts: "2026-09-01T09:01:00Z",
          costUsdMicros: 125,
        }),
        ev({
          sessionId: "free",
          eventId: "e4",
          ts: "2026-09-01T08:00:00Z",
          costUsdMicros: 0,
        }),
      ]);

      const sessions = (await listSessions(db, auth(), {})).sessions;
      const bySession = Object.fromEntries(
        sessions.map((s) => [s.sessionId, s.totalCostUsdMicros]),
      );
      expect(bySession).toEqual({ nocost: null, partial: 125, free: 0 });

      const events = (await getSessionEvents(db, auth(), "partial")).events;
      expect(events.map((e) => e.costUsdMicros)).toEqual([null, 125]);
    });

    it("orders sessions by most recent activity", async () => {
      await seed([
        ev({ sessionId: "old", eventId: "e1", ts: "2026-08-01T00:00:00Z" }),
        ev({ sessionId: "new", eventId: "e2", ts: "2026-09-02T00:00:00Z" }),
        ev({ sessionId: "mid", eventId: "e3", ts: "2026-08-20T00:00:00Z" }),
      ]);

      const page = await listSessions(db, auth(), {});

      expect(page.sessions.map((s) => s.sessionId)).toEqual([
        "new",
        "mid",
        "old",
      ]);
    });

    /**
     * The cursor filters an aggregate, so it has to live in HAVING. In WHERE it would drop
     * matching events from a session's rollup rather than skipping the session — every
     * page after the first would silently under-report event counts and costs while still
     * looking like a valid answer.
     */
    it("keeps each session's counts whole when paging", async () => {
      await seed([
        ev({ sessionId: "a", eventId: "a1", ts: "2026-09-03T00:00:00Z" }),
        ev({ sessionId: "a", eventId: "a2", ts: "2026-09-03T00:00:01Z" }),
        ev({ sessionId: "b", eventId: "b1", ts: "2026-09-02T00:00:00Z" }),
        ev({ sessionId: "b", eventId: "b2", ts: "2026-09-02T00:00:01Z" }),
        ev({ sessionId: "c", eventId: "c1", ts: "2026-09-01T00:00:00Z" }),
      ]);

      const first = await listSessions(db, auth(), {}, { limit: 1 });
      expect(first.sessions.map((s) => s.sessionId)).toEqual(["a"]);
      expect(first.sessions[0]?.eventCount).toBe(2);

      const second = await listSessions(
        db,
        auth(),
        {},
        {
          limit: 1,
          cursor: first.nextCursor,
        },
      );
      expect(second.sessions.map((s) => s.sessionId)).toEqual(["b"]);
      // Two events, not one: the cursor must skip the session, not filter its events.
      expect(second.sessions[0]?.eventCount).toBe(2);
    });
  });

  describe("content bounding is opt-in and visible", () => {
    it("returns content whole by default", async () => {
      const long = "x".repeat(5_000);
      await seed([ev({ sessionId: "s1", eventId: "e1", content: long })]);

      const page = await getSessionEvents(db, auth(), "s1");

      expect(page.events[0]?.content).toBe(long);
      expect(page.events[0]?.contentTruncated).toBe(false);
    });

    /**
     * If a caller caps content, the response must say so. A silently shortened transcript
     * is indistinguishable from a short one, which is how a replay stops being a replay
     * without anyone noticing.
     */
    it("flags truncation when the caller caps content", async () => {
      await seed([
        ev({ sessionId: "s1", eventId: "e1", content: "y".repeat(5_000) }),
      ]);

      const page = await getSessionEvents(db, auth(), "s1", {
        maxContent: 100,
      });

      expect(page.events[0]?.contentTruncated).toBe(true);
      expect(page.events[0]?.content).toContain("[truncated by maxContent]");
      expect(page.events[0]?.content!.length).toBeLessThan(200);
    });
  });

  describe("field shaping", () => {
    it("re-expands confidence from basis points to 0..1", async () => {
      await seed([ev({ sessionId: "s1", eventId: "e1", confidence: 8_500 })]);

      const page = await getSessionEvents(db, auth(), "s1");

      expect(page.events[0]?.confidence).toBeCloseTo(0.85, 5);
    });

    it("leaves a missing confidence null rather than defaulting it to zero", async () => {
      await seed([ev({ sessionId: "s1", eventId: "e1", confidence: null })]);

      const page = await getSessionEvents(db, auth(), "s1");

      expect(page.events[0]?.confidence).toBeNull();
    });
  });

  /**
   * Aggregates come back from the driver as local-offset strings while mapped columns come
   * back as Dates. If the rollup does not normalise them, a session's bounds disagree with
   * the timestamps of the events inside it — and both still look like valid timestamps.
   */
  describe("timestamps are normalised to ISO UTC everywhere", () => {
    it("reports session bounds in the same format as the events they bound", async () => {
      await seed([
        ev({ sessionId: "s1", eventId: "e1", ts: "2026-09-01T10:00:00Z" }),
        ev({ sessionId: "s1", eventId: "e2", ts: "2026-09-01T11:00:00Z" }),
      ]);

      const rollup = (await listSessions(db, auth(), {})).sessions[0]!;
      const events = (await getSessionEvents(db, auth(), "s1")).events;

      const iso = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;
      expect(rollup.firstTs).toMatch(iso);
      expect(rollup.lastTs).toMatch(iso);
      expect(events[0]!.ts).toMatch(iso);

      // The bounds must actually bound the events, not sit an offset away from them.
      expect(Date.parse(rollup.firstTs)).toBe(Date.parse(events[0]!.ts));
      expect(Date.parse(rollup.lastTs)).toBe(Date.parse(events[1]!.ts));
    });
  });

  /**
   * Delivery stamps `ts` with `now()`, which carries microseconds. A cursor holding only
   * milliseconds lands between two rows that share a millisecond, so the keyset
   * comparison skips or repeats whichever one sits past the truncated value.
   */
  describe("cursors keep microsecond timestamps", () => {
    async function pageAll<T>(
      fetchPage: (
        cursor: string | null,
      ) => Promise<{ items: T[]; nextCursor: string | null }>,
    ): Promise<T[]> {
      const seen: T[] = [];
      let cursor: string | null = null;
      for (let pageNumber = 0; pageNumber < 5; pageNumber += 1) {
        const page = await fetchPage(cursor);
        seen.push(...page.items);
        cursor = page.nextCursor;
        if (!cursor) break;
      }
      return seen;
    }

    it("pages sessions whose last activity differs only in microseconds", async () => {
      await seed([
        ev({
          sessionId: "a",
          eventId: "a1",
          ts: "2026-09-01T10:00:00.123456Z",
        }),
        ev({
          sessionId: "b",
          eventId: "b1",
          ts: "2026-09-01T10:00:00.123100Z",
        }),
      ]);

      const seen = await pageAll(async (cursor) => {
        const page = await listSessions(db, auth(), {}, { limit: 1, cursor });
        return {
          items: page.sessions.map((s) => s.sessionId),
          nextCursor: page.nextCursor,
        };
      });

      expect(seen).toEqual(["a", "b"]);
    });

    it.each(["asc", "desc"] as const)(
      "pages events that share a millisecond (%s)",
      async (order) => {
        await seed([
          ev({
            sessionId: "s1",
            eventId: "e1",
            ts: "2026-09-01T10:00:00.123100Z",
          }),
          ev({
            sessionId: "s1",
            eventId: "e2",
            ts: "2026-09-01T10:00:00.123456Z",
          }),
        ]);

        const seen = await pageAll(async (cursor) => {
          const page = await queryEvents(
            db,
            auth(),
            { sessionId: "s1" },
            { limit: 1, cursor, order },
          );
          return {
            items: page.events.map((e) => e.eventId),
            nextCursor: page.nextCursor,
          };
        });

        expect(seen).toEqual(order === "asc" ? ["e1", "e2"] : ["e2", "e1"]);
      },
    );

    it("still accepts millisecond cursors issued before microsecond cursors", async () => {
      await seed([
        ev({ sessionId: "a", eventId: "a1", ts: "2026-09-01T10:00:02Z" }),
        ev({ sessionId: "b", eventId: "b1", ts: "2026-09-01T10:00:01Z" }),
      ]);
      const legacySession = `sessions-v2:${JSON.stringify({ ts: "2026-09-01T10:00:02.000Z", source: "claude", sessionId: "a" })}`;
      const legacyEvent = `events-v2:${encodeURIComponent(
        JSON.stringify([
          "2026-09-01T10:00:01.000Z",
          "b1",
          "m_1",
          "claude",
          "b",
          "prompt",
        ]),
      )}`;

      const sessions = await listSessions(
        db,
        auth(),
        {},
        { cursor: legacySession },
      );
      const events = await queryEvents(db, auth(), {}, { cursor: legacyEvent });
      const pipeEvents = await queryEvents(
        db,
        auth(),
        {},
        {
          cursor: encodeCursor(new Date("2026-09-01T10:00:01Z"), "b1"),
        },
      );

      expect(sessions.sessions.map((s) => s.sessionId)).toEqual(["b"]);
      expect(events.events.map((e) => e.eventId)).toEqual(["a1"]);
      expect(pipeEvents.events.map((e) => e.eventId)).toEqual(["a1"]);
    });

    it("rejects a cursor timestamp that is not canonical ISO UTC", async () => {
      const cursor = `sessions-v2:${JSON.stringify({ ts: "Tue Sep 01 2026", source: "claude", sessionId: "a" })}`;
      await seed([ev({ sessionId: "a", eventId: "a1" })]);

      const page = await listSessions(db, auth(), {}, { cursor });

      // An unusable cursor reads from the start rather than reaching ::timestamptz.
      expect(page.sessions.map((s) => s.sessionId)).toEqual(["a"]);
    });

    /**
     * JS rolls an impossible calendar date over into the next month; Postgres rejects it.
     * Such a cursor must read as invalid, not fail the request at ::timestamptz.
     */
    it.each([
      "2026-02-30T10:00:00Z",
      "2026-04-31T00:00:00.123456Z",
      "2026-01-01T24:00:00Z",
      "0000-01-01T00:00:00Z",
    ])("rejects the impossible cursor timestamp %s", async (ts) => {
      await seed([ev({ sessionId: "a", eventId: "a1" })]);
      const sessionCursor = `sessions-v2:${JSON.stringify({ ts, source: "claude", sessionId: "z" })}`;
      const eventCursor = `events-v2:${encodeURIComponent(
        JSON.stringify([ts, "z", "m_1", "claude", "z", "prompt"]),
      )}`;

      const sessions = await listSessions(
        db,
        auth(),
        {},
        {
          cursor: sessionCursor,
        },
      );
      const events = await queryEvents(db, auth(), {}, { cursor: eventCursor });

      expect(sessions.sessions.map((s) => s.sessionId)).toEqual(["a"]);
      expect(events.events.map((e) => e.eventId)).toEqual(["a1"]);
    });
  });

  describe("cursor encoding", () => {
    it("round-trips", () => {
      const encoded = encodeCursor(new Date("2026-09-01T10:00:00Z"), "e1");
      const decoded = decodeCursor(encoded);
      expect(decoded?.eventId).toBe("e1");
      expect(decoded?.ts.toISOString()).toBe("2026-09-01T10:00:00.000Z");
    });

    it("rejects malformed cursors instead of paging from an arbitrary point", () => {
      expect(decodeCursor("garbage")).toBeNull();
      expect(decodeCursor("|e1")).toBeNull();
      expect(decodeCursor("not-a-date|e1")).toBeNull();
      expect(decodeCursor(null)).toBeNull();
      expect(decodeCursor("")).toBeNull();
    });
  });
  /**
   * A rollup of session ids is not readable. `8817afd3 · claude · 1 ev` tells a human
   * nothing about what happened, and the harness only sometimes records a taskTitle —
   * so the sessions with the least metadata were exactly the ones rendering as bare ids.
   */
  describe("session summaries", () => {
    it("prefers the harness's own task title", async () => {
      await seed([
        ev({
          sessionId: "s1",
          eventId: "e1",
          ts: "2026-09-01T10:00:00Z",
          task_title: "Fix the scrub bug",
        }),
      ]);
      await seedTurns([
        {
          sessionId: "s1",
          turnIndex: 0,
          role: "user",
          content: "something else entirely",
        },
      ]);

      const rollup = (await listSessions(db, auth(), {})).sessions[0]!;

      expect(rollup.summary).toBe("Fix the scrub bug");
      expect(rollup.summarySource).toBe("task_title");
    });

    it("falls back to the opening user turn when the harness never titled the session", async () => {
      await seed([
        ev({ sessionId: "s1", eventId: "e1", ts: "2026-09-01T10:00:00Z" }),
      ]);
      await seedTurns([
        {
          sessionId: "s1",
          turnIndex: 2,
          role: "user",
          content: "a later aside",
        },
        {
          sessionId: "s1",
          turnIndex: 0,
          role: "user",
          content: "why did ai-hist push fail?",
        },
        {
          sessionId: "s1",
          turnIndex: 1,
          role: "assistant",
          content: "because the scrub went quadratic",
        },
      ]);

      const rollup = (await listSessions(db, auth(), {})).sessions[0]!;

      // Lowest turn_index, not insertion order, and never an assistant turn.
      expect(rollup.summary).toBe("why did ai-hist push fail?");
      expect(rollup.summarySource).toBe("first_user_turn");
    });

    it("attributes an untagged opening user turn to the source persisted from its request's tag", async () => {
      await seed([
        ev({ sessionId: "shared", source: "claude", eventId: "c1" }),
        ev({ sessionId: "shared", source: "codex", eventId: "x1" }),
      ]);
      // The harness tagged only its opening (assistant) turn; the write path stored
      // the untagged user turn under that request's source.
      await seedTurns([
        {
          sessionId: "shared",
          turnIndex: 0,
          role: "assistant",
          content: "codex greeting",
          source: "codex",
          metadata: { nativeCli: "codex" },
        },
        {
          sessionId: "shared",
          turnIndex: 1,
          role: "user",
          content: "codex opening ask",
          source: "codex",
        },
      ]);

      const sessions = (await listSessions(db, auth(), {})).sessions;
      const codex = sessions.find((row) => row.source === "codex")!;
      const claude = sessions.find((row) => row.source === "claude")!;
      expect(codex.summary).toBe("codex opening ask");
      expect(codex.summarySource).toBe("first_user_turn");
      expect(claude.summary).not.toBe("codex opening ask");
      expect(
        (await listConversationTurns(db, "org_a", "shared", "codex")).map(
          (row) => row.content,
        ),
      ).toEqual(["codex greeting", "codex opening ask"]);
      expect(
        await listConversationTurns(db, "org_a", "shared", "claude"),
      ).toHaveLength(0);
    });

    it("reports null rather than an empty string when there is nothing to summarise", async () => {
      await seed([
        ev({
          sessionId: "s1",
          eventId: "e1",
          content: null,
          ts: "2026-09-01T10:00:00Z",
        }),
      ]);

      const rollup = (await listSessions(db, auth(), {})).sessions[0]!;

      // Distinguishable from "": a blank line is not a summary.
      expect(rollup.summary).toBeNull();
      expect(rollup.summarySource).toBeNull();
    });

    it("treats a whitespace-only task title as untitled", async () => {
      // `prepareConvergenceEvent` stores it (it checks truthiness only), so the
      // rollup must not call such a session titled and skip its turn lookup —
      // `summaryFor` trims and rejects the same value, and the session would end up
      // with no summary despite having a perfectly good opening turn.
      await seed([
        ev({
          sessionId: "s1",
          eventId: "e1",
          ts: "2026-09-01T10:00:00Z",
          task_title: "   ",
        }),
      ]);
      await seedTurns([
        {
          sessionId: "s1",
          turnIndex: 0,
          role: "user",
          content: "the real intent",
        },
      ]);

      const rollup = (await listSessions(db, auth(), {})).sessions[0]!;

      expect(rollup.summary).toBe("the real intent");
      expect(rollup.summarySource).toBe("first_user_turn");
    });

    it("does not read another org's turns", async () => {
      await seed([
        ev({
          sessionId: "s1",
          eventId: "e1",
          content: null,
          ts: "2026-09-01T10:00:00Z",
        }),
      ]);
      await seedTurns([
        {
          sessionId: "s1",
          turnIndex: 0,
          role: "user",
          content: "org b's private prompt",
          orgId: "org_b",
        },
      ]);

      const rollup = (await listSessions(db, auth("org_a"), {})).sessions[0]!;

      expect(rollup.summary).toBeNull();
    });

    it("flattens and bounds a long prompt instead of dumping it into the rollup", async () => {
      await seed([
        ev({ sessionId: "s1", eventId: "e1", ts: "2026-09-01T10:00:00Z" }),
      ]);
      await seedTurns([
        {
          sessionId: "s1",
          turnIndex: 0,
          role: "user",
          content: `line one\n\n   line two indented\n${"padding word ".repeat(60)}`,
        },
      ]);

      const rollup = (await listSessions(db, auth(), {})).sessions[0]!;

      expect(rollup.summary).not.toBeNull();
      expect(rollup.summary!.length).toBeLessThanOrEqual(
        MAX_SESSION_SUMMARY_CHARS + 1,
      );
      expect(rollup.summary).not.toContain("\n");
      expect(rollup.summary!.startsWith("line one line two indented")).toBe(
        true,
      );
      // Truncation announces itself; a summary that stops mid-word reads like the
      // session ended there.
      expect(rollup.summary!.endsWith("…")).toBe(true);
    });

    it("summarises every session on the page, not just the first", async () => {
      await seed([
        ev({ sessionId: "s1", eventId: "e1", ts: "2026-09-01T10:00:01Z" }),
        ev({ sessionId: "s2", eventId: "e2", ts: "2026-09-01T10:00:02Z" }),
      ]);
      await seedTurns([
        {
          sessionId: "s1",
          turnIndex: 0,
          role: "user",
          content: "first session intent",
        },
        {
          sessionId: "s2",
          turnIndex: 0,
          role: "user",
          content: "second session intent",
        },
      ]);

      const page = await listSessions(db, auth(), {});

      expect(
        Object.fromEntries(page.sessions.map((s) => [s.sessionId, s.summary])),
      ).toEqual({
        s1: "first session intent",
        s2: "second session intent",
      });
    });
  });
});

/**
 * Bounding repeated `?taskRef=` values at the route boundary — the same shape of guard
 * `rollup routes reject an ambiguous project filter` in `rollups.test.ts` exercises for
 * `noProject`/`project`. Without a cap, `or(...)` over an attacker-supplied `taskRef`
 * list becomes an unbounded OR-list a caller fully controls the size of.
 */
describe("/v1/sessions rejects too many taskRef values", () => {
  it("400s past the taskRef repeat limit", async () => {
    const app = new Hono<HistoryEnv>();
    app.use("*", async (c, next) => {
      c.set("correlationId", "corr-test");
      c.set("auth", {
        userId: "user-a",
        orgId: "org-a",
        workspaceId: "workspace-a",
        tokenSubject: "user-a",
        scopes: ["rth:read"],
        claims: {},
      });
      await next();
    });
    // Cloud's DATABASE_URL "postgres://example": a configured database never reached.
    app.route("/", createRecallRoutes({ database: () => ({}) as HistoryDb }));

    const tooMany = Array.from({ length: 51 }, (_, i) => `taskRef=id${i}`).join(
      "&",
    );
    const response = await app.request(`/sessions?${tooMany}`);

    expect(response.status).toBe(400);
    await expect(response.json()).resolves.toMatchObject({
      error: { code: "bad_request" },
    });
  });
});

describe("workspace-scoped event route guard", () => {
  function app(workspaceId: string | null = "workspace-a") {
    const result = new Hono<HistoryEnv>();
    result.use("*", async (c, next) => {
      c.set("correlationId", "corr-workspace");
      c.set("auth", {
        userId: "user-a",
        orgId: "org-a",
        workspaceId: workspaceId ?? undefined,
        tokenSubject: "user-a",
        scopes: ["rth:read"],
        claims: {},
      });
      await next();
    });
    // Cloud's empty DATABASE_URL: no database.
    result.route("/", createRecallRoutes({ database: () => undefined }));
    return result;
  }

  it.each(["/events", "/sessions/session-a/events", "/sessions"])(
    "rejects a workspace that differs from the token on %s",
    async (path) => {
      const response = await app().request(`${path}?workspace=workspace-b`);

      expect(response.status).toBe(403);
      expect(response.headers.get("X-Relayhistory-Workspace-Id")).toBeNull();
      await expect(response.json()).resolves.toMatchObject({
        error: { code: "forbidden" },
        correlationId: "corr-workspace",
      });
    },
  );

  it.each(["/events", "/sessions/session-a/events", "/sessions"])(
    "rejects repeated workspace selectors on %s",
    async (path) => {
      const response = await app().request(
        `${path}?workspace=workspace-a&workspace=workspace-a`,
      );

      expect(response.status).toBe(400);
      expect(response.headers.get("X-Relayhistory-Workspace-Id")).toBeNull();
    },
  );

  it.each(["/events", "/sessions/session-a/events", "/sessions"])(
    "rejects workspace mode when the token has no workspace on %s",
    async (path) => {
      const response = await app(null).request(`${path}?workspace=workspace-a`);

      expect(response.status).toBe(403);
      expect(response.headers.get("X-Relayhistory-Workspace-Id")).toBeNull();
    },
  );

  it("does not attest legacy organization-wide requests", async () => {
    const response = await app().request("/events");

    expect(response.status).toBe(503);
    expect(response.headers.get("X-Relayhistory-Workspace-Id")).toBeNull();
  });
});

async function seedTurns(
  rows: Array<{
    sessionId: string;
    turnIndex: number;
    role: string;
    content: string;
    orgId?: string;
    source?: string;
    metadata?: Record<string, unknown>;
  }>,
) {
  for (const row of rows) {
    await database.query(
      `INSERT INTO conversation_turns
         (id, org_id, session_id, session_owner, turn_index, role, content,
          actor_name, actor_role, ts, source, metadata)
       VALUES (gen_random_uuid(),$1,$2,$3,$4,$5,$6,$7,$8,now(),$9,$10)`,
      [
        row.orgId ?? "org_a",
        row.sessionId,
        "user_1",
        row.turnIndex,
        row.role,
        row.content,
        "user_1",
        "owner",
        row.source ?? "",
        JSON.stringify(row.metadata ?? {}),
      ],
    );
  }
}

function auth(orgId = "org_a"): AuthContext {
  return {
    orgId,
    workspaceId: "ws_1",
    userId: "user_1",
    scopes: ["rth:read"],
  } as AuthContext;
}

let seq = 0;

function ev(overrides: Record<string, unknown> = {}) {
  seq += 1;
  return {
    org_id: "org_a",
    workspace_id: "ws_1",
    machine_id: "m_1",
    user_id: "user_1",
    source: "claude",
    lens: "history",
    session_id: `s${seq}`,
    event_id: `e${seq}`,
    kind: "prompt",
    type: "prompt",
    ts: "2026-09-01T10:00:00Z",
    project_id: null,
    task_ref: {},
    task_title: null,
    content: "hello",
    confidence: null,
    tags: [],
    model: null,
    input_tokens: 0,
    output_tokens: 0,
    reasoning_tokens: 0,
    cache_read_tokens: 0,
    cache_create_tokens: 0,
    cache_create_5m_tokens: null,
    cache_create_1h_tokens: null,
    cost_usd_micros: 0,
    record: {},
    ...Object.fromEntries(
      Object.entries(overrides).map(([k, v]) => [
        k.replace(/[A-Z]/g, (m) => `_${m.toLowerCase()}`),
        v,
      ]),
    ),
  };
}

async function seed(rows: Array<Record<string, unknown>>) {
  for (const row of rows) {
    await database.query(
      `INSERT INTO convergence_events
         (org_id, workspace_id, machine_id, user_id, source, lens, session_id, event_id,
          kind, type, ts, project_id, task_ref, task_title, content,
          confidence_basis_points, tags, model, input_tokens, output_tokens,
          cost_usd_micros, record, reasoning_tokens, cache_read_tokens,
          cache_create_tokens, cache_create_5m_tokens, cache_create_1h_tokens)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27)`,
      [
        row.org_id,
        row.workspace_id,
        row.machine_id,
        row.user_id,
        row.source,
        row.lens,
        row.session_id,
        row.event_id,
        row.kind,
        row.type,
        row.ts,
        row.project_id,
        JSON.stringify(row.task_ref ?? {}),
        row.task_title,
        row.content,
        row.confidence,
        JSON.stringify(row.tags ?? []),
        row.model,
        row.input_tokens,
        row.output_tokens,
        row.cost_usd_micros,
        JSON.stringify(row.record ?? {}),
        row.reasoning_tokens,
        row.cache_read_tokens,
        row.cache_create_tokens,
        row.cache_create_5m_tokens,
        row.cache_create_1h_tokens,
      ],
    );
  }
}

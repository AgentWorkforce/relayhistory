import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { applyIngest } from "../src/lib/ingest.js";
import type { AuthContext } from "../src/env.js";
import type { OutcomeEnvelope } from "../src/lib/types.js";
import { createTestDatabase, type TestDatabase } from "./support/database.js";

let database: TestDatabase;

describe("applyIngest convergence row integration", () => {
  beforeEach(async () => {
    database = await createTestDatabase();
  });

  afterEach(async () => {
    await database.close();
  });

  it("persists realistic trajectory decision and retrospective rows", async () => {
    const db = database.db;
    const auth: AuthContext = {
      userId: "user-a",
      orgId: "org-a",
      workspaceId: "workspace-a",
      tokenSubject: "user-a",
      scopes: ["rth:sync"],
      claims: {},
      sessionId: "session-a",
    };
    const body = {
      machine: { id: "machine-a", hostname: "devbox" },
      batchId: "batch-trajectory-a",
      cursors: { trajectories: 7 },
      records: trajectoryRecordsWithSecret(),
    };

    const first = await applyIngest(db, auth, body);
    const second = await applyIngest(db, auth, body);

    expect(first).toMatchObject({ received: 6, accepted: 6 });
    expect(second).toMatchObject({ received: 6, accepted: 6 });

    const events = await database.query<Record<string, unknown>>(
      `SELECT
        org_id,
        workspace_id,
        machine_id,
        user_id,
        source,
        lens,
        session_id,
        kind,
        event_id,
        type,
        trajectory_id,
        project_id,
        actor_name,
        task_title,
        task_description,
        task_status,
        confidence_basis_points,
        content,
        record
      FROM convergence_events
      ORDER BY kind, event_id`,
    );

    expect(events.rows).toHaveLength(6);
    expect(
      events.rows.map((row) => [
        row.event_id,
        row.kind,
        row.confidence_basis_points,
      ]),
    ).toEqual([
      ["decision:traj_abc:0", "decision", 9000],
      ["finding:traj_abc:challenge:0", "finding", null],
      ["finding:traj_abc:learning:0", "finding", null],
      ["reflection:traj_abc:approach", "reflection", 8000],
      ["reflection:traj_abc:suggestion:0", "reflection", null],
      ["reflection:traj_abc:summary", "reflection", 8000],
    ]);

    for (const row of events.rows) {
      expect(row).toMatchObject({
        org_id: "org-a",
        workspace_id: "workspace-a",
        machine_id: "machine-a",
        user_id: "user-a",
        source: "trajectories",
        lens: "trajectories",
        session_id: "traj_abc",
        trajectory_id: "traj_abc",
        project_id: null,
        actor_name: "planner",
        task_title: "Build WS-1 schema",
        task_description: null,
        task_status: null,
      });
      expect(String(row.content)).toMatch(/^Task: Build WS-1 schema/);
    }

    const decision = events.rows.find(
      (row) => row.event_id === "decision:traj_abc:0",
    );
    expect(decision?.content).toContain("Which DB?");
    expect(decision?.content).toContain("Neon");
    expect(decision?.content).toContain("pgvector");
    expect(decision?.content).toContain("D1");
    expect(decision?.content).toContain("Aurora (heavier)");
    expect(decision?.record).toEqual({
      decision: {
        alternatives: ["D1", "Aurora (heavier)"],
        chosen: "Neon",
      },
      task: {
        title: "Build WS-1 schema",
      },
    });

    const learning = events.rows.find(
      (row) => row.event_id === "finding:traj_abc:learning:0",
    );
    expect(learning?.content).toContain("[REDACTED]");
    expect(learning?.content).not.toContain("ghp_");
    expect(learning?.record).toEqual({
      task: {
        title: "Build WS-1 schema",
      },
    });

    const batches = await database.query<Record<string, unknown>>(
      `SELECT id, record_count, accepted_count, cursors_json FROM sync_batches`,
    );
    expect(batches.rows).toHaveLength(1);
    expect(batches.rows[0]).toMatchObject({
      id: "batch-trajectory-a",
      record_count: 6,
      accepted_count: 6,
      cursors_json: { trajectories: 7 },
    });
  });

  it("upserts session outcomes idempotently with auth tenancy and scrubbed readable fields", async () => {
    const db = database.db;
    const auth: AuthContext = {
      userId: "auth-user",
      orgId: "auth-org",
      workspaceId: "auth-workspace",
      tokenSubject: "auth-user",
      scopes: ["rth:sync"],
      claims: {},
      sessionId: "auth-session",
    };

    const firstOutcome = {
      kind: "session_outcome",
      source: "relayhistory",
      lens: "history",
      sessionId: "session-a",
      orgId: "payload-org",
      workspaceId: "payload-workspace",
      userId: "payload-user",
      commit_sha: "abc123",
      repo: "git@github.com:dev@example.com/private",
      branch: "feature/ghp_123456789012345678901234567890123456",
      match_method: "author dev@example.com",
      confidence: 0.875,
      numstat: {
        note: "token ghp_123456789012345678901234567890123456",
      },
      files: [
        {
          path: "/Users/khaliqgant/project/src/index.ts",
          owner: "dev@example.com",
        },
      ],
      shipped_at: "2026-06-21T12:00:00.000Z",
      reverted: false,
    } satisfies OutcomeEnvelope & {
      orgId: string;
      workspaceId: string;
      userId: string;
    };

    const first = await applyIngest(db, auth, {
      machine: { id: "machine-a" },
      batchId: "batch-outcome-a",
      records: [firstOutcome],
    });
    const second = await applyIngest(db, auth, {
      machine: { id: "machine-b" },
      batchId: "batch-outcome-b",
      records: [
        {
          kind: "session_outcome",
          source: "relayhistory",
          sessionId: "session-a",
          commitSha: "abc123",
          repo: "git@github.com:dev@example.com/private",
          branch: "main ghp_123456789012345678901234567890123456",
          matchMethod: "author dev@example.com",
          // Same method and confidence: a replay, so the newest write wins. A weaker
          // method is covered by the precedence test below.
          confidence: 0.875,
          numstatJson: {
            note: "token ghp_123456789012345678901234567890123456",
          },
          filesJson: [
            {
              path: "/Users/khaliqgant/project/src/index.ts",
              owner: "dev@example.com",
            },
          ],
          shippedAt: "2026-06-22T12:00:00.000Z",
          reverted: true,
          revertedBySha: "def456",
          revertedAt: "2026-06-23T12:00:00.000Z",
        },
      ],
    });

    expect(first).toMatchObject({ received: 1, accepted: 1 });
    expect(second).toMatchObject({ received: 1, accepted: 1 });

    const outcomes = await database.query<Record<string, unknown>>(
      `SELECT
        org_id,
        workspace_id,
        machine_id,
        user_id,
        source,
        session_id,
        repo,
        branch,
        commit_sha,
        match_method,
        confidence_basis_points,
        numstat_json,
        files_json,
        shipped_at,
        reverted,
        reverted_by_sha,
        reverted_at
      FROM session_outcomes`,
    );

    expect(outcomes.rows).toHaveLength(1);
    expect(outcomes.rows[0]).toMatchObject({
      org_id: "auth-org",
      workspace_id: "auth-workspace",
      machine_id: "machine-b",
      user_id: "auth-user",
      source: "relayhistory",
      session_id: "session-a",
      // The scp-style remote keeps its host; only the address in the path is redacted.
      repo: "git@github.com:[REDACTED]/private",
      branch: "main [REDACTED]",
      commit_sha: "abc123",
      match_method: "author [REDACTED]",
      confidence_basis_points: 8750,
      numstat_json: { note: "token [REDACTED]" },
      files_json: [
        {
          path: "~/project/src/index.ts",
          owner: "[REDACTED]",
        },
      ],
      reverted: true,
      reverted_by_sha: "def456",
    });
    expect(
      new Date(outcomes.rows[0].shipped_at as any).toISOString(),
    ).toContain("2026-06-22");
    expect(
      new Date(outcomes.rows[0].reverted_at as any).toISOString(),
    ).toContain("2026-06-23");
  });

  it("keeps an outcome's strongest match method and its project and touched files", async () => {
    const db = database.db;
    const auth: AuthContext = {
      userId: "auth-user",
      orgId: "auth-org",
      workspaceId: "auth-workspace",
      tokenSubject: "auth-user",
      scopes: ["rth:sync"],
      claims: {},
      sessionId: "auth-session",
    };
    const outcome = (overrides: Record<string, unknown>) =>
      ({
        kind: "session_outcome",
        source: "claude",
        sessionId: "synthetic-outcome-precedence",
        commitSha: "f00d",
        ...overrides,
      }) as OutcomeEnvelope;
    const push = (batchId: string, record: OutcomeEnvelope) =>
      applyIngest(db, auth, {
        machine: { id: "machine-a" },
        batchId,
        records: [record],
      });
    const row = async () =>
      (
        await database.query<Record<string, unknown>>(
          `SELECT match_method, confidence_basis_points, project_id, files_touched,
                  reverted
             FROM session_outcomes
            WHERE session_id = 'synthetic-outcome-precedence'`,
        )
      ).rows;

    await push(
      "b1",
      outcome({
        matchMethod: "cwd+branch",
        confidence: 0.9,
        projectId: "AgentWorkforce/relayhistory",
        filesTouched: ["src/a.ts", "/Users/khaliqgant/project/src/b.ts"],
      }),
    );
    // Weaker method for the same commit: must not demote the stored link, and an
    // envelope without project/files must not erase them. Commit facts still update.
    await push(
      "b2",
      outcome({ matchMethod: "time-window", confidence: 0.4, reverted: true }),
    );
    await push("b3", outcome({ matchMethod: "unscored", reverted: true }));

    expect(await row()).toEqual([
      {
        match_method: "cwd+branch",
        confidence_basis_points: 9000,
        project_id: "AgentWorkforce/relayhistory",
        files_touched: ["src/a.ts", "~/project/src/b.ts"],
        reverted: true,
      },
    ]);

    // A stronger method replaces it; so does an equal-confidence replay.
    await push(
      "b4",
      outcome({ matchMethod: "explicit_session", confidence: 1 }),
    );
    expect((await row())[0]).toMatchObject({
      match_method: "explicit_session",
      confidence_basis_points: 10000,
      project_id: "AgentWorkforce/relayhistory",
    });
    await push("b5", outcome({ matchMethod: "manual", confidence: 1 }));
    expect((await row())[0]).toMatchObject({
      match_method: "manual",
      confidence_basis_points: 10000,
    });

    // An explicit null / [] is a correction and clears the stored project and files.
    await push(
      "b6",
      outcome({
        matchMethod: "manual",
        confidence: 1,
        projectId: null,
        filesTouched: [],
      }),
    );
    expect((await row())[0]).toMatchObject({
      project_id: null,
      files_touched: [],
    });

    // The project id is scrubbed like every other readable outcome field.
    await push(
      "b7",
      outcome({
        matchMethod: "manual",
        confidence: 1,
        projectId: "https://user:hunter2secret@example.com/o/r",
      }),
    );
    expect((await row())[0]).toMatchObject({
      project_id: "https://user:[REDACTED]@example.com/o/r",
    });
  });

  it("keeps the cache-write TTL split and leaves unreported cost null", async () => {
    const db = database.db;
    const auth: AuthContext = {
      userId: "auth-user",
      orgId: "auth-org",
      workspaceId: "auth-workspace",
      tokenSubject: "auth-user",
      scopes: ["rth:sync"],
      claims: {},
      sessionId: "auth-session",
    };
    const base = {
      v: 1,
      kind: "usage",
      source: "claude",
      lens: "burn",
      sessionId: "synthetic-cache-split",
      ts: "2026-09-01T10:00:00Z",
    };
    await applyIngest(db, auth, {
      machine: { id: "machine-a" },
      batchId: "batch-cache-split",
      records: [
        {
          ...base,
          eventId: "split",
          usage: { input: 10, cacheCreate5m: 100, cacheCreate1h: 200 },
          costUsdMicros: 42,
        },
        { ...base, eventId: "one-bucket", usage: { cacheCreate1h: 30 } },
        { ...base, eventId: "unsplit", usage: { cacheCreate: 50 } },
      ],
    });

    const rows = (
      await database.query<Record<string, unknown>>(
        `SELECT event_id, cache_create_tokens::int AS total,
                cache_create_5m_tokens::int AS five_minute,
                cache_create_1h_tokens::int AS one_hour,
                cost_usd_micros::int AS cost
           FROM convergence_events
          WHERE session_id = 'synthetic-cache-split'
          ORDER BY event_id`,
      )
    ).rows;
    expect(rows).toEqual([
      {
        event_id: "usage:one-bucket",
        total: 30,
        five_minute: 0,
        one_hour: 30,
        cost: null,
      },
      {
        event_id: "usage:split",
        total: 300,
        five_minute: 100,
        one_hour: 200,
        cost: 42,
      },
      {
        event_id: "usage:unsplit",
        total: 50,
        five_minute: null,
        one_hour: null,
        cost: null,
      },
    ]);
  });
});

function trajectoryRecordsWithSecret() {
  return SIX_TRAJECTORY_ENVELOPES.map((record) =>
    record.eventId === "finding:traj_abc:learning:0"
      ? {
          ...record,
          content:
            "kind in PK and secret ghp_123456789012345678901234567890123456",
        }
      : record,
  );
}

const SIX_TRAJECTORY_ENVELOPES = [
  {
    v: 1,
    kind: "decision",
    source: "trajectories",
    lens: "trajectories",
    sessionId: "traj_abc",
    eventId: "decision:traj_abc:0",
    ts: "2026-06-21T10:00:00.000Z",
    type: "decision",
    content:
      "Question: Which DB?\nChose: Neon\nBecause: pgvector\nAlternatives: D1; Aurora (heavier)",
    confidence: 0.9,
    actorName: "planner",
    taskTitle: "Build WS-1 schema",
    record: {
      alternatives: ["D1", "Aurora (heavier)"],
      chosen: "Neon",
    },
  },
  {
    v: 1,
    kind: "reflection",
    source: "trajectories",
    lens: "trajectories",
    sessionId: "traj_abc",
    eventId: "reflection:traj_abc:summary",
    ts: "2026-06-21T10:00:00.000Z",
    type: "reflection",
    content: "Shipped schema",
    confidence: 0.8,
    actorName: "planner",
    taskTitle: "Build WS-1 schema",
  },
  {
    v: 1,
    kind: "reflection",
    source: "trajectories",
    lens: "trajectories",
    sessionId: "traj_abc",
    eventId: "reflection:traj_abc:approach",
    ts: "2026-06-21T10:00:00.000Z",
    type: "reflection",
    content: "TDD",
    confidence: 0.8,
    actorName: "planner",
    taskTitle: "Build WS-1 schema",
  },
  {
    v: 1,
    kind: "finding",
    source: "trajectories",
    lens: "trajectories",
    sessionId: "traj_abc",
    eventId: "finding:traj_abc:learning:0",
    ts: "2026-06-21T10:00:00.000Z",
    type: "finding",
    content: "kind in PK",
    confidence: null,
    actorName: "planner",
    taskTitle: "Build WS-1 schema",
  },
  {
    v: 1,
    kind: "reflection",
    source: "trajectories",
    lens: "trajectories",
    sessionId: "traj_abc",
    eventId: "reflection:traj_abc:suggestion:0",
    ts: "2026-06-21T10:00:00.000Z",
    type: "reflection",
    content: "scrub paths",
    confidence: null,
    actorName: "planner",
    taskTitle: "Build WS-1 schema",
  },
  {
    v: 1,
    kind: "finding",
    source: "trajectories",
    lens: "trajectories",
    sessionId: "traj_abc",
    eventId: "finding:traj_abc:challenge:0",
    ts: "2026-06-21T10:00:00.000Z",
    type: "finding",
    content: "union parsing",
    confidence: null,
    actorName: "planner",
    taskTitle: "Build WS-1 schema",
  },
] as const;

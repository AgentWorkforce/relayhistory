import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { HistoryDb } from "../src/db/database.js";
import * as schema from "../src/db/schema.js";
import { createHistoryEngine } from "../src/engine.js";
import { createSession } from "../src/auth/tokens.js";
import { createTestDatabase, type TestDatabase } from "./support/database.js";

const env = { DATABASE_URL: "postgres://pglite", ENVIRONMENT: "test" } as any;
const app = createHistoryEngine({ database: () => db });
const commitSha = "c".repeat(40);
let database: TestDatabase;
let db: HistoryDb;

async function tokenFor(workspaceId: string, scopes: string[]) {
  const session = await createSession(
    db,
    { userId: "user-a", orgId: "org-a", workspaceId },
    { scopes },
  );
  return session.accessToken;
}

async function ingest(workspaceId: string, reverted: boolean) {
  const response = await app.request(
    "/v1/ingest",
    {
      method: "POST",
      headers: {
        Authorization: `Bearer ${await tokenFor(workspaceId, ["rth:sync"])}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        machine: { id: `machine-${workspaceId}` },
        batchId: crypto.randomUUID(),
        records: [
          {
            v: 1,
            kind: "event",
            source: "claude",
            sessionId: "shared-session",
            eventId: `event-${workspaceId}`,
            lens: "history",
            ts: "2026-09-05T10:00:00Z",
            content: "Synthetic workspace outcome test",
          },
          {
            kind: "session_outcome",
            source: "claude",
            sessionId: "shared-session",
            commitSha,
            reverted,
          },
        ],
      }),
    },
    env,
  );
  expect(response.status).toBe(200);
  expect(await response.json()).toMatchObject({ accepted: 2 });
}

describe("session outcomes keyed per workspace", () => {
  beforeAll(async () => {
    // Every migration in order, so the table is built exactly as production is and
    // 0018 rekeys a table created with the original four-column key.
    database = await createTestDatabase();
    db = database.db;
  }, 60_000);

  afterAll(async () => {
    await database.close();
  });

  it("keeps each workspace's classification for the same source, session and commit", async () => {
    await ingest("workspace-a", true);
    await ingest("workspace-b", false);

    const rows = await db
      .select({
        workspaceId: schema.sessionOutcomes.workspaceId,
        reverted: schema.sessionOutcomes.reverted,
      })
      .from(schema.sessionOutcomes)
      .orderBy(schema.sessionOutcomes.workspaceId);
    expect(rows).toEqual([
      { workspaceId: "workspace-a", reverted: true },
      { workspaceId: "workspace-b", reverted: false },
    ]);
  });

  it("persists the envelope's project, touched files and strongest match method through the migrated table", async () => {
    const push = async (record: Record<string, unknown>) => {
      const response = await app.request(
        "/v1/ingest",
        {
          method: "POST",
          headers: {
            Authorization: `Bearer ${await tokenFor("workspace-p", ["rth:sync"])}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            machine: { id: "machine-p" },
            batchId: crypto.randomUUID(),
            records: [
              {
                kind: "session_outcome",
                source: "claude",
                sessionId: "synthetic-project-outcome",
                commitSha,
                ...record,
              },
            ],
          }),
        },
        env,
      );
      expect(response.status).toBe(200);
    };

    await push({
      matchMethod: "explicit_session",
      confidence: 1,
      projectId: "AgentWorkforce/relayhistory",
      filesTouched: ["src/lib/ingest.ts"],
    });
    await push({ matchMethod: "time-window", confidence: 0.3 });

    const { rows } = await database.query(
      `SELECT workspace_id, match_method, confidence_basis_points, project_id,
              files_touched
         FROM sessions.session_outcomes
        WHERE session_id = 'synthetic-project-outcome'`,
    );
    expect(rows).toEqual([
      {
        workspace_id: "workspace-p",
        match_method: "explicit_session",
        confidence_basis_points: 10000,
        project_id: "AgentWorkforce/relayhistory",
        files_touched: ["src/lib/ingest.ts"],
      },
    ]);
  });
});

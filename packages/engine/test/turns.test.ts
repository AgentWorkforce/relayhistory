import { Hono } from "hono";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import type { AuthContext, HistoryEnv } from "../src/env.js";
import { createTurnRoutes } from "../src/routes/turns.js";
import { createTestDatabase, type TestDatabase } from "./support/database.js";

let database: TestDatabase;

// The table is built by the real migrations, so routes write against the production
// key rather than a hand-written copy of it.
async function openMigratedDb() {
  database = await createTestDatabase();
}

describe("conversation turns journal", () => {
  beforeAll(openMigratedDb, 120_000);

  beforeEach(async () => {
    await database.exec(
      "TRUNCATE sessions.conversation_turns, sessions.convergence_events",
    );
  });

  afterAll(async () => {
    await database.close();
  });

  it("ingests idempotently, returns turns in order, and exposes native resume metadata", async () => {
    const app = appFor(authFor("org-a", ["rth:sync", "rth:read"]));
    const first = await app.request(
      "/sessions/session-a/turns",
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          sessionOwner: "Danny",
          turns: [
            {
              turnIndex: 1,
              role: "assistant",
              content: "I found ghp_123456789012345678901234567890123456",
              actorName: "Dev",
              actorRole: "steerer",
              metadata: {
                nativeResumeId: "codex-resume-123",
                originNode: "node-b",
              },
              ts: "2026-08-13T10:01:00.000Z",
            },
            {
              turnIndex: 0,
              role: "user",
              content: "Start the task",
              actorName: "Danny",
              actorRole: "owner",
              metadata: { nativeCli: "codex", originNode: "node-a" },
              ts: "2026-08-13T10:00:00.000Z",
            },
          ],
        }),
      },
      { DATABASE_URL: "postgres://pglite" },
    );

    expect(first.status).toBe(200);
    await expect(first.json()).resolves.toMatchObject({
      sessionId: "session-a",
      received: 2,
      accepted: 2,
    });

    const retry = await app.request(
      "/sessions/session-a/turns",
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify([
          {
            sessionOwner: "Danny",
            turnIndex: 0,
            role: "user",
            content: "Start the updated task",
            actorName: "Danny",
            actorRole: "owner",
            metadata: { nativeCli: "codex", originNode: "node-a" },
            ts: "2026-08-13T10:00:30.000Z",
          },
        ]),
      },
      { DATABASE_URL: "postgres://pglite" },
    );
    expect(retry.status).toBe(200);

    const turnsResponse = await app.request(
      "/sessions/session-a/turns",
      {},
      { DATABASE_URL: "postgres://pglite" },
    );
    const turnsBody = (await turnsResponse.json()) as {
      sessionId: string;
      turns: Array<Record<string, unknown>>;
    };

    expect(turnsResponse.status).toBe(200);
    expect(turnsBody.sessionId).toBe("session-a");
    expect(turnsBody.turns).toHaveLength(2);
    expect(turnsBody.turns.map((turn) => turn.turnIndex)).toEqual([0, 1]);
    expect(turnsBody.turns[0]).toMatchObject({
      sessionOwner: "Danny",
      role: "user",
      content: "Start the updated task",
      actorName: "Danny",
      actorRole: "owner",
      metadata: { nativeCli: "codex", originNode: "node-a" },
    });
    expect(turnsBody.turns[1]?.content).toBe("I found [REDACTED]");

    const metadata = await app.request(
      "/sessions/session-a/metadata",
      {},
      { DATABASE_URL: "postgres://pglite" },
    );
    expect(metadata.status).toBe(200);
    await expect(metadata.json()).resolves.toEqual({
      nativeCli: "codex",
      nativeResumeId: "codex-resume-123",
      sessionOwner: "Danny",
      originNode: "node-b",
    });
  });

  it("enforces write/read scopes and validates turn enums before database access", async () => {
    const readOnly = appFor(authFor("org-a", ["rth:read"]));
    const writeDenied = await readOnly.request(
      "/sessions/session-a/turns",
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ turns: [] }),
      },
      { DATABASE_URL: "postgres://pglite" },
    );
    expect(writeDenied.status).toBe(403);

    const syncOnly = appFor(authFor("org-a", ["rth:sync"]));
    const readDenied = await syncOnly.request(
      "/sessions/session-a/turns",
      {},
      { DATABASE_URL: "postgres://pglite" },
    );
    expect(readDenied.status).toBe(403);

    const invalid = await syncOnly.request(
      "/sessions/session-a/turns",
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          turns: [
            {
              sessionOwner: "Danny",
              turnIndex: 0,
              role: "tool",
              content: "invalid role",
              actorName: "Danny",
              actorRole: "owner",
              ts: "2026-08-13T10:00:00.000Z",
            },
          ],
        }),
      },
      { DATABASE_URL: "postgres://pglite" },
    );
    expect(invalid.status).toBe(400);
    await expect(invalid.json()).resolves.toMatchObject({
      error: {
        code: "bad_request",
        message: "turns[0].role must be one of: user, assistant, system",
      },
    });
  });

  it("keeps transcripts and metadata isolated to the authenticated org", async () => {
    const orgA = appFor(authFor("org-a", ["rth:sync", "rth:read"]));
    await orgA.request(
      "/sessions/shared-session/turns",
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify([
          {
            sessionOwner: "Danny",
            turnIndex: 0,
            role: "system",
            content: "Org A only",
            actorName: "Danny",
            actorRole: "owner",
            metadata: { nativeCli: "claude" },
            ts: "2026-08-13T10:00:00.000Z",
          },
        ]),
      },
      { DATABASE_URL: "postgres://pglite" },
    );

    const orgB = appFor(authFor("org-b", ["rth:read"]));
    const turns = await orgB.request(
      "/sessions/shared-session/turns",
      {},
      { DATABASE_URL: "postgres://pglite" },
    );
    await expect(turns.json()).resolves.toMatchObject({ turns: [] });

    const metadata = await orgB.request(
      "/sessions/shared-session/metadata",
      {},
      { DATABASE_URL: "postgres://pglite" },
    );
    expect(metadata.status).toBe(404);
  });
});

describe("conversation turns keyed by workspace and source", () => {
  beforeAll(openMigratedDb, 120_000);

  beforeEach(async () => {
    await database.exec(
      "TRUNCATE sessions.conversation_turns, sessions.convergence_events",
    );
  });

  afterAll(async () => {
    await database.close();
  });

  const env = { DATABASE_URL: "postgres://pglite" };

  async function post(app: Hono<HistoryEnv>, sessionId: string, body: unknown) {
    const response = await app.request(
      `/sessions/${sessionId}/turns`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      },
      env,
    );
    expect(response.status).toBe(200);
  }

  async function read(app: Hono<HistoryEnv>, path: string) {
    const response = await app.request(path, {}, env);
    expect(response.status).toBe(200);
    return ((await response.json()) as { turns: any[] }).turns;
  }

  function turn(turnIndex: number, content: string, metadata = {}) {
    return {
      turnIndex,
      role: "user",
      content,
      actorName: "Danny",
      actorRole: "owner",
      metadata,
      ts: `2026-08-13T10:0${turnIndex}:00.000Z`,
    };
  }

  async function event(workspaceId: string, source: string, sessionId: string) {
    await database.query(
      `INSERT INTO sessions.convergence_events
         (org_id, workspace_id, machine_id, user_id, source, session_id, event_id,
          kind, type, ts, record)
       VALUES ('org-a', $1, 'machine', 'user', $2, $3, $2, 'prompt', 'prompt', now(), '{}')`,
      [workspaceId, source, sessionId],
    );
  }

  async function stored() {
    return (
      await database.query<{
        workspace_id: string;
        source: string;
        turn_index: number;
        content: string;
      }>(
        `SELECT workspace_id, source, turn_index, content
           FROM sessions.conversation_turns
          ORDER BY workspace_id COLLATE "C", source COLLATE "C", turn_index`,
      )
    ).rows;
  }

  it("keeps two sources' turns for one session id apart and filters them by source", async () => {
    const app = appFor(authFor("org-a", ["rth:sync", "rth:read"]));
    await post(app, "shared", {
      sessionOwner: "Danny",
      turns: [turn(0, "Claude opening", { nativeCli: "claude" })],
    });
    await post(app, "shared", {
      sessionOwner: "Danny",
      turns: [turn(0, "Codex opening", { source: "codex" })],
    });

    expect(await stored()).toEqual([
      {
        workspace_id: "workspace-org-a",
        source: "claude",
        turn_index: 0,
        content: "Claude opening",
      },
      {
        workspace_id: "workspace-org-a",
        source: "codex",
        turn_index: 0,
        content: "Codex opening",
      },
    ]);
    expect(
      (await read(app, "/sessions/shared/turns?source=claude")).map(
        (row) => row.content,
      ),
    ).toEqual(["Claude opening"]);
    expect(
      (await read(app, "/sessions/shared/turns?source=codex")).map(
        (row) => row.content,
      ),
    ).toEqual(["Codex opening"]);
    // Unfiltered reads return both, ordered by the full key rather than by index alone.
    expect(
      (await read(app, "/sessions/shared/turns")).map((row) => row.content),
    ).toEqual(["Claude opening", "Codex opening"]);
  });

  it("keys OSS-uploaded turns by their session owner and replays them per source", async () => {
    const app = appFor(authFor("org-a", ["rth:sync", "rth:read"]));
    await event("workspace-org-a", "claude", "shared");
    await event("workspace-org-a", "codex", "shared");
    for (const source of ["claude", "codex"]) {
      await post(app, "shared", {
        sessionOwner: source,
        turns: [
          {
            ...turn(0, `${source} prompt`, {
              kind: "text",
              sourceRole: "user",
            }),
            actorName: source,
          },
        ],
      });
    }

    expect((await stored()).map((row) => row.source)).toEqual([
      "claude",
      "codex",
    ]);
    expect(
      (await read(app, "/sessions/shared/turns?source=codex")).map(
        (row) => row.content,
      ),
    ).toEqual(["codex prompt"]);
    const metadata = await app.request(
      "/sessions/shared/metadata?source=claude",
      {},
      env,
    );
    await expect(metadata.json()).resolves.toMatchObject({
      sessionOwner: "claude",
    });
  });

  it("keeps two workspaces' turns for one session id apart", async () => {
    const first = appFor(
      authFor("org-a", ["rth:sync", "rth:read"], "workspace-1"),
    );
    const second = appFor(authFor("org-a", ["rth:sync"], "workspace-2"));
    await post(first, "shared", {
      sessionOwner: "Danny",
      turns: [turn(0, "Workspace one", { nativeCli: "claude" })],
    });
    await post(second, "shared", {
      sessionOwner: "Danny",
      turns: [turn(0, "Workspace two", { nativeCli: "claude" })],
    });

    expect(await stored()).toEqual([
      {
        workspace_id: "workspace-1",
        source: "claude",
        turn_index: 0,
        content: "Workspace one",
      },
      {
        workspace_id: "workspace-2",
        source: "claude",
        turn_index: 0,
        content: "Workspace two",
      },
    ]);
    expect(
      (await read(first, "/sessions/shared/turns")).map((row) => row.content),
    ).toEqual(["Workspace one", "Workspace two"]);
  });

  it("resolves untagged turns to the request's source, so a retry updates in place", async () => {
    const app = appFor(authFor("org-a", ["rth:sync", "rth:read"]));
    await post(app, "session-b", {
      sessionOwner: "Danny",
      turns: [turn(0, "Opening", { nativeCli: "codex" }), turn(1, "Follow-up")],
    });
    await post(app, "session-b", {
      sessionOwner: "Danny",
      turns: [
        turn(0, "Opening", { nativeCli: "codex" }),
        turn(1, "Follow-up, edited"),
      ],
    });
    // A wholly untagged request resolves to the session owner, deterministically.
    await post(app, "session-c", {
      sessionOwner: "Danny",
      turns: [turn(0, "Untagged")],
    });
    await post(app, "session-c", {
      sessionOwner: "Danny",
      turns: [turn(0, "Untagged, edited")],
    });

    expect(await stored()).toEqual([
      {
        workspace_id: "workspace-org-a",
        source: "Danny",
        turn_index: 0,
        content: "Untagged, edited",
      },
      {
        workspace_id: "workspace-org-a",
        source: "codex",
        turn_index: 0,
        content: "Opening",
      },
      {
        workspace_id: "workspace-org-a",
        source: "codex",
        turn_index: 1,
        content: "Follow-up, edited",
      },
    ]);
  });

  it("never lets an untagged upload overwrite a turn another source stored for the owner", async () => {
    const app = appFor(authFor("org-a", ["rth:sync", "rth:read"]));
    await post(app, "shared", {
      sessionOwner: "Danny",
      turns: [turn(0, "Claude opening", { nativeCli: "claude" })],
    });
    // Only one source is stored for this owner, so a stored-source lookup would have
    // put this untagged upload on the claude row.
    await post(app, "shared", {
      sessionOwner: "Danny",
      turns: [turn(0, "Untagged opening")],
    });
    await post(app, "shared", {
      sessionOwner: "Danny",
      turns: [turn(0, "Codex opening", { nativeCli: "codex" })],
    });
    await post(app, "shared", {
      sessionOwner: "Danny",
      turns: [turn(0, "Untagged opening, edited")],
    });

    expect(await stored()).toEqual([
      {
        workspace_id: "workspace-org-a",
        source: "Danny",
        turn_index: 0,
        content: "Untagged opening, edited",
      },
      {
        workspace_id: "workspace-org-a",
        source: "claude",
        turn_index: 0,
        content: "Claude opening",
      },
      {
        workspace_id: "workspace-org-a",
        source: "codex",
        turn_index: 0,
        content: "Codex opening",
      },
    ]);
  });

  it("reads untagged follow-up turns under the source their request tagged", async () => {
    const app = appFor(authFor("org-a", ["rth:sync", "rth:read"]));
    // Both sources recorded activity for this session id, so the legacy rule alone
    // would leave the untagged follow-up unattributed and drop it.
    await event("workspace-org-a", "claude", "shared");
    await event("workspace-org-a", "codex", "shared");
    await post(app, "shared", {
      sessionOwner: "Danny",
      turns: [
        turn(0, "Codex opening", {
          nativeCli: "codex",
          nativeResumeId: "resume-codex",
        }),
        turn(1, "Codex follow-up"),
      ],
    });
    await post(app, "shared", {
      sessionOwner: "Danny",
      turns: [turn(0, "Claude opening", { nativeCli: "claude" })],
    });

    expect(
      (await read(app, "/sessions/shared/turns?source=codex")).map(
        (row) => row.content,
      ),
    ).toEqual(["Codex opening", "Codex follow-up"]);
    expect(
      (await read(app, "/sessions/shared/turns?source=claude")).map(
        (row) => row.content,
      ),
    ).toEqual(["Claude opening"]);
    const metadata = await app.request(
      "/sessions/shared/metadata?source=codex",
      {},
      env,
    );
    expect(metadata.status).toBe(200);
    await expect(metadata.json()).resolves.toMatchObject({
      nativeCli: "codex",
      nativeResumeId: "resume-codex",
      sessionOwner: "Danny",
    });
  });

  it("keeps the legacy rule for turns stored under their human session owner", async () => {
    const app = appFor(authFor("org-a", ["rth:sync", "rth:read"]));
    await event("workspace-org-a", "claude", "shared");
    await event("workspace-org-a", "codex", "shared");
    await post(app, "shared", {
      sessionOwner: "Danny",
      turns: [turn(0, "Untagged")],
    });

    expect(await read(app, "/sessions/shared/turns?source=codex")).toEqual([]);
    expect(await read(app, "/sessions/shared/turns?source=claude")).toEqual([]);
    expect(
      (await read(app, "/sessions/shared/turns")).map((row) => row.content),
    ).toEqual(["Untagged"]);
  });
});

function appFor(auth: AuthContext): Hono<HistoryEnv> {
  const app = new Hono<HistoryEnv>();
  app.use("*", async (c, next) => {
    c.set("requestId", "req-test");
    c.set("correlationId", "corr-test");
    c.set("auth", auth);
    await next();
  });
  app.route("/", createTurnRoutes({ database: () => database.db }));
  return app;
}

function authFor(
  orgId: string,
  scopes: string[],
  workspaceId = `workspace-${orgId}`,
): AuthContext {
  return {
    userId: `user-${orgId}`,
    orgId,
    workspaceId,
    tokenSubject: `user-${orgId}`,
    scopes,
    claims: {},
  };
}

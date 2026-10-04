import { readdirSync, readFileSync } from "node:fs";
import { URL } from "node:url";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createTestDatabase, type TestDatabase } from "./support/database.js";

const migrationsDir = new URL("../migrations/", import.meta.url);
const KEY_MIGRATION = "0020_conversation_turns_source_key.sql";
const migrations = readdirSync(migrationsDir)
  .filter((file) => file.endsWith(".sql"))
  .sort();

let database: TestDatabase;

async function apply(names: string[]) {
  for (const name of names) {
    await database.exec(readFileSync(new URL(name, migrationsDir), "utf8"));
  }
}

async function legacyTurn(
  sessionId: string,
  owner: string,
  turnIndex: number,
  metadata: object,
) {
  await database.query(
    `INSERT INTO sessions.conversation_turns
       (id, org_id, session_id, session_owner, turn_index, role, content,
        actor_name, actor_role, metadata, ts)
     VALUES (gen_random_uuid(), 'org-a', $1, $2, $3, 'user', $4, $2, 'owner',
             $5::jsonb, '2026-09-01T00:00:00Z')`,
    [sessionId, owner, turnIndex, `${sessionId}-${turnIndex}`, metadata],
  );
}

async function event(sessionId: string, source: string, workspaceId: string) {
  await database.query(
    `INSERT INTO sessions.convergence_events
       (org_id, workspace_id, machine_id, user_id, source, session_id, event_id,
        kind, type, ts, record)
     VALUES ('org-a', $3, 'machine', 'user', $2, $1, $3, 'prompt', 'prompt',
             '2026-09-01T00:00:00Z', '{}')`,
    [sessionId, source, workspaceId],
  );
}

describe("0020 conversation turns keyed by workspace and source", () => {
  beforeAll(async () => {
    // The harness migrates the whole ledger; rebuild the schema up to 0020 instead.
    database = await createTestDatabase();
    await database.exec(
      "DROP SCHEMA sessions CASCADE; CREATE SCHEMA sessions;",
    );
    await apply(migrations.filter((name) => name < KEY_MIGRATION));

    // The retired OSS publisher: sessionOwner=<source>, metadata {kind, sourceRole}.
    // Its session id also has codex activity in another workspace.
    await legacyTurn("oss", "claude", 0, { kind: "text", sourceRole: "user" });
    await legacyTurn("oss", "claude", 1, {
      kind: "control",
      sourceRole: "user",
      controlKind: "reminder",
    });
    await event("oss", "claude", "ws-1");
    await event("oss", "codex", "ws-2");
    // A broker-style session tagged only on its opening turn.
    await legacyTurn("tagged", "Danny", 0, { nativeCli: "codex" });
    await legacyTurn("tagged", "Danny", 1, {});
    await event("tagged", "codex", "ws-3");
    // A human owner whose name matches no source; the session has one workspace.
    await legacyTurn("human", "user_1", 0, {});
    await event("human", "claude", "ws-4");
    // The source was recorded in two workspaces, so the turn's cannot be known.
    await legacyTurn("ambiguous", "claude", 0, {});
    await event("ambiguous", "claude", "ws-5");
    await event("ambiguous", "claude", "ws-6");
    // No activity at all.
    await legacyTurn("orphan", "gemini", 0, { source: "gemini" });

    await apply(migrations.filter((name) => name >= KEY_MIGRATION));
  }, 120_000);

  afterAll(async () => {
    await database.close();
  });

  it("backfills source from metadata or the session owner, and workspace from unambiguous activity", async () => {
    const { rows } = await database.query(
      `SELECT session_id, turn_index, source, workspace_id
         FROM sessions.conversation_turns
        ORDER BY session_id, turn_index`,
    );
    expect(rows).toEqual([
      {
        session_id: "ambiguous",
        turn_index: 0,
        source: "claude",
        workspace_id: "default",
      },
      {
        session_id: "human",
        turn_index: 0,
        source: "user_1",
        workspace_id: "ws-4",
      },
      {
        session_id: "orphan",
        turn_index: 0,
        source: "gemini",
        workspace_id: "default",
      },
      {
        session_id: "oss",
        turn_index: 0,
        source: "claude",
        workspace_id: "ws-1",
      },
      {
        session_id: "oss",
        turn_index: 1,
        source: "claude",
        workspace_id: "ws-1",
      },
      {
        session_id: "tagged",
        turn_index: 0,
        source: "codex",
        workspace_id: "ws-3",
      },
      {
        session_id: "tagged",
        turn_index: 1,
        source: "codex",
        workspace_id: "ws-3",
      },
    ]);
  });

  it("lets another source or workspace store the same session turn, but not the same key twice", async () => {
    const insert = (workspaceId: string, source: string) =>
      database.query(
        `INSERT INTO sessions.conversation_turns
           (id, org_id, workspace_id, source, session_id, session_owner, turn_index,
            role, content, actor_name, actor_role, ts)
         VALUES (gen_random_uuid(), 'org-a', $1, $2, 'oss', $2, 0, 'user', 'x', $2,
                 'owner', now())`,
        [workspaceId, source],
      );
    await insert("ws-2", "codex");
    await insert("ws-2", "claude");
    await expect(insert("ws-2", "codex")).rejects.toThrow(/unique/i);

    const { rows } = await database.query(
      `SELECT workspace_id, source FROM sessions.conversation_turns
        WHERE session_id = 'oss' AND turn_index = 0
        ORDER BY workspace_id, source`,
    );
    expect(rows).toEqual([
      { workspace_id: "ws-1", source: "claude" },
      { workspace_id: "ws-2", source: "claude" },
      { workspace_id: "ws-2", source: "codex" },
    ]);
  });

  it("drops the old three-column key", async () => {
    const { rows } = await database.query<{ conname: string }>(
      `SELECT conname FROM pg_catalog.pg_constraint
        WHERE conrelid = 'sessions.conversation_turns'::regclass AND contype = 'u'`,
    );
    expect(rows.map((row) => row.conname)).toEqual([
      "conversation_turns_scope_turn_key",
    ]);
  });
});

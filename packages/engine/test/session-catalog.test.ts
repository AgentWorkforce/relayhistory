/**
 * The delivered session catalog (migration 0022): the `session`, `relationship`,
 * `session_marker` and `commit_link` delivery kinds projected into typed tables, and
 * the read paths over them.
 *
 * Every write goes through the real `acceptDelivery` and `accept_delivery_batch`, so the
 * trigger under test is the one production fires. Reads go through the real engine
 * with real `rth_at_*` tokens. The database is what `applyMigrations` leaves: every
 * packaged migration and the 0029 rollout that switches the projections on.
 */
import { copyFileSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { sql } from "drizzle-orm";
import {
  afterAll,
  afterEach,
  beforeAll,
  beforeEach,
  describe,
  expect,
  it,
} from "vitest";
import { createHistoryEngine } from "../src/engine.js";
import {
  MIGRATIONS_DIR,
  applyMigrations,
  readMigrations,
} from "../src/migrate/index.js";
import { createSession } from "../src/auth/tokens.js";
import type { AuthContext } from "../src/env.js";
import { acceptDelivery, deliveryAccount } from "../src/lib/delivery.js";
import type {
  DeliveryKind,
  HistoryExportRecord,
} from "../src/lib/delivery-contracts.js";
import {
  sessionCatalogResponse,
  sessionsResponse,
} from "./support/recall-contract.js";
import { createTestDatabase, type TestDatabase } from "./support/database.js";
import {
  createFreshDatabase,
  type FreshDatabase,
} from "./support/fresh-database.js";

let database: TestDatabase;
let db: any;
const app = createHistoryEngine({ database: () => database?.db });

function tenant(
  orgId = "org-catalog",
  workspaceId = "ws-catalog",
): AuthContext {
  return {
    orgId,
    workspaceId,
    userId: "user-catalog",
    tokenSubject: "user-catalog",
    scopes: ["rth:sync", "rth:read"],
    claims: {},
  };
}

function record(
  kind: DeliveryKind,
  recordId: string,
  revision: number,
  sessionId: string | null,
  payload: Record<string, unknown> | null,
  originId = "origin-a",
): HistoryExportRecord {
  return {
    schema_version: 1,
    origin_id: originId,
    record_id: recordId,
    revision_id: `${originId}:${recordId}:${revision}`,
    revision,
    kind,
    source: "claude",
    session_id: sessionId,
    operation: payload === null ? "delete" : "upsert",
    payload,
  };
}

let batchCounter = 0;
async function deliver(
  auth: AuthContext,
  records: HistoryExportRecord[],
  originId = records[0]!.origin_id,
) {
  await acceptDelivery(db, auth, {
    schema_version: 1,
    origin_id: originId,
    batch_id: `batch-${++batchCounter}`,
    job_id: "job-catalog",
    generation: 1,
    destination_id: "relayhistory",
    instance_id: "catalog-test",
    account_id: await deliveryAccount(auth),
    mapping_version: "relayhistory-delivery-v1",
    records,
  });
}

function sessionPayload(overrides: Record<string, unknown> = {}) {
  return {
    source: "claude",
    session_id: "sess-parent",
    cwd: "/work/repo",
    git_branch: "feature/catalog",
    repo_url: "https://github.com/example/repo",
    initial_commit: "a".repeat(40),
    first_prompt: "Build the session catalog",
    models_json: '["claude-opus","claude-haiku"]',
    originator: "cli",
    agent_version: "2.1.0",
    workspace_roots_json: '["/work/repo"]',
    project_key: "github.com/example/repo",
    project_key_method: "git_remote",
    first_activity_ms: 1_758_000_000_000,
    last_activity_ms: 1_758_000_600_000,
    raw_path: "/home/someone/.claude/projects/x.jsonl",
    ...overrides,
  };
}

function relationshipPayload(
  uid: string,
  child: string | null,
  overrides: Record<string, unknown> = {},
) {
  return {
    source: "claude",
    parent_session_id: "sess-parent",
    relationship_uid: uid,
    child_session_id: child,
    relationship: "subagent",
    identity_status: child ? "observed" : "unlinked",
    child_agent_type: "general-purpose",
    child_agent_name: "explorer",
    child_model: "claude-haiku",
    spawn_depth: 1,
    evidence_kind: "sidecar",
    child_has_events: child ? 1 : 0,
    spawned_at_ms: 1_758_000_100_000,
    created_ms: 1_758_000_100_000,
    updated_ms: 1_758_000_100_000,
    ...overrides,
  };
}

async function rows(table: string, order: string) {
  return (
    await database.query<Record<string, any>>(
      `SELECT * FROM sessions.${table} ORDER BY ${order}`,
    )
  ).rows;
}

async function tokenFor(auth: AuthContext) {
  const session = await createSession(
    db,
    {
      userId: auth.userId,
      orgId: auth.orgId,
      workspaceId: auth.workspaceId ?? "",
    },
    { scopes: ["rth:read"] },
  );
  return session.accessToken;
}

async function get(path: string, auth: AuthContext) {
  return app.request(path, {
    headers: { Authorization: `Bearer ${await tokenFor(auth)}` },
  });
}

beforeAll(async () => {
  database = await createTestDatabase();
  db = database.db;
}, 120_000);

afterAll(async () => {
  await database?.close();
});

beforeEach(async () => {
  await database.exec(`TRUNCATE sessions.delivery_records, sessions.delivery_receipts,
    sessions.delivery_origins, sessions.convergence_events, sessions.session_catalog,
    sessions.session_relationships, sessions.session_markers, sessions.session_commit_links,
    sessions.delivery_catalog_acceptance`);
});

describe("delivery catalog projection", () => {
  it("projects a session record into typed catalog columns", async () => {
    await deliver(tenant(), [
      record("session", "rec-session", 1, "sess-parent", sessionPayload()),
    ]);
    const [row, ...rest] = await rows("session_catalog", "session_id");
    expect(rest).toEqual([]);
    expect(row).toMatchObject({
      org_id: "org-catalog",
      workspace_id: "ws-catalog",
      source: "claude",
      session_id: "sess-parent",
      record_id: "rec-session",
      origin_id: "origin-a",
      git_branch: "feature/catalog",
      repo_url: "https://github.com/example/repo",
      initial_commit: "a".repeat(40),
      first_prompt: "Build the session catalog",
      models: ["claude-opus", "claude-haiku"],
      originator: "cli",
      agent_version: "2.1.0",
      workspace_roots: ["/work/repo"],
      project_key: "github.com/example/repo",
      project_key_method: "git_remote",
    });
    expect(new Date(row!.first_activity_at).getTime()).toBe(1_758_000_000_000);
    expect(new Date(row!.last_activity_at).getTime()).toBe(1_758_000_600_000);
    // Metadata is not activity: the convergence projection still ignores it.
    expect(await rows("convergence_events", "event_id")).toEqual([]);
  });

  it("keeps the latest revision and ignores a stale one", async () => {
    await deliver(tenant(), [
      record("session", "rec-session", 2, "sess-parent", sessionPayload()),
    ]);
    await deliver(tenant(), [
      record(
        "session",
        "rec-session",
        3,
        "sess-parent",
        sessionPayload({ git_branch: "main", models_json: null }),
      ),
    ]);
    await deliver(tenant(), [
      record(
        "session",
        "rec-session",
        1,
        "sess-parent",
        sessionPayload({ git_branch: "stale" }),
      ),
    ]);
    const catalog = await rows("session_catalog", "session_id");
    expect(catalog).toHaveLength(1);
    expect(catalog[0]).toMatchObject({ git_branch: "main", models: null });
  });

  it("removes the projection on a delete tombstone", async () => {
    await deliver(tenant(), [
      record("session", "rec-session", 1, "sess-parent", sessionPayload()),
      record(
        "relationship",
        "rec-rel",
        1,
        "sess-parent",
        relationshipPayload("uid-1", "sess-child"),
      ),
    ]);
    expect(
      await rows("session_relationships", "relationship_uid"),
    ).toHaveLength(1);
    await deliver(tenant(), [
      record("session", "rec-session", 2, "sess-parent", null),
      record("relationship", "rec-rel", 2, "sess-parent", null),
    ]);
    expect(await rows("session_catalog", "session_id")).toEqual([]);
    expect(await rows("session_relationships", "relationship_uid")).toEqual([]);
  });

  it("does not duplicate one record delivered by several origins", async () => {
    await deliver(tenant(), [
      record("session", "rec-session", 7, "sess-parent", sessionPayload()),
    ]);
    await deliver(tenant(), [
      record(
        "session",
        "rec-session",
        1,
        "sess-parent",
        sessionPayload({ git_branch: "from-origin-b" }),
        "origin-b",
      ),
    ]);
    let catalog = await rows("session_catalog", "session_id");
    expect(catalog).toHaveLength(1);
    // Revisions are per-origin; the most recently accepted upsert wins.
    expect(catalog[0]).toMatchObject({
      origin_id: "origin-b",
      git_branch: "from-origin-b",
    });

    // One origin pruning its copy does not erase what the other still reports.
    await deliver(tenant(), [
      record("session", "rec-session", 2, "sess-parent", null, "origin-b"),
    ]);
    catalog = await rows("session_catalog", "session_id");
    expect(catalog).toHaveLength(1);
    expect(catalog[0]).toMatchObject({
      origin_id: "origin-a",
      git_branch: "feature/catalog",
    });

    await deliver(tenant(), [
      record("session", "rec-session", 8, "sess-parent", null),
    ]);
    expect(await rows("session_catalog", "session_id")).toEqual([]);
  });

  it("lets the later write win even when its received_at is older", async () => {
    // accept_delivery_batch stamps received_at with now(), the transaction start. A
    // writer that started first but waited on the per-record lock commits later with
    // the older stamp; simulate that by pushing origin-a's stamp into the future.
    const branch = async () =>
      (await rows("session_catalog", "session_id")).map((row) => [
        row.origin_id,
        row.git_branch,
      ]);
    await deliver(tenant(), [
      record(
        "session",
        "rec-session",
        1,
        "sess-parent",
        sessionPayload({ git_branch: "from-origin-a" }),
      ),
    ]);
    await database.query(
      `UPDATE sessions.delivery_records SET received_at = now() + interval '1 day'
        WHERE origin_id = 'origin-a' AND record_id = 'rec-session'`,
    );
    expect(await branch()).toEqual([["origin-a", "from-origin-a"]]);

    await deliver(tenant(), [
      record(
        "session",
        "rec-session",
        1,
        "sess-parent",
        sessionPayload({ git_branch: "from-origin-b" }),
        "origin-b",
      ),
    ]);
    expect(await branch()).toEqual([["origin-b", "from-origin-b"]]);

    // A third origin's tombstone leaves the projected live copy, not the copy with
    // the newest stamp.
    await deliver(tenant(), [
      record(
        "session",
        "rec-session",
        1,
        "sess-parent",
        sessionPayload(),
        "origin-c",
      ),
    ]);
    await deliver(tenant(), [
      record(
        "session",
        "rec-session",
        2,
        "sess-parent",
        sessionPayload({ git_branch: "from-origin-b-2" }),
        "origin-b",
      ),
    ]);
    await deliver(tenant(), [
      record("session", "rec-session", 2, "sess-parent", null, "origin-c"),
    ]);
    expect(await branch()).toEqual([["origin-b", "from-origin-b-2"]]);

    // Once the projected origin deletes, a remaining live copy is restored.
    await deliver(tenant(), [
      record("session", "rec-session", 3, "sess-parent", null, "origin-b"),
    ]);
    expect(await branch()).toEqual([["origin-a", "from-origin-a"]]);
    await deliver(tenant(), [
      record("session", "rec-session", 2, "sess-parent", null),
    ]);
    expect(await branch()).toEqual([]);
  });

  it("keeps a key another live record still maps to when the projected record is deleted", async () => {
    const commit = (repo: string) => ({
      commit_sha: "d".repeat(40),
      match_method: "trailer",
      repo,
      confidence: 1,
    });
    // Two distinct records map to one session key and one commit-link key.
    await deliver(tenant(), [
      record(
        "session",
        "rec-session-a",
        1,
        "sess-parent",
        sessionPayload({ git_branch: "from-record-a" }),
      ),
      record("commit_link", "rec-commit-a", 1, "sess-parent", commit("a/repo")),
    ]);
    await deliver(tenant(), [
      record(
        "session",
        "rec-session-b",
        1,
        "sess-parent",
        sessionPayload({ git_branch: "from-record-b" }),
      ),
      record("commit_link", "rec-commit-b", 1, "sess-parent", commit("b/repo")),
    ]);
    const shown = async () => [
      ...(await rows("session_catalog", "session_id")).map((row) => [
        row.record_id,
        row.git_branch,
      ]),
      ...(await rows("session_commit_links", "commit_sha")).map((row) => [
        row.record_id,
        row.repo,
      ]),
    ];
    expect(await shown()).toEqual([
      ["rec-session-b", "from-record-b"],
      ["rec-commit-b", "b/repo"],
    ]);

    await deliver(tenant(), [
      record("session", "rec-session-b", 2, "sess-parent", null),
      record("commit_link", "rec-commit-b", 2, "sess-parent", null),
    ]);
    expect(await shown()).toEqual([
      ["rec-session-a", "from-record-a"],
      ["rec-commit-a", "a/repo"],
    ]);

    // A revision that re-keys the remaining record frees the key it left.
    await deliver(tenant(), [
      record(
        "session",
        "rec-session-a",
        2,
        "sess-other",
        sessionPayload({ session_id: "sess-other", git_branch: "moved" }),
      ),
    ]);
    expect(
      (await rows("session_catalog", "session_id")).map((row) => [
        row.session_id,
        row.git_branch,
      ]),
    ).toEqual([["sess-other", "moved"]]);
  });

  it("restores the contender accepted last, not the one with the newest received_at", async () => {
    const stamp = (origin: string, when: string) =>
      database.query(
        `UPDATE sessions.delivery_records SET received_at = ${when}
          WHERE origin_id = $1 AND record_id = 'rec-session'`,
        [origin],
      );
    const branch = async () =>
      (await rows("session_catalog", "session_id")).map((row) => [
        row.origin_id,
        row.git_branch,
      ]);
    // origin-a is accepted first but carries the newest stamp; origin-c is
    // accepted after it with an older stamp (it started first, then waited).
    await deliver(tenant(), [
      record(
        "session",
        "rec-session",
        1,
        "sess-parent",
        sessionPayload({ git_branch: "from-origin-a" }),
      ),
    ]);
    await stamp("origin-a", "now() + interval '1 day'");
    await deliver(tenant(), [
      record(
        "session",
        "rec-session",
        1,
        "sess-parent",
        sessionPayload({ git_branch: "from-origin-c" }),
        "origin-c",
      ),
    ]);
    await stamp("origin-c", "now() - interval '1 day'");
    await deliver(tenant(), [
      record(
        "session",
        "rec-session",
        1,
        "sess-parent",
        sessionPayload({ git_branch: "from-origin-b" }),
        "origin-b",
      ),
    ]);
    expect(await branch()).toEqual([["origin-b", "from-origin-b"]]);

    await deliver(tenant(), [
      record("session", "rec-session", 2, "sess-parent", null, "origin-b"),
    ]);
    expect(await branch()).toEqual([["origin-c", "from-origin-c"]]);
  });

  it("keeps unlinked sidecars of one parent as separate relationships", async () => {
    await deliver(tenant(), [
      record(
        "relationship",
        "rec-rel-1",
        1,
        "sess-parent",
        relationshipPayload("uid-1", null),
      ),
      record(
        "relationship",
        "rec-rel-2",
        1,
        "sess-parent",
        relationshipPayload("uid-2", null),
      ),
      record(
        "relationship",
        "rec-rel-3",
        1,
        "sess-parent",
        relationshipPayload("uid-3", "sess-child", { child_has_events: true }),
      ),
    ]);
    const relationships = await rows(
      "session_relationships",
      "relationship_uid",
    );
    expect(
      relationships.map((row) => ({
        uid: row.relationship_uid,
        parent: row.parent_session_id,
        child: row.child_session_id,
        status: row.identity_status,
        depth: row.spawn_depth,
        hasEvents: row.child_has_events,
      })),
    ).toEqual([
      {
        uid: "uid-1",
        parent: "sess-parent",
        child: null,
        status: "unlinked",
        depth: 1,
        hasEvents: false,
      },
      {
        uid: "uid-2",
        parent: "sess-parent",
        child: null,
        status: "unlinked",
        depth: 1,
        hasEvents: false,
      },
      {
        uid: "uid-3",
        parent: "sess-parent",
        child: "sess-child",
        status: "observed",
        depth: 1,
        hasEvents: true,
      },
    ]);
  });

  it("projects markers and commit links with confidence in basis points", async () => {
    await deliver(tenant(), [
      record("session_marker", "rec-marker", 1, "sess-parent", {
        source: "claude",
        session_id: "sess-parent",
        marker_uid: "marker-1",
        ts_ms: 1_758_000_300_000,
        kind: "compaction",
        subkind: "auto",
        turn_id: "turn-9",
        text: "Summary of earlier context",
        payload_json: '{"trigger":"auto"}',
      }),
      record("commit_link", "rec-commit", 1, "sess-parent", {
        source: "claude",
        session_id: "sess-parent",
        repo: "example/repo",
        branch: "feature/catalog",
        commit_sha: "b".repeat(40),
        match_method: "trailer",
        confidence: 0.875,
        files_json: '["src/a.ts"]',
        numstat_json: null,
        created_at_ms: 1_758_000_500_000,
      }),
      // Missing identity: stored raw, never projected.
      record("commit_link", "rec-commit-bad", 1, "sess-parent", {
        repo: "example/repo",
      }),
    ]);
    expect(await rows("session_markers", "marker_uid")).toEqual([
      expect.objectContaining({
        session_id: "sess-parent",
        marker_uid: "marker-1",
        marker_kind: "compaction",
        subkind: "auto",
        turn_id: "turn-9",
        text: "Summary of earlier context",
        payload: { trigger: "auto" },
      }),
    ]);
    expect(await rows("session_commit_links", "commit_sha")).toEqual([
      expect.objectContaining({
        commit_sha: "b".repeat(40),
        match_method: "trailer",
        repo: "example/repo",
        confidence_basis_points: 8750,
        files: ["src/a.ts"],
        numstat: null,
      }),
    ]);
  });

  it("serializes a batch under one workspace lock, however many keys it touches", async () => {
    // Per-key locks let two origins' batches take the same keys in opposite
    // orders and deadlock; one workspace lock (shared with the activity
    // projection) cannot. Two different catalog keys, one transaction: one lock.
    // One connection for the whole transaction, under PGlite or a pool; the thrown
    // error rolls it back.
    const rollback = new Error("rollback");
    await expect(
      db.transaction(async (tx: any) => {
        for (const [id, kind, payload] of [
          ["rec-lock-session", "session", sessionPayload()],
          [
            "rec-lock-rel",
            "relationship",
            relationshipPayload("uid-lock", "sess-child"),
          ],
        ] as const)
          await tx.execute(
            sql`INSERT INTO sessions.delivery_records
             (org_id, workspace_id, origin_id, record_id, revision_id, revision, digest, kind, source, session_id, operation, payload, user_id)
             VALUES ('org-lock', 'ws-lock', 'origin-a', ${id}, ${id} || ':1', 1, 'digest', ${kind}, 'claude', 'sess-parent', 'upsert', ${JSON.stringify(payload)}::jsonb, 'user')`,
          );
        const locks = (
          await tx.execute(
            sql`SELECT count(*)::int AS count FROM pg_locks
              WHERE locktype = 'advisory' AND pid = pg_backend_pid()`,
          )
        ).rows[0]!.count;
        expect(locks).toBe(1);
        throw rollback;
      }),
    ).rejects.toBe(rollback);
  });

  it("keys every projection by organization and workspace", async () => {
    const same = [
      record("session", "rec-session", 1, "sess-parent", sessionPayload()),
      record(
        "relationship",
        "rec-rel",
        1,
        "sess-parent",
        relationshipPayload("uid-1", "sess-child"),
      ),
    ];
    await deliver(tenant(), same);
    await deliver(tenant("org-catalog", "ws-other"), same);
    await deliver(tenant("org-other", "ws-catalog"), same);
    expect(
      (await rows("session_catalog", "org_id, workspace_id")).map((row) => [
        row.org_id,
        row.workspace_id,
      ]),
    ).toEqual([
      ["org-catalog", "ws-catalog"],
      ["org-catalog", "ws-other"],
      ["org-other", "ws-catalog"],
    ]);
    expect(
      await rows("session_relationships", "org_id, workspace_id"),
    ).toHaveLength(3);

    // A tombstone in one workspace leaves the others intact.
    await deliver(tenant("org-catalog", "ws-other"), [
      record("session", "rec-session", 2, "sess-parent", null),
    ]);
    expect(
      (await rows("session_catalog", "org_id, workspace_id")).map((row) => [
        row.org_id,
        row.workspace_id,
      ]),
    ).toEqual([
      ["org-catalog", "ws-catalog"],
      ["org-other", "ws-catalog"],
    ]);
  });
});

describe("migration backfill", () => {
  let fresh: FreshDatabase;
  // The production runner over just the packaged migrations `filter` selects; what
  // the ledger already holds is skipped, as on a real upgrade.
  async function migrate(filter: (name: string) => boolean) {
    const directory = mkdtempSync(join(tmpdir(), "rh-engine-migrations-"));
    try {
      for (const { name } of readMigrations())
        if (filter(name))
          copyFileSync(resolve(MIGRATIONS_DIR, name), join(directory, name));
      await applyMigrations(fresh.client, { directory });
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  }
  beforeEach(async () => {
    fresh = await createFreshDatabase();
  });
  afterEach(async () => {
    await fresh?.close();
  });

  it("projects records retained before 0022, choosing the live copy across origins", async () => {
    await migrate((name) => name < "0022");
    const insert = (
      origin: string,
      id: string,
      revision: number,
      kind: string,
      payload: object | null,
      receivedAt: string,
    ) =>
      fresh.query(
        `INSERT INTO sessions.delivery_records
         (org_id, workspace_id, origin_id, record_id, revision_id, revision, digest, kind, source, session_id, operation, payload, user_id, received_at)
         VALUES ('org', 'ws', $1, $2, $1 || ':' || $2 || ':' || $3::text, $3::bigint, 'digest', $4, 'claude', 'sess-parent', $5, $6::jsonb, 'user', $7)`,
        [
          origin,
          id,
          revision,
          kind,
          payload ? "upsert" : "delete",
          payload ? JSON.stringify(payload) : null,
          receivedAt,
        ],
      );
    await insert(
      "origin-a",
      "rec-session",
      4,
      "session",
      sessionPayload({ git_branch: "older" }),
      "2026-09-01T00:00:00Z",
    );
    await insert(
      "origin-b",
      "rec-session",
      1,
      "session",
      // Retained before structured evidence was validated; dropped, not raised.
      sessionPayload({ git_branch: "newer", models_json: "not json" }),
      "2026-09-02T00:00:00Z",
    );
    await insert(
      "origin-a",
      "rec-rel",
      1,
      "relationship",
      relationshipPayload("uid-1", "sess-child"),
      "2026-09-01T00:00:00Z",
    );
    await insert(
      "origin-a",
      "rec-gone",
      2,
      "relationship",
      null,
      "2026-09-01T00:00:00Z",
    );
    await insert(
      "origin-a",
      "rec-history",
      1,
      "history",
      { prompt: "hello", timestamp_ms: 1_758_000_000_000 },
      "2026-09-01T00:00:00Z",
    );

    await migrate((name) => name >= "0022");

    const catalog = (
      await fresh.query<Record<string, any>>(
        "SELECT origin_id, git_branch, models FROM sessions.session_catalog",
      )
    ).rows;
    expect(catalog).toEqual([
      { origin_id: "origin-b", git_branch: "newer", models: null },
    ]);
    const relationships = (
      await fresh.query<Record<string, any>>(
        "SELECT relationship_uid, child_session_id FROM sessions.session_relationships",
      )
    ).rows;
    expect(relationships).toEqual([
      { relationship_uid: "uid-1", child_session_id: "sess-child" },
    ]);
  });
});

describe("catalog read paths", () => {
  async function seed(auth = tenant()) {
    await deliver(auth, [
      record("session", "rec-parent", 1, "sess-parent", sessionPayload()),
      record(
        "session",
        "rec-child",
        1,
        "sess-child",
        sessionPayload({
          session_id: "sess-child",
          git_branch: "feature/child",
          models_json: '["claude-haiku"]',
        }),
      ),
      record(
        "relationship",
        "rec-rel-1",
        1,
        "sess-parent",
        relationshipPayload("uid-1", "sess-child"),
      ),
      record(
        "relationship",
        "rec-rel-2",
        1,
        "sess-parent",
        relationshipPayload("uid-2", null, {
          spawned_at_ms: 1_758_000_200_000,
        }),
      ),
      record("session_marker", "rec-marker", 1, "sess-parent", {
        marker_uid: "marker-1",
        kind: "compaction",
        ts_ms: 1_758_000_300_000,
      }),
      record("commit_link", "rec-commit", 1, "sess-parent", {
        commit_sha: "c".repeat(40),
        match_method: "trailer",
        repo: "example/repo",
        confidence: 1,
      }),
      // Activity, so the sessions appear in the convergence rollup.
      record("history", "rec-prompt-parent", 1, "sess-parent", {
        prompt: "Build the session catalog",
        timestamp_ms: 1_758_000_000_000,
      }),
      record("history", "rec-prompt-child", 1, "sess-child", {
        prompt: "Explore the repo",
        timestamp_ms: 1_758_000_100_000,
      }),
    ]);
  }

  it("returns catalog, relationships, markers and commit links for the token workspace", async () => {
    await seed();
    const response = await get(
      "/v1/sessions/sess-parent/catalog?source=claude",
      tenant(),
    );
    expect(response.status).toBe(200);
    expect(response.headers.get("X-Relayhistory-Workspace-Id")).toBe(
      "ws-catalog",
    );
    const body = sessionCatalogResponse.parse(await response.json());
    expect(body).toMatchObject({
      sessionId: "sess-parent",
      source: "claude",
      workspaceId: "ws-catalog",
      catalog: {
        gitBranch: "feature/catalog",
        repoUrl: "https://github.com/example/repo",
        projectKey: "github.com/example/repo",
        models: ["claude-opus", "claude-haiku"],
        agentVersion: "2.1.0",
        firstPrompt: "Build the session catalog",
        firstActivityAt: new Date(1_758_000_000_000).toISOString(),
      },
      parents: [],
      markers: [{ markerUid: "marker-1", kind: "compaction" }],
      commitLinks: [
        { commitSha: "c".repeat(40), matchMethod: "trailer", confidence: 1 },
      ],
      truncated: false,
    });
    expect(body.children.map((child) => child.childSessionId)).toEqual([
      "sess-child",
      null,
    ]);

    const child = sessionCatalogResponse.parse(
      await (
        await get("/v1/sessions/sess-child/catalog?source=claude", tenant())
      ).json(),
    );
    expect(child.parents).toEqual([
      expect.objectContaining({
        relationshipUid: "uid-1",
        parentSessionId: "sess-parent",
        relationship: "subagent",
        childAgentName: "explorer",
      }),
    ]);
  });

  it("requires a source and never reads another tenant's catalog", async () => {
    await seed();
    expect(
      (await get("/v1/sessions/sess-parent/catalog", tenant())).status,
    ).toBe(400);
    for (const other of [
      tenant("org-catalog", "ws-other"),
      tenant("org-other", "ws-catalog"),
    ]) {
      const response = await get(
        "/v1/sessions/sess-parent/catalog?source=claude",
        other,
      );
      expect(response.status).toBe(404);
    }
  });

  it("attaches catalog summaries to the session list", async () => {
    await seed();
    const response = await get("/v1/sessions?workspace=ws-catalog", tenant());
    expect(response.status).toBe(200);
    const body = sessionsResponse.parse(await response.json());
    const byId = new Map(body.sessions.map((s) => [s.sessionId, s]));
    expect(byId.get("sess-parent")?.catalog).toMatchObject({
      workspaceId: "ws-catalog",
      gitBranch: "feature/catalog",
      models: ["claude-opus", "claude-haiku"],
      parentSessionIds: [],
      childSessionCount: 2,
    });
    expect(byId.get("sess-child")?.catalog).toMatchObject({
      gitBranch: "feature/child",
      parentSessionIds: ["sess-parent"],
      childSessionCount: 0,
    });
  });

  it("omits the catalog when an organization-wide session spans workspaces", async () => {
    await seed();
    await seed(tenant("org-catalog", "ws-other"));
    await seed(tenant("org-other", "ws-catalog"));
    const orgWide = sessionsResponse.parse(
      await (await get("/v1/sessions", tenant())).json(),
    );
    expect(orgWide.sessions.length).toBeGreaterThan(0);
    for (const session of orgWide.sessions)
      expect(session.catalog).toBeUndefined();

    const scoped = sessionsResponse.parse(
      await (
        await get(
          "/v1/sessions?workspace=ws-other",
          tenant("org-catalog", "ws-other"),
        )
      ).json(),
    );
    expect(
      scoped.sessions.map((session) => session.catalog?.workspaceId),
    ).toEqual(["ws-other", "ws-other"]);
  });
});

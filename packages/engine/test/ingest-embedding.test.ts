import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { AuthContext } from "../src/env.js";
import {
  DEFAULT_EMBEDDING_DIM,
  DEFAULT_EMBEDDING_MODEL,
  EMBEDDING_SKIP_LOG_PREFIX,
  sha256Hex,
  type EmbeddingProvider,
} from "../src/lib/embed.js";
import { applyIngest, buildReadableContent } from "../src/lib/ingest.js";
import { createTestDatabase, type TestDatabase } from "./support/database.js";

let database: TestDatabase;

describe("ingest embedding path", () => {
  beforeEach(async () => {
    database = await createTestDatabase();
  });

  afterEach(async () => {
    await database.close();
    vi.restoreAllMocks();
  });

  it("writes the embedding column and provenance when a provider is configured", async () => {
    const db = database.db;
    const secret =
      "kind in PK and secret ghp_123456789012345678901234567890123456";
    const seen: string[] = [];
    const vector = Array.from({ length: DEFAULT_EMBEDDING_DIM }, (_, i) =>
      i === 0 ? 0.25 : 0,
    );
    const provider: EmbeddingProvider = {
      model: DEFAULT_EMBEDDING_MODEL,
      dim: DEFAULT_EMBEDDING_DIM,
      async embed(text) {
        seen.push(text);
        return vector;
      },
    };

    const outcome = await applyIngest(
      db,
      authFor("org-embed"),
      {
        machine: { id: "machine-embed" },
        batchId: "batch-embed-configured",
        records: [
          {
            v: 1,
            kind: "finding",
            source: "trajectories",
            lens: "trajectories",
            sessionId: "traj_embed",
            eventId: "finding:traj_embed:learning:0",
            ts: "2026-09-02T10:00:00.000Z",
            type: "finding",
            content: secret,
            taskTitle: "Build embeddings",
            record: {},
          },
        ],
      },
      { embeddings: provider },
    );

    expect(outcome).toMatchObject({ received: 1, accepted: 1 });
    expect(seen).toHaveLength(1);
    expect(seen[0]).not.toContain("ghp_");
    expect(seen[0]).toContain("[REDACTED]");
    const expectedContent = buildReadableContent(secret, {
      title: "Build embeddings",
    });
    expect(seen[0]).toBe(expectedContent);

    const rows = await database.query<Record<string, unknown>>(
      `SELECT
        content,
        embedding,
        embedding_model,
        embedding_dim,
        content_hash,
        generated_at,
        embedding_skip_reason
      FROM convergence_events`,
    );
    expect(rows.rows).toHaveLength(1);
    const row = rows.rows[0];
    expect(String(row.content)).toContain("[REDACTED]");
    expect(String(row.content)).not.toContain("ghp_");
    expect(row.embedding_model).toBe(DEFAULT_EMBEDDING_MODEL);
    expect(Number(row.embedding_dim)).toBe(DEFAULT_EMBEDDING_DIM);
    expect(row.embedding_skip_reason).toBeNull();
    expect(row.generated_at).toBeTruthy();
    expect(row.content_hash).toBe(await sha256Hex(String(row.content)));
    const stored = parseVector(row.embedding);
    expect(stored).toHaveLength(DEFAULT_EMBEDDING_DIM);
    expect(stored[0]).toBeCloseTo(0.25);
  });

  it("writes the row and records the skip when no provider is configured", async () => {
    const db = database.db;
    const info = vi.spyOn(console, "info").mockImplementation(() => {});

    const outcome = await applyIngest(db, authFor("org-embed"), {
      machine: { id: "machine-embed" },
      batchId: "batch-embed-skip",
      records: [
        {
          v: 1,
          kind: "finding",
          source: "trajectories",
          lens: "trajectories",
          sessionId: "traj_skip",
          eventId: "finding:traj_skip:learning:0",
          ts: "2026-09-02T10:00:00.000Z",
          type: "finding",
          content: "kind in PK",
          taskTitle: "Build embeddings",
          record: {},
        },
      ],
    });

    expect(outcome).toMatchObject({ received: 1, accepted: 1 });
    expect(
      info.mock.calls.some((args) =>
        String(args[0]).startsWith(
          `${EMBEDDING_SKIP_LOG_PREFIX} provider_not_configured`,
        ),
      ),
    ).toBe(true);

    const rows = await database.query<Record<string, unknown>>(
      `SELECT
        event_id,
        content,
        embedding,
        embedding_model,
        embedding_dim,
        content_hash,
        generated_at,
        embedding_skip_reason
      FROM convergence_events`,
    );
    expect(rows.rows).toHaveLength(1);
    const row = rows.rows[0];
    expect(row.event_id).toBe("finding:traj_skip:learning:0");
    expect(String(row.content)).toMatch(/^Task: Build embeddings/);
    expect(row.embedding).toBeNull();
    expect(row.embedding_model).toBeNull();
    expect(row.embedding_dim).toBeNull();
    expect(row.generated_at).toBeNull();
    expect(row.embedding_skip_reason).toBe("provider_not_configured");
    expect(row.content_hash).toBe(await sha256Hex(String(row.content)));
  });

  it("embeds a batch of records in one provider call after scrub", async () => {
    const db = database.db;
    const seen: string[][] = [];
    const vector = Array.from({ length: DEFAULT_EMBEDDING_DIM }, (_, i) =>
      i === 0 ? 0.5 : 0,
    );
    const provider: EmbeddingProvider = {
      model: DEFAULT_EMBEDDING_MODEL,
      dim: DEFAULT_EMBEDDING_DIM,
      async embed() {
        throw new Error("ingest must use embedMany, not serial embed()");
      },
      async embedMany(texts) {
        seen.push([...texts]);
        return texts.map(() => vector);
      },
    };

    const outcome = await applyIngest(
      db,
      authFor("org-embed"),
      {
        machine: { id: "machine-embed" },
        batchId: "batch-embed-many",
        records: [
          {
            v: 1,
            kind: "finding",
            source: "trajectories",
            lens: "trajectories",
            sessionId: "traj_batch_a",
            eventId: "finding:traj_batch_a:learning:0",
            ts: "2026-09-02T10:00:00.000Z",
            type: "finding",
            content:
              "kind in PK and secret ghp_123456789012345678901234567890123456",
            taskTitle: "Build embeddings",
            record: {},
          },
          {
            v: 1,
            kind: "finding",
            source: "trajectories",
            lens: "trajectories",
            sessionId: "traj_batch_b",
            eventId: "finding:traj_batch_b:learning:0",
            ts: "2026-09-02T10:01:00.000Z",
            type: "finding",
            content: "kind in PK second record",
            taskTitle: "Build embeddings",
            record: {},
          },
          {
            v: 1,
            kind: "finding",
            source: "trajectories",
            lens: "trajectories",
            sessionId: "traj_batch_c",
            eventId: "finding:traj_batch_c:learning:0",
            ts: "2026-09-02T10:02:00.000Z",
            type: "finding",
            content: "kind in PK third record",
            taskTitle: "Build embeddings",
            record: {},
          },
        ],
      },
      { embeddings: provider },
    );

    expect(outcome).toMatchObject({ received: 3, accepted: 3 });
    expect(seen).toHaveLength(1);
    expect(seen[0]).toHaveLength(3);
    expect(seen[0]?.some((text) => text.includes("ghp_"))).toBe(false);
    expect(seen[0]?.some((text) => text.includes("[REDACTED]"))).toBe(true);

    const rows = await database.query<{ event_id: string; embedding: unknown }>(
      `SELECT event_id, embedding FROM convergence_events ORDER BY event_id`,
    );
    expect(rows.rows).toHaveLength(3);
    for (const row of rows.rows) {
      expect(parseVector(row.embedding)).toHaveLength(DEFAULT_EMBEDDING_DIM);
    }
  });

  it("preserves a stored embedding when a replay cannot embed", async () => {
    const db = database.db;
    const info = vi.spyOn(console, "info").mockImplementation(() => {});
    const vector = Array.from({ length: DEFAULT_EMBEDDING_DIM }, (_, i) =>
      i === 0 ? 0.42 : 0,
    );
    const provider: EmbeddingProvider = {
      model: DEFAULT_EMBEDDING_MODEL,
      dim: DEFAULT_EMBEDDING_DIM,
      async embed() {
        return vector;
      },
    };
    const record = {
      v: 1 as const,
      kind: "finding",
      source: "trajectories",
      lens: "trajectories",
      sessionId: "traj_replay",
      eventId: "finding:traj_replay:learning:0",
      ts: "2026-09-02T10:00:00.000Z",
      type: "finding",
      content: "kind in PK and replay must keep the vector",
      taskTitle: "Build embeddings",
      record: {},
    };

    await applyIngest(
      db,
      authFor("org-embed"),
      {
        machine: { id: "machine-embed" },
        batchId: "batch-embed-first",
        records: [record],
      },
      { embeddings: provider },
    );

    const before = await database.query<Record<string, unknown>>(
      `SELECT
        embedding,
        embedding_model,
        embedding_dim,
        content_hash,
        generated_at,
        embedding_skip_reason
      FROM convergence_events`,
    );
    expect(before.rows).toHaveLength(1);
    const stored = before.rows[0];
    const storedVector = parseVector(stored.embedding);
    expect(storedVector).toHaveLength(DEFAULT_EMBEDDING_DIM);
    expect(storedVector[0]).toBeCloseTo(0.42);
    expect(stored.embedding_skip_reason).toBeNull();
    const originalHash = stored.content_hash;
    const originalGeneratedAt = stored.generated_at;

    const replay = await applyIngest(db, authFor("org-embed"), {
      machine: { id: "machine-embed" },
      batchId: "batch-embed-replay",
      records: [record],
    });

    expect(replay).toMatchObject({ received: 1, accepted: 1 });
    expect(
      info.mock.calls.some((args) =>
        String(args[0]).startsWith(
          `${EMBEDDING_SKIP_LOG_PREFIX} provider_not_configured`,
        ),
      ),
    ).toBe(true);

    const after = await database.query<Record<string, unknown>>(
      `SELECT
        embedding,
        embedding_model,
        embedding_dim,
        content_hash,
        generated_at,
        embedding_skip_reason
      FROM convergence_events`,
    );
    expect(after.rows).toHaveLength(1);
    const row = after.rows[0];
    const replayed = parseVector(row.embedding);
    expect(replayed).toHaveLength(DEFAULT_EMBEDDING_DIM);
    expect(replayed[0]).toBeCloseTo(0.42);
    expect(row.embedding_model).toBe(DEFAULT_EMBEDDING_MODEL);
    expect(Number(row.embedding_dim)).toBe(DEFAULT_EMBEDDING_DIM);
    expect(row.content_hash).toBe(originalHash);
    expect(row.generated_at).toEqual(originalGeneratedAt);
    expect(row.embedding_skip_reason).toBe("provider_not_configured");
  });
});

function authFor(orgId: string): AuthContext {
  return {
    userId: "user-embed",
    orgId,
    workspaceId: "workspace-embed",
    tokenSubject: "user-embed",
    scopes: ["rth:sync"],
    claims: {},
    sessionId: `session-${orgId}`,
  };
}

function parseVector(value: unknown): number[] {
  if (Array.isArray(value)) {
    return value.map(Number);
  }
  if (typeof value !== "string" || !value) {
    return [];
  }
  const trimmed = value.trim().replace(/^\[/, "").replace(/\]$/, "");
  if (!trimmed) {
    return [];
  }
  return trimmed.split(",").map(Number);
}

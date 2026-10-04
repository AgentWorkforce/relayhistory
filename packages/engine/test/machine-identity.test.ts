import { afterEach, beforeEach, describe, expect, it } from "vitest";
import type { AuthContext } from "../src/env.js";
import { applyIngest } from "../src/lib/ingest.js";
import { createTestDatabase, type TestDatabase } from "./support/database.js";

let database: TestDatabase;

describe("machine identity across pushes", () => {
  beforeEach(async () => {
    database = await createTestDatabase();
  });

  afterEach(async () => {
    await database.close();
  });

  /**
   * The bug this closes. `hostname` is best-effort on the client: the CLI reads it from
   * `$HOSTNAME`, which launchd does not export. So an interactive `ai-hist push` sends the
   * real name and the scheduled push 300s later sends nothing — and the upsert used to
   * write that nothing straight over the top.
   *
   * Measured on the live fleet: all three actively-pushing machines had NULL hostnames,
   * while the only rows that still carried one were machines that had stopped pushing in
   * June and so had never been overwritten. The machines you most need to identify were
   * the ones guaranteed to be anonymous.
   */
  it("keeps a hostname learned from one push when a later push omits it", async () => {
    const db = database.db;

    // Interactive push from a shell that exports $HOSTNAME.
    await push(db, { id: "m_laptop", hostname: "kjg-laptop", os: "macos" });
    expect(await identity("m_laptop")).toMatchObject({
      hostname: "kjg-laptop",
    });

    // The launchd timer fires. Same machine, no $HOSTNAME in its environment.
    await push(db, { id: "m_laptop", os: "macos" });

    expect(await identity("m_laptop")).toMatchObject({
      hostname: "kjg-laptop",
    });
  });

  it("still takes a new hostname when the client actually reports one", async () => {
    const db = database.db;

    await push(db, { id: "m_laptop", hostname: "old-name" });
    await push(db, { id: "m_laptop", hostname: "renamed-host" });

    // Only null is ignored — a real rename must not be pinned to the first value seen.
    expect(await identity("m_laptop")).toMatchObject({
      hostname: "renamed-host",
    });
  });

  it("preserves label, os and version by the same rule", async () => {
    const db = database.db;

    await push(db, {
      id: "m_laptop",
      hostname: "kjg-laptop",
      label: "fleet-primary",
      os: "macos",
      relayhistoryVersion: "0.9.0",
    });
    await push(db, { id: "m_laptop" });

    expect(await identity("m_laptop")).toMatchObject({
      hostname: "kjg-laptop",
      label: "fleet-primary",
      os: "macos",
      relayhistory_version: "0.9.0",
    });
  });

  /**
   * The version column was NULL for all 16 machines in production, which read as "no
   * machine has ever reported a version" rather than as a bug. It was a name mismatch:
   * `ai-hist` serializes this field as `cliVersion` (MachineIdentity in ai-hist-core)
   * while the server only ever read `relayhistoryVersion`, so every push silently
   * dropped it. The server accepts both, so machines still on an older binary report
   * their version without needing to be upgraded first.
   */
  it("records the version the CLI actually sends it (cliVersion)", async () => {
    const db = database.db;

    await push(db, {
      id: "m_cli",
      hostname: "sf-mac-mini",
      cliVersion: "0.2.0",
    });

    expect(await identity("m_cli")).toMatchObject({
      relayhistory_version: "0.2.0",
    });
  });

  it("prefers the canonical name when a client sends both", async () => {
    const db = database.db;

    await push(db, {
      id: "m_both",
      relayhistoryVersion: "0.3.0",
      cliVersion: "0.2.0",
    });

    expect(await identity("m_both")).toMatchObject({
      relayhistory_version: "0.3.0",
    });
  });

  /**
   * Freshness and cursors describe the *push*, not the machine, so they must keep
   * last-write-wins. If preserving identity accidentally froze `last_seen_at`, every
   * machine would look permanently stale — the opposite failure.
   */
  it("still advances lastSeenAt and cursors on every push", async () => {
    const db = database.db;

    await push(
      db,
      { id: "m_laptop", hostname: "kjg-laptop" },
      { history_id: 10 },
    );
    const first = await identity("m_laptop");

    await database.query(
      `UPDATE machines SET last_seen_at = now() - interval '1 hour' WHERE machine_id = 'm_laptop'`,
    );
    const aged = await identity("m_laptop");

    await push(db, { id: "m_laptop" }, { history_id: 4242 });
    const after = await identity("m_laptop");

    expect(after.cursors_json).toEqual({ history_id: 4242 });
    expect(new Date(after.last_seen_at as string).getTime()).toBeGreaterThan(
      new Date(aged.last_seen_at as string).getTime(),
    );
    expect(first.hostname).toBe("kjg-laptop");
    expect(after.hostname).toBe("kjg-laptop");
  });
});

async function push(
  db: any,
  machine: Record<string, unknown>,
  cursors: Record<string, number> = { history_id: 1 },
) {
  return applyIngest(db, auth(), {
    machine: machine as any,
    batchId: `b_${machine.id}_${JSON.stringify(cursors)}`,
    cursors,
    records: [],
  });
}

async function identity(machineId: string): Promise<Record<string, unknown>> {
  const { rows } = await database.query<Record<string, unknown>>(
    `SELECT hostname, label, os, relayhistory_version, last_seen_at, cursors_json
       FROM machines WHERE machine_id = $1`,
    [machineId],
  );
  return rows[0];
}

function auth(): AuthContext {
  return {
    userId: "user-identity",
    orgId: "org-identity",
    workspaceId: "workspace-identity",
    tokenSubject: "user-identity",
    scopes: ["rth:sync"],
    claims: {},
    sessionId: "session-identity",
  };
}

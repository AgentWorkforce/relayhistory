import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import * as schema from "../src/db/schema.js";
import { createHistoryEngine } from "../src/engine.js";
import { createSession } from "../src/auth/tokens.js";
import type { AuthContext } from "../src/env.js";
import {
  decodeLinkCursor,
  encodeLinkCursor,
  sessionsForCommit,
} from "../src/lib/session-links.js";
import { MIGRATIONS_DIR } from "../src/migrate/index.js";
import { createTestDatabase, type TestDatabase } from "./support/database.js";

const migration = (name: string) =>
  readFileSync(resolve(MIGRATIONS_DIR, `${name}.sql`), "utf8");
const auth: AuthContext = {
  orgId: "org-a",
  workspaceId: "workspace-a",
  userId: "user-a",
  tokenSubject: "user-a",
  scopes: ["rth:read", "rth:sync"],
  claims: {},
};
let database: TestDatabase;
let db: any;
let token: string;
const app = createHistoryEngine({ database: () => database?.db });
const ref = "AgentWorkforce/relayhistory-cloud#123";

async function request(path: string, access = token, body?: unknown) {
  return app.request(path, {
    method: body === undefined ? "GET" : "POST",
    headers: {
      Authorization: `Bearer ${access}`,
      "Content-Type": "application/json",
    },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
  });
}
async function thread(query = "", access = token) {
  const response = await request(
    `/v1/sessions/session-a/thread?source=claude${query}`,
    access,
  );
  expect(response.status).toBe(200);
  return response.json() as Promise<any>;
}
async function link(overrides: Record<string, unknown> = {}) {
  await db
    .insert(schema.sessionLinks)
    .values({
      orgId: auth.orgId,
      workspaceId: auth.workspaceId,
      source: "claude",
      sessionId: "session-a",
      linkKind: "github_pr",
      linkRef: ref,
      linkTs: "2026-09-08T10:00:00Z",
      provenanceLens: "github",
      confidence: 9000,
      ...overrides,
    })
    .onConflictDoNothing();
}
async function ingest(records: unknown[]) {
  const response = await request("/v1/ingest", token, {
    machine: { id: "machine-a" },
    batchId: crypto.randomUUID(),
    records,
  });
  expect(response.status).toBe(200);
  expect(await response.json()).toMatchObject({ accepted: records.length });
}
function event(lens = "history") {
  return {
    v: 1,
    kind: "event",
    source: "claude",
    sessionId: "session-a",
    eventId: `event:${lens}`,
    lens,
    ts: "2026-09-08T09:00:00Z",
    content: "Synthetic thread test",
  };
}
async function outcome(overrides: Record<string, unknown> = {}) {
  await db.insert(schema.sessionOutcomes).values({
    orgId: auth.orgId,
    workspaceId: auth.workspaceId,
    userId: auth.userId,
    machineId: "machine-a",
    source: "claude",
    sessionId: "session-a",
    commitSha: "abc123",
    shippedAt: new Date("2026-09-08T11:00:00Z"),
    ...overrides,
  });
}

describe("session links lifecycle thread", () => {
  beforeAll(async () => {
    database = await createTestDatabase();
    db = database.db;
  }, 60_000);
  beforeEach(async () => {
    await database.exec(
      "TRUNCATE machines, convergence_events, session_outcomes, session_links, auth_sessions, sync_batches RESTART IDENTITY",
    );
    token = (
      await createSession(db, {
        userId: auth.userId,
        orgId: auth.orgId,
        workspaceId: auth.workspaceId!,
      })
    ).accessToken;
  });
  afterAll(async () => {
    await database?.close();
  });

  it("insert + uniqueness: github_pr #123 inserts once for an identical tuple; migration reruns", async () => {
    await link();
    await link();
    expect(
      (await database.query("select link_ref from session_links")).rows,
    ).toEqual([{ link_ref: ref }]);
    await database.exec(migration("0008_session_links"));
    expect(
      (await database.query("select count(*)::int as count from session_links"))
        .rows,
    ).toEqual([{ count: 1 }]);
  });

  it("tenancy: other org sees absent rows; owning token sees envelope, outcome and link; source isolates collisions", async () => {
    await ingest([event()]);
    await outcome();
    await link();
    const other = (
      await createSession(db, {
        orgId: "org-b",
        workspaceId: "workspace-b",
        userId: "user-b",
      })
    ).accessToken;
    const absent = await thread("&orgId=org-a&workspaceId=workspace-a", other);
    expect(absent).toEqual({
      session: null,
      outcomes: [],
      links: [],
      nextCursor: null,
    });
    const own = await thread();
    expect(own.links).toHaveLength(1);
    expect(own.outcomes).toHaveLength(1);
    expect(own.session.orgId).toBe("org-a");
    const differentSource = await request(
      "/v1/sessions/session-a/thread?source=codex",
    );
    expect(await differentSource.json()).toEqual(absent);
    expect(
      await sessionsForCommit(db, { ...auth, orgId: "org-b" }, "abc123"),
    ).toEqual([]);
    expect(await sessionsForCommit(db, auth, "abc123")).toEqual([
      { workspaceId: "workspace-a", source: "claude", sessionId: "session-a" },
    ]);
  });

  it("thread endpoint: authenticated GET returns session envelope, outcomes and one link", async () => {
    await ingest([event()]);
    await outcome();
    await link({
      linkUrl: "https://github.com/AgentWorkforce/relayhistory-cloud/pull/123",
      metadata: { state: "open" },
    });
    expect(await thread()).toEqual({
      session: {
        source: "claude",
        sessionId: "session-a",
        orgId: "org-a",
        workspaceId: "workspace-a",
        firstEventAt: "2026-09-08T09:00:00.000Z",
        lastEventAt: "2026-09-08T09:00:00.000Z",
      },
      outcomes: [
        {
          commitSha: "abc123",
          shippedAt: "2026-09-08T11:00:00.000Z",
          reverted: false,
          revertedBySha: null,
          revertedAt: null,
        },
      ],
      links: [
        {
          linkKind: "github_pr",
          linkRef: ref,
          linkUrl:
            "https://github.com/AgentWorkforce/relayhistory-cloud/pull/123",
          linkTs: "2026-09-08T10:00:00.000000Z",
          metadata: { state: "open" },
          confidence: 0.9,
        },
      ],
      nextCursor: null,
    });
  });

  it("thread outcomes are the envelope workspace's, not another workspace's colliding session", async () => {
    await ingest([event()]);
    await outcome({ reverted: false });
    await outcome({ workspaceId: "workspace-b", reverted: true });
    const body = await thread();
    expect(body.session.workspaceId).toBe("workspace-a");
    expect(body.outcomes).toEqual([
      expect.objectContaining({ commitSha: "abc123", reverted: false }),
    ]);
  });

  it("cursor pagination: three distinct timestamps with limit=2 yield 2 then 1, exhausted", async () => {
    for (let n = 1; n <= 3; n++)
      await link({ linkRef: `ref-${n}`, linkTs: `2026-09-08T10:00:0${n}Z` });
    await outcome();
    const first = await thread("&limit=2");
    expect(first.links.map((r: any) => r.linkRef)).toEqual(["ref-3", "ref-2"]);
    expect(first.nextCursor).toEqual(expect.any(String));
    const second = await thread(
      `&limit=2&cursor=${encodeURIComponent(first.nextCursor)}`,
    );
    expect(second.links.map((r: any) => r.linkRef)).toEqual(["ref-1"]);
    expect(second.nextCursor).toBeNull();
    expect(second.outcomes).toEqual(first.outcomes);
  });

  it("cursor pagination: equal timestamps use id tiebreak across page boundary", async () => {
    await link({ linkRef: "older" });
    await link({ linkRef: "newer" });
    const first = await thread("&limit=1");
    expect(first.links[0].linkRef).toBe("newer");
    const second = await thread(
      `&limit=1&cursor=${encodeURIComponent(first.nextCursor)}`,
    );
    expect(second.links[0].linkRef).toBe("older");
    expect(second.nextCursor).toBeNull();
  });

  it("cursor preserves microseconds, bigint ids and undated links without skipping or repeating", async () => {
    await link({
      id: 9007199254740993n,
      linkRef: "micro-1",
      linkTs: "2026-09-08T10:00:00.000001Z",
    });
    await link({
      id: 9007199254740994n,
      linkRef: "micro-2",
      linkTs: "2026-09-08T10:00:00.000002Z",
    });
    await link({ linkRef: "null-1", linkTs: null });
    await link({ linkRef: "null-2", linkTs: null });
    const refs: string[] = [];
    let cursor: string | null = null;
    do {
      const page = await thread(
        `&limit=1${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ""}`,
      );
      refs.push(...page.links.map((r: any) => r.linkRef));
      cursor = page.nextCursor;
      expect(refs.length).toBeLessThanOrEqual(4);
    } while (cursor);
    expect(refs).toEqual(["micro-2", "micro-1", "null-2", "null-1"]);
  });

  it.each([
    null,
    "2026-09-08T12:00:00.000002+02:00",
    "2026-09-09T01:59:00.000002+15:59",
  ])(
    "since preserves a returned linkTs microsecond bound and excludes earlier links in the same millisecond (offset: %s)",
    async (offsetSince) => {
      await link({ linkRef: "earlier", linkTs: "2026-09-08T10:00:00.000001Z" });
      await link({ linkRef: "later", linkTs: "2026-09-08T10:00:00.000002Z" });
      const initial = await thread();
      expect(initial.links.map((row: any) => row.linkRef)).toEqual([
        "later",
        "earlier",
      ]);
      const since = initial.links[0].linkTs;
      expect(since).toBe("2026-09-08T10:00:00.000002Z");
      const filtered = await thread(
        `&since=${encodeURIComponent(offsetSince ?? since)}`,
      );
      expect(filtered.links.map((row: any) => row.linkRef)).toEqual(["later"]);
      expect(filtered.nextCursor).toBeNull();
    },
  );

  it("since and kinds narrow links while outcomes and envelope remain whole", async () => {
    await link();
    await link({
      linkKind: "incident",
      linkRef: "incident-a",
      linkTs: "2026-09-08T12:00:00Z",
    });
    await outcome();
    const page = await thread(
      "&kinds=incident,unknown&since=2026-09-08T11:00:00Z",
    );
    expect(page.links.map((r: any) => r.linkKind)).toEqual(["incident"]);
    expect(page.outcomes).toHaveLength(1);
    const empty = await thread("&kinds=unknown");
    expect(empty.links).toEqual([]);
    expect(empty.session).toEqual(page.session);
    expect(empty.session.firstEventAt).toBeNull();
  });

  it("requires rth:read and source; rejects malformed since and cursor", async () => {
    const syncOnly = (
      await createSession(
        db,
        {
          userId: auth.userId,
          orgId: auth.orgId,
          workspaceId: auth.workspaceId!,
        },
        { scopes: ["rth:sync"] },
      )
    ).accessToken;
    expect(
      (await request("/v1/sessions/session-a/thread?source=claude", syncOnly))
        .status,
    ).toBe(403);
    expect((await request("/v1/sessions/session-a/thread")).status).toBe(400);
    for (const query of [
      "since=garbage",
      "since=0000-09-08T10:00:00.000002Z",
      `since=${encodeURIComponent("2026-09-08T10:00:00.000002+16:00")}`,
      `since=${encodeURIComponent("2026-09-08T10:00:00.000002+23:59")}`,
      "since=2026-02-30T10:00:00.000002Z",
      "since=2026-09-08T10:00:00.0000002Z",
      "cursor=garbage",
      `cursor=${encodeURIComponent(btoa('["2026-02-30T10:00:00.000000Z","1"]'))}`,
      "cursor=",
      `cursor=${btoa('[null,"0"]')}`,
    ]) {
      expect(
        (await request(`/v1/sessions/session-a/thread?source=claude&${query}`))
          .status,
      ).toBe(400);
    }
    expect(
      decodeLinkCursor(
        encodeLinkCursor({ linkTs: null, id: "9007199254740993" }),
      ),
    ).toEqual({ linkTs: null, id: "9007199254740993" });
  });

  it("github ingest projects an explicit taskRef into a tenant-owned link, idempotently", async () => {
    const record = {
      ...event("github"),
      taskRef: {
        system: "github",
        id: ref,
        url: "https://github.com/AgentWorkforce/relayhistory-cloud/pull/123",
      },
      confidence: 0.8,
      orgId: "org-attacker",
      workspaceId: "workspace-attacker",
    };
    await ingest([record]);
    await ingest([
      {
        ...record,
        eventId: "event:newer-pr",
        ts: "2026-09-08T12:00:00Z",
        confidence: 0.9,
      },
    ]);
    await ingest([record]);
    const page = await thread("&since=2026-09-08T11:00:00Z");
    expect(page.links).toEqual([
      expect.objectContaining({
        linkKind: "github_pr",
        linkRef: ref,
        confidence: 0.9,
      }),
    ]);
    expect(
      (
        await database.query(
          "select org_id, workspace_id, provenance_lens from session_links",
        )
      ).rows,
    ).toEqual([
      {
        org_id: "org-a",
        workspace_id: "workspace-a",
        provenance_lens: "github",
      },
    ]);
    await ingest([
      {
        ...event("github"),
        eventId: "event:ambiguous",
        taskRef: { system: "github", id: "ambiguous" },
      },
    ]);
    await ingest([
      {
        ...event("github"),
        eventId: "event:issue",
        taskRef: {
          system: "github",
          id: "AgentWorkforce/relayhistory-cloud#124",
          url: "https://github.com/AgentWorkforce/relayhistory-cloud/issues/124",
        },
      },
    ]);
    expect((await thread()).links).toHaveLength(1);
  });

  it.each(["github", "nightcto", "custom-future-lens"])(
    "lens regression: POST /v1/ingest lens=%s succeeds and round-trips",
    async (lens) => {
      await ingest([event(lens)]);
      const response = await request(
        "/v1/events?source=claude&session=session-a",
      );
      expect(response.status).toBe(200);
      expect(((await response.json()) as any).events).toEqual([
        expect.objectContaining({ lens, sessionId: "session-a" }),
      ]);
    },
  );
});

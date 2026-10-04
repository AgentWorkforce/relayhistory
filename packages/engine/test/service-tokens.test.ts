import { Hono } from "hono";
import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  it,
  vi,
} from "vitest";
import type { HistoryDb } from "../src/db/database.js";
import {
  SERVICE_PREFIX,
  SERVICE_TTL_MAX_DAYS,
  ServiceTokenError,
  createServiceToken,
  createSession,
  hashToken,
  listServiceTokens,
  refreshSession,
  resolveAccessToken,
  revokeServiceToken,
} from "../src/auth/tokens.js";
import {
  createRequireAuth,
  getAuth,
  requireScope,
} from "../src/middleware/auth.js";
import type { HistoryEnv } from "../src/env.js";
import { createTestDatabase, type TestDatabase } from "./support/database.js";

let client: TestDatabase;
let db: HistoryDb;

const minter = {
  userId: "user_1",
  orgId: "org_a",
  workspaceId: "ws_1",
  scopes: ["rth:sync", "rth:read"],
};

/**
 * Service tokens exist because a deployed agent had no credential it could hold. An
 * `rth_at_` token lives 24 hours and `refreshSession` rotates it in place, so a copy taken
 * from a developer's machine works today and silently 401s tomorrow — and
 * `/v1/admin/mint` is deliberately 404 in production, so there was no other path.
 *
 * These tests hold the properties that make one safe to hand to a background job.
 */
describe("service tokens", () => {
  // The real migrated `sessions.auth_sessions`, emptied before each test.
  beforeAll(async () => {
    client = await createTestDatabase();
    db = client.db;
  });

  beforeEach(async () => {
    await client.exec("DELETE FROM sessions.auth_sessions");
  });

  afterAll(async () => {
    await client?.close();
  });

  it("mints a usable, distinguishable credential", async () => {
    const issued = await createServiceToken(db, minter, {
      label: "repo-intel",
    });

    expect(issued.token.startsWith(SERVICE_PREFIX)).toBe(true);
    const resolved = await resolveAccessToken(db, issued.token);
    expect(resolved?.orgId).toBe("org_a");
    expect(resolved?.subjectType).toBe("service");
  });

  it("coalesces usage writes without caching authorization", async () => {
    const issued = await createSession(db, minter);
    const update = vi.spyOn(db, "update");
    vi.useFakeTimers({ toFake: ["Date"] });
    const now = Date.now();
    try {
      vi.setSystemTime(now);
      expect(await resolveAccessToken(db, issued.accessToken)).not.toBeNull();
      expect(update).toHaveBeenCalledTimes(1);
      vi.setSystemTime(now + 299_999);
      expect(await resolveAccessToken(db, issued.accessToken)).not.toBeNull();
      expect(update).toHaveBeenCalledTimes(1);
      vi.setSystemTime(now + 300_000);
      expect(await resolveAccessToken(db, issued.accessToken)).not.toBeNull();
      expect(update).toHaveBeenCalledTimes(2);
      await client.query(
        "UPDATE auth_sessions SET revoked_at = now() WHERE id = $1",
        [issued.sessionId],
      );
      expect(await resolveAccessToken(db, issued.accessToken)).toBeNull();
      expect(update).toHaveBeenCalledTimes(2);
      await client.query(
        "UPDATE auth_sessions SET revoked_at = NULL, access_token_expires_at = $1 WHERE id = $2",
        [new Date(now + 300_000).toISOString(), issued.sessionId],
      );
      expect(await resolveAccessToken(db, issued.accessToken)).toBeNull();
      expect(update).toHaveBeenCalledTimes(2);
    } finally {
      vi.useRealTimers();
      update.mockRestore();
    }
  });

  /**
   * The secret must exist in exactly one place: the caller's hands. Anything that can read
   * it back turns the token store into a second copy to protect.
   */
  describe("the secret is never retrievable", () => {
    it("stores only a hash, never the token", async () => {
      const issued = await createServiceToken(db, minter, { label: "agent" });

      const { rows } = await client.query<{ access_token_hash: string }>(
        "SELECT access_token_hash FROM auth_sessions WHERE subject_type = 'service'",
      );
      expect(rows[0]?.access_token_hash).toBe(await hashToken(issued.token));
      expect(rows[0]?.access_token_hash).not.toBe(issued.token);
    });

    it("does not expose it when listing", async () => {
      const issued = await createServiceToken(db, minter, { label: "agent" });

      const listed = await listServiceTokens(db, "org_a");
      expect(JSON.stringify(listed)).not.toContain(issued.token);
      expect(listed[0]).toMatchObject({ label: "agent" });
    });
  });

  /**
   * A service token is a narrowing of the authority the caller already proved. If it could
   * widen, any read-only session could mint itself write access.
   */
  describe("scopes cannot escalate", () => {
    it("intersects requested scopes with the minter's", async () => {
      const readOnly = { ...minter, scopes: ["rth:read"] };

      const issued = await createServiceToken(db, readOnly, {
        label: "agent",
        scopes: ["rth:read", "rth:sync"],
      });

      expect(issued.scopes).toEqual(["rth:read"]);
    });

    it("refuses outright when no requested scope is held", async () => {
      const readOnly = { ...minter, scopes: ["rth:read"] };

      await expect(
        createServiceToken(db, readOnly, {
          label: "agent",
          scopes: ["rth:sync"],
        }),
      ).rejects.toBeInstanceOf(ServiceTokenError);
    });

    it("inherits the minter's scopes when none are requested", async () => {
      const issued = await createServiceToken(db, minter, { label: "agent" });
      expect(issued.scopes.sort()).toEqual(["rth:read", "rth:sync"]);
    });
  });

  /**
   * No refresh path. The stored refresh hash is a value nobody holds, and `refreshSession`
   * refuses `subject_type = 'service'` — closed twice, so the property survives a change
   * to either half.
   */
  describe("cannot be exchanged for a new token", () => {
    it("is not refreshable even when its refresh hash is known", async () => {
      await createServiceToken(db, minter, { label: "agent" });

      // Hand the service row a refresh token an attacker is holding — the strongest
      // version of this test, since the real stored hash is of a value nobody has.
      const known = "rth_rt_known_value_for_this_test";
      await client.query(
        `UPDATE auth_sessions SET refresh_token_hash = $1 WHERE subject_type = 'service'`,
        [await hashToken(known)],
      );

      expect(await refreshSession(db, known)).toBeNull();
    });
  });

  describe("lifetime is bounded and revocable", () => {
    it("caps the requested lifetime", async () => {
      const issued = await createServiceToken(db, minter, {
        label: "agent",
        expiresInDays: SERVICE_TTL_MAX_DAYS * 10,
      });

      const days =
        (new Date(issued.expiresAt).getTime() - Date.now()) / 86_400_000;
      expect(days).toBeLessThanOrEqual(SERVICE_TTL_MAX_DAYS + 1);
    });

    it("stops resolving once revoked", async () => {
      const issued = await createServiceToken(db, minter, { label: "agent" });
      expect(await resolveAccessToken(db, issued.token)).not.toBeNull();

      expect(await revokeServiceToken(db, "org_a", issued.id)).toBe(true);

      expect(await resolveAccessToken(db, issued.token)).toBeNull();
    });

    it("stops resolving once expired", async () => {
      const issued = await createServiceToken(db, minter, { label: "agent" });
      await client.query(
        `UPDATE auth_sessions SET access_token_expires_at = now() - interval '1 hour'
         WHERE subject_type = 'service'`,
      );

      expect(await resolveAccessToken(db, issued.token)).toBeNull();
    });
  });

  /**
   * Cross-org revocation must fail as "no such token", not as "not yours" — a
   * distinguishable answer confirms which ids are real.
   */
  describe("tenancy", () => {
    it("cannot revoke another org's token", async () => {
      const issued = await createServiceToken(db, minter, { label: "agent" });

      expect(await revokeServiceToken(db, "org_b", issued.id)).toBe(false);
      // And it still works, i.e. the failed attempt changed nothing.
      expect(await resolveAccessToken(db, issued.token)).not.toBeNull();
    });

    it("cannot list another org's tokens", async () => {
      await createServiceToken(db, minter, { label: "mine" });

      expect(await listServiceTokens(db, "org_b")).toEqual([]);
    });

    it("does not report a second revoke as success", async () => {
      const issued = await createServiceToken(db, minter, { label: "agent" });

      expect(await revokeServiceToken(db, "org_a", issued.id)).toBe(true);
      expect(await revokeServiceToken(db, "org_a", issued.id)).toBe(false);
    });
  });

  /**
   * The CLI refuses to read with a stored session that has no org, so a pair without
   * tenancy is a session that can push but never read back. Login and refresh both
   * report the session's own tenancy, and a refresh can never report another one.
   */
  it("reports the session's own tenancy on login and on refresh", async () => {
    const orgA = await createSession(db, {
      userId: "user_1",
      orgId: "org_a",
      workspaceId: "ws_1",
    });
    const orgB = await createSession(db, {
      userId: "user_2",
      orgId: "org_b",
      workspaceId: "ws_2",
    });

    expect(orgA).toMatchObject({ orgId: "org_a", workspaceId: "ws_1" });
    expect(orgB).toMatchObject({ orgId: "org_b", workspaceId: "ws_2" });

    const refreshedA = await refreshSession(db, orgA.refreshToken);
    const refreshedB = await refreshSession(db, orgB.refreshToken);
    expect(refreshedA).toMatchObject({ orgId: "org_a", workspaceId: "ws_1" });
    expect(refreshedB).toMatchObject({ orgId: "org_b", workspaceId: "ws_2" });

    // And what the pair reports is what the bearer actually resolves to.
    const resolved = await resolveAccessToken(db, refreshedA!.accessToken);
    expect(resolved).toMatchObject({ orgId: "org_a", workspaceId: "ws_1" });
  });

  /** A user session must not gain service-token powers, or the distinction is cosmetic. */
  it("leaves ordinary sessions refreshable and unaffected", async () => {
    const session = await createSession(db, {
      userId: "user_1",
      orgId: "org_a",
      workspaceId: "ws_1",
    } as any);

    const refreshed = await refreshSession(db, session.refreshToken);
    expect(refreshed).not.toBeNull();
    expect(await listServiceTokens(db, "org_a")).toEqual([]);
  });
  /**
   * Through `requireAuth`, not `resolveAccessToken`.
   *
   * This is the test that was missing when service tokens first shipped. The middleware
   * gated on `ACCESS_PREFIX` before ever calling the resolver, so every `rth_st_` token
   * 401'd — while the whole resolver-level suite passed, because it never went through
   * the middleware. A credential is only real if the request path accepts it.
   */
  describe("through the real auth middleware", () => {
    function app() {
      const hono = new Hono<HistoryEnv>();
      hono.use("*", createRequireAuth({ database: () => db }));
      hono.get("/read", requireScope("rth:read"), (c) =>
        c.json({ ok: true, org: getAuth(c).orgId }),
      );
      hono.get("/sync", requireScope("rth:sync"), (c) => c.json({ ok: true }));
      return hono;
    }

    it("accepts a service token on a scoped route", async () => {
      const issued = await createServiceToken(db, minter, {
        label: "agent",
        scopes: ["rth:read"],
      });

      const res = await app().request("/read", {
        headers: { Authorization: `Bearer ${issued.token}` },
      });

      expect(res.status).toBe(200);
      expect(await res.json()).toMatchObject({ org: "org_a" });
    });

    it("still enforces scope through the middleware", async () => {
      const issued = await createServiceToken(db, minter, {
        label: "agent",
        scopes: ["rth:read"],
      });

      const res = await app().request("/sync", {
        headers: { Authorization: `Bearer ${issued.token}` },
      });

      expect(res.status).toBe(403);
    });

    it("rejects a revoked service token at the middleware", async () => {
      const issued = await createServiceToken(db, minter, { label: "agent" });
      await revokeServiceToken(db, "org_a", issued.id);

      const res = await app().request("/read", {
        headers: { Authorization: `Bearer ${issued.token}` },
      });

      expect(res.status).toBe(401);
    });

    it("rejects a token that merely looks like one", async () => {
      const res = await app().request("/read", {
        headers: { Authorization: "Bearer rth_st_not_a_real_token" },
      });

      expect(res.status).toBe(401);
    });
  });
});

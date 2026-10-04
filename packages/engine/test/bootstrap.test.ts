import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import {
  SERVICE_PREFIX,
  SERVICE_TTL_DEFAULT_DAYS,
  SERVICE_TTL_MAX_DAYS,
  ServiceTokenError,
  bootstrapServiceToken,
  hashToken,
  refreshSession,
  resolveAccessToken,
} from "../src/index.js";
import { createTestDatabase, type TestDatabase } from "./support/database.js";

let database: TestDatabase;

beforeAll(async () => {
  database = await createTestDatabase();
});
beforeEach(async () => {
  await database.exec("DELETE FROM sessions.auth_sessions");
});
afterAll(async () => {
  await database?.close();
});

const valid = { orgId: "org_a", workspaceId: "ws_1", label: "self-host agent" };

const tokenRows = () =>
  database.query<Record<string, unknown>>(
    "SELECT * FROM sessions.auth_sessions",
  );

describe("bootstrapServiceToken", () => {
  describe("validation", () => {
    it.each([
      ["orgId", { orgId: "" }],
      ["orgId", { orgId: "org a" }],
      ["orgId", { orgId: "-org" }],
      ["orgId", { orgId: "o".repeat(129) }],
      ["workspaceId", { workspaceId: "ws/1" }],
      ["workspaceId", { workspaceId: "ws'1" }],
      ["userId", { userId: "" }],
      ["userId", { userId: "user;drop" }],
    ])("rejects a malformed %s (%j)", async (field, override) => {
      await expect(
        bootstrapServiceToken(database.db, { ...valid, ...override }),
      ).rejects.toThrow(
        new ServiceTokenError(
          `${field} must be 1..128 characters of letters, digits and . _ : @ -`,
        ),
      );
      expect((await tokenRows()).rows).toEqual([]);
    });

    it("accepts every identifier character it documents, up to 128", async () => {
      const issued = await bootstrapServiceToken(database.db, {
        ...valid,
        orgId: "Org.1_a:b@c-d",
        workspaceId: "w".repeat(128),
        userId: "ops@example.test",
      });
      expect(issued).toMatchObject({
        orgId: "Org.1_a:b@c-d",
        workspaceId: "w".repeat(128),
      });
    });

    it.each(["", "   ", "x".repeat(121)])("rejects label %j", async (label) => {
      await expect(
        bootstrapServiceToken(database.db, { ...valid, label }),
      ).rejects.toThrow(
        new ServiceTokenError("label must be 1..120 characters"),
      );
    });

    it("trims the label and accepts 120 characters", async () => {
      const issued = await bootstrapServiceToken(database.db, {
        ...valid,
        label: `  ${"x".repeat(120)}  `,
      });
      expect(issued.label).toBe("x".repeat(120));
    });

    it("rejects scopes outside rth:sync and rth:read", async () => {
      await expect(
        bootstrapServiceToken(database.db, {
          ...valid,
          scopes: ["rth:read", "rth:admin", "*"],
        }),
      ).rejects.toThrow(new ServiceTokenError("unknown scopes: rth:admin, *"));
      expect((await tokenRows()).rows).toEqual([]);
    });

    it.each([0, -1, 1.5, SERVICE_TTL_MAX_DAYS + 1, Number.NaN])(
      "rejects expiresInDays %s",
      async (expiresInDays) => {
        await expect(
          bootstrapServiceToken(database.db, { ...valid, expiresInDays }),
        ).rejects.toThrow(
          new ServiceTokenError(
            `expiresInDays must be an integer from 1 to ${SERVICE_TTL_MAX_DAYS}`,
          ),
        );
      },
    );

    it.each([1, SERVICE_TTL_MAX_DAYS])(
      "accepts expiresInDays %s exactly",
      async (expiresInDays) => {
        const before = Date.now();
        const issued = await bootstrapServiceToken(database.db, {
          ...valid,
          expiresInDays,
        });
        const days =
          (new Date(issued.expiresAt).getTime() - before) / 86_400_000;
        expect(days).toBeGreaterThanOrEqual(expiresInDays);
        expect(days).toBeLessThan(expiresInDays + 1 / 24);
      },
    );

    it(`defaults to ${SERVICE_TTL_DEFAULT_DAYS} days, both scopes and the operator subject`, async () => {
      const before = Date.now();
      const issued = await bootstrapServiceToken(database.db, valid);
      const days = (new Date(issued.expiresAt).getTime() - before) / 86_400_000;
      expect(Math.round(days)).toBe(SERVICE_TTL_DEFAULT_DAYS);
      expect(issued.scopes).toEqual(["rth:sync", "rth:read"]);
      const [row] = (await tokenRows()).rows;
      expect(row).toMatchObject({ user_id: "operator" });
    });
  });

  describe("the secret", () => {
    it("is returned once and only its sha256 is stored", async () => {
      const issued = await bootstrapServiceToken(database.db, valid);

      expect(issued.token.startsWith(SERVICE_PREFIX)).toBe(true);
      const { rows } = await tokenRows();
      expect(rows).toHaveLength(1);
      expect(rows[0]).toMatchObject({
        id: issued.id,
        subject_type: "service",
        org_id: "org_a",
        workspace_id: "ws_1",
        label: "self-host agent",
        access_token_hash: await hashToken(issued.token),
      });
      expect(rows[0]!.access_token_hash).toMatch(/^[0-9a-f]{64}$/);
      // No column holds the secret or any part of it.
      const stored = JSON.stringify(rows);
      expect(stored).not.toContain(issued.token);
      expect(stored).not.toContain(issued.token.slice(SERVICE_PREFIX.length));
    });

    it("is different on every call", async () => {
      const first = await bootstrapServiceToken(database.db, valid);
      const second = await bootstrapServiceToken(database.db, valid);
      expect(first.token).not.toBe(second.token);
      expect(first.id).not.toBe(second.id);
    });
  });

  describe("scopes", () => {
    it.each([[["rth:read"]], [["rth:sync"]], [["rth:read", "rth:sync"]]])(
      "issues exactly the requested subset %j",
      async (scopes) => {
        const issued = await bootstrapServiceToken(database.db, {
          ...valid,
          scopes,
        });
        expect(issued.scopes).toEqual(scopes);
        const resolved = await resolveAccessToken(database.db, issued.token);
        expect(resolved?.scopes).toEqual(scopes);
      },
    );

    it("treats an empty scope list as the default", async () => {
      const issued = await bootstrapServiceToken(database.db, {
        ...valid,
        scopes: [],
      });
      expect(issued.scopes).toEqual(["rth:sync", "rth:read"]);
    });
  });

  it("resolves to the stored tenancy and cannot be refreshed", async () => {
    const issued = await bootstrapServiceToken(database.db, {
      ...valid,
      userId: "deploy-bot",
    });
    expect(issued).toMatchObject({ orgId: "org_a", workspaceId: "ws_1" });

    const resolved = await resolveAccessToken(database.db, issued.token);
    expect(resolved).toMatchObject({
      id: issued.id,
      orgId: "org_a",
      workspaceId: "ws_1",
      userId: "deploy-bot",
      subjectType: "service",
      revokedAt: null,
    });
    expect(await refreshSession(database.db, issued.token)).toBeNull();
  });
});

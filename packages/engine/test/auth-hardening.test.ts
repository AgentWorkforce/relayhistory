/**
 * Credential and error-reporting boundaries of the engine's own auth: who may revoke a
 * service token, how long a minted one may live, what an empty scope list means, what
 * reaches the logs when verification fails, and how a recall request names its
 * workspace.
 */
import { Hono } from "hono";
import { HTTPException } from "hono/http-exception";
import {
  afterAll,
  afterEach,
  beforeAll,
  beforeEach,
  describe,
  expect,
  it,
  vi,
} from "vitest";
import {
  ServiceTokenError,
  attestWorkspace,
  bootstrapServiceToken,
  createHistoryEngine,
  createServiceToken,
  createSession,
  refreshSession,
  resolveAccessToken,
  revokeServiceToken,
  type AuthContext,
  type HistoryEngineDeps,
  type HistoryEnv,
} from "../src/index.js";
import { createTestDatabase, type TestDatabase } from "./support/database.js";

let database: TestDatabase;

beforeAll(async () => {
  database = await createTestDatabase();
});
beforeEach(async () => {
  await database.exec("DELETE FROM sessions.auth_sessions");
});
afterEach(() => {
  vi.restoreAllMocks();
});
afterAll(async () => {
  await database?.close();
});

function engine(deps: Partial<HistoryEngineDeps> = {}) {
  return createHistoryEngine({ database: () => database.db, ...deps });
}

const bearer = (token: string) => ({ Authorization: `Bearer ${token}` });

function serviceToken(scopes?: string[], expiresInDays?: number) {
  return bootstrapServiceToken(database.db, {
    orgId: "org_a",
    workspaceId: "ws_a",
    label: `token ${scopes?.join("+") ?? "default"}`,
    scopes,
    expiresInDays,
  });
}

function mint(token: string, body: Record<string, unknown>) {
  return engine().request("/v1/auth/service-tokens", {
    method: "POST",
    headers: { ...bearer(token), "content-type": "application/json" },
    body: JSON.stringify({ label: "child", ...body }),
  });
}

describe("revoking a service token", () => {
  it("is refused when the target holds a scope the caller does not", async () => {
    const reader = await serviceToken(["rth:read"]);
    const uploader = await serviceToken(["rth:read", "rth:sync"]);

    const res = await engine().request(
      `/v1/auth/service-tokens/${uploader.id}`,
      { method: "DELETE", headers: bearer(reader.token) },
    );

    expect(res.status).toBe(404);
    expect(await res.json()).toMatchObject({
      error: { code: "not_found", message: "no such service token" },
    });
    expect(
      await resolveAccessToken(database.db, uploader.token),
    ).not.toBeNull();
  });

  it("is allowed for a target within the caller's scopes", async () => {
    const reader = await serviceToken(["rth:read"]);
    const otherReader = await serviceToken(["rth:read"]);
    const uploader = await serviceToken(["rth:read", "rth:sync"]);
    const app = engine();

    const narrower = await app.request(
      `/v1/auth/service-tokens/${otherReader.id}`,
      { method: "DELETE", headers: bearer(uploader.token) },
    );
    expect(narrower.status).toBe(200);
    const equal = await app.request(`/v1/auth/service-tokens/${reader.id}`, {
      method: "DELETE",
      headers: bearer(reader.token),
    });
    expect(equal.status).toBe(200);
    expect(await resolveAccessToken(database.db, reader.token)).toBeNull();
  });

  it("keeps the four-argument call unrestricted and enforces withinScopes in SQL", async () => {
    const uploader = await serviceToken(["rth:read", "rth:sync"]);
    const other = await serviceToken(["rth:read", "rth:sync"]);

    expect(
      await revokeServiceToken(database.db, "org_a", uploader.id, undefined, {
        withinScopes: ["rth:read"],
      }),
    ).toBe(false);
    expect(
      await revokeServiceToken(database.db, "org_a", uploader.id, undefined, {
        withinScopes: ["rth:sync", "rth:read"],
      }),
    ).toBe(true);
    expect(
      await revokeServiceToken(database.db, "org_a", other.id, "rotated"),
    ).toBe(true);
    const { rows } = await database.query(
      "SELECT revoked_reason FROM sessions.auth_sessions WHERE id = $1",
      [other.id],
    );
    expect(rows).toEqual([{ revoked_reason: "rotated" }]);
  });
});

describe("minting through a service token", () => {
  it("cannot outlive the minting token", async () => {
    const parent = await serviceToken(["rth:read", "rth:sync"], 2);

    const res = await mint(parent.token, { expiresInDays: 365 });

    expect(res.status).toBe(201);
    const child = (await res.json()) as { expiresAt: string; token: string };
    expect(new Date(child.expiresAt).getTime()).toBeLessThanOrEqual(
      new Date(parent.expiresAt).getTime(),
    );
    expect(await resolveAccessToken(database.db, child.token)).not.toBeNull();
  });

  it("keeps a shorter requested lifetime", async () => {
    const parent = await serviceToken(["rth:read"], 30);
    const before = Date.now();

    const child = (await (
      await mint(parent.token, { expiresInDays: 1 })
    ).json()) as { expiresAt: string };

    const days = (new Date(child.expiresAt).getTime() - before) / 86_400_000;
    expect(days).toBeGreaterThanOrEqual(1);
    expect(days).toBeLessThan(1 + 1 / 24);
  });

  it("leaves a user session's mint uncapped by the session's own expiry", async () => {
    const session = await createSession(database.db, {
      userId: "user_1",
      orgId: "org_a",
      workspaceId: "ws_a",
    });
    const before = Date.now();

    const child = (await (
      await mint(session.accessToken, { expiresInDays: 30 })
    ).json()) as { expiresAt: string };

    expect(
      Math.round((new Date(child.expiresAt).getTime() - before) / 86_400_000),
    ).toBe(30);
  });

  it("caps the library call at notAfter", async () => {
    const notAfter = new Date(Date.now() + 3_600_000);
    const issued = await createServiceToken(
      database.db,
      {
        userId: "u",
        orgId: "org_a",
        workspaceId: "ws_a",
        scopes: ["rth:read"],
      },
      { label: "capped", expiresInDays: 90, notAfter },
    );
    expect(issued.expiresAt).toBe(notAfter.toISOString());
  });
});

describe("an empty scope list", () => {
  it("is a 400 at the route, never the minter's full authority", async () => {
    const parent = await serviceToken(["rth:read", "rth:sync"]);

    const res = await mint(parent.token, { scopes: [] });

    expect(res.status).toBe(400);
    expect(await res.json()).toMatchObject({
      error: { code: "bad_request", message: "scopes must not be empty" },
    });
    const { rows } = await database.query(
      "SELECT count(*)::int AS n FROM sessions.auth_sessions",
    );
    expect(rows).toEqual([{ n: 1 }]);
  });

  it("is refused by createServiceToken rather than read as omitted", async () => {
    await expect(
      createServiceToken(
        database.db,
        {
          userId: "u",
          orgId: "org_a",
          workspaceId: "ws_a",
          scopes: ["rth:read", "rth:sync"],
        },
        { label: "empty", scopes: [] },
      ),
    ).rejects.toBeInstanceOf(ServiceTokenError);
  });
});

describe("requested scopes", () => {
  it("rejects unknown scope strings instead of dropping them", async () => {
    const parent = await serviceToken(["rth:read", "rth:sync"]);

    const res = await mint(parent.token, {
      scopes: ["rth:sync", "typo", "rth:admin"],
    });

    expect(res.status).toBe(400);
    expect(await res.json()).toMatchObject({
      error: {
        code: "bad_request",
        message: "unknown scopes: typo, rth:admin",
      },
    });
    const { rows } = await database.query(
      "SELECT count(*)::int AS n FROM sessions.auth_sessions",
    );
    expect(rows).toEqual([{ n: 1 }]);
  });

  it("still narrows known scopes the minter does not hold", async () => {
    const parent = await serviceToken(["rth:read"]);

    const res = await mint(parent.token, { scopes: ["rth:read", "rth:sync"] });

    expect(res.status).toBe(201);
    expect(await res.json()).toMatchObject({ scopes: ["rth:read"] });
  });
});

describe("error reporting", () => {
  const SECRET = "rth_secret_value_in_error";

  it("reports a failed verifier with only its name and code", async () => {
    const reportError = vi.fn();
    const log = vi.spyOn(console, "error").mockImplementation(() => {});
    const res = await engine({
      reportError,
      verifyBearer: async (token) => {
        throw Object.assign(new TypeError(`bad token ${token}`), {
          code: "ERR_JWKS",
        });
      },
    }).request("/v1/sessions", { headers: bearer(SECRET) });

    expect(res.status).toBe(401);
    expect(reportError).toHaveBeenCalledTimes(1);
    expect(reportError.mock.calls[0]![0]).toEqual({
      name: "TypeError",
      code: "ERR_JWKS",
    });
    expect(JSON.stringify(reportError.mock.calls[0]![0])).not.toContain(SECRET);
    expect(log).not.toHaveBeenCalled();
  });

  it("logs only the name and code of a failed verifier by default", async () => {
    const log = vi.spyOn(console, "error").mockImplementation(() => {});
    await engine({
      verifyBearer: async (token) => {
        throw new Error(`bad token ${token}`);
      },
    }).request("/v1/sessions", { headers: bearer(SECRET) });

    expect(log).toHaveBeenCalledTimes(1);
    expect(JSON.stringify(log.mock.calls)).not.toContain(SECRET);
    expect(log.mock.calls[0]).toEqual([
      "[relayhistory] request failed",
      { name: "Error", code: undefined },
    ]);
  });

  it("hands an unhandled route error to reportError and answers 500", async () => {
    const reportError = vi.fn();
    const failing = new Hono<HistoryEnv>();
    const thrown = new Error(`SELECT * FROM t WHERE token = '${SECRET}'`);
    failing.get("/explode", () => {
      throw thrown;
    });

    const res = await engine({ reportError, publicRoutes: [failing] }).request(
      "/v1/explode",
    );

    expect(res.status).toBe(500);
    expect(await res.text()).not.toContain(SECRET);
    expect(reportError).toHaveBeenCalledWith(thrown, expect.anything());
  });

  it("logs only the name and code of an unhandled route error by default", async () => {
    const log = vi.spyOn(console, "error").mockImplementation(() => {});
    const failing = new Hono<HistoryEnv>();
    failing.get("/explode", () => {
      throw Object.assign(new Error(`row ${SECRET}`), { code: "23505" });
    });

    await engine({ publicRoutes: [failing] }).request("/v1/explode");

    expect(JSON.stringify(log.mock.calls)).not.toContain(SECRET);
    expect(log.mock.calls).toEqual([
      ["[relayhistory] request failed", { name: "Error", code: "23505" }],
    ]);
  });

  it("answers the JSON 500 when the host reporter itself throws", async () => {
    const log = vi.spyOn(console, "error").mockImplementation(() => {});
    const failing = new Hono<HistoryEnv>();
    failing.get("/explode", () => {
      throw Object.assign(new Error(`row ${SECRET}`), { code: "XX000" });
    });

    const res = await engine({
      reportError: () => {
        throw new Error(`sink rejected ${SECRET}`);
      },
      publicRoutes: [failing],
    }).request("/v1/explode", { headers: { "X-Correlation-Id": "trace-r" } });

    expect(res.status).toBe(500);
    expect(await res.json()).toEqual({
      error: { code: "internal_error", message: "Internal server error" },
      correlationId: "trace-r",
    });
    expect(JSON.stringify(log.mock.calls)).not.toContain(SECRET);
    expect(log.mock.calls).toContainEqual([
      "[relayhistory] request failed",
      { name: "Error", code: "XX000" },
    ]);
  });

  it("answers a thrown HTTPException with its own response", async () => {
    const reportError = vi.fn();
    const failing = new Hono<HistoryEnv>();
    failing.get("/teapot", () => {
      throw new HTTPException(418, { message: "short and stout" });
    });

    const res = await engine({ reportError, publicRoutes: [failing] }).request(
      "/v1/teapot",
    );

    expect(res.status).toBe(418);
    expect(await res.text()).toBe("short and stout");
    expect(reportError).not.toHaveBeenCalled();
  });
});

describe("token usage telemetry", () => {
  function failUsageWrites() {
    return vi.spyOn(database.db, "update").mockImplementation(
      () =>
        ({
          set: () => ({
            where: () =>
              Promise.reject(
                Object.assign(new Error("read-only transaction"), {
                  code: "25006",
                }),
              ),
          }),
        }) as never,
    );
  }

  it("does not reject a valid token when the last-used write fails", async () => {
    const issued = await serviceToken(["rth:read"]);
    failUsageWrites();
    const failures: unknown[] = [];

    const resolved = await resolveAccessToken(database.db, issued.token, {
      onUsageError: (error) => failures.push(error),
    });

    expect(resolved?.id).toBe(issued.id);
    expect(failures).toHaveLength(1);
  });

  it("authenticates the request when the host reporter itself throws", async () => {
    const issued = await serviceToken(["rth:read"]);
    failUsageWrites();
    const log = vi.spyOn(console, "error").mockImplementation(() => {});

    const res = await engine({
      reportError: () => {
        throw new Error("telemetry sink down");
      },
    }).request("/v1/sessions", { headers: bearer(issued.token) });

    expect(res.status).toBe(200);
    // The usage failure still reaches the default log, sanitized.
    expect(log.mock.calls).toContainEqual([
      "[relayhistory] request failed",
      { name: "Error", code: "25006" },
    ]);
  });

  it("authenticates the request and reports the write failure sanitized", async () => {
    const issued = await serviceToken(["rth:read"]);
    failUsageWrites();
    const reportError = vi.fn();

    const res = await engine({ reportError }).request("/v1/sessions", {
      headers: bearer(issued.token),
    });

    expect(res.status).toBe(200);
    expect(reportError).toHaveBeenCalledWith(
      { name: "Error", code: "25006" },
      expect.anything(),
    );
  });
});

/** Defers every query built from `builder` until `ready` settles. */
function deferred<T extends object>(builder: T, ready: Promise<void>): T {
  return new Proxy(builder, {
    get(target, property) {
      const value = Reflect.get(target, property, target);
      if (property === "then")
        return (resolve: never, reject: never) =>
          ready.then(() =>
            (target as PromiseLike<unknown>).then(resolve, reject),
          );
      if (typeof value !== "function") return value;
      return (...args: unknown[]) => {
        const result = value.apply(target, args);
        return result && typeof result === "object"
          ? deferred(result, ready)
          : result;
      };
    },
  });
}

describe("refreshing a session", () => {
  it("rotates once when the same refresh token is presented concurrently", async () => {
    const session = await createSession(database.db, {
      userId: "user_1",
      orgId: "org_a",
      workspaceId: "ws_a",
    });
    // Both refreshes read the session before either rotates it: each rotation waits
    // until the other has been issued.
    const update = database.db.update.bind(database.db);
    let issue!: () => void;
    const bothIssued = new Promise<void>((resolve) => (issue = resolve));
    let updates = 0;
    const spy = vi.spyOn(database.db, "update").mockImplementation(((
      table: Parameters<typeof update>[0],
    ) => {
      if (++updates === 2) issue();
      return deferred(update(table), bothIssued);
    }) as typeof update);

    const results = await Promise.all([
      refreshSession(database.db, session.refreshToken),
      refreshSession(database.db, session.refreshToken),
    ]);
    spy.mockRestore();
    expect(updates).toBe(2);

    const issued = results.filter((pair) => pair !== null);
    expect(issued).toHaveLength(1);
    expect(
      await resolveAccessToken(database.db, issued[0]!.accessToken),
    ).not.toBeNull();
    expect(
      await refreshSession(database.db, issued[0]!.refreshToken),
    ).not.toBeNull();
  });
});

describe("workspace recall scope", () => {
  function hostEngine(workspaceId: string) {
    const auth: AuthContext = {
      userId: "u",
      orgId: "org_ws",
      workspaceId,
      tokenSubject: "u",
      scopes: ["rth:read"],
      claims: {},
    };
    return engine({ verifyBearer: async () => auth });
  }

  it("compares the authenticated workspace exactly, without trimming it", async () => {
    const app = hostEngine(" ws_a ");

    const trimmed = await app.request("/v1/sessions?workspace=ws_a", {
      headers: bearer("idp"),
    });
    expect(trimmed.status).toBe(403);
    expect(await trimmed.json()).toMatchObject({
      error: { message: "workspace does not match the authenticated session" },
    });
  });

  it.each([
    ["surrounding whitespace", " ws_a ", "%20ws_a%20"],
    ["a trailing tab", "ws_a\t", "ws_a%09"],
    ["an embedded newline", "ws\na", "ws%0Aa"],
    ["a character outside Latin-1", "ws_\u2603", "ws_%E2%98%83"],
  ])(
    "refuses a workspace scope it cannot attest: %s",
    async (_, workspaceId, query) => {
      const app = hostEngine(workspaceId);

      const res = await app.request(`/v1/sessions?workspace=${query}`, {
        headers: bearer("idp"),
      });

      expect(res.status).toBe(403);
      expect(await res.json()).toMatchObject({
        error: {
          code: "forbidden",
          message: "authenticated workspace cannot be attested",
        },
      });
      expect(res.headers.get("X-Relayhistory-Workspace-Id")).toBeNull();
      // Organization-wide recall needs no attestation and is unchanged.
      const orgWide = await app.request("/v1/sessions", {
        headers: bearer("idp"),
      });
      expect(orgWide.status).toBe(200);
    },
  );

  it("attests an ordinary workspace with interior spaces exactly", async () => {
    const res = await hostEngine("team a").request(
      "/v1/sessions?workspace=team%20a",
      { headers: bearer("idp") },
    );
    expect(res.status).toBe(200);
    expect(res.headers.get("X-Relayhistory-Workspace-Id")).toBe("team a");
  });

  it("refuses a blank authenticated workspace", async () => {
    const res = await hostEngine("   ").request(
      "/v1/sessions?workspace=%20%20%20",
      { headers: bearer("idp") },
    );
    expect(res.status).toBe(400);
  });

  it.each(["workspace_id", "workspaceId"])(
    "rejects the %s selector instead of reading organization-wide",
    async (selector) => {
      for (const path of [
        "/v1/sessions",
        "/v1/events",
        "/v1/sessions/s/events",
      ]) {
        const res = await hostEngine("ws_a").request(
          `${path}?${selector}=ws_a`,
          { headers: bearer("idp") },
        );
        expect(res.status).toBe(400);
        expect(await res.json()).toMatchObject({
          error: {
            code: "bad_request",
            message: `unsupported selector ${selector}; use workspace`,
          },
        });
      }
    },
  );

  it.each(["org", "org_id", "orgId"])(
    "rejects the %s selector instead of ignoring it",
    async (selector) => {
      for (const path of [
        "/v1/sessions",
        "/v1/events",
        "/v1/sessions/s/events",
      ]) {
        const res = await hostEngine("ws_a").request(
          `${path}?${selector}=org_other`,
          { headers: bearer("idp") },
        );
        expect(res.status).toBe(400);
        expect(await res.json()).toMatchObject({
          error: {
            code: "bad_request",
            message: `unsupported selector ${selector}`,
          },
        });
      }
    },
  );
});

describe("session catalog attestation", () => {
  function hostEngine(workspaceId: string | undefined) {
    return engine({
      verifyBearer: async () => ({
        userId: "u",
        orgId: "org_catalog",
        workspaceId,
        tokenSubject: "u",
        scopes: ["rth:read"],
        claims: {},
      }),
    });
  }
  const catalog = (workspaceId: string | undefined) =>
    hostEngine(workspaceId).request(
      "/v1/sessions/session-x/catalog?source=claude",
      { headers: bearer("idp") },
    );

  it.each([
    ["surrounding whitespace", " ws_a "],
    ["a trailing tab", "ws_a\t"],
    ["an embedded newline", "ws\na"],
    ["a character outside Latin-1", "ws_\u2603"],
  ])("refuses a workspace it cannot attest: %s", async (_, workspaceId) => {
    const res = await catalog(workspaceId);

    expect(res.status).toBe(403);
    expect(await res.json()).toMatchObject({
      error: {
        code: "forbidden",
        message: "authenticated workspace cannot be attested",
      },
    });
    expect(res.headers.get("X-Relayhistory-Workspace-Id")).toBeNull();
  });

  it("attests an ordinary workspace exactly", async () => {
    const res = await catalog("team a");
    expect(res.status).toBe(404);
    expect(res.headers.get("X-Relayhistory-Workspace-Id")).toBe("team a");
  });

  it("attests nothing for an identity without a workspace", async () => {
    const res = await catalog(undefined);
    expect(res.status).toBe(404);
    expect(res.headers.get("X-Relayhistory-Workspace-Id")).toBeNull();
  });
});

describe("attestWorkspace", () => {
  it("refuses a workspace a header cannot carry exactly", async () => {
    const app = new Hono();
    app.get("/", (c) => {
      attestWorkspace(c, " ws_a ");
      return c.text("attested");
    });
    app.onError((error, c) => c.text(error.message, 500));

    const res = await app.request("/");

    expect(res.status).toBe(500);
    expect(await res.text()).toBe("workspace cannot be attested");
    expect(res.headers.get("X-Relayhistory-Workspace-Id")).toBeNull();
  });
});

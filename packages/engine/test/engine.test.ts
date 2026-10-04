import { Hono, type MiddlewareHandler } from "hono";
import {
  afterAll,
  afterEach,
  beforeAll,
  describe,
  expect,
  it,
  vi,
} from "vitest";
import {
  AuthError,
  bootstrapServiceToken,
  createHistoryEngine,
  createSession,
  deliveryAccount,
  getAuth,
  requireScope,
  revokeServiceToken,
  type AuthContext,
  type HistoryEngineDeps,
  type HistoryEnv,
  type HistoryExportBatch,
  type HistoryExportRecord,
} from "../src/index.js";
import { createTestDatabase, type TestDatabase } from "./support/database.js";

let database: TestDatabase;

beforeAll(async () => {
  database = await createTestDatabase();
});
afterAll(async () => {
  await database?.close();
});
afterEach(() => {
  vi.restoreAllMocks();
});

/** A host route behind auth that reports the tenant the engine resolved. */
function whoami() {
  const routes = new Hono<HistoryEnv>();
  routes.get("/whoami", (c) => c.json({ auth: getAuth(c) }));
  routes.get("/whoami/sync", requireScope("rth:sync"), (c) =>
    c.json({ ok: true }),
  );
  return routes;
}

function engine(deps: Partial<HistoryEngineDeps> = {}) {
  return createHistoryEngine({
    database: () => database.db,
    routes: [whoami()],
    ...deps,
  });
}

const bearer = (token: string) => ({ Authorization: `Bearer ${token}` });

async function serviceToken(
  orgId: string,
  workspaceId: string,
  scopes?: string[],
) {
  return bootstrapServiceToken(database.db, {
    orgId,
    workspaceId,
    label: `${orgId}/${workspaceId}`,
    scopes,
  });
}

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

describe("createHistoryEngine", () => {
  describe("application shell", () => {
    it("answers /health without auth", async () => {
      const res = await engine().request("/health");
      expect(res.status).toBe(200);
      expect(await res.json()).toEqual({ ok: true, service: "relayhistory" });
    });

    it("stamps a fresh request id and defaults the correlation id to it", async () => {
      const app = engine();
      const first = await app.request("/health");
      const second = await app.request("/health");
      const id = first.headers.get("X-Request-Id");
      expect(id).toMatch(UUID);
      expect(first.headers.get("X-Correlation-Id")).toBe(id);
      expect(second.headers.get("X-Request-Id")).not.toBe(id);
    });

    it("carries a supplied correlation id, trimmed", async () => {
      const res = await engine().request("/health", {
        headers: { "X-Correlation-Id": "  trace-123  " },
      });
      expect(res.headers.get("X-Correlation-Id")).toBe("trace-123");
      expect(res.headers.get("X-Request-Id")).toMatch(UUID);
    });

    it("answers an unknown route with a JSON 404 carrying the correlation id", async () => {
      const res = await engine().request("/nope?x=1", {
        headers: { "X-Correlation-Id": "trace-404" },
      });
      expect(res.status).toBe(404);
      expect(await res.json()).toEqual({
        error: { code: "not_found", message: "No route for GET /nope" },
        correlationId: "trace-404",
      });
      expect(res.headers.get("X-Correlation-Id")).toBe("trace-404");
      expect(res.headers.get("X-Request-Id")).toMatch(UUID);
    });

    it("authenticates before routing under /v1, so unknown paths do not enumerate", async () => {
      const app = engine();
      expect((await app.request("/v1/nope")).status).toBe(401);
      const { token } = await serviceToken("org_404", "ws_404");
      const res = await app.request("/v1/nope", { headers: bearer(token) });
      expect(res.status).toBe(404);
      expect(await res.json()).toMatchObject({
        error: { code: "not_found", message: "No route for GET /v1/nope" },
      });
    });

    it("sets secure headers and answers CORS preflight without auth", async () => {
      const app = engine();
      const health = await app.request("/health");
      expect(health.headers.get("X-Content-Type-Options")).toBe("nosniff");
      expect(health.headers.get("X-Frame-Options")).toBe("SAMEORIGIN");

      const preflight = await app.request("/v1/sessions", {
        method: "OPTIONS",
        headers: {
          Origin: "https://example.test",
          "Access-Control-Request-Method": "GET",
          "Access-Control-Request-Headers": "Authorization",
        },
      });
      expect(preflight.status).toBe(204);
      expect(preflight.headers.get("Access-Control-Allow-Origin")).toBe("*");
      expect(preflight.headers.get("X-Request-Id")).toMatch(UUID);
    });

    it("runs host middleware in order, after the ids and before CORS and auth", async () => {
      const seen: string[] = [];
      const middleware =
        (name: string): MiddlewareHandler<HistoryEnv> =>
        async (c, next) => {
          seen.push(
            `${name} ${c.req.method} ${c.get("requestId") ? "id" : "no-id"} ${c.get("correlationId")}`,
          );
          await next();
          seen.push(`${name} after ${c.res.status}`);
        };
      const app = engine({ middleware: [middleware("a"), middleware("b")] });
      const headers = { "X-Correlation-Id": "trace-mw" };

      await app.request("/v1/sessions", { headers });
      await app.request("/v1/sessions", {
        method: "OPTIONS",
        headers: {
          ...headers,
          Origin: "https://example.test",
          "Access-Control-Request-Method": "GET",
        },
      });

      expect(seen).toEqual([
        "a GET id trace-mw",
        "b GET id trace-mw",
        "b after 401",
        "a after 401",
        "a OPTIONS id trace-mw",
        "b OPTIONS id trace-mw",
        "b after 204",
        "a after 204",
      ]);
    });

    it("mounts rootRoutes at / and publicRoutes at /v1 without auth", async () => {
      const root = new Hono<HistoryEnv>();
      root.get("/status", (c) => c.json({ root: true }));
      const open = new Hono<HistoryEnv>();
      open.get("/public/ping", (c) =>
        c.json({ auth: c.get("auth") ?? null, id: c.get("correlationId") }),
      );
      const app = engine({ rootRoutes: [root], publicRoutes: [open] });

      expect(await (await app.request("/status")).json()).toEqual({
        root: true,
      });
      const res = await app.request("/v1/public/ping", {
        headers: { "X-Correlation-Id": "trace-public" },
      });
      expect(res.status).toBe(200);
      expect(await res.json()).toEqual({ auth: null, id: "trace-public" });
    });

    it("puts host routes behind auth, after the engine's own", async () => {
      const shadow = new Hono<HistoryEnv>();
      shadow.get("/sessions", (c) => c.json({ host: true }));
      const app = engine({ routes: [whoami(), shadow] });

      const anonymous = await app.request("/v1/whoami");
      expect(anonymous.status).toBe(401);
      expect(await anonymous.json()).toMatchObject({
        error: { code: "missing_authorization" },
      });

      const { token } = await serviceToken("org_host", "ws_host");
      const res = await app.request("/v1/whoami", { headers: bearer(token) });
      expect(res.status).toBe(200);
      expect(await res.json()).toMatchObject({ auth: { orgId: "org_host" } });

      const sessions = await app.request("/v1/sessions", {
        headers: bearer(token),
      });
      expect(await sessions.json()).toMatchObject({ sessions: [] });
    });

    it("answers a thrown error with a 500 that hides its detail", async () => {
      const error = vi.spyOn(console, "error").mockImplementation(() => {});
      const failing = new Hono<HistoryEnv>();
      failing.get("/explode", () => {
        throw new Error("secret connection detail");
      });
      const app = engine({ publicRoutes: [failing] });

      const res = await app.request("/v1/explode", {
        headers: { "X-Correlation-Id": "trace-500" },
      });
      expect(res.status).toBe(500);
      const body = await res.text();
      expect(JSON.parse(body)).toEqual({
        error: { code: "internal_error", message: "Internal server error" },
        correlationId: "trace-500",
      });
      expect(body).not.toContain("secret");
      expect(error).toHaveBeenCalled();
    });

    it("answers a BadRequestError with its message as a 400", async () => {
      const failing = new Hono<HistoryEnv>();
      failing.get("/bad", () => {
        throw Object.assign(new Error("limit must be a number"), {
          name: "BadRequestError",
        });
      });
      const res = await engine({ publicRoutes: [failing] }).request("/v1/bad");
      expect(res.status).toBe(400);
      expect(await res.json()).toMatchObject({
        error: { code: "bad_request", message: "limit must be a number" },
      });
    });
  });

  describe("authentication", () => {
    const whoamiWith = (headers: Record<string, string>, deps = {}) =>
      engine(deps).request("/v1/whoami", { headers });

    it.each([
      ["no Authorization header", {}],
      ["a non-bearer scheme", { Authorization: "Basic dXNlcjpwYXNz" }],
      ["an empty bearer", { Authorization: "Bearer   " }],
    ])("answers %s with 401 missing_authorization", async (_, headers) => {
      const res = await whoamiWith(headers);
      expect(res.status).toBe(401);
      expect(await res.json()).toMatchObject({
        error: {
          code: "missing_authorization",
          message: "missing Authorization header",
        },
      });
    });

    it.each(["rth_st_not_a_real_token", "rth_at_not_a_real_token", "opaque"])(
      "answers an unknown bearer %s with 401 invalid_token",
      async (token) => {
        const res = await whoamiWith(bearer(token));
        expect(res.status).toBe(401);
        expect(await res.json()).toMatchObject({
          error: { code: "invalid_token", message: "invalid or expired token" },
        });
      },
    );

    it("takes the tenant from a bootstrapped service token's stored row", async () => {
      const issued = await serviceToken("org_row", "ws_row", ["rth:read"]);
      const res = await whoamiWith({
        authorization: `bearer ${issued.token}`,
      });
      expect(res.status).toBe(200);
      expect(await res.json()).toEqual({
        auth: {
          userId: "operator",
          orgId: "org_row",
          workspaceId: "ws_row",
          tokenSubject: "operator",
          scopes: ["rth:read"],
          claims: { sessionId: issued.id, subjectType: "service" },
          sessionId: issued.id,
        },
      });
    });

    it("accepts a user session token", async () => {
      const session = await createSession(database.db, {
        userId: "user_cli",
        orgId: "org_cli",
        workspaceId: "ws_cli",
      });
      const res = await whoamiWith(bearer(session.accessToken));
      expect(await res.json()).toMatchObject({
        auth: {
          orgId: "org_cli",
          claims: { subjectType: "cli" },
          scopes: ["rth:sync", "rth:read"],
        },
      });
    });

    it("lets no request select another organization or workspace", async () => {
      const { token } = await serviceToken("org_mine", "ws_mine");
      const app = engine();
      const res = await app.request("/v1/whoami?orgId=org_other", {
        headers: {
          ...bearer(token),
          "X-Org-Id": "org_other",
          "X-Workspace-Id": "ws_other",
          "X-Relayhistory-Workspace-Id": "ws_other",
        },
      });
      expect(await res.json()).toMatchObject({
        auth: { orgId: "org_mine", workspaceId: "ws_mine" },
      });

      const other = await app.request("/v1/sessions?workspace=ws_other", {
        headers: bearer(token),
      });
      expect(other.status).toBe(403);
      expect(await other.json()).toMatchObject({
        error: {
          code: "forbidden",
          message: "workspace does not match the authenticated session",
        },
      });
      const own = await app.request("/v1/sessions?workspace=ws_mine", {
        headers: bearer(token),
      });
      expect(own.status).toBe(200);
    });

    it("enforces scopes per route", async () => {
      const read = await serviceToken("org_scope", "ws_scope", ["rth:read"]);
      const sync = await serviceToken("org_scope", "ws_scope", ["rth:sync"]);
      const app = engine();

      const upload = await app.request("/v1/delivery/batches", {
        method: "POST",
        headers: { ...bearer(read.token), "content-type": "application/json" },
        body: "{}",
      });
      expect(upload.status).toBe(403);
      expect(await upload.json()).toMatchObject({
        error: {
          code: "forbidden",
          message: "missing required scope: rth:sync",
        },
      });
      expect(
        (await app.request("/v1/sessions", { headers: bearer(read.token) }))
          .status,
      ).toBe(200);

      expect(
        (await app.request("/v1/sessions", { headers: bearer(sync.token) }))
          .status,
      ).toBe(403);
      expect(
        (await app.request("/v1/whoami/sync", { headers: bearer(sync.token) }))
          .status,
      ).toBe(200);
    });

    it("rejects a revoked service token", async () => {
      const issued = await serviceToken("org_revoked", "ws_revoked");
      expect((await whoamiWith(bearer(issued.token))).status).toBe(200);
      await revokeServiceToken(database.db, "org_revoked", issued.id);

      const res = await whoamiWith(bearer(issued.token));
      expect(res.status).toBe(401);
      expect(await res.json()).toMatchObject({
        error: { code: "invalid_token" },
      });
    });

    it("rejects an expired service token", async () => {
      const issued = await serviceToken("org_expired", "ws_expired");
      await database.query(
        "UPDATE sessions.auth_sessions SET access_token_expires_at = now() - interval '1 second' WHERE id = $1",
        [issued.id],
      );
      const res = await whoamiWith(bearer(issued.token));
      expect(res.status).toBe(401);
      expect(await res.json()).toMatchObject({
        error: { code: "invalid_token" },
      });
    });

    describe("verifyBearer", () => {
      const hostAuth: AuthContext = {
        userId: "idp_user",
        orgId: "org_idp",
        workspaceId: "ws_idp",
        tokenSubject: "idp_user",
        scopes: ["rth:read"],
        claims: { iss: "idp" },
      };

      it("accepts the tenant it returns, with the request's context", async () => {
        const verifyBearer = vi.fn(async (token: string, c) =>
          token === "idp-token" && c.req.header("X-Host") === "yes"
            ? hostAuth
            : undefined,
        );
        const res = await whoamiWith(
          { ...bearer("idp-token"), "X-Host": "yes" },
          { verifyBearer },
        );
        expect(res.status).toBe(200);
        expect(await res.json()).toEqual({ auth: hostAuth });
        expect(verifyBearer).toHaveBeenCalledTimes(1);
      });

      it("answers undefined with 401 invalid_token", async () => {
        const res = await whoamiWith(bearer("idp-token"), {
          verifyBearer: async () => undefined,
        });
        expect(res.status).toBe(401);
        expect(await res.json()).toMatchObject({
          error: { code: "invalid_token", message: "invalid or expired token" },
        });
      });

      it("answers an AuthError with its code and message", async () => {
        const res = await whoamiWith(bearer("idp-token"), {
          verifyBearer: async () => {
            throw new AuthError("token_expired", "the IdP token has expired");
          },
        });
        expect(res.status).toBe(401);
        expect(await res.json()).toMatchObject({
          error: {
            code: "token_expired",
            message: "the IdP token has expired",
          },
        });
      });

      it("answers any other failure with a generic 401 that hides it", async () => {
        const error = vi.spyOn(console, "error").mockImplementation(() => {});
        const res = await whoamiWith(bearer("idp-token"), {
          verifyBearer: async () => {
            throw new Error("jwks fetch failed for idp-token");
          },
        });
        expect(res.status).toBe(401);
        const body = await res.text();
        expect(JSON.parse(body)).toMatchObject({
          error: { code: "invalid_token", message: "invalid bearer token" },
        });
        expect(body).not.toContain("jwks");
        expect(error).toHaveBeenCalled();
      });

      it("is not consulted for a service-local token that resolves", async () => {
        const verifyBearer = vi.fn(async () => hostAuth);
        const { token } = await serviceToken("org_local", "ws_local");
        const res = await whoamiWith(bearer(token), { verifyBearer });
        expect(await res.json()).toMatchObject({
          auth: { orgId: "org_local" },
        });
        expect(verifyBearer).not.toHaveBeenCalled();
      });

      it("is consulted for a service-local token that does not resolve", async () => {
        const verifyBearer = vi.fn(async () => undefined);
        const res = await whoamiWith(bearer("rth_st_unknown"), {
          verifyBearer,
        });
        expect(res.status).toBe(401);
        expect(verifyBearer).toHaveBeenCalledWith(
          "rth_st_unknown",
          expect.anything(),
        );
      });
    });

    describe("without a database", () => {
      const noDatabase = { database: () => undefined };

      it("answers a service-local token with 503 not_configured", async () => {
        const res = await whoamiWith(bearer("rth_st_anything"), noDatabase);
        expect(res.status).toBe(503);
        expect(await res.json()).toMatchObject({
          error: {
            code: "not_configured",
            message: "DATABASE_URL is required for service-local auth",
          },
        });
      });

      it("answers routes with 503 not_configured after host auth", async () => {
        const app = engine({
          ...noDatabase,
          verifyBearer: async () => ({
            userId: "u",
            orgId: "org_nodb",
            workspaceId: "ws_nodb",
            tokenSubject: "u",
            scopes: ["rth:read", "rth:sync"],
            claims: {},
          }),
        });
        const headers = bearer("idp-token");

        const sessions = await app.request("/v1/sessions", { headers });
        expect(sessions.status).toBe(503);
        expect(await sessions.json()).toMatchObject({
          error: { code: "not_configured" },
        });

        const upload = await app.request("/v1/delivery/batches", {
          method: "POST",
          headers: { ...headers, "content-type": "application/json" },
          body: "{}",
        });
        expect(upload.status).toBe(503);
        expect(await upload.json()).toMatchObject({
          error: { code: "not_configured" },
        });

        const records = await app.request("/v1/delivery/records", { headers });
        expect(records.status).toBe(503);
        expect(await records.json()).toMatchObject({
          error: { code: "not_configured" },
        });
      });
    });
  });

  describe("upload and recall over HTTP", () => {
    function record(
      sessionId: string,
      id: string,
      prompt: string,
      timestampMs: number,
    ): HistoryExportRecord {
      return {
        schema_version: 1,
        origin_id: "origin-e2e",
        record_id: id,
        revision_id: `${id}-r1`,
        revision: 1,
        kind: "history",
        source: "claude",
        session_id: sessionId,
        operation: "upsert",
        payload: {
          source: "claude",
          session_id: sessionId,
          prompt,
          timestamp_ms: timestampMs,
        },
      };
    }

    async function batch(
      auth: Pick<AuthContext, "orgId" | "workspaceId">,
      records: HistoryExportRecord[],
      id: string,
    ): Promise<HistoryExportBatch> {
      return {
        schema_version: 1,
        origin_id: "origin-e2e",
        batch_id: id,
        job_id: "job-e2e",
        generation: 1,
        destination_id: "relayhistory",
        instance_id: "engine-test",
        account_id: await deliveryAccount(auth as AuthContext),
        mapping_version: "relayhistory-delivery-v1",
        records,
      };
    }

    function upload(app: Hono<any>, token: string, value: HistoryExportBatch) {
      return app.request("/v1/delivery/batches", {
        method: "POST",
        headers: { ...bearer(token), "content-type": "application/json" },
        body: JSON.stringify({ protocolVersion: 1, batch: value }),
      });
    }

    async function get(
      app: Hono<any>,
      token: string,
      path: string,
      headers = {},
    ) {
      const res = await app.request(path, {
        headers: { ...bearer(token), ...headers },
      });
      return { status: res.status, body: (await res.json()) as any };
    }

    it("stores a batch through the engine and recalls it only for its tenant", async () => {
      const app = engine();
      const a = await serviceToken("org_e2e_a", "ws_e2e_a");
      const b = await serviceToken("org_e2e_b", "ws_e2e_b");
      // Same organization as A, another workspace: delivered evidence is per workspace.
      const c = await serviceToken("org_e2e_a", "ws_e2e_c");

      const batchA = await batch(
        a,
        [
          record("session-a", "a-1", "alpha needle opening", 1_788_000_000_000),
          record("session-a", "a-2", "alpha follow-up", 1_788_000_001_000),
        ],
        "batch-a",
      );
      const batchB = await batch(
        b,
        [record("session-b", "b-1", "bravo needle opening", 1_788_000_002_000)],
        "batch-b",
      );

      const receiptA = await upload(app, a.token, batchA);
      expect(receiptA.status).toBe(200);
      const storedA = (await receiptA.json()) as { receiptId: string };
      expect(storedA).toMatchObject({
        protocolVersion: 1,
        batchId: "batch-a",
        acceptedRevisionIds: ["a-1-r1", "a-2-r1"],
        acceptanceLevel: "durable",
      });
      expect((await upload(app, b.token, batchB)).status).toBe(200);

      // A replay is idempotent: the same content-addressed receipt.
      const replay = await upload(app, a.token, batchA);
      expect(((await replay.json()) as typeof storedA).receiptId).toBe(
        storedA.receiptId,
      );

      // A tenant cannot deliver into another tenant's account.
      const forged = await upload(
        app,
        a.token,
        await batch(b, [record("session-x", "x-1", "forged", 1)], "forged"),
      );
      expect(forged.status).toBe(403);
      expect(await forged.json()).toMatchObject({
        error: { code: "delivery_account_mismatch" },
      });

      const sessionsA = await get(app, a.token, "/v1/sessions");
      expect(sessionsA.status).toBe(200);
      expect(sessionsA.body.sessions.map((s: any) => s.sessionId)).toEqual([
        "session-a",
      ]);
      expect(sessionsA.body.sessions[0]).toMatchObject({
        source: "claude",
        eventCount: 2,
      });
      const sessionsB = await get(app, b.token, "/v1/sessions");
      expect(sessionsB.body.sessions.map((s: any) => s.sessionId)).toEqual([
        "session-b",
      ]);

      const eventsA = await get(app, a.token, "/v1/sessions/session-a/events");
      expect(eventsA.body).toMatchObject({ sessionId: "session-a" });
      expect(eventsA.body.events.map((e: any) => e.content)).toEqual([
        "alpha needle opening",
        "alpha follow-up",
      ]);
      const crossEvents = await get(
        app,
        b.token,
        "/v1/sessions/session-a/events",
      );
      expect(crossEvents.body.events).toEqual([]);

      const searchA = await get(app, a.token, "/v1/events?q=needle");
      expect(searchA.body.events.map((e: any) => e.content)).toEqual([
        "alpha needle opening",
      ]);
      const searchB = await get(app, b.token, "/v1/events?q=needle");
      expect(searchB.body.events.map((e: any) => e.content)).toEqual([
        "bravo needle opening",
      ]);

      const accountA = { "X-RelayHistory-Expected-Account": batchA.account_id };
      const recordsA = await get(
        app,
        a.token,
        "/v1/delivery/records",
        accountA,
      );
      expect(recordsA.status).toBe(200);
      expect(recordsA.body.records.map((r: any) => r.record_id)).toEqual([
        "a-1",
        "a-2",
      ]);
      const recordsB = await get(app, b.token, "/v1/delivery/records", {
        "X-RelayHistory-Expected-Account": batchB.account_id,
      });
      expect(recordsB.body.records.map((r: any) => r.record_id)).toEqual([
        "b-1",
      ]);
      // B cannot read A's records by naming A's account.
      const crossRecords = await get(
        app,
        b.token,
        "/v1/delivery/records",
        accountA,
      );
      expect(crossRecords.status).toBe(403);
      expect(crossRecords.body).toMatchObject({
        error: { code: "delivery_account_mismatch" },
      });
      // Nor can another workspace of A's organization.
      const workspaceRecords = await get(app, c.token, "/v1/delivery/records", {
        "X-RelayHistory-Expected-Account": await deliveryAccount(
          c as unknown as AuthContext,
        ),
      });
      expect(workspaceRecords.body.records).toEqual([]);
    });
  });
});

/**
 * Service tokens — long-lived, scoped, revocable credentials for unattended agents.
 *
 * These endpoints sit behind `requireAuth`, so minting one requires an already-valid
 * session. That is the point: a service token is a *narrowing* of the authority the
 * caller already proved, never a new grant.
 */
import { Hono } from "hono";
import {
  hostContext,
  type HistoryContext,
  type HistoryEngineDeps,
  type HistoryEnv,
} from "../env.js";
import { getAuth } from "../middleware/auth.js";
import {
  SERVICE_TTL_DEFAULT_DAYS,
  SERVICE_TTL_MAX_DAYS,
  ServiceTokenError,
  createServiceToken,
  listServiceTokens,
  revokeServiceToken,
} from "../auth/tokens.js";

const MAX_LABEL = 120;

function badRequest(c: any, message: string) {
  return c.json(
    {
      error: { code: "bad_request", message },
      correlationId: c.get("correlationId") ?? "",
    },
    400,
  );
}

export function createServiceTokenRoutes<E extends HistoryEnv>(
  deps: HistoryEngineDeps<E>,
): Hono<HistoryEnv> {
  const database = (c: HistoryContext) => deps.database(hostContext<E>(c));
  const serviceTokenRoutes = new Hono<HistoryEnv>();

  /**
   * `POST /v1/auth/service-tokens`
   *
   * Returns the secret **once**. There is deliberately no endpoint that can read it back —
   * only the SHA-256 hash is stored, exactly as for a user session.
   */
  serviceTokenRoutes.post("/auth/service-tokens", async (c) => {
    const db = database(c);
    if (!db) {
      return c.json({ error: "DATABASE_URL is required" }, 503);
    }
    const body = await c.req.json<Record<string, unknown>>().catch(() => null);
    if (!body || typeof body !== "object") {
      return badRequest(c, "Body must be an object");
    }
    const label = body.label;
    if (typeof label !== "string" || !label.trim()) {
      // A label is required so an operator can tell two credentials apart when deciding
      // which to revoke. An unlabelled secret is one nobody dares rotate.
      return badRequest(c, "label is required");
    }
    if (label.length > MAX_LABEL) {
      return badRequest(c, `label must be ${MAX_LABEL} characters or fewer`);
    }
    if (body.scopes != null) {
      if (
        !Array.isArray(body.scopes) ||
        body.scopes.some((s) => typeof s !== "string")
      ) {
        return badRequest(c, "scopes must be an array of strings");
      }
    }
    if (body.expiresInDays != null) {
      if (
        typeof body.expiresInDays !== "number" ||
        !Number.isInteger(body.expiresInDays) ||
        body.expiresInDays < 1
      ) {
        return badRequest(c, "expiresInDays must be a positive integer");
      }
      if (body.expiresInDays > SERVICE_TTL_MAX_DAYS) {
        return badRequest(
          c,
          `expiresInDays must be ${SERVICE_TTL_MAX_DAYS} or fewer`,
        );
      }
    }

    const auth = getAuth(c);
    try {
      const issued = await createServiceToken(
        db,
        {
          userId: auth.userId,
          orgId: auth.orgId,
          workspaceId: auth.workspaceId ?? "default",
          scopes: auth.scopes ?? [],
        },
        {
          label: label.trim(),
          scopes: body.scopes as string[] | undefined,
          expiresInDays:
            (body.expiresInDays as number | undefined) ??
            SERVICE_TTL_DEFAULT_DAYS,
        },
      );
      return c.json(
        {
          ...issued,
          warning:
            "Store this token now — it is not retrievable. Revoke it with DELETE /v1/auth/service-tokens/{id}.",
          correlationId: c.get("correlationId") ?? "",
        },
        201,
      );
    } catch (error) {
      if (error instanceof ServiceTokenError) {
        return c.json(
          {
            error: { code: "forbidden_scope", message: error.message },
            correlationId: c.get("correlationId") ?? "",
          },
          403,
        );
      }
      throw error;
    }
  });

  /** `GET /v1/auth/service-tokens` — metadata only, scoped to the caller's org. */
  serviceTokenRoutes.get("/auth/service-tokens", async (c) => {
    const db = database(c);
    if (!db) {
      return c.json({ error: "DATABASE_URL is required" }, 503);
    }
    const tokens = await listServiceTokens(db, getAuth(c).orgId);
    return c.json({ tokens, correlationId: c.get("correlationId") ?? "" });
  });

  /** `DELETE /v1/auth/service-tokens/:id` — revoke, scoped to the caller's org. */
  serviceTokenRoutes.delete("/auth/service-tokens/:id", async (c) => {
    const db = database(c);
    if (!db) {
      return c.json({ error: "DATABASE_URL is required" }, 503);
    }
    const revoked = await revokeServiceToken(
      db,
      getAuth(c).orgId,
      c.req.param("id"),
    );
    if (!revoked) {
      // Same response whether the id belongs to another org or does not exist: a
      // distinguishable 404 would confirm which ids are real.
      return c.json(
        {
          error: { code: "not_found", message: "no such service token" },
          correlationId: c.get("correlationId") ?? "",
        },
        404,
      );
    }
    return c.json({
      revoked: true,
      correlationId: c.get("correlationId") ?? "",
    });
  });

  return serviceTokenRoutes;
}

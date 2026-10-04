import type { Context, MiddlewareHandler } from "hono";
import { createMiddleware } from "hono/factory";
import {
  hostContext,
  type AuthContext,
  type HistoryContext,
  type HistoryEngineDeps,
  type HistoryEnv,
} from "../env.js";
import {
  ACCESS_PREFIX,
  SERVICE_PREFIX,
  resolveAccessToken,
} from "../auth/tokens.js";

/**
 * A specific 401 from a host's bearer verifier (`HistoryEngineDeps.verifyBearer`). The
 * code and message are returned to the caller, so neither may carry the token.
 */
export class AuthError extends Error {
  constructor(
    public readonly code: string,
    message: string,
  ) {
    super(message);
    this.name = "AuthError";
  }
}

export function getAuth(c: Context<any>): AuthContext {
  const auth = c.get("auth");
  if (!auth) {
    throw new Error("auth context is not available");
  }
  return auth;
}

/** Usable on any host route whose env carries the engine's `auth` variable. */
export function requireScope(scope: string): MiddlewareHandler<any> {
  return createMiddleware<HistoryEnv>(async (c, next) => {
    const auth = getAuth(c);
    if (!auth.scopes.includes(scope)) {
      return c.json(
        {
          error: {
            code: "forbidden",
            message: `missing required scope: ${scope}`,
          },
          correlationId: c.get("correlationId") ?? "",
        },
        403,
      );
    }

    await next();
  });
}

/**
 * Resolves the request's tenant from its bearer. Service-local tokens (`rth_at_` user
 * sessions and `rth_st_` service tokens) resolve against the engine's own token store;
 * any other bearer, or a service-local one that did not resolve, goes to the host's
 * `verifyBearer` when there is one.
 */
export function createRequireAuth<E extends HistoryEnv>(
  deps: Pick<HistoryEngineDeps<E>, "database" | "verifyBearer">,
): MiddlewareHandler<HistoryEnv> {
  return createMiddleware<HistoryEnv>(async (c, next) => {
    const token = bearerToken(c.req.header("Authorization"));
    if (!token) {
      return authError(
        c,
        401,
        "missing_authorization",
        "missing Authorization header",
      );
    }

    // Both service-local prefixes route to the same resolver: `rth_at_` user sessions and
    // `rth_st_` service tokens. Gating on ACCESS_PREFIX alone silently 401'd every service
    // token — and did so *before* the resolver, so no amount of testing `resolveAccessToken`
    // directly would have caught it. The test for this goes through `requireAuth`.
    if (token.startsWith(ACCESS_PREFIX) || token.startsWith(SERVICE_PREFIX)) {
      const db = deps.database(hostContext<E>(c));
      if (!db) {
        return authError(
          c,
          503,
          "not_configured",
          "DATABASE_URL is required for service-local auth",
        );
      }

      const session = await resolveAccessToken(db, token);
      if (session) {
        c.set("auth", {
          userId: session.userId,
          orgId: session.orgId,
          workspaceId: session.workspaceId,
          tokenSubject: session.userId,
          scopes: (session.scopes as string[]) ?? [],
          claims: { sessionId: session.id, subjectType: session.subjectType },
          sessionId: session.id,
        });
        await next();
        return;
      }
    }

    if (deps.verifyBearer) {
      let auth: AuthContext | undefined;
      try {
        auth = await deps.verifyBearer(token, hostContext<E>(c));
      } catch (error) {
        if (error instanceof AuthError) {
          return authError(c, 401, error.code, error.message);
        }
        console.error(error);
        return authError(c, 401, "invalid_token", "invalid bearer token");
      }
      if (auth) {
        c.set("auth", auth);
        await next();
        return;
      }
    }

    return authError(c, 401, "invalid_token", "invalid or expired token");
  });
}

function bearerToken(authorization: string | undefined): string | null {
  const value = authorization?.trim();
  if (!value) {
    return null;
  }
  const match = value.match(/^Bearer\s+(.+)$/i);
  return match ? match[1].trim() : null;
}

function authError(
  c: HistoryContext,
  status: 401 | 503,
  code: string,
  message: string,
): Response {
  return c.json(
    {
      error: {
        code,
        message,
      },
      correlationId: c.get("correlationId") ?? "",
    },
    status,
  );
}

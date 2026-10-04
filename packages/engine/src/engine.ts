import { Hono } from "hono";
import { cors } from "hono/cors";
import { secureHeaders } from "hono/secure-headers";
import type { HistoryEngineDeps, HistoryEnv } from "./env.js";
import { createRequireAuth } from "./middleware/auth.js";
import { createDeliveryRoutes } from "./routes/delivery.js";
import { createHealthRoutes } from "./routes/health.js";
import { createIngestRoutes } from "./routes/ingest.js";
import { createRecallRoutes } from "./routes/recall.js";
import { createServiceTokenRoutes } from "./routes/service-tokens.js";
import { createTurnRoutes } from "./routes/turns.js";

type ErrorCode = "bad_request" | "not_found" | "internal_error";

/** The engine's route groups, for a host that assembles its own application. */
export function createHistoryRoutes<E extends HistoryEnv>(
  deps: HistoryEngineDeps<E>,
) {
  return {
    health: createHealthRoutes(),
    delivery: createDeliveryRoutes(deps),
    ingest: createIngestRoutes(deps),
    turns: createTurnRoutes(deps),
    recall: createRecallRoutes(deps),
    serviceTokens: createServiceTokenRoutes(deps),
  };
}

/**
 * The History service as one Hono application: request ids, CORS and secure headers,
 * `/health`, and every `/v1` route behind service-local authentication. The hosted
 * Worker and the self-hosted Node server both serve exactly this; they differ only in
 * `deps`.
 */
export function createHistoryEngine<E extends HistoryEnv = HistoryEnv>(
  deps: HistoryEngineDeps<E>,
): Hono<E> {
  const routes = createHistoryRoutes(deps);
  const app = new Hono<E>();

  app.use("*", async (c, next) => {
    const requestId = crypto.randomUUID();
    const correlationId = c.req.header("X-Correlation-Id")?.trim() || requestId;

    c.set("requestId", requestId);
    c.set("correlationId", correlationId);

    await next();

    c.header("X-Request-Id", requestId);
    c.header("X-Correlation-Id", correlationId);
  });
  // After the ids are set and ahead of CORS (whose preflight answer ends the chain) and
  // auth, so host middleware sees every request with its route.
  for (const middleware of deps.middleware ?? []) app.use("*", middleware);
  app.use("*", cors());
  app.use("*", secureHeaders());

  const publicV1Routes = new Hono<E>();
  for (const route of deps.publicRoutes ?? []) publicV1Routes.route("/", route);

  const v1Routes = new Hono<HistoryEnv>();
  v1Routes.use("*", createRequireAuth(deps));
  v1Routes.route("/", routes.delivery);
  v1Routes.route("/", routes.ingest);
  v1Routes.route("/", routes.turns);
  v1Routes.route("/", routes.recall);
  v1Routes.route("/", routes.serviceTokens);
  for (const route of deps.routes ?? []) v1Routes.route("/", route);

  app.route("/", routes.health);
  for (const route of deps.rootRoutes ?? []) app.route("/", route);
  app.route("/v1", publicV1Routes);
  app.route("/v1", v1Routes);

  app.notFound((c) =>
    jsonError(
      c.get("correlationId"),
      c,
      404,
      "not_found",
      `No route for ${c.req.method} ${new URL(c.req.url).pathname}`,
    ),
  );
  app.onError((err, c) => {
    if (err.name === "BadRequestError") {
      return jsonError(
        c.get("correlationId"),
        c,
        400,
        "bad_request",
        err.message,
      );
    }
    console.error(err);
    return jsonError(
      c.get("correlationId"),
      c,
      500,
      "internal_error",
      "Internal server error",
    );
  });

  return app;
}

function jsonError(
  correlationId: string | undefined,
  c: { json(body: unknown, status: any): Response },
  status: number,
  code: ErrorCode,
  message: string,
): Response {
  return c.json(
    {
      error: {
        code,
        message,
      },
      correlationId: correlationId ?? "",
    },
    status,
  );
}

/**
 * `@relayhistory/engine` — the RelayHistory service engine.
 *
 * The hosted service (Cloudflare Worker + Neon) and self-hosted deployments (Node +
 * PostgreSQL) both run `createHistoryEngine`. This entry point is runtime-neutral: it
 * uses only Web APIs, so it bundles for Workers. Migrations, which read SQL files from
 * disk, are at `@relayhistory/engine/migrations`.
 */
export { createHistoryEngine, createHistoryRoutes } from "./engine.js";
export * from "./env.js";
export type {
  HistoryDb,
  HistoryQueryResult,
  HistoryQueryResultHKT,
} from "./db/database.js";
export * as schema from "./db/schema.js";

export {
  AuthError,
  createRequireAuth,
  getAuth,
  requireScope,
} from "./middleware/auth.js";
export {
  WORKSPACE_RECALL_HEADER,
  attestWorkspace,
  readWorkspaceRecallScope,
} from "./middleware/workspace-recall.js";
export * from "./auth/tokens.js";
export * from "./auth/bootstrap.js";

export * from "./lib/delivery-contracts.js";
export * from "./lib/delivery.js";
export { deliveryBatchShape } from "./routes/delivery.js";
export * from "./lib/scrub.js";
export * from "./lib/recall.js";
export * from "./lib/session-catalog.js";
export * from "./lib/session-links.js";
export * from "./lib/session-work-state.js";
export * from "./lib/turns.js";
export * from "./lib/ingest.js";
export * from "./lib/types.js";
export * from "./lib/embed.js";

export { createDeliveryRoutes } from "./routes/delivery.js";
export { createHealthRoutes } from "./routes/health.js";
export { createIngestRoutes } from "./routes/ingest.js";
export { createRecallRoutes } from "./routes/recall.js";
export { createServiceTokenRoutes } from "./routes/service-tokens.js";
export { createTurnRoutes, parseTurnRequest } from "./routes/turns.js";

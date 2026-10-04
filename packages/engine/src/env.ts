import type { Context, Hono, MiddlewareHandler } from "hono";
import type { HistoryDb } from "./db/database.js";
import type { EmbeddingProvider } from "./lib/embed.js";
import type { HistoryExportBatch } from "./lib/delivery-contracts.js";
import type { SessionSummary } from "./lib/recall.js";

/** The authenticated tenant of one request. Tenancy only ever comes from here. */
export interface AuthContext {
  userId: string;
  orgId: string;
  workspaceId?: string;
  tokenSubject: string;
  scopes: string[];
  claims: Record<string, unknown>;
  sessionId?: string;
}

export interface HistoryVariables {
  requestId: string;
  correlationId: string;
  auth?: AuthContext;
}

/** The Hono environment the engine's routes run in. A host's own env extends it. */
export interface HistoryEnv {
  Bindings?: object;
  Variables: HistoryVariables;
}

export type HistoryContext = Context<HistoryEnv>;

/** What one `POST /v1/delivery/batches` request did, reported when it ends. */
export interface DeliveryBatchObservation {
  auth: AuthContext;
  /** Undefined when the body never parsed into a batch. */
  batch: HistoryExportBatch | undefined;
  /** Request bytes read, including when parsing failed. */
  bytes: number;
  /** `accepted` or the delivery error code. */
  outcome: string;
  conflictCount?: number;
  durationMs: number;
}

/**
 * Everything the engine needs from the host that runs it. Infrastructure (the database
 * connection, a second identity provider, model providers, telemetry) is injected here;
 * evidence handling, tenancy, scrubbing and receipts are not configurable.
 */
export interface HistoryEngineDeps<E extends HistoryEnv = HistoryEnv> {
  /** The request's database. `undefined` answers each route's 503 `not_configured`. */
  database(c: Context<E>): HistoryDb | undefined;
  /**
   * A bearer that is not a service-local `rth_at_`/`rth_st_` token, or one that did not
   * resolve. Return the tenant to accept it, `undefined` to answer 401 `invalid_token`,
   * or throw `AuthError` for a specific 401 code.
   */
  verifyBearer?(token: string, c: Context<E>): Promise<AuthContext | undefined>;
  /** Embedding provider for `POST /v1/ingest`. Absent: events are stored without one. */
  embeddings?(c: Context<E>): EmbeddingProvider | null | undefined;
  /** Enriches an organization-scoped `GET /v1/sessions` page in place. */
  enrichSessions?(
    c: Context<E>,
    db: HistoryDb,
    auth: AuthContext,
    sessions: SessionSummary[],
  ): Promise<void>;
  /**
   * A failure the engine answered without exposing it: an unhandled route error (the
   * error itself, answered 500), a host bearer verifier that threw, or a failed token
   * usage write (each as `{ name, code }` only, since those may carry the bearer).
   * Absent: logs `{ name, code }`, never a message, SQL or row contents.
   */
  reportError?(error: unknown, c: Context<E>): void;
  /** Called as a delivery batch request starts; the result receives its outcome. */
  observeDeliveryBatch?(
    c: Context<E>,
  ): (observation: DeliveryBatchObservation) => void;
  /** Runs after request and correlation ids are set, before CORS and auth. */
  middleware?: MiddlewareHandler<E>[];
  /** Mounted at `/`, beside `/health`. */
  rootRoutes?: Hono<any, any, any>[];
  /** Mounted at `/v1` without authentication. */
  publicRoutes?: Hono<any, any, any>[];
  /** Mounted at `/v1` behind `requireAuth`, after the engine's own routes. */
  routes?: Hono<any, any, any>[];
}

/** Calls a host dependency with the host's own context type. */
export function hostContext<E extends HistoryEnv>(
  c: HistoryContext,
): Context<E> {
  return c as unknown as Context<E>;
}

import type { HistoryDb } from "../db/database.js";
import {
  DEFAULT_SCOPES,
  SERVICE_TTL_DEFAULT_DAYS,
  SERVICE_TTL_MAX_DAYS,
  ServiceTokenError,
  createServiceToken,
} from "./tokens.js";

export interface BootstrapServiceTokenOptions {
  orgId: string;
  workspaceId: string;
  /** Recorded as the token's subject. Defaults to `operator`. */
  userId?: string;
  label: string;
  /** A subset of `rth:sync` and `rth:read`. Defaults to both. */
  scopes?: string[];
  /** 1..`SERVICE_TTL_MAX_DAYS`. Defaults to `SERVICE_TTL_DEFAULT_DAYS`. */
  expiresInDays?: number;
}

export interface BootstrappedServiceToken {
  id: string;
  /** The secret. Returned once; only its SHA-256 hash is stored. */
  token: string;
  label: string;
  scopes: string[];
  expiresAt: string;
  orgId: string;
  workspaceId: string;
}

const IDENTIFIER = /^[A-Za-z0-9][A-Za-z0-9._:@-]{0,127}$/;

/**
 * Mint a scoped `rth_st_` service token directly in the database, for an operator who
 * holds the database credentials of a self-hosted deployment.
 *
 * This is the only way a deployment without an external identity provider gets its
 * first credential. It is a database operation, not an HTTP endpoint: nothing a network
 * caller can reach mints a token without already holding one, and the hosted service's
 * `/v1/admin/mint` stays disabled. The token resolves through the same `requireAuth`
 * path as every other service token, so tenancy comes from the stored row and scopes
 * are enforced per route.
 */
export async function bootstrapServiceToken(
  db: HistoryDb,
  options: BootstrapServiceTokenOptions,
): Promise<BootstrappedServiceToken> {
  for (const [field, value] of [
    ["orgId", options.orgId],
    ["workspaceId", options.workspaceId],
    ["userId", options.userId ?? "operator"],
  ] as const) {
    if (!IDENTIFIER.test(value)) {
      throw new ServiceTokenError(
        `${field} must be 1..128 characters of letters, digits and . _ : @ -`,
      );
    }
  }
  const label = options.label.trim();
  if (!label || label.length > 120) {
    throw new ServiceTokenError("label must be 1..120 characters");
  }
  const scopes = options.scopes?.length ? options.scopes : [...DEFAULT_SCOPES];
  const unknown = scopes.filter(
    (scope) => !(DEFAULT_SCOPES as readonly string[]).includes(scope),
  );
  if (unknown.length) {
    throw new ServiceTokenError(`unknown scopes: ${unknown.join(", ")}`);
  }
  const days = options.expiresInDays ?? SERVICE_TTL_DEFAULT_DAYS;
  if (!Number.isInteger(days) || days < 1 || days > SERVICE_TTL_MAX_DAYS) {
    throw new ServiceTokenError(
      `expiresInDays must be an integer from 1 to ${SERVICE_TTL_MAX_DAYS}`,
    );
  }
  const issued = await createServiceToken(
    db,
    {
      userId: options.userId ?? "operator",
      orgId: options.orgId,
      workspaceId: options.workspaceId,
      scopes: [...DEFAULT_SCOPES],
    },
    { label, scopes, expiresInDays: days },
  );
  return {
    ...issued,
    orgId: options.orgId,
    workspaceId: options.workspaceId,
  };
}

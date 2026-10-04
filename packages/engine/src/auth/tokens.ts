import { and, eq, isNull, lte, or, sql } from "drizzle-orm";
import type { HistoryDb as Db } from "../db/database.js";
import { authSessions, type AuthSession } from "../db/schema.js";

/** The tenant a user session is issued to, as verified by the host's identity provider. */
export interface Identity {
  userId: string;
  orgId: string;
  workspaceId: string;
}

export const ACCESS_PREFIX = "rth_at_";
export const REFRESH_PREFIX = "rth_rt_";
/**
 * Service tokens carry their own prefix rather than reusing `rth_at_`.
 *
 * An operator (and a secret scanner) can then tell at a glance whether a leaked string is
 * a 24-hour user session or a 90-day unattended credential — very different incidents.
 * The steady-state resolver accepts both.
 */
export const SERVICE_PREFIX = "rth_st_";

const TOKEN_USAGE_INTERVAL_MS = 5 * 60_000;

const ACCESS_TTL_SECONDS = 60 * 60 * 24;
const REFRESH_TTL_SECONDS = 60 * 60 * 24 * 90;

/** Default life of a service token. Long enough to be useful, short enough to rotate. */
export const SERVICE_TTL_DEFAULT_DAYS = 90;
/** Hard ceiling. A credential that never expires is one nobody ever rotates. */
export const SERVICE_TTL_MAX_DAYS = 365;

export const DEFAULT_SCOPES = ["rth:sync", "rth:read"] as const;

export interface IssuedTokenPair {
  sessionId: string;
  accessToken: string;
  accessTokenExpiresAt: string;
  refreshToken: string;
  refreshTokenExpiresAt: string;
  scopes: string[];
  /**
   * The tenancy this pair is bound to. The service always derives tenancy from the
   * bearer, never from these fields; they are returned so the CLI can record which org
   * its stored session belongs to. The CLI's recall path refuses a stored session with
   * no org (`cloud::recall_auth`), so omitting them left every `/v1/cli/login` session
   * able to push but never read: `stats --remote` and `sessions list --remote` were
   * empty while `/v1/sessions` held the data.
   */
  orgId: string;
  workspaceId: string;
}

export async function createSession(
  db: Db,
  identity: Identity,
  opts: { scopes?: string[]; label?: string } = {},
): Promise<IssuedTokenPair> {
  const accessToken = generateOpaqueToken(ACCESS_PREFIX);
  const refreshToken = generateOpaqueToken(REFRESH_PREFIX);
  const scopes = opts.scopes ?? [...DEFAULT_SCOPES];
  const id = crypto.randomUUID();
  const accessExp = expiresAt(ACCESS_TTL_SECONDS);
  const refreshExp = expiresAt(REFRESH_TTL_SECONDS);

  await db.insert(authSessions).values({
    id,
    tokenFamilyId: crypto.randomUUID(),
    subjectType: "cli",
    userId: identity.userId,
    orgId: identity.orgId,
    workspaceId: identity.workspaceId,
    scopes,
    accessTokenHash: await hashToken(accessToken),
    accessTokenExpiresAt: accessExp,
    refreshTokenHash: await hashToken(refreshToken),
    refreshTokenExpiresAt: refreshExp,
    label: opts.label ?? null,
  });

  return {
    sessionId: id,
    accessToken,
    accessTokenExpiresAt: accessExp.toISOString(),
    refreshToken,
    refreshTokenExpiresAt: refreshExp.toISOString(),
    scopes,
    orgId: identity.orgId,
    workspaceId: identity.workspaceId,
  };
}

export async function resolveAccessToken(
  db: Db,
  accessToken: string,
  options: {
    /** A failed usage-timestamp write. The token still resolves. */
    onUsageError?(error: unknown): void;
  } = {},
): Promise<AuthSession | null> {
  if (
    !accessToken.startsWith(ACCESS_PREFIX) &&
    !accessToken.startsWith(SERVICE_PREFIX)
  ) {
    return null;
  }
  const tokenHash = await hashToken(accessToken);
  const rows = await db
    .select()
    .from(authSessions)
    .where(
      and(
        eq(authSessions.accessTokenHash, tokenHash),
        isNull(authSessions.revokedAt),
      ),
    )
    .limit(1);
  const record = rows[0];
  if (!record) return null;
  if (record.accessTokenExpiresAt.getTime() <= Date.now()) return null;

  // Authorization is checked on every request. Usage timestamps are approximate
  // telemetry: avoid a write for every heartbeat, status poll, or uploaded batch, and
  // never fail an authorized request because the write did.
  const now = new Date();
  const cutoff = new Date(now.getTime() - TOKEN_USAGE_INTERVAL_MS);
  if (!record.lastUsedAt || record.lastUsedAt <= cutoff) {
    try {
      await db
        .update(authSessions)
        .set({ lastUsedAt: now, updatedAt: now })
        .where(
          and(
            eq(authSessions.id, record.id),
            or(
              isNull(authSessions.lastUsedAt),
              lte(authSessions.lastUsedAt, cutoff),
            ),
          ),
        );
    } catch (error) {
      options.onUsageError?.(error);
    }
  }

  return record;
}

export async function refreshSession(
  db: Db,
  refreshToken: string,
): Promise<IssuedTokenPair | null> {
  // A service token has no refresh path. Its stored refresh hash is already unusable;
  // this makes the refusal explicit rather than incidental, so the property survives a
  // future change to how that hash is generated.
  if (!refreshToken.startsWith(REFRESH_PREFIX)) return null;
  const tokenHash = await hashToken(refreshToken);
  const rows = await db
    .select()
    .from(authSessions)
    .where(
      and(
        eq(authSessions.refreshTokenHash, tokenHash),
        isNull(authSessions.revokedAt),
      ),
    )
    .limit(1);
  const record = rows[0];
  if (!record) return null;
  if (record.subjectType === "service") {
    // Belt and braces: the stored refresh hash for a service token is already a value
    // nobody holds, so reaching here should be impossible. Refusing explicitly means the
    // property survives a future change to how that hash is generated.
    return null;
  }
  if (record.refreshTokenExpiresAt.getTime() <= Date.now()) {
    await revokeById(db, record.id, "refresh_token_expired");
    return null;
  }

  const nextAccess = generateOpaqueToken(ACCESS_PREFIX);
  const nextRefresh = generateOpaqueToken(REFRESH_PREFIX);
  const accessExp = expiresAt(ACCESS_TTL_SECONDS);
  const refreshExp = expiresAt(REFRESH_TTL_SECONDS);
  const now = new Date();

  // Conditional on the presented refresh hash: of two concurrent refreshes of one
  // token, only the first rotation returns a pair, so no caller holds a pair the
  // other's rotation already replaced.
  const rotated = await db
    .update(authSessions)
    .set({
      accessTokenHash: await hashToken(nextAccess),
      accessTokenExpiresAt: accessExp,
      refreshTokenHash: await hashToken(nextRefresh),
      refreshTokenExpiresAt: refreshExp,
      lastRefreshedAt: now,
      updatedAt: now,
    })
    .where(
      and(
        eq(authSessions.id, record.id),
        eq(authSessions.refreshTokenHash, tokenHash),
        isNull(authSessions.revokedAt),
      ),
    )
    .returning({ id: authSessions.id });
  if (rotated.length === 0) return null;

  return {
    sessionId: record.id,
    accessToken: nextAccess,
    accessTokenExpiresAt: accessExp.toISOString(),
    refreshToken: nextRefresh,
    refreshTokenExpiresAt: refreshExp.toISOString(),
    scopes: (record.scopes as string[]) ?? [...DEFAULT_SCOPES],
    // From the stored session, not the request: a refresh can never move a session
    // to another tenancy. Returned so a CLI whose login predates these fields
    // repairs its stored org on its next routine refresh, with no re-login.
    orgId: record.orgId,
    workspaceId: record.workspaceId,
  };
}

export async function revokeByAnyToken(
  db: Db,
  token: string,
): Promise<boolean> {
  const tokenHash = await hashToken(token);
  const column = token.startsWith(REFRESH_PREFIX)
    ? authSessions.refreshTokenHash
    : authSessions.accessTokenHash;
  const rows = await db
    .update(authSessions)
    .set({
      revokedAt: new Date(),
      revokedReason: "user_requested",
      updatedAt: new Date(),
    })
    .where(and(eq(column, tokenHash), isNull(authSessions.revokedAt)))
    .returning({ id: authSessions.id });
  return rows.length > 0;
}

export async function hashToken(token: string): Promise<string> {
  const data = new TextEncoder().encode(token);
  const digest = await crypto.subtle.digest("SHA-256", data);
  const bytes = new Uint8Array(digest);
  let hex = "";
  for (const b of bytes) hex += b.toString(16).padStart(2, "0");
  return hex;
}

function generateOpaqueToken(prefix: string): string {
  const bytes = new Uint8Array(32);
  crypto.getRandomValues(bytes);
  return `${prefix}${base64url(bytes)}`;
}

function base64url(bytes: Uint8Array): string {
  let bin = "";
  for (const b of bytes) bin += String.fromCharCode(b);
  return btoa(bin).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

function expiresAt(seconds: number, from = Date.now()): Date {
  return new Date(from + seconds * 1000);
}

async function revokeById(db: Db, id: string, reason: string): Promise<void> {
  await db
    .update(authSessions)
    .set({
      revokedAt: new Date(),
      revokedReason: reason,
      updatedAt: new Date(),
    })
    .where(eq(authSessions.id, id));
}

/**
 * Mint a long-lived, scoped, revocable credential for an unattended agent.
 *
 * The gap this closes: a deployed agent had no credential it could hold. An `rth_at_`
 * access token lives 24 hours and `refreshSession` rotates it *in place*, so a copy taken
 * from a developer's machine stops working the moment that machine refreshes — an agent
 * that works today and silently 401s tomorrow. `/v1/admin/mint` is deliberately 404 in
 * production, so there was no other path.
 *
 * Three properties make this safe to hand to a background job:
 *
 *   - **No refresh.** A service token cannot be exchanged for a new one; when it expires
 *     an operator mints a fresh one. The refresh hash stored below is a random value
 *     nobody holds, and `refreshSession` additionally refuses `subject_type = 'service'`,
 *     so the path is closed twice over.
 *   - **No escalation.** Scopes are intersected with the minting session's, so a caller
 *     can never mint a credential more powerful than the one it authenticated with.
 *   - **Bounded life and revocable.** Capped at `SERVICE_TTL_MAX_DAYS`, and `revokedAt`
 *     is honoured by the same resolver every other request goes through.
 *
 * The secret is returned once and stored only as a SHA-256 hash. There is no endpoint
 * that can read it back.
 */
export async function createServiceToken(
  db: Db,
  minter: {
    userId: string;
    orgId: string;
    workspaceId: string;
    scopes: string[];
  },
  opts: {
    label: string;
    /** Omitted: the minter's scopes. An empty list is refused. */
    scopes?: string[];
    expiresInDays?: number;
    /** Latest expiry, e.g. the minting service token's own. */
    notAfter?: Date;
  },
): Promise<{
  id: string;
  token: string;
  label: string;
  scopes: string[];
  expiresAt: string;
}> {
  const requested = opts.scopes ?? [...minter.scopes];
  // Intersect, never union: a service token is a narrowing of the minter's authority.
  const scopes = requested.filter((scope) => minter.scopes.includes(scope));
  if (scopes.length === 0) {
    throw new ServiceTokenError(
      "requested scopes are not a subset of the calling session's scopes",
    );
  }

  const days = Math.min(
    Math.max(opts.expiresInDays ?? SERVICE_TTL_DEFAULT_DAYS, 1),
    SERVICE_TTL_MAX_DAYS,
  );
  const token = generateOpaqueToken(SERVICE_PREFIX);
  const id = crypto.randomUUID();
  const requestedExp = expiresAt(days * 24 * 60 * 60);
  const exp =
    opts.notAfter && opts.notAfter < requestedExp
      ? opts.notAfter
      : requestedExp;

  await db.insert(authSessions).values({
    id,
    tokenFamilyId: crypto.randomUUID(),
    subjectType: "service",
    userId: minter.userId,
    orgId: minter.orgId,
    workspaceId: minter.workspaceId,
    scopes,
    accessTokenHash: await hashToken(token),
    accessTokenExpiresAt: exp,
    // Unusable by construction: a hash of a value that is generated here, never
    // returned, and immediately discarded. Combined with the guard in `refreshSession`
    // this leaves no way to exchange a service token for a new one.
    refreshTokenHash: await hashToken(generateOpaqueToken(REFRESH_PREFIX)),
    refreshTokenExpiresAt: exp,
    label: opts.label,
  });

  return { id, token, label: opts.label, scopes, expiresAt: exp.toISOString() };
}

export class ServiceTokenError extends Error {}

/** When a session's access token expires, or null when no such session exists. */
export async function accessTokenExpiry(
  db: Db,
  sessionId: string,
): Promise<Date | null> {
  const rows = await db
    .select({ expiresAt: authSessions.accessTokenExpiresAt })
    .from(authSessions)
    .where(eq(authSessions.id, sessionId))
    .limit(1);
  return rows[0]?.expiresAt ?? null;
}

/** List an org's service tokens. Never returns a secret — there is no way to. */
export async function listServiceTokens(db: Db, orgId: string) {
  const rows = await db
    .select({
      id: authSessions.id,
      label: authSessions.label,
      scopes: authSessions.scopes,
      createdAt: authSessions.createdAt,
      lastUsedAt: authSessions.lastUsedAt,
      expiresAt: authSessions.accessTokenExpiresAt,
      revokedAt: authSessions.revokedAt,
    })
    .from(authSessions)
    .where(
      and(
        eq(authSessions.orgId, orgId),
        eq(authSessions.subjectType, "service"),
      ),
    )
    .orderBy(authSessions.createdAt);
  return rows;
}

/**
 * Revoke one service token, scoped to the caller's org and, with `withinScopes`, to
 * tokens whose scopes are a subset of the caller's: the same narrowing as minting, so a
 * read-only credential cannot revoke an upload credential.
 *
 * Both predicates are in the WHERE clause rather than checked after the read: a revoke
 * that matched a forbidden row and then declined to act would still have confirmed that
 * the id exists.
 *
 * Only this token is revoked. Tokens it minted are independent credentials (their expiry
 * is capped at this token's), so each must be revoked on its own.
 */
export async function revokeServiceToken(
  db: Db,
  orgId: string,
  id: string,
  reason = "revoked by operator",
  options: { withinScopes?: string[] } = {},
): Promise<boolean> {
  const rows = await db
    .update(authSessions)
    .set({
      revokedAt: new Date(),
      revokedReason: reason,
      updatedAt: new Date(),
    })
    .where(
      and(
        eq(authSessions.id, id),
        eq(authSessions.orgId, orgId),
        eq(authSessions.subjectType, "service"),
        isNull(authSessions.revokedAt),
        options.withinScopes
          ? sql`${authSessions.scopes} <@ ${JSON.stringify(options.withinScopes)}::jsonb`
          : undefined,
      ),
    )
    .returning({ id: authSessions.id });
  return rows.length > 0;
}

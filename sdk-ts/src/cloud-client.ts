import { readFile, readdir, rename, writeFile, mkdir } from 'node:fs/promises';
import { homedir } from 'node:os';
import { join, dirname } from 'node:path';
import {
  AuthenticationExpiredError, ConnectorFailureError, InvalidArgumentError, SOURCES,
  UnsupportedOperationError, isSource,
} from './index.js';

export interface RelayhistoryAuth {
  baseUrl: string;
  accessToken: string;
  refreshToken?: string;
  /** Server-issued RFC 3339 expiry. Absent in sessions stored before it existed. */
  accessTokenExpiresAt?: string;
  /**
   * Locally cached org, for provenance only — never sent as an authorization
   * selector. The native store records it; this SDK's own login does not.
   */
  orgId?: string;
}

export type LoginCloudResult =
  | { ok: true; auth: RelayhistoryAuth }
  | { ok: false; error: string };

function authPath(): string {
  const configDir = process.env.AI_HIST_CONFIG_DIR ?? join(homedir(), '.config', 'ai-hist');
  return join(configDir, 'auth.json');
}

async function saveRelayhistoryAuth(auth: RelayhistoryAuth): Promise<void> {
  const p = authPath();
  await mkdir(dirname(p), { recursive: true });
  await writeFile(p, JSON.stringify(auth, null, 2), { mode: 0o600 });
}

export async function loadStoredRelayhistoryAuth(): Promise<RelayhistoryAuth | null> {
  try {
    const body = await readFile(authPath(), 'utf-8');
    const parsed = JSON.parse(body) as Partial<RelayhistoryAuth>;
    if (typeof parsed.accessToken !== 'string' || typeof parsed.baseUrl !== 'string') return null;
    return parsed as RelayhistoryAuth;
  } catch {
    return null;
  }
}

export async function loginCloud(
  relayAccessToken: string,
  opts: { baseUrl?: string; label?: string } = {}
): Promise<LoginCloudResult> {
  const baseUrl = opts.baseUrl ?? process.env.AI_HIST_BASE_URL ?? 'https://history.agentrelay.com';
  const url = `${baseUrl.replace(/\/$/, '')}/v1/cli/login`;

  let resp: Response;
  try {
    resp = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ agentRelayToken: relayAccessToken, label: opts.label }),
      signal: AbortSignal.timeout(15_000),
    });
  } catch (err) {
    return { ok: false, error: `Network error: ${err instanceof Error ? err.message : String(err)}` };
  }

  if (!resp.ok) {
    const text = await resp.text().catch(() => '');
    return { ok: false, error: `Login failed (HTTP ${resp.status}): ${text.slice(0, 200)}` };
  }

  let payload: Record<string, unknown>;
  try {
    payload = (await resp.json()) as Record<string, unknown>;
  } catch {
    return { ok: false, error: 'Login response was not valid JSON' };
  }

  const accessToken = payload.accessToken;
  const refreshToken = payload.refreshToken;
  if (typeof accessToken !== 'string') {
    return { ok: false, error: 'Login response missing accessToken' };
  }

  const auth: RelayhistoryAuth = {
    baseUrl,
    accessToken,
    ...(typeof refreshToken === 'string' ? { refreshToken } : {}),
  };

  try {
    await saveRelayhistoryAuth(auth);
  } catch (err) {
    return {
      ok: false,
      error: `Failed to save auth: ${err instanceof Error ? err.message : String(err)}`,
    };
  }

  return { ok: true, auth };
}

// ---------------------------------------------------------------------------
// Session thread (SPEC §3.4 / §3.5)
//
// `get_session_thread` is the *lifecycle* fan-out for one session — the PRs,
// reviews, commits, incidents, tickets, Slack threads, hotfixes and follow-up
// sessions the cloud has stitched to it. It is cloud-only on purpose: a thread
// exists only once the cloud has ingested lens events, so there is no local
// fallback and no local cache. Threads change as PRs and incidents land, so
// every call fetches.
// ---------------------------------------------------------------------------

/** The operation name the unsupported-operation message is phrased around. */
const THREAD_OPERATION = 'thread';

/**
 * Query parameters that select tenancy. Tenancy is derived from the bearer
 * token server-side; sending one of these would be a client asserting its own
 * scope. Mirrors the guard in the Rust transport (`cloud::recall_page`).
 */
const TENANCY_PARAMS: ReadonlySet<string> = new Set([
  'org_id', 'orgId', 'workspace_id', 'workspaceId',
]);

const DEFAULT_CLOUD_BASE_URL = 'https://history.agentrelay.com';

/** Where the stored RelayHistory session lives for this process. */
function relayhistoryConfigDir(): string {
  return process.env.AI_HIST_CONFIG_DIR ?? join(homedir(), '.config', 'ai-hist');
}

/**
 * A base URL reduced to the identity of its stage, so two spellings of one
 * stage compare equal. Mirrors `cloud::normalized_stage`.
 *
 * Scheme and host are case-insensitive per RFC 3986 and are lowercased; the
 * path is not, and is preserved exactly. A stage mounted under a case-sensitive
 * prefix (`https://host/Recall`) must keep it or the request goes elsewhere.
 */
function normalizeStage(baseUrl: string): string {
  const trimmed = baseUrl.trim().replace(/\/+$/, '');
  let parsed: URL;
  try {
    parsed = new URL(trimmed);
  } catch {
    // Not a URL this build can parse: compare it verbatim rather than
    // inventing an origin for it.
    return trimmed;
  }
  return `${parsed.protocol.toLowerCase()}//${parsed.host.toLowerCase()}${parsed.pathname}`
    .replace(/\/+$/, '');
}

/**
 * A base URL reduced to its stage identity, or `null` when it does not name
 * one. Mirrors `cloud::normalize_base_url`, which rejects a value with no
 * host, or carrying credentials, a query or a fragment — and, like it, treats
 * a rejected value as "no stage named" rather than as an error.
 */
function parseStageUrl(value: string): string | null {
  const trimmed = value.trim();
  if (!trimmed) return null;
  let url: URL;
  try {
    url = new URL(trimmed);
  } catch {
    return null;
  }
  if (!url.hostname || url.username || url.password || url.search || url.hash) return null;
  return normalizeStage(trimmed);
}

/**
 * The stage this call names, or `undefined` for "whichever single stage is
 * stored".
 *
 * The engine reads `RELAYHISTORY_BASE_URL` before `AI_HIST_BASE_URL` and
 * ignores a value that does not parse, falling through to its single-stage
 * rule. Honouring only `AI_HIST_BASE_URL` would ignore the variable a
 * multi-stage CLI install already sets, and treating an empty or malformed one
 * as an explicit request would fail closed where the engine falls through.
 */
function requestedStage(explicit: string | undefined): string | undefined {
  if (explicit !== undefined) {
    const named = parseStageUrl(explicit);
    if (!named) {
      // An explicit argument is the caller's own words, so a malformed one is a
      // mistake to report, not an environment default to skip past.
      throw new InvalidArgumentError(
        `baseUrl '${explicit}' does not name a stage (expected an absolute URL with a host `
          + 'and no credentials, query or fragment)',
        'INVALID_ARGUMENT',
      );
    }
    return named;
  }
  for (const key of ['RELAYHISTORY_BASE_URL', 'AI_HIST_BASE_URL'] as const) {
    const value = process.env[key];
    if (value === undefined) continue;
    const named = parseStageUrl(value);
    if (named) return named;
  }
  return undefined;
}

/**
 * The store the native `ai-hist login` writes: `RELAYHISTORY_HOME`, else
 * `~/.agentworkforce/relayhistory`. Sessions live in `stages/<key>.auth.json`
 * with snake_case fields, plus a legacy single `auth.json` from before stages
 * existed.
 */
function nativeCloudHome(): string {
  const configured = process.env.RELAYHISTORY_HOME;
  if (configured) return configured;
  return join(homedir(), '.agentworkforce', 'relayhistory');
}

/** Reads one native store file, mapping its snake_case fields. Absent or malformed reads as none. */
async function readNativeAuth(path: string): Promise<RelayhistoryAuth | null> {
  let parsed: Record<string, unknown>;
  try {
    parsed = JSON.parse(await readFile(path, 'utf-8')) as Record<string, unknown>;
  } catch {
    return null;
  }
  const baseUrl = parsed.base_url ?? parsed.baseUrl;
  const accessToken = parsed.access_token ?? parsed.accessToken;
  if (typeof baseUrl !== 'string' || typeof accessToken !== 'string') return null;
  const refreshToken = parsed.refresh_token ?? parsed.refreshToken;
  const expiresAt = parsed.access_token_expires_at ?? parsed.accessTokenExpiresAt;
  const orgId = parsed.org_id ?? parsed.orgId;
  return {
    baseUrl,
    accessToken,
    ...(typeof refreshToken === 'string' ? { refreshToken } : {}),
    ...(typeof expiresAt === 'string' ? { accessTokenExpiresAt: expiresAt } : {}),
    ...(typeof orgId === 'string' ? { orgId } : {}),
  };
}

/**
 * Every session the native store holds, newest layout first. Mirrors
 * `cloud::staged_auths` plus its legacy fallback: the stage directory is
 * enumerated and each file's own `base_url` identifies it, so this never has
 * to reproduce the engine's stage-key hash.
 */
async function nativeStoredSessions(): Promise<StoredCandidate[]> {
  const home = nativeCloudHome();
  const found: StoredCandidate[] = [];
  let entries: string[] = [];
  try {
    entries = (await readdir(join(home, 'stages'))).filter((name) => name.endsWith('.auth.json'));
  } catch {
    entries = [];
  }
  for (const entry of entries.sort()) {
    const path = join(home, 'stages', entry);
    const auth = await readNativeAuth(path);
    if (auth) found.push({ auth, origin: 'native', path });
  }
  const legacyPath = join(home, 'auth.json');
  const legacy = await readNativeAuth(legacyPath);
  if (legacy
    && !found.some((c) => normalizeStage(c.auth.baseUrl) === normalizeStage(legacy.baseUrl))) {
    found.push({ auth: legacy, origin: 'native', path: legacyPath });
  }
  return found;
}

/** Milliseconds of remaining validity below which a session is treated as spent. */
const EXPIRY_FLOOR_MS = 60_000;

/** Which store a candidate came from. The two carry different written contracts. */
type StoreOrigin = 'native' | 'sdk';

interface StoredCandidate {
  auth: RelayhistoryAuth;
  origin: StoreOrigin;
  /** The file this session was read from, so a rotated pair replaces it in place. */
  path: string;
}

/**
 * Why a stored session cannot serve a recall read, or `null` when it can.
 *
 * For native-store sessions this is `cloud::recall_auth`'s contract, whole: an
 * `rth_at_` access token, an expiry that is present, parseable and at least
 * {@link EXPIRY_FLOOR_MS} away, and a non-blank `org_id`. A session failing any
 * of those is one the engine's own connector already reports as unconfigured,
 * so answering a thread from it would claim a capability the rest of the
 * toolchain denies.
 *
 * The SDK store is held to the subset it can satisfy. {@link loginCloud} has
 * never persisted an expiry or an org, so requiring them there would not
 * enforce a contract — it would retire a login path that works. Teaching
 * `loginCloud` to store both, then holding one bar everywhere, is the follow-up.
 */
function ineligibleReason(candidate: StoredCandidate, now: number): string | null {
  const { auth, origin } = candidate;
  if (!auth.accessToken.startsWith('rth_at_')) {
    return 'the stored relayhistory session has no rth_at_ access token';
  }
  const stated = auth.accessTokenExpiresAt;
  if (origin === 'native' || stated !== undefined) {
    const expiry = stated === undefined ? Number.NaN : Date.parse(stated);
    if (!Number.isFinite(expiry) || expiry < now + EXPIRY_FLOOR_MS) {
      // Expiry is the one precondition rotation exists to repair, so a session
      // that can still rotate is not unconfigured — it is stale, and the
      // transport refreshes it. Without a refresh token there is nothing to
      // rotate and it is unconfigured exactly as `recall_auth` says.
      if (!auth.refreshToken?.trim()) {
        return 'the stored relayhistory access-token expiry is missing, invalid, or less than 60s away';
      }
    }
  }
  if (origin === 'native' && !auth.orgId?.trim()) {
    return 'the stored cloud session has no orgId for provenance';
  }
  return null;
}

/** Why no usable cloud session was found, phrased for {@link cloudUnconfiguredMessage}. */
export type CloudSessionResolution =
  | { auth: RelayhistoryAuth; session?: StoredCandidate }
  | { auth: null; detail: string };

/**
 * Resolve the cloud session a thread read should use.
 *
 * Both stores are consulted, native first: the Rust `ai-hist login` writes
 * `~/.agentworkforce/relayhistory/stages/*.auth.json`, while this SDK's own
 * {@link loginCloud} writes `~/.config/ai-hist/auth.json`. Reading only the
 * latter is what made the documented login flow look unconfigured.
 *
 * A caller that names a stage gets that stage or nothing. A caller that does
 * not, with more than one stage stored, gets a refusal rather than a guess —
 * the same rule as `cloud::load_auth`, and for the same reason: silently
 * picking a stage sends a token somewhere the caller did not ask for.
 */
export async function resolveCloudSession(
  requestedBaseUrl?: string,
  now: number = Date.now(),
): Promise<CloudSessionResolution> {
  const candidates = await nativeStoredSessions();
  const sdkStored = await loadStoredRelayhistoryAuth();
  if (sdkStored
    && !candidates.some((c) => normalizeStage(c.auth.baseUrl) === normalizeStage(sdkStored.baseUrl))) {
    candidates.push({ auth: sdkStored, origin: 'sdk', path: authPath() });
  }

  const where = `${nativeCloudHome()} or ${relayhistoryConfigDir()}`;
  if (candidates.length === 0) {
    return { auth: null, detail: `${where}: no stored relayhistory session (run \`ai-hist login\`)` };
  }

  // Selection first, eligibility second — the engine's order, where `load_auth`
  // picks the stage and `recall_auth` then judges the one it picked.
  let selected: StoredCandidate;
  if (requestedBaseUrl !== undefined) {
    const wanted = normalizeStage(requestedBaseUrl);
    const match = candidates.find((c) => normalizeStage(c.auth.baseUrl) === wanted);
    if (!match) {
      return {
        auth: null,
        detail: `${where}: no stored relayhistory session for ${wanted} `
          + `(stored: ${candidates.map((c) => normalizeStage(c.auth.baseUrl)).join(', ')})`,
      };
    }
    selected = match;
  } else if (candidates.length > 1) {
    return {
      auth: null,
      detail: `${where}: ${candidates.length} relayhistory stages are configured `
        + `(${candidates.map((c) => normalizeStage(c.auth.baseUrl)).join(', ')}); `
        + 'set AI_HIST_BASE_URL to select one. Refusing to guess, because a thread read '
        + 'against the wrong stage answers about a different org',
    };
  } else {
    selected = candidates[0]!;
  }

  const ineligible = ineligibleReason(selected, now);
  return ineligible
    ? { auth: null, detail: `${where}: ${ineligible} (run \`ai-hist login\`)` }
    : { auth: selected.auth, session: selected };
}

/**
 * The unsupported-operation message, in the shape the sibling remote
 * connectors use.
 *
 * The leading phrase `no remote provider connectors are configured` is an
 * explicit compatibility contract that callers and tests match on — see
 * `crates/ai-hist/src/remote.rs::unconfigured_message`, whose format this
 * reproduces for the one connector a thread can be served by (`cloud`).
 */
export function cloudUnconfiguredMessage(operation: string, detail: string): string {
  return `no remote provider connectors are configured: remote session ${operation} is not available (cloud: ${detail})`;
}

/** Most `link_kind` values the recall route accepts in one `kinds` filter. */
const MAX_THREAD_KINDS = 50;

/** The `limit` range the recall route accepts before clamping. */
const MIN_THREAD_LIMIT = 1;
const MAX_THREAD_LIMIT = 500;

/** One lifecycle link, as the recall API returns it. */
export interface SessionThreadLink {
  linkKind: string;
  linkRef: string;
  linkUrl: string | null;
  /** ISO 8601, up to six fractional digits. Null for undated links. */
  linkTs: string | null;
  metadata: unknown;
  /** 0..1, converted server-side from stored basis points. */
  confidence: number | null;
}

/** One shipped-commit outcome, as the recall API returns it. */
export interface SessionThreadOutcome {
  commitSha: string;
  shippedAt: string | null;
  reverted: boolean;
  revertedBySha: string | null;
  revertedAt: string | null;
}

/**
 * The `GET /v1/sessions/:sessionId/thread` envelope. This is a description of
 * what the recall API returns, not a normalization target: the envelope
 * reaches the caller exactly as the service sent it, so a field this SDK build
 * does not know about is passed through rather than dropped.
 */
export interface SessionThread {
  session: {
    source: string;
    sessionId: string;
    orgId: string;
    workspaceId: string;
    firstEventAt: string | null;
    lastEventAt: string | null;
  } | null;
  outcomes: SessionThreadOutcome[];
  links: SessionThreadLink[];
  nextCursor: string | null;
}

export interface SessionThreadQuery {
  /** Upstream source. Validated against this build's source list. */
  source: string;
  /** The session id. Carried in the path, never as a query parameter. */
  sessionId: string;
  /**
   * Optional `link_kind` filter, at most {@link MAX_THREAD_KINDS} values. Sent
   * comma-separated, as the route expects. Not validated against a fixed list:
   * new lenses add kinds, and the route filters on whatever it is given.
   */
  kinds?: readonly string[];
  /** Optional ISO 8601 lower bound. */
  since?: string;
  /** Opaque page cursor from a previous `nextCursor`. */
  cursor?: string;
  /**
   * Links per page. The route clamps to {@link MIN_THREAD_LIMIT}..{@link
   * MAX_THREAD_LIMIT} and defaults to 100. Only links are paged; outcomes come
   * back whole on every page.
   */
  limit?: number;
}

export interface SessionThreadOptions {
  /** Overrides the stored session's base URL. */
  baseUrl?: string;
  /**
   * The connector-status resolver: a usable stored RelayHistory session means
   * the `cloud` connector is configured, and anything else carries the reason
   * it is not. Defaults to {@link resolveCloudSession}.
   */
  resolveSession?: (requestedBaseUrl?: string) => Promise<CloudSessionResolution>;
  /** HTTP transport. Defaults to the global `fetch`. */
  fetchImpl?: typeof fetch;
}

/**
 * Persist a rotated pair over the file it came from, in that store's own
 * schema, via temp-file + rename so a crash cannot leave a torn session.
 *
 * The rotated fields are merged **over the file's existing contents**, never
 * written in place of them. The native store records fields this SDK does not
 * model — `workspace_id` today, whatever the engine adds next — and the Rust
 * CLI reads the same file, so rewriting it whole would quietly delete that
 * state and degrade the CLI's session.
 */
async function persistRotated(candidate: StoredCandidate, auth: RelayhistoryAuth): Promise<void> {
  let existing: Record<string, unknown> = {};
  try {
    const parsed = JSON.parse(await readFile(candidate.path, 'utf-8')) as unknown;
    if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
      existing = parsed as Record<string, unknown>;
    }
  } catch {
    // Unreadable or malformed. Still write, with the fields this SDK knows:
    // leaving a revoked refresh token on disk strands the next caller.
  }
  const rotated: Record<string, unknown> = candidate.origin === 'native'
    ? {
      base_url: existing.base_url ?? auth.baseUrl,
      access_token: auth.accessToken,
      access_token_expires_at: auth.accessTokenExpiresAt ?? null,
      refresh_token: auth.refreshToken ?? null,
      // Carried explicitly: when the existing file could not be read there is
      // nothing to merge, and a native session without an org fails its own
      // provenance precondition on the very next resolve.
      org_id: existing.org_id ?? auth.orgId ?? null,
    }
    : {
      baseUrl: existing.baseUrl ?? auth.baseUrl,
      accessToken: auth.accessToken,
      // Written as `undefined` when absent so `JSON.stringify` drops the key
      // rather than preserving a superseded value from `existing`.
      accessTokenExpiresAt: auth.accessTokenExpiresAt,
      refreshToken: auth.refreshToken,
    };
  const tmp = `${candidate.path}.tmp.${process.pid}.${Date.now()}`;
  await mkdir(dirname(candidate.path), { recursive: true });
  await writeFile(tmp, JSON.stringify({ ...existing, ...rotated }, null, 2), { mode: 0o600 });
  await rename(tmp, candidate.path);
}

/** Re-read this session's file, for the pair another process may have rotated. */
async function rereadSession(candidate: StoredCandidate): Promise<RelayhistoryAuth | null> {
  return candidate.origin === 'native'
    ? readNativeAuth(candidate.path)
    : loadStoredRelayhistoryAuth();
}

/**
 * Exchange the stored refresh token for a new pair. Mirrors
 * `cloud::refresh_auth`: `POST /v1/auth/token/refresh`, `accessToken` and
 * `refreshToken` required in the response, org and stage carried over.
 */
async function refreshCloudSession(
  auth: RelayhistoryAuth,
  baseUrl: string,
  doFetch: typeof fetch,
): Promise<RelayhistoryAuth | null> {
  const refreshToken = auth.refreshToken?.trim();
  if (!refreshToken) return null;
  let resp: Response;
  try {
    resp = await doFetch(`${baseUrl}/v1/auth/token/refresh`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      redirect: 'error',
      body: JSON.stringify({ refreshToken }),
      signal: AbortSignal.timeout(30_000),
    });
  } catch {
    return null;
  }
  if (!resp.ok) return null;
  let payload: Record<string, unknown>;
  try {
    payload = (await resp.json()) as Record<string, unknown>;
  } catch {
    return null;
  }
  const accessToken = payload.accessToken;
  const rotated = payload.refreshToken;
  if (typeof accessToken !== 'string' || typeof rotated !== 'string') return null;
  const expiresAt = payload.accessTokenExpiresAt;
  return {
    baseUrl: auth.baseUrl,
    accessToken,
    refreshToken: rotated,
    ...(typeof expiresAt === 'string' ? { accessTokenExpiresAt: expiresAt } : {}),
    ...(auth.orgId ? { orgId: auth.orgId } : {}),
  };
}

function requireSecureTransport(baseUrl: string): void {
  if (baseUrl.startsWith('https://')) return;
  const authority = baseUrl.startsWith('http://') ? baseUrl.slice('http://'.length) : null;
  const hostPort = authority?.split('/')[0];
  // An IPv6 literal is bracketed and full of colons, so the port cannot be
  // split off before the brackets are removed.
  const host = hostPort?.startsWith('[')
    ? hostPort.slice(1, hostPort.indexOf(']')).toLowerCase()
    : hostPort?.split(':')[0]?.toLowerCase();
  if (host === 'localhost' || host === '127.0.0.1' || host === '::1') return;
  throw new ConnectorFailureError(
    `refusing to send the relayhistory bearer token in cleartext to \`${baseUrl}\` — use an `
      + 'https:// endpoint. Plain http:// is accepted only for loopback.',
    'CONNECTOR_FAILURE',
  );
}

/**
 * Fetch one page of a session's lifecycle thread from the recall API.
 *
 * Cloud-only. When no RelayHistory session is stored the `cloud` connector is
 * unconfigured and this throws {@link UnsupportedOperationError} *without
 * performing a network call* — the same classification the native engine gives
 * a remote-only request no connector can serve.
 */
export async function getSessionThread(
  query: SessionThreadQuery,
  opts: SessionThreadOptions = {},
): Promise<SessionThread> {
  // A misspelled source is an invalid argument, not an unsupported remote
  // request — reject it with the engine's own message before classifying
  // anything, exactly as `ensure_remote_connectors_configured_for_at` does.
  if (!isSource(query.source)) {
    throw new InvalidArgumentError(
      `invalid source '${query.source}' (choose from ${SOURCES.join(', ')})`,
      'INVALID_ARGUMENT',
    );
  }
  if (typeof query.sessionId !== 'string' || query.sessionId.length === 0) {
    throw new InvalidArgumentError('session_id must be a non-empty string', 'INVALID_ARGUMENT');
  }
  if (query.kinds && query.kinds.length > MAX_THREAD_KINDS) {
    throw new InvalidArgumentError(
      `kinds accepts at most ${MAX_THREAD_KINDS} values (got ${query.kinds.length})`,
      'INVALID_ARGUMENT',
    );
  }
  if (query.limit !== undefined
    && (!Number.isInteger(query.limit)
      || query.limit < MIN_THREAD_LIMIT || query.limit > MAX_THREAD_LIMIT)) {
    // Rejected here rather than sent to be clamped, so a caller asking for 5000
    // learns it is getting 500 instead of silently believing it got 5000.
    throw new InvalidArgumentError(
      `limit must be an integer in ${MIN_THREAD_LIMIT}..${MAX_THREAD_LIMIT} (got ${query.limit})`,
      'INVALID_ARGUMENT',
    );
  }

  // A stored session belongs to one stage, so naming a stage selects it rather
  // than redirecting it: sending this stage's bearer token to another host is
  // what `cloud::load_auth`'s `same_stage` check refuses. A stage no stored
  // session can serve is an unconfigured connector, not a request to attempt.
  const resolve = opts.resolveSession ?? resolveCloudSession;
  const resolved = await resolve(requestedStage(opts.baseUrl));
  if (!resolved.auth) {
    throw new UnsupportedOperationError(
      cloudUnconfiguredMessage(THREAD_OPERATION, resolved.detail),
      'UNSUPPORTED_OPERATION',
    );
  }
  const auth = resolved.auth;
  const baseUrl = normalizeStage(auth.baseUrl || DEFAULT_CLOUD_BASE_URL);
  requireSecureTransport(baseUrl);

  const params = new URLSearchParams();
  params.set('source', query.source);
  if (query.kinds?.length) params.set('kinds', query.kinds.join(','));
  if (query.since) params.set('since', query.since);
  if (query.cursor) params.set('cursor', query.cursor);
  if (query.limit !== undefined) params.set('limit', String(query.limit));
  for (const key of params.keys()) {
    // Defence in depth: tenancy comes from the token, never a query parameter.
    if (TENANCY_PARAMS.has(key)) {
      throw new InvalidArgumentError(
        'recall tenancy comes from the token, not query parameters',
        'INVALID_ARGUMENT',
      );
    }
  }

  const url = `${baseUrl}/v1/sessions/${encodeURIComponent(query.sessionId)}/thread?${params}`;
  const doFetch = opts.fetchImpl ?? fetch;

  const send = async (bearer: string): Promise<Response> => {
    try {
      return await doFetch(url, {
        headers: { Authorization: `Bearer ${bearer}`, Accept: 'application/json' },
        // A redirect would carry the bearer token to whatever the hop names.
        redirect: 'error',
        signal: AbortSignal.timeout(30_000),
      });
    } catch (cause) {
      throw new ConnectorFailureError(
        `cloud thread request failed: ${cause instanceof Error ? cause.message : String(cause)}`,
        'CONNECTOR_FAILURE',
        { cause },
      );
    }
  };

  let resp = await send(auth.accessToken);

  // Rotate an expired session at most once, in the order `send_with_auth_refresh`
  // uses: prefer a pair another process already persisted, and only then spend
  // the stored refresh token, which is one-time and revokes its predecessor.
  //
  // The engine serializes this per stage with an flock that Node cannot take
  // here, so instead of preventing a concurrent double-spend this recovers from
  // one: a refresh that loses the race re-reads and adopts the winner's pair.
  // The flock-correct version belongs behind a napi export of the recall
  // transport; see the PR discussion.
  // Only 401 means "this token is spent". `cloud::is_unauthorized` matches the
  // same single status, and the recall API answers 403 for a session that is
  // authenticated but lacks `rth:read` — rotating on that would spend a
  // one-time refresh token to fix a scope problem it cannot fix.
  const rotatable = resolved.session;
  if (resp.status === 401 && rotatable) {
    const current = await rereadSession(rotatable);
    if (current && current.accessToken !== auth.accessToken) {
      resp = await send(current.accessToken);
    } else {
      const refreshed = await refreshCloudSession(auth, baseUrl, doFetch);
      if (refreshed) {
        // Persist before retrying, and never swallow the failure: the old
        // refresh token is already revoked, so a store left holding it strands
        // the next caller — and the `ai-hist` CLI reading the same file with
        // it. Reporting a rotation that could not be saved is better than one
        // silent success followed by an unexplained dead session.
        try {
          await persistRotated(rotatable, refreshed);
        } catch (cause) {
          throw new ConnectorFailureError(
            `the relayhistory session rotated but could not be saved to ${rotatable.path}: `
              + `${cause instanceof Error ? cause.message : String(cause)}. The previous refresh `
              + 'token is now revoked; run `ai-hist login` to store a new session.',
            'CONNECTOR_FAILURE',
            { cause },
          );
        }
        resp = await send(refreshed.accessToken);
      } else {
        // Lost the race, most likely. Whoever won has persisted a live pair.
        const afterward = await rereadSession(rotatable);
        if (afterward && afterward.accessToken !== auth.accessToken) {
          resp = await send(afterward.accessToken);
        }
      }
    }
  }

  if (resp.status === 401) {
    throw new AuthenticationExpiredError(
      'the stored relayhistory session was rejected (HTTP 401); run `ai-hist login`',
      'AUTHENTICATION_EXPIRED',
    );
  }
  if (resp.status === 403) {
    // Authenticated but not permitted: a different session, and a different fix.
    throw new ConnectorFailureError(
      'the stored relayhistory session is not permitted to read this thread (HTTP 403); '
        + 'it needs the `rth:read` scope',
      'CONNECTOR_FAILURE',
    );
  }
  if (!resp.ok) {
    const text = await resp.text().catch(() => '');
    throw new ConnectorFailureError(
      `cloud thread request failed (HTTP ${resp.status}): ${text.slice(0, 200)}`,
      'CONNECTOR_FAILURE',
    );
  }

  try {
    // Passed through unchanged: the recall API owns this shape.
    return (await resp.json()) as SessionThread;
  } catch (cause) {
    throw new ConnectorFailureError(
      'cloud thread response was not valid JSON',
      'CONNECTOR_FAILURE',
      { cause },
    );
  }
}

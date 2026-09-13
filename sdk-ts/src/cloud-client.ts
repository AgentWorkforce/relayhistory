import {
  AuthenticationExpiredError, ConnectorFailureError, InvalidArgumentError, SOURCES,
  UnsupportedOperationError, isSource, resolveCloudSession, refreshCloudSession,
  type CloudSessionResolution,
} from './index.js';

export { loginCloud, loadStoredRelayhistoryAuth, resolveCloudSession } from './index.js';
export type { RelayhistoryAuth, LoginCloudResult, CloudSessionResolution } from './index.js';

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
  const resolved = await resolve(opts.baseUrl);
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

  // Only sessions resolved from the canonical store can spend a refresh token.
  // Rust serializes rotation with the same stage lock used by native requests.
  if (resp.status === 401 && resolved.session) {
    const refreshed = await refreshCloudSession(auth.baseUrl, auth.accessToken);
    if (refreshed) resp = await send(refreshed.accessToken);
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

import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import test from 'node:test';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';
import { fileURLToPath } from 'node:url';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { InvalidArgumentError, UnsupportedOperationError } from './index.js';
import {
  cloudUnconfiguredMessage, getSessionThread, resolveCloudSession,
  type CloudSessionResolution, type RelayhistoryAuth, type SessionThread,
} from './cloud-client.js';

const sourceDir = join(dirname(fileURLToPath(import.meta.url)), '..', 'src');
const repositoryRoot = join(sourceDir, '..', '..');

const AUTH: RelayhistoryAuth = {
  baseUrl: 'https://history.agentrelay.com',
  accessToken: 'rth_at_test',
};

const configured = async (): Promise<CloudSessionResolution> => ({ auth: AUTH });
const unconfigured = async (): Promise<CloudSessionResolution> => (
  { auth: null, detail: '/nowhere: no stored relayhistory session (run `ai-hist login`)' }
);

/** A resolver honouring a requested stage, as the real one does. */
function stagedResolver(...auths: RelayhistoryAuth[]) {
  return async (requested?: string): Promise<CloudSessionResolution> => {
    if (requested === undefined) return { auth: auths[0]! };
    const match = auths.find((a) => a.baseUrl.replace(/\/+$/, '') === requested.replace(/\/+$/, ''));
    return match ? { auth: match } : { auth: null, detail: `no session for ${requested}` };
  };
}

/**
 * A `fetch` stand-in that records every call. Tests assert on `calls.length`
 * directly: "the unsupported path made no request" is only proven by the
 * transport never being reached, never by the shape of the error alone.
 */
function recordingFetch(respond: () => Response) {
  const calls: { url: string; init: RequestInit | undefined }[] = [];
  const impl = (async (input: string | URL | Request, init?: RequestInit) => {
    calls.push({ url: String(input), init });
    return respond();
  }) as unknown as typeof fetch;
  return { impl, calls };
}

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json' },
  });
}

/** A synthetic envelope in the exact shape `GET /v1/sessions/:id/thread` returns. */
const ENVELOPE = {
  session: {
    source: 'claude',
    sessionId: 'session-example',
    orgId: 'org-example',
    workspaceId: 'workspace-example',
    firstEventAt: '2026-09-08T09:00:00.000Z',
    lastEventAt: '2026-09-08T10:00:00.000Z',
  },
  outcomes: [
    {
      commitSha: 'abc123',
      shippedAt: '2026-09-08T11:00:00.000Z',
      reverted: false,
      revertedBySha: null,
      revertedAt: null,
    },
  ],
  links: [
    {
      linkKind: 'github_pr',
      linkRef: 'AgentWorkforce/relayhistory-cloud#123',
      linkUrl: 'https://github.com/AgentWorkforce/relayhistory-cloud/pull/123',
      linkTs: '2026-09-08T10:00:00.000000Z',
      metadata: { state: 'open' },
      confidence: 0.9,
      // A field this SDK build does not know about. It must survive the round
      // trip: the recall API owns this shape, the SDK only carries it.
      unknownFutureField: 'preserved',
    },
  ],
  nextCursor: null,
};

test('a thread envelope reaches the caller unchanged', async () => {
  const { impl, calls } = recordingFetch(() => jsonResponse(ENVELOPE));
  const thread = await getSessionThread(
    { source: 'claude', sessionId: 'session-example' },
    { resolveSession: configured, fetchImpl: impl },
  );
  assert.equal(calls.length, 1);
  assert.deepEqual(thread as unknown, ENVELOPE);
});

test('an unconfigured cloud connector is unsupported and performs no network call', async () => {
  const { impl, calls } = recordingFetch(() => jsonResponse(ENVELOPE));
  await assert.rejects(
    () => getSessionThread(
      { source: 'claude', sessionId: 'session-example' },
      { resolveSession: unconfigured, fetchImpl: impl },
    ),
    (error: unknown) => error instanceof UnsupportedOperationError
      && error.code === 'UNSUPPORTED_OPERATION'
      // The leading phrase is the sibling connectors' compatibility contract.
      && error.message.startsWith('no remote provider connectors are configured')
      && error.message.includes('remote session thread is not available')
      && error.message.includes('(cloud: '),
  );
  assert.equal(calls.length, 0, 'the unsupported path must not reach the transport');
});

test('an invalid source is an invalid argument, not an unsupported remote request', async () => {
  const { impl, calls } = recordingFetch(() => jsonResponse(ENVELOPE));
  // 'agent-relay' is a plausible-looking name that is not a source: the real
  // ones are 'relay' and 'trajectory'. The recall route does not validate
  // this — it answers 200 with an empty envelope — so rejecting it is the
  // client's job.
  await assert.rejects(
    () => getSessionThread(
      { source: 'agent-relay', sessionId: 'session-example' },
      { resolveSession: configured, fetchImpl: impl },
    ),
    (error: unknown) => error instanceof InvalidArgumentError
      && error.code === 'INVALID_ARGUMENT'
      && error.message.includes("invalid source 'agent-relay'")
      && error.message.includes('claude, codex, cursor, grok, relay, trajectory, opencode'),
  );
  assert.equal(calls.length, 0, 'an invalid source must not reach the transport');
});

test('the request carries the session identity and never a tenancy selector', async () => {
  const { impl, calls } = recordingFetch(() => jsonResponse(ENVELOPE));
  await getSessionThread(
    {
      source: 'codex',
      sessionId: 'session/with spaces',
      kinds: ['github_pr', 'incident'],
      since: '2026-09-08T00:00:00Z',
      cursor: 'b64cursor==',
    },
    { resolveSession: configured, fetchImpl: impl },
  );
  assert.equal(calls.length, 1);
  const url = new URL(calls[0]!.url);
  assert.equal(url.pathname, '/v1/sessions/session%2Fwith%20spaces/thread');
  assert.equal(url.searchParams.get('source'), 'codex');
  assert.equal(url.searchParams.get('kinds'), 'github_pr,incident');
  assert.equal(url.searchParams.get('since'), '2026-09-08T00:00:00Z');
  assert.equal(url.searchParams.get('cursor'), 'b64cursor==');
  for (const leak of ['org_id', 'orgId', 'workspace_id', 'workspaceId']) {
    assert.equal(url.searchParams.has(leak), false, `${leak} must never be sent`);
  }
  const headers = new Headers(calls[0]!.init?.headers);
  assert.equal(headers.get('authorization'), 'Bearer rth_at_test');
});

test('optional filters are omitted rather than sent empty', async () => {
  const { impl, calls } = recordingFetch(() => jsonResponse(ENVELOPE));
  await getSessionThread(
    { source: 'claude', sessionId: 'session-example', kinds: [] },
    { resolveSession: configured, fetchImpl: impl },
  );
  const url = new URL(calls[0]!.url);
  assert.equal(url.searchParams.has('kinds'), false);
  assert.equal(url.searchParams.has('since'), false);
  assert.equal(url.searchParams.has('cursor'), false);
});

test('a rejected stored session is reported as expired auth, not as a thread', async () => {
  const { impl } = recordingFetch(() => jsonResponse({ error: 'nope' }, 401));
  await assert.rejects(
    () => getSessionThread(
      { source: 'claude', sessionId: 'session-example' },
      { resolveSession: configured, fetchImpl: impl },
    ),
    (error: unknown) => (error as { code?: string }).code === 'AUTHENTICATION_EXPIRED',
  );
});

test('the bearer token is never sent over cleartext to a non-loopback host', async () => {
  const { impl, calls } = recordingFetch(() => jsonResponse(ENVELOPE));
  const cleartext = async (): Promise<CloudSessionResolution> => (
    { auth: { baseUrl: 'http://history.agentrelay.com', accessToken: 'rth_at_test' } }
  );
  await assert.rejects(
    () => getSessionThread(
      { source: 'claude', sessionId: 'session-example' },
      { resolveSession: cleartext, fetchImpl: impl },
    ),
    (error: unknown) => (error as { code?: string }).code === 'CONNECTOR_FAILURE'
      && String((error as Error).message).includes('cleartext'),
  );
  assert.equal(calls.length, 0);
  // Loopback stays usable for `wrangler dev`.
  const loopback = async (): Promise<CloudSessionResolution> => (
    { auth: { baseUrl: 'http://127.0.0.1:8787', accessToken: 'rth_at_test' } }
  );
  await getSessionThread(
    { source: 'claude', sessionId: 'session-example' },
    { resolveSession: loopback, fetchImpl: impl },
  );
  assert.equal(calls.length, 1);
  assert.equal(new URL(calls[0]!.url).origin, 'http://127.0.0.1:8787');
});

test('a base URL naming another stage is unconfigured, not a cross-stage request', async () => {
  const { impl, calls } = recordingFetch(() => jsonResponse(ENVELOPE));
  // The stored session belongs to one stage. Honouring a base URL that names a
  // different one would send this stage's bearer token to another host.
  await assert.rejects(
    () => getSessionThread(
      { source: 'claude', sessionId: 'session-example' },
      { resolveSession: stagedResolver(AUTH), fetchImpl: impl, baseUrl: 'https://staging.example.com' },
    ),
    (error: unknown) => error instanceof UnsupportedOperationError
      && error.message.startsWith('no remote provider connectors are configured')
      && error.message.includes('https://staging.example.com'),
  );
  assert.equal(calls.length, 0, 'a stage mismatch must not reach the transport');

  // The same stage spelled with a trailing slash is the same stage.
  await getSessionThread(
    { source: 'claude', sessionId: 'session-example' },
    { resolveSession: stagedResolver(AUTH), fetchImpl: impl, baseUrl: 'https://history.agentrelay.com/' },
  );
  assert.equal(calls.length, 1);
});

// A compile-time check that the declared envelope type is what the tests
// exercise; `SessionThread` is a description of the service's shape, so it
// must accept the service's own documented example.
const _typecheck: SessionThread = ENVELOPE as unknown as SessionThread;
void _typecheck;

// ---------------------------------------------------------------------------
// Credential resolution.
//
// The tool reads the store the native `ai-hist login` actually writes. Reading
// only the SDK's own `~/.config/ai-hist/auth.json` is what made the documented
// login flow report the connector unconfigured.
// ---------------------------------------------------------------------------

const STORE_ENV = [
  'RELAYHISTORY_HOME', 'AI_HIST_CONFIG_DIR', 'AI_HIST_BASE_URL', 'RELAYHISTORY_BASE_URL',
] as const;

/** Runs one case against private stores so the developer's own login is invisible. */
async function withStores(
  body: (dirs: { nativeHome: string; sdkDir: string }) => Promise<void>,
): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), 'ai-hist-thread-auth-'));
  const nativeHome = join(root, 'relayhistory');
  const sdkDir = join(root, 'ai-hist');
  await mkdir(join(nativeHome, 'stages'), { recursive: true });
  await mkdir(sdkDir, { recursive: true });
  const saved = new Map(STORE_ENV.map((key) => [key, process.env[key]] as const));
  process.env.RELAYHISTORY_HOME = nativeHome;
  process.env.AI_HIST_CONFIG_DIR = sdkDir;
  delete process.env.AI_HIST_BASE_URL;
  delete process.env.RELAYHISTORY_BASE_URL;
  try {
    await body({ nativeHome, sdkDir });
  } finally {
    for (const [key, value] of saved) {
      if (value === undefined) delete process.env[key]; else process.env[key] = value;
    }
    await rm(root, { recursive: true, force: true });
  }
}

/** One stage file in the native store's snake_case schema. */
async function writeStage(
  nativeHome: string,
  key: string,
  fields: Record<string, unknown>,
): Promise<void> {
  const stage = new URL(String(fields.base_url)).toString().replace(/\/+$/, '');
  const hash = createHash('sha256').update(stage).digest('hex').slice(0, 16);
  await writeFile(join(nativeHome, 'stages', `${hash}.auth.json`), JSON.stringify(fields), { mode: 0o600 });
}

const HOUR_AHEAD = new Date(Date.now() + 3_600_000).toISOString();
/** The fields `cloud::recall_auth` requires of a native-store session. */
const ELIGIBLE = { access_token_expires_at: HOUR_AHEAD, org_id: 'org-example' };

test('a session stored by the native ai-hist login is found', async () => {
  await withStores(async ({ nativeHome }) => {
    // Exactly the shape `cloud::save_auth` writes: snake_case, stage-scoped.
    await writeStage(nativeHome, 'f482e90bb4722263', {
      base_url: 'https://history.agentrelay.com',
      access_token: 'rth_at_native',
      refresh_token: 'rth_rt_native',
      workspace_id: null,
      ...ELIGIBLE,
    });
    const resolved = await resolveCloudSession();
    assert.equal(resolved.auth?.accessToken, 'rth_at_native');
    assert.equal(resolved.auth?.baseUrl, 'https://history.agentrelay.com');
    assert.equal(resolved.auth?.refreshToken, 'rth_rt_native');
  });
});

test('the native store is reached through getSessionThread, not just the resolver', async () => {
  await withStores(async ({ nativeHome }) => {
    await writeStage(nativeHome, 'stage', {
      base_url: 'https://history.agentrelay.com',
      access_token: 'rth_at_native',
      ...ELIGIBLE,
    });
    const { impl, calls } = recordingFetch(() => jsonResponse(ENVELOPE));
    // No resolveSession override: this is the production default path.
    const thread = await getSessionThread(
      { source: 'claude', sessionId: 'session-example' },
      { fetchImpl: impl },
    );
    assert.deepEqual(thread as unknown, ENVELOPE);
    assert.equal(calls.length, 1);
    assert.equal(new Headers(calls[0]!.init?.headers).get('authorization'), 'Bearer rth_at_native');
  });
});

test('obsolete SDK and native single-file credentials are ignored', async () => {
  await withStores(async ({ nativeHome, sdkDir }) => {
    for (const dir of [nativeHome, sdkDir]) {
      await writeFile(join(dir, 'auth.json'), JSON.stringify({
        baseUrl: 'https://history.agentrelay.com', accessToken: 'rth_at_obsolete',
        base_url: 'https://history.agentrelay.com', access_token: 'rth_at_obsolete', ...ELIGIBLE,
      }));
    }
    const resolved = await resolveCloudSession();
    assert.equal(resolved.auth, null);
    assert.ok(resolved.auth === null && resolved.detail.includes('No eligible stored RelayHistory session'));
  });
});

test('two stored stages are a refusal to guess, not a coin flip', async () => {
  await withStores(async ({ nativeHome }) => {
    await writeStage(nativeHome, 'prod', {
      base_url: 'https://history.agentrelay.com',
      access_token: 'rth_at_prod',
      ...ELIGIBLE,
    });
    await writeStage(nativeHome, 'dev', {
      base_url: 'http://127.0.0.1:8787',
      access_token: 'rth_at_dev',
      ...ELIGIBLE,
    });
    const ambiguous = await resolveCloudSession();
    assert.equal(ambiguous.auth, null);
    assert.ok(ambiguous.auth === null && ambiguous.detail.includes('No eligible stored RelayHistory session'));

    // Naming the stage selects it rather than redirecting the other one's token.
    const picked = await resolveCloudSession('http://127.0.0.1:8787');
    assert.equal(picked.auth?.accessToken, 'rth_at_dev');
    const other = await resolveCloudSession('https://history.agentrelay.com');
    assert.equal(other.auth?.accessToken, 'rth_at_prod');
  });
});

test('a native session missing any recall_auth precondition is unconfigured', async () => {
  // `cloud::recall_auth` requires an rth_at_ token, a present+parseable expiry
  // at least 60s away, and a non-blank org. A session failing any of them is
  // one the engine's own connector reports unconfigured, so the tool must not
  // answer from it — and must say which precondition failed.
  const cases: [string, Record<string, unknown>, string][] = [
    ['spent expiry', { ...ELIGIBLE, access_token_expires_at: new Date(Date.now() - 1_000).toISOString() }, 'expiry'],
    ['unparseable expiry', { ...ELIGIBLE, access_token_expires_at: 'not-a-date' }, 'expiry'],
    ['missing expiry', { org_id: 'org-example' }, 'expiry'],
    ['missing org', { access_token_expires_at: HOUR_AHEAD }, 'orgId'],
    ['blank org', { access_token_expires_at: HOUR_AHEAD, org_id: '   ' }, 'orgId'],
  ];
  for (const [label, fields, expected] of cases) {
    await withStores(async ({ nativeHome }) => {
      await writeStage(nativeHome, 'stage', {
        base_url: 'https://history.agentrelay.com',
        access_token: 'rth_at_native',
        ...fields,
      });
      const resolved = await resolveCloudSession();
      assert.equal(resolved.auth, null, label);
      assert.ok(resolved.auth === null && resolved.detail.includes('No eligible stored RelayHistory session'), `${label} names ${expected}`);
    });
  }

  // A token that is not an rth_at_ session is rejected.
  await withStores(async ({ nativeHome }) => {
    await writeStage(nativeHome, 'stage', {
      base_url: 'https://history.agentrelay.com',
      access_token: 'nope_not_a_session',
      ...ELIGIBLE,
    });
    const resolved = await resolveCloudSession();
    assert.equal(resolved.auth, null);
    assert.ok(resolved.auth === null && resolved.detail.includes('No eligible stored RelayHistory session'));
  });
});

test('an ineligible native session performs no network call', async () => {
  await withStores(async ({ nativeHome }) => {
    // The precondition gate has to run before the transport, exactly like the
    // no-session case, or an unconfigured connector becomes a 401 instead.
    await writeStage(nativeHome, 'stage', {
      base_url: 'https://history.agentrelay.com',
      access_token: 'rth_at_native',
      access_token_expires_at: HOUR_AHEAD,
    });
    const { impl, calls } = recordingFetch(() => jsonResponse(ENVELOPE));
    await assert.rejects(
      () => getSessionThread({ source: 'claude', sessionId: 'sid' }, { fetchImpl: impl }),
      (error: unknown) => error instanceof UnsupportedOperationError
        && error.message.startsWith('no remote provider connectors are configured')
        && error.message.includes('No eligible stored RelayHistory session'),
    );
    assert.equal(calls.length, 0);
  });
});

test('a malformed canonical store reports an unconfigured connector without exposing credentials', async () => {
  await withStores(async ({ nativeHome }) => {
    // `JSON.parse` succeeds on all but the first of these, so parsing alone is
    // not enough — reading fields off `null` would throw an unclassified
    // TypeError out through resolveCloudSession.
    for (const [name, body] of [
      ['broken', '{ not json'], ['null', 'null'], ['array', '[]'],
      ['scalar', '42'], ['string', '"nope"'],
    ]) {
      await writeFile(join(nativeHome, 'stages', `${name}.auth.json`), body!, { mode: 0o600 });
    }
    await writeStage(nativeHome, 'good', {
      base_url: 'https://history.agentrelay.com',
      access_token: 'rth_at_good',
      ...ELIGIBLE,
    });
    const resolved = await resolveCloudSession();
    assert.equal(resolved.auth, null);
    assert.ok(resolved.auth === null && resolved.detail.includes('No eligible stored RelayHistory session'));
  });
});

test('a stage path keeps its case while scheme and host are folded', async () => {
  await withStores(async ({ nativeHome }) => {
    // The path is case-sensitive: lowercasing it sends the request elsewhere.
    await writeStage(nativeHome, 'stage', {
      base_url: 'https://History.AgentRelay.COM/Recall',
      access_token: 'rth_at_cased',
      ...ELIGIBLE,
    });
    const { impl, calls } = recordingFetch(() => jsonResponse(ENVELOPE));
    await getSessionThread({ source: 'claude', sessionId: 'sid' }, { fetchImpl: impl });
    const url = new URL(calls[0]!.url);
    assert.equal(url.host, 'history.agentrelay.com');
    assert.equal(url.pathname, '/Recall/v1/sessions/sid/thread');
  });
});

test('IPv6 loopback is accepted over cleartext', async () => {
  const { impl, calls } = recordingFetch(() => jsonResponse(ENVELOPE));
  const ipv6 = async (): Promise<CloudSessionResolution> => (
    { auth: { baseUrl: 'http://[::1]:8787', accessToken: 'rth_at_test' } }
  );
  await getSessionThread(
    { source: 'claude', sessionId: 'session-example' },
    { resolveSession: ipv6, fetchImpl: impl },
  );
  assert.equal(calls.length, 1);
  assert.equal(new URL(calls[0]!.url).origin, 'http://[::1]:8787');
});

test('limit is sent when given and rejected when out of the route range', async () => {
  const { impl, calls } = recordingFetch(() => jsonResponse(ENVELOPE));
  await getSessionThread(
    { source: 'claude', sessionId: 'session-example', limit: 250 },
    { resolveSession: configured, fetchImpl: impl },
  );
  assert.equal(new URL(calls[0]!.url).searchParams.get('limit'), '250');

  for (const limit of [0, 501, 1.5]) {
    await assert.rejects(
      () => getSessionThread(
        { source: 'claude', sessionId: 'session-example', limit },
        { resolveSession: configured, fetchImpl: impl },
      ),
      (error: unknown) => error instanceof InvalidArgumentError
        && error.message.includes('limit must be an integer in 1..500'),
      `limit ${limit} is rejected`,
    );
  }
  assert.equal(calls.length, 1, 'a rejected limit must not reach the transport');
});

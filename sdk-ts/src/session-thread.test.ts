import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import test from 'node:test';
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

test('the unsupported message reproduces the Rust connector format verbatim', async () => {
  // crates/ai-hist/src/remote.rs::unconfigured_message builds
  //   "no remote provider connectors are configured: remote session
  //    {operation} is not available ({connector}: {detail})"
  assert.equal(
    cloudUnconfiguredMessage('thread', 'DETAIL'),
    'no remote provider connectors are configured: remote session thread is not available (cloud: DETAIL)',
  );
  const remote = await readFile(
    join(repositoryRoot, 'crates', 'ai-hist', 'src', 'remote.rs'), 'utf8',
  );
  assert.ok(
    remote.includes(
      'no remote provider connectors are configured: remote session {operation} is not available ({reasons})',
    ),
    'the Rust format string this message mirrors still exists',
  );
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

test('get_session_thread is registered as a cloud-backed read', async () => {
  const mcp = await readFile(join(sourceDir, 'mcp-server.ts'), 'utf8');
  const start = mcp.indexOf("server.tool('get_session_thread'");
  assert.notEqual(start, -1, 'get_session_thread is registered');
  const end = mcp.indexOf("server.tool('", start + 13);
  const registration = mcp.slice(start, end === -1 ? undefined : end);
  assert.match(registration, /source: SOURCE,/, 'the tool validates against SOURCE_CHOICES');
  assert.match(registration, /session_id: z\.string\(\)\.min\(1\)/);
  assert.match(registration, /CLOUD_READ/, 'a thread is a read that reaches the network');
  assert.match(registration, /limit: z\.number\(\)\.int\(\)\.min\(1\)\.max\(500\)/, 'limit is exposed and bounded to the route range');
  assert.doesNotMatch(registration, /SESSION_SCOPE/, 'a thread is addressed by identity');
  assert.doesNotMatch(registration, /org_?[Ii]d/, 'tenancy comes from the token');
  assert.match(mcp, /const CLOUD_READ = \{ readOnlyHint: true, idempotentHint: true, openWorldHint: true \}/);
});

test('the MCP server registers fourteen tools', async () => {
  const mcp = await readFile(join(sourceDir, 'mcp-server.ts'), 'utf8');
  const names = [...mcp.matchAll(/server\.tool\('([a-z_]+)'/g)].map((match) => match[1]);
  assert.equal(names.length, 14, `expected 14 tools, got ${names.length}: ${names.join(', ')}`);
  assert.ok(names.includes('get_session_thread'));
  assert.equal(new Set(names).size, names.length, 'tool names are unique');
});

test('the tool inventories in both READMEs list get_session_thread', async () => {
  for (const readme of ['README.md', join('mcp-package', 'README.md')]) {
    const body = await readFile(join(repositoryRoot, readme), 'utf8');
    assert.ok(body.includes('get_session_thread'), `${readme} documents the tool`);
  }
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

const STORE_ENV = ['RELAYHISTORY_HOME', 'AI_HIST_CONFIG_DIR', 'AI_HIST_BASE_URL'] as const;

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
  await writeFile(join(nativeHome, 'stages', `${key}.auth.json`), JSON.stringify(fields), { mode: 0o600 });
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

test('the SDK store still works when the native store is empty', async () => {
  await withStores(async ({ sdkDir }) => {
    await writeFile(join(sdkDir, 'auth.json'), JSON.stringify({
      baseUrl: 'https://history.agentrelay.com',
      accessToken: 'rth_at_sdk',
    }), { mode: 0o600 });
    const resolved = await resolveCloudSession();
    assert.equal(resolved.auth?.accessToken, 'rth_at_sdk');
  });
});

test('an empty pair of stores is unconfigured and names both', async () => {
  await withStores(async ({ nativeHome, sdkDir }) => {
    const resolved = await resolveCloudSession();
    assert.equal(resolved.auth, null);
    assert.ok(resolved.auth === null && resolved.detail.includes(nativeHome));
    assert.ok(resolved.auth === null && resolved.detail.includes(sdkDir));
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
    assert.ok(ambiguous.auth === null && ambiguous.detail.includes('2 relayhistory stages'));

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
      assert.ok(resolved.auth === null && resolved.detail.includes(expected), `${label} names ${expected}`);
    });
  }

  // A token that is not an rth_at_ session is rejected in either store.
  await withStores(async ({ nativeHome }) => {
    await writeStage(nativeHome, 'stage', {
      base_url: 'https://history.agentrelay.com',
      access_token: 'nope_not_a_session',
      ...ELIGIBLE,
    });
    const resolved = await resolveCloudSession();
    assert.equal(resolved.auth, null);
    assert.ok(resolved.auth === null && resolved.detail.includes('rth_at_'));
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
        && error.message.includes('orgId'),
    );
    assert.equal(calls.length, 0);
  });
});

test('the SDK store is held only to the contract it can satisfy', async () => {
  // `loginCloud` has never written an expiry or an org. Requiring them here
  // would retire a working login path rather than enforce anything.
  await withStores(async ({ sdkDir }) => {
    await writeFile(join(sdkDir, 'auth.json'), JSON.stringify({
      baseUrl: 'https://history.agentrelay.com',
      accessToken: 'rth_at_sdk',
    }), { mode: 0o600 });
    const resolved = await resolveCloudSession();
    assert.equal(resolved.auth?.accessToken, 'rth_at_sdk');
  });

  // A stated expiry is still honoured there when it is spent.
  await withStores(async ({ sdkDir }) => {
    await writeFile(join(sdkDir, 'auth.json'), JSON.stringify({
      baseUrl: 'https://history.agentrelay.com',
      accessToken: 'rth_at_sdk',
      accessTokenExpiresAt: new Date(Date.now() - 1_000).toISOString(),
    }), { mode: 0o600 });
    const resolved = await resolveCloudSession();
    assert.equal(resolved.auth, null);
  });
});

test('a malformed stage file is skipped rather than failing every read', async () => {
  await withStores(async ({ nativeHome }) => {
    await writeFile(join(nativeHome, 'stages', 'broken.auth.json'), '{ not json', { mode: 0o600 });
    await writeStage(nativeHome, 'good', {
      base_url: 'https://history.agentrelay.com',
      access_token: 'rth_at_good',
      ...ELIGIBLE,
    });
    const resolved = await resolveCloudSession();
    assert.equal(resolved.auth?.accessToken, 'rth_at_good');
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

// ---------------------------------------------------------------------------
// Rotation.
//
// An MCP-only install — a WS-4 sandbox, say — has no human to run `ai-hist
// login` and no Rust CLI rotating the store for it, so without this the tool
// simply stops working at the token's ~24h boundary.
// ---------------------------------------------------------------------------

/** A fetch stand-in that answers the thread route by token and the refresh route by script. */
function rotatingFetch(opts: {
  accept: string[];
  refresh?: { accessToken: string; refreshToken: string; expiresAt?: string } | 'reject';
}) {
  const calls: string[] = [];
  const impl = (async (input: string | URL | Request, init?: RequestInit) => {
    const url = String(input);
    if (url.includes('/v1/auth/token/refresh')) {
      calls.push('refresh');
      if (!opts.refresh || opts.refresh === 'reject') return new Response('no', { status: 401 });
      return new Response(JSON.stringify({
        accessToken: opts.refresh.accessToken,
        refreshToken: opts.refresh.refreshToken,
        accessTokenExpiresAt: opts.refresh.expiresAt ?? HOUR_AHEAD,
      }), { status: 200, headers: { 'Content-Type': 'application/json' } });
    }
    const bearer = new Headers(init?.headers).get('authorization')?.replace('Bearer ', '') ?? '';
    calls.push(`thread:${bearer}`);
    return opts.accept.includes(bearer)
      ? new Response(JSON.stringify(ENVELOPE), { status: 200, headers: { 'Content-Type': 'application/json' } })
      : new Response(JSON.stringify({ error: 'invalid_token' }), { status: 401 });
  }) as unknown as typeof fetch;
  return { impl, calls };
}

async function readStage(nativeHome: string, key = 'stage'): Promise<Record<string, unknown>> {
  return JSON.parse(await readFile(join(nativeHome, 'stages', `${key}.auth.json`), 'utf8')) as Record<string, unknown>;
}

test('a rejected token is rotated once and the new pair is persisted', async () => {
  await withStores(async ({ nativeHome }) => {
    await writeStage(nativeHome, 'stage', {
      base_url: 'https://history.agentrelay.com',
      access_token: 'rth_at_old',
      refresh_token: 'rth_rt_old',
      ...ELIGIBLE,
    });
    const { impl, calls } = rotatingFetch({
      accept: ['rth_at_new'],
      refresh: { accessToken: 'rth_at_new', refreshToken: 'rth_rt_new' },
    });
    const thread = await getSessionThread(
      { source: 'claude', sessionId: 'sid' }, { fetchImpl: impl },
    );
    assert.deepEqual(thread as unknown, ENVELOPE);
    assert.deepEqual(calls, ['thread:rth_at_old', 'refresh', 'thread:rth_at_new']);

    // Persisted in the native store's own schema, so the Rust CLI can read it.
    const stored = await readStage(nativeHome);
    assert.equal(stored.access_token, 'rth_at_new');
    assert.equal(stored.refresh_token, 'rth_rt_new');
    assert.equal(stored.org_id, 'org-example', 'the org survives rotation');
    assert.equal(stored.base_url, 'https://history.agentrelay.com');
  });
});

test('rotation preserves native-store fields this SDK does not model', async () => {
  await withStores(async ({ nativeHome }) => {
    // The Rust CLI reads this same file. Rewriting it whole would delete state
    // the SDK has no type for — `workspace_id` today, whatever comes next.
    await writeStage(nativeHome, 'stage', {
      base_url: 'https://history.agentrelay.com',
      access_token: 'rth_at_old',
      refresh_token: 'rth_rt_old',
      workspace_id: 'workspace-example',
      some_future_field: { nested: true },
      ...ELIGIBLE,
    });
    const { impl } = rotatingFetch({
      accept: ['rth_at_new'],
      refresh: { accessToken: 'rth_at_new', refreshToken: 'rth_rt_new' },
    });
    await getSessionThread({ source: 'claude', sessionId: 'sid' }, { fetchImpl: impl });

    const stored = await readStage(nativeHome);
    assert.equal(stored.access_token, 'rth_at_new', 'the pair rotated');
    assert.equal(stored.workspace_id, 'workspace-example', 'workspace survives rotation');
    assert.deepEqual(stored.some_future_field, { nested: true }, 'unmodelled fields survive');
    assert.equal(stored.org_id, 'org-example');
  });
});

test('rotation replaces a superseded expiry rather than keeping the stale one', async () => {
  await withStores(async ({ nativeHome }) => {
    await writeStage(nativeHome, 'stage', {
      base_url: 'https://history.agentrelay.com',
      access_token: 'rth_at_old',
      refresh_token: 'rth_rt_old',
      ...ELIGIBLE,
    });
    const later = new Date(Date.now() + 7_200_000).toISOString();
    const { impl } = rotatingFetch({
      accept: ['rth_at_new'],
      refresh: { accessToken: 'rth_at_new', refreshToken: 'rth_rt_new', expiresAt: later },
    });
    await getSessionThread({ source: 'claude', sessionId: 'sid' }, { fetchImpl: impl });
    const stored = await readStage(nativeHome);
    assert.equal(stored.access_token_expires_at, later, 'merging must not keep the old expiry');
  });
});

test('a pair another process already rotated is adopted without spending the refresh token', async () => {
  await withStores(async ({ nativeHome }) => {
    await writeStage(nativeHome, 'stage', {
      base_url: 'https://history.agentrelay.com',
      access_token: 'rth_at_old',
      refresh_token: 'rth_rt_old',
      ...ELIGIBLE,
    });
    const resolved = await resolveCloudSession();
    assert.equal(resolved.auth?.accessToken, 'rth_at_old');

    // Another process rotates between resolution and the retry.
    await writeStage(nativeHome, 'stage', {
      base_url: 'https://history.agentrelay.com',
      access_token: 'rth_at_theirs',
      refresh_token: 'rth_rt_theirs',
      ...ELIGIBLE,
    });
    const { impl, calls } = rotatingFetch({ accept: ['rth_at_theirs'] });
    await getSessionThread(
      { source: 'claude', sessionId: 'sid' },
      { fetchImpl: impl, resolveSession: async () => resolved },
    );
    assert.deepEqual(calls, ['thread:rth_at_old', 'thread:rth_at_theirs']);
    assert.equal(calls.includes('refresh'), false, 'a one-time refresh token must not be spent needlessly');
  });
});

test('losing the rotation race recovers from the winner\'s persisted pair', async () => {
  await withStores(async ({ nativeHome }) => {
    await writeStage(nativeHome, 'stage', {
      base_url: 'https://history.agentrelay.com',
      access_token: 'rth_at_old',
      refresh_token: 'rth_rt_old',
      ...ELIGIBLE,
    });
    const resolved = await resolveCloudSession();
    // The refresh is rejected because a concurrent process spent the token
    // first; that process then persists its own live pair.
    const { impl, calls } = rotatingFetch({ accept: ['rth_at_winner'], refresh: 'reject' });
    const wrapped = (async (input: string | URL | Request, init?: RequestInit) => {
      const url = String(input);
      const out = await impl(input as never, init as never);
      if (url.includes('/v1/auth/token/refresh')) {
        await writeStage(nativeHome, 'stage', {
          base_url: 'https://history.agentrelay.com',
          access_token: 'rth_at_winner',
          refresh_token: 'rth_rt_winner',
          ...ELIGIBLE,
        });
      }
      return out;
    }) as unknown as typeof fetch;

    const thread = await getSessionThread(
      { source: 'claude', sessionId: 'sid' },
      { fetchImpl: wrapped, resolveSession: async () => resolved },
    );
    assert.deepEqual(thread as unknown, ENVELOPE);
    assert.deepEqual(calls, ['thread:rth_at_old', 'refresh', 'thread:rth_at_winner']);
  });
});

test('a session with no refresh token still fails cleanly rather than looping', async () => {
  await withStores(async ({ nativeHome }) => {
    await writeStage(nativeHome, 'stage', {
      base_url: 'https://history.agentrelay.com',
      access_token: 'rth_at_old',
      ...ELIGIBLE,
    });
    const { impl, calls } = rotatingFetch({ accept: [] });
    await assert.rejects(
      () => getSessionThread({ source: 'claude', sessionId: 'sid' }, { fetchImpl: impl }),
      (error: unknown) => (error as { code?: string }).code === 'AUTHENTICATION_EXPIRED',
    );
    assert.deepEqual(calls, ['thread:rth_at_old'], 'nothing to rotate, so no refresh attempt');
  });
});

test('an expired session that can rotate is refreshed, not reported unconfigured', async () => {
  await withStores(async ({ nativeHome }) => {
    // Expiry is the one precondition rotation repairs. With a refresh token the
    // session is stale, not unconfigured.
    await writeStage(nativeHome, 'stage', {
      base_url: 'https://history.agentrelay.com',
      access_token: 'rth_at_stale',
      refresh_token: 'rth_rt_old',
      access_token_expires_at: new Date(Date.now() - 1_000).toISOString(),
      org_id: 'org-example',
    });
    const resolved = await resolveCloudSession();
    assert.equal(resolved.auth?.accessToken, 'rth_at_stale', 'still resolvable');

    const { impl, calls } = rotatingFetch({
      accept: ['rth_at_fresh'],
      refresh: { accessToken: 'rth_at_fresh', refreshToken: 'rth_rt_fresh' },
    });
    const thread = await getSessionThread({ source: 'claude', sessionId: 'sid' }, { fetchImpl: impl });
    assert.deepEqual(thread as unknown, ENVELOPE);
    assert.deepEqual(calls, ['thread:rth_at_stale', 'refresh', 'thread:rth_at_fresh']);
  });

  await withStores(async ({ nativeHome }) => {
    // Expired with nothing to rotate is still unconfigured, as recall_auth says.
    await writeStage(nativeHome, 'stage', {
      base_url: 'https://history.agentrelay.com',
      access_token: 'rth_at_stale',
      access_token_expires_at: new Date(Date.now() - 1_000).toISOString(),
      org_id: 'org-example',
    });
    const resolved = await resolveCloudSession();
    assert.equal(resolved.auth, null);
    assert.ok(resolved.auth === null && resolved.detail.includes('expiry'));
  });
});

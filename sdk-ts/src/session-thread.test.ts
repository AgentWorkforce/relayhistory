import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import { InvalidArgumentError, UnsupportedOperationError } from './index.js';
import {
  cloudUnconfiguredMessage, getSessionThread,
  type RelayhistoryAuth, type SessionThread,
} from './cloud-client.js';

const sourceDir = join(dirname(fileURLToPath(import.meta.url)), '..', 'src');
const repositoryRoot = join(sourceDir, '..', '..');

const AUTH: RelayhistoryAuth = {
  baseUrl: 'https://history.agentrelay.com',
  accessToken: 'rth_at_test',
};

const configured = async (): Promise<RelayhistoryAuth | null> => AUTH;
const unconfigured = async (): Promise<RelayhistoryAuth | null> => null;

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
    { loadAuth: configured, fetchImpl: impl },
  );
  assert.equal(calls.length, 1);
  assert.deepEqual(thread as unknown, ENVELOPE);
});

test('an unconfigured cloud connector is unsupported and performs no network call', async () => {
  const { impl, calls } = recordingFetch(() => jsonResponse(ENVELOPE));
  await assert.rejects(
    () => getSessionThread(
      { source: 'claude', sessionId: 'session-example' },
      { loadAuth: unconfigured, fetchImpl: impl },
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
      { loadAuth: configured, fetchImpl: impl },
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
    { loadAuth: configured, fetchImpl: impl },
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
    { loadAuth: configured, fetchImpl: impl },
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
      { loadAuth: configured, fetchImpl: impl },
    ),
    (error: unknown) => (error as { code?: string }).code === 'AUTHENTICATION_EXPIRED',
  );
});

test('the bearer token is never sent over cleartext to a non-loopback host', async () => {
  const { impl, calls } = recordingFetch(() => jsonResponse(ENVELOPE));
  const cleartext = async (): Promise<RelayhistoryAuth | null> => (
    { baseUrl: 'http://history.agentrelay.com', accessToken: 'rth_at_test' }
  );
  await assert.rejects(
    () => getSessionThread(
      { source: 'claude', sessionId: 'session-example' },
      { loadAuth: cleartext, fetchImpl: impl },
    ),
    (error: unknown) => (error as { code?: string }).code === 'CONNECTOR_FAILURE'
      && String((error as Error).message).includes('cleartext'),
  );
  assert.equal(calls.length, 0);
  // Loopback stays usable for `wrangler dev`.
  const loopback = async (): Promise<RelayhistoryAuth | null> => (
    { baseUrl: 'http://127.0.0.1:8787', accessToken: 'rth_at_test' }
  );
  await getSessionThread(
    { source: 'claude', sessionId: 'session-example' },
    { loadAuth: loopback, fetchImpl: impl },
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
      { loadAuth: configured, fetchImpl: impl, baseUrl: 'https://staging.example.com' },
    ),
    (error: unknown) => error instanceof UnsupportedOperationError
      && error.message.startsWith('no remote provider connectors are configured')
      && error.message.includes('https://staging.example.com'),
  );
  assert.equal(calls.length, 0, 'a stage mismatch must not reach the transport');

  // The same stage spelled with a trailing slash is the same stage.
  await getSessionThread(
    { source: 'claude', sessionId: 'session-example' },
    { loadAuth: configured, fetchImpl: impl, baseUrl: 'https://history.agentrelay.com/' },
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

import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp, readFile, readdir, rm, writeFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import test from 'node:test';

// CI reruns this suite against the packed/installed SDK and platform addon.
// Import and CLI resolution must both point at that artifact when requested.
const packageDir = process.env.AI_HIST_TEST_PACKAGE_DIR;
const sdkUrl = packageDir ? pathToFileURL(join(packageDir, 'dist/index.js')) : new URL('./index.js', import.meta.url);
const cli = packageDir ? join(packageDir, 'dist/cli.js') : fileURLToPath(new URL('./cli.js', import.meta.url));
const sdk: typeof import('./index.js') = await import(sdkUrl.href);

test('native npm token and replay: secrets, rotation, stages, pagination and atomic output', { timeout: 120_000 }, async () => {
  const saved = { ...process.env };
  const root = await mkdtemp(join(tmpdir(), 'ai-hist-cloud-commands-'));
  const state = join(root, 'state');
  const oldToken = 'rth_at_old_fixture';
  const newToken = 'rth_at_rotated_fixture';
  const refreshToken = 'rth_rt_fixture';
  const future = '2999-01-01T00:00:00Z';
  let refreshes = 0, failRefresh = false;
  let pageMode: 'ok' | 'fail' | 'repeat' = 'ok';
  const requests: URL[] = [];
  const events = [
    { eventId: 'tie-a', ts: '2026-09-08T12:00:00Z', source: 'claude', kind: 'prompt', content: 'first synthetic prompt', futureField: { preserved: true } },
    { eventId: 'tie-b', ts: '2026-09-08T12:00:00Z', source: 'claude', kind: 'response', content: '', contentTruncated: true },
  ];
  const cursor = 'opaque:tie-a/next+page=';
  const server = createServer(async (req, res) => {
    res.setHeader('Content-Type', 'application/json');
    const url = new URL(req.url!, 'http://fixture');
    if (url.pathname.endsWith('/v1/cli/login')) {
      res.end(JSON.stringify({ accessToken: oldToken, refreshToken, accessTokenExpiresAt: future }));
    } else if (url.pathname === '/v1/auth/token/refresh') {
      refreshes++;
      let raw = ''; for await (const chunk of req) raw += chunk;
      assert.equal(JSON.parse(raw).refreshToken, refreshToken);
      if (failRefresh) { res.statusCode = 401; res.end(JSON.stringify({ error: `${oldToken} ${newToken} ${refreshToken}` })); }
      else res.end(JSON.stringify({ accessToken: newToken, refreshToken: 'rth_rt_rotated_fixture', accessTokenExpiresAt: future }));
    } else if (url.pathname === '/v1/sessions/session%2Fwith%20space/events') {
      assert.equal(req.headers.authorization, `Bearer ${newToken}`);
      requests.push(url);
      if (url.searchParams.has('cursor') && pageMode === 'fail') { res.statusCode = 503; res.end('{}'); }
      else res.end(JSON.stringify({
        events: [events[url.searchParams.has('cursor') ? 1 : 0]],
        nextCursor: !url.searchParams.has('cursor') || pageMode === 'repeat' ? cursor : null,
      }));
    } else { res.statusCode = 404; res.end('{}'); }
  });
  server.listen(0, '127.0.0.1'); await once(server, 'listening');
  const baseUrl = `http://127.0.0.1:${(server.address() as { port: number }).port}`;
  async function command(...args: string[]) {
    const child = spawn(process.execPath, [cli, ...args], { env: process.env, cwd: root, stdio: ['ignore', 'pipe', 'pipe'] });
    let stdout = '', stderr = '';
    child.stdout.on('data', chunk => { stdout += chunk; });
    child.stderr.on('data', chunk => { stderr += chunk; });
    const [code] = await once(child, 'close');
    return { code, stdout, stderr };
  }
  function failed(result: Awaited<ReturnType<typeof command>>) {
    assert.notEqual(result.code, 0);
    assert.equal(result.stdout, '');
    for (const secret of [oldToken, newToken, refreshToken]) assert.equal(result.stderr.includes(secret), false);
  }
  try {
    Object.assign(process.env, { HOME: root, USERPROFILE: root, RELAYHISTORY_HOME: state, AI_HIST_CONFIG_DIR: join(root, 'legacy-sdk'), AI_HIST_DB: join(root, 'must-not-open.db'), RUST_LOG: 'trace', RELAYHISTORY_NO_UPDATE_CHECK: '1' });
    for (const key of ['RELAYHISTORY_BASE_URL', 'AI_HIST_BASE_URL', 'CLOUD_API_ACCESS_TOKEN', 'CODEX_HOME']) delete process.env[key];
    await writeFile(process.env.AI_HIST_DB!, 'not a SQLite database');
    failed(await command('token', '--base-url', baseUrl));
    failed(await command('replay', 'session/with space', '--base-url', baseUrl));
    assert.equal((await sdk.loginCloud('fixture-relay-token', { baseUrl })).ok, true);
    const authPath = join(state, 'stages', (await readdir(join(state, 'stages'))).find(f => f.endsWith('.auth.json'))!);
    const auth = JSON.parse(await readFile(authPath, 'utf8'));
    const token = await command('token', '--base-url', baseUrl);
    assert.deepEqual(token, { code: 0, stdout: oldToken + '\n', stderr: '' });
    assert.equal(await sdk.accessToken({ baseUrl }), oldToken);
    assert.equal(refreshes, 0);
    await writeFile(authPath, JSON.stringify({ ...auth, access_token_expires_at: '2000-01-01T00:00:00Z' }));
    const refreshed = await command('token', '--base-url', baseUrl);
    assert.deepEqual(refreshed, { code: 0, stdout: newToken + '\n', stderr: '' });
    assert.equal(refreshes, 1);
    assert.equal(JSON.parse(await readFile(authPath, 'utf8')).refresh_token, 'rth_rt_rotated_fixture');
    assert.equal(await sdk.accessToken({ baseUrl }), newToken);

    const replayArgs = ['replay', 'session/with space', '--base-url', baseUrl, '--limit', '3', '--max-content', '7'];
    const replayed = await command(...replayArgs, '--json');
    assert.equal(replayed.code, 0, replayed.stderr);
    assert.deepEqual(JSON.parse(replayed.stdout), events);
    assert.equal(replayed.stderr, '');
    assert.equal(requests.length, 2, 'a short first page must not stop pagination');
    assert.equal(requests[1].searchParams.get('cursor'), cursor);
    for (const url of requests) {
      assert.equal(url.searchParams.get('order'), 'asc');
      assert.equal(url.searchParams.get('limit'), '3');
      assert.equal(url.searchParams.get('maxContent'), '7');
    }
    const direct = await sdk.replay('session/with space', { baseUrl });
    assert.equal(direct.eventCount, 2);
    assert.equal(direct.outputPath, null);
    assert.match(direct.transcript!, /CONTENT TRUNCATED/);
    assert.ok(direct.transcript!.indexOf('tie-a') < direct.transcript!.indexOf('tie-b'));
    const out = join(root, 'offline transcript.json');
    await writeFile(out, 'original');
    assert.deepEqual(await command(...replayArgs, '--json', '--out', out), { code: 0, stdout: '', stderr: '' });
    const complete = await readFile(out, 'utf8');
    assert.deepEqual(JSON.parse(complete), events);
    pageMode = 'fail';
    failed(await command(...replayArgs, '--json', '--out', out));
    assert.equal(await readFile(out, 'utf8'), complete);
    pageMode = 'repeat';
    const repeated = await command(...replayArgs, '--out', out);
    failed(repeated); assert.match(repeated.stderr, /repeated nextCursor/);
    assert.equal(await readFile(out, 'utf8'), complete);
    pageMode = 'ok';
    const savedReplay = await sdk.replay('session/with space', { baseUrl, out, json: true });
    assert.deepEqual(savedReplay, { eventCount: 2, transcript: null, outputPath: out });

    await writeFile(authPath, JSON.stringify({ ...auth, access_token_expires_at: null }));
    failRefresh = true;
    const rejected = await command('token', '--base-url', baseUrl);
    failed(rejected); assert.match(rejected.stderr, /refreshing relayhistory session failed/);
    await writeFile(authPath, JSON.stringify({ ...auth, access_token: { secret: oldToken } }));
    const malformed = await command('token', '--base-url', baseUrl);
    failed(malformed); assert.match(malformed.stderr, /could not parse stored relayhistory session/);
    await writeFile(authPath, JSON.stringify(auth));
    assert.equal((await sdk.loginCloud('fixture-relay-token', { baseUrl: baseUrl + '/other' })).ok, true);
    const ambiguous = await command('token');
    failed(ambiguous); assert.match(ambiguous.stderr, /Refusing to guess/);
    process.env.RELAYHISTORY_BASE_URL = baseUrl;
    assert.deepEqual(await command('token'), { code: 0, stdout: oldToken + '\n', stderr: '' });
    assert.equal((await command('token', '--base-url', baseUrl + '/other')).stdout, oldToken + '\n');
    for (const args of [['token', '--json'], ['token', 'extra'], ['replay'], [...replayArgs, 'extra'], [...replayArgs, '--limit', '-1']]) failed(await command(...args));
    await assert.rejects(sdk.replay('session', { limit: -1 }), /limit must be an integer/);
    assert.equal(await readFile(process.env.AI_HIST_DB!, 'utf8'), 'not a SQLite database');
    assert.equal((await readdir(state)).some(name => name.endsWith('.db')), false);
  } finally {
    for (const key of Object.keys(process.env)) if (!(key in saved)) delete process.env[key];
    Object.assign(process.env, saved);
    server.closeAllConnections(); await new Promise<void>(resolve => server.close(() => resolve()));
    await rm(root, { recursive: true, force: true });
  }
});

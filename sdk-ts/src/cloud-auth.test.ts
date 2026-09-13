import assert from 'node:assert/strict';
import { once } from 'node:events';
import { createServer } from 'node:http';
import { mkdir, mkdtemp, readFile, readdir, rm, stat, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
// Exercise the installed package entrypoints, including the cloud subpath.
import * as main from 'ai-hist';
import * as cloud from 'ai-hist/cloud';

const FUTURE = '2999-01-01T00:00:00Z';

test('both public imports share login, stage selection, metadata, storage and rotation', { timeout: 30_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'ai-hist-one-auth-'));
  const saved = { ...process.env };
  const state = join(root, 'state');
  const oldSdk = join(root, 'old-sdk');
  const requests: { path: string; body: any; bearer?: string }[] = [];
  let loginCount = 0;
  let refreshCount = 0;
  let accepted = '';
  let status = 200;
  let refreshFails = false;
  let blockSavePath: string | undefined;
  const server = createServer(async (req, res) => {
    let raw = ''; for await (const chunk of req) raw += chunk;
    const body = raw ? JSON.parse(raw) : {};
    requests.push({ path: req.url!, body, bearer: req.headers.authorization });
    res.setHeader('Content-Type', 'application/json');
    if (req.url?.endsWith('/v1/cli/login')) {
      loginCount++;
      accepted = `rth_at_login_${loginCount}`;
      res.end(JSON.stringify({ accessToken: accepted, refreshToken: `rth_rt_login_${loginCount}`,
        accessTokenExpiresAt: FUTURE, orgId: 'org-test', workspaceId: 'workspace-test' }));
    } else if (req.url === '/v1/auth/token/refresh') {
      refreshCount++;
      if (refreshFails) { res.statusCode = 401; res.end('{}'); return; }
      if (blockSavePath) {
        await rm(blockSavePath);
        await mkdir(blockSavePath);
      }
      accepted = `rth_at_rotated_${refreshCount}`;
      res.end(JSON.stringify({ accessToken: accepted, refreshToken: `rth_rt_rotated_${refreshCount}`,
        accessTokenExpiresAt: FUTURE }));
    } else {
      res.statusCode = status === 200 && req.headers.authorization !== `Bearer ${accepted}` ? 401 : status;
      res.end(JSON.stringify({ session: null, outcomes: [], links: [], nextCursor: null }));
    }
  });
  try {
    Object.assign(process.env, { HOME: root, USERPROFILE: root, RELAYHISTORY_HOME: state, AI_HIST_CONFIG_DIR: oldSdk });
    delete process.env.RELAYHISTORY_BASE_URL;
    delete process.env.AI_HIST_BASE_URL;
    server.listen(0, '127.0.0.1'); await once(server, 'listening');
    const base = `http://127.0.0.1:${(server.address() as { port: number }).port}`;
    const second = `${base}/SecondStage`;
    await mkdir(oldSdk, { recursive: true });
    await mkdir(state, { recursive: true });
    for (const dir of [oldSdk, state]) await writeFile(join(dir, 'auth.json'), 'obsolete malformed auth');
    assert.equal(main.loginCloud, cloud.loginCloud);
    assert.equal(main.loadStoredRelayhistoryAuth, cloud.loadStoredRelayhistoryAuth);
    for (const entry of [main, cloud]) assert.equal(await entry.loadStoredRelayhistoryAuth(), null);

    process.env.RELAYHISTORY_BASE_URL = `${base}/`;
    process.env.AI_HIST_BASE_URL = second;
    for (const entry of [main, cloud]) {
      const result = await entry.loginCloud('supplied-bearer', { label: 'same-label' });
      assert.equal(result.ok, true);
      assert.deepEqual(result.auth, { baseUrl: base, accessToken: accepted,
        refreshToken: `rth_rt_login_${loginCount}`, accessTokenExpiresAt: FUTURE,
        orgId: 'org-test', workspaceId: 'workspace-test' });
      for (const reader of [main, cloud]) assert.deepEqual(await reader.loadStoredRelayhistoryAuth(), result.auth);
      assert.equal(requests.at(-1)?.path, '/v1/cli/login');
      assert.deepEqual(requests.at(-1)?.body, { agentRelayToken: 'supplied-bearer', label: 'same-label', mode: 'sync' });
    }
    const files = await readdir(join(state, 'stages'));
    assert.equal(files.length, 1, 'both logins overwrite the same canonical stage file');
    const path = join(state, 'stages', files[0]!);
    assert.equal((await stat(path)).mode & 0o777, 0o600);
    const stored = JSON.parse(await readFile(path, 'utf8'));
    assert.equal(stored.access_token_expires_at, FUTURE);
    assert.equal(stored.org_id, 'org-test');
    assert.equal(stored.workspace_id, 'workspace-test');
    for (const dir of [oldSdk, state]) assert.equal(await readFile(join(dir, 'auth.json'), 'utf8'), 'obsolete malformed auth');

    // HTTP to a remote host is rejected before attempting an exchange.
    const before = requests.length;
    for (const entry of [main, cloud]) {
      const result = await entry.loginCloud('must-not-send', { baseUrl: 'http://remote.invalid' });
      assert.equal(result.ok, false);
      if (!result.ok) assert.match(result.error, /cleartext|https:\/\//);
    }
    assert.equal(requests.length, before);
    assert.equal(await readFile(path, 'utf8'), JSON.stringify(stored, null, 2));

    // The same strict environment selection applies to login and reads.
    process.env.RELAYHISTORY_BASE_URL = 'malformed';
    for (const entry of [main, cloud]) {
      assert.equal((await entry.loginCloud('must-not-send')).ok, false);
      await assert.rejects(entry.loadStoredRelayhistoryAuth(), /RELAYHISTORY_BASE_URL/);
      assert.equal((await entry.loadStoredRelayhistoryAuth(base))?.baseUrl, base);
      assert.equal((await entry.loginCloud('explicit', { baseUrl: second })).ok, true);
    }
    delete process.env.RELAYHISTORY_BASE_URL;
    for (const entry of [main, cloud]) assert.equal((await entry.loadStoredRelayhistoryAuth())?.baseUrl, second);
    delete process.env.AI_HIST_BASE_URL;
    for (const entry of [main, cloud]) await assert.rejects(entry.loadStoredRelayhistoryAuth(), /Refusing to guess/);
    assert.equal((await cloud.resolveCloudSession()).auth, null);
    process.env.RELAYHISTORY_BASE_URL = base;

    // Concurrent thread reads rotate through Rust, preserving all native metadata.
    accepted = 'rth_at_reject_previous';
    const query = { source: 'claude', sessionId: 'sid' };
    await Promise.all(Array.from({ length: 4 }, () => cloud.getSessionThread(query)));
    assert.equal(refreshCount, 1, 'one-time refresh is serialized across concurrent callers');
    for (const entry of [main, cloud]) {
      const auth = await entry.loadStoredRelayhistoryAuth();
      assert.equal(auth?.accessToken, accepted);
      assert.equal(auth?.accessTokenExpiresAt, FUTURE);
      assert.equal(auth?.orgId, 'org-test');
      assert.equal(auth?.workspaceId, 'workspace-test');
    }
    const rotated = JSON.parse(await readFile(path, 'utf8'));
    assert.equal(rotated.refresh_token, 'rth_rt_rotated_1');
    assert.equal((await stat(path)).mode & 0o777, 0o600);
    assert.equal(await main.accessToken(), accepted);

    // A stale snapshot adopts a newer persisted pair without spending it again.
    const snapshot = await cloud.resolveCloudSession();
    await main.login({ baseUrl: base, relayAccessToken: 'replacement' });
    await cloud.getSessionThread(query, { resolveSession: async () => snapshot });
    assert.equal(refreshCount, 1);

    // Expired credentials remain refreshable; status probes do not perform I/O.
    const expired = JSON.parse(await readFile(path, 'utf8'));
    expired.access_token_expires_at = '2000-01-01T00:00:00Z';
    await writeFile(path, JSON.stringify(expired));
    const readsBefore = requests.length;
    assert.ok((await cloud.resolveCloudSession()).auth);
    assert.equal(requests.length, readsBefore);
    accepted = 'rth_at_reject_expired';
    await cloud.getSessionThread(query);
    assert.equal(refreshCount, 2);

    status = 403;
    await assert.rejects(cloud.getSessionThread(query), /rth:read/);
    assert.equal(refreshCount, 2, 'permission failures never rotate credentials');
    status = 200;
    accepted = 'rth_at_reject';
    refreshFails = true;
    await assert.rejects(cloud.getSessionThread(query), /refreshing relayhistory session/);
    assert.equal(refreshCount, 3, 'failed refresh is attempted only once');

    // Without a refresh token, a rejection is final and no refresh is attempted.
    const noRefresh = JSON.parse(await readFile(path, 'utf8'));
    noRefresh.refresh_token = null;
    await writeFile(path, JSON.stringify(noRefresh));
    await assert.rejects(cloud.getSessionThread(query), /HTTP 401/);
    assert.equal(refreshCount, 3);

    // Never report success if the newly rotated pair cannot be persisted.
    await main.login({ baseUrl: base, relayAccessToken: 'replacement' });
    accepted = 'rth_at_reject';
    refreshFails = false;
    blockSavePath = path;
    const requestCount = requests.length;
    await assert.rejects(cloud.getSessionThread(query), /persisting refreshed relayhistory session/);
    assert.equal(refreshCount, 4);
    assert.equal(requests.length - requestCount, 2, 'failed save prevents a retry with an unpersisted pair');
  } finally {
    server.closeAllConnections();
    if (server.listening) await new Promise<void>((resolve) => server.close(() => resolve()));
    for (const key of Object.keys(process.env)) if (!(key in saved)) delete process.env[key];
    Object.assign(process.env, saved);
    await rm(root, { recursive: true, force: true });
  }
});

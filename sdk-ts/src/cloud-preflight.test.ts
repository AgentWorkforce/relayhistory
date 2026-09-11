import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';

import { RelayHistoryError } from './index.js';
import {
  prepareCloudSessionForEnableCloud,
  shouldPrepareCloudSession,
} from './cloud-preflight.js';

test('enable-cloud prepares and returns an Agent Relay Cloud SDK token', async () => {
  const env: NodeJS.ProcessEnv = {};
  let received: Record<string, unknown> | undefined;
  const token = await prepareCloudSessionForEnableCloud(
    'enable-cloud',
    ['enable-cloud', '--once'],
    env,
    {
      interactive: true,
      ensureCloudSession: async (options) => {
        received = options as unknown as Record<string, unknown>;
        return { auth: { accessToken: 'cld_at_fixture' } };
      },
    },
  );

  assert.equal(token, 'cld_at_fixture');
  assert.equal(received?.apiUrl, 'https://agentrelay.com/cloud');
  assert.equal(received?.client, 'relayhistory');
  assert.equal(received?.interactive, true);
  assert.equal(received?.loginTimeoutMs, 300_000);
  assert.equal(received?.refreshTimeoutMs, 10_000);
  assert.equal(received?.env, env);
  assert.equal(received?.signal instanceof AbortSignal, true);
});

test('explicit and environment tokens bypass Agent Relay Cloud preflight', async () => {
  assert.equal(shouldPrepareCloudSession('enable-cloud', ['enable-cloud', '--token', 'fixture'], {}), false);
  assert.equal(shouldPrepareCloudSession('login', ['login', '--token=fixture'], {}), false);
  assert.equal(
    shouldPrepareCloudSession('enable-cloud', ['enable-cloud'], { CLOUD_API_ACCESS_TOKEN: 'fixture' }),
    false,
  );
  assert.equal(shouldPrepareCloudSession('search', ['search', 'cloud'], {}), false);
  assert.equal(
    shouldPrepareCloudSession('enable-cloud', ['--no-warning', 'enable-cloud', '--once'], {}),
    true,
  );

  let calls = 0;
  const prepared = await prepareCloudSessionForEnableCloud(
    'enable-cloud',
    ['enable-cloud', '--once'],
    { CLOUD_API_ACCESS_TOKEN: 'fixture' },
    { ensureCloudSession: async () => { calls++; return { auth: { accessToken: 'unused' } }; } },
  );
  assert.equal(prepared, null);
  assert.equal(calls, 0);
});

test('non-interactive preflight fails fast with token and terminal guidance', async () => {
  const started = Date.now();
  await assert.rejects(
    prepareCloudSessionForEnableCloud(
      'enable-cloud',
      ['enable-cloud', '--once'],
      {},
      {
        interactive: false,
        ensureCloudSession: async (options) => {
          assert.equal(options.interactive, false);
          throw Object.assign(new Error('Cloud login required'), { code: 'AUTH_BROWSER_REQUIRED' });
        },
      },
    ),
    (error: unknown) => error instanceof RelayHistoryError
      && error.code === 'CLOUD_AUTH_FAILED'
      && /interactive terminal.*--token.*CLOUD_API_ACCESS_TOKEN/.test(error.message),
  );
  assert.ok(Date.now() - started < 1_000);
});

test('npm CLI reuses the canonical Agent Relay Cloud session without its CLI', { timeout: 10_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'ai-hist-cloud-session-'));
  const cloudAuthDir = join(root, '.agentworkforce', 'relay');
  await mkdir(cloudAuthDir, { recursive: true });
  await writeFile(join(cloudAuthDir, 'cloud-auth.json'), JSON.stringify({
    apiUrl: 'https://agentrelay.com/cloud',
    accessToken: 'cld_at_shared_fixture',
    refreshToken: 'cld_rt_shared_fixture',
    accessTokenExpiresAt: '2999-01-01T00:00:00.000Z',
  }));

  let exchanges = 0;
  const server = createServer(async (request, response) => {
    let body = '';
    for await (const chunk of request) body += chunk;
    const payload = JSON.parse(body) as { agentRelayToken?: string };
    assert.equal(payload.agentRelayToken, 'cld_at_shared_fixture');
    exchanges++;
    response.setHeader('content-type', 'application/json');
    response.end(JSON.stringify({
      accessToken: 'rth_at_fixture',
      refreshToken: 'rth_rt_fixture',
      accessTokenExpiresAt: '2999-01-01T00:00:00.000Z',
    }));
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const baseUrl = `http://127.0.0.1:${(server.address() as { port: number }).port}`;
  const cli = fileURLToPath(new URL('./cli.js', import.meta.url));
  const env: NodeJS.ProcessEnv = {
    ...process.env,
    HOME: root,
    USERPROFILE: root,
    RELAYHISTORY_HOME: join(root, 'relayhistory'),
    RELAYHISTORY_NO_UPDATE_CHECK: '1',
    RELAYHISTORY_ALLOW_UNTRUSTED_CLOUD_BASE_URL: '1',
    AGENT_RELAY_BIN: join(root, 'does-not-exist'),
  };
  for (const key of [
    'CLOUD_API_ACCESS_TOKEN',
    'CLOUD_API_REFRESH_TOKEN',
    'CLOUD_API_ACCESS_TOKEN_EXPIRES_AT',
    'CLOUD_API_REFRESH_TOKEN_EXPIRES_AT',
  ]) delete env[key];

  const child = spawn(process.execPath, [cli, 'login', '--base-url', baseUrl, '--json'], {
    cwd: root,
    env,
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let stdout = '', stderr = '';
  child.stdout.on('data', (chunk) => { stdout += chunk; });
  child.stderr.on('data', (chunk) => { stderr += chunk; });
  const [code] = await once(child, 'close');

  try {
    assert.equal(code, 0, stderr);
    assert.equal(exchanges, 1);
    assert.deepEqual(JSON.parse(stdout), { ok: true, base_url: baseUrl });

    // Once RelayHistory has its own stage session, enable-cloud must use it
    // without requiring the separate Agent Relay auth store.
    await rm(join(cloudAuthDir, 'cloud-auth.json'));
    const enableChild = spawn(process.execPath, [
      cli, '--no-warning', 'enable-cloud', '--base-url', baseUrl, '--once', '--json',
    ], {
      cwd: root,
      env,
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    let enableStdout = '', enableStderr = '';
    enableChild.stdout.on('data', (chunk) => { enableStdout += chunk; });
    enableChild.stderr.on('data', (chunk) => { enableStderr += chunk; });
    const [enableCode] = await once(enableChild, 'close');
    assert.equal(enableCode, 0, enableStderr);
    assert.equal(JSON.parse(enableStdout).base_url, baseUrl);
    assert.equal(exchanges, 1, 'stored RelayHistory auth must avoid another Cloud exchange');
  } finally {
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await rm(root, { recursive: true, force: true });
  }
});

test('npm CLI trust-gates an SDK bearer before a custom base-url exchange', { timeout: 10_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'ai-hist-cloud-trust-'));
  const cloudAuthDir = join(root, '.agentworkforce', 'relay');
  await mkdir(cloudAuthDir, { recursive: true });
  await writeFile(join(cloudAuthDir, 'cloud-auth.json'), JSON.stringify({
    apiUrl: 'https://agentrelay.com/cloud',
    accessToken: 'cld_at_shared_fixture',
    refreshToken: 'cld_rt_shared_fixture',
    accessTokenExpiresAt: '2999-01-01T00:00:00.000Z',
  }));

  let requests = 0;
  const server = createServer((_request, response) => {
    requests++;
    response.end('{}');
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const baseUrl = `http://127.0.0.1:${(server.address() as { port: number }).port}`;
  const cli = fileURLToPath(new URL('./cli.js', import.meta.url));
  const env: NodeJS.ProcessEnv = {
    ...process.env,
    HOME: root,
    USERPROFILE: root,
    RELAYHISTORY_HOME: join(root, 'relayhistory'),
    RELAYHISTORY_NO_UPDATE_CHECK: '1',
  };
  delete env.RELAYHISTORY_ALLOW_UNTRUSTED_CLOUD_BASE_URL;
  delete env.CLOUD_API_ACCESS_TOKEN;

  const child = spawn(process.execPath, [cli, 'login', '--base-url', baseUrl], {
    cwd: root,
    env,
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let stderr = '';
  child.stderr.on('data', (chunk) => { stderr += chunk; });
  const [code] = await once(child, 'close');

  try {
    assert.notEqual(code, 0);
    assert.match(stderr, /refusing to send the Agent Relay Cloud bearer/);
    assert.equal(requests, 0, 'the SDK bearer must not reach an untrusted destination');
  } finally {
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await rm(root, { recursive: true, force: true });
  }
});

test('npm CLI exits promptly without a TTY instead of starting native login', { timeout: 10_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'ai-hist-cloud-preflight-'));
  const cli = fileURLToPath(new URL('./cli.js', import.meta.url));
  const env: NodeJS.ProcessEnv = {
    ...process.env,
    HOME: root,
    USERPROFILE: root,
    RELAYHISTORY_HOME: join(root, 'relayhistory'),
    RELAYHISTORY_NO_UPDATE_CHECK: '1',
  };
  for (const key of [
    'CLOUD_API_ACCESS_TOKEN',
    'CLOUD_API_REFRESH_TOKEN',
    'CLOUD_API_ACCESS_TOKEN_EXPIRES_AT',
    'CLOUD_API_REFRESH_TOKEN_EXPIRES_AT',
  ]) delete env[key];

  const started = Date.now();
  const child = spawn(process.execPath, [cli, 'enable-cloud', '--once'], {
    cwd: root,
    env,
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let stdout = '', stderr = '';
  child.stdout.on('data', (chunk) => { stdout += chunk; });
  child.stderr.on('data', (chunk) => { stderr += chunk; });
  const kill = setTimeout(() => child.kill('SIGKILL'), 5_000);
  const [code, signal] = await once(child, 'close') as [number | null, NodeJS.Signals | null];
  clearTimeout(kill);

  try {
    assert.equal(signal, null, `CLI was killed after hanging: ${stderr}`);
    assert.notEqual(code, 0);
    assert.equal(stdout, '');
    assert.match(stderr, /interactive terminal/);
    assert.match(stderr, /--token|CLOUD_API_ACCESS_TOKEN/);
    assert.doesNotMatch(stderr, /install Agent Relay|agent-relay cloud login/);
    assert.ok(Date.now() - started < 5_000);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

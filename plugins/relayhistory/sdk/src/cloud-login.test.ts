/**
 * End-to-end CLI coverage for Agent Relay Cloud sign-in.
 *
 * Sign-in itself lives in the Rust helper: there is no JavaScript Cloud SDK,
 * no bundled `ensureCloudSession`, and nothing in this package that can read a
 * Cloud credential. These tests therefore drive the shipped `dist/cli.js`
 * against the real helper (`RELAYHISTORY_PLUGIN_BIN`) and a fake RelayHistory
 * server, which is the only place the behaviour is observable.
 */
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';

const CLI = fileURLToPath(new URL('./cli.js', import.meta.url));

/** The env vars that would hand the helper an identity the test did not set up. */
const CLOUD_TOKEN_VARS = [
  'CLOUD_API_ACCESS_TOKEN',
  'CLOUD_API_REFRESH_TOKEN',
  'CLOUD_API_ACCESS_TOKEN_EXPIRES_AT',
  'CLOUD_API_REFRESH_TOKEN_EXPIRES_AT',
];

function cliEnv(root: string, extra: NodeJS.ProcessEnv = {}): NodeJS.ProcessEnv {
  const env: NodeJS.ProcessEnv = {
    ...process.env,
    HOME: root,
    USERPROFILE: root,
    RELAYHISTORY_HOME: join(root, 'relayhistory'),
    RELAYHISTORY_NO_UPDATE_CHECK: '1',
    ...extra,
  };
  for (const key of CLOUD_TOKEN_VARS) delete env[key];
  return env;
}

/** Run the packaged CLI with no stdin, so `process.stdin.isTTY` is never true. */
async function runCli(args: readonly string[], options: { cwd: string; env: NodeJS.ProcessEnv }) {
  const child = spawn(process.execPath, [CLI, ...args], {
    cwd: options.cwd,
    env: options.env,
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let stdout = '', stderr = '';
  child.stdout.on('data', (chunk) => { stdout += chunk; });
  child.stderr.on('data', (chunk) => { stderr += chunk; });
  const kill = setTimeout(() => child.kill('SIGKILL'), 5_000);
  const [code, signal] = await once(child, 'close') as [number | null, NodeJS.Signals | null];
  clearTimeout(kill);
  return { code, signal, stdout, stderr };
}

/** The canonical Agent Relay CLI session file, written where the helper reads it. */
async function writeCloudAuth(root: string): Promise<string> {
  const dir = join(root, '.agentworkforce', 'relay');
  await mkdir(dir, { recursive: true });
  await writeFile(join(dir, 'cloud-auth.json'), JSON.stringify({
    apiUrl: 'https://agentrelay.com/cloud',
    accessToken: 'cld_at_shared_fixture',
    refreshToken: 'cld_rt_shared_fixture',
    accessTokenExpiresAt: '2999-01-01T00:00:00.000Z',
  }));
  return dir;
}

test('npm CLI reuses the canonical Agent Relay Cloud session without its CLI', { timeout: 10_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'ai-hist-cloud-session-'));
  const cloudAuthDir = await writeCloudAuth(root);

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
  const env = cliEnv(root, {
    RELAYHISTORY_ALLOW_UNTRUSTED_CLOUD_BASE_URL: '1',
    // The stored session must be enough on its own: no Agent Relay CLI exists here.
  });

  try {
    const login = await runCli(['login', '--base-url', baseUrl, '--json'], { cwd: root, env });
    assert.equal(login.code, 0, login.stderr);
    assert.equal(exchanges, 1);
    assert.deepEqual(JSON.parse(login.stdout), { ok: true, base_url: baseUrl });

    // Once RelayHistory has its own stage session, enable-cloud must use it
    // without requiring the separate Agent Relay auth store.
    await rm(join(cloudAuthDir, 'cloud-auth.json'));
    const enable = await runCli(
      ['--no-warning', 'enable-cloud', '--base-url', baseUrl, '--once', '--json'],
      { cwd: root, env },
    );
    assert.equal(enable.code, 0, enable.stderr);
    assert.equal(JSON.parse(enable.stdout).base_url, baseUrl);
    assert.equal(exchanges, 1, 'stored RelayHistory auth must avoid another Cloud exchange');
  } finally {
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await rm(root, { recursive: true, force: true });
  }
});

test('npm CLI trust-gates an SDK bearer before a custom base-url exchange', { timeout: 10_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'ai-hist-cloud-trust-'));
  await writeCloudAuth(root);

  let requests = 0;
  const server = createServer((_request, response) => {
    requests++;
    response.end('{}');
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const baseUrl = `http://127.0.0.1:${(server.address() as { port: number }).port}`;
  const env = cliEnv(root);
  delete env.RELAYHISTORY_ALLOW_UNTRUSTED_CLOUD_BASE_URL;

  try {
    const { code, stderr } = await runCli(['login', '--base-url', baseUrl], { cwd: root, env });
    assert.notEqual(code, 0);
    assert.match(stderr, /CLOUD_LOGIN_FAILED/);
    assert.equal(requests, 0, 'the SDK bearer must not reach an untrusted destination');
  } finally {
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await rm(root, { recursive: true, force: true });
  }
});

test('npm CLI exits promptly without a TTY instead of starting native login', { timeout: 20_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'ai-hist-cloud-no-tty-'));
  // No stored Agent Relay session, no environment bearer, no terminal: the
  // helper has nothing to sign in with and must say so instead of starting a
  // browser/device approval nobody can complete.
  const env = cliEnv(root);

  try {
    for (const args of [['login'], ['enable-cloud', '--once']]) {
      const started = Date.now();
      const { code, signal, stdout, stderr } = await runCli(args, { cwd: root, env });
      assert.equal(signal, null, `CLI was killed after hanging (${args[0]}): ${stderr}`);
      assert.notEqual(code, 0, `${args[0]} unexpectedly succeeded`);
      assert.equal(stdout, '');
      assert.match(stderr, /CLOUD_(LOGIN|ENABLE)_FAILED/);
      // The bundled Cloud SDK is gone, and so is any advice to install a CLI.
      assert.doesNotMatch(stderr, /install Agent Relay|agent-relay cloud login/);
      assert.ok(Date.now() - started < 5_000, `${args[0]} took too long to give up`);
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

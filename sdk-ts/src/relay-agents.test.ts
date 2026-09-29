import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { chmod, mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { createConnection, createServer, type Server, type Socket } from 'node:net';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';
import {
  joinRelay, leaveRelay, listRelayAgents, relaySocketCandidates, relayStatus,
  RelayAgentsError,
} from './relay-agents.js';

const NOT_RUNNING = "Agent Relay desktop isn't running on this machine; open it, or use the Agent Relay MCP";

async function fakeRosterServer(
  t: test.TestContext, path: string, requests: string[],
  options: {
    notAllowed?: boolean;
    hangMutations?: boolean;
    truncateMutations?: boolean;
    closeDelimitedMutations?: boolean;
    malformedMutations?: boolean;
    oversizedMutations?: boolean;
  } = {},
): Promise<Server> {
  await mkdir(dirname(path), { recursive: true });
  let registered = false;
  const server = createServer((connection) => {
    let request = '';
    connection.setEncoding('utf8');
    let responded = false;
    connection.on('data', (chunk) => {
      request += chunk;
      if (responded || !request.includes('\r\n\r\n')) return;
      responded = true;
      requests.push(request);
      if (options.hangMutations && /^(POST|DELETE) \/register /.test(request)) return;
      if (options.truncateMutations && /^(POST|DELETE) \/register /.test(request)) {
        const partial = '{"ok":true,"data":';
        connection.end(`HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: ${Buffer.byteLength(partial) + 20}\r\nConnection: close\r\n\r\n${partial}`);
        return;
      }
      if (options.malformedMutations && /^(POST|DELETE) \/register /.test(request)) {
        const malformed = '{"ok":true,"data":';
        connection.end(`HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: ${Buffer.byteLength(malformed)}\r\nConnection: close\r\n\r\n${malformed}`);
        return;
      }
      if (options.oversizedMutations && /^(POST|DELETE) \/register /.test(request)) {
        const oversized = 'x'.repeat((1024 * 1024) + 1);
        connection.end(`HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: ${Buffer.byteLength(oversized)}\r\nConnection: close\r\n\r\n${oversized}`);
        return;
      }
      let status = 200;
      let data: unknown;
      if (request.startsWith('GET /agents')) {
        data = {
          agents: [{
            name: 'bob', address: 'bob@laptop', kind: 'agent', where: 'this_computer',
            status: 'active', last_seen_ms: 1_790_683_200_000,
            description: 'Fix the roster', is_self: true,
          }],
          fetched_at_ms: 1_790_683_200_123,
        };
      } else if (request.startsWith('GET /whoami')) {
        data = { name: registered ? 'review-bot' : null, session_id: 'session-1', registered };
      } else if (request.startsWith('POST /register')) {
        if (options.notAllowed) {
          status = 403;
          data = undefined;
        } else {
          const already = registered;
          registered = true;
          data = { name: 'review-bot', address: 'review-bot@direct', already_registered: already };
        }
      } else if (request.startsWith('DELETE /register')) {
        const already = !registered;
        registered = false;
        data = { registered: false, already_unregistered: already };
      }
      const envelope = status === 200
        ? { ok: true, data }
        : { ok: false, error: { code: 'not_allowed', message: 'Turn on "Let sessions put themselves on the relay" in Agent Relay Settings first.' } };
      const body = JSON.stringify(envelope);
      const length = options.closeDelimitedMutations && /^(POST|DELETE) \/register /.test(request)
        ? '' : `Content-Length: ${Buffer.byteLength(body)}\r\n`;
      connection.end(`HTTP/1.1 ${status} ${status === 200 ? 'OK' : 'Forbidden'}\r\nContent-Type: application/json\r\n${length}Connection: close\r\n\r\n${body}`);
    });
  });
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(path, resolve);
  });
  t.after(async () => {
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await rm(path, { force: true });
  });
  return server;
}

test('decodes chunked desktop responses', async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'relay-agents-chunked-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  const socket = join(root, 'relay.sock');
  const server = createServer((connection) => {
    connection.once('data', () => {
      const body = JSON.stringify({
        ok: true,
        data: { agents: [], fetched_at_ms: 1_790_683_200_123 },
      });
      const midpoint = Math.floor(body.length / 2);
      const chunks = [body.slice(0, midpoint), body.slice(midpoint)];
      connection.write('HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n');
      for (const chunk of chunks) connection.write(`${Buffer.byteLength(chunk).toString(16)}\r\n${chunk}\r\n`);
      connection.end('0\r\n\r\n');
    });
  });
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(socket, resolve);
  });
  t.after(async () => {
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await rm(socket, { force: true });
  });

  const result = await listRelayAgents({}, {
    env: { AGENT_RELAY_SOCKET: socket }, home: root, platform: 'linux', temporaryDirectory: root, uid: 501,
  });
  assert.deepEqual(result, { agents: [], fetched_at_ms: 1_790_683_200_123 });
});

test('preserves HTTP status when a desktop error has no typed envelope', async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'relay-agents-http-error-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  const socket = join(root, 'relay.sock');
  const server = createServer((connection) => {
    connection.once('data', () => {
      const body = JSON.stringify({ message: 'Not Found' });
      connection.end(`HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\nContent-Length: ${Buffer.byteLength(body)}\r\nConnection: close\r\n\r\n${body}`);
    });
  });
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(socket, resolve);
  });
  t.after(async () => {
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await rm(socket, { force: true });
  });

  await assert.rejects(
    listRelayAgents({}, {
      env: { AGENT_RELAY_SOCKET: socket }, home: root, platform: 'linux', temporaryDirectory: root, uid: 501,
    }),
    (error: unknown) => error instanceof Error
      && (error as { code?: string }).code === 'http_404'
      && error.message === 'Agent Relay desktop returned HTTP 404.',
  );
});

test('reports a slow desktop as a timeout instead of claiming it is absent', async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'relay-agents-deadline-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  const socket = join(root, 'relay.sock');
  const server = createServer((connection) => {
    connection.once('data', () => {
      connection.write('HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n');
      const interval = setInterval(() => connection.write('1\r\n{\r\n'), 10);
      connection.once('close', () => clearInterval(interval));
      connection.on('error', () => clearInterval(interval));
    });
  });
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(socket, resolve);
  });
  t.after(async () => {
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await rm(socket, { force: true });
  });

  const started = Date.now();
  await assert.rejects(
    listRelayAgents({}, {
      env: { AGENT_RELAY_SOCKET: socket }, home: root, platform: 'linux', temporaryDirectory: root, uid: 501,
      timeoutMs: 60,
    }),
    (error: unknown) => error instanceof Error
      && (error as { code?: string }).code === 'timeout'
      && error.message === "Agent Relay desktop didn't answer in time",
  );
  assert.ok(Date.now() - started < 500);
});

test('tries a later candidate after an earlier socket stalls', async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'relay-agents-stalled-candidate-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  const stalledSocket = join(root, 'stalled.sock');
  const workingSocket = join(root, 'working.sock');
  const pointer = join(root, '.agentworkforce', 'desktop', 'relay-socket');
  await mkdir(dirname(pointer), { recursive: true });
  await writeFile(pointer, `${workingSocket}\n`, { mode: 0o600 });

  const stalled = createServer((connection) => connection.once('data', () => undefined));
  await new Promise<void>((resolve, reject) => {
    stalled.once('error', reject);
    stalled.listen(stalledSocket, resolve);
  });
  t.after(async () => {
    await new Promise<void>((resolve) => stalled.close(() => resolve()));
    await rm(stalledSocket, { force: true });
  });
  const requests: string[] = [];
  await fakeRosterServer(t, workingSocket, requests);

  const result = await listRelayAgents({}, {
    env: { AGENT_RELAY_SOCKET: stalledSocket }, home: root, platform: 'linux', temporaryDirectory: root, uid: 501,
    timeoutMs: 60,
  });
  assert.ok('agents' in result);
  assert.equal(requests.length, 1);
});

test('allows a slow first roster response within the default local deadline', async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'relay-agents-slow-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  const socket = join(root, 'relay.sock');
  const server = createServer((connection) => {
    connection.once('data', () => {
      setTimeout(() => {
        const body = JSON.stringify({
          ok: true,
          data: { agents: [], fetched_at_ms: 1_790_683_200_123 },
        });
        connection.end(`HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: ${Buffer.byteLength(body)}\r\nConnection: close\r\n\r\n${body}`);
      }, 1_600);
    });
  });
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(socket, resolve);
  });
  t.after(async () => {
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await rm(socket, { force: true });
  });

  const result = await listRelayAgents({}, {
    env: { AGENT_RELAY_SOCKET: socket }, home: root, platform: 'linux', temporaryDirectory: root, uid: 501,
  });
  assert.deepEqual(result, { agents: [], fetched_at_ms: 1_790_683_200_123 });
});

test('falls back when a socket cannot connect before the deadline', {
  skip: process.platform === 'win32',
}, async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'relay-agents-connect-timeout-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  const stalled = join(root, 'stalled.sock');
  const healthy = join(root, 'healthy.sock');
  const healthyRequests: string[] = [];
  await fakeRosterServer(t, healthy, healthyRequests);
  const pointer = join(root, '.agentworkforce', 'desktop', 'relay-socket');
  await mkdir(dirname(pointer), { recursive: true });
  await writeFile(pointer, `${healthy}\n`);

  // Keep a real socket listening while preventing its process from accepting.
  // Filling the kernel backlog makes the next connection either wait or report
  // EAGAIN, depending on the Unix kernel; both are pre-request unavailability.
  const childScript = [
    'const { createServer } = require("node:net");',
    'const server = createServer();',
    'server.listen({ path: process.argv[1], backlog: 1 }, () => {',
    '  process.stdout.write("ready\\n");',
    '  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0);',
    '});',
  ].join('\n');
  const stalledServer = spawn(process.execPath, ['-e', childScript, stalled], {
    stdio: ['ignore', 'pipe', 'inherit'],
  });
  await new Promise<void>((resolve, reject) => {
    stalledServer.once('error', reject);
    stalledServer.stdout.once('data', () => resolve());
  });
  const fillers: Socket[] = [];
  t.after(async () => {
    for (const socket of fillers) socket.destroy();
    if (stalledServer.exitCode === null) {
      stalledServer.kill();
      await new Promise<void>((resolve) => stalledServer.once('exit', () => resolve()));
    }
  });
  for (let index = 0; index < 2; index += 1) {
    const filler = createConnection(stalled);
    fillers.push(filler);
    await new Promise<void>((resolve, reject) => {
      filler.once('connect', resolve);
      filler.once('error', reject);
    });
  }

  const result = await listRelayAgents({}, {
    env: { AGENT_RELAY_SOCKET: stalled }, home: root, platform: 'linux',
    temporaryDirectory: root, uid: 501, timeoutMs: 50,
  });
  assert.deepEqual(result, { agents: [{
    name: 'bob', address: 'bob@laptop', kind: 'agent', where: 'this_computer',
    status: 'active', last_seen_ms: 1_790_683_200_000,
    description: 'Fix the roster', is_self: true,
  }], fetched_at_ms: 1_790_683_200_123 });
  assert.equal(healthyRequests.length, 1);
});

test('reports inaccessible sockets instead of claiming the desktop is absent', async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'relay-agents-denied-'));
  const denied = join(root, 'denied');
  await mkdir(denied);
  await chmod(denied, 0o000);
  t.after(async () => {
    await chmod(denied, 0o700);
    await rm(root, { recursive: true, force: true });
  });

  await assert.rejects(
    listRelayAgents({}, {
      env: { AGENT_RELAY_SOCKET: join(denied, 'relay.sock') },
      home: root, platform: 'linux', temporaryDirectory: root, uid: 501,
      timeoutMs: 50,
    }),
    (error: unknown) => error instanceof Error
      && (error as { code?: string }).code === 'socket_access_denied'
      && error.message.includes('Cannot access Agent Relay desktop socket'),
  );
});

test('lists and filters live participants through AGENT_RELAY_SOCKET', async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'relay-agents-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  const socket = join(root, 'explicit.sock');
  const requests: string[] = [];
  await fakeRosterServer(t, socket, requests);

  const result = await listRelayAgents(
    { query: 'review', where: 'cloud', includeIdle: true },
    { env: { AGENT_RELAY_SOCKET: socket }, home: root, platform: 'linux', temporaryDirectory: root, uid: 501 },
  );

  assert.deepEqual(result, {
    agents: [{
      name: 'bob', address: 'bob@laptop', kind: 'agent', where: 'this_computer',
      status: 'active', last_seen_ms: 1_790_683_200_000,
      description: 'Fix the roster', is_self: true,
    }],
    fetched_at_ms: 1_790_683_200_123,
  });
  assert.match(requests[0] ?? '', /^GET \/agents\?q=review&where=cloud&include_idle=1 HTTP\/1\.1\r\n/);
});

test('uses the private pointer file before platform defaults', async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'relay-agents-pointer-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  const socket = join(root, 'desktop.sock');
  const pointer = join(root, '.agentworkforce', 'desktop', 'relay-socket');
  await mkdir(dirname(pointer), { recursive: true });
  await writeFile(pointer, `${socket}\n`, { mode: 0o600 });
  const requests: string[] = [];
  await fakeRosterServer(t, socket, requests);

  const result = await listRelayAgents({}, {
    env: { XDG_RUNTIME_DIR: join(root, 'unused-runtime') },
    home: root, platform: 'linux', temporaryDirectory: root, uid: 501,
  });

  assert.ok('agents' in result);
  assert.equal(requests.length, 1);
});

test('socket candidates preserve the documented macOS discovery order', async () => {
  const paths = await relaySocketCandidates({
    env: { AGENT_RELAY_SOCKET: '/chosen/relay.sock' },
    home: '/users/alice', platform: 'darwin', temporaryDirectory: '/private/tmp', uid: 501,
  });
  assert.deepEqual(paths, [
    '/chosen/relay.sock',
    '/users/alice/Library/Application Support/com.agentrelay.desktop/run/relay.sock',
    '/users/alice/Library/Application Support/com.agentrelay.desktop.dev/run/relay.sock',
    '/private/tmp/agent-relay-501/relay.sock',
    '/private/tmp/agent-relay-dev-501/relay.sock',
    '/tmp/agent-relay-501/relay.sock',
    '/tmp/agent-relay-dev-501/relay.sock',
  ]);
});

test('Linux discovery tries both runtime and data-directory defaults', async () => {
  const paths = await relaySocketCandidates({
    env: { XDG_RUNTIME_DIR: '/run/user/501', XDG_DATA_HOME: '/users/alice/data' },
    home: '/users/alice', platform: 'linux', temporaryDirectory: '/tmp', uid: 501,
  });
  assert.deepEqual(paths, [
    '/run/user/501/agent-relay/relay.sock',
    '/users/alice/data/com.agentrelay.desktop/run/relay.sock',
  ]);
});

test('missing desktop is a clear non-fatal result', async () => {
  const root = await mkdtemp(join(tmpdir(), 'relay-agents-missing-'));
  try {
    const runtime = {
      env: { AGENT_RELAY_SOCKET: join(root, 'missing.sock') },
      home: root, platform: 'linux' as const, temporaryDirectory: root, uid: 501,
      timeoutMs: 50,
    };
    const results = await Promise.all([
      listRelayAgents({}, runtime), relayStatus(runtime), joinRelay({}, runtime), leaveRelay(runtime),
    ]);
    for (const result of results) assert.deepEqual(result, { available: false, message: NOT_RUNNING });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('join_relay MCP returns readable not_allowed guidance', async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'relay-not-allowed-mcp-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  const socket = join(root, 'relay.sock');
  await fakeRosterServer(t, socket, [], { notAllowed: true });
  const transport = new StdioClientTransport({
    command: process.execPath,
    args: [fileURLToPath(new URL('./mcp-server.js', import.meta.url))],
    env: {
      ...Object.fromEntries(Object.entries(process.env).filter(
        (entry): entry is [string, string] => entry[1] !== undefined,
      )),
      AGENT_RELAY_SOCKET: socket,
      HOME: root,
      USERPROFILE: root,
      AI_HIST_DB: join(root, 'history.db'),
    },
    stderr: 'pipe',
  });
  const client = new Client({ name: 'relay-not-allowed-test', version: '1' });
  t.after(() => client.close());
  await client.connect(transport);
  const response = await client.callTool({ name: 'join_relay', arguments: {} });
  assert.equal(response.isError, true);
  const content = (response as { content: Array<{ type: string; text?: string }> }).content[0];
  assert.match(content?.text ?? '', /^not_allowed: .*Let sessions put themselves on the relay/);
});

test('status, join, idempotent join and leave use only the local session socket', async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'relay-registration-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  const socket = join(root, 'relay.sock');
  const requests: string[] = [];
  await fakeRosterServer(t, socket, requests);
  const runtime = { env: { AGENT_RELAY_SOCKET: socket }, home: root, platform: 'linux' as const, temporaryDirectory: root, uid: 501 };

  assert.deepEqual(await relayStatus(runtime), { name: null, session_id: 'session-1', registered: false });
  assert.deepEqual(await joinRelay({ name: 'review-bot', description: 'Reviews releases.' }, runtime), {
    name: 'review-bot', address: 'review-bot@direct', already_registered: false,
  });
  assert.deepEqual(await joinRelay({}, runtime), {
    name: 'review-bot', address: 'review-bot@direct', already_registered: true,
  });
  assert.deepEqual(await relayStatus(runtime), { name: 'review-bot', session_id: 'session-1', registered: true });
  assert.deepEqual(await leaveRelay(runtime), { registered: false, already_unregistered: false });

  assert.match(requests[1] ?? '', /^POST \/register HTTP\/1\.1\r\n/);
  assert.match(requests[1] ?? '', /\r\nContent-Type: application\/json\r\n/i);
  assert.match(requests[1] ?? '', /\r\n\r\n\{"name":"review-bot","description":"Reviews releases\."\}$/);
  assert.doesNotMatch(requests.join('\n'), /token|api[_-]?key|workspace[_-]?key/i);
  assert.match(requests[4] ?? '', /^DELETE \/register HTTP\/1\.1\r\n/);
});

test('registration refusal preserves the desktop not_allowed code and guidance', async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'relay-not-allowed-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  const socket = join(root, 'relay.sock');
  await fakeRosterServer(t, socket, [], { notAllowed: true });
  await assert.rejects(
    joinRelay({}, { env: { AGENT_RELAY_SOCKET: socket }, home: root, platform: 'linux', temporaryDirectory: root, uid: 501 }),
    (error: unknown) => error instanceof RelayAgentsError
      && error.code === 'not_allowed'
      && error.message.includes('Let sessions put themselves on the relay'),
  );
});

test('a sent mutation is never retried against another desktop when its result is unknown', async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'relay-indeterminate-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  const first = join(root, 'first.sock');
  const second = join(root, 'second.sock');
  const firstRequests: string[] = [];
  const secondRequests: string[] = [];
  await fakeRosterServer(t, first, firstRequests, { hangMutations: true });
  await fakeRosterServer(t, second, secondRequests);
  const pointer = join(root, '.agentworkforce', 'desktop', 'relay-socket');
  await mkdir(dirname(pointer), { recursive: true });
  await writeFile(pointer, `${second}\n`);
  const runtime = {
    env: { AGENT_RELAY_SOCKET: first }, home: root, platform: 'linux' as const,
    temporaryDirectory: root, uid: 501, timeoutMs: 50,
  };

  for (const operation of [() => joinRelay({}, runtime), () => leaveRelay(runtime)]) {
    await assert.rejects(operation, (error: unknown) => error instanceof RelayAgentsError
      && error.code === 'indeterminate_result'
      && error.message.includes('use relay_status'));
  }
  assert.equal(firstRequests.length, 2);
  assert.equal(secondRequests.length, 0);
});

test('a truncated mutation response is indeterminate and is not retried', async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'relay-truncated-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  const first = join(root, 'first.sock');
  const second = join(root, 'second.sock');
  const firstRequests: string[] = [];
  const secondRequests: string[] = [];
  await fakeRosterServer(t, first, firstRequests, { truncateMutations: true });
  await fakeRosterServer(t, second, secondRequests);
  const pointer = join(root, '.agentworkforce', 'desktop', 'relay-socket');
  await mkdir(dirname(pointer), { recursive: true });
  await writeFile(pointer, `${second}\n`);

  await assert.rejects(
    joinRelay({}, {
      env: { AGENT_RELAY_SOCKET: first }, home: root, platform: 'linux',
      temporaryDirectory: root, uid: 501, timeoutMs: 50,
    }),
    (error: unknown) => error instanceof RelayAgentsError && error.code === 'indeterminate_result',
  );
  assert.equal(firstRequests.length, 1);
  assert.equal(secondRequests.length, 0);
});

test('a complete close-delimited mutation response is accepted', async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'relay-close-delimited-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  const socket = join(root, 'relay.sock');
  const requests: string[] = [];
  await fakeRosterServer(t, socket, requests, { closeDelimitedMutations: true });

  const result = await joinRelay({}, {
    env: { AGENT_RELAY_SOCKET: socket }, home: root, platform: 'linux',
    temporaryDirectory: root, uid: 501,
  });

  assert.deepEqual(result, {
    name: 'review-bot', address: 'review-bot@direct', already_registered: false,
  });
  assert.equal(requests.length, 1);
});

test('a malformed successful mutation response is indeterminate and is not retried', async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'relay-malformed-mutation-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  const first = join(root, 'first.sock');
  const second = join(root, 'second.sock');
  const firstRequests: string[] = [];
  const secondRequests: string[] = [];
  await fakeRosterServer(t, first, firstRequests, { malformedMutations: true });
  await fakeRosterServer(t, second, secondRequests);
  const pointer = join(root, '.agentworkforce', 'desktop', 'relay-socket');
  await mkdir(dirname(pointer), { recursive: true });
  await writeFile(pointer, `${second}\n`);

  await assert.rejects(
    joinRelay({}, {
      env: { AGENT_RELAY_SOCKET: first }, home: root, platform: 'linux',
      temporaryDirectory: root, uid: 501,
    }),
    (error: unknown) => error instanceof RelayAgentsError
      && error.code === 'indeterminate_result'
      && error.message.includes('use relay_status'),
  );
  assert.equal(firstRequests.length, 1);
  assert.equal(secondRequests.length, 0);
});

test('an oversized mutation response is indeterminate and is not retried', async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'relay-oversized-mutation-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  const first = join(root, 'first.sock');
  const second = join(root, 'second.sock');
  const firstRequests: string[] = [];
  const secondRequests: string[] = [];
  await fakeRosterServer(t, first, firstRequests, { oversizedMutations: true });
  await fakeRosterServer(t, second, secondRequests);
  const pointer = join(root, '.agentworkforce', 'desktop', 'relay-socket');
  await mkdir(dirname(pointer), { recursive: true });
  await writeFile(pointer, `${second}\n`);

  await assert.rejects(
    joinRelay({}, {
      env: { AGENT_RELAY_SOCKET: first }, home: root, platform: 'linux',
      temporaryDirectory: root, uid: 501,
    }),
    (error: unknown) => error instanceof RelayAgentsError
      && error.code === 'indeterminate_result'
      && error.message.includes('use relay_status'),
  );
  assert.equal(firstRequests.length, 1);
  assert.equal(secondRequests.length, 0);
});

test('relay roster, status, join and leave are in the MCP inventory and call the local socket', async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'relay-agents-mcp-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  const socket = join(root, 'relay.sock');
  const requests: string[] = [];
  await fakeRosterServer(t, socket, requests);
  const transport = new StdioClientTransport({
    command: process.execPath,
    args: [fileURLToPath(new URL('./mcp-server.js', import.meta.url))],
    env: {
      ...Object.fromEntries(Object.entries(process.env).filter(
        (entry): entry is [string, string] => entry[1] !== undefined,
      )),
      AGENT_RELAY_SOCKET: socket,
      HOME: root,
      USERPROFILE: root,
      AI_HIST_DB: join(root, 'history.db'),
    },
    stderr: 'pipe',
  });
  const client = new Client({ name: 'relay-roster-test', version: '1' });
  t.after(() => client.close());
  await client.connect(transport);
  const tools = await client.listTools();
  for (const name of ['list_relay_agents', 'relay_status', 'join_relay', 'leave_relay']) {
    assert.ok(tools.tools.some((tool) => tool.name === name), `${name} is registered`);
  }
  for (const name of ['join_relay', 'leave_relay']) {
    const tool = tools.tools.find((candidate) => candidate.name === name);
    assert.equal(tool?.annotations?.readOnlyHint, false, `${name} is a mutation`);
    assert.equal(tool?.annotations?.idempotentHint, true, `${name} is idempotent`);
    assert.equal(tool?.annotations?.openWorldHint, true, `${name} changes Relay presence`);
  }

  const response = await client.callTool({
    name: 'list_relay_agents',
    arguments: { query: 'bob', include_idle: true },
  });
  const content = (response as { content: Array<{ type: string; text?: string }> }).content[0];
  assert.equal(content?.type, 'text');
  const result = JSON.parse(content?.text ?? '{}') as { agents?: Array<{ name: string }> };
  assert.equal(result.agents?.[0]?.name, 'bob');
  assert.match(requests[0] ?? '', /^GET \/agents\?q=bob&include_idle=1 HTTP\/1\.1\r\n/);

  for (const [name, arguments_] of [
    ['relay_status', {}],
    ['join_relay', { name: 'review-bot', description: 'Reviews releases.' }],
    ['leave_relay', {}],
  ] as const) {
    const toolResponse = await client.callTool({ name, arguments: arguments_ });
    assert.equal(toolResponse.isError, undefined, `${name} succeeds`);
  }
});

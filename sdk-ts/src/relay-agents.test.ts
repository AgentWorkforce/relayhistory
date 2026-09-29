import assert from 'node:assert/strict';
import { chmod, mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { createServer, type Server } from 'node:net';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';
import { listRelayAgents, relaySocketCandidates } from './relay-agents.js';

const NOT_RUNNING = "Agent Relay desktop isn't running on this machine; open it, or use the Agent Relay MCP";

async function fakeRosterServer(t: test.TestContext, path: string, requests: string[]): Promise<Server> {
  await mkdir(dirname(path), { recursive: true });
  const server = createServer((connection) => {
    let request = '';
    connection.setEncoding('utf8');
    let responded = false;
    connection.on('data', (chunk) => {
      request += chunk;
      if (responded || !request.includes('\r\n\r\n')) return;
      responded = true;
      requests.push(request);
      const body = JSON.stringify({
        ok: true,
        data: {
          agents: [{
            name: 'bob', address: 'bob@laptop', kind: 'agent', where: 'this_computer',
            status: 'active', last_seen_ms: 1_790_683_200_000,
            description: 'Fix the roster', is_self: true,
          }],
          fetched_at_ms: 1_790_683_200_123,
        },
      });
      connection.end(`HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: ${Buffer.byteLength(body)}\r\nConnection: close\r\n\r\n${body}`);
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
    const result = await listRelayAgents({}, {
      env: { AGENT_RELAY_SOCKET: join(root, 'missing.sock') },
      home: root, platform: 'linux', temporaryDirectory: root, uid: 501,
      timeoutMs: 50,
    });
    assert.deepEqual(result, { available: false, message: NOT_RUNNING });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('list_relay_agents is in the MCP inventory and calls the local socket', async (t) => {
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
  assert.ok(tools.tools.some((tool) => tool.name === 'list_relay_agents'));

  const response = await client.callTool({
    name: 'list_relay_agents',
    arguments: { query: 'bob', include_idle: true },
  });
  const content = (response as { content: Array<{ type: string; text?: string }> }).content[0];
  assert.equal(content?.type, 'text');
  const result = JSON.parse(content?.text ?? '{}') as { agents?: Array<{ name: string }> };
  assert.equal(result.agents?.[0]?.name, 'bob');
  assert.match(requests[0] ?? '', /^GET \/agents\?q=bob&include_idle=1 HTTP\/1\.1\r\n/);
});

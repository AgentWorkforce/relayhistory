// Run after building the native addon and SDK in this checkout. Synthetic workspace only.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { spawnSync } from 'node:child_process';

const root = path.resolve(process.argv[2] ?? '.');
if (!process.env.RH_VTCODE_SURFACE_CHILD) {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'vtcode-surfaces-'));
  const tmp = path.join(home, 'tmp');
  fs.mkdirSync(tmp);
  const env = {
    PATH: '/usr/bin:/bin:/usr/sbin:/sbin', HOME: home, USERPROFILE: home,
    TMPDIR: tmp, AI_HIST_DB: path.join(home, 'history.db'),
    RELAYHISTORY_HOME: path.join(home, 'other'),
    RELAYHISTORY_VTCODE_WORKSPACES: home, RH_VTCODE_SURFACE_CHILD: '1',
  };
  const target = path.join(home, '.vtcode/sessions/vt-synthetic-session/events.jsonl');
  fs.mkdirSync(path.dirname(target), { recursive: true });
  fs.copyFileSync(path.join(root, 'crates/ai-hist/tests/fixtures/vtcode/basic/.vtcode/sessions/vt-synthetic-session/events.jsonl'), target);
  try {
    const result = spawnSync(process.execPath, [import.meta.filename, root], { env, encoding: 'utf8' });
    process.stdout.write(result.stdout);
    process.stderr.write(result.stderr);
    assert.equal(result.status, 0);
  } finally {
    fs.rmSync(home, { recursive: true, force: true });
  }
} else {
  const sdk = await import(pathToFileURL(path.join(root, 'sdk-ts/dist/index.js')));
  const dbPath = process.env.AI_HIST_DB;
  const discovered = await sdk.discoverSessions({ dbPath, sources: ['vtcode'], scope: 'local' });
  assert.equal(discovered.discovered, 1);
  const page = await sdk.listSessionCatalogPage({ dbPath, sources: ['vtcode'] });
  assert.equal(page.sessions.length, 1);
  const sid = page.sessions[0].sessionId;
  assert.match(sid, /^root:[0-9a-f]{64}:vt-synthetic-session$/);
  await sdk.hydrateSession({ dbPath, source: 'vtcode', sessionId: sid });
  const hits = await sdk.search('synthetic verification', { dbPath, source: 'vtcode' });
  assert.ok(hits.length > 0);
  assert.ok(hits.every((hit) => hit.source === 'vtcode'));
  assert.equal((await sdk.search('synthetic verification', { dbPath, source: 'pi' })).length, 0);
  const events = await sdk.getSessionEvents(sid, { dbPath, source: 'vtcode' });
  assert.ok(events.some((event) => event.messageId === 'turn-b'));
  assert.ok(events.some((event) => event.messageId === 'assistant-b'));
  assert.equal((await sdk.getSourceCapabilities('vtcode')).source, 'vtcode');

  const cli = spawnSync(process.execPath,
    [path.join(root, 'sdk-ts/dist/cli.js'), 'search', 'synthetic verification', '--source', 'vtcode', '--db', dbPath, '--json'],
    { env: process.env, encoding: 'utf8' });
  assert.equal(cli.status, 0, cli.stderr);
  assert.ok(JSON.parse(cli.stdout).length > 0);

  const { Client } = await import(pathToFileURL(path.join(root, 'sdk-ts/node_modules/@modelcontextprotocol/sdk/dist/esm/client/index.js')));
  const { StdioClientTransport } = await import(pathToFileURL(path.join(root, 'sdk-ts/node_modules/@modelcontextprotocol/sdk/dist/esm/client/stdio.js')));
  const client = new Client({ name: 'vtcode-synthetic-acceptance', version: '1.0.0' });
  await client.connect(new StdioClientTransport({ command: process.execPath,
    args: [path.join(root, 'sdk-ts/dist/mcp-server.js')], env: process.env, stderr: 'pipe' }));
  try {
    const tools = await client.listTools();
    assert.ok(tools.tools.find((tool) => tool.name === 'search_history').inputSchema.properties.source.enum.includes('vtcode'));
    const caps = await client.callTool({ name: 'get_source_capabilities', arguments: { source: 'vtcode' } });
    assert.ok(!caps.isError);
    assert.equal(JSON.parse(caps.content[0].text).source, 'vtcode');
    const result = await client.callTool({ name: 'search_history', arguments: { query: 'synthetic verification', source: 'vtcode' } });
    assert.ok(!result.isError);
    assert.ok(JSON.parse(result.content[0].text).length > 0);
    const filtered = await client.callTool({ name: 'search_history', arguments: { query: 'synthetic verification', source: 'pi' } });
    assert.deepEqual(JSON.parse(filtered.content[0].text), []);
  } finally {
    await client.close();
  }
  console.log('VT Code public SDK/CLI/MCP discover, hydrate, native identities, search, capabilities and source filtering passed');
}

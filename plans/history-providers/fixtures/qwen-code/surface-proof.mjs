// Run after building native + SDK from this checkout. Pinned Qwen Code recorder fixtures only.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { spawnSync } from 'node:child_process';

const root = path.resolve(process.argv[2] ?? '.');
if (!process.env.RH_QWEN_SURFACE_CHILD) {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'qwen-code-surfaces-'));
  const temporary = path.join(home, 'tmp');
  fs.mkdirSync(temporary);
  const env = {
    PATH: '/usr/bin:/bin:/usr/sbin:/sbin', HOME: home, USERPROFILE: home,
    TMPDIR: temporary, AI_HIST_DB: path.join(home, 'history.db'),
    RELAYHISTORY_HOME: path.join(home, 'commercial'), RH_QWEN_SURFACE_CHILD: '1',
  };
  fs.cpSync(path.join(root, 'crates/ai-hist/tests/fixtures/qwen-code/ordinary/.qwen'),
    path.join(home, '.qwen'), { recursive: true });
  try {
    const result = spawnSync(process.execPath, [import.meta.filename, root], { env, encoding: 'utf8' });
    process.stdout.write(result.stdout);
    process.stderr.write(result.stderr);
    assert.equal(result.status, 0);
  } finally { fs.rmSync(home, { recursive: true, force: true }); }
} else {
  const sdk = await import(pathToFileURL(path.join(root, 'sdk-ts/dist/index.js')));
  const dbPath = process.env.AI_HIST_DB;
  const discovered = await sdk.discoverSessions({ dbPath, sources: ['qwen-code'], scope: 'local' });
  assert.equal(discovered.discovered, 1);
  const catalog = await sdk.listSessionCatalogPage({ dbPath, sources: ['qwen-code'] });
  assert.equal(catalog.sessions.length, 1);
  const sid = catalog.sessions[0].sessionId;
  assert.match(sid, /^root:[a-f0-9]+:11111111-1111-4111-8111-000000000001$/);
  await sdk.hydrateSession({ dbPath, source: 'qwen-code', sessionId: sid });
  const hits = await sdk.search('synthetic Qwen', { dbPath, source: 'qwen-code' });
  assert.ok(hits.length > 0);
  assert.ok(hits.every(hit => hit.source === 'qwen-code'));
  assert.ok((await sdk.getSessionEvents(sid, { dbPath, source: 'qwen-code' })).length > 0);
  const caps = await sdk.getSourceCapabilities('qwen-code');
  assert.equal(caps.source, 'qwen-code');
  assert.ok(caps.evidenceKinds.includes('tool_call'));
  const cli = spawnSync(process.execPath, [path.join(root, 'sdk-ts/dist/cli.js'),
    'search', 'synthetic Qwen', '--source', 'qwen-code', '--db', dbPath, '--json'],
  { env: process.env, encoding: 'utf8' });
  assert.equal(cli.status, 0, cli.stderr);
  assert.ok(JSON.parse(cli.stdout).length > 0);
  const { Client } = await import(pathToFileURL(path.join(root, 'sdk-ts/node_modules/@modelcontextprotocol/sdk/dist/esm/client/index.js')));
  const { StdioClientTransport } = await import(pathToFileURL(path.join(root, 'sdk-ts/node_modules/@modelcontextprotocol/sdk/dist/esm/client/stdio.js')));
  const client = new Client({ name: 'qwen-code-synthetic-acceptance', version: '1.0.0' });
  await client.connect(new StdioClientTransport({ command: process.execPath,
    args: [path.join(root, 'sdk-ts/dist/mcp-server.js')], env: process.env, stderr: 'pipe' }));
  try {
    const tools = await client.listTools();
    const sources = tools.tools.find(tool => tool.name === 'search_history').inputSchema.properties.source.enum;
    assert.ok(sources.includes('qwen-code'));
    const capability = await client.callTool({ name: 'get_source_capabilities', arguments: { source: 'qwen-code' } });
    assert.ok(!capability.isError);
    assert.equal(JSON.parse(capability.content[0].text).source, 'qwen-code');
    const result = await client.callTool({ name: 'search_history', arguments: { query: 'synthetic Qwen', source: 'qwen-code' } });
    assert.ok(!result.isError);
    assert.ok(JSON.parse(result.content[0].text).length > 0);
    const filtered = await client.callTool({ name: 'search_history', arguments: { query: 'synthetic Qwen', source: 'pi' } });
    assert.deepEqual(JSON.parse(filtered.content[0].text), []);
  } finally { await client.close(); }
  console.log('Qwen Code native recorder discover/catalog/hydrate/read/search + CLI search + MCP descriptors/capabilities/search/source filtering passed (pinned recorder fixture).');
}

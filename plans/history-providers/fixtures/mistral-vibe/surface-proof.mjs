// Run after building native + SDK from this checkout. native CLI fixtures only.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { spawnSync } from 'node:child_process';

const root = path.resolve(process.argv[2] ?? '.');
if (!process.env.RH_VIBE_SURFACE_CHILD) {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'vibe-surfaces-'));
  const temporary = path.join(home, 'tmp');
  fs.mkdirSync(temporary);
  const env = {
    PATH: '/usr/bin:/bin:/usr/sbin:/sbin', HOME: home, USERPROFILE: home,
    TMPDIR: temporary, AI_HIST_DB: path.join(home, 'history.db'),
    RELAYHISTORY_HOME: path.join(home, 'commercial'), RH_VIBE_SURFACE_CHILD: '1',
  };
  fs.cpSync(path.join(root, 'crates/ai-hist/tests/fixtures/mistral-vibe/fixture-home/.vibe'), path.join(home, '.vibe'), { recursive: true });
  try {
    const result = spawnSync(process.execPath, [import.meta.filename, root], { env, encoding: 'utf8' });
    process.stdout.write(result.stdout);
    process.stderr.write(result.stderr);
    assert.equal(result.status, 0);
  } finally { fs.rmSync(home, { recursive: true, force: true }); }
} else {
  const sdk = await import(pathToFileURL(path.join(root, 'sdk-ts/dist/index.js')));
  const dbPath = process.env.AI_HIST_DB;
  const discovered = await sdk.discoverSessions({ dbPath, sources: ['mistral-vibe'], scope: 'local' });
  assert.equal(discovered.discovered, 1);
  const catalog = await sdk.listSessionCatalogPage({ dbPath, sources: ['mistral-vibe'] });
  assert.equal(catalog.sessions.length, 1);
  const sid = catalog.sessions[0].sessionId;
  assert.equal(sid, 'vibe-synthetic-001');
  await sdk.hydrateSession({ dbPath, source: 'mistral-vibe', sessionId: sid });
  const hits = await sdk.search('Explain the synthetic repository', { dbPath, source: 'mistral-vibe' });
  assert.ok(hits.length > 0);
  assert.ok(hits.every(hit => hit.source === 'mistral-vibe'));
  const events = await sdk.getSessionEvents(sid, { dbPath, source: 'mistral-vibe' });
  assert.ok(events.some(event => event.kind === 'tool_use' && event.toolUseId === 'effect-action-1'));
  assert.ok(events.some(event => event.kind === 'tool_result' && event.toolUseId === 'effect-action-1'));
  const tools = await sdk.getSessionToolCalls('mistral-vibe', sid, { dbPath });
  assert.equal(tools.filter(tool => tool.toolUseId === 'effect-action-1').length, 1);
  const caps = await sdk.getSourceCapabilities('mistral-vibe');
  assert.equal(caps.source, 'mistral-vibe');
  assert.ok(caps.evidenceKinds.includes('tool_call'));
  assert.ok(!caps.evidenceKinds.includes('usage'));
  const cli = spawnSync(process.execPath, [path.join(root, 'sdk-ts/dist/cli.js'),
    'search', 'Explain the synthetic repository', '--source', 'mistral-vibe', '--db', dbPath, '--json'],
  { env: process.env, encoding: 'utf8' });
  assert.equal(cli.status, 0, cli.stderr);
  assert.ok(JSON.parse(cli.stdout).length > 0);
  const { Client } = await import(pathToFileURL(path.join(root, 'sdk-ts/node_modules/@modelcontextprotocol/sdk/dist/esm/client/index.js')));
  const { StdioClientTransport } = await import(pathToFileURL(path.join(root, 'sdk-ts/node_modules/@modelcontextprotocol/sdk/dist/esm/client/stdio.js')));
  const client = new Client({ name: 'vibe-synthetic-acceptance', version: '1.0.0' });
  await client.connect(new StdioClientTransport({ command: process.execPath,
    args: [path.join(root, 'sdk-ts/dist/mcp-server.js')], env: process.env, stderr: 'pipe' }));
  try {
    const tools = await client.listTools();
    const sources = tools.tools.find(tool => tool.name === 'search_history').inputSchema.properties.source.enum;
    assert.ok(sources.includes('mistral-vibe'));
    assert.ok(!sources.includes('gemini'));
    const capability = await client.callTool({ name: 'get_source_capabilities', arguments: { source: 'mistral-vibe' } });
    assert.ok(!capability.isError);
    assert.equal(JSON.parse(capability.content[0].text).source, 'mistral-vibe');
    const result = await client.callTool({ name: 'search_history', arguments: { query: 'Explain the synthetic repository', source: 'mistral-vibe' } });
    assert.ok(!result.isError);
    assert.ok(JSON.parse(result.content[0].text).length > 0);
    const filtered = await client.callTool({ name: 'search_history', arguments: { query: 'Explain the synthetic repository', source: 'pi' } });
    assert.deepEqual(JSON.parse(filtered.content[0].text), []);
  } finally { await client.close(); }
  console.log('Mistral Vibe native SDK discover/catalog/hydrate/read/search + CLI search + MCP descriptors/capabilities/search/source filtering passed (native CLI fixture).');
}

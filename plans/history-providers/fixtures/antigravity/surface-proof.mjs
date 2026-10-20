// Run after building native + SDK from this checkout. SDK-native fixtures only.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { spawnSync } from 'node:child_process';

const root = path.resolve(process.argv[2] ?? '.');
if (!process.env.RH_ANTIGRAVITY_SURFACE_CHILD) {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'antigravity-surfaces-'));
  const temporary = path.join(home, 'tmp');
  fs.mkdirSync(temporary);
  const env = {
    PATH: '/usr/bin:/bin:/usr/sbin:/sbin', HOME: home, USERPROFILE: home,
    TMPDIR: temporary, AI_HIST_DB: path.join(home, 'history.db'),
    RELAYHISTORY_HOME: path.join(home, 'commercial'), RH_ANTIGRAVITY_SURFACE_CHILD: '1',
  };
  fs.cpSync(path.join(root, 'crates/ai-hist/tests/fixtures/antigravity/ordinary/.gemini'),
    path.join(home, '.gemini'), { recursive: true });
  try {
    const result = spawnSync(process.execPath, [import.meta.filename, root], { env, encoding: 'utf8' });
    process.stdout.write(result.stdout);
    process.stderr.write(result.stderr);
    assert.equal(result.status, 0);
  } finally { fs.rmSync(home, { recursive: true, force: true }); }
} else {
  const sdk = await import(pathToFileURL(path.join(root, 'sdk-ts/dist/index.js')));
  const dbPath = process.env.AI_HIST_DB;
  const discovered = await sdk.discoverSessions({ dbPath, sources: ['antigravity'], scope: 'local' });
  assert.equal(discovered.discovered, 1);
  const catalog = await sdk.listSessionCatalogPage({ dbPath, sources: ['antigravity'] });
  assert.equal(catalog.sessions.length, 1);
  const sid = catalog.sessions[0].sessionId;
  assert.match(sid, /^root:[a-f0-9]+:00000000000000000000000000000001$/);
  await sdk.hydrateSession({ dbPath, source: 'antigravity', sessionId: sid });
  const hits = await sdk.search('synthetic fixture', { dbPath, source: 'antigravity' });
  assert.ok(hits.length > 0);
  assert.ok(hits.every(hit => hit.source === 'antigravity'));
  assert.ok((await sdk.getSessionEvents(sid, { dbPath, source: 'antigravity' })).length > 0);
  const caps = await sdk.getSourceCapabilities('antigravity');
  assert.equal(caps.source, 'antigravity');
  assert.ok(!caps.evidenceKinds.includes('usage'));
  const cli = spawnSync(process.execPath, [path.join(root, 'sdk-ts/dist/cli.js'),
    'search', 'synthetic fixture', '--source', 'antigravity', '--db', dbPath, '--json'],
  { env: process.env, encoding: 'utf8' });
  assert.equal(cli.status, 0, cli.stderr);
  assert.ok(JSON.parse(cli.stdout).length > 0);
  const { Client } = await import(pathToFileURL(path.join(root, 'sdk-ts/node_modules/@modelcontextprotocol/sdk/dist/esm/client/index.js')));
  const { StdioClientTransport } = await import(pathToFileURL(path.join(root, 'sdk-ts/node_modules/@modelcontextprotocol/sdk/dist/esm/client/stdio.js')));
  const client = new Client({ name: 'antigravity-synthetic-acceptance', version: '1.0.0' });
  await client.connect(new StdioClientTransport({ command: process.execPath,
    args: [path.join(root, 'sdk-ts/dist/mcp-server.js')], env: process.env, stderr: 'pipe' }));
  try {
    const tools = await client.listTools();
    const sources = tools.tools.find(tool => tool.name === 'search_history').inputSchema.properties.source.enum;
    assert.ok(sources.includes('antigravity'));
    assert.ok(!sources.includes('gemini'));
    const capability = await client.callTool({ name: 'get_source_capabilities', arguments: { source: 'antigravity' } });
    assert.ok(!capability.isError);
    assert.equal(JSON.parse(capability.content[0].text).source, 'antigravity');
    const result = await client.callTool({ name: 'search_history', arguments: { query: 'synthetic fixture', source: 'antigravity' } });
    assert.ok(!result.isError);
    assert.ok(JSON.parse(result.content[0].text).length > 0);
    const filtered = await client.callTool({ name: 'search_history', arguments: { query: 'synthetic fixture', source: 'pi' } });
    assert.deepEqual(JSON.parse(filtered.content[0].text), []);
  } finally { await client.close(); }
  console.log('Antigravity native SDK discover/catalog/hydrate/read/search + CLI search + MCP descriptors/capabilities/search/source filtering passed (SDK-native fixture).');
}

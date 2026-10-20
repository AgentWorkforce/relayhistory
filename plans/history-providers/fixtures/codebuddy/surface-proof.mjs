// Run after building native + SDK from this checkout. Official CLI/native recorder fixtures.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { spawnSync } from 'node:child_process';

const root = path.resolve(process.argv[2] ?? '.');
if (!process.env.RH_CODEBUDDY_SURFACE_CHILD) {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'codebuddy-surfaces-'));
  const temporary = path.join(home, 'tmp');
  fs.mkdirSync(temporary);
  const env = {
    PATH: '/usr/bin:/bin:/usr/sbin:/sbin', HOME: home, USERPROFILE: home,
    TMPDIR: temporary, AI_HIST_DB: path.join(home, 'history.db'),
    RELAYHISTORY_HOME: path.join(home, 'commercial'), RH_CODEBUDDY_SURFACE_CHILD: '1',
  };
  for (const fixture of ['ordinary', 'tool']) {
    fs.cpSync(path.join(root, `crates/ai-hist/tests/fixtures/codebuddy/${fixture}/.codebuddy`),
      path.join(home, '.codebuddy'), { recursive: true });
  }
  try {
    const result = spawnSync(process.execPath, [import.meta.filename, root], { env, encoding: 'utf8' });
    process.stdout.write(result.stdout);
    process.stderr.write(result.stderr);
    assert.equal(result.status, 0);
  } finally { fs.rmSync(home, { recursive: true, force: true }); }
} else {
  const sdk = await import(pathToFileURL(path.join(root, 'sdk-ts/dist/index.js')));
  const dbPath = process.env.AI_HIST_DB;
  const discovered = await sdk.discoverSessions({ dbPath, sources: ['codebuddy'], scope: 'local' });
  assert.equal(discovered.discovered, 2);
  const catalog = await sdk.listSessionCatalogPage({ dbPath, sources: ['codebuddy'] });
  assert.equal(catalog.sessions.length, 2);
  const sid = catalog.sessions.find(row => row.sessionId.endsWith(':fixture-ordinary')).sessionId;
  assert.match(sid, /^project:[a-f0-9]+:fixture-ordinary$/);
  await sdk.hydrateSession({ dbPath, source: 'codebuddy', sessionId: sid });
  const hits = await sdk.search('synthetic CodeBuddy needle', { dbPath, source: 'codebuddy' });
  assert.ok(hits.length > 0);
  assert.ok(hits.every(hit => hit.source === 'codebuddy'));
  assert.ok((await sdk.getSessionEvents(sid, { dbPath, source: 'codebuddy' })).length > 0);
  const usage = await sdk.getSessionUsage('codebuddy', sid, { dbPath });
  assert.equal(usage.usage.inputTokens, 23);
  assert.equal(usage.usage.outputTokens, 7);
  assert.deepEqual(usage.accounting, ['per-message']);
  const toolSid = catalog.sessions.find(row => row.sessionId.endsWith(':fixture-tool')).sessionId;
  await sdk.hydrateSession({ dbPath, source: 'codebuddy', sessionId: toolSid });
  const calls = await sdk.getSessionToolCalls('codebuddy', toolSid, { dbPath });
  assert.equal(calls[0].toolUseId, 'fixture-call-001');
  const events = await sdk.getSessionEvents(toolSid, { dbPath, source: 'codebuddy' });
  assert.equal(events.find(event => event.kind === 'tool_use').toolUseId, calls[0].toolUseId);
  const result = events.find(event => event.kind === 'tool_result');
  assert.equal(result.toolUseId, calls[0].toolUseId);
  assert.match(result.text, /Synthetic fixture file color blue/);
  const caps = await sdk.getSourceCapabilities('codebuddy');
  assert.equal(caps.source, 'codebuddy');
  assert.ok(caps.evidenceKinds.includes('tool_call'));
  const cli = spawnSync(process.execPath, [path.join(root, 'sdk-ts/dist/cli.js'),
    'search', 'synthetic CodeBuddy needle', '--source', 'codebuddy', '--db', dbPath, '--json'],
  { env: process.env, encoding: 'utf8' });
  assert.equal(cli.status, 0, cli.stderr);
  assert.ok(JSON.parse(cli.stdout).length > 0);
  const { Client } = await import(pathToFileURL(path.join(root, 'sdk-ts/node_modules/@modelcontextprotocol/sdk/dist/esm/client/index.js')));
  const { StdioClientTransport } = await import(pathToFileURL(path.join(root, 'sdk-ts/node_modules/@modelcontextprotocol/sdk/dist/esm/client/stdio.js')));
  const client = new Client({ name: 'codebuddy-synthetic-acceptance', version: '1.0.0' });
  await client.connect(new StdioClientTransport({ command: process.execPath,
    args: [path.join(root, 'sdk-ts/dist/mcp-server.js')], env: process.env, stderr: 'pipe' }));
  try {
    const tools = await client.listTools();
    const sources = tools.tools.find(tool => tool.name === 'search_history').inputSchema.properties.source.enum;
    assert.ok(sources.includes('codebuddy'));
      const capability = await client.callTool({ name: 'get_source_capabilities', arguments: { source: 'codebuddy' } });
    assert.ok(!capability.isError);
    assert.equal(JSON.parse(capability.content[0].text).source, 'codebuddy');
    const result = await client.callTool({ name: 'search_history', arguments: { query: 'synthetic CodeBuddy needle', source: 'codebuddy' } });
    assert.ok(!result.isError);
    assert.ok(JSON.parse(result.content[0].text).length > 0);
    const filtered = await client.callTool({ name: 'search_history', arguments: { query: 'synthetic CodeBuddy needle', source: 'pi' } });
    assert.deepEqual(JSON.parse(filtered.content[0].text), []);
  } finally { await client.close(); }
  console.log('Codebuddy native CLI discover/catalog/hydrate/read/search + CLI search + MCP descriptors/capabilities/search/source filtering passed.');
}

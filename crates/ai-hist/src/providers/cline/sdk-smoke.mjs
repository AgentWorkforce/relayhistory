// Run after building the native addon and sdk-ts. Fixture is synthetic.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { execFileSync } from 'node:child_process';
import { createRequire } from 'node:module';
const sdkRequire = createRequire(new URL('../../../../../sdk-ts/package.json', import.meta.url));
const { Client } = await import(sdkRequire.resolve('@modelcontextprotocol/sdk/client/index.js'));
const { StdioClientTransport } = await import(sdkRequire.resolve('@modelcontextprotocol/sdk/client/stdio.js'));
const sdkDir = process.env.CLINE_PACKED_SDK_DIR ?? fileURLToPath(new URL('../../../../../sdk-ts', import.meta.url));
const { discoverSessions, hydrateSession, search, getSourceCapabilities, getSessionEventsPage } = await import(pathToFileURL(path.join(sdkDir, 'dist/index.js')));
const root = fs.mkdtempSync(path.join(os.tmpdir(), 'cline-sdk-'));
const sid = 'fixture-success-01';
const fixture = fileURLToPath(new URL('../../../tests/fixtures/cline/native/.cline/data/sessions/fixture-success-01/fixture-success-01.messages.json', import.meta.url));
const sessions = path.join(root, '.cline', 'data', 'sessions');
const dest = path.join(sessions, sid, `${sid}.messages.json`);
fs.mkdirSync(path.dirname(dest), { recursive: true });
fs.copyFileSync(fixture, dest);
process.env.CLINE_SESSION_DATA_DIR = sessions;
const dbPath = path.join(root, 'history.db');
try {
  const discovered = await discoverSessions({ dbPath, sources: ['cline'], scope: 'local' });
  assert.equal(discovered.discovered, 1);
  assert.equal(discovered.sessions[0].source, 'cline');
  const hydrated = await hydrateSession({ dbPath, source: 'cline', sessionId: sid });
  assert.equal(hydrated.status, 'hydrated');
  for (const [query, role] of [
    ['Inspect the README', 'user'],
    ['The README describes', 'assistant'],
    ['A small test fixture', 'assistant'],
  ]) {
    const matches = await search(query, { dbPath, source: 'cline', role });
    assert.ok(matches.some(m => m.source === 'cline' && m.sessionId === sid), `${role} search: ${query}`);
  }
  const capabilities = await getSourceCapabilities('cline');
  assert.ok(capabilities.evidenceKinds.includes('session_event'));
  assert.ok(capabilities.evidenceKinds.includes('tool_call'));
  assert.ok(capabilities.evidenceKinds.includes('session_marker'));
  assert.ok(!capabilities.evidenceKinds.includes('file_edit'));
  assert.equal(capabilities.fullCoverage, false);
  const events = await getSessionEventsPage(sid, { dbPath, source: 'cline' });
  assert.ok(events.events.some(e => e.kind === 'tool_result'));
  const cli = path.join(sdkDir, 'dist/cli.js');
  const cliMatches = JSON.parse(execFileSync(process.execPath, [cli, 'search', 'README', '--source', 'cline', '--role', 'assistant', '--db', dbPath, '--json', '--no-warning', '--no-bootstrap'], { encoding: 'utf8' }));
  assert.ok(cliMatches.some(m => m.source === 'cline'));
  const client = new Client({ name: 'cline-fixture', version: '1' });
  const transport = new StdioClientTransport({ command: process.execPath, args: [path.join(sdkDir, 'dist/mcp-server.js')], env: { ...process.env, AI_HIST_DB: dbPath }, stderr: 'pipe' });
  try {
    await client.connect(transport);
    const response = await client.callTool({ name: 'search_history', arguments: { query: 'README', source: 'cline', role: 'assistant' } });
    assert.ok(!response.isError);
    assert.ok(JSON.parse(response.content[0].text).some(m => m.source === 'cline'));
  } finally {
    await client.close();
    await transport.close();
  }
  console.log('Cline SDK, CLI and MCP native fixture discovery, read, search and capability smoke passed');
} finally {
  fs.rmSync(root, { recursive: true, force: true });
}

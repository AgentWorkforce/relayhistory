// Run after building the native addon and sdk-ts. All history is synthetic.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { execFileSync } from 'node:child_process';
import { createRequire } from 'node:module';
const sdkRequire = createRequire(new URL('../../../../../sdk-ts/package.json', import.meta.url));
const { Client } = await import(sdkRequire.resolve('@modelcontextprotocol/sdk/client/index.js'));
const { StdioClientTransport } = await import(sdkRequire.resolve('@modelcontextprotocol/sdk/client/stdio.js'));
import { discoverSessions, hydrateSession, search, getSourceCapabilities, getSessionEventsPage } from '../../../../../sdk-ts/dist/index.js';
const sid = '5f0c2a8e-1d4b-4c6e-9a7f-3b2d1e0c9a81';
const root = fs.mkdtempSync(path.join(os.tmpdir(), 'copilot-sdk-'));
const fixture = fileURLToPath(new URL(`../../../tests/fixtures/copilot/cli-usage/.copilot/session-state/${sid}/events.jsonl`, import.meta.url));
const sessionDir = path.join(root, 'copilot-home', 'session-state', sid);
fs.mkdirSync(sessionDir, { recursive: true });
fs.copyFileSync(fixture, path.join(sessionDir, 'events.jsonl'));
process.env.COPILOT_HOME = path.join(root, 'copilot-home');
const dbPath = path.join(root, 'history.db');
try {
  const discovery = await discoverSessions({ dbPath, sources: ['copilot'], scope: 'local' });
  assert.equal(discovery.discovered, 1); assert.equal(discovery.sessions[0].source, 'copilot');
  const hydrated = await hydrateSession({ dbPath, source: 'copilot', sessionId: sid });
  assert.equal(hydrated.status, 'hydrated'); assert.equal(hydrated.capability, 'partial');
  for (const [query, role] of [['Fix the failing fixture test', 'user'], ['fails with exit status 101', 'assistant'], ['assertion compares', 'assistant']]) {
    const matches = await search(query, { dbPath, source: 'copilot', role });
    assert.ok(matches.some(m => m.source === 'copilot' && m.sessionId === sid), `public ${role} search: ${query}`);
  }
  assert.deepEqual(await search('fixture', { dbPath, source: 'claude' }), []);
  const capabilities = await getSourceCapabilities('copilot');
  for (const kind of ['session_event', 'tool_call', 'session_marker']) assert.ok(capabilities.evidenceKinds.includes(kind), kind);
  assert.ok(!capabilities.evidenceKinds.includes('file_edit')); assert.equal(capabilities.fullCoverage, false);
  const events = await getSessionEventsPage(sid, { dbPath, source: 'copilot' }); assert.ok(events.events.length > 0);
  const cli = fileURLToPath(new URL('../../../../../sdk-ts/dist/cli.js', import.meta.url));
  const cliMatches = JSON.parse(execFileSync(process.execPath, [cli, 'search', 'assertion', '--source', 'copilot', '--role', 'assistant', '--db', dbPath, '--json', '--no-warning', '--no-bootstrap'], { encoding: 'utf8' }));
  assert.ok(cliMatches.some(m => m.source === 'copilot'));
  const client = new Client({ name: 'copilot-fixture', version: '1' });
  const transport = new StdioClientTransport({ command: process.execPath, args: [fileURLToPath(new URL('../../../../../sdk-ts/dist/mcp-server.js', import.meta.url))], env: { ...process.env, AI_HIST_DB: dbPath }, stderr: 'pipe' });
  try {
    await client.connect(transport);
    const result = await client.callTool({ name: 'search_history', arguments: { query: 'assertion', source: 'copilot', role: 'assistant' } });
    assert.ok(!result.isError); const matches = JSON.parse(result.content[0].text); assert.ok(matches.some(m => m.source === 'copilot'));
  } finally { await client.close(); await transport.close(); }
  console.log('Copilot SDK/CLI/MCP source filtering, user/assistant search, events and honest capability assertions passed');
} finally { fs.rmSync(root, { recursive: true, force: true }); }

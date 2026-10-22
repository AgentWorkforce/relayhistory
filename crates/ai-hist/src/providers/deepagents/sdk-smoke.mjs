// Run after a fresh native addon build and sdk-ts compilation. All data is synthetic.
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

const root = fs.mkdtempSync(path.join(os.tmpdir(), 'deepagents-sdk-'));
const native = path.join(root, '.deepagents', '.state', 'sessions.db');
const fixture = fileURLToPath(new URL('../../../tests/fixtures/deepagents/sessions.sql', import.meta.url));
const dbPath = path.join(root, 'history.db');
try {
  fs.mkdirSync(path.dirname(native), { recursive: true });
  execFileSync('python3', ['-c', 'import sqlite3,sys; sqlite3.connect(sys.argv[1]).executescript(open(sys.argv[2]).read())', native, fixture]);
  process.env.DEEPAGENTS_HOME = path.join(root, '.deepagents');
  const discovery = await discoverSessions({ dbPath, sources: ['deepagents'], scope: 'local' });
  const sessionId = discovery.sessions.find(session => session.source === 'deepagents' && session.sessionId?.endsWith('343433'))?.sessionId;
  assert.ok(sessionId, JSON.stringify({ sessions: discovery.sessions, diagnostics: discovery.diagnostics }));
  const hydrated = await hydrateSession({ dbPath, source: 'deepagents', sessionId });
  assert.equal(hydrated.status, 'hydrated');
  for (const [query, role] of [['needle', 'user'], ['Looking now', 'assistant'], ['Found needle', 'all']]) {
    const matches = await search(query, { dbPath, source: 'deepagents', role });
    assert.ok(matches.some(match => match.sessionId === sessionId), `public ${role} search: ${query}`);
  }
  const events = await getSessionEventsPage(sessionId, { dbPath, source: 'deepagents' });
  assert.ok(events.events.some(event => event.role === 'tool_result'));
  const capabilities = await getSourceCapabilities('deepagents');
  assert.ok(capabilities.evidenceKinds.includes('tool_call'));
  assert.ok(capabilities.evidenceKinds.includes('session_marker'));
  assert.ok(!capabilities.evidenceKinds.includes('file_edit'));
  const cli = fileURLToPath(new URL('../../../../../sdk-ts/dist/cli.js', import.meta.url));
  const cliMatches = JSON.parse(execFileSync(process.execPath, [cli, 'search', 'needle', '--source', 'deepagents', '--role', 'user', '--db', dbPath, '--json', '--no-warning', '--no-bootstrap'], { encoding: 'utf8' }));
  assert.ok(cliMatches.some(match => (match.sessionId ?? match.session_id) === sessionId), JSON.stringify(cliMatches));
  const client = new Client({ name: 'deepagents-fixture', version: '1' });
  const transport = new StdioClientTransport({ command: process.execPath, args: [fileURLToPath(new URL('../../../../../sdk-ts/dist/mcp-server.js', import.meta.url))], env: { ...process.env, AI_HIST_DB: dbPath }, stderr: 'pipe' });
  try {
    await client.connect(transport);
    const result = await client.callTool({ name: 'search_history', arguments: { query: 'needle', source: 'deepagents', role: 'user' } });
    assert.ok(!result.isError);
    assert.ok(JSON.parse(result.content[0].text).some(match => (match.sessionId ?? match.session_id) === sessionId));
  } finally { await client.close(); await transport.close(); }
  console.log('DeepAgents compiled native SDK/CLI/MCP discovery, hydration, user/assistant/tool search and events passed');
} finally { fs.rmSync(root, { recursive: true, force: true }); }

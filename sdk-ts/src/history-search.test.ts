import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';
import {
  InvalidArgumentError, hydrateSession, search, sync,
  type SearchMatch, type SearchOptions,
} from './index.js';
import { scrubHistoryEnv } from './test-env.js';

// One search contract (`history_search::search_all`) serves the CLI, the SDK
// and MCP. These fixtures pin what a caller sees through each surface.

const run = promisify(execFile);
const cli = fileURLToPath(new URL('./cli.js', import.meta.url));
const mcp = fileURLToPath(new URL('./mcp-server.js', import.meta.url));

async function withIndexedSession(body: (dbPath: string) => Promise<void>): Promise<void> {
  const home = await mkdtemp(join(tmpdir(), 'relayhistory-search-'));
  const restoreEnv = scrubHistoryEnv();
  process.env.HOME = home;
  process.env.USERPROFILE = home;
  process.env.RELAYHISTORY_HOME = join(home, 'commercial');
  process.env.RELAYHISTORY_NO_UPDATE_CHECK = '1';
  process.env.AI_HIST_DB = join(home, 'history.db');
  try {
    const directory = join(home, '.claude', 'projects', '-work-search');
    await mkdir(directory, { recursive: true });
    const sessionId = 'search-session';
    const common = { sessionId, cwd: '/work/search' };
    await writeFile(join(directory, `${sessionId}.jsonl`), [
      { ...common, type: 'user', uuid: 'u1', timestamp: '2026-09-01T10:00:00.000Z',
        message: { role: 'user', content: 'searchneedle please fix parity-check' } },
      { ...common, type: 'assistant', uuid: 'a1', parentUuid: 'u1', timestamp: '2026-09-01T10:00:05.000Z',
        requestId: 'req-1', message: { id: 'msg-1', role: 'assistant', model: 'claude-test',
          content: [{ type: 'text', text: 'searchneedle answered by the assistant' }] } },
    ].map((record) => JSON.stringify(record)).join('\n') + '\n');
    const dbPath = process.env.AI_HIST_DB;
    await sync({ dbPath, scope: 'local', sourceConnectors: [] });
    await hydrateSession({ source: 'claude', sessionId, dbPath });
    await body(dbPath);
  } finally {
    restoreEnv();
    await rm(home, { recursive: true, force: true });
  }
}

const identity = (match: Pick<SearchMatch, 'matchSource' | 'id'>) => `${match.matchSource}:${match.id}`;

test('search spans prompts and session events and says where each match came from', async () => {
  await withIndexedSession(async (dbPath) => {
    const all = await search('searchneedle', { dbPath });
    const assistant = all.find((match) => match.role === 'assistant');
    assert.ok(assistant, 'the assistant reply is searchable');
    assert.equal(assistant.matchSource, 'session_event');
    assert.equal(assistant.kind, 'text');
    assert.match(assistant.prompt, /answered by the assistant/);
    const prompt = all.find((match) => match.matchSource === 'history');
    assert.ok(prompt, 'the prompt is searchable');
    assert.equal(prompt.role, 'user');
    assert.equal(prompt.kind, 'history');
    assert.deepEqual(prompt.locations, ['local']);

    // Newest first, with a stable tie-break, so the call is repeatable.
    for (let index = 1; index < all.length; index += 1) {
      assert.ok(all[index - 1].timestampMs >= all[index].timestampMs);
    }
    assert.deepEqual((await search('searchneedle', { dbPath })).map(identity), all.map(identity));

    const onlyAssistant = await search('searchneedle', { dbPath, role: 'assistant' });
    assert.deepEqual(onlyAssistant.map(identity), [identity(assistant)]);
    const onlyPrompts = await search('searchneedle', { dbPath, role: 'prompt' });
    assert.deepEqual(onlyPrompts.map(identity), [identity(prompt)]);
    assert.ok((await search('searchneedle', { dbPath, role: 'user' })).every((match) => match.role === 'user'));

    const older = await search('searchneedle', { dbPath, beforeMs: assistant.timestampMs });
    assert.ok(older.length > 0);
    assert.ok(older.every((match) => match.timestampMs < assistant.timestampMs));

    await assert.rejects(
      () => search('searchneedle', { dbPath, role: 'tool' as SearchOptions['role'] }),
      (error: unknown) => error instanceof InvalidArgumentError && /all, user, assistant, prompt/.test(error.message),
    );
  });
});

test('raw FTS is opt-in and a malformed expression is an actionable error', async () => {
  await withIndexedSession(async (dbPath) => {
    // Default mode quotes each word, so punctuation is literal.
    assert.ok((await search('parity-check', { dbPath, role: 'prompt' })).length === 1);
    assert.equal((await search('searchneedle OR nomatch', { dbPath, role: 'prompt', rawFts: true })).length, 1);
    await assert.rejects(
      () => search('parity-check', { dbPath, rawFts: true }),
      /Invalid raw FTS5 MATCH expression/,
    );
  });
});

test('the TypeScript CLI and MCP return the SDK search result', async () => {
  await withIndexedSession(async (dbPath) => {
    const expected = await search('searchneedle', { dbPath, role: 'all' });
    assert.ok(expected.length >= 2);

    const { stdout } = await run(process.execPath, [
      cli, 'search', 'searchneedle', '--role', 'assistant', '--db', dbPath, '--json', '--no-warning',
    ], { env: process.env });
    const cliRows = JSON.parse(stdout) as Array<Record<string, unknown>>;
    assert.deepEqual(
      cliRows.map((row) => `${String(row.match_source ?? row.matchSource)}:${String(row.id)}`),
      expected.filter((match) => match.role === 'assistant').map(identity),
    );

    const env = Object.fromEntries(Object.entries(process.env).filter((entry): entry is [string, string] => entry[1] !== undefined));
    const client = new Client({ name: 'history-search-fixture', version: '1' });
    const transport = new StdioClientTransport({ command: process.execPath, args: [mcp], env, stderr: 'pipe' });
    try {
      await client.connect(transport);
      const { tools } = await client.listTools();
      const tool = tools.find((entry) => entry.name === 'search_history');
      assert.ok(tool?.inputSchema.properties?.role, 'search_history exposes role');
      assert.ok(tool.inputSchema.properties?.raw_fts, 'search_history exposes raw_fts');

      const result = await client.callTool({ name: 'search_history', arguments: { query: 'searchneedle' } });
      assert.equal(result.isError, undefined);
      const text = (result.content as Array<{ text: string }>)[0].text;
      assert.deepEqual(JSON.parse(text), JSON.parse(JSON.stringify(expected)));

      assert.ok(tool.inputSchema.properties?.before_ms, 'search_history exposes before_ms');
      const newest = expected[0];
      const bounded = await client.callTool({
        name: 'search_history', arguments: { query: 'searchneedle', before_ms: newest.timestampMs },
      });
      assert.equal(bounded.isError, undefined);
      const boundedRows = JSON.parse((bounded.content as Array<{ text: string }>)[0].text) as SearchMatch[];
      assert.deepEqual(
        boundedRows.map(identity),
        expected.filter((match) => match.timestampMs < newest.timestampMs).map(identity),
      );
      assert.ok(boundedRows.length > 0, 'an older match is still returned');

      const malformed = await client.callTool({
        name: 'search_history', arguments: { query: 'parity-check', raw_fts: true },
      });
      assert.equal(malformed.isError, true);
      assert.match(JSON.stringify(malformed.content), /Invalid raw FTS5 MATCH expression/);
    } finally {
      await client.close();
      await transport.close();
    }
  });
});

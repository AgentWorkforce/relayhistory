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
  InvalidArgumentError, hydrateSession, recent, recentPage, search, searchPage, sync,
  type HistoryCursor,
} from './index.js';
import { scrubHistoryEnv } from './test-env.js';

// Every record below shares one timestamp, as every prompt of a Cursor
// transcript does. Only the `(timestampMs, id)` keyset can page through it.

const run = promisify(execFile);
const cli = fileURLToPath(new URL('./cli.js', import.meta.url));
const mcp = fileURLToPath(new URL('./mcp-server.js', import.meta.url));
const TIMESTAMP = '2026-09-01T10:00:00.000Z';
const TIMESTAMP_MS = Date.parse(TIMESTAMP);

async function withTiedSession(body: (dbPath: string) => Promise<void>): Promise<void> {
  const home = await mkdtemp(join(tmpdir(), 'relayhistory-paging-'));
  const restoreEnv = scrubHistoryEnv();
  process.env.HOME = home;
  process.env.USERPROFILE = home;
  process.env.RELAYHISTORY_HOME = join(home, 'commercial');
  process.env.RELAYHISTORY_NO_UPDATE_CHECK = '1';
  process.env.AI_HIST_DB = join(home, 'history.db');
  try {
    const directory = join(home, '.claude', 'projects', '-work-paging');
    await mkdir(directory, { recursive: true });
    const sessionId = 'paging-session';
    const records = [];
    let parent: string | undefined;
    for (let turn = 0; turn < 5; turn += 1) {
      const user = `u${turn}`;
      const assistant = `a${turn}`;
      records.push({ sessionId, cwd: '/work/paging', type: 'user', uuid: user, parentUuid: parent, timestamp: TIMESTAMP,
        message: { role: 'user', content: `pageneedle prompt ${turn}` } });
      records.push({ sessionId, cwd: '/work/paging', type: 'assistant', uuid: assistant, parentUuid: user, timestamp: TIMESTAMP,
        requestId: `req-${turn}`, message: { id: `msg-${turn}`, role: 'assistant', model: 'claude-test',
          content: [{ type: 'text', text: `pageneedle reply ${turn}` }] } });
      parent = assistant;
    }
    await writeFile(join(directory, `${sessionId}.jsonl`), records.map((record) => JSON.stringify(record)).join('\n') + '\n');
    const dbPath = process.env.AI_HIST_DB;
    await sync({ dbPath, scope: 'local', sourceConnectors: [] });
    await hydrateSession({ source: 'claude', sessionId, dbPath });
    await body(dbPath);
  } finally {
    restoreEnv();
    await rm(home, { recursive: true, force: true });
  }
}

const key = (row: { id: number; matchSource?: string }) => `${row.matchSource ?? 'history'}:${row.id}`;

test('recentPage and searchPage walk tied timestamps without skips or repeats', async () => {
  await withTiedSession(async (dbPath) => {
    const allPrompts = await recent({ dbPath, limit: 1000 });
    assert.equal(allPrompts.length, 5);
    assert.ok(allPrompts.every((entry) => entry.timestampMs === TIMESTAMP_MS));
    const allMatches = await search('pageneedle', { dbPath, limit: 1000 });
    assert.equal(allMatches.length, 10, 'five prompts and five replies');

    for (const limit of [1, 2, 5, 10, 50]) {
      const walkedPrompts: string[] = [];
      let after: HistoryCursor | undefined;
      for (;;) {
        const page = await recentPage({ dbPath, limit, after });
        assert.ok(page.entries.length <= limit);
        walkedPrompts.push(...page.entries.map(key));
        if (!page.nextCursor) break;
        after = page.nextCursor;
      }
      assert.deepEqual(walkedPrompts, allPrompts.map(key), `recent, page size ${limit}`);

      const walkedMatches: string[] = [];
      after = undefined;
      for (;;) {
        const page = await searchPage('pageneedle', { dbPath, limit, after });
        walkedMatches.push(...page.matches.map(key));
        if (!page.nextCursor) break;
        assert.equal(page.matches.length, limit, 'a cursor is only returned after a full page');
        after = page.nextCursor;
      }
      assert.deepEqual(walkedMatches, allMatches.map(key), `search, page size ${limit}`);
    }

    // The same keyset works on the array reads, built from the last row.
    const [first] = await search('pageneedle', { dbPath, limit: 1 });
    const rest = await search('pageneedle', { dbPath, limit: 1000, after: first });
    assert.deepEqual(rest.map(key), allMatches.slice(1).map(key));

    // The deprecated exclusive bound keeps its semantics, and so skips the tie.
    assert.deepEqual(await recent({ dbPath, beforeMs: TIMESTAMP_MS }), []);
  });
});

test('sinceMs and untilMs are inclusive and validated at the boundary', async () => {
  await withTiedSession(async (dbPath) => {
    assert.equal((await recent({ dbPath, sinceMs: TIMESTAMP_MS, untilMs: TIMESTAMP_MS })).length, 5);
    assert.equal((await search('pageneedle', { dbPath, sinceMs: TIMESTAMP_MS, limit: 100 })).length, 10);
    assert.deepEqual(await recent({ dbPath, sinceMs: TIMESTAMP_MS + 1 }), []);
    assert.deepEqual(await search('pageneedle', { dbPath, untilMs: TIMESTAMP_MS - 1 }), []);
    const page = await recentPage({ dbPath, sinceMs: TIMESTAMP_MS, limit: 2 });
    assert.equal(page.entries.length, 2);
    assert.ok(page.nextCursor);

    for (const read of [
      () => recent({ dbPath, sinceMs: 2, untilMs: 1 }),
      () => searchPage('pageneedle', { dbPath, sinceMs: 2, untilMs: 1 }),
    ]) {
      await assert.rejects(read, (error: unknown) =>
        error instanceof InvalidArgumentError && /since_ms \(2\) must not be later than until_ms \(1\)/.test(error.message));
    }
    for (const matchSource of ['tool', '']) {
      await assert.rejects(
        () => search('pageneedle', { dbPath, after: { timestampMs: 1, id: 1, matchSource: matchSource as never } }),
        (error: unknown) => error instanceof InvalidArgumentError
          && /cursor match_source must be history or session_event/.test(error.message),
        `matchSource ${JSON.stringify(matchSource)} is rejected, not read as history`,
      );
    }
  });
});

test('the TypeScript CLI and MCP page with the same cursor and window', async () => {
  await withTiedSession(async (dbPath) => {
    const expected = (await search('pageneedle', { dbPath, limit: 1000 })).map(key);

    const walked: string[] = [];
    let after: string | undefined;
    for (;;) {
      const { stdout } = await run(process.execPath, [
        cli, 'search', 'pageneedle', '--limit', '3', '--db', dbPath, '--json', '--no-warning',
        ...(after ? ['--after', after] : []),
      ], { env: process.env });
      const rows = JSON.parse(stdout) as Array<Record<string, unknown>>;
      walked.push(...rows.map((row) => `${String(row.match_source)}:${String(row.id)}`));
      if (rows.length < 3) break;
      after = JSON.stringify(rows[rows.length - 1]);
    }
    assert.deepEqual(walked, expected);

    await assert.rejects(
      () => run(process.execPath, [cli, 'recent', '--since-ms', '2', '--until-ms', '1', '--db', dbPath, '--no-warning'], { env: process.env }),
      (error: unknown) => typeof error === 'object' && error !== null && 'stderr' in error
        && /must not be later than until_ms/.test(String(error.stderr)),
    );
    // An unknown cursor source fails in the shared validation, as in the SDK
    // and MCP, not with a CLI-only message.
    await assert.rejects(
      () => run(process.execPath, [
        cli, 'search', 'pageneedle', '--db', dbPath, '--no-warning',
        '--after', '{"timestampMs":1,"id":1,"matchSource":"tool"}',
      ], { env: process.env }),
      (error: unknown) => typeof error === 'object' && error !== null && 'stderr' in error
        && /INVALID_ARGUMENT: cursor match_source must be history or session_event \(got tool\)/.test(String(error.stderr)),
    );

    const env = Object.fromEntries(Object.entries(process.env).filter((entry): entry is [string, string] => entry[1] !== undefined));
    const client = new Client({ name: 'history-paging-fixture', version: '1' });
    const transport = new StdioClientTransport({ command: process.execPath, args: [mcp], env, stderr: 'pipe' });
    try {
      await client.connect(transport);
      const prompts: string[] = [];
      let cursor: Record<string, unknown> | undefined;
      for (;;) {
        const result = await client.callTool({ name: 'recent_history', arguments: { n: 2, ...(cursor ? { after: cursor } : {}) } });
        assert.equal(result.isError, undefined);
        const rows = JSON.parse((result.content as Array<{ text: string }>)[0].text) as Array<{ id: number; timestampMs: number }>;
        prompts.push(...rows.map((row) => `history:${row.id}`));
        if (rows.length < 2) break;
        const last = rows[rows.length - 1];
        cursor = { timestampMs: last.timestampMs, id: last.id };
      }
      assert.deepEqual(prompts, (await recent({ dbPath, limit: 1000 })).map(key));

      const invalid = await client.callTool({ name: 'search_history', arguments: { query: 'pageneedle', since_ms: 2, until_ms: 1 } });
      assert.equal(invalid.isError, true);
      assert.match(JSON.stringify(invalid.content), /since_ms \(2\) must not be later than until_ms \(1\)/);

      // An unknown cursor source reaches the shared validation, not a schema error.
      const badCursor = await client.callTool({
        name: 'search_history',
        arguments: { query: 'pageneedle', after: { timestampMs: 1, id: 1, matchSource: 'tool' } },
      });
      assert.equal(badCursor.isError, true);
      assert.match(JSON.stringify(badCursor.content), /INVALID_ARGUMENT: cursor match_source must be history or session_event \(got tool\)/);
    } finally {
      await client.close();
      await transport.close();
    }
  });
});

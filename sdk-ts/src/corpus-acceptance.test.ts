import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { copyFile, mkdir, mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';
import { SESSION_USAGE_CONTRACT_VERSION, getSessionUsage, sync, type SessionUsage } from './index.js';

// The acceptance checks of #181, run against the Rust fixture corpus itself
// rather than a TypeScript restatement of it, so the JS boundaries (native
// addon, SDK normalization, MCP, CLI) answer from the same bytes the Rust
// tests assert on.

const run = promisify(execFile);
const cli = fileURLToPath(new URL('./cli.js', import.meta.url));
const mcp = fileURLToPath(new URL('./mcp-server.js', import.meta.url));
const corpus = (name: string) => fileURLToPath(new URL(`../../crates/ai-hist/tests/fixtures/claude/${name}`, import.meta.url));

const MULTI_BLOCK_SESSION = '22222222-2222-2222-2222-222222222222';
const SLASH_SESSION = 'slash-session';

async function withCorpus(body: (dbPath: string, env: Record<string, string>) => Promise<void>): Promise<void> {
  const home = await mkdtemp(join(tmpdir(), 'relayhistory-corpus-'));
  const saved = { ...process.env };
  for (const key of Object.keys(process.env)) {
    if (/^(HOME|USERPROFILE|XDG_|OPENCODE_|TRAJECTORY_|AI_HIST_|RELAYHISTORY_|RELAYCAST_)/.test(key)) delete process.env[key];
  }
  const dbPath = join(home, 'history.db');
  process.env.HOME = home;
  process.env.USERPROFILE = home;
  process.env.RELAYHISTORY_HOME = join(home, 'commercial');
  process.env.RELAYHISTORY_NO_UPDATE_CHECK = '1';
  process.env.AI_HIST_DB = dbPath;
  try {
    const project = join(home, '.claude', 'projects', '-tmp-project');
    await mkdir(project, { recursive: true });
    await copyFile(corpus('multi-block-turn.jsonl'), join(project, `${MULTI_BLOCK_SESSION}.jsonl`));
    await copyFile(corpus('slash-command-triad.jsonl'), join(project, `${SLASH_SESSION}.jsonl`));
    await sync({ dbPath, scope: 'local', sourceConnectors: [] });
    const env = Object.fromEntries(Object.entries(process.env).filter((entry): entry is [string, string] => entry[1] !== undefined));
    await body(dbPath, env);
  } finally {
    for (const key of Object.keys(process.env)) if (!(key in saved)) delete process.env[key];
    Object.assign(process.env, saved);
    await rm(home, { recursive: true, force: true });
  }
}

test('MCP get_session_usage on the corpus multi-block-turn matches the Rust session_usage_summary', async () => {
  await withCorpus(async (dbPath, env) => {
    const client = new Client({ name: 'corpus-acceptance', version: '1' });
    const transport = new StdioClientTransport({ command: process.execPath, args: [mcp], env, stderr: 'pipe' });
    let viaMcp: SessionUsage;
    try {
      await client.connect(transport);
      const result = await client.callTool({ name: 'get_session_usage', arguments: { source: 'claude', session_id: MULTI_BLOCK_SESSION } });
      assert.equal(result.isError, undefined, JSON.stringify(result.content));
      viaMcp = JSON.parse((result.content as Array<{ text: string }>)[0].text) as SessionUsage;
    } finally {
      await client.close();
    }

    // The numbers `a_multi_block_turn_is_one_request_per_request_id`
    // (crates/ai-hist/src/session_usage/fixtures.rs) asserts on the same file:
    // four content-block records of one request collapse to one request.
    assert.equal(viaMcp.contractVersion, SESSION_USAGE_CONTRACT_VERSION);
    assert.equal(viaMcp.requestCount, 1);
    assert.deepEqual(viaMcp.accounting, ['per-message']);
    assert.deepEqual(viaMcp.diagnostics, []);
    assert.equal(viaMcp.overflowed, false);
    const usage = viaMcp.usage;
    assert.ok(usage, 'the corpus turn carries usage evidence');
    assert.equal(usage.inputTokens, 3);
    assert.equal(usage.outputTokens, 43);
    assert.equal(usage.cacheReadTokens, 11496);
    assert.equal(usage.cacheWriteTokens, 4773);
    assert.equal(usage.cacheWrite5mTokens, 0);
    assert.equal(usage.cacheWrite1hTokens, 4773);
    // No cost is computed; the corpus carries none.
    assert.equal(usage.reportedCostUsd, null);

    // MCP and the SDK answer the same document.
    assert.deepEqual(viaMcp, JSON.parse(JSON.stringify(await getSessionUsage('claude', MULTI_BLOCK_SESSION, { dbPath }))));
  });
});

test('ai-hist events --json on the corpus slash-command-triad shows control_kind on the triad rows', async () => {
  await withCorpus(async (dbPath, env) => {
    const { stdout } = await run(process.execPath, [cli, 'events', SLASH_SESSION, '--source', 'claude', '--db', dbPath, '--json', '--no-warning'], { env });
    // `--json` answers in snake_case, so the field is `control_kind` here.
    const page = JSON.parse(stdout) as { events: Array<{ event_uid: string; control_kind: string | null }> };
    const kinds = new Map(page.events.map((event) => [event.event_uid, event.control_kind]));
    // The same rows `claude_slash_command_triad_is_typed_grouped_and_kept_out_of_history`
    // (crates/ai-hist/tests/fixture_corpus.rs) types.
    for (const [uid, kind] of [
      ['u-prompt-1:0', null],
      ['u-cav-1:0', 'slash_command_caveat'],
      ['u-inv-1:0', 'slash_command_invocation'],
      ['u-out-1:0', 'slash_command_output'],
      ['u-cav-2:0', 'slash_command_caveat'],
      ['u-inv-2:0', 'slash_command_invocation'],
      ['u-out-2:0', 'slash_command_output'],
    ] as const) {
      assert.ok(kinds.has(uid), `event ${uid} is in the page`);
      assert.equal(kinds.get(uid), kind, uid);
    }
  });
});

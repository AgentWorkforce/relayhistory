import assert from 'node:assert/strict';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';

import {
  InvalidArgumentError, SESSION_USAGE_CONTRACT_VERSION,
  getSessionRequests, getSessionRequestsPage, getSessionUsage, sessionRequests, sync,
  type SessionRequest,
} from './index.js';

const CLAUDE_SESSION = 'usage-claude-1';
const CODEX_SESSION = 'usage-codex-1';

/**
 * One Claude turn whose `message.usage` is copied onto every content block —
 * the layout that makes counting rows multiply one API call by its block
 * count — followed by a second turn with an ordinary single block.
 */
const CLAUDE_USAGE = {
  input_tokens: 3,
  cache_creation_input_tokens: 4773,
  cache_read_input_tokens: 11496,
  cache_creation: { ephemeral_5m_input_tokens: 0, ephemeral_1h_input_tokens: 4773 },
  output_tokens: 43,
};

const CLAUDE_TRANSCRIPT = [
  { type: 'user', uuid: 'u1', sessionId: CLAUDE_SESSION, cwd: '/work/app', timestamp: '2026-08-30T10:00:00.000Z', message: { role: 'user', content: 'check the repo' } },
  // Three content blocks of one API call, each a copy of the same usage. The
  // `requestId` is what keeps them one request rather than three.
  {
    type: 'assistant', uuid: 'a1', parentUuid: 'u1', requestId: 'req_1', sessionId: CLAUDE_SESSION, cwd: '/work/app', timestamp: '2026-08-30T10:00:01.000Z',
    message: {
      role: 'assistant', model: 'claude-test', id: 'msg_1', usage: CLAUDE_USAGE,
      content: [
        { type: 'thinking', thinking: 'weigh it up', signature: 'sig' },
        { type: 'text', text: 'Looking now.' },
        { type: 'tool_use', id: 'toolu_1', name: 'Bash', input: { command: 'ls' } },
      ],
    },
  },
  // Input reported, output not: missing evidence, not a zero-output turn.
  {
    type: 'assistant', uuid: 'a2', parentUuid: 'a1', requestId: 'req_2', sessionId: CLAUDE_SESSION, cwd: '/work/app', timestamp: '2026-08-30T10:00:02.000Z',
    message: { role: 'assistant', model: 'claude-test', id: 'msg_2', usage: { input_tokens: 10 }, content: [{ type: 'text', text: 'Done.' }] },
  },
];

const HUGE_SESSION = 'usage-huge-1';

/**
 * A counter core accepts (it is a non-negative integer) that JavaScript
 * cannot hold exactly: `Number.MAX_SAFE_INTEGER + 1`.
 */
const HUGE_TRANSCRIPT = [
  { type: 'user', uuid: 'hu1', sessionId: HUGE_SESSION, cwd: '/work/app', timestamp: '2026-08-30T12:00:00.000Z', message: { role: 'user', content: 'hello' } },
  {
    type: 'assistant', uuid: 'ha1', parentUuid: 'hu1', requestId: 'req_huge', sessionId: HUGE_SESSION, cwd: '/work/app', timestamp: '2026-08-30T12:00:01.000Z',
    message: { role: 'assistant', model: 'claude-test', id: 'msg_huge', usage: { input_tokens: 9007199254740992 }, content: [{ type: 'text', text: 'ok' }] },
  },
];

const CODEX_ROLLOUT = [
  { timestamp: '2026-08-30T11:00:00.000Z', type: 'session_meta', payload: { id: CODEX_SESSION, cwd: '/work/codex' } },
  { timestamp: '2026-08-30T11:00:01.000Z', type: 'event_msg', payload: { type: 'user_message', message: 'fix the importer' } },
  { timestamp: '2026-08-30T11:00:02.000Z', type: 'event_msg', payload: { type: 'agent_message', message: 'Fixing it.' } },
  { timestamp: '2026-08-30T11:00:03.000Z', type: 'event_msg', payload: { type: 'token_count', info: { total_token_usage: { input_tokens: 3000, cached_input_tokens: 1000, output_tokens: 200, reasoning_output_tokens: 50, total_tokens: 3200 } } } },
];

async function seededDatabase(): Promise<{ dbPath: string; cleanup: () => Promise<void> }> {
  const root = await mkdtemp(join(tmpdir(), 'relayhistory-usage-'));
  const home = join(root, 'home');
  const claude = join(home, '.claude', 'projects', 'work-app');
  const codex = join(home, '.codex', 'sessions', '2026', '08', '30');
  await mkdir(claude, { recursive: true });
  await mkdir(codex, { recursive: true });
  await writeFile(join(claude, `${CLAUDE_SESSION}.jsonl`), `${CLAUDE_TRANSCRIPT.map((line) => JSON.stringify(line)).join('\n')}\n`);
  await writeFile(join(claude, `${HUGE_SESSION}.jsonl`), `${HUGE_TRANSCRIPT.map((line) => JSON.stringify(line)).join('\n')}\n`);
  await writeFile(join(codex, `rollout-${CODEX_SESSION}.jsonl`), `${CODEX_ROLLOUT.map((line) => JSON.stringify(line)).join('\n')}\n`);
  const saved = { HOME: process.env.HOME, USERPROFILE: process.env.USERPROFILE };
  process.env.HOME = home;
  process.env.USERPROFILE = home;
  const dbPath = join(root, 'history.db');
  try {
    await sync({ dbPath });
  } finally {
    if (saved.HOME === undefined) delete process.env.HOME; else process.env.HOME = saved.HOME;
    if (saved.USERPROFILE === undefined) delete process.env.USERPROFILE; else process.env.USERPROFILE = saved.USERPROFILE;
  }
  return { dbPath, cleanup: () => rm(root, { recursive: true, force: true }) };
}

test('a request page collapses one message into one request with normalized usage', async () => {
  const { dbPath, cleanup } = await seededDatabase();
  try {
    const page = await getSessionRequestsPage('claude', CLAUDE_SESSION, { dbPath });
    assert.equal(page.contractVersion, SESSION_USAGE_CONTRACT_VERSION);
    assert.equal(page.source, 'claude');
    assert.equal(page.sessionId, CLAUDE_SESSION);
    const multiBlock = page.requests.find((request) => request.requestKey === 'request-id:req_1') as SessionRequest;
    assert.ok(multiBlock, 'the three-block message is one request');
    assert.equal(multiBlock.requestKeySource, 'request-id');
    // One record carrying three content blocks: one message id, three events.
    assert.equal(multiBlock.messageIds.length, 1);
    assert.equal(multiBlock.eventCount, 3);
    assert.equal(multiBlock.hasThinking, true);
    assert.deepEqual(multiBlock.toolUseIds, ['toolu_1']);
    assert.deepEqual(multiBlock.diagnostics, []);
    assert.equal(multiBlock.model, 'claude-test');
    // No local source records a provider, and it is never inferred from the
    // model string — burn owns that.
    assert.equal(multiBlock.provider, null);
    const usage = multiBlock.usage;
    assert.ok(usage);
    assert.equal(usage.inputTokens, 3);
    assert.equal(usage.outputTokens, 43);
    assert.equal(usage.cacheReadTokens, 11496);
    assert.equal(usage.cacheWriteTokens, 4773);
    // The split burn prices differently survives the whole boundary.
    assert.equal(usage.cacheWrite5mTokens, 0);
    assert.equal(usage.cacheWrite1hTokens, 4773);
    assert.equal(usage.accounting, 'per-message');
    // Claude reports no reasoning count and no total of its own; neither is
    // invented.
    assert.equal(usage.reasoningTokens, null);
    assert.equal(usage.providerTotalTokens, null);
    assert.equal(usage.reportedCostUsd, null);
  } finally {
    await cleanup();
  }
});

test('an absent output count reads as zero with the coverage flag that says so', async () => {
  const { dbPath, cleanup } = await seededDatabase();
  try {
    const requests = await getSessionRequests('claude', CLAUDE_SESSION, { dbPath });
    const partial = requests.find((request) => request.requestKey === 'request-id:req_2') as SessionRequest;
    assert.ok(partial);
    const usage = partial.usage;
    assert.ok(usage);
    assert.equal(usage.inputTokens, 10);
    assert.equal(usage.outputTokens, 0);
    assert.equal(usage.hasInputTokens, true);
    assert.equal(usage.hasOutputTokens, false);
  } finally {
    await cleanup();
  }
});

test('a session rollup counts each request once and reports its accounting mode', async () => {
  const { dbPath, cleanup } = await seededDatabase();
  try {
    const summary = await getSessionUsage('claude', CLAUDE_SESSION, { dbPath });
    assert.equal(summary.contractVersion, SESSION_USAGE_CONTRACT_VERSION);
    assert.equal(summary.requestCount, 2);
    assert.equal(summary.totalRequestCount, 2);
    const usage = summary.usage;
    assert.ok(usage);
    // One multi-block turn (3 + 43) plus one partial turn (10 + nothing).
    assert.equal(usage.inputTokens, 13);
    assert.equal(usage.outputTokens, 43);
    assert.equal(usage.cacheReadTokens, 11496);
    assert.deepEqual(summary.accounting, ['per-message']);
    assert.deepEqual(summary.models, ['claude-test']);
    assert.equal(summary.overflowed, false);
    // The second request reports `{input_tokens: 10}` and says nothing about
    // cache writes, so it cannot certify the pair's TTL split. The buckets are
    // withheld and the summary says why, rather than passing the first
    // request's split off as the session's.
    assert.equal(usage.cacheWrite5mTokens, null);
    assert.equal(usage.cacheWrite1hTokens, null);
    assert.deepEqual(summary.diagnostics, ['partial-cache-write-split']);

    const codex = await getSessionUsage('codex', CODEX_SESSION, { dbPath });
    assert.deepEqual(codex.accounting, ['cumulative-delta']);
    const codexUsage = codex.usage;
    assert.ok(codexUsage);
    // Input arrives inclusive of cache reads and is made exclusive here.
    assert.equal(codexUsage.inputTokens, 2000);
    assert.equal(codexUsage.cacheReadTokens, 1000);
    assert.equal(codexUsage.reasoningTokens, 50);
    // As reported, never recomputed from the parts.
    assert.equal(codexUsage.providerTotalTokens, 3200);
  } finally {
    await cleanup();
  }
});

test('a session with no usage evidence reports null rather than an assumed zero', async () => {
  const { dbPath, cleanup } = await seededDatabase();
  try {
    const summary = await getSessionUsage('claude', 'no-such-session', { dbPath });
    assert.equal(summary.usage, null);
    assert.equal(summary.requestCount, 0);
    assert.deepEqual(summary.accounting, []);
    assert.equal(summary.firstTsMs, null);
  } finally {
    await cleanup();
  }
});

test('a count JavaScript cannot represent is refused rather than rounded', async () => {
  const { dbPath, cleanup } = await seededDatabase();
  try {
    const page = await getSessionRequestsPage('claude', HUGE_SESSION, { dbPath });
    assert.equal(page.requests.length, 1);
    const request = page.requests[0] as SessionRequest;
    // Core normalized it — it is a valid non-negative integer — but it cannot
    // cross the boundary intact, so no number is handed over at all.
    assert.equal(request.usage, null);
    assert.equal(request.usageError, 'USAGE_COUNT_NOT_REPRESENTABLE');
    assert.ok(request.diagnostics.includes('count-not-representable'));

    const summary = await getSessionUsage('claude', HUGE_SESSION, { dbPath });
    assert.equal(summary.usage, null);
    assert.equal(summary.totalRequestCount, 1);
  } finally {
    await cleanup();
  }
});

test('a session whose usage is unreadable still reports its requests and why', async () => {
  const { dbPath, cleanup } = await seededDatabase();
  try {
    // The huge-count session is the readable-rows / unusable-usage shape: the
    // request exists and is counted, the totals are withheld, and the reason
    // travels with them. Corrupt usage must not look like no session.
    const summary = await getSessionUsage('claude', HUGE_SESSION, { dbPath });
    assert.equal(summary.usage, null);
    assert.equal(summary.totalRequestCount, 1);
    assert.deepEqual(summary.models, ['claude-test']);
    assert.ok(summary.diagnostics.length > 0);
    assert.notEqual(summary.firstTsMs, null);

    // And a session that was never recorded is still the empty answer.
    const absent = await getSessionUsage('claude', 'no-such-session', { dbPath });
    assert.equal(absent.usage, null);
    assert.equal(absent.totalRequestCount, 0);
    assert.deepEqual(absent.diagnostics, []);
    assert.equal(absent.firstTsMs, null);
  } finally {
    await cleanup();
  }
});

test('the request iterator walks every page and both halves of the identity are required', async () => {
  const { dbPath, cleanup } = await seededDatabase();
  try {
    const walked: string[] = [];
    for await (const request of sessionRequests('claude', CLAUDE_SESSION, { dbPath, limit: 1 })) {
      walked.push(request.requestKey);
    }
    // Namespace-qualified, so a key is unique even when two identity
    // namespaces carry the same text.
    assert.deepEqual(walked.sort(), ['request-id:req_1', 'request-id:req_2']);

    for (const [source, sessionId] of [['claude', ''], ['', CLAUDE_SESSION], ['claude', ' padded ']] as const) {
      await assert.rejects(
        () => getSessionRequestsPage(source as 'claude', sessionId, { dbPath }),
        InvalidArgumentError,
      );
      await assert.rejects(
        () => getSessionUsage(source as 'claude', sessionId, { dbPath }),
        InvalidArgumentError,
      );
    }
  } finally {
    await cleanup();
  }
});

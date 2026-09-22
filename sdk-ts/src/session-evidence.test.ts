import assert from 'node:assert/strict';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';

import {
  EVIDENCE_KINDS, FULL_SESSION_KINDS, InvalidArgumentError, NativeContractMismatchError,
  SESSION_EVIDENCE_CONTRACT_VERSION, SESSION_HYDRATION_CONTRACT_VERSION,
  getSessionEvents, getSessionFileEdits, getSessionFileEditsPage, getSessionToolCalls,
  getSessionToolCallsPage, getSessionUserTurns, getSessionUserTurnsPage,
  hydrateSession, parseStoredJson, sessionFileEdits, sessionToolCalls, sync,
  type EvidenceCursor, type SessionFileEdit, type SessionToolCall,
} from './index.js';
import { combineHydration, normalizeHydration } from './normalization.js';

// Undated tool calls and file edits are legal — both `ts_ms` columns are
// nullable — but no provider adapter writes one, so the only way to build the
// fixture that exercises a null cursor end to end is to add the rows directly.
// Production code stays SQLite-free; this is test-only, and `node:sqlite`
// arrived in Node 22 while the SDK still supports Node 20.
const sqlite = await import('node:sqlite').catch(() => null);
const needsNodeSqlite = sqlite ? false : 'node:sqlite requires Node >= 22';

const SHARED_SESSION = 'shared-1';

const CLAUDE_TRANSCRIPT = [
  { type: 'user', uuid: 'u1', sessionId: SHARED_SESSION, cwd: '/work/app', gitBranch: 'main', timestamp: '2026-08-30T10:00:00.000Z', message: { role: 'user', content: 'update auth' } },
  { type: 'assistant', uuid: 'a1', parentUuid: 'u1', sessionId: SHARED_SESSION, cwd: '/work/app', gitBranch: 'main', timestamp: '2026-08-30T10:00:01.000Z', message: { role: 'assistant', model: 'claude-test', content: [{ type: 'tool_use', id: 'toolu_1', name: 'Edit', input: { file_path: '/work/app/auth.ts', old_string: 'old', new_string: 'new' } }] } },
  { type: 'user', uuid: 'r1', parentUuid: 'a1', sessionId: SHARED_SESSION, cwd: '/work/app', gitBranch: 'main', timestamp: '2026-08-30T10:00:02.000Z', message: { role: 'user', content: [{ type: 'tool_result', tool_use_id: 'toolu_1', content: 'ok', toolUseResult: { filePath: '/work/app/auth.ts', structuredPatch: '--- a/auth.ts\n+++ b/auth.ts\n-old\n+new\n', userModified: true } }] } },
  { type: 'assistant', uuid: 'a2', parentUuid: 'r1', sessionId: SHARED_SESSION, cwd: '/work/app', gitBranch: 'main', timestamp: '2026-08-30T10:00:03.000Z', message: { role: 'assistant', model: 'claude-test', content: [{ type: 'tool_use', id: 'toolu_2', name: 'Write', input: { file_path: '/work/app/notes.md', content: 'notes' } }] } },
  { type: 'user', uuid: 'r2', parentUuid: 'a2', sessionId: SHARED_SESSION, cwd: '/work/app', gitBranch: 'main', timestamp: '2026-08-30T10:00:04.000Z', message: { role: 'user', content: [{ type: 'tool_result', tool_use_id: 'toolu_2', content: 'ok', toolUseResult: { filePath: '/work/app/notes.md', structuredPatch: [{ oldStart: 1, newStart: 1, lines: ['+notes'] }], userModified: false } }] } },
  { type: 'assistant', uuid: 'a3', parentUuid: 'r2', sessionId: SHARED_SESSION, cwd: '/work/app', gitBranch: 'main', timestamp: '2026-08-30T10:00:05.000Z', message: { role: 'assistant', model: 'claude-test', content: [{ type: 'tool_use', id: 'toolu_3', name: 'Bash', input: { command: 'cargo test' } }] } },
  { type: 'user', uuid: 'r3', parentUuid: 'a3', sessionId: SHARED_SESSION, cwd: '/work/app', gitBranch: 'main', timestamp: '2026-08-30T10:00:06.000Z', message: { role: 'user', content: [{ type: 'tool_result', tool_use_id: 'toolu_3', is_error: true, content: 'failed' }] } },
  // A harness line, not a block on a user message. It is stored as a tool
  // result because that is what it is evidence of, so anything that groups on
  // role alone turns it into a user turn that never happened.
  { type: 'system', subtype: 'subagent_completed', sessionId: SHARED_SESSION, timestamp: '2026-08-30T10:00:07.000Z', parent_tool_use_id: 'toolu_3', agent_id: 'agent-1', subagent_session_id: 'child-1', status: 'completed', content: 'subagent completed' },
];

const CODEX_ROLLOUT = [
  { timestamp: '2026-08-30T11:00:00.000Z', type: 'session_meta', payload: { id: SHARED_SESSION, cwd: '/work/codex', git: { branch: 'main' } } },
  { timestamp: '2026-08-30T11:00:01.000Z', type: 'event_msg', payload: { type: 'user_message', message: 'fix the importer' } },
  { timestamp: '2026-08-30T11:00:02.000Z', type: 'response_item', payload: { type: 'function_call', id: 'fc_1', name: 'exec_command', arguments: '{"cmd":"git status"}', call_id: 'call_1' } },
  { timestamp: '2026-08-30T11:00:03.000Z', type: 'response_item', payload: { type: 'custom_tool_call', id: 'ctc_1', status: 'completed', call_id: 'call_2', name: 'apply_patch', input: '*** Begin Patch\n*** Update File: /work/codex/a.rs\n@@\n+one\n*** End Patch\n' } },
  // One apply_patch call touching two files: file_edits is keyed by tool use,
  // so the writer scopes the key per path rather than losing a file.
  { timestamp: '2026-08-30T11:00:04.000Z', type: 'event_msg', payload: { type: 'patch_apply_end', call_id: 'call_2', success: true, changes: { '/work/codex/a.rs': { type: 'update', unified_diff: '@@\n+one\n-zero' }, '/work/codex/b.rs': { type: 'update', unified_diff: '@@\n+two' } } } },
];

async function seededDatabase(): Promise<{
  dbPath: string; home: string; cleanup: () => Promise<void>;
}> {
  const root = await mkdtemp(join(tmpdir(), 'relayhistory-evidence-'));
  const home = join(root, 'home');
  const claude = join(home, '.claude', 'projects', 'work-app');
  const codex = join(home, '.codex', 'sessions', '2026', '08', '30');
  await mkdir(claude, { recursive: true });
  await mkdir(codex, { recursive: true });
  await writeFile(join(claude, `${SHARED_SESSION}.jsonl`), `${CLAUDE_TRANSCRIPT.map((line) => JSON.stringify(line)).join('\n')}\n`);
  await writeFile(join(codex, `rollout-${SHARED_SESSION}.jsonl`), `${CODEX_ROLLOUT.map((line) => JSON.stringify(line)).join('\n')}\n`);
  const saved = { HOME: process.env.HOME, USERPROFILE: process.env.USERPROFILE, XDG_DATA_HOME: process.env.XDG_DATA_HOME };
  process.env.HOME = home;
  process.env.USERPROFILE = home;
  process.env.XDG_DATA_HOME = join(home, 'share');
  const dbPath = join(root, 'history.db');
  try {
    await sync({ dbPath });
  } finally {
    if (saved.HOME === undefined) delete process.env.HOME; else process.env.HOME = saved.HOME;
    if (saved.USERPROFILE === undefined) delete process.env.USERPROFILE; else process.env.USERPROFILE = saved.USERPROFILE;
    if (saved.XDG_DATA_HOME === undefined) delete process.env.XDG_DATA_HOME; else process.env.XDG_DATA_HOME = saved.XDG_DATA_HOME;
  }
  return { dbPath, home, cleanup: () => rm(root, { recursive: true, force: true }) };
}

test('tool call pages expose structured arguments, errors, and identity', async () => {
  const { dbPath, cleanup } = await seededDatabase();
  try {
    const page = await getSessionToolCallsPage('claude', SHARED_SESSION, { dbPath });
    assert.equal(page.contractVersion, SESSION_EVIDENCE_CONTRACT_VERSION);
    assert.equal(page.source, 'claude');
    assert.equal(page.sessionId, SHARED_SESSION);
    assert.equal(page.nextCursor, null);
    assert.deepEqual(page.toolCalls.map((call) => call.toolUseId), ['toolu_1', 'toolu_2', 'toolu_3']);

    const [edit, write, bash] = page.toolCalls;
    assert.deepEqual(edit.args, { file_path: '/work/app/auth.ts', old_string: 'old', new_string: 'new' });
    assert.deepEqual(JSON.parse(String(edit.argsJson)), edit.args);
    assert.equal(edit.name, 'Edit');
    assert.equal(edit.target, '/work/app/auth.ts');
    assert.equal(edit.messageId, 'a1');
    assert.equal(typeof edit.tsMs, 'number');
    // A tool result that never reported a verdict leaves the error unknown
    // rather than asserting success.
    assert.equal(edit.isError, null);
    assert.equal(write.name, 'Write');
    assert.equal(bash.isError, true);
    assert.deepEqual(bash.args, { command: 'cargo test' });
  } finally {
    await cleanup();
  }
});

test('file edit pages expose patches, provenance, and one row per edited file', async () => {
  const { dbPath, cleanup } = await seededDatabase();
  try {
    const claude = await getSessionFileEditsPage('claude', SHARED_SESSION, { dbPath });
    assert.equal(claude.contractVersion, SESSION_EVIDENCE_CONTRACT_VERSION);
    assert.deepEqual(claude.fileEdits.map((edit) => edit.filePath), ['/work/app/auth.ts', '/work/app/notes.md']);
    const [auth, notes] = claude.fileEdits;
    assert.equal(auth.toolName, 'Edit');
    assert.equal(auth.messageId, 'a1');
    assert.equal(auth.gitBranch, 'main');
    assert.equal(auth.cwd, '/work/app');
    assert.equal(auth.userModified, true);
    assert.equal(auth.linesAdded, 1);
    assert.equal(auth.linesRemoved, 1);
    assert.equal(auth.structuredPatch, '--- a/auth.ts\n+++ b/auth.ts\n-old\n+new\n');
    assert.equal(auth.structuredPatchJson, JSON.stringify('--- a/auth.ts\n+++ b/auth.ts\n-old\n+new\n'));
    assert.equal(notes.userModified, false);
    assert.deepEqual(notes.structuredPatch, [{ oldStart: 1, newStart: 1, lines: ['+notes'] }]);

    // One codex apply_patch call touching two files stores one row per file,
    // keyed by `<call id>#<path>` because file_edits is unique per tool use.
    const codex = await getSessionFileEditsPage('codex', SHARED_SESSION, { dbPath });
    assert.deepEqual(codex.fileEdits.map((edit) => edit.filePath), ['/work/codex/a.rs', '/work/codex/b.rs']);
    assert.deepEqual(codex.fileEdits.map((edit) => edit.toolUseId), ['call_2#/work/codex/a.rs', 'call_2#/work/codex/b.rs']);
    assert.ok(codex.fileEdits.every((edit) => edit.toolName === 'apply_patch'));
  } finally {
    await cleanup();
  }
});

test('evidence pages never mix two providers that share a session id', async () => {
  const { dbPath, cleanup } = await seededDatabase();
  try {
    const claudeCalls = await getSessionToolCalls('claude', SHARED_SESSION, { dbPath });
    const codexCalls = await getSessionToolCalls('codex', SHARED_SESSION, { dbPath });
    assert.ok(claudeCalls.length > 0 && codexCalls.length > 0);
    assert.ok(claudeCalls.every((call) => call.source === 'claude'));
    assert.ok(codexCalls.every((call) => call.source === 'codex'));
    assert.deepEqual(codexCalls.map((call) => call.toolUseId), ['call_1', 'call_2']);

    const claudeEdits = await getSessionFileEdits('claude', SHARED_SESSION, { dbPath });
    const codexEdits = await getSessionFileEdits('codex', SHARED_SESSION, { dbPath });
    assert.ok(claudeEdits.every((edit) => edit.source === 'claude'));
    assert.ok(codexEdits.every((edit) => edit.source === 'codex'));
  } finally {
    await cleanup();
  }
});

test('paged, iterated, and collected reads agree at every page size', async () => {
  const { dbPath, cleanup } = await seededDatabase();
  try {
    const allCalls = await getSessionToolCalls('claude', SHARED_SESSION, { dbPath });
    const allEdits = await getSessionFileEdits('claude', SHARED_SESSION, { dbPath });
    assert.equal(allCalls.length, 3);
    assert.equal(allEdits.length, 2);

    for (const limit of [1, 2, 3, 1000]) {
      const calls: SessionToolCall[] = [];
      for await (const call of sessionToolCalls('claude', SHARED_SESSION, { dbPath, limit })) calls.push(call);
      assert.deepEqual(calls, allCalls, `tool calls at limit ${limit}`);
      const edits: SessionFileEdit[] = [];
      for await (const edit of sessionFileEdits('claude', SHARED_SESSION, { dbPath, limit })) edits.push(edit);
      assert.deepEqual(edits, allEdits, `file edits at limit ${limit}`);
    }

    // Cursors survive the JSON round trip the CLI and MCP server use.
    const first = await getSessionToolCallsPage('claude', SHARED_SESSION, { dbPath, limit: 1 });
    assert.equal(first.toolCalls.length, 1);
    const roundTripped = JSON.parse(JSON.stringify(first.nextCursor)) as EvidenceCursor;
    const second = await getSessionToolCallsPage('claude', SHARED_SESSION, { dbPath, limit: 2, after: roundTripped });
    assert.deepEqual(second.toolCalls, allCalls.slice(1));
    assert.equal(second.nextCursor, null);

    // Both spellings of the undated tail cross the native boundary, which
    // takes an absent `tsMs` and cannot convert an explicit null: the cursor
    // a page hands back for an undated row is `tsMs: null`, and a transport
    // that drops nulls delivers the same cursor without the field at all.
    // On this fully dated database both name an empty tail rather than
    // failing the call.
    for (const after of [{ tsMs: null, id: allCalls[0].id }, { id: allCalls[0].id }]) {
      const tail = await getSessionToolCallsPage('claude', SHARED_SESSION, { dbPath, after });
      assert.deepEqual(tail.toolCalls, []);
      assert.equal(tail.nextCursor, null);
      const edits = await getSessionFileEditsPage('claude', SHARED_SESSION, { dbPath, after });
      assert.deepEqual(edits.fileEdits, []);
      assert.equal(edits.nextCursor, null);
    }
  } finally {
    await cleanup();
  }
});

test('undated records page after the dated ones through a null cursor', { skip: needsNodeSqlite }, async () => {
  const { dbPath, cleanup } = await seededDatabase();
  try {
    const database = new sqlite!.DatabaseSync(dbPath);
    try {
      database.exec(`
        INSERT INTO tool_calls (source, session_id, message_id, tool_use_id, name, target, args_json, is_error, ts_ms)
        VALUES ('claude', '${SHARED_SESSION}', 'a9', 'toolu_undated_1', 'Bash', 'ls', '{"command":"ls"}', 0, NULL),
               ('claude', '${SHARED_SESSION}', 'a9', 'toolu_undated_2', 'Bash', 'pwd', '{"command":"pwd"}', NULL, NULL);
        INSERT INTO file_edits (source, session_id, message_id, tool_use_id, file_path, tool_name, lines_added, lines_removed, ts_ms)
        VALUES ('claude', '${SHARED_SESSION}', 'a9', 'toolu_undated_1', '/work/app/undated-a.ts', 'Edit', NULL, NULL, NULL),
               ('claude', '${SHARED_SESSION}', 'a9', 'toolu_undated_2', '/work/app/undated-b.ts', 'Edit', 3, 1, NULL);
      `);
    } finally {
      database.close();
    }

    const allCalls = await getSessionToolCalls('claude', SHARED_SESSION, { dbPath });
    const allEdits = await getSessionFileEdits('claude', SHARED_SESSION, { dbPath });
    assert.deepEqual(allCalls.map((call) => call.toolUseId), [
      'toolu_1', 'toolu_2', 'toolu_3', 'toolu_undated_1', 'toolu_undated_2',
    ]);
    assert.deepEqual(allCalls.slice(3).map((call) => call.tsMs), [null, null]);
    assert.deepEqual(allEdits.map((edit) => edit.filePath), [
      '/work/app/auth.ts', '/work/app/notes.md', '/work/app/undated-a.ts', '/work/app/undated-b.ts',
    ]);

    // Every page size crosses from the dated head into the undated tail, so a
    // cursor whose `tsMs` is null is walked rather than only constructed.
    for (const limit of [1, 2, 3, 4, 5]) {
      const calls: SessionToolCall[] = [];
      for await (const call of sessionToolCalls('claude', SHARED_SESSION, { dbPath, limit })) calls.push(call);
      assert.deepEqual(calls, allCalls, `tool calls at limit ${limit}`);
      const edits: SessionFileEdit[] = [];
      for await (const edit of sessionFileEdits('claude', SHARED_SESSION, { dbPath, limit })) edits.push(edit);
      assert.deepEqual(edits, allEdits, `file edits at limit ${limit}`);
    }

    // The cursor that lands inside the tail is the null one, and it round
    // trips through JSON exactly as the CLI and MCP server send it.
    const inTail = await getSessionToolCallsPage('claude', SHARED_SESSION, { dbPath, limit: 4 });
    assert.equal(inTail.nextCursor?.tsMs, null);
    const resumed = await getSessionToolCallsPage('claude', SHARED_SESSION, {
      dbPath, after: JSON.parse(JSON.stringify(inTail.nextCursor)) as EvidenceCursor,
    });
    assert.deepEqual(resumed.toolCalls.map((call) => call.toolUseId), ['toolu_undated_2']);
  } finally {
    await cleanup();
  }
});

test('evidence pages read unknown sessions empty and require both identity parts', async () => {
  const { dbPath, cleanup } = await seededDatabase();
  try {
    const missing = await getSessionToolCallsPage('claude', 'no-such-session', { dbPath });
    assert.deepEqual(missing.toolCalls, []);
    assert.equal(missing.nextCursor, null);
    assert.deepEqual((await getSessionFileEditsPage('cursor', SHARED_SESSION, { dbPath })).fileEdits, []);

    for (const operation of [
      () => getSessionToolCallsPage('claude', '', { dbPath }),
      () => getSessionFileEditsPage('claude', '   ', { dbPath }),
      () => getSessionToolCallsPage('' as never, SHARED_SESSION, { dbPath }),
    ]) {
      await assert.rejects(operation(), (error: unknown) => error instanceof InvalidArgumentError
        && error.code === 'INVALID_ARGUMENT');
    }
  } finally {
    await cleanup();
  }
});

test('an unsupported provider is rejected, not answered with an empty page', async () => {
  const { dbPath, cleanup } = await seededDatabase();
  try {
    // `source` is half the identity of an evidence page, so a provider this
    // build does not know cannot honestly read as "this session recorded
    // nothing" -- both page readers reject it the way hydrateSession does.
    for (const source of ['claud', 'Claude', 'openai', 'sqlite']) {
      for (const operation of [
        () => getSessionToolCallsPage(source as never, SHARED_SESSION, { dbPath }),
        () => getSessionFileEditsPage(source as never, SHARED_SESSION, { dbPath }),
        () => getSessionToolCalls(source as never, SHARED_SESSION, { dbPath }),
        () => getSessionFileEdits(source as never, SHARED_SESSION, { dbPath }),
      ]) {
        await assert.rejects(operation(), (error: unknown) => error instanceof InvalidArgumentError
          && error.code === 'INVALID_ARGUMENT'
          && error.message.includes(source));
      }
    }

    // Every id the SDK does publish stays accepted, including `trajectory`,
    // which the catalog excludes but evidence rows may carry.
    for (const source of ['claude', 'codex', 'cursor', 'grok', 'relay', 'trajectory', 'opencode', 'devin'] as const) {
      assert.equal((await getSessionToolCallsPage(source, 'no-such-session', { dbPath })).source, source);
      assert.equal((await getSessionFileEditsPage(source, 'no-such-session', { dbPath })).source, source);
    }
  } finally {
    await cleanup();
  }
});

test('hydration reports coverage alongside a capability computed from it', async () => {
  const { dbPath, home, cleanup } = await seededDatabase();
  const saved = { HOME: process.env.HOME, USERPROFILE: process.env.USERPROFILE, XDG_DATA_HOME: process.env.XDG_DATA_HOME };
  process.env.HOME = home;
  process.env.USERPROFILE = home;
  process.env.XDG_DATA_HOME = join(home, 'share');
  try {
    const hydrated = await hydrateSession({ source: 'claude', sessionId: SHARED_SESSION, dbPath });
    assert.equal(hydrated.contractVersion, SESSION_HYDRATION_CONTRACT_VERSION);
    assert.equal(SESSION_HYDRATION_CONTRACT_VERSION, 3);
    assert.equal(hydrated.capability, 'full');
    assert.deepEqual(hydrated.coverage, [...FULL_SESSION_KINDS]);
    for (const kind of hydrated.coverage) {
      assert.ok((EVIDENCE_KINDS as readonly string[]).includes(kind), `${kind} is a known kind`);
    }

    // includeRelated: false never walks the subagent sidecars, so the result
    // must not claim relationship coverage it did not look for -- and it must
    // stay `partial`, since a merger reading `full` would treat unexamined
    // delegation as fully indexed.
    const alone = await hydrateSession({
      source: 'claude', sessionId: SHARED_SESSION, dbPath, includeRelated: false,
    });
    assert.equal(alone.capability, 'partial');
    assert.deepEqual(alone.coverage, FULL_SESSION_KINDS.filter((kind) => kind !== 'relationship'));
    const declined = alone.diagnostics.find((item) => item.code === 'HYDRATION_PARTIAL_COVERAGE');
    assert.ok(declined, 'the declined evidence is named');
    assert.ok(declined.message.includes('relationship'), declined.message);
    assert.ok(declined.message.includes('include_related is off'), declined.message);
  } finally {
    if (saved.HOME === undefined) delete process.env.HOME; else process.env.HOME = saved.HOME;
    if (saved.USERPROFILE === undefined) delete process.env.USERPROFILE;
    else process.env.USERPROFILE = saved.USERPROFILE;
    if (saved.XDG_DATA_HOME === undefined) delete process.env.XDG_DATA_HOME;
    else process.env.XDG_DATA_HOME = saved.XDG_DATA_HOME;
    await cleanup();
  }
});

test('a native full capability unsupported by its coverage is a contract mismatch, not a value', () => {
  const base = {
    contractVersion: SESSION_HYDRATION_CONTRACT_VERSION,
    source: 'cursor', sessionId: 's', status: 'hydrated',
    capability: 'full', discoveryState: 'full', presence: 'local',
    indexedThrough: { sourceStamp: null, lastEventAtMs: null },
    evidence: { prompts: 1, events: 0, toolCalls: 0, fileEdits: 0, relatedSessions: 0 },
    bytesRead: 0,
    relatedSessionIds: [], diagnostics: [],
  };
  // Exactly the defect contract 3 removes: a well-formed result asserting
  // `full` over evidence kinds nothing looked at must not reach a caller.
  assert.throws(
    () => normalizeHydration({ ...base, coverage: ['history'] }),
    (error: unknown) => error instanceof NativeContractMismatchError
      && error.code === 'NATIVE_CONTRACT_MISMATCH',
  );
  assert.throws(
    () => normalizeHydration({ ...base, capability: 'partial', coverage: ['prompts'] }),
    (error: unknown) => error instanceof NativeContractMismatchError
      && error.message.includes('prompts'),
  );
  assert.equal(
    normalizeHydration({ ...base, coverage: [...FULL_SESSION_KINDS] }).capability,
    'full',
  );
  assert.deepEqual(
    normalizeHydration({ ...base, capability: 'partial', coverage: ['history'] }).coverage,
    ['history'],
  );
  // An absent or non-list `coverage` is malformed, not "covers nothing":
  // defaulting it would pass validation and strip the coverage a merge needs.
  for (const coverage of [undefined, null, 'history', {}]) {
    assert.throws(
      () => normalizeHydration({ ...base, capability: 'partial', coverage }),
      (error: unknown) => error instanceof NativeContractMismatchError
        && error.code === 'NATIVE_CONTRACT_MISMATCH'
        && error.message.includes('without a coverage list'),
      `coverage: ${JSON.stringify(coverage)} is rejected`,
    );
  }
  // An empty list is legitimate -- a listing-only connector covers nothing.
  assert.deepEqual(
    normalizeHydration({ ...base, capability: 'shallow_only', coverage: [] }).coverage,
    [],
  );

  // The mirror-image defects are rejected too. They read as harmless
  // understatements, but `combineHydration` ranks the parts of a merge by the
  // reported capability before recomputing, so an under-reported result loses
  // the `best` selection and the top-level fields that come with it.
  for (const [capability, coverage, expected] of [
    ['partial', [...FULL_SESSION_KINDS], 'full'],
    ['shallow_only', ['history'], 'partial'],
    ['shallow_only', [...FULL_SESSION_KINDS], 'full'],
    ['full', [], 'shallow_only'],
  ] as const) {
    assert.throws(
      () => normalizeHydration({ ...base, capability, coverage: [...coverage] }),
      (error: unknown) => error instanceof NativeContractMismatchError
        && error.code === 'NATIVE_CONTRACT_MISMATCH'
        && error.message.includes('inconsistent with its coverage')
        && error.message.includes(`expected ${expected}`),
      `${capability} over [${coverage.join(', ')}] is rejected`,
    );
  }
  // A genuine partial -- some kinds covered, not all -- still normalizes.
  assert.equal(
    normalizeHydration({ ...base, capability: 'partial', coverage: ['history', 'tool_call'] })
      .capability,
    'partial',
  );
});

function diagnostic(code: string, message: string): Record<string, unknown> {
  return { code, message, durationMs: null, sourceBytes: null, recordsParsed: null };
}

function hydrationPart(
  capability: 'full' | 'partial' | 'shallow_only',
  coverage: readonly string[],
  evidence: Partial<{ prompts: number; events: number; toolCalls: number; fileEdits: number }> = {},
): ReturnType<typeof normalizeHydration> {
  // A real partial result carries its own partial-coverage note, naming only
  // the kinds *that* presence is missing.
  const missing = FULL_SESSION_KINDS.filter((kind) => !coverage.includes(kind));
  return normalizeHydration({
    contractVersion: SESSION_HYDRATION_CONTRACT_VERSION,
    source: 'claude', sessionId: SHARED_SESSION, status: 'hydrated',
    capability, discoveryState: capability === 'full' ? 'full' : 'shallow', presence: 'local',
    indexedThrough: { sourceStamp: null, lastEventAtMs: null },
    evidence: {
      prompts: 0, events: 0, toolCalls: 0, fileEdits: 0, relatedSessions: 0, ...evidence,
    },
    bytesRead: 0,
    coverage: [...coverage],
    relatedSessionIds: [],
    diagnostics: [
      diagnostic('HYDRATION_METRICS', `metrics for ${coverage.join('+') || 'nothing'}`),
      ...(missing.length > 0
        ? [diagnostic('HYDRATION_PARTIAL_COVERAGE', `no ${missing.join(', ')}`)]
        : []),
    ],
  });
}

test('merging complementary partial presences yields a capability the union supports', () => {
  // Neither connector is `full` on its own, but between them every kind in
  // FULL_SESSION_KINDS is indexed. Carrying an input's `partial` through the
  // merge ranked complete merged evidence below a single full result -- the
  // same "capability that does not describe the coverage" defect one level up.
  const a = hydrationPart('partial', ['history', 'session_event'], { prompts: 2, events: 5 });
  const b = hydrationPart('partial', ['tool_call', 'file_edit', 'relationship'], { toolCalls: 3, fileEdits: 1 });

  const merged = combineHydration(a, b);
  assert.deepEqual(merged.coverage, [...FULL_SESSION_KINDS]);
  assert.equal(merged.capability, 'full');
  // Each part arrived with its own partial-coverage note naming kinds the
  // other one covers. Concatenating them would leave a `full` result carrying
  // a claim that four kinds are absent.
  assert.deepEqual(
    merged.diagnostics.filter((item) => item.code === 'HYDRATION_PARTIAL_COVERAGE'),
    [],
  );
  // Every other diagnostic is a per-presence fact and survives.
  assert.deepEqual(
    merged.diagnostics.map((item) => item.message),
    ['metrics for history+session_event', 'metrics for tool_call+file_edit+relationship'],
  );
  // The union is what makes it full, so the evidence it reports must be the
  // union too, not just the winning presence's.
  assert.equal(merged.evidence.prompts, 2);
  assert.equal(merged.evidence.events, 5);
  assert.equal(merged.evidence.toolCalls, 3);
  assert.equal(merged.evidence.fileEdits, 1);
  // Order of folding must not change the verdict.
  assert.equal(combineHydration(b, a).capability, 'full');
  assert.deepEqual(combineHydration(b, a).coverage, [...FULL_SESSION_KINDS]);
});

test('merged status reports work from either presence regardless of capability or fold order', () => {
  const unchanged = {
    ...hydrationPart('full', FULL_SESSION_KINDS), status: 'unchanged' as const,
    presence: 'local' as const,
    indexedThrough: { sourceStamp: 'local-v1', lastEventAtMs: 1 },
  };
  const updated = {
    ...hydrationPart('partial', ['history']), status: 'updated' as const,
    presence: 'remote' as const,
    indexedThrough: { sourceStamp: 'remote-v2', lastEventAtMs: 2 },
  };
  for (const merged of [combineHydration(unchanged, updated), combineHydration(updated, unchanged)]) {
    assert.equal(merged.status, 'updated');
    assert.equal(merged.presence, 'remote');
    assert.deepEqual(merged.indexedThrough, updated.indexedThrough);
    assert.equal(merged.capability, 'full');
  }

  const hydrated = { ...hydrationPart('partial', ['tool_call']), status: 'hydrated' as const };
  assert.equal(combineHydration(updated, hydrated).status, 'hydrated');
  assert.equal(combineHydration(hydrated, updated).status, 'hydrated');

  const limited = { ...hydrationPart('shallow_only', []), status: 'capability_limited' as const };
  assert.equal(combineHydration(unchanged, limited).status, 'unchanged');
});

test('a merge that is still short of full coverage stays partial, and empty coverage stays shallow_only', () => {
  const short = combineHydration(
    hydrationPart('partial', ['history']),
    hydrationPart('partial', ['tool_call']),
  );
  assert.deepEqual(short.coverage, ['history', 'tool_call']);
  assert.equal(short.capability, 'partial');
  // Exactly one reconciled note, naming what the *union* still lacks -- not
  // one per part, each naming kinds the other one supplied.
  const reconciled = short.diagnostics.filter(
    (item) => item.code === 'HYDRATION_PARTIAL_COVERAGE',
  );
  assert.equal(reconciled.length, 1);
  assert.equal(
    reconciled[0].message,
    'merged hydration covers history, tool_call; it does not cover session_event, file_edit, relationship',
  );
  assert.equal(short.diagnostics.filter((item) => item.code === 'HYDRATION_METRICS').length, 2);

  // Two listing-only connectors covered nothing; `partial` would imply some
  // evidence kind was indexed, so shallow_only survives.
  const nothing = combineHydration(
    hydrationPart('shallow_only', []),
    hydrationPart('shallow_only', []),
  );
  assert.deepEqual(nothing.coverage, []);
  assert.equal(nothing.capability, 'shallow_only');
  assert.equal(
    nothing.diagnostics.find((item) => item.code === 'HYDRATION_PARTIAL_COVERAGE')?.message,
    'merged hydration covers no evidence kinds; it does not cover '
    + 'history, session_event, tool_call, file_edit, relationship',
  );

  // One of them did index something: no longer shallow_only.
  assert.equal(
    combineHydration(hydrationPart('shallow_only', []), hydrationPart('partial', ['history']))
      .capability,
    'partial',
  );
  // A single result is returned untouched.
  assert.equal(combineHydration(undefined, hydrationPart('partial', ['history'])).capability, 'partial');
});

test('a merge does not infer provider inability from coverage the request declined', () => {
  // scope: 'all' over two presences that *can* record delegation, hydrated
  // with includeRelated: false. Each part's own note carries the reason the
  // producing side knew ("include_related is off"); the merge does not, and a
  // reason does not survive a union of presences that may have had different
  // ones -- so the reconciled note must state what is uncovered and no more.
  const thread = FULL_SESSION_KINDS.filter((kind) => kind !== 'relationship');
  const local = hydrationPart('partial', thread);
  const remote = hydrationPart('partial', thread);

  const merged = combineHydration(local, remote);
  assert.equal(merged.capability, 'partial');
  assert.deepEqual(merged.coverage, [...thread]);
  const reconciled = merged.diagnostics.filter(
    (item) => item.code === 'HYDRATION_PARTIAL_COVERAGE',
  );
  assert.equal(reconciled.length, 1);
  assert.equal(
    reconciled[0].message,
    'merged hydration covers history, session_event, tool_call, file_edit; '
    + 'it does not cover relationship',
  );
  // Both presences record delegation; only the request declined it. Any claim
  // about a producer being unable to supply the kind would be false here.
  for (const claim of [/no presence produces/, /cannot/, /does not produce/, /provider/i]) {
    assert.doesNotMatch(reconciled[0].message, claim);
  }
});

test('unparseable stored JSON yields null without discarding the raw string', () => {
  assert.deepEqual(parseStoredJson('{"a":1}'), { a: 1 });
  assert.equal(parseStoredJson('not json'), null);
  assert.equal(parseStoredJson(null), null);
  assert.equal(parseStoredJson(undefined), null);
  assert.equal(parseStoredJson('null'), null);
});

test('tool result events carry measured payload facts across the native boundary', async () => {
  const { dbPath, cleanup } = await seededDatabase();
  try {
    const events = await getSessionEvents(SHARED_SESSION, { dbPath, source: 'claude' });
    const results = events.filter((event) => event.kind === 'tool_result');
    assert.deepEqual(results.map((event) => event.toolUseId), ['toolu_1', 'toolu_2', 'toolu_3', 'toolu_3']);
    // The notification is a tool result on a different rail, with the child
    // identity that is the only reason it exists.
    const notification = results[3];
    assert.equal(notification.eventSource, 'subagent_notification');
    assert.equal(notification.subagentSessionId, 'child-1');
    assert.equal(notification.agentId, 'agent-1');
    // Byte counts are measurements of the raw payload: 'ok' is two bytes and
    // 'failed' is six, asserted as those numbers rather than as "non-zero",
    // which a fabricated default would also satisfy.
    assert.deepEqual(results.map((event) => event.payloadBytes), [2, 2, 6, 'subagent completed'.length]);
    assert.deepEqual(results.map((event) => event.payloadTruncated), [false, false, false, false]);
    assert.deepEqual(results.map((event) => event.eventIndex), [0, 1, 2, 3]);
    // `toolu_3` answers twice: once as the tool result, once as the
    // notification that the delegated agent finished.
    assert.deepEqual(results.map((event) => event.callIndex), [0, 0, 0, 1]);
    assert.deepEqual(results.map((event) => event.resultStatus), ['completed', 'completed', 'errored', 'completed']);
    assert.deepEqual(results.map((event) => event.errorSignal), [null, null, 'tool_result.is_error', null]);
    for (const event of results) assert.match(String(event.payloadHash), /^[0-9a-f]{16}$/);

    // Rows that are not tool results report nothing rather than a zero that
    // would read like a measured empty payload.
    for (const event of events.filter((candidate) => candidate.kind !== 'tool_result')) {
      assert.equal(event.payloadBytes, null);
      assert.equal(event.eventSource, null);
      assert.equal(event.resultStatus, null);
    }
  } finally {
    await cleanup();
  }
});

test('user turn pages group each message with its blocks and page by keyset', async () => {
  const { dbPath, cleanup } = await seededDatabase();
  try {
    const turns = await getSessionUserTurns('claude', SHARED_SESSION, { dbPath });
    // Four turns, not five: the subagent notification is a harness line and
    // never arrived on a user message, so it is not a turn.
    assert.equal(turns.length, 4);
    assert.deepEqual(
      turns.map((turn) => turn.blocks.map((block) => [block.kind, block.toolUseId, block.byteLen, block.isError])),
      [
        [['text', null, 'update auth'.length, null]],
        [['tool_result', 'toolu_1', 2, false]],
        [['tool_result', 'toolu_2', 2, false]],
        [['tool_result', 'toolu_3', 6, true]],
      ],
    );

    // Each turn names the messages recorded either side of it, so a consumer
    // can stitch a turn back into the conversation without re-reading the
    // event stream. The last turn's neighbour is the harness notification
    // that follows it — what was actually recorded next, not a guess at what
    // a reader would prefer.
    const events = await getSessionEvents(SHARED_SESSION, { dbPath, source: 'claude' });
    const notificationId = events[events.length - 1].messageId;
    assert.deepEqual(
      turns.map((turn) => [turn.messageId, turn.precedingMessageId, turn.followingMessageId]),
      [
        ['u1', null, 'a1'],
        ['r1', 'a1', 'a2'],
        ['r2', 'a2', 'a3'],
        ['r3', 'a3', notificationId],
      ],
    );

    const first = await getSessionUserTurnsPage('claude', SHARED_SESSION, { dbPath, limit: 1 });
    assert.equal(first.contractVersion, SESSION_EVIDENCE_CONTRACT_VERSION);
    assert.equal(first.userTurns.length, 1);
    assert.notEqual(first.nextCursor, null);
    const rest = await getSessionUserTurnsPage('claude', SHARED_SESSION, {
      dbPath,
      after: first.nextCursor ?? undefined,
    });
    assert.deepEqual(rest.userTurns.map((turn) => turn.id), turns.slice(1).map((turn) => turn.id));

    await assert.rejects(
      getSessionUserTurnsPage('claude', '', { dbPath }),
      (error: unknown) => error instanceof InvalidArgumentError,
    );
  } finally {
    await cleanup();
  }
});

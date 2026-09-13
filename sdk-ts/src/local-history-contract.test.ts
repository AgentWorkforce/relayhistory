import assert from 'node:assert/strict';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import {
  RelayHistoryError, SessionSourceUnavailableError,
  discoverSessions, getSessionEvents, getSessionEventsPage, getSessionFileEdits,
  getSessionToolCalls, hydrateSession, listSessionCatalogPage, recent, search, stats, sync,
  type CatalogCursor, type CatalogSession, type EventCursor,
} from './index.js';

// These fixtures exercise public SDK/native contracts before package moves.
// Clear every provider/transport override used by these operations so neither
// credentials nor history from the operator's environment enter the fixture.
const ENVIRONMENT = [
  'HOME', 'USERPROFILE', 'XDG_DATA_HOME', 'OPENCODE_DB', 'TRAJECTORY_ROOT', 'AI_HIST_DB',
  'RELAYHISTORY_HOME', 'RELAYHISTORY_BASE_URL', 'AI_HIST_BASE_URL',
  'RELAYHISTORY_CLAUDE_CREDENTIALS', 'RELAYHISTORY_CLAUDE_API_BASE_URL',
  'RELAYCAST_API_KEY', 'RELAYCAST_WORKSPACE_ID', 'RELAYCAST_BASE_URL',
];

async function withFixture(
  body: (fixture: { home: string; dbPath: string; cloudHome: string }) => Promise<void>,
): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), 'relayhistory-local-contract-'));
  const saved = new Map(ENVIRONMENT.map((name) => [name, process.env[name]]));
  for (const name of ENVIRONMENT) delete process.env[name];
  const home = join(root, 'home');
  const cloudHome = join(root, 'commercial');
  process.env.HOME = home;
  process.env.USERPROFILE = home;
  process.env.XDG_DATA_HOME = join(home, '.local', 'share');
  process.env.RELAYHISTORY_HOME = cloudHome;
  try {
    await mkdir(home, { recursive: true });
    await body({ home, cloudHome, dbPath: join(root, 'history.db') });
  } finally {
    for (const [name, value] of saved) {
      if (value === undefined) delete process.env[name]; else process.env[name] = value;
    }
    await rm(root, { recursive: true, force: true });
  }
}

async function claudeSession(home: string, sessionId: string): Promise<string> {
  const directory = join(home, '.claude', 'projects', '-work-contract');
  await mkdir(directory, { recursive: true });
  const path = join(directory, `${sessionId}.jsonl`);
  const common = { sessionId, cwd: '/work/contract', timestamp: '2026-09-01T10:00:00.000Z' };
  await writeFile(path, [
    { ...common, type: 'user', uuid: `${sessionId}-user`, message: { role: 'user', content: `contractneedle ${sessionId}` } },
    { ...common, type: 'assistant', uuid: `${sessionId}-assistant`, parentUuid: `${sessionId}-user`, message: {
      role: 'assistant', content: [{ type: 'tool_use', id: `${sessionId}-edit`, name: 'Edit', input: {
        file_path: '/work/contract/file.ts', old_string: 'before', new_string: 'after',
      } }],
    } },
    { ...common, type: 'user', uuid: `${sessionId}-result`, parentUuid: `${sessionId}-assistant`, message: {
      role: 'user', content: [{ type: 'tool_result', tool_use_id: `${sessionId}-edit`, content: 'done',
        toolUseResult: { filePath: '/work/contract/file.ts', structuredPatch: '@@\n-before\n+after\n' } }],
    } },
  ].map((record) => JSON.stringify(record)).join('\n') + '\n');
  return path;
}

test('KNOWN VIOLATION (stage 2): absent auth rejects only some cached remote query APIs', async () => {
  await withFixture(async ({ dbPath }) => {
    // Search/recent already permit offline remote reads. Catalog/stats reject
    // that same scope before consulting the cache. Stage 2 must make all four
    // honor the requested scope and replace these rejection assertions.
    assert.deepEqual(await search('contractneedle', { dbPath, scope: 'remote' }), []);
    assert.deepEqual(await recent({ dbPath, scope: 'remote' }), []);
    for (const operation of [
      () => listSessionCatalogPage({ dbPath, scope: 'remote' }),
      () => stats({ dbPath, scope: 'remote' }),
    ]) {
      await assert.rejects(operation, (error: unknown) => error instanceof RelayHistoryError
        && error.code === 'CLOUD_AUTH_FAILED');
    }
  });
});

test('KNOWN VIOLATION (stage 2): malformed commercial auth breaks every cached all-scope query', async () => {
  await withFixture(async ({ cloudHome, dbPath }) => {
    const stages = join(cloudHome, 'stages');
    await mkdir(stages, { recursive: true });
    await writeFile(join(stages, 'broken.auth.json'), '{not-json', { mode: 0o600 });
    for (const operation of [
      () => search('contractneedle', { dbPath, scope: 'all' }),
      () => recent({ dbPath, scope: 'all' }),
      () => listSessionCatalogPage({ dbPath, scope: 'all' }),
      () => stats({ dbPath, scope: 'all' }),
    ]) {
      await assert.rejects(operation, (error: unknown) => error instanceof RelayHistoryError
        && error.code === 'CLOUD_AUTH_FAILED'
        && error.message === 'could not parse stored relayhistory session; run `ai-hist login`');
    }
  });
});

test('local discovery, hydration and cached evidence survive malformed commercial auth and repeated ingestion', async () => {
  await withFixture(async ({ home, cloudHome, dbPath }) => {
    const stages = join(cloudHome, 'stages');
    await mkdir(stages, { recursive: true });
    await writeFile(join(stages, 'broken.auth.json'), '{not-json', { mode: 0o600 });
    await claudeSession(home, 'contract-session');
    await discoverSessions({ dbPath, scope: 'local', sources: ['claude'] });
    const hydrated = await hydrateSession({ source: 'claude', sessionId: 'contract-session', dbPath });
    assert.equal(hydrated.presence, 'local');
    assert.equal(hydrated.discoveryState, 'full');

    const readEvidence = async () => ({
      events: await getSessionEvents('contract-session', { source: 'claude', dbPath, limit: 1 }),
      calls: await getSessionToolCalls('claude', 'contract-session', { dbPath, limit: 1 }),
      edits: await getSessionFileEdits('claude', 'contract-session', { dbPath, limit: 1 }),
    });
    const original = await readEvidence();
    assert.ok(original.events.length >= 3);
    assert.equal(original.calls.length, 1);
    assert.equal(original.edits.length, 1);
    for (const event of original.events) {
      assert.equal(event.source, 'claude');
      assert.equal(event.sessionId, 'contract-session');
      assert.ok(event.eventUid.length > 0);
    }
    assert.equal(new Set(original.events.map((event) => event.eventUid)).size, original.events.length);
    assert.equal(original.calls[0].messageId, 'contract-session-assistant');
    assert.equal(original.edits[0].toolUseId, original.calls[0].toolUseId);

    // Every timestamp is equal: continuation must use the stable row ID too.
    const first = await getSessionEventsPage('contract-session', { source: 'claude', dbPath, limit: 1 });
    assert.ok(first.nextCursor);
    const tail = await getSessionEvents('contract-session', { source: 'claude', dbPath });
    const resumed = await getSessionEventsPage('contract-session', {
      source: 'claude', dbPath, after: JSON.parse(JSON.stringify(first.nextCursor)) as EventCursor,
    });
    assert.deepEqual(resumed.events, tail.slice(1));
    assert.equal(resumed.nextCursor, null);

    await sync({ dbPath, scope: 'local' });
    await hydrateSession({ source: 'claude', sessionId: 'contract-session', dbPath });
    // These record IDs, event UIDs, raw patches and arguments are the existing
    // evidence contract a future exporter must preserve across retry/restart.
    assert.deepEqual(await readEvidence(), original);
    const prompts = await search('contractneedle', { dbPath, scope: 'local' });
    assert.equal(prompts.length, 1);
    assert.deepEqual(prompts[0].locations, ['local']);
    assert.deepEqual(await recent({ dbPath, scope: 'local' }), prompts);
    assert.equal((await stats({ dbPath, scope: 'local' })).total, 1);
  });
});

test('catalog cursors retain tied session identities and hydration failure leaves cached evidence readable', async () => {
  await withFixture(async ({ home, dbPath }) => {
    const paths = await Promise.all(['session-c', 'session-a', 'session-b'].map((id) => claudeSession(home, id)));
    await sync({ dbPath, scope: 'local' });
    const all = await listSessionCatalogPage({ dbPath, scope: 'local', limit: 100 });
    assert.equal(all.sessions.length, 3);
    const paged: CatalogSession[] = [];
    let after: CatalogCursor | undefined;
    do {
      const page = await listSessionCatalogPage({ dbPath, scope: 'local', limit: 1, after });
      paged.push(...page.sessions);
      assert.ok(paged.length <= all.sessions.length, 'cursor must not repeat a page');
      after = page.nextCursor ? JSON.parse(JSON.stringify(page.nextCursor)) as CatalogCursor : undefined;
    } while (after);
    assert.deepEqual(paged, all.sessions);
    assert.equal(new Set(paged.map((row) => `${row.source}:${row.sessionId}`)).size, 3);

    const before = await getSessionEvents('session-c', { source: 'claude', dbPath });
    assert.ok(before.length > 0);
    await rm(paths[0]);
    await assert.rejects(
      () => hydrateSession({ source: 'claude', sessionId: 'session-c', dbPath }),
      (error: unknown) => error instanceof SessionSourceUnavailableError
        && error.code === 'SESSION_SOURCE_UNAVAILABLE'
        && error.message.includes('disappeared after discovery'),
    );
    assert.deepEqual(await getSessionEvents('session-c', { source: 'claude', dbPath }), before);
    assert.deepEqual((await listSessionCatalogPage({ dbPath, scope: 'local', limit: 100 })).sessions, all.sessions);
  });
});

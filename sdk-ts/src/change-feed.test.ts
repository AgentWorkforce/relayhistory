import assert from 'node:assert/strict';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';

import {
  CHANGE_KINDS, InvalidArgumentError, RelayHistoryError,
  changesSince, commitChanges, getChangesPage, sync,
  type FeedChange,
} from './index.js';
import { feedChange } from './normalization.js';

const SESSION = 'feed-1';

const CLAUDE_TRANSCRIPT = [
  { type: 'user', uuid: 'u1', sessionId: SESSION, cwd: '/work/app', timestamp: '2026-08-30T10:00:00.000Z', message: { role: 'user', content: 'ship the feed' } },
  { type: 'assistant', uuid: 'a1', parentUuid: 'u1', sessionId: SESSION, cwd: '/work/app', timestamp: '2026-08-30T10:00:02.000Z', message: { role: 'assistant', model: 'claude-test', content: [{ type: 'text', text: 'Shipping.' }] } },
  { type: 'user', uuid: 'u2', parentUuid: 'a1', sessionId: SESSION, cwd: '/work/app', timestamp: '2026-08-30T10:00:03.000Z', message: { role: 'user', content: 'thanks' } },
];

async function seededDatabase(): Promise<{ dbPath: string; cleanup: () => Promise<void> }> {
  const root = await mkdtemp(join(tmpdir(), 'relayhistory-feed-'));
  const home = join(root, 'home');
  const claude = join(home, '.claude', 'projects', 'work-app');
  await mkdir(claude, { recursive: true });
  await writeFile(join(claude, `${SESSION}.jsonl`), `${CLAUDE_TRANSCRIPT.map((line) => JSON.stringify(line)).join('\n')}\n`);
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

async function drain(iterable: AsyncIterable<FeedChange>): Promise<FeedChange[]> {
  const changes: FeedChange[] = [];
  for await (const change of iterable) changes.push(change);
  return changes;
}

test('the change feed pages every stored row with its identity, revision and columns', async () => {
  const { dbPath, cleanup } = await seededDatabase();
  try {
    const all = await drain(changesSince({ dbPath, limit: 2 }));
    assert.ok(all.length > 0);
    // Strictly increasing revisions: pages resume with no gap and no repeat.
    for (let index = 1; index < all.length; index += 1) {
      assert.ok(all[index].revision > all[index - 1].revision, 'revisions are unique and ordered');
    }
    assert.ok(all.every((change) => CHANGE_KINDS.includes(change.kind)));

    const session = all.find((change) => change.kind === 'session' && change.sessionId === SESSION);
    assert.ok(session, `the catalog row is reported: ${all.map((change) => change.kind).join(', ')}`);
    assert.equal(session.op, 'upsert');
    assert.equal(session.source, 'claude');
    assert.equal(session.sourceName, 'claude');
    assert.deepEqual(session.key, ['session', 'claude', SESSION]);
    assert.equal(session.columns?.session_id, SESSION);

    const events = all.filter((change) => change.kind === 'session_event' && change.sessionId === SESSION);
    assert.ok(events.length >= 3, 'every transcript row is a session_event change');

    // A kind filter and a session filter narrow the same drain.
    const onlyEvents = await drain(changesSince({ dbPath, kinds: ['session_event'], session: { source: 'claude', sessionId: SESSION } }));
    assert.deepEqual(onlyEvents.map((change) => change.revision), events.map((change) => change.revision));

    const page = await getChangesPage({ dbPath, limit: 1 });
    assert.equal(page.changes.length, 1);
    assert.match(page.head.epoch, /^[0-9a-f]{16}$/);
    assert.equal(page.position.revision, page.changes[0].revision);
    assert.equal(page.done, false);
  } finally {
    await cleanup();
  }
});

test('a named consumer resumes from its committed cursor only', async () => {
  const { dbPath, cleanup } = await seededDatabase();
  try {
    const kinds = ['session_event'] as const;
    const first = await getChangesPage({ dbPath, consumer: 'burn', kinds, limit: 1 });
    assert.equal(first.consumer, 'burn');
    // Unacknowledged, the page is served again.
    const again = await getChangesPage({ dbPath, consumer: 'burn', kinds, limit: 1 });
    assert.deepEqual(again.changes, first.changes);

    const committed = await commitChanges('burn', first.position, { dbPath, kinds });
    assert.deepEqual(committed, { consumer: 'burn', cursor: first.position });

    const rest = await drain(changesSince({ dbPath, consumer: 'burn', kinds, commit: true }));
    assert.ok(rest.length > 0);
    assert.ok(rest.every((change) => change.revision > first.position.revision));
    // The iterator committed as it went: nothing is left for the cursor.
    assert.deepEqual(await drain(changesSince({ dbPath, consumer: 'burn', kinds })), []);

    await assert.rejects(
      getChangesPage({ dbPath, consumer: 'burn', kinds: ['session'] }),
      (error: unknown) => error instanceof RelayHistoryError && error.code === 'CONSUMER_KINDS_MISMATCH',
    );
  } finally {
    await cleanup();
  }
});

test('the feed refuses what it cannot serve and never creates a database', async () => {
  const root = await mkdtemp(join(tmpdir(), 'relayhistory-feed-absent-'));
  try {
    const dbPath = join(root, 'absent.db');
    const page = await getChangesPage({ dbPath });
    assert.deepEqual(page.changes, []);
    assert.equal(page.done, true);
    assert.equal(existsSync(dbPath), false);

    await assert.rejects(getChangesPage({ dbPath, from: 'consumer' }), InvalidArgumentError);
    await assert.rejects(
      getChangesPage({ dbPath, kinds: ['nope' as never] }),
      InvalidArgumentError,
    );
    await assert.rejects(drain(changesSince({ dbPath, commit: true })), InvalidArgumentError);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('a feed change keeps every source the native feed names, trajectory included', () => {
  const row = {
    kind: 'trajectory', source: 'trajectory', sourceName: 'trajectory', sessionId: 't-1',
    recordKey: 't-1', key: ['trajectory', 't-1'], revision: 7, op: 'upsert', columns: { id: 't-1' },
  };
  assert.equal(feedChange(row).source, 'trajectory');
  // A source a newer release wrote is carried, not failed on.
  const future = feedChange({ ...row, source: null, sourceName: 'future-harness' });
  assert.equal(future.source, null);
  assert.equal(future.sourceName, 'future-harness');
  // A revision a JavaScript number cannot hold exactly is a broken contract.
  assert.throws(() => feedChange({ ...row, revision: 2 ** 53 + 2 }), RelayHistoryError);
});

import assert from 'node:assert/strict';
import { test } from 'node:test';

import initSqlJs, { type Database } from 'sql.js';

import {
  promptHash,
  batchId,
  normalizeHomePath,
  buildOutboxBatch,
  type SyncCursor,
} from './cloud-push.js';

// ----- hashing (must match ai_hist_core::prompt_hash) -----

test('promptHash is sha256 hex truncated to 16 chars', () => {
  // sha256("hello") = 2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824
  assert.equal(promptHash('hello'), '2cf24dba5fb0a30e');
  assert.equal(promptHash('hello').length, 16);
});

test('batchId hashes the exact cursor-span material with a b_ prefix', () => {
  const from: SyncCursor = { history_id: 0, trajectory_rowid: 0 };
  const to: SyncCursor = { history_id: 5, trajectory_rowid: 2 };
  const expected = `b_${promptHash('m_abc:0:0:5:2:3')}`;
  assert.equal(batchId('m_abc', from, to, 3), expected);
});

// ----- home-path scrub (must match convergence::normalize_home_path) -----

test('normalizeHomePath replaces the username segment, preserving prefix case', () => {
  assert.equal(normalizeHomePath('/Users/alice/foo'), '/Users/~/foo');
  assert.equal(normalizeHomePath('/home/bob/x'), '/home/~/x');
  assert.equal(normalizeHomePath('C:\\Users\\carol\\y'), 'C:\\Users\\~\\y');
  // Prefix case is preserved; only the username becomes ~.
  assert.equal(normalizeHomePath('/USERS/Dave/z'), '/USERS/~/z');
  // Mid-string and multiple occurrences.
  assert.equal(normalizeHomePath('see /Users/eve/a and /home/eve/b'), 'see /Users/~/a and /home/~/b');
  // No trailing segment → unchanged.
  assert.equal(normalizeHomePath('/Users/'), '/Users/');
  assert.equal(normalizeHomePath('nothing here'), 'nothing here');
});

// ----- outbox builder against a real in-memory sql.js DB -----

async function seedDb(): Promise<Database> {
  const SQL = await initSqlJs();
  const db = new SQL.Database();
  db.run(`CREATE TABLE history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    session_id TEXT,
    project TEXT,
    prompt TEXT NOT NULL,
    prompt_hash TEXT,
    timestamp_ms INTEGER NOT NULL
  )`);
  db.run(`CREATE TABLE trajectories (
    id TEXT PRIMARY KEY,
    persona_id TEXT,
    project_id TEXT,
    task_title TEXT,
    task_description TEXT,
    status TEXT,
    decisions_json TEXT NOT NULL,
    retrospective_json TEXT NOT NULL,
    timestamp_ms INTEGER NOT NULL
  )`);
  return db;
}

test('buildOutboxBatch maps history rows and advances the cursor', async () => {
  const db = await seedDb();
  db.run(
    "INSERT INTO history (source, session_id, prompt, prompt_hash, timestamp_ms) VALUES ('claude','s1','hi there','deadbeef0000cafe',1700000000000)",
  );
  db.run(
    "INSERT INTO history (source, session_id, prompt, timestamp_ms) VALUES ('codex','s2','path /Users/me/x',1700000001000)",
  );

  const batch = buildOutboxBatch(db, { history_id: 0, trajectory_rowid: 0 }, 500, new Set());
  db.close();

  assert.equal(batch.records.length, 2);
  assert.equal(batch.cursor.history_id, 2);

  const first = batch.records[0];
  assert.equal(first.kind, 'prompt');
  assert.equal(first.source, 'claude');
  assert.equal(first.lens, 'history');
  assert.equal(first.sessionId, 's1');
  assert.equal(first.eventId, 'prompt:1700000000000:deadbeef0000cafe');
  assert.equal(first.ts, new Date(1700000000000).toISOString());
  assert.equal(first.confidence, null);

  // prompt_hash absent → derived from the prompt text.
  const second = batch.records[1];
  assert.equal(second.eventId, `prompt:1700000001000:${promptHash('path /Users/me/x')}`);
  // content is home-path scrubbed.
  assert.equal(second.content, 'path /Users/~/x');

  // confidence must serialize as null (present), optional fields omitted.
  const json = JSON.stringify(first);
  assert.match(json, /"confidence":null/);
  assert.doesNotMatch(json, /significance/);
  assert.doesNotMatch(json, /actorName/);
});

test('buildOutboxBatch advances the cursor past incognito rows without emitting them', async () => {
  const db = await seedDb();
  db.run("INSERT INTO history (source, session_id, prompt, timestamp_ms) VALUES ('claude','secret','x',1700000000000)");
  db.run("INSERT INTO history (source, session_id, prompt, timestamp_ms) VALUES ('claude','ok','y',1700000001000)");

  const batch = buildOutboxBatch(db, { history_id: 0, trajectory_rowid: 0 }, 500, new Set(['secret']));
  db.close();

  assert.equal(batch.records.length, 1);
  assert.equal(batch.records[0].sessionId, 'ok');
  // Cursor still advances past the skipped row so it is never re-scanned.
  assert.equal(batch.cursor.history_id, 2);
});

test('buildOutboxBatch fans a trajectory into decision + retrospective events in order', async () => {
  const db = await seedDb();
  const decisions = JSON.stringify([
    { question: 'Use X or Y?', chosen: 'X', reasoning: 'faster', confidence: 0.8 },
  ]);
  const retro = JSON.stringify({
    confidence: 0.5,
    summary: 'went well',
    learnings: ['learned a', 'learned b'],
    challenges: ['hard c'],
  });
  db.run(
    'INSERT INTO trajectories (id, persona_id, project_id, task_title, status, decisions_json, retrospective_json, timestamp_ms) VALUES ($id,$p,$pr,$t,$s,$d,$r,$ts)',
    {
      $id: 'traj-1',
      $p: 'planner',
      $pr: '/repo',
      $t: 'Do the thing',
      $s: 'completed',
      $d: decisions,
      $r: retro,
      $ts: 1700000000000,
    },
  );

  const batch = buildOutboxBatch(db, { history_id: 0, trajectory_rowid: 0 }, 500, new Set());
  db.close();

  const ids = batch.records.map((r) => r.eventId);
  assert.deepEqual(ids, [
    'decision:traj-1:0',
    'reflection:traj-1:summary',
    'finding:traj-1:learning:0',
    'finding:traj-1:learning:1',
    'finding:traj-1:challenge:0',
  ]);

  const decision = batch.records[0];
  assert.equal(decision.kind, 'decision');
  assert.equal(decision.source, 'trajectories');
  assert.equal(decision.sessionId, 'traj-1');
  assert.equal(decision.actorName, 'planner');
  assert.equal(decision.projectId, '/repo');
  assert.equal(decision.taskTitle, 'Do the thing');
  assert.equal(decision.confidence, 0.8);
  assert.match(decision.content, /Question: Use X or Y\?/);
  assert.match(decision.content, /Chose: X/);
  assert.deepEqual(decision.record, { chosen: 'X' });

  // Retrospective summary inherits the retro-level confidence.
  assert.equal(batch.records[1].confidence, 0.5);
  // Array findings carry null confidence.
  assert.equal(batch.records[2].confidence, null);

  assert.equal(batch.cursor.trajectory_rowid, 1);
});

test('buildOutboxBatch handles compacted rollups with tag-derived source/lens', async () => {
  const db = await seedDb();
  const retro = JSON.stringify({
    type: 'compacted',
    sourceTrajectories: ['a', 'b'],
    source: 'burn',
    tags: ['learn'],
    decisions: [{ chosen: 'go', impact: 'big' }],
    keyLearnings: ['k1'],
    narrative: 'should be dropped because is_learn',
  });
  db.run(
    'INSERT INTO trajectories (id, decisions_json, retrospective_json, timestamp_ms) VALUES ($id,$d,$r,$ts)',
    { $id: 'roll-1', $d: '[]', $r: retro, $ts: 1700000000000 },
  );

  const batch = buildOutboxBatch(db, { history_id: 0, trajectory_rowid: 0 }, 500, new Set());
  db.close();

  const ids = batch.records.map((r) => r.eventId);
  // narrative omitted (is_learn), decisions + keyLearnings present.
  assert.deepEqual(ids, ['decision:roll-1:0', 'finding:roll-1:learning:0']);
  assert.equal(batch.records[0].source, 'burn');
  assert.equal(batch.records[0].lens, 'burn');
  assert.deepEqual(batch.records[0].tags, ['learn']);
  assert.deepEqual(batch.records[0].record, { chosen: 'go', impact: 'big' });
});

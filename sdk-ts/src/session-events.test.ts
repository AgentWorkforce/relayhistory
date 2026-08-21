import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import initSqlJs from 'sql.js';
import { openAiHist } from './index.js';

async function writeEventsFixtureDb(dbPath: string): Promise<void> {
  const SQL = await initSqlJs();
  const db = new SQL.Database();
  try {
    db.run(`CREATE TABLE history (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      source TEXT NOT NULL,
      session_id TEXT,
      project TEXT,
      prompt TEXT NOT NULL,
      timestamp_ms INTEGER NOT NULL,
      git_branch TEXT
    )`);
    db.run(`CREATE TABLE session_events (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      source TEXT NOT NULL,
      session_id TEXT NOT NULL,
      project TEXT,
      cwd TEXT,
      git_branch TEXT,
      message_id TEXT,
      parent_id TEXT,
      ts_ms INTEGER NOT NULL,
      role TEXT NOT NULL,
      kind TEXT NOT NULL,
      text TEXT,
      model TEXT,
      token_json TEXT,
      event_uid TEXT NOT NULL
    )`);
    db.run(`CREATE TABLE tool_calls (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      source TEXT NOT NULL,
      session_id TEXT NOT NULL,
      message_id TEXT,
      tool_use_id TEXT NOT NULL,
      name TEXT NOT NULL,
      target TEXT,
      args_json TEXT,
      is_error INTEGER,
      ts_ms INTEGER
    )`);
    const insertEvent = `INSERT INTO session_events
      (source, session_id, project, cwd, git_branch, message_id, parent_id, ts_ms, role, kind, text, model, token_json, event_uid)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
    db.run(insertEvent, [
      'codex', 'sess-1', '/tmp/proj', '/tmp/proj', 'main', '4:user_message', null,
      1000, 'user', 'text', 'fix the importer', null, null, '4:user_message',
    ]);
    db.run(insertEvent, [
      'codex', 'sess-1', '/tmp/proj', '/tmp/proj', 'main', 'fc_1', null,
      2000, 'assistant', 'tool_use', 'exec_command git status', 'gpt-5.4',
      '{"input_tokens":1000,"cached_input_tokens":400,"output_tokens":120,"total_tokens":1120}',
      '8:function_call',
    ]);
    db.run(insertEvent, [
      'claude', 'sess-other', '/tmp/other', '/tmp/other', null, 'msg_1', null,
      1500, 'assistant', 'text', 'unrelated session', 'claude-opus-5', 'not json', 'msg_1:0',
    ]);
    db.run(
      `INSERT INTO tool_calls (source, session_id, message_id, tool_use_id, name, target, args_json, is_error, ts_ms)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      ['codex', 'sess-1', 'fc_1', 'call_1', 'exec_command', 'git status', '{"cmd":"git status"}', 0, 2000],
    );
    db.run(
      `INSERT INTO tool_calls (source, session_id, message_id, tool_use_id, name, target, args_json, is_error, ts_ms)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      ['claude', 'sess-other', 'msg_1', 'toolu_1', 'Bash', 'ls', '{"command":"ls"}', null, 1500],
    );
    await writeFile(dbPath, Buffer.from(db.export()));
  } finally {
    db.close();
  }
}

async function writePreEventsDb(dbPath: string): Promise<void> {
  const SQL = await initSqlJs();
  const db = new SQL.Database();
  try {
    db.run(`CREATE TABLE history (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      source TEXT NOT NULL,
      session_id TEXT,
      project TEXT,
      prompt TEXT NOT NULL,
      timestamp_ms INTEGER NOT NULL,
      git_branch TEXT
    )`);
    db.run(
      `INSERT INTO history (source, session_id, project, prompt, timestamp_ms) VALUES (?, ?, ?, ?, ?)`,
      ['codex', 'sess-1', '/tmp/proj', 'hello', 1000],
    );
    await writeFile(dbPath, Buffer.from(db.export()));
  } finally {
    db.close();
  }
}

test('getSessionEvents returns ordered typed events with parsed token usage', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'ai-hist-events-'));
  const dbPath = join(dir, 'events.db');
  try {
    await writeEventsFixtureDb(dbPath);
    const hist = await openAiHist({ dbPath });
    try {
      const events = hist.getSessionEvents('sess-1');
      assert.equal(events.length, 2);
      assert.deepEqual(
        events.map((e) => e.eventUid),
        ['4:user_message', '8:function_call'],
      );
      assert.equal(events[0]!.role, 'user');
      assert.equal(events[0]!.tokenUsage, null);
      assert.equal(events[1]!.kind, 'tool_use');
      assert.equal(events[1]!.model, 'gpt-5.4');
      assert.deepEqual(events[1]!.tokenUsage, {
        input_tokens: 1000,
        cached_input_tokens: 400,
        output_tokens: 120,
        total_tokens: 1120,
      });

      // Source filter and unparsable token_json.
      const other = hist.getSessionEvents('sess-other', { source: 'claude' });
      assert.equal(other.length, 1);
      assert.equal(other[0]!.tokenUsage, null);
      assert.equal(hist.getSessionEvents('sess-other', { source: 'codex' }).length, 0);

      const calls = hist.getToolCalls('sess-1');
      assert.equal(calls.length, 1);
      assert.equal(calls[0]!.name, 'exec_command');
      assert.equal(calls[0]!.target, 'git status');
      assert.equal(calls[0]!.isError, false);
    } finally {
      hist.close();
    }
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test('a configured project scope constrains event and tool-call reads', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'ai-hist-events-scope-'));
  const dbPath = join(dir, 'events.db');
  try {
    await writeEventsFixtureDb(dbPath);
    const scoped = await openAiHist({ dbPath, projectScope: '/tmp/other' });
    try {
      // sess-1 lives in /tmp/proj — outside the scope.
      assert.deepEqual(scoped.getSessionEvents('sess-1'), []);
      assert.deepEqual(scoped.getToolCalls('sess-1'), []);
      // sess-other lives in /tmp/other — inside it.
      assert.equal(scoped.getSessionEvents('sess-other').length, 1);
      assert.equal(scoped.getToolCalls('sess-other').length, 1);
    } finally {
      scoped.close();
    }
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test('events reads return [] on databases that predate session_events', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'ai-hist-preevents-'));
  const dbPath = join(dir, 'old.db');
  try {
    await writePreEventsDb(dbPath);
    const hist = await openAiHist({ dbPath });
    try {
      assert.deepEqual(hist.getSessionEvents('sess-1'), []);
      assert.deepEqual(hist.getToolCalls('sess-1'), []);
      assert.equal(hist.getSession('sess-1').length, 1);
    } finally {
      hist.close();
    }
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtemp, mkdir, realpath, stat, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import initSqlJs from 'sql.js';
import { openAiHist } from './index.js';

test('SDK projectScope restricts all history and trajectory reads', async () => {
  const root = await mkdtemp(join(tmpdir(), 'ai-hist-scope-'));
  const dbPath = join(root, 'history.db');
  await writeScopeFixtureDb(dbPath);

  const hist = await openAiHist({ dbPath, projectScope: '/work/app' });
  try {
    assert.equal(hist.projectScope, '/work/app');
    assert.deepEqual(
      hist.recent({ limit: 10 }).map((entry) => entry.prompt),
      ['scoped child prompt', 'scoped root prompt'],
    );
    assert.deepEqual(
      hist.recent({ limit: 10, project: '/work/app/pkg' }).map((entry) => entry.prompt),
      ['scoped child prompt'],
    );
    assert.deepEqual(hist.recent({ limit: 10, project: '/work/other' }), []);
    assert.deepEqual(
      hist.search('prompt', { limit: 10 }).map((entry) => entry.prompt),
      ['scoped child prompt', 'scoped root prompt'],
    );
    assert.deepEqual(
      hist.getSession('shared').map((entry) => entry.prompt),
      ['scoped root prompt', 'scoped child prompt'],
    );
    assert.equal(hist.getEntry(3), null);
    assert.deepEqual(
      hist.getInTimeWindow(2_000, 2_000).map((entry) => entry.prompt),
      ['scoped root prompt', 'scoped child prompt'],
    );
    assert.equal(hist.stats().total, 2);
    assert.deepEqual(
      hist.searchTrajectories('decision', { limit: 10 }).map((entry) => entry.id),
      ['scoped-run'],
    );
  } finally {
    hist.close();
  }

  const rootScopedHist = await openAiHist({ dbPath, projectScope: '/' });
  try {
    assert.deepEqual(
      rootScopedHist.recent({ limit: 10 }).map((entry) => entry.prompt),
      ['outside prompt', 'scoped child prompt', 'scoped root prompt'],
    );
    assert.deepEqual(
      rootScopedHist.searchTrajectories('decision', { limit: 10 }).map((entry) => entry.id),
      ['outside-run', 'scoped-run'],
    );
  } finally {
    rootScopedHist.close();
  }
});

test('SDK tags sessions, filters reads by tag, and persists SQLite writes', async () => {
  const root = await mkdtemp(join(tmpdir(), 'ai-hist-tags-'));
  const dbPath = join(root, 'history.db');
  await writeScopeFixtureDb(dbPath);

  const hist = await openAiHist({ dbPath });
  try {
    const tagged = hist.tagSession('shared', 'Release Work', { source: 'claude', color: 'blue' });
    assert.equal(tagged.length, 1);
    assert.deepEqual(
      hist.search('prompt', { tag: 'release work', limit: 10 }).map((entry) => entry.prompt),
      ['scoped root prompt'],
    );
    assert.equal(hist.listTags({ includeSessions: true })[0].sessions?.[0].sessionId, 'shared');
  } finally {
    hist.close();
  }

  const reopened = await openAiHist({ dbPath });
  try {
    assert.deepEqual(
      reopened.recent({ tag: 'release work', limit: 10 }).map((entry) => entry.prompt),
      ['scoped root prompt'],
    );
    assert.equal(reopened.untagSession('shared', 'release work', { source: 'claude' }), 1);
    assert.deepEqual(reopened.recent({ tag: 'release work', limit: 10 }), []);
  } finally {
    reopened.close();
  }
});

test('SDK projectScope restricts tag counts and included tagged sessions', async () => {
  const root = await mkdtemp(join(tmpdir(), 'ai-hist-tag-scope-'));
  const dbPath = join(root, 'history.db');
  await writeScopeFixtureDb(dbPath);

  const unscoped = await openAiHist({ dbPath });
  try {
    assert.equal(unscoped.tagSession('shared', 'Scoped Tag', { source: 'claude' }).length, 1);
    assert.equal(unscoped.tagSession('shared', 'Scoped Tag', { source: 'cursor' }).length, 1);
  } finally {
    unscoped.close();
  }

  const scoped = await openAiHist({ dbPath, projectScope: '/work/app' });
  try {
    const tags = scoped.listTags({ tag: 'scoped tag', includeSessions: true });
    assert.equal(tags.length, 1);
    assert.equal(tags[0].sessionCount, 1);
    assert.deepEqual(tags[0].sessions?.map((session) => session.source), ['claude']);
    assert.deepEqual(
      scoped.recent({ tag: 'scoped tag', limit: 10 }).map((entry) => entry.prompt),
      ['scoped root prompt'],
    );
  } finally {
    scoped.close();
  }
});

test('SDK fallback ingests compacted per-run trajectories from TRAJECTORY_ROOT', async () => {
  const root = await mkdtemp(join(tmpdir(), 'ai-hist-trajectory-'));
  const compacted = join(root, 'planner', 'compacted');
  await mkdir(compacted, { recursive: true });
  await writeFile(
    join(compacted, 'run-1.json'),
    JSON.stringify({
      id: 'run-1',
      version: 1,
      personaId: 'planner',
      projectId: 'agent-workforce',
      task: { title: 'Latency budget', description: 'Choose retry behavior for API calls.' },
      status: 'completed',
      startedAt: '2026-06-06T10:00:00.000Z',
      completedAt: '2026-06-06T10:05:00.000Z',
      decisions: [
        {
          question: 'How should retries behave?',
          chosen: 'Use capped exponential backoff',
          reasoning: 'It protects downstream services while preserving UX.',
          alternatives: ['fixed delay', 'no retry'],
        },
      ],
      retrospective: {
        summary: 'Retry policy selected.',
        approach: 'Compared failure modes and downstream pressure.',
        learnings: ['Bound retries by elapsed time.'],
        confidence: 0.82,
      },
    }),
  );

  const previousRoot = process.env.TRAJECTORY_ROOT;
  const previousDb = process.env.AI_HIST_DB;
  process.env.TRAJECTORY_ROOT = root;
  process.env.AI_HIST_DB = join(root, 'missing.db');
  const hist = await openAiHist({ dbPath: join(root, 'missing.db') });
  try {
    const results = hist.searchTrajectories('exponential backoff', { limit: 5 });
    assert.equal(results.length, 1);
    assert.equal(results[0].id, 'run-1');
    assert.equal(results[0].decisions[0].chosen, 'Use capped exponential backoff');
    assert.equal(hist.whyForTask('retry policy')?.retrospective.summary, 'Retry policy selected.');
    assert.ok(hist.search('Latency budget', { source: 'trajectory', limit: 5 }).length >= 1);
  } finally {
    hist.close();
    if (previousRoot === undefined) delete process.env.TRAJECTORY_ROOT;
    else process.env.TRAJECTORY_ROOT = previousRoot;
    if (previousDb === undefined) delete process.env.AI_HIST_DB;
    else process.env.AI_HIST_DB = previousDb;
  }
});

test('SDK fallback ingests OpenCode rows committed in WAL files', async () => {
  const root = await mkdtemp(join(tmpdir(), 'ai-hist-opencode-wal-'));
  const dbPath = join(root, 'opencode.db');
  const readyPath = join(root, 'ready');
  const child = spawn(
    'python3',
    [
      '-c',
      `
import json, pathlib, sqlite3, time
db = pathlib.Path(${JSON.stringify(dbPath)})
ready = pathlib.Path(${JSON.stringify(readyPath)})
conn = sqlite3.connect(db)
conn.execute("PRAGMA journal_mode=WAL")
conn.execute("PRAGMA wal_autocheckpoint=0")
conn.execute("CREATE TABLE session (id TEXT PRIMARY KEY, directory TEXT, time_created INTEGER)")
conn.execute("CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT, time_created INTEGER, data TEXT)")
conn.execute("CREATE TABLE part (id TEXT PRIMARY KEY, message_id TEXT, session_id TEXT, time_created INTEGER, data TEXT)")
conn.execute("INSERT INTO session VALUES ('oc-ts-wal', '/tmp/opencode-ts', 1700000000000)")
conn.execute("INSERT INTO message VALUES ('msg-ts-wal', 'oc-ts-wal', 1700000001000, ?)", (json.dumps({"role":"user"}),))
conn.execute("INSERT INTO part VALUES ('part-ts-wal', 'msg-ts-wal', 'oc-ts-wal', 1700000002000, ?)", (json.dumps({"type":"text","text":"ts wal opencode prompt"}),))
conn.commit()
live = sqlite3.connect(db)
assert live.execute("SELECT COUNT(*) FROM part").fetchone()[0] == 1
live.close()
ready.write_text("ready")
time.sleep(60)
`,
    ],
    { stdio: ['ignore', 'ignore', 'pipe'] },
  );
  const stderr: Buffer[] = [];
  child.stderr.on('data', (chunk: Buffer) => stderr.push(chunk));

  const previousDb = process.env.AI_HIST_DB;
  const previousOpenCode = process.env.OPENCODE_DB;
  const previousTrajectory = process.env.TRAJECTORY_ROOT;
  process.env.AI_HIST_DB = join(root, 'missing-ai-hist.db');
  process.env.OPENCODE_DB = dbPath;
  process.env.TRAJECTORY_ROOT = join(root, 'missing-trajectories');
  try {
    await waitForFile(readyPath, () => Buffer.concat(stderr).toString('utf8'));
    const hist = await openAiHist({ dbPath: process.env.AI_HIST_DB });
    try {
      assert.deepEqual(
        hist.search('ts wal opencode', { source: 'opencode', limit: 5 }).map((entry) => entry.prompt),
        ['ts wal opencode prompt'],
      );
    } finally {
      hist.close();
    }
  } finally {
    child.kill();
    if (previousDb === undefined) delete process.env.AI_HIST_DB;
    else process.env.AI_HIST_DB = previousDb;
    if (previousOpenCode === undefined) delete process.env.OPENCODE_DB;
    else process.env.OPENCODE_DB = previousOpenCode;
    if (previousTrajectory === undefined) delete process.env.TRAJECTORY_ROOT;
    else process.env.TRAJECTORY_ROOT = previousTrajectory;
  }
});

test('MCP server exposes history and trajectory tools over stdio', async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'ai-hist-mcp-')));
  const dbPath = join(root, 'history.db');
  await writeScopeFixtureDb(dbPath);
  const child = spawn(process.execPath, [new URL('./mcp-server.js', import.meta.url).pathname, '--project', '--project-path=.'], {
    cwd: root,
    env: {
      ...process.env,
      AI_HIST_DB: dbPath,
      TRAJECTORY_ROOT: root,
    },
    stdio: ['pipe', 'pipe', 'pipe'],
  });

  const stderr: Buffer[] = [];
  child.stderr.on('data', (chunk: Buffer) => stderr.push(chunk));

  try {
    const { tools, stats } = await new Promise<{ tools: Set<string>; stats: { projectScope?: string } }>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('timed out waiting for MCP responses')), 5000);
      let buffer = '';
      let tools: Set<string> | null = null;
      let stats: { projectScope?: string } | null = null;

      child.stdout.on('data', (chunk: Buffer) => {
        buffer += chunk.toString('utf8');
        while (true) {
          const marker = buffer.indexOf('\n');
          if (marker === -1) return;
          const body = buffer.slice(0, marker).trim();
          buffer = buffer.slice(marker + 1);
          if (!body) continue;
          const message = JSON.parse(body) as {
            id?: number;
            result?: { tools?: Array<{ name: string }>; content?: Array<{ type: string; text?: string }> };
            error?: unknown;
          };
          if (message.error) {
            clearTimeout(timer);
            reject(new Error(`MCP error: ${JSON.stringify(message.error)}`));
            return;
          }
          if (message.id === 2) {
            tools = new Set((message.result?.tools ?? []).map((tool) => tool.name));
          }
          if (message.id === 3) {
            const text = message.result?.content?.find((item) => item.type === 'text')?.text ?? '{}';
            stats = JSON.parse(text) as { projectScope?: string };
          }
          if (tools && stats) {
            clearTimeout(timer);
            resolve({ tools, stats });
          }
        }
      });

      child.on('error', (err) => {
        clearTimeout(timer);
        reject(err);
      });
      child.on('exit', (code) => {
        if (code !== null && code !== 0) {
          clearTimeout(timer);
          reject(new Error(`MCP server exited ${code}: ${Buffer.concat(stderr).toString('utf8')}`));
        }
      });

      writeJsonRpc(child.stdin, {
        jsonrpc: '2.0',
        id: 1,
        method: 'initialize',
        params: {
          protocolVersion: '2025-06-18',
          capabilities: {},
          clientInfo: { name: 'ai-hist-smoke', version: '0.0.0' },
        },
      });
      writeJsonRpc(child.stdin, {
        jsonrpc: '2.0',
        method: 'notifications/initialized',
        params: {},
      });
      writeJsonRpc(child.stdin, {
        jsonrpc: '2.0',
        id: 2,
        method: 'tools/list',
        params: {},
      });
      writeJsonRpc(child.stdin, {
        jsonrpc: '2.0',
        id: 3,
        method: 'tools/call',
        params: { name: 'stats', arguments: {} },
      });
    });

    for (const name of [
      'search_history',
      'recent_entries',
      'get_session',
      'get_context',
      'stats',
      'tag_session',
      'untag_session',
      'list_tags',
      'search_trajectories',
      'why_for_task',
      'get_handoff',
    ]) {
      assert.ok(tools.has(name), `missing MCP tool ${name}`);
    }
    assert.equal(stats.projectScope, root);
  } finally {
    child.kill();
  }
});

function writeJsonRpc(stdin: NodeJS.WritableStream, payload: unknown): void {
  stdin.write(`${JSON.stringify(payload)}\n`);
}

async function waitForFile(path: string, getError: () => string): Promise<void> {
  const deadline = Date.now() + 5_000;
  while (Date.now() < deadline) {
    try {
      await stat(path);
      return;
    } catch {
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
  }
  throw new Error(`timed out waiting for ${path}: ${getError()}`);
}

async function writeScopeFixtureDb(dbPath: string): Promise<void> {
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
    db.run(`CREATE TABLE sessions (
      session_id TEXT NOT NULL,
      source TEXT NOT NULL,
      cwd TEXT,
      git_branch TEXT,
      first_activity_ms INTEGER,
      last_activity_ms INTEGER,
      last_assistant_text TEXT,
      raw_path TEXT,
      parser_version INTEGER NOT NULL DEFAULT 1,
      PRIMARY KEY (session_id, source)
    )`);
    db.run(`CREATE TABLE trajectories (
      id TEXT PRIMARY KEY,
      version INTEGER,
      persona_id TEXT,
      project_id TEXT,
      task_title TEXT,
      task_description TEXT,
      status TEXT,
      started_at TEXT,
      completed_at TEXT,
      decisions_json TEXT NOT NULL,
      retrospective_json TEXT NOT NULL,
      search_text TEXT NOT NULL,
      path TEXT,
      updated_ms INTEGER NOT NULL,
      timestamp_ms INTEGER NOT NULL
    )`);
    const insertHistory = db.prepare(
      'INSERT INTO history (source, session_id, project, prompt, timestamp_ms) VALUES (?, ?, ?, ?, ?)',
    );
    const insertTrajectory = db.prepare(
      `INSERT INTO trajectories
       (id, version, persona_id, project_id, task_title, task_description, status,
        started_at, completed_at, decisions_json, retrospective_json, search_text,
        path, updated_ms, timestamp_ms)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    );
    try {
      insertHistory.run(['claude', 'shared', '/work/app', 'scoped root prompt', 1_000]);
      insertHistory.run(['codex', 'shared', '/work/app/pkg', 'scoped child prompt', 2_000]);
      insertHistory.run(['cursor', 'shared', '/work/other', 'outside prompt', 3_000]);
      insertTrajectory.run([
        'scoped-run',
        1,
        'planner',
        'app',
        'Scoped task',
        'Scoped decision',
        'completed',
        null,
        null,
        '[]',
        '{}',
        'scoped decision',
        '/work/app/.trajectories/planner/compacted/scoped-run.json',
        4_000,
        4_000,
      ]);
      insertTrajectory.run([
        'outside-run',
        1,
        'planner',
        'other',
        'Outside task',
        'Outside decision',
        'completed',
        null,
        null,
        '[]',
        '{}',
        'outside decision',
        '/work/other/.trajectories/planner/compacted/outside-run.json',
        5_000,
        5_000,
      ]);
    } finally {
      insertHistory.free();
      insertTrajectory.free();
    }
    await writeFile(dbPath, Buffer.from(db.export()));
  } finally {
    db.close();
  }
}

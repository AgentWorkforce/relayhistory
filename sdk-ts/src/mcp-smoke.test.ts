import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtemp, mkdir, realpath, writeFile } from 'node:fs/promises';
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
      'search_trajectories',
      'why_for_task',
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
      timestamp_ms INTEGER NOT NULL
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

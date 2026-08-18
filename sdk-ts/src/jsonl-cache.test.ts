import assert from 'node:assert/strict';
import { mkdtemp, mkdir, readFile, rm, stat, unlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import initSqlJs from 'sql.js';

test('JSONL fallback persists, skips unchanged files, and yields to a real database', async () => {
  const root = await mkdtemp(join(tmpdir(), 'ai-hist-jsonl-cache-'));
  const fakeHome = join(root, 'home');
  const claudeDir = join(fakeHome, '.claude');
  const claudeHistory = join(claudeDir, 'history.jsonl');
  const cachePath = join(root, 'cache', 'jsonl-fallback-cache.db');
  const realDbPath = join(root, 'ai-history.db');
  await mkdir(claudeDir, { recursive: true });
  await writeFile(
    claudeHistory,
    `${JSON.stringify({
      display: 'fixture JSONL prompt',
      timestamp: 1_700_000_000_000,
      project: '/fixture/project',
      sessionId: 'fixture-session',
    })}\n`,
  );

  const previousEnv = {
    HOME: process.env.HOME,
    AI_HIST_JSONL_CACHE_DB: process.env.AI_HIST_JSONL_CACHE_DB,
    OPENCODE_DB: process.env.OPENCODE_DB,
    TRAJECTORY_ROOT: process.env.TRAJECTORY_ROOT,
  };
  process.env.HOME = fakeHome;
  process.env.AI_HIST_JSONL_CACHE_DB = cachePath;
  process.env.OPENCODE_DB = join(root, 'missing-opencode.db');
  process.env.TRAJECTORY_ROOT = join(root, 'missing-trajectories');

  let setJsonlReadObserver: ((observer?: (path: string) => void) => void) | undefined;
  try {
    const index = await import('./index.js');
    ({ setJsonlReadObserver } = await import('./jsonl-sources.js'));

    let reads: string[] = [];
    setJsonlReadObserver((path) => reads.push(path));
    const first = await index.openAiHist({ dbPath: realDbPath, fallback: 'jsonl' });
    try {
      assert.equal(first.sourceKind, 'jsonl');
      assert.deepEqual(first.recent({ limit: 10 }).map((entry) => entry.prompt), ['fixture JSONL prompt']);
    } finally {
      first.close();
    }
    assert.deepEqual(reads, [claudeHistory]);
    assert.equal((await stat(cachePath)).isFile(), true);

    const SQL = await initSqlJs();
    let cacheDb = new SQL.Database(await readFile(cachePath));
    try {
      const manifest = cacheDb.exec('SELECT path, mtime_ms, size FROM ingested_files');
      assert.deepEqual(manifest[0]?.values.map((row) => row[0]), [claudeHistory]);
      assert.equal(cacheDb.exec("SELECT value FROM cache_metadata WHERE key = 'schema_version'")[0]?.values[0]?.[0], '1');
    } finally {
      cacheDb.close();
    }

    reads = [];
    const second = await index.openAiHist({ dbPath: realDbPath, fallback: 'jsonl' });
    try {
      assert.deepEqual(second.recent({ limit: 10 }).map((entry) => entry.prompt), ['fixture JSONL prompt']);
    } finally {
      second.close();
    }
    assert.deepEqual(reads, [], 'the unchanged JSONL file must not be read again');

    cacheDb = new SQL.Database(await readFile(cachePath));
    try {
      cacheDb.run("UPDATE cache_metadata SET value = '999' WHERE key = 'schema_version'");
      await writeFile(cachePath, Buffer.from(cacheDb.export()));
    } finally {
      cacheDb.close();
    }
    reads = [];
    const rebuilt = await index.openAiHist({ dbPath: realDbPath, fallback: 'jsonl' });
    try {
      assert.deepEqual(rebuilt.recent({ limit: 10 }).map((entry) => entry.prompt), ['fixture JSONL prompt']);
    } finally {
      rebuilt.close();
    }
    assert.deepEqual(reads, [claudeHistory], 'an incompatible cache version must trigger a clean rebuild');

    await writeRealDb(realDbPath, 'real SQLite prompt');
    reads = [];
    const real = await index.openAiHist({ dbPath: realDbPath, fallback: 'jsonl' });
    try {
      assert.equal(real.sourceKind, 'sqlite');
      assert.deepEqual(real.recent({ limit: 10 }).map((entry) => entry.prompt), ['real SQLite prompt']);
    } finally {
      real.close();
    }
    assert.deepEqual(reads, [], 'the real database fast path must not consult the fallback sources');

    await unlink(realDbPath);
    await unlink(claudeHistory);
    const afterDeletion = await index.openAiHist({ dbPath: realDbPath, fallback: 'jsonl' });
    try {
      assert.deepEqual(afterDeletion.recent({ limit: 10 }), []);
    } finally {
      afterDeletion.close();
    }
    cacheDb = new SQL.Database(await readFile(cachePath));
    try {
      assert.equal(cacheDb.exec('SELECT path FROM ingested_files').length, 0);
      assert.equal(cacheDb.exec("SELECT prompt FROM history WHERE source = 'claude'").length, 0);
    } finally {
      cacheDb.close();
    }
  } finally {
    setJsonlReadObserver?.();
    restoreEnv('HOME', previousEnv.HOME);
    restoreEnv('AI_HIST_JSONL_CACHE_DB', previousEnv.AI_HIST_JSONL_CACHE_DB);
    restoreEnv('OPENCODE_DB', previousEnv.OPENCODE_DB);
    restoreEnv('TRAJECTORY_ROOT', previousEnv.TRAJECTORY_ROOT);
    await rm(root, { recursive: true, force: true });
  }
});

async function writeRealDb(path: string, prompt: string): Promise<void> {
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
      `INSERT INTO history (source, session_id, project, prompt, timestamp_ms, git_branch)
       VALUES ('codex', 'real-session', '/real/project', ?, 1700000001000, NULL)`,
      [prompt],
    );
    await writeFile(path, Buffer.from(db.export()));
  } finally {
    db.close();
  }
}

function restoreEnv(name: string, value: string | undefined): void {
  if (value === undefined) delete process.env[name];
  else process.env[name] = value;
}

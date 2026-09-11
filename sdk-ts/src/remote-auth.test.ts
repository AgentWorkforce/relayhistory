import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';
import { RelayHistoryError, stats } from './index.js';

const run = promisify(execFile);
const cli = join(dirname(fileURLToPath(import.meta.url)), 'cli.js');

test('stats with remote scope fails when unauthenticated', async () => {
  const root = await mkdtemp(join(tmpdir(), 'relayhistory-remote-auth-'));
  const dbPath = join(root, 'history.db');
  const saved = { HOME: process.env.HOME, USERPROFILE: process.env.USERPROFILE };
  process.env.HOME = root;
  process.env.USERPROFILE = root;

  try {
    await assert.rejects(
      () => stats({ dbPath, scope: 'remote' }),
      (error: unknown) => error instanceof RelayHistoryError
        && error.code === 'CLOUD_AUTH_FAILED'
        && error.message.includes('not authenticated for remote scope')
        && error.message.includes('ai-hist login'),
    );
  } finally {
    if (saved.HOME === undefined) delete process.env.HOME; else process.env.HOME = saved.HOME;
    if (saved.USERPROFILE === undefined) delete process.env.USERPROFILE; else process.env.USERPROFILE = saved.USERPROFILE;
    await rm(root, { recursive: true, force: true });
  }
});

test('stats with all scope keeps all and returns empty totals when unauthenticated', async () => {
  const root = await mkdtemp(join(tmpdir(), 'relayhistory-all-auth-'));
  const dbPath = join(root, 'history.db');
  const saved = { HOME: process.env.HOME, USERPROFILE: process.env.USERPROFILE };
  process.env.HOME = root;
  process.env.USERPROFILE = root;

  try {
    const allStats = await stats({ dbPath, scope: 'all' });
    assert.equal(allStats.scope, 'all');
    assert.equal(allStats.total, 0);
  } finally {
    if (saved.HOME === undefined) delete process.env.HOME; else process.env.HOME = saved.HOME;
    if (saved.USERPROFILE === undefined) delete process.env.USERPROFILE; else process.env.USERPROFILE = saved.USERPROFILE;
    await rm(root, { recursive: true, force: true });
  }
});

test('CLI stats --all returns all-scope results when unauthenticated', async () => {
  const root = await mkdtemp(join(tmpdir(), 'relayhistory-cli-stats-all-'));
  const env = { ...process.env, HOME: root, USERPROFILE: root, RELAYHISTORY_NO_UPDATE_CHECK: '1' };

  try {
    const { stdout } = await run(process.execPath, [
      cli, 'stats', '--all', '--db', join(root, 'history.db'), '--json', '--no-warning',
    ], { env });
    const result = JSON.parse(stdout) as { scope: string; total: number };
    assert.equal(result.scope, 'all');
    assert.equal(result.total, 0);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('stats with local scope works without authentication', async () => {
  const root = await mkdtemp(join(tmpdir(), 'relayhistory-local-auth-'));
  const dbPath = join(root, 'history.db');
  const saved = { HOME: process.env.HOME, USERPROFILE: process.env.USERPROFILE };
  process.env.HOME = root;
  process.env.USERPROFILE = root;

  try {
    const localStats = await stats({ dbPath, scope: 'local' });
    assert.equal(localStats.scope, 'local');
    assert.equal(localStats.total, 0);

    const defaultStats = await stats({ dbPath });
    assert.equal(defaultStats.scope, 'local');
    assert.equal(defaultStats.total, 0);
  } finally {
    if (saved.HOME === undefined) delete process.env.HOME; else process.env.HOME = saved.HOME;
    if (saved.USERPROFILE === undefined) delete process.env.USERPROFILE; else process.env.USERPROFILE = saved.USERPROFILE;
    await rm(root, { recursive: true, force: true });
  }
});

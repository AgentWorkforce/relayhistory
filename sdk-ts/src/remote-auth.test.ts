import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { execFile } from 'node:child_process';
import { dirname, join } from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';
import { RelayHistoryError, listSessionCatalogPage, search, stats } from './index.js';

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

test('stats with all scope returns local results when unauthenticated', async () => {
  const root = await mkdtemp(join(tmpdir(), 'relayhistory-all-auth-'));
  const dbPath = join(root, 'history.db');
  const saved = { HOME: process.env.HOME, USERPROFILE: process.env.USERPROFILE };
  process.env.HOME = root;
  process.env.USERPROFILE = root;

  try {
    const allStats = await stats({ dbPath, scope: 'all' });
    // When unauthenticated, 'all' scope should fall back to 'local' scope
    assert.equal(allStats.scope, 'local');
    assert.equal(allStats.total, 0);
  } finally {
    if (saved.HOME === undefined) delete process.env.HOME; else process.env.HOME = saved.HOME;
    if (saved.USERPROFILE === undefined) delete process.env.USERPROFILE; else process.env.USERPROFILE = saved.USERPROFILE;
    await rm(root, { recursive: true, force: true });
  }
});

test('listSessionCatalogPage with remote scope fails when unauthenticated', async () => {
  const root = await mkdtemp(join(tmpdir(), 'relayhistory-remote-list-'));
  const dbPath = join(root, 'history.db');
  const saved = { HOME: process.env.HOME, USERPROFILE: process.env.USERPROFILE };
  process.env.HOME = root;
  process.env.USERPROFILE = root;

  try {
    await assert.rejects(
      () => listSessionCatalogPage({ dbPath, scope: 'remote' }),
      (error: unknown) => error instanceof RelayHistoryError
        && error.code === 'CLOUD_AUTH_FAILED'
        && error.message.includes('not authenticated for remote scope'),
    );
  } finally {
    if (saved.HOME === undefined) delete process.env.HOME; else process.env.HOME = saved.HOME;
    if (saved.USERPROFILE === undefined) delete process.env.USERPROFILE; else process.env.USERPROFILE = saved.USERPROFILE;
    await rm(root, { recursive: true, force: true });
  }
});

test('search with remote scope fails when unauthenticated', async () => {
  const root = await mkdtemp(join(tmpdir(), 'relayhistory-remote-search-'));
  const dbPath = join(root, 'history.db');
  const saved = { HOME: process.env.HOME, USERPROFILE: process.env.USERPROFILE };
  process.env.HOME = root;
  process.env.USERPROFILE = root;

  try {
    await assert.rejects(
      () => search('query', { dbPath, scope: 'remote' }),
      (error: unknown) => error instanceof RelayHistoryError
        && error.code === 'CLOUD_AUTH_FAILED',
    );
  } finally {
    if (saved.HOME === undefined) delete process.env.HOME; else process.env.HOME = saved.HOME;
    if (saved.USERPROFILE === undefined) delete process.env.USERPROFILE; else process.env.USERPROFILE = saved.USERPROFILE;
    await rm(root, { recursive: true, force: true });
  }
});

test('CLI stats --all returns local results when unauthenticated', async () => {
  const root = await mkdtemp(join(tmpdir(), 'relayhistory-cli-stats-all-'));
  const env = { ...process.env, HOME: root, USERPROFILE: root, RELAYHISTORY_NO_UPDATE_CHECK: '1' };

  try {
    const { stdout } = await run(process.execPath, [
      cli, 'stats', '--all', '--db', join(root, 'history.db'), '--json', '--no-warning',
    ], { env });
    const result = JSON.parse(stdout) as { scope: string; total: number };
    assert.equal(result.scope, 'local');
    assert.equal(result.total, 0);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('CLI sessions list --remote fails when unauthenticated', async () => {
  const root = await mkdtemp(join(tmpdir(), 'relayhistory-cli-list-remote-'));
  const env = { ...process.env, HOME: root, USERPROFILE: root, RELAYHISTORY_NO_UPDATE_CHECK: '1' };

  try {
    await assert.rejects(
      run(process.execPath, [
        cli, 'sessions', 'list', '--remote', '--db', join(root, 'history.db'), '--no-warning',
      ], { env }),
      (error: unknown) => typeof error === 'object' && error !== null
        && 'code' in error && error.code === 1
        && 'stderr' in error && String(error.stderr).includes('CLOUD_AUTH_FAILED'),
    );
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

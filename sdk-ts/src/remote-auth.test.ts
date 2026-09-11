import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { RelayHistoryError, stats } from './index.js';

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
    assert.equal(allStats.scope, 'all');
    assert.equal(allStats.total, 0);
  } finally {
    if (saved.HOME === undefined) delete process.env.HOME; else process.env.HOME = saved.HOME;
    if (saved.USERPROFILE === undefined) delete process.env.USERPROFILE; else process.env.USERPROFILE = saved.USERPROFILE;
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

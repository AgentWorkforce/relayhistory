import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';
import { listSessionCatalogPage, RelayHistoryError, stats } from './index.js';

const run = promisify(execFile);
const cli = join(dirname(fileURLToPath(import.meta.url)), 'cli.js');
const HOUR_AHEAD = new Date(Date.now() + 3_600_000).toISOString();
const ELIGIBLE = { access_token_expires_at: HOUR_AHEAD, org_id: 'org-example' };

async function withIsolatedHome(runCase: (root: string, dbPath: string) => Promise<void>): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), 'relayhistory-remote-auth-'));
  const dbPath = join(root, 'history.db');
  const saved = { HOME: process.env.HOME, USERPROFILE: process.env.USERPROFILE, RELAYHISTORY_HOME: process.env.RELAYHISTORY_HOME };
  process.env.HOME = root;
  process.env.USERPROFILE = root;
  delete process.env.RELAYHISTORY_HOME;
  delete process.env.RELAYHISTORY_BASE_URL;
  delete process.env.AI_HIST_BASE_URL;

  try {
    await runCase(root, dbPath);
  } finally {
    if (saved.HOME === undefined) delete process.env.HOME; else process.env.HOME = saved.HOME;
    if (saved.USERPROFILE === undefined) delete process.env.USERPROFILE; else process.env.USERPROFILE = saved.USERPROFILE;
    if (saved.RELAYHISTORY_HOME === undefined) delete process.env.RELAYHISTORY_HOME; else process.env.RELAYHISTORY_HOME = saved.RELAYHISTORY_HOME;
    await rm(root, { recursive: true, force: true });
  }
}

test('listSessionCatalogPage with remote scope fails when unauthenticated', async () => {
  await withIsolatedHome(async (_root, dbPath) => {
    await assert.rejects(
      () => listSessionCatalogPage({ dbPath, scope: 'remote' }),
      (error: unknown) => error instanceof RelayHistoryError
        && error.code === 'CLOUD_AUTH_FAILED'
        && error.message.includes('not authenticated for remote scope')
        && error.message.includes('ai-hist login'),
    );
  });
});

test('CLI sessions list --remote exits non-zero when unauthenticated', async () => {
  const root = await mkdtemp(join(tmpdir(), 'relayhistory-cli-sessions-remote-'));
  const env = { ...process.env, HOME: root, USERPROFILE: root, RELAYHISTORY_NO_UPDATE_CHECK: '1' };

  try {
    await assert.rejects(
      () => run(process.execPath, [
        cli, 'sessions', 'list', '--remote', '--db', join(root, 'history.db'), '--json', '--no-warning',
      ], { env }),
      (error: unknown) => typeof error === 'object' && error !== null
        && 'code' in error
        && error.code === 1
        && 'stderr' in error
        && String(error.stderr).includes('CLOUD_AUTH_FAILED')
        && String(error.stderr).includes('not authenticated for remote scope'),
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('stats with remote scope fails when unauthenticated', async () => {
  await withIsolatedHome(async (_root, dbPath) => {
    await assert.rejects(
      () => stats({ dbPath, scope: 'remote' }),
      (error: unknown) => error instanceof RelayHistoryError
        && error.code === 'CLOUD_AUTH_FAILED'
        && error.message.includes('not authenticated for remote scope')
        && error.message.includes('ai-hist login'),
    );
  });
});

test('stats with remote scope propagates ambiguous stage selection errors', async () => {
  await withIsolatedHome(async (root, dbPath) => {
    const nativeHome = join(root, 'relayhistory');
    const stages = join(nativeHome, 'stages');
    await mkdir(stages, { recursive: true });
    process.env.RELAYHISTORY_HOME = nativeHome;
    for (const [key, baseUrl] of [['prod', 'https://history.agentrelay.com'], ['dev', 'https://dev.agentrelay.com']] as const) {
      await writeFile(join(stages, `${key}.auth.json`), JSON.stringify({
        base_url: baseUrl,
        access_token: `rth_at_${key}`,
        refresh_token: `rth_rt_${key}`,
        workspace_id: null,
        ...ELIGIBLE,
      }), { mode: 0o600 });
    }

    await assert.rejects(
      () => stats({ dbPath, scope: 'remote' }),
      (error: unknown) => error instanceof RelayHistoryError
        && error.code === 'CLOUD_AUTH_FAILED'
        && error.message.includes('Refusing to guess'),
    );
  });
});

test('stats with all scope falls back to local when credentials are absent', async () => {
  await withIsolatedHome(async (_root, dbPath) => {
    const allStats = await stats({ dbPath, scope: 'all' });
    assert.equal(allStats.scope, 'local');
    assert.equal(allStats.total, 0);
  });
});

test('stats with all scope propagates ambiguous stage selection errors', async () => {
  await withIsolatedHome(async (root, dbPath) => {
    const nativeHome = join(root, 'relayhistory');
    const stages = join(nativeHome, 'stages');
    await mkdir(stages, { recursive: true });
    process.env.RELAYHISTORY_HOME = nativeHome;
    for (const [key, baseUrl] of [['prod', 'https://history.agentrelay.com'], ['dev', 'https://dev.agentrelay.com']] as const) {
      await writeFile(join(stages, `${key}.auth.json`), JSON.stringify({
        base_url: baseUrl,
        access_token: `rth_at_${key}`,
        refresh_token: `rth_rt_${key}`,
        workspace_id: null,
        ...ELIGIBLE,
      }), { mode: 0o600 });
    }

    await assert.rejects(
      () => stats({ dbPath, scope: 'all' }),
      (error: unknown) => error instanceof RelayHistoryError
        && error.code === 'CLOUD_AUTH_FAILED'
        && error.message.includes('Refusing to guess'),
    );
  });
});

test('CLI stats --all returns local-scope results when unauthenticated', async () => {
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

test('stats with local scope works without authentication', async () => {
  await withIsolatedHome(async (_root, dbPath) => {
    const localStats = await stats({ dbPath, scope: 'local' });
    assert.equal(localStats.scope, 'local');
    assert.equal(localStats.total, 0);

    const defaultStats = await stats({ dbPath });
    assert.equal(defaultStats.scope, 'local');
    assert.equal(defaultStats.total, 0);
  });
});

import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';
import { gunzipSync } from 'node:zlib';
import { getSessionEvents, listSessionCatalogPage, recent, search, stats, type CatalogCursor, type SessionScope } from './index.js';
import { nativeCall } from './native.js';

const run = promisify(execFile);
const cli = fileURLToPath(new URL('./cli.js', import.meta.url));
const states = ['absent', 'malformed', 'expired', 'ambiguous'] as const;
type AuthState = typeof states[number];

async function withFixture(state: AuthState, body: (dbPath: string) => Promise<void>): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), 'relayhistory-cached-scope-'));
  const keys = ['HOME', 'USERPROFILE', 'RELAYHISTORY_HOME', 'RELAYHISTORY_BASE_URL', 'AI_HIST_BASE_URL', 'RELAYHISTORY_NO_UPDATE_CHECK'];
  const saved = new Map(keys.map((name) => [name, process.env[name]]));
  for (const name of keys) delete process.env[name];
  process.env.HOME = root;
  process.env.USERPROFILE = root;
  process.env.RELAYHISTORY_HOME = join(root, 'commercial');
  process.env.RELAYHISTORY_NO_UPDATE_CHECK = '1';
  try {
    const stages = join(process.env.RELAYHISTORY_HOME, 'stages');
    await mkdir(stages, { recursive: true });
    if (state === 'malformed') await writeFile(join(stages, 'broken.auth.json'), '{not-json', { mode: 0o600 });
    if (state === 'expired' || state === 'ambiguous') {
      for (const stage of state === 'expired' ? ['prod'] : ['prod', 'dev']) {
        await writeFile(join(stages, `${stage}.auth.json`), JSON.stringify({
          base_url: `https://${stage}.example.invalid`, access_token: 'synthetic-expired-token',
          refresh_token: 'synthetic-refresh-token', access_token_expires_at: '2000-01-01T00:00:00Z',
          org_id: 'fixture-org', workspace_id: null,
        }), { mode: 0o600 });
      }
    }
    // Current core schema, generated only from three synthetic Claude sessions.
    // The regeneration recipe is fixtures/regenerate-offline-history.mjs. Each
    // test gets its own copy, including when a later native schema migrates it.
    const dbPath = join(root, 'history.db');
    await writeFile(dbPath, gunzipSync(await readFile(new URL('../fixtures/offline-history.db.gz', import.meta.url))));
    await body(dbPath);
  } finally {
    for (const [name, value] of saved) {
      if (value === undefined) delete process.env[name]; else process.env[name] = value;
    }
    await rm(root, { recursive: true, force: true });
  }
}

const expected: Record<SessionScope, string[]> = {
  local: ['both', 'local-only'], remote: ['both', 'remote-only'], all: ['both', 'local-only', 'remote-only'],
};

for (const state of states) {
  test(`cached scope reads preserve remote evidence with ${state} commercial credentials`, async () => {
    await withFixture(state, async (dbPath) => {
      for (const scope of ['local', 'remote', 'all'] as const) {
        const options = { dbPath, scope };
        assert.deepEqual((await search('offlinefixture', options)).map((row) => row.sessionId).sort(), expected[scope]);
        assert.deepEqual((await recent(options)).map((row) => row.sessionId).sort(), expected[scope]);
        const counts = await stats(options);
        assert.equal(counts.scope, scope);
        assert.equal(counts.total, expected[scope].length);
        const sessions: string[] = [];
        let after: CatalogCursor | undefined;
        do {
          const page = await listSessionCatalogPage({ ...options, limit: 1, after });
          assert.equal(page.scope, scope);
          sessions.push(...page.sessions.map((row) => row.sessionId));
          assert.ok(sessions.length <= expected[scope].length, 'pagination cannot repeat a session');
          after = page.nextCursor ?? undefined;
        } while (after);
        assert.deepEqual(sessions.sort(), expected[scope]);
      }
      const events = await getSessionEvents('remote-only', { dbPath, source: 'claude' });
      assert.equal(events.length, 1);
      assert.equal(events[0].text, 'offlinefixture remote-only');
    });
  });
}

test('cached queries never call the native commercial credential loader', async () => {
  await withFixture('ambiguous', async (dbPath) => {
    await nativeCall(async (native) => {
      const original = native.cloudLoadAuth;
      let reads = 0;
      native.cloudLoadAuth = async () => { reads++; throw new Error('commercial auth must not be read'); };
      try {
        for (const scope of ['local', 'remote', 'all'] as const) {
          await search('offlinefixture', { dbPath, scope });
          await recent({ dbPath, scope });
          await listSessionCatalogPage({ dbPath, scope });
          await stats({ dbPath, scope });
        }
        assert.equal(reads, 0);
      } finally {
        native.cloudLoadAuth = original;
      }
    });
  });
});

test('CLI cached remote/all reads work with malformed commercial credentials', async () => {
  await withFixture('malformed', async (dbPath) => {
    for (const scope of ['remote', 'all'] as const) {
      const flags = [`--${scope}`, '--db', dbPath, '--json', '--no-warning', '--no-bootstrap'];
      const catalog = await run(process.execPath, [cli, 'sessions', 'list', ...flags], { env: process.env });
      const page = JSON.parse(catalog.stdout) as { scope: string; sessions: Array<{ session_id: string }> };
      assert.equal(page.scope, scope);
      assert.deepEqual(page.sessions.map((row) => row.session_id).sort(), expected[scope]);
      const counts = JSON.parse((await run(process.execPath, [cli, 'stats', ...flags], { env: process.env })).stdout) as { scope: string; total: number };
      assert.equal(counts.scope, scope);
      assert.equal(counts.total, expected[scope].length);
    }
  });
});

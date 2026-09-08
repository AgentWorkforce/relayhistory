import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { promisify } from 'node:util';
import { fileURLToPath } from 'node:url';
import test from 'node:test';
import { bootstrapLocal, InvalidArgumentError } from './index.js';

const run = promisify(execFile);
const cli = fileURLToPath(new URL('./cli.js', import.meta.url));
const sdk = new URL('./index.js', import.meta.url).href;

test('bootstrap validates its work budget before loading native code', async () => {
  for (const limit of [0, -1, 1001, 1.5, NaN]) {
    await assert.rejects(bootstrapLocal({ limit }), InvalidArgumentError);
  }
});

test('SDK bootstrap retries an empty home, indexes native evidence, and skips a ready database', async () => {
  const home = await mkdtemp(join(tmpdir(), 'ai-hist-bootstrap-'));
  const env = { ...process.env, HOME: home, USERPROFILE: home, AI_HIST_DB: join(home, 'history.db') };
  const call = async () => JSON.parse((await run(process.execPath, ['--input-type=module', '-e',
    `import { bootstrapLocal } from ${JSON.stringify(sdk)}; console.log(JSON.stringify(await bootstrapLocal()));`,
  ], { env })).stdout);
  try {
    assert.equal((await call()).status, 'empty');
    const folder = join(home, '.claude', 'projects', 'app');
    await mkdir(folder, { recursive: true });
    await writeFile(join(folder, 'first.jsonl'), JSON.stringify({
      sessionId: 'first', uuid: 'user-first', cwd: '/work/app', type: 'user',
      message: { role: 'user', content: 'find the bootstrap needle' }, timestamp: '2026-09-08T10:00:00Z',
    }) + '\n');
    const skipped = await run(process.execPath, [cli, '--no-bootstrap', '--json'], { env });
    assert.equal(JSON.parse(skipped.stdout).sessions.length, 0);
    const first = await call();
    assert.equal(first.status, 'ready');
    assert.equal(first.alreadyIndexed, false);
    assert.equal(first.indexedPrompts, 1);
    assert.equal(first.hydratedSessions, 1);
    const found = await run(process.execPath, [cli, 'search', 'bootstrap needle', '--json'], { env });
    assert.equal(JSON.parse(found.stdout)[0].session_id, 'first');
    await rm(folder, { recursive: true });
    const second = await run(process.execPath, [cli, '--json'], { env });
    assert.equal(JSON.parse(second.stdout).already_indexed, true);
    assert.equal(JSON.parse(second.stdout).discovery, null);
  } finally {
    await rm(home, { recursive: true, force: true });
  }
});

test('bare CLI discovers and indexes on first invocation with an explicit database', async () => {
  const home = await mkdtemp(join(tmpdir(), 'ai-hist-first-cli-'));
  const env = { ...process.env, HOME: home, USERPROFILE: home };
  try {
    const folder = join(home, '.codex', 'sessions', '2026', '09', '08');
    await mkdir(folder, { recursive: true });
    await writeFile(join(folder, 'rollout-first.jsonl'), [
      { timestamp: '2026-09-08T10:00:00Z', type: 'session_meta', payload: { id: 'codex-first', cwd: '/work/app' } },
      { timestamp: '2026-09-08T10:00:01Z', type: 'event_msg', payload: { type: 'user_message', message: 'codex bootstrap needle' } },
    ].map((row) => JSON.stringify(row)).join('\n') + '\n');
    const db = join(home, 'custom.db');
    const first = await run(process.execPath, [cli, '--db', db], { env });
    assert.match(first.stdout, /Ready: 1 indexed prompt/);
    const found = await run(process.execPath, [cli, 'search', 'bootstrap needle', '--db', db, '--json'], { env });
    assert.equal(JSON.parse(found.stdout)[0].session_id, 'codex-first');
  } finally {
    await rm(home, { recursive: true, force: true });
  }
});

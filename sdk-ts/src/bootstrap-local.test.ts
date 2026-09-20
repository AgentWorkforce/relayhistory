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
    const pretty = await run(process.execPath, [cli, 'sessions', 'list', '--pretty'], { env });
    assert.match(pretty.stdout, /✦ \[claude\].*first.*find the bootstrap needle/);
    assert.doesNotMatch(pretty.stdout, /\x1b/);
    await assert.rejects(run(process.execPath, [cli, 'sessions', 'list', '--pretty', '--json'], { env }),
      (error: unknown) => (error as { code: number; stderr: string }).code === 2
        && (error as { stderr: string }).stderr.includes('mutually exclusive'));
    await rm(folder, { recursive: true });
    const second = await run(process.execPath, [cli, '--json'], { env });
    assert.equal(JSON.parse(second.stdout).already_indexed, true);
    assert.equal(JSON.parse(second.stdout).discovery, null);
  } finally {
    await rm(home, { recursive: true, force: true });
  }
});

// Bootstrap hydrates with includeRelated: false, so the absent `relationship`
// coverage is its own choice. Only what the provider itself cannot produce is a
// capability limitation -- otherwise every Claude bootstrap would report the
// provider as limited and land in `partial`. Grok is the prompt-only exemplar
// here; cursor stopped being one when its parser became event-level.
test('bootstrap reports only the evidence the provider cannot produce, not what it declined', async () => {
  const home = await mkdtemp(join(tmpdir(), 'ai-hist-bootstrap-coverage-'));
  const env = { ...process.env, HOME: home, USERPROFILE: home, AI_HIST_DB: join(home, 'history.db') };
  const call = async () => JSON.parse((await run(process.execPath, ['--input-type=module', '-e',
    `import { bootstrapLocal } from ${JSON.stringify(sdk)}; console.log(JSON.stringify(await bootstrapLocal()));`,
  ], { env })).stdout) as {
    status: string; diagnostics: Array<{ source: string; code: string; message: string }>;
  };
  try {
    const grok = join(home, '.grok', 'sessions', '%2Fwork%2Fapp', 'grok-boot');
    await mkdir(grok, { recursive: true });
    await writeFile(join(grok, 'summary.json'), JSON.stringify({
      info: { id: 'grok-boot', cwd: '/work/app' },
      created_at: '2026-08-31T10:00:00.000Z',
      updated_at: '2026-08-31T10:00:00.000Z',
    }));
    await writeFile(join(grok, 'chat_history.jsonl'),
      JSON.stringify({ type: 'user', content: 'bootstrap grok prompt' }) + '\n');
    const result = await call();
    const limited = result.diagnostics.find((item) => item.code === 'CAPABILITY_LIMITED');
    assert.ok(limited, 'a prompt-only provider is still reported as limited');
    assert.equal(limited.source, 'grok');
    assert.equal(limited.message, 'Provider exposes no session_event, tool_call, file_edit evidence');
    // `relationship` is absent from the message: bootstrap declined it.
    assert.doesNotMatch(limited.message, /relationship/);
    assert.equal(result.status, 'partial');
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

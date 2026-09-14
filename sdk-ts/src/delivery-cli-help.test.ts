import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { access, mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { promisify } from 'node:util';
import { fileURLToPath } from 'node:url';
import test from 'node:test';
const run = promisify(execFile);
const cli = fileURLToPath(new URL('./cli.js', import.meta.url));
test('delivery group reports missing or unknown subcommands and supports help before IO', async t => {
  const home = await mkdtemp(join(tmpdir(), 'delivery-help-'));
  t.after(() => rm(home, { recursive: true, force: true }));
  const db = join(home, 'never-created.db');
  const env = { ...process.env, HOME: home, USERPROFILE: home, AI_HIST_DB: db };
  for (const flag of ['--help', '-h']) {
    const result = await run(process.execPath, [cli, 'delivery', flag, '--no-warning'], { env });
    assert.equal(result.stderr, '');
    assert.match(result.stdout, /ai-hist delivery enable\|drain\|run/);
  }
  for (const [args, expected] of [[[], 'delivery requires a subcommand'], [['typo'], "unknown delivery subcommand 'typo'"]] as const) {
    await assert.rejects(run(process.execPath, [cli, 'delivery', ...args, '--no-warning'], { env }), (error: unknown) => {
      const result = error as { code?: number; stderr?: string };
      return result.code === 2 && result.stderr?.includes(expected) === true && result.stderr.includes('Usage:');
    });
  }
  await assert.rejects(access(db), { code: 'ENOENT' });
});

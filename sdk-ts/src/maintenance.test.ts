import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, access, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { maintainRetention, compactDatabase } from './maintenance.js';
import { runCli } from './cli.js';

// Native integration runs after rebuilding the addon for this checkout.
test('retention preview does not create a store and apply is explicit', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'retention-sdk-'));
  const dbPath = join(dir, 'history.db');
  const preview = await maintainRetention({ dbPath, days: 7 });
  assert.equal(preview.dryRun, true);
  assert.equal(preview.preview, 'policy_only');
  assert.equal(preview.effectiveDays, 7);
  assert.equal(preview.report, null);
  await assert.rejects(access(dbPath));
  const applied = await maintainRetention({ dbPath, days: 7, apply: true });
  assert.equal(applied.dryRun, false);
  assert.equal(applied.report?.awaitingUploadAck, true);
  const before = await readFile(dbPath);
  const saved = await maintainRetention({ dbPath, days: 1 });
  assert.equal(saved.storedDays, 7);
  assert.equal(saved.effectiveDays, 1);
  assert.deepEqual(await readFile(dbPath), before);
  const result = await compactDatabase({ dbPath });
  assert.ok(result.dbBytesAfter > 0);
  const compactOutput: string[] = [];
  const code = await runCli(['compact', '--db', dbPath, '--json', '--no-warning'], {
    stdout: (text) => compactOutput.push(text), stderr: () => {},
  });
  assert.equal(code, 0);
  assert.ok(JSON.parse(compactOutput.join('')).db_bytes_after > 0);
});

test('malformed retention options fail before opening a store', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'retention-invalid-'));
  const dbPath = join(dir, 'history.db');
  for (const options of [{ days: -1 }, { days: 1.5 }, { days: 0x100000000 }, { maxRows: 0 }, { maxRows: 1025 }, { allowUnuploaded: true }]) {
    await assert.rejects(maintainRetention({ ...options, dbPath }), { code: 'INVALID_ARGUMENT' });
  }
});

test('production CLI retention previews an absent store without bootstrap', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'retention-cli-'));
  const dbPath = join(dir, 'history.db');
  const stdout: string[] = [], stderr: string[] = [];
  const code = await runCli(['retention', '--db', dbPath, '--days', '7', '--json', '--no-warning'], {
    stdout: (text) => stdout.push(text), stderr: (text) => stderr.push(text),
  });
  assert.equal(code, 0, stderr.join(''));
  assert.equal(JSON.parse(stdout.join('')).dry_run, true);
  await assert.rejects(access(dbPath));
});

import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';

const verifier = fileURLToPath(new URL('./verify-first-search.mjs', import.meta.url));

test('failed verifier attempts never share stale success evidence or overwrite prior runs', () => {
  const root = mkdtempSync(join(tmpdir(), 'ai-hist-evidence-'));
  try {
    const output = join(root, 'evidence');
    const emptyPath = join(root, 'no-commands');
    mkdirSync(output);
    mkdirSync(emptyPath);
    for (const name of ['artifact.json', 'before.json', 'after.json', 'before.cast', 'after.cast']) {
      writeFileSync(join(output, name), 'old-run');
    }
    const directories = [];
    for (let attempt = 0; attempt < 2; attempt++) {
      // Prevent the real npm/Docker commands from running. Regardless of build
      // availability, this exercises the verifier's failed-attempt path.
      const result = spawnSync(process.execPath, [verifier, output], {
        env: { ...process.env, PATH: emptyPath }, encoding: 'utf8', timeout: 10_000,
      });
      assert.equal(result.status, 1, result.stderr);
      const directory = result.stdout.match(/^Evidence directory: (.+)$/m)?.[1];
      assert.ok(directory, result.stdout);
      directories.push(directory);
      assert.deepEqual(readdirSync(directory), [], 'failed attempts cannot inherit success results');
      for (const name of ['artifact.json', 'before.json', 'after.json', 'before.cast', 'after.cast']) {
        assert.equal(readFileSync(join(output, name), 'utf8'), 'old-run');
      }
    }
    assert.notEqual(directories[0], directories[1]);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

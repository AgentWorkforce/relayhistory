import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';

const smoke = new URL('./smoke-native-cli.mjs', import.meta.url);

test('native smoke retains CLI diagnostics when exit 1 has no JSON', () => {
  const root = mkdtempSync(join(tmpdir(), 'ai-hist-smoke-diagnostics-'));
  try {
    const cli = join(root, 'cli.cjs');
    writeFileSync(cli, `
      console.error('ai-hist: NATIVE_LOAD_FAILED: fixture loader error');
      process.exitCode = 1;
    `);
    const result = spawnSync(process.execPath, [fileURLToPath(smoke), cli], { encoding: 'utf8' });
    assert.equal(result.status, 1);
    assert.match(result.stderr, /empty store search did not return valid JSON/);
    assert.match(result.stderr, /exit=1/);
    assert.match(result.stderr, /NATIVE_LOAD_FAILED: fixture loader error/);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

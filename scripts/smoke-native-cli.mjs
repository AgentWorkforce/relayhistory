import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { spawnSync } from 'node:child_process';

const cli = resolve(process.argv[2]);
const temporary = mkdtempSync(join(tmpdir(), 'ai-hist-native-smoke-'));
const env = { ...process.env, HOME: temporary };
const args = ['search', 'anything', '--local', '--db', join(temporary, 'empty.db'), '--json'];

function run(args) {
  const result = spawnSync(process.execPath, [cli, ...args], {
    env, encoding: 'utf8', timeout: 30_000,
  });
  if (result.error) throw result.error;
  assert.equal(result.signal, null, `CLI killed by ${result.signal}`);
  return result;
}

function search() {
  const result = run(args);
  assert.equal(result.status, 0, `ai-hist search anything failed:\n${result.stderr}`);
  assert.deepEqual(JSON.parse(result.stdout), [], 'Fresh database should return no results');
  console.log('PASS: ai-hist search anything (native addon loaded)');
}

try {
  search();
  if (process.argv.includes('--prove-rejection')) {
    // Resolve the actual loaded binary in a child process so it is unloaded
    // before corruption. Handles both local builds and npm platform packages.
    const locate = spawnSync(process.execPath, ['--input-type=commonjs', '-e', `
      const requireFromCLI = require('node:module').createRequire(process.argv[1]);
      requireFromCLI('ai-hist-native');
      const binaries = Object.keys(require.cache).filter(path => path.endsWith('.node'));
      if (binaries.length !== 1) throw new Error('Expected exactly one native addon: ' + binaries);
      process.stdout.write(binaries[0]);
    `, cli], { env, encoding: 'utf8', timeout: 30_000 });
    if (locate.error) throw locate.error;
    assert.equal(locate.status, 0, locate.stderr);
    const binary = locate.stdout;
    const original = readFileSync(binary);
    try {
      writeFileSync(binary, 'deliberately broken native artifact\n');
      assert.equal(run(['--version']).status, 0, '--version still succeeds without loading native');
      const broken = run(args);
      assert.notEqual(broken.status, 0, 'Smoke gate accepted a broken addon');
      assert.match(broken.stderr, /NATIVE_LOAD_FAILED/, 'Failure must be a native load failure');
      console.log(`PASS: broken artifact rejected (search exit ${broken.status}; --version exit 0)`);
      console.log(broken.stderr.trim());
    } finally {
      writeFileSync(binary, original);
    }
    search();
  }
} finally {
  rmSync(temporary, { recursive: true, force: true });
}

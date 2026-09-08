import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';
import { resolveImage } from './first-search-image.mjs';

const verifier = fileURLToPath(new URL('./verify-first-search.mjs', import.meta.url));

test('locally built images without registry digests are used without pulling', () => {
  const calls = [];
  const identity = resolveImage((command, args) => {
    calls.push({ command, args });
    // Model Docker rejecting an unconditional index into empty RepoDigests.
    assert.match(args.at(-1), /{{if \.RepoDigests}}.*{{else}}{{\.Id}}{{end}}/);
    return 'sha256:local-image\n';
  }, 'local-first-search:test');
  assert.equal(identity, 'sha256:local-image');
  assert.equal(calls.length, 1);
  assert.equal(calls[0].command, 'docker');
  assert.deepEqual(calls[0].args.slice(0, 3), ['image', 'inspect', 'local-first-search:test']);
});

test('an absent image is pulled before retrying inspection', () => {
  const calls = [];
  let available = false;
  const identity = resolveImage((command, args) => {
    assert.equal(command, 'docker');
    assert.equal(args.includes('node:test'), true);
    calls.push(args[0]);
    if (args[0] === 'pull') {
      available = true;
      return null;
    }
    if (!available) throw new Error('No such image');
    return 'node@sha256:registry-digest\n';
  }, 'node:test');
  assert.equal(identity, 'node@sha256:registry-digest');
  assert.deepEqual(calls, ['image', 'pull', 'image']);
});

test('a failed image pull aborts verification', () => {
  const calls = [];
  assert.throws(() => resolveImage((command, args) => {
    calls.push(args[0]);
    throw new Error(args[0] === 'pull' ? 'pull denied' : 'No such image');
  }, 'missing:test'), /pull denied/);
  assert.deepEqual(calls, ['image', 'pull']);
});

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

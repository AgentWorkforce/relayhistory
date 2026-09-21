import assert from 'node:assert/strict';
import { existsSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';

import {
  installWithRegistryRetry,
  isRegistryVisibilityFailure,
} from './npm-install-with-registry-retry.mjs';

const quiet = {
  emit: () => {},
  log: () => {},
  reset: () => {},
};

test('recognizes only npm registry visibility errors as retryable', () => {
  assert.equal(isRegistryVisibilityFailure('npm error code ETARGET'), true);
  assert.equal(isRegistryVisibilityFailure('npm error code E404'), true);
  assert.equal(isRegistryVisibilityFailure('npm error code EACCES'), false);
  assert.equal(isRegistryVisibilityFailure('network timeout'), false);
});

test('retries a registry miss until the exact release becomes installable', async () => {
  const results = [
    { status: 1, stdout: '', stderr: 'npm error code ETARGET' },
    { status: 1, stdout: '', stderr: 'npm error code E404' },
    { status: 0, stdout: 'installed', stderr: '' },
  ];
  const waits = [];
  let resets = 0;

  await installWithRegistryRetry(['ai-hist@0.9.0'], {
    ...quiet,
    attempts: 5,
    delayMs: 7,
    runInstall: () => results.shift(),
    sleep: async (ms) => waits.push(ms),
    reset: () => { resets += 1; },
  });

  assert.deepEqual(waits, [7, 7]);
  assert.equal(resets, 2);
  assert.equal(results.length, 0);
});

test('fails immediately for authentication and other non-propagation errors', async () => {
  let calls = 0;
  await assert.rejects(
    installWithRegistryRetry(['ai-hist@0.9.0'], {
      ...quiet,
      attempts: 5,
      runInstall: () => {
        calls += 1;
        return { status: 7, stdout: '', stderr: 'npm error code E401' };
      },
      sleep: async () => assert.fail('non-retryable failures must not sleep'),
    }),
    (error) => error.exitCode === 7 && /non-registry-visibility/.test(error.message),
  );
  assert.equal(calls, 1);
});

test('fails after the bounded number of registry visibility attempts', async () => {
  let calls = 0;
  await assert.rejects(
    installWithRegistryRetry(['ai-hist@0.9.0'], {
      ...quiet,
      attempts: 3,
      delayMs: 1,
      runInstall: () => {
        calls += 1;
        return { status: 1, stdout: '', stderr: 'npm error code ETARGET' };
      },
      sleep: async () => {},
    }),
    /after 3 attempts/,
  );
  assert.equal(calls, 3);
});

test('retries clear node_modules under options.cwd, not process.cwd', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'ai-hist-retry-cwd-'));
  mkdirSync(join(dir, 'node_modules'));
  writeFileSync(join(dir, 'package-lock.json'), '{}\n');
  try {
    let calls = 0;
    await installWithRegistryRetry(['--prefix', dir, 'pkg'], {
      attempts: 2,
      delayMs: 1,
      cwd: dir,
      emit: () => {},
      log: () => {},
      sleep: async () => {},
      runInstall: () => {
        calls += 1;
        return calls === 1
          ? { status: 1, stdout: '', stderr: 'npm error code ETARGET' }
          : { status: 0, stdout: '', stderr: '' };
      },
    });
    assert.equal(calls, 2);
    assert.equal(existsSync(join(dir, 'node_modules')), false);
    assert.equal(existsSync(join(dir, 'package-lock.json')), false);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

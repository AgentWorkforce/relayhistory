import assert from 'node:assert/strict';
import test from 'node:test';

import {
  REGISTRY_RELEASE_PACKAGES,
  waitForRegistryPackages,
} from './registry-clean-install-smoke.mjs';

test('release package list covers the public npm family', () => {
  assert.ok(REGISTRY_RELEASE_PACKAGES.includes('ai-hist'));
  assert.ok(REGISTRY_RELEASE_PACKAGES.includes('ai-hist-mcp'));
  assert.ok(REGISTRY_RELEASE_PACKAGES.includes('ai-hist-native-linux-x64-gnu'));
  assert.equal(REGISTRY_RELEASE_PACKAGES.length, 10);
});

test('waitForRegistryPackages retries until every package resolves', async () => {
  const seen = new Map(REGISTRY_RELEASE_PACKAGES.map((pkg) => [`${pkg}@0.15.7`, 0]));
  const waits = [];
  await waitForRegistryPackages('0.15.7', {
    attempts: 4,
    delayMs: 3,
    sleep: async (ms) => waits.push(ms),
    log: () => {},
    view: (spec) => {
      const count = (seen.get(spec) ?? 0) + 1;
      seen.set(spec, count);
      if (spec === 'ai-hist-native-linux-x64-gnu@0.15.7' && count < 3) return null;
      return '0.15.7';
    },
  });
  assert.deepEqual(waits, [3, 3]);
});

test('waitForRegistryPackages fails with the remaining package names', async () => {
  await assert.rejects(
    waitForRegistryPackages('0.15.8', {
      attempts: 2,
      delayMs: 1,
      sleep: async () => {},
      log: () => {},
      view: (spec) => (spec === 'ai-hist@0.15.8' ? '0.15.8' : null),
    }),
    (error) => error.missing.length === REGISTRY_RELEASE_PACKAGES.length - 1
      && error.missing.includes('ai-hist-mcp'),
  );
});

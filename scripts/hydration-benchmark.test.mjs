import assert from 'node:assert/strict';
import test from 'node:test';
import { durationSummary, selectHydrationSessions } from './hydration-benchmark-lib.mjs';

function session(source, sessionId, discoveryState = 'shallow', rawPath = `/tmp/${sessionId}`) {
  return { source, sessionId, discoveryState, rawPath, locations: ['local'] };
}

test('selection prefers shallow sessions and rotates across providers', () => {
  const selected = selectHydrationSessions([
    session('claude', 'c1'),
    session('claude', 'c2'),
    session('codex', 'x1'),
    session('grok', 'g1', 'full'),
    { ...session('cursor', 'remote'), locations: ['remote'] },
  ], 4);
  assert.deepEqual(selected.map(({ source, sessionId }) => `${source}:${sessionId}`), [
    'claude:c1', 'codex:x1', 'claude:c2', 'grok:g1',
  ]);
});

test('duration summary uses nearest-rank percentiles', () => {
  assert.deepEqual(durationSummary([5, 1, 3, 2, 4]), {
    samples: 5, minMs: 1, meanMs: 3, p50Ms: 3, p95Ms: 5, maxMs: 5,
  });
  assert.equal(durationSummary([]), null);
});

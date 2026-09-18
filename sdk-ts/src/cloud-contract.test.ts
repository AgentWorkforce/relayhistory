import assert from 'node:assert/strict';
import test from 'node:test';

import { createRelayhistoryCloudClient } from '@relayhistory/cloud-client';

import type { RelayhistoryCloudClient } from './cloud-contract.js';

/**
 * `ai-hist` declares the cloud client's shape rather than importing it, so the
 * published package carries no runtime dependency on `@relayhistory/cloud-client`.
 * The cost of that is a second declaration, and this is what stops it drifting:
 * if the real client stops satisfying the declared shape, this file fails to
 * compile and `npm run build` fails with it.
 *
 * The dependency is devDependencies-only and deliberately type-only — nothing in
 * `src/relay-cli.ts` or `src/cloud-contract.ts` imports it.
 */
type RealClient = ReturnType<typeof createRelayhistoryCloudClient>;

// The assignment is the assertion: a structural mismatch is a compile error.
const _satisfiesDeclaredShape: RelayhistoryCloudClient = null as unknown as RealClient;
void _satisfiesDeclaredShape;

test('the real cloud client satisfies the shape ai-hist declares', () => {
  // Compile-time already proved it; this keeps the file in the run and makes
  // the guarantee visible in the test output rather than only in a build log.
  assert.ok(typeof createRelayhistoryCloudClient === 'function');
});

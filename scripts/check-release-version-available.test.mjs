import assert from 'node:assert/strict';
import test from 'node:test';

import { checkReleaseVersionAvailable } from './check-release-version-available.mjs';

test('release preflight rejects a version occupied after the build began', () => {
  assert.throws(
    () => checkReleaseVersionAvailable('0.32.0', (spec) => (
      spec === 'ai-hist-native-darwin-arm64@0.32.0' ? '0.32.0' : null
    )),
    /0\.32\.0 is already visible on npm for: ai-hist-native-darwin-arm64/,
  );
});

test('release preflight fails closed when npm cannot answer', () => {
  assert.throws(
    () => checkReleaseVersionAvailable('0.32.1', () => { throw new Error('registry unavailable'); }),
    /registry unavailable/,
  );
});

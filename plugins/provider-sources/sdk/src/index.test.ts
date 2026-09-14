import assert from 'node:assert/strict';
import test from 'node:test';
import { HistoryPluginRegistry, InvalidArgumentError } from 'ai-hist';
import { createHistoryPlugin } from './index.js';
import { helperRequest } from './helper.js';
test('provider package registration is inert and connector IDs are selected explicitly', () => {
  const registry = new HistoryPluginRegistry();
  registry.register(createHistoryPlugin({ binaryPath: '/fixture/must-not-run' }));
  assert.deepEqual(
    registry.sourceConnectors(['claude-web']).map((source) => source.id),
    ['claude-web'],
  );
  assert.deepEqual(registry.sourceConnectors([]), []);
  assert.throws(
    () => createHistoryPlugin({ connectors: ['claude-web', 'claude-web'] }),
    InvalidArgumentError,
  );
});
test('actual provider helper rejects invalid selector before provider credential access', async () => {
  await assert.rejects(
    helperRequest('discover', { connectorId: 'fixture-not-a-provider' }),
    (error: unknown) => error instanceof InvalidArgumentError,
  );
});

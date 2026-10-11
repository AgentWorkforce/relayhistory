import assert from 'node:assert/strict';
import test from 'node:test';
import { HISTORY_ENV_PATTERN, scrubHistoryEnv } from './test-env.js';

test('history isolation removes pilot roots and pins Hermes without clearing system app data', () => {
  const names = ['PI_CODING_AGENT_DIR', 'PI_CODING_AGENT_SESSION_DIR', 'HERMES_HOME', 'HERMES_DATA_DIR_SUFFIX'];
  const saved = { ...process.env };
  try {
    for (const name of names) { assert.ok(HISTORY_ENV_PATTERN.test(name)); process.env[name] = '/operator/history'; }
    process.env.LOCALAPPDATA = '/system/application-data';
    const restore = scrubHistoryEnv();
    assert.equal(process.env.PI_CODING_AGENT_DIR, undefined);
    assert.equal(process.env.PI_CODING_AGENT_SESSION_DIR, undefined);
    assert.equal(process.env.HERMES_DATA_DIR_SUFFIX, undefined);
    assert.notEqual(process.env.HERMES_HOME, '/operator/history');
    assert.equal(process.env.LOCALAPPDATA, '/system/application-data');
    restore();
    for (const name of names) assert.equal(process.env[name], '/operator/history');
  } finally {
    for (const name of Object.keys(process.env)) if (!(name in saved)) delete process.env[name];
    Object.assign(process.env, saved);
  }
});

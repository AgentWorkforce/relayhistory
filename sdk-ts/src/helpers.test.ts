import assert from 'node:assert/strict';
import { join } from 'node:path';
import test from 'node:test';

import {
  CATALOG_SOURCES,
  SOURCES,
  defaultDbPath,
  isCatalogSource,
  isSource,
  resumeCommand,
} from './index.js';

test('exports immutable, aligned runtime source registries and matching type guards', () => {
  assert.deepEqual(SOURCES, ['claude', 'codex', 'cursor', 'grok', 'relay', 'trajectory', 'opencode', 'devin']);
  assert.deepEqual(CATALOG_SOURCES, SOURCES.filter((source) => source !== 'trajectory'));
  assert.equal(Object.isFrozen(SOURCES), true);
  assert.equal(Object.isFrozen(CATALOG_SOURCES), true);

  for (const source of SOURCES) assert.equal(isSource(source), true);
  for (const source of CATALOG_SOURCES) assert.equal(isCatalogSource(source), true);
  assert.equal(isCatalogSource('trajectory'), false);
  for (const value of ['unknown', '', null, undefined, 1, {}]) {
    assert.equal(isSource(value), false);
    assert.equal(isCatalogSource(value), false);
  }
});

test('defaultDbPath follows native environment precedence', () => {
  const previousDb = process.env.AI_HIST_DB;
  const previousXdg = process.env.XDG_DATA_HOME;
  try {
    delete process.env.AI_HIST_DB;
    process.env.XDG_DATA_HOME = '/tmp/relayhistory-xdg';
    assert.equal(defaultDbPath(), join('/tmp/relayhistory-xdg', 'ai-hist', 'ai-history.db'));
    process.env.AI_HIST_DB = '/tmp/relayhistory-explicit.db';
    assert.equal(defaultDbPath(), '/tmp/relayhistory-explicit.db');
  } finally {
    if (previousDb === undefined) delete process.env.AI_HIST_DB;
    else process.env.AI_HIST_DB = previousDb;
    if (previousXdg === undefined) delete process.env.XDG_DATA_HOME;
    else process.env.XDG_DATA_HOME = previousXdg;
  }
});

test('resumeCommand preserves source and project-aware commands', () => {
  assert.equal(resumeCommand({ source: 'claude', sessionId: 's1', project: '/work/app', locations: [] }), 'cd /work/app && claude --resume s1');
  assert.equal(resumeCommand({ source: 'codex', sessionId: 's2', project: null, locations: ['local'] }), 'codex resume s2');
  assert.equal(resumeCommand({ source: 'cursor', sessionId: 's3', project: '/work/app', locations: ['local', 'remote'] }), 'cd /work/app && cursor-agent --resume=s3');
  assert.equal(resumeCommand({ source: 'grok', sessionId: 's4', project: null, locations: ['remote'] }), null);
  assert.equal(resumeCommand({ source: 'relay', sessionId: 's5', project: null, locations: ['local'] }), null);
});

import assert from 'node:assert/strict';
import test from 'node:test';
import { formatSessionRow, InvalidArgumentError, type CatalogSession } from './index.js';

const session: CatalogSession = {
  source: 'claude', sessionId: 'session-1', cwd: '/work/demo', gitBranch: null,
  firstActivityMs: null, lastActivityMs: 0, firstPrompt: 'Fix the search', lastAssistantText: null,
  models: [], originator: null, agentVersion: null, repoUrl: null, initialCommit: null,
  workspaceRoots: [], rawPath: null, sourceStamp: null, discoveryState: 'shallow', fromCache: true,
  locations: ['local'],
};

test('pretty SDK rows expose source, icon, age, identity, location and prompt', () => {
  assert.equal(formatSessionRow(session, { nowMs: 120_000 }),
    '✦ [claude] 2m ago [local]  session-1  /work/demo  Fix the search');
  assert.match(formatSessionRow(session, { nowMs: 120_000, color: true }), /\x1b\[36m✦ \[claude\]\x1b\[0m/);
  assert.doesNotMatch(formatSessionRow(session), /\x1b/);
});

test('pretty rows handle absent/future dates and untrusted multiline provider text', () => {
  assert.match(formatSessionRow({ ...session, lastActivityMs: null }), /unknown age/);
  assert.match(formatSessionRow({ ...session, lastActivityMs: 2000 }, { nowMs: 1000 }), /just now/);
  const text = formatSessionRow({ ...session, firstPrompt: 'hello\n\x1b[31mthere\tfriend', cwd: '/tmp\nfolder' });
  assert.doesNotMatch(text, /[\x00-\x1f\x7f-\x9f]/);
  assert.match(text, /hello \[31mthere friend/);
  assert.match(formatSessionRow({ ...session, firstPrompt: '😀😀😀' }, { maxPromptLength: 2 }), /😀😀…$/);
  assert.throws(() => formatSessionRow(session, { maxPromptLength: 0 }), InvalidArgumentError);
});

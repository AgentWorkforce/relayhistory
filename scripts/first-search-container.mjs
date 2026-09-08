// Invoked inside a fresh container by verify-first-search.mjs.
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { existsSync, mkdirSync, writeFileSync } from 'node:fs';
import { performance } from 'node:perf_hooks';

const mode = process.argv[2];
const home = '/tmp/first-search-home';
const env = { ...process.env, HOME: home, USERPROFILE: home, AI_HIST_DB: `${home}/history.db`,
  npm_config_cache: `${home}/.npm`, npm_config_update_notifier: 'false', RELAYHISTORY_NO_UPDATE_CHECK: '1' };
assert.equal(existsSync(home), false, 'home, npm cache and database must start absent');
mkdirSync(`${home}/.claude/projects/demo`, { recursive: true });
const prompt = 'find the zero friction needle';
writeFileSync(`${home}/.claude/projects/demo/first.jsonl`, JSON.stringify({
  sessionId: 'first-search-fixture', uuid: 'user-1', cwd: '/work/demo', type: 'user',
  timestamp: '2026-09-08T10:00:00Z', message: { role: 'user', content: prompt },
}) + '\n');
const artifact = mode === 'before' ? 'ai-hist@0.14.3' : process.env.AI_HIST_TARBALL;
const start = performance.now();
const frames = [{ version: 2, width: 110, height: 28, timestamp: Math.floor(Date.now() / 1000),
  title: `ai-hist ${mode}: actual clean-container execution`, env: { TERM: 'xterm-256color', SHELL: '/bin/sh' } }];
const commands = [];
function execute(args, expected = 0) {
  const displayed = `npx --yes ${artifact} ${args.join(' ')}`.trim();
  frames.push([(performance.now() - start) / 1000, 'o', `$ ${displayed}\r\n`]);
  const result = spawnSync('npx', ['--yes', artifact, ...args], { env, encoding: 'utf8', timeout: 90_000 });
  commands.push({ command: displayed, exitCode: result.status });
  frames.push([(performance.now() - start) / 1000, 'o', (result.stdout + result.stderr).replace(/\r?\n/g, '\r\n')]);
  assert.equal(result.status, expected, result.stderr);
  return result.stdout;
}
try {
  if (mode === 'before') {
    execute([], 2);
    execute(['sessions', 'discover']);
    execute(['sessions', 'hydrate', 'claude', 'first-search-fixture']);
  } else {
    assert.match(execute([]), /Ready: 1 indexed prompt/);
  }
  const results = JSON.parse(execute(['search', 'zero friction needle', '--json']));
  assert.equal(results[0]?.session_id, 'first-search-fixture');
  assert.equal(results[0]?.prompt, prompt);
  const elapsedMs = Math.round(performance.now() - start);
  const result = { mode, elapsedMs, under30Seconds: elapsedMs < 30_000, node: process.version,
    platform: `${process.platform}-${process.arch}`, fixtureSessions: 1,
    emptyNpmCache: true, emptyDatabase: true, commands, matchedSession: results[0].session_id };
  writeFileSync(`/evidence/${mode}.json`, JSON.stringify(result, null, 2) + '\n');
  console.log(JSON.stringify(result));
  if (mode === 'after') assert.ok(result.under30Seconds, `First search took ${elapsedMs}ms (budget 30000ms)`);
} finally {
  writeFileSync(`/evidence/${mode}.cast`, frames.map((frame) => JSON.stringify(frame)).join('\n') + '\n');
}

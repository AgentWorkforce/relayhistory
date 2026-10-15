import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {fileURLToPath} from 'node:url';
import {replay, durableProjection} from './replay.mjs';
const root = fileURLToPath(new URL('.', import.meta.url));
const oracles = JSON.parse(fs.readFileSync(path.join(root, 'official-reader.json')));
const manifest = JSON.parse(fs.readFileSync(path.join(root, 'manifest.json')));
const entries = new Map(manifest.cases.map(entry => [entry.name, entry]));
const text = name => fs.readFileSync(path.join(root, entries.get(name).path), 'utf8');
const parsed = name => replay(text(name), {legacy: entries.get(name).format === 'legacy-json'}).conversation;

test('all fixture bytes match provenance and the pinned official reader projection', () => {
  for (const entry of manifest.cases) {
    const bytes = fs.readFileSync(path.join(root, entry.path));
    assert.equal(crypto.createHash('sha256').update(bytes).digest('hex'), entry.sha256, entry.name);
    assert.deepEqual(durableProjection(parsed(entry.name)), durableProjection(oracles[entry.name]), entry.name);
  }
});

test('full UUID metadata distinguishes colliding eight-character filename prefixes', () => {
  assert.equal(parsed('ordinary').sessionId.slice(0, 8), parsed('short-prefix-collision').sessionId.slice(0, 8));
  assert.notEqual(parsed('ordinary').sessionId, parsed('short-prefix-collision').sessionId);
  assert.match(entries.get('short-prefix-collision').path, /-1-11111111\.jsonl$/);
});

test('duplicate message revisions collapse by id while tools, thoughts and response usage remain recorded', () => {
  const raw = text('ordinary').split('\n').filter(Boolean).map(JSON.parse);
  assert(raw.filter(record => record.id === 'assistant-one').length > 1);
  const message = parsed('ordinary').messages.find(message => message.id === 'assistant-one');
  assert.equal(message.toolCalls[0].id, 'call-real-id');
  assert.equal(message.toolCalls[0].status, 'success');
  assert.equal(message.toolCalls[0].result[0].functionResponse.id, message.toolCalls[0].id);
  assert.equal(message.thoughts[0].description, 'Inspect the synthetic fixture');
  assert.deepEqual(message.tokens, {input: 120, output: 20, cached: 30, thoughts: 4, tool: 2, total: 146});
  assert.equal(parsed('ordinary').messages.find(message => message.id === 'assistant-two').tokens, null);
});

test('masked tool results replay actual $patch updates by tool id', () => {
  assert.match(text('tool-result-patched'), /"\$patch"/);
  const tool = parsed('tool-result-patched').messages.find(message => message.id === 'assistant-one').toolCalls[0];
  assert.equal(tool.result[0].functionResponse.response.output, 'synthetic masked result');
});

test('resume appends to the same full native session instead of starting a new identity', () => {
  const original = parsed('ordinary');
  const resumed = parsed('resumed');
  assert.equal(original.sessionId, resumed.sessionId);
  assert.equal(original.startTime, resumed.startTime);
  assert(resumed.messages.some(message => message.id === 'user-one'));
  assert(resumed.messages.some(message => message.id === 'user-resumed'));
  assert(text('resumed').startsWith(text('ordinary')));
});

test('child folder carries complete parent id and child kind, without native relationship metadata', () => {
  assert(entries.get('child').path.includes('/11111111-1111-4111-8111-111111111111/'));
  const child = parsed('child');
  assert.equal(child.sessionId, '33333333-3333-4333-8333-333333333333');
  assert.equal(child.kind, 'subagent');
  assert.deepEqual(child.directories, ['/synthetic/gemini-project']);
  assert(!('parentSessionId' in child));
});

test('compaction-shaped remove/order/content patches finalize logical history while raw older turns survive', () => {
  const compacted = parsed('compacted');
  assert.deepEqual(compacted.messages.map(message => message.id), ['summary-control', 'assistant-resumed']);
  assert.match(text('compacted'), /Find the synthetic Gemini needle/);
  assert.match(text('compacted'), /"removeIds"/);
  assert.match(text('compacted'), /"orderIds"/);
  assert.equal(compacted.messages[1].content[0].text, 'Response after synthetic compaction.');
  assert.equal(compacted.summary, 'Synthetic compacted session');
});

test('rewind removes the target and trailing logical turns without erasing journal evidence', () => {
  assert.deepEqual(parsed('rewound').messages, parsed('compacted').messages);
  assert.match(text('rewound'), /"\$rewindTo":"user-undone"/);
  assert.match(text('rewound'), /This synthetic turn will be undone/);
});

test('legacy JSON resumes through actual writer migration to JSONL with the same UUID', () => {
  assert.equal(parsed('legacy').sessionId, parsed('legacy-migrated').sessionId);
  assert(parsed('legacy-migrated').messages.some(message => message.id === 'legacy-resumed'));
  assert(text('legacy-migrated').split('\n').every(line => !line.trim() || JSON.parse(line)));
});

test('partial final record is deferred, while malformed complete records and missing metadata fail', () => {
  const valid = text('resumed');
  const partial = replay(valid + '{"id":"incomplete', {allowPartialTail: true});
  assert.equal(partial.deferredTail, true);
  assert.deepEqual(partial.conversation, parsed('resumed'));
  assert.throws(() => replay(valid + '{"id":"broken\n', {allowPartialTail: true}), /Malformed complete/);
  assert.throws(() => replay('{"id":"m","type":"user","content":"orphan","timestamp":"2026-10-11T12:00:00Z"}\n'), /Missing full session identity/);
});

test('pinned root oracle applies home override before seatbelt runtime routing and uses registry slug', () => {
  const roots = JSON.parse(fs.readFileSync(path.join(root, 'root-oracle.json')));
  assert.equal(roots.default.runtime, '/synthetic/native-home/.gemini');
  assert.equal(roots['custom-home'].runtime, '/synthetic/custom-home/.gemini');
  assert.equal(roots.seatbelt.runtime, '/synthetic/native-home/.cache/.gemini');
  assert.equal(roots['custom-seatbelt'].runtime, '/synthetic/custom-home/.cache/.gemini');
  assert.equal(roots['other-sandbox'].runtime, '/synthetic/native-home/.gemini');
  for (const oracle of Object.values(roots)) assert.equal(oracle.project, `${oracle.runtime}/tmp/gemini-project`);
});

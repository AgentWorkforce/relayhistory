import { test } from 'node:test';
import assert from 'node:assert/strict';
import { factoryEvidence, factoryMessagePage, factorySessionId } from './factory-source.js';
const sid = '4f8a2220-4e95-484b-8a2d-62604fb2be1a';
const message = { id: 'message-1', role: 'assistant', createdAt: 1794067200000, updatedAt: 1794067201000, content: [{ id: 'block-1', type: 'text', text: 'Factory reply' }], future: { keep: true } };
test('Factory API keeps native IDs, epoch timestamps, updates and raw fields', () => {
  const [row] = factoryEvidence(sid, [message]);
  assert.equal(row.source, 'droid');
  assert.equal(row.sessionId, sid);
  assert.equal(row.kind, 'assistant');
  assert.equal(row.content, 'Factory reply');
  assert.equal(row.ts, new Date(message.createdAt).toISOString());
  assert.deepEqual(row.record.blockIds, ['block-1']);
  assert.deepEqual(row.record.raw, message);
  assert.equal(row.record.updatedAt, message.updatedAt);
  assert.equal(row.record.messageVersion, null);
  assert.equal(factoryEvidence(sid, [{ ...message, updatedAt: message.updatedAt + 100 }])[0].eventId, row.eventId);
});
test('API tool_use and camelCase tool results retain call identity and explicit outcomes', () => {
  const rows = factoryEvidence(sid, [
    { ...message, content: [{ type: 'tool_use', id: 'call-1', name: 'Read', input: { file_path: '/fixture/a.txt' } }] },
    { ...message, id: 'result-1', role: 'user', content: [{ type: 'tool_result', toolUseId: 'call-1', content: [{ type: 'text', text: 'File missing' }], isError: true, modelContent: 'Keep this raw' }] },
  ]);
  assert.equal(rows[0].kind, 'tool_call');
  assert.deepEqual(rows[0].toolCalls, [{ id: 'call-1', tool: 'Read', input: { file_path: '/fixture/a.txt' } }]);
  assert.equal(rows[1].kind, 'tool_call');
  assert.equal(rows[1].actorRole, 'tool');
  assert.equal(rows[1].content, 'File missing');
  assert.equal(rows[1].record.toolResults?.[0].status, 'errored');
  assert.deepEqual(rows[1].record.toolResults?.[0].error, [{ type: 'text', text: 'File missing' }]);
  assert.equal((rows[1].record.raw.content as Array<Record<string, unknown>>)[0].modelContent, 'Keep this raw');
});
test('mixed human prose and tool outcomes have separate stable evidence identities', () => {
  const raw = { ...message, role: 'user', content: [
    { type: 'tool_result', toolUseId: 'call-1', isError: false, content: 'Tool output' },
    { type: 'text', text: 'Human follow-up' },
  ] };
  const [prompt, result] = factoryEvidence(sid, [raw]);
  assert.equal(prompt.kind, 'prompt');
  assert.equal(prompt.content, 'Human follow-up');
  assert.equal(result.kind, 'tool_call');
  assert.equal(result.content, 'Tool output');
  assert.notEqual(prompt.eventId, result.eventId);
  assert.deepEqual(factoryEvidence(sid, [{ ...raw, updatedAt: message.updatedAt + 5 }]).map(r => r.eventId), [prompt.eventId, result.eventId]);
  assert.equal(result.record.toolResults?.[0].status, 'completed');
});
test('opaque content, thinking, system and future roles are markers with raw retained', () => {
  for (const raw of [
    { ...message, role: 'future', content: [{ type: 'text', text: 'Unknown actor' }] },
    { ...message, role: 'system', content: [{ type: 'text', text: 'System text' }] },
    { ...message, content: [{ type: 'thinking', thinking: 'Native thinking', signature: 'fixture-signature' }] },
    { ...message, content: [{ type: 'image', source: { type: 'base64', data: 'fixture' } }] },
  ]) {
    const [row] = factoryEvidence(sid, [raw]);
    assert.equal(row.kind, 'marker');
    assert.deepEqual(row.record.raw, raw);
  }
});
test('unknown mixed blocks do not become human prose or erase known results', () => {
  const raw = { ...message, role: 'user', content: [{ type: 'future', text: 'Not known prose' }, { type: 'tool_result', toolUseId: 'call-1', content: 'Known result' }] };
  const [row] = factoryEvidence(sid, [raw]);
  assert.equal(row.kind, 'marker');
  assert.equal(row.content, 'Known result');
  assert.equal(row.record.toolResults?.[0].status, null);
});
test('rejects missing IDs, duplicate message/block IDs and ambiguous dates', () => {
  for (const raw of [
    { ...message, id: '' }, { ...message, id: 1 }, { ...message, id: 'bad\nidentity' },
    { ...message, createdAt: undefined }, { ...message, createdAt: '2026-11-07T00:00:00Z' },
    { ...message, createdAt: Infinity }, { ...message, createdAt: -1 },
    { ...message, updatedAt: null }, { ...message, content: 'not the API schema' },
    { ...message, content: [message.content[0], message.content[0]] },
    { ...message, content: [{ type: 'tool_use', id: 'call-1' }] },
    { ...message, role: 'user', content: [{ type: 'tool_result', tool_use_id: 'wrong-casing', is_error: true }] },
    { ...message, role: 'user', content: [{ type: 'tool_result', toolUseId: 'call-1', isError: 'true' }] },
  ]) assert.throws(() => factoryEvidence(sid, [raw]));
  assert.throws(() => factoryEvidence(sid, [message, message]));
  for (const id of ['../elsewhere', 'https://app.factory.ai/session/example', 'id?query', ' ', 'a/b']) assert.throws(() => factorySessionId(id));
});
test('encoded native message IDs cannot collide with event namespace delimiters', () => {
  const rows = factoryEvidence(sid, [{ ...message, id: 'a:b' }, { ...message, id: 'a%3Ab' }]);
  assert.notEqual(rows[0].eventId, rows[1].eventId);
});
test('page envelopes enforce explicit pagination; hosts aggregate all pages', () => {
  const first = factoryMessagePage({ messages: [message], pagination: { hasMore: true, nextCursor: 'opaque-cursor' } });
  const second = factoryMessagePage({ messages: [{ ...message, id: 'message-2' }], pagination: { hasMore: false, nextCursor: null } });
  assert.equal(first.nextCursor, 'opaque-cursor');
  assert.equal(second.nextCursor, null);
  assert.equal(factoryEvidence(sid, [...first.messages, ...second.messages]).length, 2);
  for (const value of [{}, { messages: [], pagination: {} }, { messages: [], pagination: { hasMore: true, nextCursor: null } }, { messages: [], pagination: { hasMore: false, nextCursor: 'stale' } }, { messages: [null], pagination: { hasMore: false, nextCursor: null } }]) assert.throws(() => factoryMessagePage(value));
});

test('canonical API blocks reproduce native local event/tool keys and measured facts', async () => {
  const { readFileSync } = await import('node:fs');
  const { createHash } = await import('node:crypto');
  const { factoryCanonicalEvidence } = await import('./factory-source.js');
  const base = new URL('../../crates/ai-hist/tests/', import.meta.url);
  const native = readFileSync(new URL('fixtures/droid/v2-synthetic/.factory/sessions/-fixture/factory-fixture.jsonl', base), 'utf8').trim().split('\n').map(line => JSON.parse(line));
  const api = native.filter(row => row.type === 'message').map(row => ({
    ...row.message, id: row.id, createdAt: Date.parse(row.timestamp), updatedAt: Date.parse(row.timestamp), ...(row.parentId ? { parentId: row.parentId } : {}),
    content: row.message.content.map((block: Record<string, unknown>) => block.type === 'tool_result' ? Object.fromEntries(Object.entries(block).map(([key, value]) => [key === 'tool_use_id' ? 'toolUseId' : key === 'is_error' ? 'isError' : key, value])) : block),
  }));
  const expected = JSON.parse(readFileSync(new URL('snapshots/droid/v2-synthetic.json', base), 'utf8'));
  const records = await factoryCanonicalEvidence('factory-fixture', api);
  for (const [kind, rows, column] of [
    ['session_event', expected.session_events, 'event_uid'],
    ['tool_call', expected.tool_calls, 'tool_use_id'],
  ] as const) {
    const actual = records.filter(row => row.kind === kind);
    assert.equal(actual.length, rows.length);
    for (const local of rows) {
      const row = actual.find(row => row.payload[column] === local[column]);
      assert.ok(row, `native row ${local[column]} must exist through the API`);
      const key = [kind, 'droid', 'factory-fixture', local[column]];
      assert.deepEqual(row.key, key);
      assert.equal(row.record_id, createHash('sha256').update(JSON.stringify(key)).digest('hex'));
      // Cwd/project attribution comes from the local header and is not exposed by
      // the API message response. Every other observed native fact must exist.
      const localOnly = new Set(['cwd', 'project', 'git_branch', 'project_key', 'project_key_method']);
      for (const [field, value] of Object.entries(local)) if (value !== null && !localOnly.has(field)) assert.deepEqual(row.payload[field], value, `${local[column]}.${field}`);
    }
  }
  assert.ok(records.some(row => row.kind === 'session_marker' && row.payload.marker_uid === 'block:a3:0'));
  assert.equal(records.filter(row => row.payload.kind === 'factory_api_message').length, api.length);
});
test('canonical role tool never becomes a human prompt or unbound native text event', async () => {
  const { factoryCanonicalEvidence } = await import('./factory-source.js');
  const records = await factoryCanonicalEvidence(sid, [{ ...message, role: 'tool', content: [
    { type: 'text', text: 'Unbound tool text stays raw' },
    { type: 'tool_result', toolUseId: 'call-1', isError: false, content: 'Bound output' },
  ] }]);
  const events = records.filter(row => row.kind === 'session_event');
  assert.equal(events.length, 1);
  assert.equal(events[0].payload.role, 'tool_result');
  assert.equal(events[0].payload.tool_use_id, 'call-1');
  assert.ok(records.some(row => row.payload.kind === 'opaque_block'));
});

test('explicit native hook and model-only context is never projected as a human turn', async () => {
  const { factoryCanonicalEvidence } = await import('./factory-source.js');
  for (const extra of [{ visibility: 'llm_only' }, { visibility: 'user_only' }, { hookEventName: 'UserPromptSubmit' }]) {
    const raw = { ...message, ...extra, role: 'user', content: [{ type: 'text', text: 'Runtime context, not a human prompt' }] };
    assert.equal(factoryEvidence(sid, [raw])[0].kind, 'marker');
    const canonical = await factoryCanonicalEvidence(sid, [raw]);
    assert.equal(canonical.filter(row => row.kind === 'session_event').length, 0);
    assert.deepEqual(JSON.parse(String(canonical[0].payload.payload_json)), raw);
  }
});

test('readable tool activity requires a bound result and never absorbs unbound text', () => {
  const text = { type: 'text', text: 'Unbound output' };
  const result = { type: 'tool_result', toolUseId: 'call-1', isError: false, content: 'Bound output' };
  const raw = { ...message, role: 'tool', content: [text] };
  const [marker] = factoryEvidence(sid, [raw]);
  assert.equal(marker.kind, 'marker');
  assert.equal(marker.content, 'Unbound output');
  assert.equal(marker.record.toolResults, undefined);
  for (const extra of [text, { type: 'future-block' }]) {
    const [context, output] = factoryEvidence(sid, [{ ...raw, content: [extra, result] }]);
    assert.equal(context.kind, 'marker');
    assert.equal(context.record.toolResults, undefined);
    assert.equal(output.kind, 'tool_call');
    assert.equal(output.content, 'Bound output');
    assert.equal(output.record.toolResults?.[0].id, 'call-1');
    assert.notEqual(context.eventId, output.eventId);
  }
});

test('canonical markers obey native intake bounds without truncating text events or separate raw capture', async (t) => {
  const { factoryCanonicalEvidence } = await import('./factory-source.js');
  const { HistoryPluginRegistry, discoverSourcePlugins, hydrateSourcePlugin, getSessionEventsPage } = await import('./index.js');
  const { mkdtemp, rm } = await import('node:fs/promises');
  const { tmpdir } = await import('node:os');
  const { join } = await import('node:path');
  const dir = await mkdtemp(join(tmpdir(), 'factory-canonical-intake-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const longText = '界'.repeat(300);
  const raw = { ...message, content: [
    { type: 'text', text: longText },
    { type: 'future', payload: { ['k'.repeat(200)]: Array.from({ length: 40 }, () => 'long'.repeat(100)), values: Object.fromEntries(Array.from({ length: 40 }, (_, i) => [String(i), i])) } },
  ] };
  const records = await factoryCanonicalEvidence(sid, [raw]);
  assert.deepEqual(records.find(row => row.raw)?.raw, raw);
  const bounded = (value: unknown): void => {
    if (typeof value === 'string') assert.ok(Array.from(value).length <= 128);
    if (Array.isArray(value)) { assert.ok(value.length <= 32); value.forEach(bounded); }
    else if (value && typeof value === 'object') {
      assert.ok(Object.keys(value).length <= 32);
      for (const [key, part] of Object.entries(value)) { assert.ok(Array.from(key).length <= 128); bounded(part); }
    }
  };
  records.filter(row => row.kind === 'session_marker').forEach(row => bounded(JSON.parse(String(row.payload.payload_json))));
  const source: import('./source-contracts.js').HistorySource = {
    id: 'factory-intake-fixture', instanceId: 'owned', location: 'remote', supportedSources: ['droid'],
    discover: async () => ({ observations: [{ source: 'droid', session_id: sid, raw_path: 'fixture:factory', source_stamp: 'fixture-v1' }] }),
    hydrate: async () => ({ source_stamp: 'fixture-v1', source_bytes: Buffer.byteLength(JSON.stringify(raw)), covered_kinds: ['session_event', 'session_marker', 'tool_call'], records: records.map(({ kind, payload }) => ({ kind, payload })) }),
  };
  const registry = new HistoryPluginRegistry();
  registry.register({ sources: [source] });
  const dbPath = join(dir, 'history.db');
  await discoverSourcePlugins(registry, { dbPath });
  await hydrateSourcePlugin(source, { source: 'droid', sessionId: sid }, { dbPath });
  const events = (await getSessionEventsPage(sid, { source: 'droid', dbPath })).events;
  assert.equal(events.length, 1);
  assert.equal(events[0].text, longText);
});

test('API-only tool metadata stays raw while activity text uses the shared native contract', async () => {
  const { factoryCanonicalEvidence } = await import('./factory-source.js');
  const block = { type: 'tool_use', id: 'call-1', name: 'Read', input: { file_path: '/fixture/a.txt' }, namespace: 'api-only', thoughtSignature: 'fixture-signature' };
  const rows = await factoryCanonicalEvidence(sid, [{ ...message, content: [block] }]);
  const event = rows.find(row => row.kind === 'session_event');
  assert.ok(event);
  assert.deepEqual(JSON.parse(String(event.payload.text)), { type: 'tool_use', id: 'call-1', name: 'Read', input: block.input });
  assert.equal(event.payload.raw_kind, 'tool_use');
  assert.deepEqual((rows.find(row => row.raw)?.raw?.content as unknown[])[0], block);
});

test('canonical Factory blocks retain numeric native order through source intake', async (t) => {
  const { factoryCanonicalEvidence } = await import('./factory-source.js');
  const { HistoryPluginRegistry, discoverSourcePlugins, hydrateSourcePlugin, getSessionEventsPage } = await import('./index.js');
  const { mkdtemp, rm } = await import('node:fs/promises');
  const { tmpdir } = await import('node:os');
  const { join } = await import('node:path');
  const dir = await mkdtemp(join(tmpdir(), 'factory-block-order-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const expected = Array.from({ length: 12 }, (_, index) => `Block ${index}`);
  const raw = { ...message, role: 'assistant', content: expected.map(text => ({ type: 'text', text })) };
  const records = await factoryCanonicalEvidence(sid, [raw]);
  const source: import('./source-contracts.js').HistorySource = {
    id: 'factory-order-fixture', instanceId: 'owned', location: 'remote', supportedSources: ['droid'],
    discover: async () => ({ observations: [{ source: 'droid', session_id: sid, raw_path: 'fixture:factory-order', source_stamp: 'order-v1' }] }),
    hydrate: async () => ({ source_stamp: 'order-v1', source_bytes: Buffer.byteLength(JSON.stringify(raw)), covered_kinds: ['session_event', 'session_marker', 'tool_call'], records: records.map(({ kind, payload }) => ({ kind, payload })) }),
  };
  const registry = new HistoryPluginRegistry();
  registry.register({ sources: [source] });
  const dbPath = join(dir, 'history.db');
  await discoverSourcePlugins(registry, { dbPath });
  await hydrateSourcePlugin(source, { source: 'droid', sessionId: sid }, { dbPath });
  const read = async () => (await getSessionEventsPage(sid, { source: 'droid', dbPath })).events;
  const events = await read();
  assert.deepEqual(events.map(event => event.text), expected);
  assert.deepEqual(events.map(event => event.eventUid), expected.map((_, index) => `${message.id}:${index}`));
  await hydrateSourcePlugin(source, { source: 'droid', sessionId: sid }, { dbPath });
  assert.deepEqual(await read(), events);
});

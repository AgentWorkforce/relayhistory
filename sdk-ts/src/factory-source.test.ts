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
      for (const [field, value] of Object.entries(row.payload)) if (Object.hasOwn(local, field)) assert.deepEqual(value, local[field], `${local[column]}.${field}`);
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

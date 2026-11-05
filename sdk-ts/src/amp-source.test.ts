import { test } from 'node:test';
import assert from 'node:assert/strict';
import { ampEvidence, ampExport, ampThreadId } from './amp-source.js';
const tid = 'T-fixture-thread';
const msg = {
  messageID: 'm1',
  messageVersion: 2,
  createdAt: '2026-11-05T00:00:00Z',
  role: 'assistant',
  content: [{ type: 'text', text: 'Hello' }],
  future: { keep: true },
};
test('retains native identity, versions and raw fields beside normalized text', () => {
  const [row] = ampEvidence(tid, [msg]);
  assert.equal(row.kind, 'assistant');
  assert.equal(row.content, 'Hello');
  assert.equal(row.record.messageVersion, 2);
  assert.deepEqual(row.record.raw, msg);
  assert.equal(
    ampEvidence(tid, [{ ...msg, messageVersion: 3 }])[0].eventId,
    row.eventId,
  );
});
test('unknown message unions remain raw markers', () => {
  for (const content of [{ unknown: true }, [{ type: 'tool_use', id: 'call-1', name: 'Read' }]])
    assert.equal(ampEvidence(tid, [{ ...msg, role: 'new-role', content }])[0].kind, 'marker');
});
test('native CLI exports use protocol identity and keep tool results out of human prompts', () => {
  const rows = ampExport(tid, {
    id: tid,
    created: 1793836800000,
    messages: [
      {
        protocolMessageID: 'native-1',
        protocolMessageVersion: 4,
        messageId: 1,
        role: 'user',
        meta: { sentAt: 1793836800000 },
        content: [{ type: 'text', text: 'Inspect this' }],
      },
      {
        protocolMessageID: 'native-2',
        protocolMessageVersion: 1,
        messageId: 2,
        role: 'assistant',
        createdAt: msg.createdAt,
        content: [
          {
            type: 'tool_use',
            id: 'call-1',
            name: 'Read',
            input: { path: 'file.txt' },
          },
        ],
      },
      {
        protocolMessageID: 'native-3',
        protocolMessageVersion: 1,
        messageId: 3,
        role: 'user',
        createdAt: msg.createdAt,
        content: [
          { type: 'tool_result', toolUseID: 'call-1', run: { status: 'done' } },
        ],
      },
    ],
  });
  assert.deepEqual(
    rows.map((row) => row.kind),
    ['prompt', 'tool_call', 'marker'],
  );
  assert.equal(rows[0].record.messageId, 'native-1');
  assert.equal(rows[0].record.messageVersion, 4);
  assert.equal(rows[0].ts, '2026-11-05T00:00:00.000Z');
});
test('rejects ambiguous identity, malformed versions, missing timestamps and wrong exports', () => {
  for (const item of [
    { ...msg, messageID: null },
    { ...msg, messageID: null, id: 'generic', messageId: 1 },
    { ...msg, protocolMessageID: null, protocolMessageVersion: 1, messageId: 1 },
    { ...msg, messageID: undefined, messageId: 1 },
    { ...msg, messageID: NaN },
    { ...msg, messageVersion: null },
    { ...msg, messageVersion: -1 },
    { ...msg, createdAt: null },
  ])
    assert.throws(() => ampEvidence(tid, [item]));
  assert.throws(() => ampEvidence(tid, [msg, msg]));
  assert.throws(() =>
    ampExport(tid, { id: 'T-somewhere-else', messages: [msg] }),
  );
  assert.throws(() => ampThreadId('T-fixture; malicious'));
});

test('mixed assistant text and tool content keeps readable assistant prose', () => {
  const [row] = ampEvidence(tid, [{ ...msg, content: [{ type: 'text', text: 'Checking' }, { type: 'text', text: 'now' }, { type: 'tool_use', id: 'call-1', name: 'Read', input: { path: 'a' } }, { type: 'tool_use', id: 'call-2', name: 'Read', input: { path: 'b' } }] }]);
  assert.equal(row.kind, 'assistant');
  assert.equal(row.content, 'Checking\nnow');
  assert.equal((row.record.raw.content as unknown[]).length, 4);
  assert.deepEqual(row.toolCalls, [
    { id: 'call-1', tool: 'Read', input: { path: 'a' } },
    { id: 'call-2', tool: 'Read', input: { path: 'b' } },
  ]);
  assert.deepEqual(ampEvidence(tid, [{ ...msg, content: (row.record.raw.content as unknown[]).slice(2) }])[0].toolCalls, row.toolCalls);
});

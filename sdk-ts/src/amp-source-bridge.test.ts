import { test } from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import { AMP_CLI_BRIDGE, type AmpOperation } from './amp-source.js';

function bridge(operation: AmpOperation, result = { status: 0 as number | null, stdout: '[]' }) {
  let output = '';
  let call: { args: string[]; options: Record<string, unknown> } | undefined;
  const exited = Symbol('exit');
  try {
    vm.runInNewContext(AMP_CLI_BRIDGE, {
      require: () => ({ spawnSync: (_command: string, args: string[], options: Record<string, unknown>) => { call = { args, options }; return result; } }),
      process: {
        env: { RELAY_AMP_OPERATION: JSON.stringify(operation), AMP_API_KEY: 'fixture-only' },
        stdout: { write: (text: string) => { output += text; } },
        exit: () => { throw exited; },
      },
    });
  } catch (error) { if (error !== exited) throw error; }
  return { call: call!, result: JSON.parse(output) };
}

test('provider bridge selects read operations without starting an agent and bounds its process', () => {
  const discovery = bridge({ kind: 'discover' });
  assert.deepEqual(Array.from(discovery.call.args), ['--log-file', '/dev/null', 'threads', 'search', 'author:me', '--json', '--limit', '100']);
  assert.deepEqual(discovery.result, { ok: true, value: [] });
  const exported = bridge({ kind: 'export', threadId: 'T-fixture-thread' }, { status: 0, stdout: '{"messages":[]}' });
  assert.deepEqual(Array.from(exported.call.args), ['--log-file', '/dev/null', 'threads', 'export', 'T-fixture-thread']);
  assert.equal(exported.call.options.timeout, 45000);
  assert.equal(exported.call.options.maxBuffer, 16777216);
  assert.equal((exported.call.options.env as Record<string, unknown>).RELAY_AMP_OPERATION, '');
});
test('provider bridge sends literal arguments to the existing orb and classifies uncertain submissions', () => {
  const operation: AmpOperation = { kind: 'inject', threadId: 'T-fixture-thread', text: 'literal $(text) `not a shell`' };
  const sent = bridge(operation);
  assert.deepEqual(Array.from(sent.call.args), ['--log-file', '/dev/null', 'threads', 'continue', operation.threadId, '-ox', operation.text]);
  assert.deepEqual(sent.result, { ok: true, value: { accepted: true } });
  for (const status of [1, null]) {
    assert.deepEqual(bridge(operation, { status, stdout: 'secret output' }).result, { ok: false, outcome: 'unknown', code: 'amp_command_failed' });
    assert.deepEqual(bridge({ kind: 'discover' }, { status, stdout: 'secret output' }).result, { ok: false, outcome: 'rejected', code: 'amp_command_failed' });
  }
});
test('provider bridge rejects malformed read output without echoing it', () => {
  assert.deepEqual(bridge({ kind: 'export', threadId: 'T-fixture-thread' }, { status: 0, stdout: 'private malformed text' }).result,
    { ok: false, outcome: 'rejected', code: 'amp_invalid_json' });
});

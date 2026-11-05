import { test } from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import { AMP_CLI_BRIDGE, type AmpOperation } from './amp-source.js';

function bridge(operation: unknown, result = { status: 0 as number | null, stdout: '[]' }, raw?: string, cliPath?: string) {
  let output = '';
  const calls: Array<{ command: string; args: string[]; options: Record<string, unknown> }> = [];
  const exited = Symbol('exit');
  try {
    vm.runInNewContext(AMP_CLI_BRIDGE, {
      require: () => ({ spawnSync: (command: string, args: string[], options: Record<string, unknown>) => { calls.push({ command, args, options }); return result; } }),
      process: {
        env: { RELAY_AMP_OPERATION: raw ?? JSON.stringify(operation), AMP_API_KEY: 'fixture-only', RELAY_AMP_CLI_PATH: cliPath },
        stdout: { write: (text: string) => { output += text; } },
        exit: () => { throw exited; },
      },
    });
  } catch (error) { if (error !== exited) throw error; }
  return { call: calls[0]!, calls, result: JSON.parse(output) };
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
  for (const read of [discovery, exported]) {
    assert.equal(read.calls.length, 1);
    assert.equal(read.call.command, '/tmp/relay-amp/node_modules/.bin/amp');
  }
});
test('provider bridge sends literal arguments to the existing orb and classifies uncertain submissions', () => {
  const operation: AmpOperation = { kind: 'inject', threadId: 'T-fixture-thread', text: 'literal $(text) `not a shell`' };
  const sent = bridge(operation);
  assert.deepEqual(Array.from(sent.call.args), ['--log-file', '/dev/null', 'threads', 'continue', operation.threadId, '-ox', operation.text]);
  assert.deepEqual(sent.result, { ok: true, value: { accepted: true } });
  assert.equal(sent.calls.length, 1);
  assert.equal(sent.call.command, '/tmp/relay-amp/node_modules/.bin/amp');
  for (const status of [1, null]) {
    assert.deepEqual(bridge(operation, { status, stdout: 'secret output' }).result, { ok: false, outcome: 'unknown', code: 'amp_command_failed' });
    assert.deepEqual(bridge({ kind: 'discover' }, { status, stdout: 'secret output' }).result, { ok: false, outcome: 'rejected', code: 'amp_command_failed' });
  }
});
test('provider bridge rejects malformed read output without echoing it', () => {
  assert.deepEqual(bridge({ kind: 'export', threadId: 'T-fixture-thread' }, { status: 0, stdout: 'private malformed text' }).result,
    { ok: false, outcome: 'rejected', code: 'amp_invalid_json' });
});
test('provider bridge rejects missing, malformed and future operations without running any command', () => {
  for (const operation of [undefined, null, {}, { kind: 'future', threadId: 'T-fixture-thread', text: 'must not send' },
    { kind: 'inject', threadId: 'T-fixture-thread' }, { kind: 'export', threadId: 'T-bad; shell' }]) {
    const rejected = bridge(operation);
    assert.equal(rejected.calls.length, 0);
    assert.deepEqual(rejected.result, { ok: false, outcome: 'rejected', code: 'amp_invalid_operation' });
  }
  const malformed = bridge(null, undefined, '{invalid');
  assert.equal(malformed.calls.length, 0);
  assert.deepEqual(malformed.result, { ok: false, outcome: 'rejected', code: 'amp_invalid_operation' });
});
test('provider bridge normalizes supported thread URLs and rejects NUL text before spawning', () => {
  const read = bridge({ kind: 'export', threadId: ' https://ampcode.com/threads/T-fixture-thread/ ' });
  assert.equal(read.call.args.at(-1), 'T-fixture-thread');
  const rejected = bridge({ kind: 'inject', threadId: 'T-fixture-thread', text: 'bad' + String.fromCharCode(0) });
  assert.equal(rejected.calls.length, 0);
  assert.deepEqual(rejected.result, { ok: false, outcome: 'rejected', code: 'amp_invalid_operation' });
});

test('provider bridge accepts a host-owned preinstalled CLI path without taking commands from operations', () => {
  const read = bridge({ kind: 'discover', executable: '/untrusted' }, undefined, undefined, '/opt/relay-amp/node_modules/.bin/amp');
  assert.equal(read.call.command, '/opt/relay-amp/node_modules/.bin/amp');
  assert.equal(read.calls.length, 1);
});

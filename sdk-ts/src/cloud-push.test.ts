import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { test } from 'node:test';

import { pushToCloud } from './cloud-push.js';

/** Minimal fake child process that scripts stdout/stderr/exit for one run. */
function fakeSpawn(script: { stdout?: string; stderr?: string; code?: number; errorCode?: string }) {
  const calls: Array<{ bin: string; args: string[] }> = [];
  const spawnFn = ((bin: string, args: string[]) => {
    calls.push({ bin, args });
    const child = new EventEmitter() as EventEmitter & {
      stdout: EventEmitter;
      stderr: EventEmitter;
    };
    child.stdout = new EventEmitter();
    child.stderr = new EventEmitter();
    // Emit asynchronously so the caller has attached its listeners.
    setImmediate(() => {
      if (script.errorCode) {
        const err = new Error('spawn failed') as NodeJS.ErrnoException;
        err.code = script.errorCode;
        child.emit('error', err);
        return;
      }
      if (script.stdout) child.stdout.emit('data', script.stdout);
      if (script.stderr) child.stderr.emit('data', script.stderr);
      child.emit('close', script.code ?? 0);
    });
    return child;
  }) as unknown as typeof import('node:child_process').spawn;
  return { spawnFn, calls };
}

test('pushToCloud parses ai-hist push --json output into a report', async () => {
  const { spawnFn, calls } = fakeSpawn({
    stdout: JSON.stringify({ sent: 3, accepted: 3, batchId: 'b_abc', cursor: { history_id: 9 } }),
  });

  const report = await pushToCloud({ binPath: '/bin/echo', spawnFn, limit: 200, incognito: ['s1', 's2'] });

  assert.deepEqual(report, { sent: 3, accepted: 3, batchId: 'b_abc', cursor: { history_id: 9 } });
  // Forwards the flags to the binary.
  assert.deepEqual(calls[0].args, ['push', '--json', '--limit', '200', '--incognito', 's1', '--incognito', 's2']);
});

test('pushToCloud returns null when the binary is not installed (ENOENT)', async () => {
  const { spawnFn } = fakeSpawn({ errorCode: 'ENOENT' });
  const report = await pushToCloud({ binPath: '/bin/echo', spawnFn });
  assert.equal(report, null);
});

test('pushToCloud returns null when not authenticated', async () => {
  const { spawnFn } = fakeSpawn({ code: 1, stderr: 'error: not authenticated — run `ai-hist login`' });
  const report = await pushToCloud({ binPath: '/bin/echo', spawnFn });
  assert.equal(report, null);
});

test('pushToCloud rejects on other non-zero exits', async () => {
  const { spawnFn } = fakeSpawn({ code: 2, stderr: 'boom: server exploded' });
  await assert.rejects(pushToCloud({ binPath: '/bin/echo', spawnFn }), /ai-hist push failed \(exit 2\).*boom/);
});

test('pushToCloud treats empty stdout as a zero-record push', async () => {
  const { spawnFn } = fakeSpawn({ stdout: '' });
  const report = await pushToCloud({ binPath: '/bin/echo', spawnFn });
  assert.deepEqual(report, { sent: 0, accepted: 0, batchId: null, cursor: undefined });
});

test('pushToCloud rejects when the binary prints malformed JSON', async () => {
  const { spawnFn } = fakeSpawn({ code: 0, stdout: '{ truncated' });
  await assert.rejects(pushToCloud({ binPath: '/bin/echo', spawnFn }), /could not parse ai-hist push output/);
});

import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';

import { usesCancellation } from './cli.js';

const cli = new URL('./cli.js', import.meta.url).pathname;

interface Ended { code: number | null; signal: NodeJS.Signals | null; stdout: string; stderr: string }

/**
 * Run the bin, wait for it to report readiness, interrupt it once, and report
 * how it ended.
 *
 * A single `SIGINT` is the whole point: a user pressing Ctrl-C once expects the
 * command to stop. The child is killed hard after the deadline so a swallowed
 * signal fails this test rather than hanging the runner, and the readiness
 * marker means the signal always arrives after the bin has decided what to do
 * with it rather than racing module load.
 */
function interrupt(args: readonly string[], deadlineMs = 10_000): Promise<Ended> {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [cli, ...args], { stdio: ['ignore', 'pipe', 'pipe'] });
    let stdout = '';
    let stderr = '';
    let interrupted = false;
    let timedOut = false;
    const deadline = setTimeout(() => { timedOut = true; child.kill('SIGKILL'); }, deadlineMs);
    child.stdout.on('data', (chunk: Buffer) => {
      stdout += chunk.toString();
      if (!interrupted && stdout.includes('ready')) { interrupted = true; child.kill('SIGINT'); }
    });
    child.stderr.on('data', (chunk: Buffer) => { stderr += chunk.toString(); });
    child.on('error', reject);
    child.on('close', (code, signal) => {
      clearTimeout(deadline);
      if (timedOut) {
        reject(new Error(`the command was still running ${deadlineMs}ms after a single SIGINT: ${stdout}${stderr}`));
      } else if (!interrupted) {
        reject(new Error(`the command ended before it was interrupted: ${stdout}${stderr}`));
      } else {
        resolve({ code, signal, stdout, stderr });
      }
    });
  });
}

test('the bin claims the process signals only for the commands that read them', () => {
  // `main` asks this before installing a handler, so it has to agree with the
  // dispatch it precedes: every command that is handed `options.signal` and no
  // other.
  assert.equal(usesCancellation(['sessions', 'list']), false);
  assert.equal(usesCancellation(['sync']), false);
  assert.equal(usesCancellation([]), false);
  assert.equal(usesCancellation(['--version']), false);
  assert.equal(usesCancellation(['--not-a-real-flag']), false,
    'a command line that will be refused runs nothing, so it reads no signal');
});

/** A config directory holding one local source-connector plugin module. */
async function pluginConfig(t: { after(fn: () => unknown): void }, source: string): Promise<{ root: string; configPath: string }> {
  const root = await mkdtemp(join(tmpdir(), 'relayhistory-signals-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  await writeFile(join(root, 'plugin.mjs'), source);
  const configPath = join(root, 'config.json');
  await writeFile(configPath, JSON.stringify({ plugins: [{ module: './plugin.mjs' }] }));
  return { root, configPath };
}

test('Ctrl-C ends a command that does not read the cancellation signal', async (t) => {
  // Remote discovery is an ordinary command: the bin hands it no signal, so a
  // handler installed for it can only swallow the user's first Ctrl-C. The
  // connector blocks so that the signal has something to interrupt.
  const { root, configPath } = await pluginConfig(t, `
    export function createHistoryPlugin() {
      return { sources: [{ id: 'block', instanceId: 'one', location: 'remote', supportedSources: ['claude'],
        hydrate: () => new Promise(() => {}),
        discover: () => new Promise(() => {
          setInterval(() => {}, 1000);
          process.stdout.write('ready\\n');
        }) }] };
    }
  `);

  const ended = await interrupt(['sessions', 'discover', '--remote', '--config', configPath,
    '--source-connector', 'block', '--db', join(root, 'history.db')]);
  assert.equal(ended.signal, 'SIGINT',
    `one Ctrl-C must end an ordinary command; it ended with code ${ended.code}/${ended.signal}`);
});

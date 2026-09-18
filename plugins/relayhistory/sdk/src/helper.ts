/** Bounded, cancellable bridge to the optional RelayHistory Rust executable. */
import { StringDecoder } from 'node:string_decoder';
import { spawn, type ChildProcess } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { createRequire } from 'node:module';
import { dirname, join } from 'node:path';
import { RelayHistoryError, InvalidArgumentError, UnsupportedOperationError, AuthenticationExpiredError, ConnectorFailureError, runtimePlatform } from 'ai-hist';
import type { CloudPushResult, RelayhistoryAuth, ReplayOptions, ReplayResult } from './cloud-client.js';
export interface HelperOptions { binaryPath?: string; signal?: AbortSignal; timeoutMs?: number }
function packagedBinary(): string {
  const binary = 'relayhistory-plugin'+(process.platform==='win32'?'.exe':'');
  try { return join(dirname(createRequire(import.meta.url).resolve('@relayhistory/capture-'+runtimePlatform()+'/package.json')),binary); }
  catch { return fileURLToPath(new URL('../bin/'+runtimePlatform()+'/'+binary,import.meta.url)); }
}
/** Only terminate the process tree rooted at the helper we created. POSIX
 * helpers lead a fresh process group; Windows uses taskkill's PID-scoped tree.
 * Cancellation is not complete until the helper's stdio/process is reaped. */
export async function terminateHelperTree(child: ChildProcess, closed: Promise<void>, isClosed: () => boolean): Promise<void> {
  if (isClosed()) return;
  let deadline: ReturnType<typeof setTimeout> | undefined;
  try {
    await Promise.race([
      (async () => {
        if (child.pid !== undefined) {
          if (process.platform === 'win32') {
            // Do not target a potentially reused PID after observing its exit.
            if (child.exitCode !== null || child.signalCode !== null) throw new Error('Helper exited before tree cleanup');
            await new Promise<void>((resolve, reject) => {
              const killer = spawn(join(process.env.SystemRoot ?? 'C:\\Windows', 'System32', 'taskkill.exe'), ['/PID', String(child.pid), '/T', '/F'], { windowsHide: true, stdio: 'ignore' });
              const limit = setTimeout(() => { killer.kill(); reject(new Error('Tree cleanup timed out')); }, 2000);
              killer.once('error', () => { clearTimeout(limit); reject(new Error('Tree cleanup unavailable')); });
              killer.once('close', code => { clearTimeout(limit); if (code === 0) resolve(); else reject(new Error('Tree cleanup failed')); });
            });
          } else {
            try { process.kill(-child.pid, 'SIGKILL'); }
            catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ESRCH') throw error; }
          }
        }
        await closed;
      })(),
      new Promise<never>((_resolve, reject) => { deadline = setTimeout(() => reject(new Error('Helper cleanup timed out')), 3000); }),
    ]);
  } catch {
    // Release our handles even if a broken/escaping subprocess does not close
    // inherited pipes. Report failed cleanup instead of promising cancellation.
    if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
    child.stdin?.destroy(); child.stdout?.destroy(); child.stderr?.destroy(); child.unref();
    throw new RelayHistoryError('Helper process tree cleanup failed', 'HISTORY_PLUGIN_CLEANUP_FAILED');
  } finally { if (deadline !== undefined) clearTimeout(deadline); }
}
export async function helperRequest<T>(operation: string, args: Record<string, unknown> = {}, options: HelperOptions = {}): Promise<T> {
  options.signal?.throwIfAborted();
  const body = JSON.stringify({ version: 1, operation, args });
  if (Buffer.byteLength(body) > 16 * 1_048_576) throw new InvalidArgumentError('RelayHistory helper request exceeds 16 MiB', 'INVALID_ARGUMENT');
  const executable = options.binaryPath ?? process.env.RELAYHISTORY_PLUGIN_BIN
    ?? packagedBinary();
  return new Promise<T>((resolve, reject) => {
    const child = spawn(executable, [], { stdio: ['pipe', 'pipe', 'pipe'], windowsHide: true, detached: process.platform !== 'win32' });
    const decoder=new StringDecoder('utf8');
    let output = ''; let bytes = 0; let settled = false; let closed = false;
    const childClosed = new Promise<void>(resolve => child.once('close', () => { closed = true; resolve(); }));
    const finish = (error?: Error, value?: T) => {
      if (settled) return; settled = true;
      clearTimeout(timer); options.signal?.removeEventListener('abort', abort);
      if (error) {
        void terminateHelperTree(child, childClosed, () => closed).then(() => reject(error), reject);
      } else resolve(value as T);
    };
    const abort = () => finish(new RelayHistoryError('RelayHistory helper operation cancelled', 'HISTORY_PLUGIN_CANCELLED'));
    const timer = setTimeout(() => finish(new RelayHistoryError('RelayHistory helper operation timed out', 'HISTORY_PLUGIN_TIMEOUT')), options.timeoutMs ?? 60_000);
    options.signal?.addEventListener('abort', abort, { once: true });
    child.on('error', () => finish(new RelayHistoryError('RelayHistory helper is unavailable; install its matching platform artifact', 'HISTORY_PLUGIN_BINARY_MISSING')));
    child.stdin.on('error', () => finish(new RelayHistoryError('RelayHistory helper input failed', 'HISTORY_PLUGIN_PROTOCOL_FAILED')));
    // Never forward arbitrary stderr, which could contain auth or history.
    child.stderr.resume();
    child.stdout.on('data', (chunk: Buffer) => {
      bytes += chunk.length;
      if (bytes > 32 * 1_048_576) return finish(new RelayHistoryError('RelayHistory helper response exceeds 32 MiB', 'HISTORY_PLUGIN_PROTOCOL_FAILED'));
      output += decoder.write(chunk);
    });
    child.on('close', code => {
      if (settled) return;
      try {
        if (code !== 0) throw new Error();
        const envelope = JSON.parse(output+decoder.end()) as { version?: unknown; ok?: unknown; value?: T; error?: { code?: unknown; message?: unknown } };
        if (envelope.version !== 1 || typeof envelope.ok !== 'boolean') throw new Error();
        if (!envelope.ok) {
          const code = envelope.error?.code;
          if (typeof code !== 'string' || !/^[A-Z_]{1,100}$/.test(code)) throw new Error();
          // Stable public classes come from the installed local SDK instance.
          const ErrorType = code === 'INVALID_ARGUMENT' ? InvalidArgumentError
            : code === 'UNSUPPORTED_OPERATION' ? UnsupportedOperationError
            : code === 'AUTHENTICATION_EXPIRED' ? AuthenticationExpiredError
            : code === 'CONNECTOR_FAILURE' ? ConnectorFailureError : RelayHistoryError;
          finish(new ErrorType(`RelayHistory operation failed (${code})`, code));
        } else finish(undefined, envelope.value);
      } catch { finish(new RelayHistoryError('RelayHistory helper returned an invalid response', 'HISTORY_PLUGIN_PROTOCOL_FAILED')); }
    });
    child.stdin.end(body);
  });
}
interface HelperBinding {
  accessToken(baseUrl?: string): Promise<string>;
  replay(sessionId: string, options: ReplayOptions): Promise<ReplayResult>;
  createShareableTrace(sessionId: string, visibility: string, source?: string, baseUrl?: string): Promise<string>;
  cloudLoadAuth(baseUrl?: string): Promise<RelayhistoryAuth | null>;
  cloudResolveSession(baseUrl: string | undefined, now: number): Promise<{auth?:RelayhistoryAuth|null;detail?:string}>;
  cloudRefreshSession(baseUrl: string, rejectedToken: string): Promise<RelayhistoryAuth|null>;
  cloudLogin(options: object): Promise<RelayhistoryAuth>;
  enableCloud(options: object): Promise<CloudPushResult>;
  pushCloud(options: object): Promise<CloudPushResult>;
}
function cloudOptions(value: object): Record<string,unknown> {
  const options=value as Record<string,unknown>;
  return Object.fromEntries(['baseUrl','dbPath','relayAccessToken','label','interactive','workspace'].map(key=>[key,options[key]]));
}
const bridge: HelperBinding = {
  accessToken: baseUrl => helperRequest('accessToken',{baseUrl}),
  replay: (sessionId,options) => helperRequest('replay',{sessionId,...options}),
  createShareableTrace: (sessionId,visibility,source,baseUrl) => helperRequest('createShareableTrace',{sessionId,visibility,source,baseUrl}),
  cloudLoadAuth: baseUrl => helperRequest('cloudLoadAuth',{baseUrl}),
  cloudResolveSession: (baseUrl,now) => helperRequest('cloudResolveSession',{baseUrl,now}),
  cloudRefreshSession: (baseUrl,rejectedToken) => helperRequest('cloudRefreshSession',{baseUrl,rejectedToken}),
  cloudLogin: options => helperRequest('cloudLogin',{...options},{timeoutMs:300_000}),
  enableCloud: options => helperRequest('enableCloud',cloudOptions(options),{timeoutMs:300_000}),
  pushCloud: options => helperRequest('pushCloud',cloudOptions(options),{timeoutMs:300_000}),
};
export function helperCall<T>(call: (helper: HelperBinding) => Promise<T>): Promise<T> { return call(bridge); }

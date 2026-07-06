/**
 * In-process cloud push — a thin wrapper over the `ai-hist` Rust binary.
 *
 * Rather than re-implement the push pipeline (outbox batching, cursor, wire
 * envelopes, dedup hashing) in TypeScript — which would have to stay byte-for-
 * byte compatible with the Rust client forever — this spawns the real
 * `ai-hist push --json` and parses its result. The Rust binary is the single
 * source of truth for the push logic; this is just an ergonomic SDK surface
 * over it, so hosts (e.g. the Agent Relay runtime) can sync without shelling
 * out to a CLI by hand.
 */
import { spawn, type SpawnOptions } from 'node:child_process';
import { accessSync, constants } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';

export interface PushReport {
  sent: number;
  accepted: number;
  batchId: string | null;
  cursor?: unknown;
}

export interface PushOptions {
  /** Explicit path to the ai-hist binary (overrides discovery). */
  binPath?: string;
  /** Rows scanned per source (forwarded as `--limit`). */
  limit?: number;
  /** Session/trajectory ids to exclude (forwarded as repeated `--incognito`). */
  incognito?: Iterable<string>;
  /** Extra environment for the spawned binary (merged over `process.env`). */
  env?: NodeJS.ProcessEnv;
  /** Injectable spawn for testing. */
  spawnFn?: typeof spawn;
}

const AI_HIST_RUST_BIN_ENV = 'AI_HIST_RUST_BIN';

function isExecutable(path: string): boolean {
  try {
    accessSync(path, constants.X_OK);
    return true;
  } catch {
    return false;
  }
}

/**
 * Resolve the ai-hist binary: explicit override → `$AI_HIST_RUST_BIN` → the
 * install.sh location → the `ai-hist` wrapper on `PATH`. Returns `null` only if
 * an explicit/known path was given but isn't executable; otherwise falls back
 * to `'ai-hist'` and lets spawn surface ENOENT (handled as a no-op).
 */
export function resolveAiHistBinary(explicit?: string): string {
  const known = [explicit, process.env[AI_HIST_RUST_BIN_ENV], join(homedir(), '.local', 'share', 'ai-hist', 'ai-hist-rust-bin')].filter(
    (c): c is string => typeof c === 'string' && c.length > 0
  );
  for (const candidate of known) {
    if (isExecutable(candidate)) return candidate;
  }
  // Let PATH resolution (and ENOENT handling) take over.
  return 'ai-hist';
}

/**
 * Push new local history to relayhistory-cloud by driving `ai-hist push --json`.
 * Resolves `null` when the binary isn't installed or the user isn't
 * authenticated (both non-fatal for a background loop); rejects on other
 * non-zero exits or unparseable output.
 */
export function pushToCloud(opts: PushOptions = {}): Promise<PushReport | null> {
  const spawnFn = opts.spawnFn ?? spawn;
  const bin = resolveAiHistBinary(opts.binPath);

  const args = ['push', '--json'];
  if (opts.limit != null) args.push('--limit', String(opts.limit));
  for (const id of opts.incognito ?? []) args.push('--incognito', id);

  const spawnOpts: SpawnOptions = {
    env: { ...process.env, ...opts.env },
    stdio: ['ignore', 'pipe', 'pipe'],
  };

  return new Promise((resolve, reject) => {
    const child = spawnFn(bin, args, spawnOpts);

    let stdout = '';
    let stderr = '';
    child.stdout?.on('data', (chunk) => {
      stdout += String(chunk);
    });
    child.stderr?.on('data', (chunk) => {
      stderr += String(chunk);
    });

    child.on('error', (err: NodeJS.ErrnoException) => {
      // Binary not on PATH / not installed → treat as "nothing to do".
      if (err.code === 'ENOENT') {
        resolve(null);
        return;
      }
      reject(err);
    });

    child.on('close', (code) => {
      if (code !== 0) {
        // Not logged in yet is expected before `reflex on` completes.
        if (/not authenticated|no relayhistory auth|run `?ai-hist login/i.test(stderr)) {
          resolve(null);
          return;
        }
        reject(new Error(`ai-hist push failed (exit ${code}): ${stderr.trim().slice(0, 300)}`));
        return;
      }
      try {
        const parsed = (stdout.trim() ? JSON.parse(stdout) : {}) as Partial<PushReport>;
        resolve({
          sent: typeof parsed.sent === 'number' ? parsed.sent : 0,
          accepted: typeof parsed.accepted === 'number' ? parsed.accepted : 0,
          batchId: typeof parsed.batchId === 'string' ? parsed.batchId : null,
          cursor: parsed.cursor,
        });
      } catch (err) {
        reject(
          new Error(`could not parse ai-hist push output: ${err instanceof Error ? err.message : String(err)}`)
        );
      }
    });
  });
}

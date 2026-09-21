#!/usr/bin/env node

import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';

const DEFAULT_ATTEMPTS = 18;
const DEFAULT_DELAY_MS = 5_000;

export function isRegistryVisibilityFailure(output) {
  return /\b(?:ETARGET|E404)\b/.test(output);
}

function runNpmInstall(args, extra = {}) {
  const cache = mkdtempSync(join(tmpdir(), 'ai-hist-npm-cache-'));
  try {
    const result = spawnSync('npm', ['install', '--prefer-online', ...args], {
      encoding: 'utf8',
      cwd: extra.cwd,
      env: { ...process.env, ...extra.env, npm_config_cache: cache },
    });
    if (result.error) throw result.error;
    return {
      status: result.status ?? 1,
      stdout: result.stdout ?? '',
      stderr: result.stderr ?? '',
    };
  } finally {
    rmSync(cache, { recursive: true, force: true });
  }
}

function clearPartialInstall(cwd = process.cwd()) {
  rmSync(join(cwd, 'node_modules'), { recursive: true, force: true });
  rmSync(join(cwd, 'package-lock.json'), { force: true });
}

function delay(ms) {
  return new Promise((resolveDelay) => setTimeout(resolveDelay, ms));
}

function positiveInteger(value, fallback, name) {
  if (value === undefined || value === '') return fallback;
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive integer`);
  }
  return parsed;
}

export async function installWithRegistryRetry(args, options = {}) {
  if (args.length === 0) throw new Error('at least one npm install argument is required');

  const attempts = options.attempts ?? DEFAULT_ATTEMPTS;
  const delayMs = options.delayMs ?? DEFAULT_DELAY_MS;
  const cwd = options.cwd;
  const env = options.env;
  const runInstall = options.runInstall ?? ((installArgs) => runNpmInstall(installArgs, { cwd, env }));
  const sleep = options.sleep ?? delay;
  const reset = options.reset ?? (() => clearPartialInstall(cwd ?? process.cwd()));
  const emit = options.emit ?? ((stream, output) => stream.write(output));
  const log = options.log ?? ((message) => console.error(message));

  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    const result = runInstall(args);
    emit(process.stdout, result.stdout);
    emit(process.stderr, result.stderr);
    if (result.status === 0) return;

    const output = `${result.stdout}\n${result.stderr}`;
    const retryable = isRegistryVisibilityFailure(output);
    if (!retryable || attempt === attempts) {
      const reason = retryable
        ? `npm registry did not expose the requested packages after ${attempts} attempts`
        : 'npm install failed with a non-registry-visibility error';
      const error = new Error(reason);
      error.exitCode = result.status || 1;
      throw error;
    }

    log(
      `npm registry has not exposed all requested packages `
        + `(attempt ${attempt}/${attempts}); retrying in ${delayMs}ms`,
    );
    reset();
    await sleep(delayMs);
  }
}

const invokedPath = process.argv[1] ? resolve(process.argv[1]) : '';
const modulePath = fileURLToPath(import.meta.url);
if (invokedPath === modulePath) {
  const attempts = positiveInteger(
    process.env.NPM_REGISTRY_RETRY_ATTEMPTS,
    DEFAULT_ATTEMPTS,
    'NPM_REGISTRY_RETRY_ATTEMPTS',
  );
  const delayMs = positiveInteger(
    process.env.NPM_REGISTRY_RETRY_DELAY_MS,
    DEFAULT_DELAY_MS,
    'NPM_REGISTRY_RETRY_DELAY_MS',
  );
  installWithRegistryRetry(process.argv.slice(2), { attempts, delayMs }).catch((error) => {
    console.error(error.message);
    process.exitCode = error.exitCode ?? 1;
  });
}

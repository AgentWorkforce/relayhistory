#!/usr/bin/env node

import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';
import { installWithRegistryRetry } from './npm-install-with-registry-retry.mjs';

export const REGISTRY_RELEASE_PACKAGES = Object.freeze([
  'ai-hist',
  'ai-hist-mcp',
  'ai-hist-native',
  'ai-hist-native-darwin-arm64',
  'ai-hist-native-darwin-x64',
  'ai-hist-native-linux-arm64-gnu',
  'ai-hist-native-linux-arm64-musl',
  'ai-hist-native-linux-x64-gnu',
  'ai-hist-native-linux-x64-musl',
  'ai-hist-native-win32-x64-msvc',
]);

const DEFAULT_VISIBILITY_ATTEMPTS = 36;
const DEFAULT_VISIBILITY_DELAY_MS = 10_000;
const DEFAULT_INSTALL_ATTEMPTS = 36;
const DEFAULT_INSTALL_DELAY_MS = 10_000;

function positiveInteger(value, fallback, name) {
  if (value === undefined || value === '') return fallback;
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive integer`);
  }
  return parsed;
}

function npmViewVersion(spec, view = (args) => spawnSync('npm', args, { encoding: 'utf8' })) {
  const result = view(['view', spec, 'version']);
  if ((result.status ?? 1) !== 0) return null;
  return String(result.stdout ?? '').trim() || null;
}

export async function waitForRegistryPackages(version, options = {}) {
  const attempts = options.attempts ?? DEFAULT_VISIBILITY_ATTEMPTS;
  const delayMs = options.delayMs ?? DEFAULT_VISIBILITY_DELAY_MS;
  const view = options.view ?? npmViewVersion;
  const sleep = options.sleep ?? ((ms) => new Promise((resolveDelay) => setTimeout(resolveDelay, ms)));
  const log = options.log ?? ((message) => console.error(message));

  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    const missing = REGISTRY_RELEASE_PACKAGES.filter((pkg) => view(`${pkg}@${version}`) !== version);
    if (missing.length === 0) {
      log(`registry exposes all ${REGISTRY_RELEASE_PACKAGES.length} release packages at ${version}`);
      return;
    }
    if (attempt === attempts) {
      const error = new Error(
        `registry did not expose all release packages at ${version} after ${attempts} attempts: `
        + `${missing.join(', ')}`,
      );
      error.missing = missing;
      throw error;
    }
    log(
      `registry missing ${missing.length}/${REGISTRY_RELEASE_PACKAGES.length} package(s) `
        + `(attempt ${attempt}/${attempts}); retrying in ${delayMs}ms`,
    );
    log(missing.join(', '));
    await sleep(delayMs);
  }
}

function runOrThrow(command, args, options = {}) {
  const result = spawnSync(command, args, { encoding: 'utf8', ...options });
  if (result.error) throw result.error;
  if ((result.status ?? 1) !== 0) {
    const error = new Error(
      `${command} ${args.join(' ')} failed with exit ${result.status ?? 'null'}:\n`
      + `${result.stdout ?? ''}${result.stderr ?? ''}`,
    );
    error.exitCode = result.status ?? 1;
    throw error;
  }
  return result;
}

function assertMcpStartup(mcpBin) {
  const result = spawnSync('timeout', ['2', mcpBin], {
    encoding: 'utf8',
    input: '',
    env: process.env,
  });
  const code = result.status ?? 1;
  // MCP should either block on stdio (124) or exit cleanly when stdin closes (0).
  if (code !== 0 && code !== 124) {
    const error = new Error(
      `unexpected ai-hist-mcp exit ${code}:\n${result.stdout ?? ''}${result.stderr ?? ''}`,
    );
    error.exitCode = code;
    throw error;
  }
  console.error(`PASS: ai-hist-mcp startup (exit ${code})`);
}

function assertNativePackageMissing(version) {
  const result = spawnSync(process.execPath, ['--input-type=module', '-e', `
    import("ai-hist").then((sdk) => sdk.recent()).then(
      () => process.exit(1),
      (error) => {
        if (error.code !== "NATIVE_PACKAGE_MISSING") {
          console.error(error);
          process.exit(1);
        }
        console.log(error.message);
      },
    );
  `], { encoding: 'utf8', cwd: process.cwd(), env: process.env });
  if ((result.status ?? 1) !== 0) {
    const error = new Error(
      `omit=optional install must fail with NATIVE_PACKAGE_MISSING:\n${result.stdout ?? ''}${result.stderr ?? ''}`,
    );
    error.exitCode = result.status ?? 1;
    throw error;
  }
  console.error(`PASS: omit=optional rejects missing native engine for ai-hist@${version}`);
}

export async function registryCleanInstallSmoke(options) {
  const version = options.version;
  if (!/^\d+\.\d+\.\d+$/.test(version)) {
    throw new Error(`expected a stable semver release version, received ${version}`);
  }

  const repoRoot = resolve(options.repoRoot ?? join(dirname(fileURLToPath(import.meta.url)), '..'));
  const visibilityAttempts = options.visibilityAttempts ?? DEFAULT_VISIBILITY_ATTEMPTS;
  const visibilityDelayMs = options.visibilityDelayMs ?? DEFAULT_VISIBILITY_DELAY_MS;
  const installAttempts = options.installAttempts ?? DEFAULT_INSTALL_ATTEMPTS;
  const installDelayMs = options.installDelayMs ?? DEFAULT_INSTALL_DELAY_MS;

  await waitForRegistryPackages(version, {
    attempts: visibilityAttempts,
    delayMs: visibilityDelayMs,
    view: options.view,
    sleep: options.sleep,
    log: options.log,
  });

  const smoke = mkdtempSync(join(tmpdir(), 'ai-hist-registry-smoke-'));
  try {
    process.chdir(smoke);
    runOrThrow('npm', ['init', '-y'], { stdio: 'ignore' });
    await installWithRegistryRetry([`ai-hist@${version}`, `ai-hist-mcp@${version}`], {
      attempts: installAttempts,
      delayMs: installDelayMs,
      runInstall: options.runInstall,
      sleep: options.sleep,
      reset: options.reset,
      log: options.log,
    });
    runOrThrow(process.execPath, [
      join(repoRoot, 'scripts/smoke-native-cli.mjs'),
      join(smoke, 'node_modules/ai-hist/dist/cli.js'),
      '--prove-rejection',
    ], { stdio: 'inherit' });
    assertMcpStartup(join(smoke, 'node_modules/.bin/ai-hist-mcp'));
  } finally {
    rmSync(smoke, { recursive: true, force: true });
  }

  const noOptional = mkdtempSync(join(tmpdir(), 'ai-hist-registry-no-optional-'));
  try {
    process.chdir(noOptional);
    runOrThrow('npm', ['init', '-y'], { stdio: 'ignore' });
    await installWithRegistryRetry(['--omit=optional', `ai-hist@${version}`], {
      attempts: installAttempts,
      delayMs: installDelayMs,
      runInstall: options.runInstall,
      sleep: options.sleep,
      reset: options.reset,
      log: options.log,
    });
    assertNativePackageMissing(version);
  } finally {
    rmSync(noOptional, { recursive: true, force: true });
  }
}

const invokedPath = process.argv[1] ? resolve(process.argv[1]) : '';
const modulePath = fileURLToPath(import.meta.url);
if (invokedPath === modulePath) {
  const version = process.env.VERSION ?? process.argv[2];
  if (!version) {
    console.error('VERSION env or argv[2] is required');
    process.exitCode = 2;
  } else {
    registryCleanInstallSmoke({
      version,
      repoRoot: process.env.GITHUB_WORKSPACE,
      visibilityAttempts: positiveInteger(
        process.env.REGISTRY_VISIBILITY_ATTEMPTS,
        DEFAULT_VISIBILITY_ATTEMPTS,
        'REGISTRY_VISIBILITY_ATTEMPTS',
      ),
      visibilityDelayMs: positiveInteger(
        process.env.REGISTRY_VISIBILITY_DELAY_MS,
        DEFAULT_VISIBILITY_DELAY_MS,
        'REGISTRY_VISIBILITY_DELAY_MS',
      ),
      installAttempts: positiveInteger(
        process.env.NPM_REGISTRY_RETRY_ATTEMPTS,
        DEFAULT_INSTALL_ATTEMPTS,
        'NPM_REGISTRY_RETRY_ATTEMPTS',
      ),
      installDelayMs: positiveInteger(
        process.env.NPM_REGISTRY_RETRY_DELAY_MS,
        DEFAULT_INSTALL_DELAY_MS,
        'NPM_REGISTRY_RETRY_DELAY_MS',
      ),
    }).catch((error) => {
      console.error(error.message);
      process.exitCode = error.exitCode ?? 1;
    });
  }
}

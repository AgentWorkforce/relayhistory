#!/usr/bin/env node

import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';
import { installWithRegistryRetry } from './npm-install-with-registry-retry.mjs';
import { hostInstallArgs, hostLibc, publicRegistryEnv } from './npm-host-install.mjs';

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

/** Read one exact version from the public registry; only E404 means absent. */
export function npmViewVersion(spec, view = (args, options) => spawnSync('npm', args, options)) {
  const result = view(
    [
      'view', spec, 'version', '--registry=https://registry.npmjs.org/',
      '--prefer-online', '--fetch-retries=0', '--fetch-timeout=10000',
    ],
    { encoding: 'utf8', env: publicRegistryEnv(), timeout: 15000 },
  );
  if (result.error) throw result.error;
  if ((result.status ?? 1) !== 0) {
    const output = `${result.stderr ?? ''}${result.stdout ?? ''}`;
    if (/\bE404\b/.test(output)) return null;
    const detail = output.trim().split('\n').slice(0, 5).join(' | ');
    throw new Error(`npm view ${spec} failed: ${detail || `exit ${result.status ?? 'null'}`}`);
  }
  return String(result.stdout ?? '').trim() || null;
}

/** Wait for the complete core release, bounded by attempts and elapsed time. */
export async function waitForRegistryPackages(version, options = {}) {
  const attempts = options.attempts ?? DEFAULT_VISIBILITY_ATTEMPTS;
  const delayMs = options.delayMs ?? DEFAULT_VISIBILITY_DELAY_MS;
  const maxWaitMs = options.maxWaitMs ?? Infinity;
  const view = options.view ?? npmViewVersion;
  const sleep = options.sleep ?? ((ms) => new Promise((resolveDelay) => setTimeout(resolveDelay, ms)));
  const log = options.log ?? ((message) => console.error(message));
  const now = options.now ?? Date.now;
  const started = now();

  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    const missing = [];
    const lookupErrors = [];
    for (const pkg of REGISTRY_RELEASE_PACKAGES) {
      try {
        if (view(`${pkg}@${version}`) !== version) missing.push(pkg);
      } catch (error) {
        lookupErrors.push(`${pkg}: ${error.message}`);
      }
    }
    if (missing.length === 0 && lookupErrors.length === 0) {
      log(`registry exposes all ${REGISTRY_RELEASE_PACKAGES.length} release packages at ${version}`);
      return;
    }
    const elapsedMs = now() - started;
    if (attempt === attempts || elapsedMs >= maxWaitMs) {
      const error = new Error(
        `registry did not expose all release packages at ${version} after ${attempt} attempts `
        + `and ${Math.ceil(elapsedMs / 1000)}s. `
        + `Missing: ${missing.join(', ') || 'none'}. `
        + `Lookup errors: ${lookupErrors.join('; ') || 'none'}. `
        + `The release tag can be resumed with skip_core and custom_version once npm exposes the packages.`,
      );
      error.missing = missing;
      error.lookupErrors = lookupErrors;
      throw error;
    }
    const waitMs = Math.min(delayMs, maxWaitMs - elapsedMs);
    log(
      `registry missing ${missing.length}/${REGISTRY_RELEASE_PACKAGES.length} package(s) `
        + `and had ${lookupErrors.length} lookup error(s) `
        + `(attempt ${attempt}/${attempts}); retrying in ${waitMs}ms`,
    );
    if (missing.length) log(missing.join(', '));
    if (lookupErrors.length) log(lookupErrors.join('; '));
    await sleep(waitMs);
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

export function coreSmokeManifest(version) {
  return {
    name: 'ai-hist-registry-smoke',
    private: true,
    version: '0.0.0',
    dependencies: {
      'ai-hist': version,
      'ai-hist-mcp': version,
    },
  };
}

function writeManifest(directory, manifest) {
  writeFileSync(join(directory, 'package.json'), `${JSON.stringify(manifest, null, 2)}\n`);
}

function assertNativePackageMissing(version, cwd) {
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
  `], { encoding: 'utf8', cwd, env: process.env });
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
  const visibilityMaxWaitMs = options.visibilityMaxWaitMs;
  const installAttempts = options.installAttempts ?? DEFAULT_INSTALL_ATTEMPTS;
  const installDelayMs = options.installDelayMs ?? DEFAULT_INSTALL_DELAY_MS;

  await waitForRegistryPackages(version, {
    attempts: visibilityAttempts,
    delayMs: visibilityDelayMs,
    maxWaitMs: visibilityMaxWaitMs,
    view: options.view,
    sleep: options.sleep,
    log: options.log,
  });

  const libc = hostLibc();
  const publicEnv = publicRegistryEnv();
  const smoke = mkdtempSync(join(tmpdir(), 'ai-hist-registry-smoke-'));
  try {
    writeManifest(smoke, coreSmokeManifest(version));
    // `--libc` is the family `ai-hist-native-*-gnu` / `*-musl` declare.
    // npm-install-checks skips those optional packages when host libc is
    // undetected (`if (target.libc && !libc)`).
    await installWithRegistryRetry(hostInstallArgs(smoke, libc), {
      attempts: installAttempts,
      delayMs: installDelayMs,
      cwd: smoke,
      env: publicEnv,
      runInstall: options.runInstall,
      sleep: options.sleep,
      reset: options.reset,
      log: options.log,
    });
    runOrThrow(process.execPath, [
      join(repoRoot, 'scripts/smoke-native-cli.mjs'),
      join(smoke, 'node_modules/ai-hist/dist/cli.js'),
      '--prove-rejection',
    ], { stdio: 'inherit', cwd: smoke });
    assertMcpStartup(join(smoke, 'node_modules/.bin/ai-hist-mcp'));
  } finally {
    rmSync(smoke, { recursive: true, force: true });
  }

  const noOptional = mkdtempSync(join(tmpdir(), 'ai-hist-registry-no-optional-'));
  try {
    writeManifest(noOptional, {
      name: 'ai-hist-registry-no-optional',
      private: true,
      version: '0.0.0',
      dependencies: { 'ai-hist': version },
    });
    await installWithRegistryRetry(['--prefix', noOptional, '--omit=optional'], {
      attempts: installAttempts,
      delayMs: installDelayMs,
      cwd: noOptional,
      env: publicEnv,
      runInstall: options.runInstall,
      sleep: options.sleep,
      reset: options.reset,
      log: options.log,
    });
    assertNativePackageMissing(version, noOptional);
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
      visibilityMaxWaitMs: process.env.REGISTRY_VISIBILITY_MAX_WAIT_MS === undefined
        ? undefined
        : positiveInteger(
          process.env.REGISTRY_VISIBILITY_MAX_WAIT_MS,
          undefined,
          'REGISTRY_VISIBILITY_MAX_WAIT_MS',
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

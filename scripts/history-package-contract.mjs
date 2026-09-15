/** Shared release metadata and shell-free npm invocation for optional packages. */
import assert from "node:assert/strict";
import { existsSync, readFileSync, realpathSync } from "node:fs";
import { delimiter, dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const repositoryRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");

/**
 * Where each half of the native contract version is declared. Both halves are
 * the checkout's own source of truth, so every gate reads the number from here
 * instead of repeating a literal that a bump would have to chase.
 */
export const nativeContractSources = {
  rust: [
    "crates/ai-hist-napi/src/lib.rs",
    /pub const NATIVE_CONTRACT_VERSION:\s*u32\s*=\s*(\d+)\s*;/,
    "the Rust binding",
  ],
  sdk: [
    "sdk-ts/src/native.ts",
    /export const NATIVE_CONTRACT_VERSION\s*=\s*(\d+)\s*;/,
    "the TypeScript SDK",
  ],
};

/** The native contract version this checkout's sources require. */
export function nativeContractVersion(half = "sdk", root = repositoryRoot) {
  const entry = nativeContractSources[half];
  assert.ok(entry, `Unknown native contract source: ${half}`);
  const [relativePath, pattern, label] = entry;
  const match = pattern.exec(readFileSync(resolve(root, relativePath), "utf8"));
  assert.ok(match, `Could not read the native contract version from ${label}`);
  return Number(match[1]);
}

export const plugins = {
  relayhistory: { name: "relayhistory", binary: "relayhistory-plugin" },
  "provider-sources": {
    name: "history-provider-sources",
    binary: "history-provider-sources",
  },
};
export const platforms = {
  "darwin-arm64": ["darwin", "arm64"],
  "darwin-x64": ["darwin", "x64"],
  "linux-x64-gnu": ["linux", "x64", "glibc"],
  "linux-arm64-gnu": ["linux", "arm64", "glibc"],
  "linux-x64-musl": ["linux", "x64", "musl"],
  "linux-arm64-musl": ["linux", "arm64", "musl"],
  "win32-x64-msvc": ["win32", "x64"],
};
export function validatePluginManifest(plugin, manifest) {
  const info = plugins[plugin];
  assert.ok(info, `Unknown history plugin: ${plugin}`);
  assert.equal(manifest.name, `@agent-relay/${info.name}`);
  assert.match(manifest.version, /^\d+\.\d+\.\d+(?:-[\w.-]+)?$/);
  assert.ok(
    manifest.peerDependencies?.["ai-hist"],
    "An ai-hist peer compatibility range is required",
  );
  assert.ok(
    !/^(file:|link:|workspace:)/.test(manifest.peerDependencies["ai-hist"]),
    "The public ai-hist peer must use a registry version range",
  );
  for (const platform of Object.keys(platforms)) {
    assert.equal(
      manifest.optionalDependencies?.[`@agent-relay/${info.name}-${platform}`],
      manifest.version,
      `${manifest.name} must pin its ${platform} helper to its own version`,
    );
  }
  return info;
}
export function helperTarball(plugin, platform, manifest) {
  const info = validatePluginManifest(plugin, manifest);
  assert.ok(platforms[platform], `Unsupported history platform: ${platform}`);
  return `agent-relay-${info.name}-${platform}-${manifest.version}.tgz`;
}

// npm.cmd cannot be executed directly by spawnSync on Windows. Locate npm's
// JavaScript entry point and invoke Node explicitly on every platform.
export function npmCli(
  env = process.env,
  execPath = process.execPath,
  platform = process.platform,
) {
  const candidates = [
    env.npm_execpath,
    join(dirname(execPath), "node_modules/npm/bin/npm-cli.js"),
  ];
  for (const directory of (env.PATH ?? env.Path ?? "").split(delimiter)) {
    if (!directory) continue;
    const launcher = join(directory, platform === "win32" ? "npm.cmd" : "npm");
    if (existsSync(launcher)) {
      candidates.push(realpathSync(launcher));
      candidates.push(
        join(dirname(launcher), "node_modules/npm/bin/npm-cli.js"),
      );
    }
  }
  const found = candidates.find(
    (candidate) =>
      candidate && /npm-cli\.js$/.test(candidate) && existsSync(candidate),
  );
  assert.ok(
    found,
    "Cannot locate npm-cli.js; run through npm or install npm alongside Node",
  );
  return found;
}

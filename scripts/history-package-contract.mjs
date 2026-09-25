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

/**
 * npm scope for the optional history plugins.
 *
 * `@relayhistory`, not `@agent-relay`: that scope belongs to the relay
 * monorepo and carries its version line (cli-surface, cloud, sdk, fleet at
 * 12.x). Publishing this repository's plugins into it made them read as relay
 * packages at an unrelated version. Renamed while both were still unpublished,
 * so no deprecation or alias was needed.
 *
 * The core packages (`ai-hist`, `ai-hist-native`, `ai-hist-mcp`) deliberately
 * keep their unscoped names: they are published and depended on.
 */
/**
 * Repository every published package must declare.
 *
 * `npm publish --provenance` verifies the manifest's `repository.url` against
 * the repository recorded in the sigstore provenance bundle, and rejects a
 * mismatch — an absent field included:
 *
 *   npm error code E422
 *   npm error 422 Unprocessable Entity - Error verifying sigstore provenance
 *     bundle: package.json: "repository.url" is "", expected to match
 *     "https://github.com/AgentWorkforce/relayhistory" from provenance
 *
 * The core packages declare it in their checked-in manifests. The plugin
 * packages did not, and the platform helpers are generated, so the value lives
 * here and both paths read it.
 */
export const REPOSITORY_URL = "https://github.com/AgentWorkforce/relayhistory";

/** The `repository` field for a package whose source lives at `directory`. */
export function repositoryField(directory) {
  return { type: "git", url: REPOSITORY_URL, directory };
}

export const SCOPE = "@relayhistory";

/** Tarball filename prefix npm derives from SCOPE (`@x/y` packs as `x-y-...`). */
const TARBALL_SCOPE = SCOPE.replace(/^@/, "");

export const plugins = {
  "provider-sources": {
    name: "provider-sources",
    binary: "history-provider-sources",
  },
};

/** Published package name for a plugin, or for one of its platform helpers. */
export function packageName(info, platform) {
  return platform ? `${SCOPE}/${info.name}-${platform}` : `${SCOPE}/${info.name}`;
}
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
  assert.equal(manifest.name, packageName(info));
  // Publishing uses --provenance, which rejects a manifest whose repository
  // does not match the one in the sigstore bundle. Checked here so a missing
  // field fails the packaging gate rather than the publish.
  assert.equal(
    manifest.repository?.url,
    REPOSITORY_URL,
    `${manifest.name} must declare repository.url ${REPOSITORY_URL}`,
  );
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
      manifest.optionalDependencies?.[packageName(info, platform)],
      manifest.version,
      `${manifest.name} must pin its ${platform} helper to its own version`,
    );
  }
  return info;
}
export function helperTarball(plugin, platform, manifest) {
  const info = validatePluginManifest(plugin, manifest);
  assert.ok(platforms[platform], `Unsupported history platform: ${platform}`);
  return `${TARBALL_SCOPE}-${info.name}-${platform}-${manifest.version}.tgz`;
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

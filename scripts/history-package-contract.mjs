/** Shared release metadata and shell-free npm invocation for optional packages. */
import assert from "node:assert/strict";
import { existsSync, realpathSync } from "node:fs";
import { delimiter, dirname, join } from "node:path";

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

/** Apply one release version to every optional plugin manifest and lockfile.
 *
 * The release workflow runs this twice: once in the publish job, so the version
 * commit carries the plugin manifests, and once in the plugin job on its own
 * checkout (dry runs included). Both runs must produce byte-identical files, so
 * this script is the single deterministic place that knows what a plugin release
 * version touches. Local helper coordinates come from the shared package
 * contract; the `ai-hist` devDependency stays a checkout link on purpose.
 */
import assert from "node:assert/strict";
import { readFile, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
  platforms,
  plugins,
  validatePluginManifest,
} from "./history-package-contract.mjs";
import { syncHelperLocks } from "./sync-history-helper-locks.mjs";

const repositoryRoot = fileURLToPath(new URL("../", import.meta.url));
/** Releases are stable semver; the core publish job enforces the same shape. */
export const releaseVersionPattern = /^\d+\.\d+\.\d+$/;
/** The checkout link every plugin keeps for local SDK development. */
export const localCoreDependency = "file:../../../sdk-ts";

/**
 * Set `version`, the seven optional helper pins, the public `ai-hist` peer range
 * and the matching lockfile coordinates for both optional plugins. Idempotent.
 *
 * @param version stable semver shared with the core release.
 * @param root repository root, so tests can run against a temporary copy.
 * @returns the paths written, in order.
 */
export async function setReleaseVersion(version, root = repositoryRoot) {
  assert.match(
    version ?? "",
    releaseVersionPattern,
    `A release version must be stable semver, received ${version}`,
  );
  const written = [];
  for (const [plugin, info] of Object.entries(plugins)) {
    const directory = resolve(root, "plugins", plugin, "sdk");
    const manifestPath = resolve(directory, "package.json");
    const manifest = JSON.parse(await readFile(manifestPath, "utf8"));
    manifest.version = version;
    manifest.peerDependencies = {
      ...manifest.peerDependencies,
      "ai-hist": `^${version}`,
    };
    assert.ok(manifest.optionalDependencies, "Helper pins are required");
    for (const platform of Object.keys(platforms)) {
      const name = `@agent-relay/${info.name}-${platform}`;
      assert.ok(
        name in manifest.optionalDependencies,
        `${manifest.name} is missing its ${platform} helper`,
      );
      manifest.optionalDependencies[name] = version;
    }
    assert.equal(
      manifest.devDependencies?.["ai-hist"],
      localCoreDependency,
      "Plugins build against the checkout SDK; the devDependency link must stay",
    );
    validatePluginManifest(plugin, manifest);
    await writeFile(manifestPath, JSON.stringify(manifest, null, 2) + "\n");
    written.push(manifestPath);

    // The lock's root entry mirrors the manifest, so `npm ci` in the plugin
    // stays consistent with the version the release just applied.
    const lockPath = resolve(directory, "package-lock.json");
    const lock = JSON.parse(await readFile(lockPath, "utf8"));
    lock.version = version;
    const rootEntry = lock.packages?.[""];
    assert.ok(rootEntry, `${lockPath} is missing its root package entry`);
    rootEntry.version = version;
    if (rootEntry.optionalDependencies) {
      rootEntry.optionalDependencies = Object.fromEntries(
        Object.keys(rootEntry.optionalDependencies).map((name) => [
          name,
          version,
        ]),
      );
    }
    if (rootEntry.peerDependencies?.["ai-hist"]) {
      rootEntry.peerDependencies["ai-hist"] = `^${version}`;
    }
    // The linked checkout SDK is published at the same release version.
    const linkedCore = lock.packages?.["../../../sdk-ts"];
    if (linkedCore) linkedCore.version = version;
    await writeFile(lockPath, JSON.stringify(lock, null, 2) + "\n");
    written.push(lockPath);
  }
  // Optional helper entries are unpublished at this point; reuse the one
  // implementation that records their declared registry coordinates.
  await syncHelperLocks(root);
  for (const plugin of Object.keys(plugins)) {
    const manifestPath = resolve(root, "plugins", plugin, "sdk/package.json");
    validatePluginManifest(
      plugin,
      JSON.parse(await readFile(manifestPath, "utf8")),
    );
  }
  return written;
}

if (
  process.argv[1] &&
  resolve(process.argv[1]) === fileURLToPath(import.meta.url)
) {
  const [version, root] = process.argv.slice(2);
  if (!version) {
    throw new Error("Usage: set-release-version VERSION [REPOSITORY_ROOT]");
  }
  const written = await setReleaseVersion(
    version,
    root ? resolve(root) : undefined,
  );
  console.log(`Applied release version ${version} to:`);
  for (const path of written) console.log(`  ${path}`);
}

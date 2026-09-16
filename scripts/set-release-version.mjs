/** Apply one release version to every optional plugin manifest and lockfile.
 *
 * The release workflow runs this three times: in the publish job, so the version
 * commit carries the plugin manifests and crates; in the helper matrix, before
 * the Rust executables are built, so `agent-relay-probe --version` reports the
 * release it ships under; and in the plugin job on its own checkout (dry runs
 * included). Every run must produce byte-identical files, so this script is the
 * single deterministic place that knows what a plugin release version touches.
 * Local helper coordinates come from the shared package contract; the `ai-hist`
 * devDependency stays a checkout link on purpose.
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

/** The `[package]` header of a crate manifest, so the crate names itself. */
const crateNamePattern = /\[package\]\nname = "([^"]+)"\nversion = "/;
/** That same crate's version line, in the manifest and in its own lockfile. */
const crateVersionPatterns = (crate) => [
  [
    "Cargo.toml",
    new RegExp(`(\\[package\\]\\nname = "${crate}"\\nversion = ")[^"]+(")`),
  ],
  [
    "Cargo.lock",
    new RegExp(
      `(\\[\\[package\\]\\]\\nname = "${crate}"\\nversion = ")[^"]+(")`,
    ),
  ],
];

/**
 * Stamp `version` into one plugin crate: its `Cargo.toml` `[package]` version
 * and the crate's own `[[package]]` entry in the lockfile beside it. Each
 * plugin `rust` directory is its own Cargo workspace with its own lock, so the
 * rewritten pair stays consistent and `cargo build --locked` still resolves.
 *
 * The binaries read `CARGO_PKG_VERSION` — that is what `agent-relay-probe
 * --version` and the `cli_version` it reports print — so the release version
 * has to reach the crate before anything is compiled. Idempotent.
 *
 * @returns the paths written, in order.
 */
async function setCrateVersion(directory, version) {
  const written = [];
  const manifestPath = resolve(directory, "Cargo.toml");
  const crate = crateNamePattern.exec(
    await readFile(manifestPath, "utf8"),
  )?.[1];
  assert.ok(crate, `${manifestPath} has no [package] name and version header`);
  for (const [file, pattern] of crateVersionPatterns(crate)) {
    const path = resolve(directory, file);
    const contents = await readFile(path, "utf8");
    assert.match(contents, pattern, `${path} has no ${crate} version to set`);
    await writeFile(
      path,
      contents.replace(
        pattern,
        (_, prefix, suffix) => prefix + version + suffix,
      ),
    );
    written.push(path);
  }
  return written;
}

/**
 * Set `version`, the seven optional helper pins, the public `ai-hist` peer range
 * and the matching lockfile coordinates for both optional plugins, plus the
 * version of each plugin's Rust crate. Idempotent.
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

    // The helper and probe executables ship under this same release version.
    written.push(
      ...(await setCrateVersion(
        resolve(root, "plugins", plugin, "rust"),
        version,
      )),
    );
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

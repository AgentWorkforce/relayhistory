/** Apply one release version to every optional plugin manifest and lockfile.
 *
 * The release workflow runs this three times: in the publish job, so the version
 * commit carries the plugin manifests and crates; in the helper matrix, before
 * the Rust executables are built, so each helper's `CARGO_PKG_VERSION` is the
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
  packageName,
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
// Windows runners check out with CRLF, so a line break is `\r?\n` here.
const crateNamePattern = /\[package\]\r?\nname = "([^"]+)"\r?\nversion = "/;
/** The published core crate every plugin crate depends on by path. */
export const coreCrate = "ai-hist";
/** One `[[package]]` version line in a lockfile. */
const lockVersionPattern = (crate) =>
  new RegExp(
    `(\\[\\[package\\]\\]\\r?\\nname = "${crate}"\\r?\\nversion = ")[^"]+(")`,
  );
/**
 * That same crate's version line in the manifest and in its own lockfile,
 * plus the lockfile's entry for the core crate. `ai-hist` is a path dependency
 * (`crates/ai-hist`) whose `Cargo.toml` the core release bumps in the same
 * commit, so the plugin lock has to name the new version too or every
 * `cargo … --locked` in CI fails with "cannot update the lock file". That is
 * exactly what took `main` red after 0.18.8.
 */
const crateVersionPatterns = (crate) => [
  [
    "Cargo.toml",
    new RegExp(
      `(\\[package\\]\\r?\\nname = "${crate}"\\r?\\nversion = ")[^"]+(")`,
    ),
  ],
  ["Cargo.lock", lockVersionPattern(crate)],
  ["Cargo.lock", lockVersionPattern(coreCrate)],
];

/**
 * Stamp `version` into one plugin crate: its `Cargo.toml` `[package]` version
 * and the crate's own `[[package]]` entry in the lockfile beside it. Each
 * plugin `rust` directory is its own Cargo workspace with its own lock, so the
 * rewritten pair stays consistent and `cargo build --locked` still resolves.
 *
 * The binaries read `CARGO_PKG_VERSION`, so the release version has to reach
 * the crate before anything is compiled. Idempotent.
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
    assert.match(contents, pattern, `${path} has no version to set for ${pattern}`);
    await writeFile(
      path,
      contents.replace(
        pattern,
        (_, prefix, suffix) => prefix + version + suffix,
      ),
    );
    if (!written.includes(path)) written.push(path);
  }
  return written;
}

/** The out-of-tree consumer that builds against the crates.io core crate. */
export const consumerExample = "examples/rust-consumer";
/** The consumer's registry dependency line on the core crate. */
const consumerDependencyPattern = new RegExp(
  `(^${coreCrate} = ")[^"]+(")`,
  "m",
);
/**
 * The consumer lock's `[[package]]` entry for the core crate: its version and,
 * when the entry came from the registry, the checksum of that version.
 */
const consumerLockPattern = new RegExp(
  `(\\[\\[package\\]\\]\\r?\\nname = "${coreCrate}"\\r?\\nversion = ")([^"]+)(")` +
    `((?:\\r?\\nsource = "[^"]+")?)((?:\\r?\\nchecksum = "[^"]+")?)`,
);

/**
 * Stamp `version` into the out-of-tree consumer example
 * (`examples/rust-consumer`): its `ai-hist = "<version>"` dependency and the
 * core crate's entry in the lockfile beside it. The example is not a
 * workspace member on purpose — it resolves the crate from crates.io — and CI
 * builds it twice: on every pull request with `[patch.crates-io]` pointing at
 * `crates/ai-hist`, which Cargo only applies while the workspace version
 * satisfies the example's requirement, and nightly against the published
 * crate. A release that moved the crate without moving this requirement would
 * turn the patch into a silent no-op and leave the nightly job on the previous
 * release.
 *
 * The lock entry's checksum is the digest of the *previous* version's tarball.
 * It is dropped when the version moves: Cargo fills a missing checksum in on
 * the next resolve, but a stale one fails every build with a checksum
 * mismatch. When the version is unchanged nothing is touched, so a re-run on
 * an already-stamped checkout is byte-identical. Idempotent.
 *
 * @returns the paths written, in order.
 */
async function setConsumerExampleVersion(root, version) {
  const written = [];
  const manifestPath = resolve(root, consumerExample, "Cargo.toml");
  const manifest = await readFile(manifestPath, "utf8");
  assert.match(
    manifest,
    consumerDependencyPattern,
    `${manifestPath} has no ${coreCrate} dependency to set`,
  );
  await writeFile(
    manifestPath,
    manifest.replace(
      consumerDependencyPattern,
      (_, prefix, suffix) => prefix + version + suffix,
    ),
  );
  written.push(manifestPath);

  const lockPath = resolve(root, consumerExample, "Cargo.lock");
  const lock = await readFile(lockPath, "utf8");
  assert.match(lock, consumerLockPattern, `${lockPath} has no ${coreCrate} entry`);
  await writeFile(
    lockPath,
    lock.replace(
      consumerLockPattern,
      (_, prefix, current, suffix, source, checksum) =>
        prefix +
        version +
        suffix +
        source +
        (current === version ? checksum : ""),
    ),
  );
  written.push(lockPath);
  return written;
}

/**
 * Stamp `version` into the core crate itself: `crates/ai-hist/Cargo.toml` and
 * its entry in the root `Cargo.lock`. Every plugin crate depends on it by path,
 * and Cargo compares that manifest's version with the plugin lockfile's entry,
 * so the two must move together in the same checkout. The publish job stamps
 * these files too (same version, so this is a no-op there); the helper matrix
 * and the plugin job run this script on their own checkouts, where nothing else
 * does. Idempotent.
 *
 * @returns the paths written, in order.
 */
async function setCoreCrateVersion(root, version) {
  const written = [];
  const files = [
    [
      resolve(root, "crates", coreCrate, "Cargo.toml"),
      new RegExp(
        `(\\[package\\]\\r?\\nname = "${coreCrate}"\\r?\\nversion = ")[^"]+(")`,
      ),
    ],
    [resolve(root, "Cargo.lock"), lockVersionPattern(coreCrate)],
  ];
  for (const [path, pattern] of files) {
    const contents = await readFile(path, "utf8");
    assert.match(contents, pattern, `${path} has no ${coreCrate} version to set`);
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
 * Set `version` on the core crate, then the seven optional helper pins, the
 * public `ai-hist` peer range and the matching lockfile coordinates for both
 * optional plugins, plus the version of each plugin's Rust crate. Idempotent.
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
  const written = await setCoreCrateVersion(root, version);
  written.push(...(await setConsumerExampleVersion(root, version)));
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
      const name = packageName(info, platform);
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
      // Only the configured helper packages follow the release version; any
      // other optional dependency keeps the version the manifest declares.
      const helperNames = new Set(
        Object.keys(platforms).map(
          (platform) => packageName(info, platform),
        ),
      );
      rootEntry.optionalDependencies = Object.fromEntries(
        Object.entries(rootEntry.optionalDependencies).map(
          ([name, current]) => [name, helperNames.has(name) ? version : current],
        ),
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

    // The helper executable ships under this same release version.
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

/** Diff the `ai-hist` crate's public API against its checked-in snapshot.
 *
 * Cargo semver is the Rust contract (docs/releasing.md), and the snapshot in
 * `crates/ai-hist/public-api.txt` is what makes a change to that contract
 * visible in review: CI regenerates the listing with `cargo public-api` on the
 * crate's default features and fails when it differs, so a pull request that
 * moves the surface has to update the file — and add a `### Rust API`
 * changelog entry — in the same change. The listing must also name no
 * `rusqlite` type: an embedder on the default features never holds a raw
 * connection, and a signature that takes one puts a dependency's types in the
 * contract.
 *
 *   node scripts/check-public-api.mjs            # diff, exit 1 on drift
 *   node scripts/check-public-api.mjs --update   # rewrite the snapshot
 *
 * Needs `cargo-public-api` (`cargo install cargo-public-api --locked`) and a
 * nightly toolchain, which rustdoc's JSON output requires; the tool selects
 * `+nightly` itself. `PUBLIC_API_TOOLCHAIN` overrides that toolchain name.
 */
import { spawnSync } from "node:child_process";
import { readFile, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

const repositoryRoot = fileURLToPath(new URL("../", import.meta.url));
export const snapshotPath = "crates/ai-hist/public-api.txt";
export const manifestPath = "crates/ai-hist/Cargo.toml";
/** A crate whose types must never appear in the default public surface. */
export const forbiddenCrates = ["rusqlite"];

/** One line per public item, sorted, trailing whitespace and blank lines gone. */
export function normalizeListing(text) {
  return text
    .split(/\r?\n/)
    .map((line) => line.trimEnd())
    .filter((line) => line.length > 0);
}

/** Lines only in `expected` (removed) and only in `actual` (added). */
export function diffListings(expected, actual) {
  const before = new Set(expected);
  const after = new Set(actual);
  return {
    removed: expected.filter((line) => !after.has(line)),
    added: actual.filter((line) => !before.has(line)),
  };
}

/** Public items that name a type from a crate the contract must not leak. */
export function forbiddenItems(lines, crates = forbiddenCrates) {
  const patterns = crates.map((crate) => new RegExp(`\\b${crate}::`));
  return lines.filter((line) => patterns.some((pattern) => pattern.test(line)));
}

/** Run `cargo public-api` and return its normalized listing. */
export function currentListing(root = repositoryRoot) {
  const args = [
    "public-api",
    "--manifest-path",
    resolve(root, manifestPath),
    // Blanket, auto-trait and derived impls are rustc/nightly and dependency
    // noise, not contract; without omitting them the snapshot would move with
    // every toolchain.
    "--omit",
    "blanket-impls,auto-trait-impls,auto-derived-impls",
    "--color",
    "never",
  ];
  if (process.env.PUBLIC_API_TOOLCHAIN) {
    args.push("--toolchain", process.env.PUBLIC_API_TOOLCHAIN);
  }
  const result = spawnSync("cargo", args, {
    cwd: root,
    encoding: "utf8",
    maxBuffer: 64 * 1024 * 1024,
  });
  if (result.error) throw result.error;
  if (result.status !== 0) {
    throw new Error(
      `cargo public-api exited ${result.status}\n${result.stderr}`,
    );
  }
  return normalizeListing(result.stdout);
}

export async function main(argv, root = repositoryRoot) {
  const update = argv.includes("--update");
  const actual = currentListing(root);
  const leaked = forbiddenItems(actual);
  if (leaked.length > 0) {
    console.error(
      `The default public API of ai-hist names ${forbiddenCrates.join("/")} types; the crate must not leak a raw database surface:`,
    );
    for (const line of leaked) console.error(`  ${line}`);
    return 1;
  }
  const path = resolve(root, snapshotPath);
  if (update) {
    await writeFile(path, actual.join("\n") + "\n");
    console.log(`wrote ${actual.length} public items to ${snapshotPath}`);
    return 0;
  }
  let expected;
  try {
    expected = normalizeListing(await readFile(path, "utf8"));
  } catch (error) {
    if (error.code !== "ENOENT") throw error;
    console.error(
      `${snapshotPath} is missing; generate it with node scripts/check-public-api.mjs --update`,
    );
    return 1;
  }
  const { removed, added } = diffListings(expected, actual);
  if (removed.length === 0 && added.length === 0) {
    console.log(`${snapshotPath} matches the crate (${actual.length} public items)`);
    return 0;
  }
  console.error(`The public API of ai-hist differs from ${snapshotPath}:`);
  for (const line of removed) console.error(`- ${line}`);
  for (const line of added) console.error(`+ ${line}`);
  console.error(
    "\nIf the change is intended, run `node scripts/check-public-api.mjs --update`," +
      " commit the snapshot, and add a `### Rust API` entry to CHANGELOG.md (docs/releasing.md).",
  );
  return 1;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  process.exitCode = await main(process.argv.slice(2));
}

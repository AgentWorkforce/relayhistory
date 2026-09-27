#!/usr/bin/env node
/**
 * Claim the optional history plugin package names on npm.
 *
 * npm OIDC trusted publishing can publish new *versions* of a package that
 * already exists, but it cannot *create* a name. The release workflow is
 * tokenless, so a name's first publish fails:
 *
 *   npm error code E404
 *   npm error 404 Not Found - PUT https://registry.npmjs.org/@relayhistory%2fprovider-sources-darwin-arm64
 *   npm error 404 The requested resource '@relayhistory/provider-sources-darwin-arm64@0.18.3'
 *                 could not be found or you do not have permission to access it.
 *
 * Each name has to exist once, published by a human account; CI owns it from
 * then on. This publishes a placeholder for every name that is still missing.
 *
 * A placeholder rather than the real package because the seven platform
 * packages each carry a cross-compiled binary for their target: no single
 * machine can build all of them. The real artifacts come from the release.
 *
 * The placeholder is published at 0.0.0 on purpose. A version publishes exactly
 * once, so claiming a name at a release version would make the next release
 * fail EPUBLISHCONFLICT on it. 0.0.0 sits below every release, is transparently
 * not a real build, and never resolves: each plugin pins its helpers to its own
 * exact version, so nothing ever asks for 0.0.0.
 *
 * Names come from the package contract rather than a list repeated here, so
 * adding a plugin or a platform needs no change in this file.
 *
 * Usage, from the repository root:
 *   node scripts/claim-plugin-package-names.mjs --dry-run
 *   node scripts/claim-plugin-package-names.mjs
 *
 * Safe to re-run: names already on the registry are skipped, so a partial
 * failure is resolved by running it again.
 */

import { execFileSync } from "node:child_process";
import { mkdtempSync, realpathSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

import { packageName, platforms, plugins } from "./history-package-contract.mjs";

/**
 * Deliberately below every release version. See the note above: this must never
 * collide with a version the release workflow intends to publish.
 */
export const PLACEHOLDER_VERSION = "0.0.0";

/** Every name a release publishes: each plugin, plus one helper per platform. */
export function publishedNames() {
  const names = [];
  for (const info of Object.values(plugins)) {
    names.push({ name: packageName(info), platform: undefined, info });
    for (const platform of Object.keys(platforms)) {
      names.push({ name: packageName(info, platform), platform, info });
    }
  }
  return names;
}

/**
 * A minimal placeholder manifest.
 *
 * Platform entries keep their real os/cpu/libc so a placeholder cannot be
 * installed onto a machine it was never meant for during the window before the
 * release replaces it.
 */
export function placeholderManifest({ name, platform, info }) {
  const [os, cpu, libc] = platform ? platforms[platform] : [];
  return {
    name,
    version: PLACEHOLDER_VERSION,
    license: "MIT",
    description: platform
      ? `Placeholder reserving the ${info.name} ${platform} helper name. Replaced by the first release.`
      : `Placeholder reserving the ${info.name} package name. Replaced by the first release.`,
    ...(platform ? { os: [os], cpu: [cpu], ...(libc ? { libc: [libc] } : {}) } : {}),
    publishConfig: { access: "public" },
  };
}

/** Whether npm already knows the name. Anything else means it is free to claim. */
function published(name) {
  try {
    execFileSync("npm", ["view", name, "version"], { stdio: "pipe" });
    return true;
  } catch {
    return false;
  }
}

function claim(entry, otp) {
  const directory = mkdtempSync(join(tmpdir(), "relayhistory-claim-"));
  writeFileSync(
    join(directory, "package.json"),
    `${JSON.stringify(placeholderManifest(entry), null, 2)}\n`,
  );
  // stdio is inherited, not piped. An account with 2FA on publish makes npm run
  // an interactive re-auth; piping it hides the prompt and the URL, so every
  // publish fails on a flow nobody could answer. Inheriting costs the captured
  // error text, so success is confirmed against the registry instead.
  execFileSync(
    "npm",
    ["publish", "--access", "public", ...(otp ? [`--otp=${otp}`] : [])],
    { cwd: directory, stdio: "inherit" },
  );
}

/** Read `--otp=<code>`, so a 2FA account can claim without a prompt per package. */
function otpFromArgv(argv) {
  const flag = argv.find((argument) => argument.startsWith("--otp="));
  return flag ? flag.slice("--otp=".length) : undefined;
}

async function main() {
  const dryRun = process.argv.includes("--dry-run");

  // Fail once, before touching the registry, rather than once per name.
  if (!dryRun) {
    try {
      const who = execFileSync("npm", ["whoami"], { stdio: "pipe" }).toString().trim();
      console.log(`npm user: ${who}\n`);
    } catch {
      console.error("Not logged in to npm. Run `npm login`, then re-run this.");
      process.exitCode = 1;
      return;
    }
  }

  const all = publishedNames();

  console.log(`Checking ${all.length} names on the registry...\n`);
  const missing = [];
  for (const entry of all) {
    const have = published(entry.name);
    console.log(`  ${have ? "have   " : "MISSING"}  ${entry.name}`);
    if (!have) missing.push(entry);
  }

  if (missing.length === 0) {
    console.log("\nEvery name exists. Nothing to claim — run the release workflow.");
    return;
  }

  console.log(
    `\n${missing.length} to claim at ${PLACEHOLDER_VERSION}, each with --access public.`,
  );
  if (dryRun) {
    console.log("--dry-run: stopping here.");
    return;
  }

  const otp = otpFromArgv(process.argv);
  const failures = [];
  for (const entry of missing) {
    console.log(`\nclaiming ${entry.name}`);
    try {
      // npm's exit status is the authority on whether the PUT succeeded.
      //
      // Do NOT confirm by reading the registry back here. A newly created name
      // serves 404 for roughly two minutes, so an immediate read reports every
      // successful claim as a failure — which it did, for every one, while
      // npm had printed `+ name@0.0.0` for each one.
      claim(entry, otp);
      console.log(`  claimed ${entry.name}`);
    } catch {
      // npm has already explained itself on the inherited stderr.
      console.log(`  FAILED ${entry.name}`);
      failures.push(entry.name);
    }
  }

  if (failures.length > 0) {
    console.error(`\n${failures.length} failed:`);
    for (const name of failures) console.error(`  ${name}`);
    console.error("\nnpm printed the reason above each failure.");
    console.error("If it asked you to authenticate, finish that once and re-run.");
    process.exitCode = 1;
    return;
  }

  console.log(
    "\nNote: a new name serves 404 for a couple of minutes before the registry" +
      "\nreturns it. A --dry-run straight after this may still report them missing.",
  );

  console.log(`\nClaimed ${missing.length}. Now run the release:`);
  console.log(
    "  gh workflow run publish-napi.yml --ref main -f version=patch -f dry_run=false -f plugins=true",
  );
}

// Importable for tests without publishing anything: main() runs only when this
// file is the entry point, compared by resolved path rather than by name.
if (process.argv[1] && realpathSync(process.argv[1]) === fileURLToPath(import.meta.url)) {
  await main();
}

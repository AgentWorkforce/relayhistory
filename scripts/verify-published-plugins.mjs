#!/usr/bin/env node
/**
 * Prove the published plugin packages are actually installable and loadable.
 *
 * Everything else in this release verifies artifacts *before* they are
 * published: `smoke-history-packages.mjs --helpers` installs local tarballs,
 * and the packaging suite checks generated manifests. Nothing looked at the
 * registry afterwards, so a publish step that failed — or succeeded into the
 * wrong names — ended the job without complaint.
 *
 * That gap let six separate faults ship undetected, each hiding the next:
 * a bare tarball path npm read as GitHub shorthand; a blank NODE_AUTH_TOKEN
 * that suppressed OIDC; an npm 10 that cannot mint an OIDC token; package
 * names that had never been created; a trusted publisher bound to a workflow
 * file name that did not exist; and a manifest without `repository`, which
 * --provenance rejects. Each needed a release to discover, because the only
 * thing that exercised the publish path was publishing.
 *
 * This installs what was just published, from the registry, into a clean
 * project, and imports it. It runs after the publish step, so a broken publish
 * fails the release that caused it rather than the next one.
 *
 * Usage:  node scripts/verify-published-plugins.mjs <version>
 */

import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";

import { packageName, platforms, plugins } from "./history-package-contract.mjs";
import { installWithRegistryRetry } from "./npm-install-with-registry-retry.mjs";

const version = process.argv[2];
assert.match(
  version ?? "",
  /^\d+\.\d+\.\d+(?:-[\w.-]+)?$/,
  "Usage: verify-published-plugins.mjs <version>",
);

/** Every name this release published, JavaScript package and platform helper. */
function expectedNames() {
  const names = [];
  for (const info of Object.values(plugins)) {
    names.push(packageName(info));
    for (const platform of Object.keys(platforms)) {
      names.push(packageName(info, platform));
    }
  }
  return names;
}

/**
 * Registry metadata for one name at one version.
 *
 * `--prefer-online` because npm's cache will happily serve a 404 it saw
 * seconds earlier, which is precisely the window a fresh publish sits in.
 */
function viewed(name, field) {
  const result = spawnSync(
    "npm",
    ["view", "--prefer-online", `${name}@${version}`, field],
    { encoding: "utf8" },
  );
  return result.status === 0 ? (result.stdout ?? "").trim() : "";
}

async function main() {
  const names = expectedNames();
  console.log(`Verifying ${names.length} published plugin packages at ${version}\n`);

  // A new name can 404 for a couple of minutes after a successful publish, so
  // absence is only meaningful once the retrying install below has settled.
  const project = await mkdtemp(join(tmpdir(), "relayhistory-verify-"));
  try {
    await writeFile(
      join(project, "package.json"),
      `${JSON.stringify({ name: "verify-published-plugins", private: true, version: "0.0.0" }, null, 2)}\n`,
    );

    // Installing the JavaScript packages pulls each one's platform helper for
    // this machine through optionalDependencies, so a helper published at the
    // wrong version or not at all fails here rather than in a user's install.
    const jsPackages = Object.values(plugins).map(
      (info) => `${packageName(info)}@${version}`,
    );
    console.log(`installing ${jsPackages.join(" ")}`);
    const install = await installWithRegistryRetry([
      "--prefix",
      project,
      "--no-save",
      ...jsPackages,
    ]);
    assert.equal(
      install.status,
      0,
      `install failed:\n${install.stdout}\n${install.stderr}`,
    );

    // Present at the right version, and pointing at this repository: an absent
    // repository field is what --provenance rejects, and it is invisible until
    // the publish is attempted.
    const missing = [];
    for (const name of names) {
      const published = viewed(name, "version");
      if (published !== version) {
        missing.push(`${name}: expected ${version}, registry has ${published || "nothing"}`);
        continue;
      }
      const repository = viewed(name, "repository.url");
      if (!repository) missing.push(`${name}: published without repository.url`);
    }
    assert.equal(missing.length, 0, `\n  ${missing.join("\n  ")}`);

    // Installable is not the same as loadable. Import each package and confirm
    // it exposes the plugin factory the SDK registers.
    for (const info of Object.values(plugins)) {
      const name = packageName(info);
      const probe = spawnSync(
        process.execPath,
        [
          "--input-type=module",
          "-e",
          `import * as plugin from ${JSON.stringify(name)};` +
            `if (typeof plugin.createHistoryPlugin !== "function") {` +
            `throw new Error(${JSON.stringify(name)} + " does not export createHistoryPlugin");}`,
        ],
        { cwd: project, encoding: "utf8" },
      );
      assert.equal(probe.status, 0, `${name} failed to load:\n${probe.stderr}`);
      console.log(`  loaded ${name}`);
    }

    console.log(`\nAll ${names.length} packages published and installable at ${version}.`);
  } finally {
    await rm(project, { recursive: true, force: true });
  }
}

await main();

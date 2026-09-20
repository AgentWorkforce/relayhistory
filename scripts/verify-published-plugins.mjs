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
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import { createRequire } from "node:module";

import { packageName, platforms, plugins } from "./history-package-contract.mjs";
import { installWithRegistryRetry, isRegistryVisibilityFailure } from "./npm-install-with-registry-retry.mjs";

/** This machine's platform key, to check the helper that should have installed. */
function currentPlatform() {
  const libc =
    process.platform === "linux"
      ? (process.report?.getReport()?.header?.glibcVersionRuntime ? "gnu" : "musl")
      : undefined;
  const key = [process.platform, process.arch, libc].filter(Boolean).join("-");
  const known = Object.keys(platforms);
  const match = known.find((candidate) => candidate === key)
    ?? known.find((candidate) => candidate.startsWith(`${process.platform}-${process.arch}`));
  assert.ok(match, `no platform entry for ${key}; known: ${known.join(", ")}`);
  return match;
}

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

/** Read one exact manifest using a fresh cache on every attempt. */
function viewed(name, version) {
  const cache = mkdtempSync(join(tmpdir(), "relayhistory-view-cache-"));
  try {
    return spawnSync(
      "npm",
      ["view", "--prefer-online", "--json", `${name}@${version}`],
      { encoding: "utf8", env: { ...process.env, npm_config_cache: cache } },
    );
  } finally {
    rmSync(cache, { recursive: true, force: true });
  }
}

/** Wait for every platform, not just the packages installable on this runner. */
export async function waitForPublishedPackages(version, {
  attempts = 60,
  delayMs = 5_000,
  runView = viewed,
  sleep = (ms) => new Promise((resolveDelay) => setTimeout(resolveDelay, ms)),
  log = (message) => console.error(message),
} = {}) {
  const pending = new Set(expectedNames());
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    const missing = [];
    for (const name of pending) {
      const result = runView(name, version);
      const context = `npm view ${name}@${version}`;
      if (result.error) throw new Error(`${context}: ${result.error.message}`, { cause: result.error });
      if (result.status !== 0) {
        const output = [result.stdout, result.stderr].filter(Boolean).join("\n").trim();
        const diagnostic = `${context} failed (exit ${result.status}, signal ${result.signal ?? "none"}):\n${output}`;
        if (!isRegistryVisibilityFailure(output)) throw new Error(diagnostic);
        missing.push(diagnostic);
        continue;
      }
      let metadata;
      try {
        metadata = JSON.parse(result.stdout);
      } catch (error) {
        throw new Error(`${context}: invalid JSON: ${error.message}`, { cause: error });
      }
      // npm versions differ: an exact-version view can return an object or a
      // singleton array. Never accept multiple versions from an exact lookup.
      const manifests = Array.isArray(metadata) ? metadata : [metadata];
      assert.equal(manifests.length, 1, `${context}: expected exactly one manifest`);
      const [manifest] = manifests;
      assert.ok(manifest && typeof manifest === "object", `${context}: invalid manifest`);
      assert.equal(manifest.version, version, `${name}: registry returned the wrong version`);
      assert.ok(manifest.repository?.url, `${name}@${version}: published without repository.url`);
      pending.delete(name);
    }
    if (pending.size === 0) return;
    if (attempt === attempts) {
      throw new Error(`npm registry did not expose all plugin packages after ${attempts} attempts:\n${missing.join("\n")}`);
    }
    log(`Waiting for ${[...pending].join(", ")} at ${version} (attempt ${attempt}/${attempts}); retrying in ${delayMs}ms`);
    await sleep(delayMs);
  }
}

async function main(version) {
  assert.match(
    version ?? "",
    /^\d+\.\d+\.\d+(?:-[\w.-]+)?$/,
    "Usage: verify-published-plugins.mjs <version>",
  );
  const names = expectedNames();
  console.log(`Verifying ${names.length} published plugin packages at ${version}\n`);

  // Registry visibility is independent for each package. A successful install
  // of the JS packages says nothing about helpers for other operating systems,
  // and npm can silently omit this runner's helper as an optional dependency.
  await waitForPublishedPackages(version);

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
    // 5 minutes, not the helper's 90s default. A newly created name took ~120s
    // to become readable when these were first published, so the default budget
    // failed the very release this step exists to protect (0.18.7: every
    // package published correctly, this step reported ETARGET and failed the
    // run). The cost of waiting is a slow release; the cost of giving up early
    // is a false alarm on a good one.
    // Resolves on success and throws on exhaustion — it returns no result to
    // inspect. Reading a `.status` off it crashed this step even when the
    // install had worked.
    await installWithRegistryRetry(
      ["--prefix", project, "--no-save", ...jsPackages],
      { attempts: 60, delayMs: 5_000 },
    );

    // npm skips an optionalDependency it cannot resolve and still exits 0, so a
    // helper missing from the registry produces a silent half-install rather
    // than a failure. Check this machine's helper actually landed.
    const require_ = createRequire(join(project, "noop.js"));
    for (const info of Object.values(plugins)) {
      const helper = packageName(info, currentPlatform());
      try {
        require_.resolve(`${helper}/package.json`);
        console.log(`  installed ${helper}`);
      } catch {
        assert.fail(
          `${helper} did not install: npm skips an unresolvable optional dependency silently`,
        );
      }
    }

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

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  await main(process.argv[2]);
}

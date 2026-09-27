import assert from "node:assert/strict";
import test from "node:test";

import { packageName, platforms, plugins } from "./history-package-contract.mjs";
import {
  PLACEHOLDER_VERSION,
  placeholderManifest,
  publishedNames,
} from "./claim-plugin-package-names.mjs";

test("claims exactly the names a release publishes", () => {
  const claimed = publishedNames().map((entry) => entry.name);
  const expected = [];
  for (const info of Object.values(plugins)) {
    expected.push(packageName(info));
    for (const platform of Object.keys(platforms)) {
      expected.push(packageName(info, platform));
    }
  }
  // Derived from the same contract the workflow uses, so a new plugin or
  // platform is claimed without touching the claim script.
  assert.deepEqual(claimed.sort(), expected.sort());
  assert.equal(claimed.length, Object.keys(plugins).length * (Object.keys(platforms).length + 1));
});

test("the placeholder version can never collide with a release", () => {
  // A version publishes exactly once. Claiming a name at a version the release
  // wants would fail that release with EPUBLISHCONFLICT, so this must stay
  // below every real version — and below the 0.x line the repo ships on.
  assert.equal(PLACEHOLDER_VERSION, "0.0.0");
  const [major, minor, patch] = PLACEHOLDER_VERSION.split(".").map(Number);
  assert.equal(major + minor + patch, 0);
});

test("platform placeholders keep their real install constraints", () => {
  for (const entry of publishedNames()) {
    const manifest = placeholderManifest(entry);
    assert.equal(manifest.version, PLACEHOLDER_VERSION);
    assert.equal(manifest.publishConfig.access, "public");
    assert.match(manifest.description, /Placeholder reserving/);

    if (!entry.platform) {
      // The plugin package itself is platform-independent.
      assert.equal(manifest.os, undefined);
      assert.equal(manifest.cpu, undefined);
      continue;
    }
    // A placeholder must not be installable on a platform it does not target,
    // in the window before the release replaces it.
    const [os, cpu, libc] = platforms[entry.platform];
    assert.deepEqual(manifest.os, [os]);
    assert.deepEqual(manifest.cpu, [cpu]);
    assert.deepEqual(manifest.libc, libc ? [libc] : undefined);
  }
});

test("importing the script publishes nothing", async () => {
  // main() is guarded on being the entry point; importing it here must be
  // inert, or a test run would publish to the registry.
  const before = process.exitCode;
  await import("./claim-plugin-package-names.mjs");
  assert.equal(process.exitCode, before);
});

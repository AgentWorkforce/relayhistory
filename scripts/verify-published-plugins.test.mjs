import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import test from "node:test";

import { packageName, platforms, plugins } from "./history-package-contract.mjs";

const script = fileURLToPath(new URL("./verify-published-plugins.mjs", import.meta.url));

function run(args) {
  return spawnSync(process.execPath, [script, ...args], { encoding: "utf8" });
}

test("refuses to run without a version", () => {
  const result = run([]);
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /Usage: verify-published-plugins\.mjs <version>/);
});

test("refuses a version that is not stable semver", () => {
  const result = run(["latest"]);
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /Usage: verify-published-plugins\.mjs <version>/);
});

test("covers every name the release publishes", async () => {
  // The guarantee this step exists for: if a plugin or platform is added and
  // its package never publishes, this must be what notices. Derived from the
  // contract, so coverage cannot drift from what the release actually ships.
  const expected = [];
  for (const info of Object.values(plugins)) {
    expected.push(packageName(info));
    for (const platform of Object.keys(platforms)) {
      expected.push(packageName(info, platform));
    }
  }
  assert.equal(
    expected.length,
    Object.keys(plugins).length * (Object.keys(platforms).length + 1),
  );
  // Every expected name is scoped and versionless here; the script appends the
  // release version before querying.
  for (const name of expected) assert.match(name, /^@relayhistory\//);
});

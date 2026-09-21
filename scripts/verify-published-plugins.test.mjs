import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import test from "node:test";

import { packageName, platforms, plugins } from "./history-package-contract.mjs";
import {
  hostLibc,
  pluginInstallArgs,
  publicRegistryEnv,
  verifyPluginManifest,
  waitForPublishedPackages,
} from "./verify-published-plugins.mjs";

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

const version = "0.19.0";
const available = {
  status: 0,
  stdout: JSON.stringify({ version, repository: { url: "https://github.com/AgentWorkforce/relayhistory" } }),
  stderr: "",
};
const quiet = { log: () => {}, sleep: async () => assert.fail("must not retry") };

test("checks every published name at the exact release version", async () => {
  const calls = [];
  await waitForPublishedPackages(version, {
    ...quiet,
    runView: (name, requestedVersion) => {
      calls.push([name, requestedVersion]);
      return available;
    },
  });
  const expected = Object.values(plugins).flatMap((info) => [
    packageName(info),
    ...Object.keys(platforms).map((platform) => packageName(info, platform)),
  ]);
  assert.deepEqual(calls, expected.map((name) => [name, version]));
});

test("accepts npm's singleton-array metadata format", async () => {
  await waitForPublishedPackages(version, {
    ...quiet,
    runView: () => ({ ...available, stdout: `[${available.stdout}]` }),
  });
});

test("waits for independently delayed helpers even when both JS packages are visible", async () => {
  // Reproduce the four missing helpers from the 0.19.0 release. The two JS
  // packages and all other platforms are already visible on the first pass.
  const delayed = new Map([
    ["@relayhistory/capture-darwin-arm64", 1],
    ["@relayhistory/capture-darwin-x64", 2],
    ["@relayhistory/provider-sources-linux-arm64-musl", 3],
    ["@relayhistory/provider-sources-win32-x64-msvc", 4],
  ]);
  const calls = new Map();
  const waits = [];
  await waitForPublishedPackages(version, {
    ...quiet,
    attempts: 5,
    delayMs: 7,
    sleep: async (ms) => waits.push(ms),
    runView: (name) => {
      const count = (calls.get(name) ?? 0) + 1;
      calls.set(name, count);
      return count <= (delayed.get(name) ?? 0)
        ? { status: 1, stdout: "", stderr: `npm error code ${count % 2 ? "E404" : "ETARGET"}` }
        : available;
    },
  });
  assert.deepEqual(waits, [7, 7, 7, 7]);
  for (const [name, count] of calls) assert.equal(count, (delayed.get(name) ?? 0) + 1, name);
});

test("fails after bounded retries with the missing package and original npm error", async () => {
  const missing = "@relayhistory/provider-sources-win32-x64-msvc";
  let lookups = 0;
  const waits = [];
  await assert.rejects(waitForPublishedPackages(version, {
    ...quiet,
    attempts: 3,
    delayMs: 1,
    sleep: async (ms) => waits.push(ms),
    runView: (name) => {
      if (name !== missing) return available;
      lookups += 1;
      return { status: 1, stdout: "", stderr: "npm error code ETARGET\nNo matching version found" };
    },
  }), (error) => {
    assert.match(error.message, /after 3 attempts/);
    assert.ok(error.message.includes(`${missing}@${version}`));
    assert.match(error.message, /npm error code ETARGET\nNo matching version found/);
    assert.ok(!error.message.includes("capture-darwin"));
    return true;
  });
  assert.equal(lookups, 3);
  assert.deepEqual(waits, [1, 1]);
});

test("authentication and network errors fail immediately with npm diagnostics", async () => {
  for (const code of ["E401", "E403", "ENOTFOUND"]) {
    let calls = 0;
    await assert.rejects(waitForPublishedPackages(version, {
      ...quiet,
      runView: () => {
        calls += 1;
        return { status: 1, stdout: "", stderr: `npm error code ${code}` };
      },
    }), new RegExp(`npm view @relayhistory/capture@0\\.19\\.0 failed.*\\n.*${code}`));
    assert.equal(calls, 1);
  }
});

test("process launch failures preserve the package context and original error", async () => {
  const cause = new Error("spawnSync npm ENOENT");
  await assert.rejects(waitForPublishedPackages(version, {
    ...quiet,
    runView: () => ({ status: null, error: cause }),
  }), (error) => {
    assert.equal(error.cause, cause);
    assert.match(error.message, /@relayhistory\/capture@0\.19\.0.*ENOENT/);
    return true;
  });
});

test("verify project depends on both JS packages at the release version", () => {
  const manifest = verifyPluginManifest(version);
  assert.deepEqual(manifest.dependencies, {
    "@relayhistory/capture": version,
    "@relayhistory/provider-sources": version,
  });
  assert.equal("optionalDependencies" in manifest, false);
});

test("linux install tells npm the helper's libc family", () => {
  assert.deepEqual(
    pluginInstallArgs("/tmp/verify", "glibc"),
    ["--prefix", "/tmp/verify", "--libc=glibc"],
  );
  assert.deepEqual(
    pluginInstallArgs("/tmp/verify", "musl"),
    ["--prefix", "/tmp/verify", "--libc=musl"],
  );
  assert.deepEqual(pluginInstallArgs("/tmp/verify", null), ["--prefix", "/tmp/verify"]);
});

test("public registry install drops the publish job's npm token and userconfig", () => {
  const env = publicRegistryEnv({
    PATH: "/usr/bin",
    NODE_AUTH_TOKEN: "secret",
    NPM_CONFIG_USERCONFIG: "/tmp/publish.npmrc",
    npm_config_userconfig: "/tmp/publish.npmrc",
  });
  assert.equal(env.PATH, "/usr/bin");
  assert.equal("NODE_AUTH_TOKEN" in env, false);
  assert.equal("NPM_CONFIG_USERCONFIG" in env, false);
  assert.equal("npm_config_userconfig" in env, false);
});

test("host libc is the contract field for this machine's platform", () => {
  const libc = hostLibc();
  if (process.platform === "linux") {
    assert.ok(libc === "glibc" || libc === "musl");
    const key = `${process.platform}-${process.arch}-${libc === "glibc" ? "gnu" : "musl"}`;
    assert.equal(platforms[key][2], libc);
  } else {
    assert.equal(libc, undefined);
  }
});

test("invalid published manifests fail immediately instead of being treated as propagation", async () => {
  for (const [stdout, message] of [
    [JSON.stringify({ version: "0.18.9", repository: { url: "repo" } }), /wrong version/],
    [JSON.stringify({ version }), /published without repository.url/],
    ["not JSON", /@relayhistory\/capture@0\.19\.0: invalid JSON/],
    ["[]", /expected exactly one manifest/],
    [`[${available.stdout},${available.stdout}]`, /expected exactly one manifest/],
    ["null", /invalid manifest/],
  ]) {
    await assert.rejects(waitForPublishedPackages(version, {
      ...quiet,
      runView: () => ({ status: 0, stdout, stderr: "" }),
    }), message);
  }
});

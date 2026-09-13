/** Refuse plugin publication until its minimum public core is actually compatible. */
import assert from "node:assert/strict";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { spawnSync } from "node:child_process";
import { npmCli, plugins } from "./history-package-contract.mjs";

export function peerMinimum(range) {
  const match = /^(?:\^)?(\d+\.\d+\.\d+)$/.exec(range ?? "");
  assert.ok(
    match,
    "Plugin release requires an exact or caret stable ai-hist peer range with an explicit minimum",
  );
  return match[1];
}
export function assertPublishedContract(sdk, native, actualVersion, minimum) {
  assert.equal(
    actualVersion,
    minimum,
    "Release gate must inspect the declared peer minimum, not a newer core",
  );
  assert.equal(
    native.nativeContractVersion?.(),
    14,
    "Published peer minimum must implement native contract 14",
  );
  for (const method of [
    "historyDelivery",
    "applySourceEvidence",
    "getSourceObservation",
  ]) {
    assert.equal(
      typeof native[method],
      "function",
      `Published native addon lacks ${method}`,
    );
  }
  for (const method of [
    "HistoryPluginRegistry",
    "createHistoryDelivery",
    "historyDeliveryStatus",
    "drainHistoryDelivery",
    "controlHistoryDelivery",
    "discoverSourcePlugins",
    "hydrateSourcePlugin",
    "getSourceObservation",
  ]) {
    assert.equal(
      typeof sdk[method],
      "function",
      `Published core lacks ${method}`,
    );
  }
  assert.equal(
    sdk.login,
    undefined,
    "Published core still exposes legacy cloud operations",
  );
  assert.equal(
    native.cloudLoadAuth,
    undefined,
    "Published native addon still owns cloud auth",
  );
  const registry = new sdk.HistoryPluginRegistry();
  assert.deepEqual(registry.sourceConnectors(), []);
}
export async function verifyPublishedCore(selection = "all") {
  assert.ok(
    selection === "all" || plugins[selection],
    "Unknown optional plugin selection",
  );
  const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
  const versions = new Set();
  for (const plugin of Object.keys(plugins)) {
    if (selection !== "all" && selection !== plugin) continue;
    const manifest = JSON.parse(
      await readFile(join(root, "plugins", plugin, "sdk/package.json"), "utf8"),
    );
    versions.add(peerMinimum(manifest.peerDependencies?.["ai-hist"]));
  }
  const npmEntry = npmCli();
  for (const version of versions) {
    const project = await mkdtemp(join(tmpdir(), "history-published-core-"));
    const env = {
      ...process.env,
      HOME: project,
      USERPROFILE: project,
      XDG_DATA_HOME: join(project, "share"),
    };
    delete env.NODE_PATH;
    try {
      await writeFile(
        join(project, "package.json"),
        JSON.stringify({ private: true, type: "module" }),
      );
      const run = (args) => {
        const result = spawnSync(process.execPath, args, {
          cwd: project,
          env,
          encoding: "utf8",
          timeout: 120_000,
          maxBuffer: 4 * 1024 * 1024,
        });
        if (result.error) throw result.error;
        assert.equal(
          result.status,
          0,
          `Published ai-hist@${version} is not ready for optional plugins. Release compatible core first, then set each plugin's peer minimum to that actual version.\n${result.stderr}`,
        );
      };
      // No checkout tarballs, manifest rewrites, lifecycle builds, or local links.
      run([
        npmEntry,
        "install",
        "--registry=https://registry.npmjs.org",
        "--ignore-scripts",
        "--strict-peer-deps",
        "--no-audit",
        "--no-fund",
        `ai-hist@${version}`,
      ]);
      const probe = join(project, "verify.mjs");
      await writeFile(
        probe,
        `import assert from 'node:assert/strict';
import * as sdk from 'ai-hist';
import { createRequire } from 'node:module';
import { readFile } from 'node:fs/promises';
import { assertPublishedContract } from ${JSON.stringify(pathToFileURL(fileURLToPath(import.meta.url)).href)};
const require = createRequire(import.meta.url);
const native = createRequire(require.resolve('ai-hist'))('ai-hist-native');
const manifest = JSON.parse(await readFile(new URL('./node_modules/ai-hist/package.json', import.meta.url), 'utf8'));
assertPublishedContract(sdk, native, manifest.version, ${JSON.stringify(version)});
assert.deepEqual(await sdk.historyDeliveryStatus(undefined, {dbPath:${JSON.stringify(join(project, "fixture.db"))}}), []);
`,
      );
      run([probe]);
      console.log(
        `Published ai-hist@${version} supports the optional plugin contract`,
      );
    } finally {
      await rm(project, { recursive: true, force: true });
    }
  }
}
if (
  process.argv[1] &&
  resolve(process.argv[1]) === fileURLToPath(import.meta.url)
) {
  await verifyPublishedCore(process.argv[2] ?? "all");
}

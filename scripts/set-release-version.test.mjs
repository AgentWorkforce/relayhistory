import assert from "node:assert/strict";
import test from "node:test";
import { spawnSync } from "node:child_process";
import { mkdir, mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { platforms, plugins } from "./history-package-contract.mjs";
import {
  localCoreDependency,
  setReleaseVersion,
} from "./set-release-version.mjs";

const scripts = dirname(fileURLToPath(import.meta.url));
/** Deliberately older than any release, so every field has to be rewritten. */
const stale = "0.1.0";

function fixture(info) {
  const name = `@agent-relay/${info.name}`;
  const optionalDependencies = Object.fromEntries(
    Object.keys(platforms).map((platform) => [`${name}-${platform}`, stale]),
  );
  const devDependencies = { "ai-hist": localCoreDependency };
  const peerDependencies = { "ai-hist": `^${stale}` };
  return {
    manifest: {
      name,
      version: stale,
      license: "MIT",
      peerDependencies,
      devDependencies,
      optionalDependencies,
    },
    lock: {
      name,
      version: stale,
      lockfileVersion: 3,
      requires: true,
      packages: {
        "": {
          name,
          version: stale,
          devDependencies,
          optionalDependencies: { ...optionalDependencies },
          peerDependencies: { ...peerDependencies },
        },
        "../../../sdk-ts": { name: "ai-hist", version: stale, dev: true },
        "node_modules/ai-hist": { resolved: "../../../sdk-ts", link: true },
        ...Object.fromEntries(
          Object.keys(platforms).map((platform) => [
            `node_modules/${name}-${platform}`,
            { version: stale, optional: true, license: "MIT" },
          ]),
        ),
      },
    },
  };
}

/** A throwaway checkout holding only the files a release version rewrites. */
async function stagePlugins() {
  const root = await mkdtemp(join(tmpdir(), "set-release-version-"));
  for (const [plugin, info] of Object.entries(plugins)) {
    const directory = join(root, "plugins", plugin, "sdk");
    await mkdir(directory, { recursive: true });
    const { manifest, lock } = fixture(info);
    await writeFile(
      join(directory, "package.json"),
      JSON.stringify(manifest, null, 2) + "\n",
    );
    await writeFile(
      join(directory, "package-lock.json"),
      JSON.stringify(lock, null, 2) + "\n",
    );
  }
  return root;
}
const read = (root, plugin, file) =>
  readFile(join(root, "plugins", plugin, "sdk", file), "utf8");
const files = (root) =>
  Promise.all(
    Object.keys(plugins).flatMap((plugin) =>
      ["package.json", "package-lock.json"].map((file) =>
        read(root, plugin, file),
      ),
    ),
  );

test("the release version reaches every manifest, peer range and lock coordinate", async () => {
  const root = await stagePlugins();
  const version = "9.9.9";
  const result = spawnSync(
    process.execPath,
    [join(scripts, "set-release-version.mjs"), version, root],
    { encoding: "utf8" },
  );
  assert.equal(result.status, 0, result.stderr);
  for (const [plugin, info] of Object.entries(plugins)) {
    const manifest = JSON.parse(await read(root, plugin, "package.json"));
    const lock = JSON.parse(await read(root, plugin, "package-lock.json"));
    assert.equal(manifest.version, version);
    assert.equal(manifest.peerDependencies["ai-hist"], `^${version}`);
    // Plugins keep building against the checkout SDK.
    assert.equal(manifest.devDependencies["ai-hist"], localCoreDependency);
    assert.equal(lock.version, version);
    assert.equal(lock.packages[""].version, version);
    assert.equal(lock.packages[""].peerDependencies["ai-hist"], `^${version}`);
    assert.equal(lock.packages["../../../sdk-ts"].version, version);
    for (const platform of Object.keys(platforms)) {
      const name = `@agent-relay/${info.name}-${platform}`;
      const [os, cpu, libc] = platforms[platform];
      assert.equal(manifest.optionalDependencies[name], version);
      assert.equal(lock.packages[""].optionalDependencies[name], version);
      assert.deepEqual(lock.packages[`node_modules/${name}`], {
        version,
        resolved: `https://registry.npmjs.org/${name}/-/${info.name}-${platform}-${version}.tgz`,
        optional: true,
        os: [os],
        cpu: [cpu],
        ...(libc ? { libc: [libc] } : {}),
        license: "MIT",
      });
    }
  }
});

test("re-applying the same version rewrites nothing", async () => {
  const root = await stagePlugins();
  await setReleaseVersion("1.2.3", root);
  const first = await files(root);
  await setReleaseVersion("1.2.3", root);
  assert.deepEqual(await files(root), first);
});

test("only a stable release version is accepted", async () => {
  const root = await stagePlugins();
  const before = await files(root);
  for (const version of ["", "latest", "1.2", "1.2.3-rc.1", "v1.2.3"]) {
    await assert.rejects(() => setReleaseVersion(version, root));
  }
  assert.deepEqual(await files(root), before);
});

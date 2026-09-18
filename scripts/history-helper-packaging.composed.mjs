import assert from "node:assert/strict";
import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import test from "node:test";
import {
  helperTarball,
  npmCli,
  packageName,
  platforms,
  plugins,
  validatePluginManifest,
} from "./history-package-contract.mjs";
import { verifyHelperResult } from "./verify-history-helper.mjs";

const envelope = JSON.stringify({
  version: 1,
  ok: false,
  error: { code: "INVALID_INPUT" },
});
test("valid JSON cannot conceal helper failure, signal, or diagnostics", () => {
  const result = { status: 0, signal: null, stdout: envelope, stderr: "" };
  verifyHelperResult(result);
  assert.throws(() => verifyHelperResult({ ...result, status: 7 }));
  assert.throws(() => verifyHelperResult({ ...result, signal: "SIGKILL" }));
  assert.throws(() =>
    verifyHelperResult({ ...result, stderr: "unexpected diagnostic" }),
  );
  assert.throws(() =>
    verifyHelperResult({ ...result, stdout: envelope + envelope }),
  );
});
function manifest(plugin, version) {
  return {
    name: packageName(plugins[plugin]),
    version,
    peerDependencies: { "ai-hist": "^0.16.0" },
    optionalDependencies: Object.fromEntries(
      Object.keys(platforms).map((platform) => [
        packageName(plugins[plugin], platform),
        version,
      ]),
    ),
  };
}
test("independent optional versions choose their own artifacts and require matching helper pins", () => {
  const relay = manifest("relayhistory", "0.16.2");
  const provider = manifest("provider-sources", "0.17.0");
  assert.equal(
    helperTarball("relayhistory", "linux-x64-gnu", relay),
    "relayhistory-capture-linux-x64-gnu-0.16.2.tgz",
  );
  assert.equal(
    helperTarball("provider-sources", "linux-x64-gnu", provider),
    "relayhistory-provider-sources-linux-x64-gnu-0.17.0.tgz",
  );
  provider.optionalDependencies[
    "@relayhistory/provider-sources-win32-x64-msvc"
  ] = "0.16.0";
  assert.throws(
    () => validatePluginManifest("provider-sources", provider),
    /own version/,
  );
  relay.peerDependencies["ai-hist"] = "file:../../../sdk-ts";
  assert.throws(
    () => validatePluginManifest("relayhistory", relay),
    /registry version/,
  );
});
test("npm resolution uses a JavaScript entry rather than Windows command shell shims", async () => {
  const temporary = await mkdtemp(join(tmpdir(), "history-npm-fixture-"));
  try {
    const entry = join(temporary, "node_modules/npm/bin/npm-cli.js");
    await mkdir(join(temporary, "node_modules/npm/bin"), { recursive: true });
    await writeFile(entry, "// fixture");
    assert.equal(
      npmCli({ PATH: "" }, join(temporary, "node.exe"), "win32"),
      entry,
    );
    assert.equal(
      npmCli({ npm_execpath: entry, PATH: "" }, "/missing/node", "linux"),
      entry,
    );
    assert.throws(
      () => npmCli({ PATH: "" }, "/missing/node.exe", "win32"),
      /Cannot locate/,
    );
  } finally {
    await rm(temporary, { recursive: true, force: true });
  }
});
test("every platform package includes the selected executable and correct platform metadata", async () => {
  const temporary = await mkdtemp(join(tmpdir(), "history-package-fixture-"));
  const source = join(temporary, "fixture-binary");
  try {
    await writeFile(source, "portable packaging fixture");
    for (const plugin of Object.keys(plugins)) {
      const version = JSON.parse(
        await readFile(
          new URL(`../plugins/${plugin}/sdk/package.json`, import.meta.url),
        ),
      ).version;
      for (const [platform, [os, cpu, libc]] of Object.entries(platforms)) {
        const output = join(temporary, plugin, platform);
        const result = spawnSync(
          process.execPath,
          [
            fileURLToPath(
              new URL("./package-history-helper.mjs", import.meta.url),
            ),
            plugin,
            platform,
            version,
            source,
            output,
          ],
          { encoding: "utf8" },
        );
        assert.equal(result.status, 0, result.stderr);
        const pkg = JSON.parse(
          await readFile(join(output, "package.json"), "utf8"),
        );
        const binary = plugins[plugin].binary + (os === "win32" ? ".exe" : "");
        assert.deepEqual(pkg.files, [binary]);
        assert.deepEqual(pkg.os, [os]);
        assert.deepEqual(pkg.cpu, [cpu]);
        assert.deepEqual(pkg.libc, libc ? [libc] : undefined);
        assert.equal(
          await readFile(join(output, binary), "utf8"),
          "portable packaging fixture",
        );
      }
    }
  } finally {
    await rm(temporary, { recursive: true, force: true });
  }
});

test('unpublished optional helpers have complete lock entries for clean npm ci', async()=>{
  for(const plugin of Object.keys(plugins)){
    const directory=new URL(`../plugins/${plugin}/sdk/`,import.meta.url);
    const manifest=JSON.parse(await readFile(new URL('package.json',directory),'utf8'));
    const lock=JSON.parse(await readFile(new URL('package-lock.json',directory),'utf8'));
    validatePluginManifest(plugin,manifest);
    for(const [name,version] of Object.entries(manifest.optionalDependencies)){
      const entry=lock.packages['node_modules/'+name];assert.ok(entry,`${name} must be locked before publication`);
      assert.equal(entry.version,version);assert.equal(entry.optional,true);assert.ok(entry.resolved.startsWith('https://registry.npmjs.org/'));
    }
  }
});

/** Install built tarballs in a new project; no checkout-relative imports or auth. */
import assert from "node:assert/strict";
import {
  cp,
  mkdir,
  mkdtemp,
  readFile,
  readdir,
  rm,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { spawnSync } from "node:child_process";
import { helperTarball, npmCli } from "./history-package-contract.mjs";
const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const helperIndex = process.argv.indexOf("--helpers");
const helpers = helperIndex < 0 ? null : resolve(process.argv[helperIndex + 1]);
const sdk = await import(
  pathToFileURL(join(root, "sdk-ts/dist/index.js")).href
);
const platform = sdk.runtimePlatform();
const temporary = await mkdtemp(join(tmpdir(), "history-tarball-smoke-"));
const packs = join(temporary, "packs"),
  project = join(temporary, "project");
await mkdir(packs);
await mkdir(project);
await mkdir(join(project, "home"));
const env = {
  ...process.env,
  HOME: join(project, "home"),
  USERPROFILE: join(project, "home"),
  XDG_DATA_HOME: join(project, "share"),
};
for (const key of [
  "RELAYHISTORY_PLUGIN_BIN",
  "HISTORY_PROVIDER_SOURCES_BIN",
  "RELAYHISTORY_HOME",
  "AI_HIST_DB",
  "AI_HIST_PLUGIN_CONFIG",
])
  delete env[key];
function run(command, args, cwd = project, extra = {}) {
  if (command === "npm") {
    args = [npmEntry, ...args];
    command = process.execPath;
  }
  const result = spawnSync(command, args, {
    cwd,
    env,
    encoding: "utf8",
    timeout: 120000,
    maxBuffer: 4 * 1024 * 1024,
    ...extra,
  });
  if (result.error) throw result.error;
  if (result.status !== 0)
    throw new Error(
      `${command} ${args.join(" ")} failed: ${result.stderr}\n${result.stdout}`,
    );
  return result.stdout;
}
const npm = "npm";
const npmEntry = npmCli();
async function pack(directory, copyFiles, change = () => {}) {
  const stage = await mkdtemp(join(temporary, "stage-"));
  const pkg = JSON.parse(
    await readFile(join(directory, "package.json"), "utf8"),
  );
  change(pkg);
  await writeFile(join(stage, "package.json"), JSON.stringify(pkg));
  for (const file of copyFiles)
    await cp(join(directory, file), join(stage, file), { recursive: true });
  const result = JSON.parse(
    run(
      npm,
      ["pack", "--json", "--ignore-scripts", "--pack-destination", packs],
      stage,
    ),
  );
  return join(
    packs,
    (Array.isArray(result)
      ? result[0]
      : result.filename
        ? result
        : Object.values(result)[0]
    ).filename,
  );
}
try {
  const version = JSON.parse(
    await readFile(join(root, "sdk-ts/package.json"), "utf8"),
  ).version;
  const native = await pack(join(root, "crates/ai-hist-napi"), [
    "index.js",
    "index.d.ts",
  ]);
  // Match the existing core release staging: replace its development native
  // file dependency with the published version and omit the checkout prepare
  // lifecycle. Optional plugin manifests are packed without modification.
  const core = await pack(
    join(root, "sdk-ts"),
    ["dist", "README.md"],
    (pkg) => {
      pkg.dependencies["ai-hist-native"] = version;
      delete pkg.scripts.prepare;
    },
  );
  const nativeStage = await mkdtemp(join(temporary, "native-"));
  const binary = `ai-hist-native.${platform}.node`;
  await cp(
    join(root, "crates/ai-hist-napi", binary),
    join(nativeStage, binary),
  );
  await writeFile(
    join(nativeStage, "package.json"),
    JSON.stringify({
      name: `ai-hist-native-${platform}`,
      version,
      main: binary,
      files: [binary],
    }),
  );
  const nativePack = JSON.parse(
    run(
      npm,
      ["pack", "--json", "--ignore-scripts", "--pack-destination", packs],
      nativeStage,
    ),
  );
  const nativePlatform = join(
    packs,
    (Array.isArray(nativePack)
      ? nativePack[0]
      : nativePack.filename
        ? nativePack
        : Object.values(nativePack)[0]
    ).filename,
  );
  run(npm, ["init", "--yes"]);
  run(npm, [
    "install",
    "--ignore-scripts",
    "--no-audit",
    "--no-fund",
    core,
    native,
    nativePlatform,
  ]);
  const script = join(project, "verify.mjs");
  await writeFile(
    script,
    `import assert from 'node:assert/strict';import * as sdk from 'ai-hist';import native from 'ai-hist-native';import {createRequire} from 'node:module';const require=createRequire(import.meta.url);\nassert.equal(sdk.login,undefined);assert.equal(sdk.pushCloud,undefined);assert.equal(native.cloudLoadAuth,undefined);assert.equal(native.nativeContractVersion(),14);assert.equal(typeof native.applySourceEvidence,'function');assert.throws(()=>require.resolve('ai-hist/cloud'));assert.throws(()=>require.resolve('@agent-relay/cloud'));assert.deepEqual(await sdk.recent({dbPath:${JSON.stringify(join(project, "missing.db"))},scope:'remote'}),[]);\n`,
  );
  run(process.execPath, [script]);
  const login = spawnSync(
    process.execPath,
    [
      join(project, "node_modules/ai-hist/dist/cli.js"),
      "login",
      "--token",
      "fixture",
    ],
    { cwd: project, env, encoding: "utf8", timeout: 10000 },
  );
  assert.equal(login.status, 2);
  if (helpers) {
    const files = await readdir(helpers);
    const installs = [];
    for (const [directory, prefix] of [
      ["relayhistory", "relayhistory"],
      ["provider-sources", "history-provider-sources"],
    ]) {
      const manifest = JSON.parse(
        await readFile(
          join(root, "plugins", directory, "sdk/package.json"),
          "utf8",
        ),
      );
      const artifact = files.find(
        (file) => file === helperTarball(directory, platform, manifest),
      );
      assert.ok(artifact, `Missing ${prefix} ${platform} helper tarball`);
      installs.push(join(helpers, artifact));
      installs.push(
        await pack(join(root, "plugins", directory, "sdk"), [
          "dist",
          "README.md",
        ]),
      );
    }
    run(npm, [
      "install",
      "--strict-peer-deps",
      "--ignore-scripts",
      "--no-audit",
      "--no-fund",
      ...installs,
    ]);
    await writeFile(
      script,
      `import assert from 'node:assert/strict';import * as sdk from 'ai-hist';import * as cloud from '@agent-relay/relayhistory';import * as provider from '@agent-relay/history-provider-sources';\nassert.equal(cloud.RelayHistoryError,sdk.RelayHistoryError);const registry=new sdk.HistoryPluginRegistry();registry.register(cloud.createHistoryPlugin({binaryPath:'/never-read-during-registration'}));registry.register(provider.createHistoryPlugin({binaryPath:'/never-read-during-registration'}));assert.equal(registry.sourceConnectors().length,3);\nfor(const name of ['relayhistory','history-provider-sources']){const {helperRequest}=await import('./node_modules/@agent-relay/'+name+'/dist/helper.js');await assert.rejects(helperRequest('fixtureUnsupported'),error=>error instanceof sdk.RelayHistoryError&&error.code!=='HISTORY_PLUGIN_BINARY_MISSING');}\n`,
    );
    run(process.execPath, [script]);
  }
  console.log(
    `Installed local${helpers ? " and optional plugin" : ""} tarballs verified on ${platform}`,
  );
} finally {
  await rm(temporary, { recursive: true, force: true });
}

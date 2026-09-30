import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

const ci = await readFile(new URL("../.github/workflows/ci.yml", import.meta.url), "utf8");
const publish = await readFile(new URL("../.github/workflows/publish.yml", import.meta.url), "utf8");

function jobBlock(workflow, name, nextName) {
  const start = workflow.indexOf(`\n  ${name}:\n`);
  assert.notEqual(start, -1, `missing ${name} job`);
  const end = nextName ? workflow.indexOf(`\n  ${nextName}:\n`, start + 1) : workflow.length;
  assert.notEqual(end, -1, `missing ${nextName} job after ${name}`);
  return workflow.slice(start, end);
}

test("CI cancels stale runs and release markers do not launch the full suite", () => {
  assert.match(ci, /paths-ignore:\n\s+- '\.release\/dispatch-patch-\*'/);
  assert.match(ci, /group: ci-\$\{\{ github\.event\.pull_request\.number \|\| github\.ref \}\}/);
  assert.match(ci, /cancel-in-progress: true/);
});

test("critical CI Rust jobs restore explicit caches", () => {
  const verify = jobBlock(ci, "verify", "public-api");
  const optional = jobBlock(ci, "optional-history-plugins", "windows-helper-cancellation");
  assert.match(verify, /Swatinem\/rust-cache@v2[\s\S]*key: ci-verify/);
  assert.match(optional, /Swatinem\/rust-cache@v2[\s\S]*key: ci-optional-history-plugins/);
});

test("plugin packaging overlaps core publication", () => {
  const packaging = jobBlock(publish, "package-plugins", "plugins");
  assert.match(packaging, /needs: \[version, helpers\]/);
  assert.doesNotMatch(packaging, /needs: \[[^\]]*publish/);
  assert.match(packaging, /name: plugin-packages/);
});

test("plugins publish from a matrix and verify afterwards", () => {
  const plugins = jobBlock(publish, "plugins", "verify-plugins");
  assert.match(plugins, /plugin: \[provider-sources\]/);
  assert.match(plugins, /name: plugin-packages/);
  assert.match(plugins, /verify-published-history-core\.mjs "\$PLUGIN"/);
  assert.doesNotMatch(plugins, /verify-published-plugins\.mjs/);

  const verification = jobBlock(publish, "verify-plugins");
  assert.match(verification, /needs: \[version, publish, plugins\]/);
  assert.match(verification, /verify-published-plugins\.mjs "\$VERSION"/);
});

test("full core smoke checks gate release finalization and downstream publication", () => {
  const version = jobBlock(publish, "version", "build");
  assert.match(version, /matching-refs\/tags\/sdk-ts-v\$VERSION/);
  assert.match(version, /resume with skip_core=true and custom_version=\$VERSION/);

  const corePublish = jobBlock(publish, "publish", "verify-core");
  const preflight = corePublish.indexOf("name: Recheck release version availability");
  const publishStep = corePublish.indexOf("name: Publish\n");
  const tag = corePublish.indexOf("name: Tag the published tree");
  assert.ok(preflight >= 0 && preflight < publishStep, "version recheck must precede publication");
  assert.match(corePublish, /git ls-remote --tags origin "refs\/tags\/sdk-ts-v\$VERSION"/);
  assert.ok(publishStep < tag, "successful publication must create a recovery tag");
  assert.doesNotMatch(corePublish, /name: Registry visibility gate/);
  assert.doesNotMatch(corePublish, /name: Registry clean-install smoke test/);
  assert.doesNotMatch(corePublish, /name: Create GitHub Release/);

  const verification = jobBlock(publish, "verify-core", "finalize-core");
  assert.match(verification, /needs: \[version, publish\]/);
  assert.match(verification, /inputs\.skip_core \|\| needs\.publish\.result == 'success'/);
  assert.match(verification, /name: Registry clean-install smoke test/);
  assert.match(verification, /REGISTRY_VISIBILITY_ATTEMPTS: 150/);
  assert.match(verification, /REGISTRY_VISIBILITY_MAX_WAIT_MS: 4200000/);
  assert.match(verification, /name: Registry CLI smoke test on older glibc/);

  const finalization = jobBlock(publish, "finalize-core", "persist-version");
  assert.match(finalization, /needs: \[version, publish, verify-core\]/);
  assert.match(finalization, /needs\.verify-core\.result == 'success'/);
  assert.match(finalization, /name: Create GitHub Release after runtime verification/);

  const plugins = jobBlock(publish, "plugins", "verify-plugins");
  assert.match(
    plugins,
    /needs: \[version, publish, verify-core, finalize-core, package-plugins\]/,
  );
  assert.match(plugins, /needs\.verify-core\.result == 'success'/);
  assert.match(plugins, /needs\.finalize-core\.result == 'success'/);
});

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

test("plugin families publish in parallel and verify afterwards", () => {
  const plugins = jobBlock(publish, "plugins", "verify-plugins");
  assert.match(plugins, /plugin: \[relayhistory, provider-sources\]/);
  assert.match(plugins, /name: plugin-packages/);
  assert.match(plugins, /verify-published-history-core\.mjs "\$PLUGIN"/);
  assert.doesNotMatch(plugins, /verify-published-plugins\.mjs/);

  const verification = jobBlock(publish, "verify-plugins", "probe");
  assert.match(verification, /needs: \[version, publish, plugins\]/);
  assert.match(verification, /verify-published-plugins\.mjs "\$VERSION"/);
});

test("full core smoke checks are required but off the publish critical path", () => {
  const corePublish = jobBlock(publish, "publish", "verify-core");
  assert.match(corePublish, /name: Registry visibility gate/);
  assert.doesNotMatch(corePublish, /name: Registry clean-install smoke test/);

  const verification = jobBlock(publish, "verify-core", "persist-version");
  assert.match(verification, /needs: \[version, publish\]/);
  assert.match(verification, /name: Registry clean-install smoke test/);
  assert.match(verification, /name: Registry CLI smoke test on older glibc/);
});

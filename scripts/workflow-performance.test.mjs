import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
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
  // PR runs collapse per PR (a newer `ci:run` cancels the superseded trunk head);
  // push runs key off the sha so back-to-back merges never cancel each other.
  assert.ok(
    ci.includes(
      "group: ci-${{ github.event_name != 'pull_request' && format('push-{0}', github.sha) || ((github.event.action == 'labeled' && github.event.label.name != 'ci:run') && format('ignored-{0}', github.run_id) || format('pr-{0}', github.event.pull_request.number)) }}\n",
    ),
    "the complete concurrency group: push by sha, ignored labels throwaway, PRs per number",
  );
  assert.match(ci, /cancel-in-progress: \$\{\{ github\.event_name == 'pull_request' \}\}/);
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
  assert.match(version, /elif \[ "\$DRY_RUN" != "true" \]; then[\s\S]*matching-refs\/tags\/sdk-ts-v\$VERSION/);
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

test("a tagged custom version can still run the build-only release path", () => {
  const version = jobBlock(publish, "version", "build");
  const marker = "        run: |\n";
  const script = version.slice(version.indexOf(marker) + marker.length)
    .split("\n")
    .map((line) => line.startsWith("          ") ? line.slice(10) : line)
    .join("\n")
    .replaceAll("${{ github.repository }}", "AgentWorkforce/relayhistory");
  const directory = mkdtempSync(join(tmpdir(), "ai-hist-release-version-test-"));
  try {
    writeFileSync(join(directory, "gh"), "#!/bin/sh\nprintf '%s\\n' refs/tags/sdk-ts-v0.32.2\n", { mode: 0o755 });
    const run = (dryRun) => spawnSync("bash", ["-c", script], {
      encoding: "utf8",
      env: {
        ...process.env,
        PATH: `${directory}:${process.env.PATH}`,
        VERSION_TYPE: "patch",
        CUSTOM_VERSION: "0.32.2",
        SKIP_CORE: "false",
        DRY_RUN: dryRun ? "true" : "false",
        GITHUB_OUTPUT: join(directory, "output"),
      },
    });
    const dryRun = run(true);
    assert.equal(dryRun.status, 0, dryRun.stderr);
    assert.match(dryRun.stdout, /Release version 0\.32\.2/);
    const publishRun = run(false);
    assert.equal(publishRun.status, 1);
    assert.match(publishRun.stderr, /Release tag sdk-ts-v0\.32\.2 already exists/);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

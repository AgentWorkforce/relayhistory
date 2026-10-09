import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { test } from "node:test";

/**
 * The merge-train sweeper (AgentWorkforce/cloud packages/web/lib/merge-train)
 * and these workflows share three names: the label that runs promotion CI
 * (`ci:run`), the check every promotion CI run creates (the sweeper's "CI ran
 * on this head" marker, the `verify` job), and the feature ready check
 * (`Merge-train ready check`, kicked by `ready:check`). A rename on either
 * side silently breaks the gate, so they are pinned here.
 */
const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const read = (file) => readFileSync(path.join(ROOT, ".github/workflows", file), "utf8");
/** The complete trunk-PR + ci:run gate. Asserted exactly: a substring check would
 * still pass if a job appended an `|| ...` bypass. */
const TRUNK_CI_GATE =
  "(github.event_name != 'pull_request' && github.event_name != 'pull_request_target') || (github.head_ref == 'trunk' && github.event.pull_request.head.repo.full_name == github.repository && github.base_ref == 'main' && (github.event.action != 'labeled' || github.event.label.name == 'ci:run'))";

/** Every `if:` of a top-level job (2-space indented key under `jobs:`). */
function jobGates(source) {
  const jobs = source.slice(source.indexOf("\njobs:\n"));
  const gates = {};
  let current = null;
  for (const line of jobs.split("\n")) {
    const job = /^  ([A-Za-z0-9_-]+):\s*$/.exec(line);
    if (job) {
      current = job[1];
      gates[current] = null;
      continue;
    }
    const gate = /^    if: (.*)$/.exec(line);
    if (current && gate) gates[current] = gate[1];
  }
  return gates;
}

for (const file of ["ci.yml"]) {
  test(`${file} runs the trunk PR only on opened/reopened/ci:run, never on synchronize`, () => {
    const source = read(file);
    const pullRequest = source.slice(source.indexOf("\n  pull_request:\n"));
    const types = /\n    types: \[([^\]]*)\]/.exec(pullRequest);
    assert.ok(types, `${file}: pull_request must declare types`);
    assert.deepEqual(
      types[1].split(",").map((t) => t.trim()),
      ["opened", "reopened", "labeled"],
    );
    const gates = jobGates(source);
    assert.ok(Object.keys(gates).length > 0);
    for (const [job, gate] of Object.entries(gates)) {
      assert.equal(gate, TRUNK_CI_GATE, `${file} ${job}: exact trunk PR + ci:run gate`);
    }
  });
}

test("an unrelated label event can never cancel a real promotion CI run", () => {
  const source = read("ci.yml");
  assert.match(
    source,
    /\(github\.event\.action == 'labeled' && github\.event\.label\.name != 'ci:run'\) && format\('ignored-\{0\}', github\.run_id\)/,
  );
  assert.match(source, /cancel-in-progress: \$\{\{ github\.event_name == 'pull_request' \}\}/);
});

test("the sweeper's CI marker job has no needs, so every real promotion run creates it", () => {
  const source = read("ci.yml");
  const marker = /\n  verify:\n([\s\S]*?)\n  [A-Za-z0-9_-]+:\n/.exec(source);
  assert.ok(marker, "verify job present");
  // Unnamed: its check run is named after the job id, `verify`.
  assert.doesNotMatch(marker[1], /\n    name:/);
  assert.doesNotMatch(marker[1], /\n    needs:/);
});

test("feature PRs into trunk get the ready check the sweeper requires", () => {
  const source = read("merge-train-ready.yml");
  assert.match(source, /\n    branches: \[trunk\]\n    types: \[labeled, synchronize, reopened\]\n/);
  assert.match(source, /\n    name: Merge-train ready check\n/);
  // A skip-bound event (no `mergeable`) must not cancel a real ready-check run.
  assert.match(source, /contains\(github\.event\.pull_request\.labels\.\*\.name, 'mergeable'\) && format\('pr-\{0\}', github\.event\.pull_request\.number\) \|\| format\('ignored-\{0\}', github\.run_id\)/);
  assert.match(source, /contains\(github\.event\.pull_request\.labels\.\*\.name, 'mergeable'\)/);
  // Fork PRs get the check too (no secrets under `pull_request`); trust is the sweeper's gate.
  assert.doesNotMatch(source, /head\.repo\.full_name/);
  assert.doesNotMatch(source, /^  pull_request_target:/m);
  assert.doesNotMatch(source, /secrets\./);
  assert.match(source, /persist-credentials: false/);
  // The sweeper kicks missing runs with `ready:check`: the job runs on ANY label event.
  assert.doesNotMatch(source, /event\.label\.name/);
});

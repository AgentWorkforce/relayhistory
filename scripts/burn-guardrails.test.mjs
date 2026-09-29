import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { readFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import {
  DEFAULT_CUTOVER_TAG,
  READER_FILES,
  checkResolution,
  countTestsRun,
  isRelevantChange,
  moduleParityFiles,
  parityPlan,
  parityProbe,
  parseArgs,
  parseTreeRoot,
  pinAiHist,
  relevantChange,
  resolutionVerdict,
  rewriteAiHistRequirement,
  runParity,
  scanParserSymbols,
  setOutput,
  tagExists,
  tripwire,
  tripwireVerdict,
} from "./burn-guardrails.mjs";

// ------------------------------------------------------------------ helpers

function tempDir(t) {
  const dir = mkdtempSync(path.join(os.tmpdir(), "burn-guardrails-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  return dir;
}

function write(root, rel, text) {
  const file = path.join(root, rel);
  mkdirSync(path.dirname(file), { recursive: true });
  writeFileSync(file, text);
}

/** An io whose runner answers from `handlers` keyed by "cmd arg0". */
function fakeIo(t, handlers = {}, env = {}) {
  const dir = tempDir(t);
  const output = path.join(dir, "github_output");
  writeFileSync(output, "");
  const calls = [];
  const logs = [];
  return {
    io: {
      env: { GITHUB_OUTPUT: output, ...env },
      log: (line) => logs.push(line),
      run: (cmd, args, options) => {
        calls.push({ cmd, args, options });
        const handler = handlers[`${cmd} ${args[0]}`];
        if (!handler) throw new Error(`unexpected command: ${cmd} ${args.join(" ")}`);
        return { stdout: "", stderr: "", ...handler(args, options) };
      },
    },
    calls,
    logs,
    outputs: () => readFileSync(output, "utf8"),
  };
}

function metadata({ features = {}, dependencies = [], targets = [] } = {}) {
  return {
    packages: [
      { name: "relayburn-cli", features: {}, dependencies: [], targets: [] },
      { name: "relayburn-sdk", features, dependencies, targets },
    ],
  };
}

const BURN_TODAY = metadata({
  features: { "test-utils": [] },
  dependencies: [{ name: "serde" }],
  targets: [
    { name: "relayburn_sdk", kind: ["lib"] },
    { name: "integration", kind: ["test"] },
  ],
});

const withDep = (extra = {}) =>
  metadata({
    features: { "relayhistory-source": ["dep:ai-hist"] },
    dependencies: [{ name: "ai-hist", optional: true }],
    ...extra,
  });

const metadataHandler = (meta) => ({ "cargo metadata": () => ({ status: 0, stdout: JSON.stringify(meta) }) });

// ---------------------------------------------------------------- parseArgs

test("parseArgs reads a command and --name value pairs", () => {
  assert.deepEqual(parseArgs(["tripwire", "--burn-dir", "burn", "--cutover-tag", ""]), {
    command: "tripwire",
    options: { "burn-dir": "burn", "cutover-tag": "" },
  });
});

test("parseArgs rejects a flag without a value", () => {
  assert.throws(() => parseArgs(["tripwire", "--burn-dir"]), /--name value pairs/);
  assert.throws(() => parseArgs(["tripwire", "--burn-dir", "--repo", "x"]), /--name value pairs/);
  assert.throws(() => parseArgs(["tripwire", "burn"]), /--name value pairs/);
});

test("setOutput appends name=value to $GITHUB_OUTPUT and is a no-op without it", (t) => {
  const { io, outputs } = fakeIo(t);
  setOutput(io, "ready", "true");
  setOutput(io, "run", "false");
  assert.equal(outputs(), "ready=true\nrun=false\n");
  setOutput({ env: {} }, "ready", "true");
});

// ---------------------------------------------------------- relevant-change

test("only changes burn could observe are relevant", () => {
  assert.equal(isRelevantChange(["crates/ai-hist/src/lib.rs"]), true);
  assert.equal(isRelevantChange(["Cargo.toml"]), true);
  assert.equal(isRelevantChange(["scripts/burn-guardrails.mjs"]), true);
  assert.equal(isRelevantChange([".github/workflows/ci.yml"]), true);
  assert.equal(isRelevantChange(["docs/architecture.md", "sdk-ts/src/index.ts"]), false);
  assert.equal(isRelevantChange(["crates/ai-hist-napi/src/lib.rs"]), false);
  assert.equal(isRelevantChange(["plugins/Cargo.toml"]), false);
});

test("relevant-change diffs HEAD^1..HEAD on pull_request and push", (t) => {
  const { io, calls, outputs } = fakeIo(
    t,
    { "git diff": () => ({ status: 0, stdout: "docs/a.md\n" }) },
    { GITHUB_EVENT_NAME: "pull_request" },
  );
  assert.equal(relevantChange({}, io), 0);
  assert.deepEqual(calls[0].args, ["diff", "--name-only", "HEAD^1", "HEAD"]);
  assert.equal(outputs(), "run=false\n");
});

test("relevant-change runs on manual events and when the diff fails", (t) => {
  const manual = fakeIo(t, {}, { GITHUB_EVENT_NAME: "workflow_dispatch" });
  relevantChange({}, manual.io);
  assert.equal(manual.outputs(), "run=true\n");
  assert.equal(manual.calls.length, 0);

  const shallow = fakeIo(t, { "git diff": () => ({ status: 128 }) }, { GITHUB_EVENT_NAME: "push" });
  relevantChange({}, shallow.io);
  assert.equal(shallow.outputs(), "run=true\n");
});

// -------------------------------------------------------------- parity plan

test("burn as it stands today has no ai-hist dependency: absent", () => {
  const plan = parityPlan(BURN_TODAY);
  assert.equal(plan.status, "absent");
  assert.match(plan.reason, /does not depend on `ai-hist`/);
});

test("the named test target wins when it exists", () => {
  const plan = parityPlan(withDep({ targets: [{ name: "relayhistory_parity", kind: ["test"] }] }), ["x"]);
  assert.equal(plan.mode, "target");
  assert.deepEqual(plan.args, [
    "test", "-p", "relayburn-sdk", "--features", "relayhistory-source", "--test", "relayhistory_parity",
  ]);
});

test("burn #557's unit-test module is run through the lib with a module filter", () => {
  const plan = parityPlan(withDep(), ["ingest/backend/relayhistory/tests.rs"]);
  assert.equal(plan.status, "ready");
  assert.equal(plan.mode, "module");
  assert.deepEqual(plan.args, [
    "test", "-p", "relayburn-sdk", "--features", "relayhistory-source", "--lib", "--", "relayhistory",
  ]);
});

test("after the cutover folds the feature away the suite still runs, without --features", () => {
  const plan = parityPlan(metadata({ dependencies: [{ name: "ai-hist" }] }), ["ingest/backend/relayhistory/tests.rs"]);
  assert.deepEqual(plan.args, ["test", "-p", "relayburn-sdk", "--lib", "--", "relayhistory"]);
});

test("a dependency with no suite, or a same-named non-test target, is no-suite", () => {
  assert.equal(parityPlan(withDep()).status, "no-suite");
  assert.equal(
    parityPlan(withDep({ targets: [{ name: "relayhistory_parity", kind: ["bench"] }] })).status,
    "no-suite",
  );
  assert.equal(parityPlan({ packages: [] }).status, "absent");
});

test("module parity files need a relayhistory path and a test attribute", () => {
  assert.deepEqual(
    moduleParityFiles([
      { rel: "ingest/backend/relayhistory/tests.rs", text: "#[test]\nfn a() {}" },
      { rel: "ingest/backend/relayhistory/adapter.rs", text: "fn map() {}" },
      { rel: "ingest/backend/relayhistory.rs", text: "#[tokio::test]\nasync fn b() {}" },
      { rel: "ingest/walk.rs", text: "#[test]\nfn c() {}" },
    ]),
    ["ingest/backend/relayhistory/tests.rs", "ingest/backend/relayhistory.rs"],
  );
});

test("parity-probe: notice when absent, warning when no suite, ready when found", (t) => {
  const absent = fakeIo(t, metadataHandler(BURN_TODAY));
  const burn = tempDir(t);
  parityProbe({ "burn-dir": burn }, absent.io);
  assert.equal(absent.outputs(), "ready=false\n");
  assert.match(absent.logs[0], /^::notice::/);

  const noSuite = fakeIo(t, metadataHandler(withDep()));
  parityProbe({ "burn-dir": burn }, noSuite.io);
  assert.equal(noSuite.outputs(), "ready=false\n");
  assert.match(noSuite.logs[0], /^::warning::/);

  write(burn, "crates/relayburn-sdk/src/ingest/backend/relayhistory/tests.rs", "#[test]\nfn parity() {}\n");
  const ready = fakeIo(t, metadataHandler(withDep()));
  parityProbe({ "burn-dir": burn }, ready.io);
  assert.equal(ready.outputs(), "ready=true\n");
});

// --------------------------------------------------------------- pin-ai-hist

const P = "/ws/crates/ai-hist";

test("a string requirement becomes a path dependency", () => {
  const { text, changed } = rewriteAiHistRequirement('[dependencies]\nai-hist = "=0.31.0"\nserde = "1"\n', P);
  assert.equal(changed, 1);
  assert.equal(text, '[dependencies]\nai-hist = { path = "/ws/crates/ai-hist" }\nserde = "1"\n');
});

test("an inline table keeps its other keys and loses only the version", () => {
  const { text } = rewriteAiHistRequirement(
    '[dependencies]\nai-hist = { version = "0.31", optional = true, default-features = false } # pin\n',
    P,
  );
  assert.equal(
    text,
    '[dependencies]\nai-hist = { path = "/ws/crates/ai-hist", optional = true, default-features = false } # pin\n',
  );
});

test("workspace inheritance is left alone and the workspace root is rewritten", () => {
  const member = rewriteAiHistRequirement('[dependencies]\nai-hist = { workspace = true, optional = true }\n', P);
  assert.equal(member.changed, 0);
  const root = rewriteAiHistRequirement('[workspace.dependencies]\n"ai-hist" = "0.31.0"\n', P);
  assert.equal(root.text, '[workspace.dependencies]\n"ai-hist" = { path = "/ws/crates/ai-hist" }\n');
});

test("table-form, renamed, target-specific and dev requirements are rewritten", () => {
  assert.equal(
    rewriteAiHistRequirement('[dependencies.ai-hist]\nversion = "0.31"\noptional = true\n', P).text,
    '[dependencies.ai-hist]\npath = "/ws/crates/ai-hist"\noptional = true\n',
  );
  assert.equal(
    rewriteAiHistRequirement('[dependencies]\nhist = { package = "ai-hist", version = "0.31" }\n', P).changed,
    1,
  );
  assert.equal(
    rewriteAiHistRequirement("[target.'cfg(unix)'.dependencies]\nai-hist = \"0.31\"\n", P).changed,
    1,
  );
  assert.equal(rewriteAiHistRequirement('[dev-dependencies]\nai-hist = "0.31"\n', P).changed, 1);
});

test("ai-hist keys outside dependency tables are not touched", () => {
  const text = '[package]\nname = "x"\n[features]\nai-hist = ["dep:ai-hist"]\n[patch.crates-io]\nai-hist = "0.1"\n';
  assert.equal(rewriteAiHistRequirement(text, P).changed, 0);
});

test("pin-ai-hist rewrites burn's manifests on disk and fails when it finds none", (t) => {
  const burn = tempDir(t);
  write(burn, "Cargo.toml", '[workspace]\nmembers = ["crates/*"]\n[workspace.dependencies]\nai-hist = "=0.31.0"\n');
  write(burn, "crates/relayburn-sdk/Cargo.toml", "[dependencies]\nai-hist = { workspace = true, optional = true }\n");
  write(burn, "target/debug/Cargo.toml", '[dependencies]\nai-hist = "1"\n');
  const { io, logs } = fakeIo(t);
  assert.equal(pinAiHist({ "burn-dir": burn, "ai-hist-path": P }, io), 0);
  assert.match(readFileSync(path.join(burn, "Cargo.toml"), "utf8"), /ai-hist = \{ path = "\/ws\/crates\/ai-hist" \}/);
  assert.match(readFileSync(path.join(burn, "target/debug/Cargo.toml"), "utf8"), /ai-hist = "1"/);
  assert.match(logs[0], /in: Cargo\.toml$/);

  const empty = tempDir(t);
  write(empty, "Cargo.toml", "[workspace]\n");
  const second = fakeIo(t);
  assert.equal(pinAiHist({ "burn-dir": empty, "ai-hist-path": P }, second.io), 1);
  assert.match(second.logs[0], /^::error::Found no `ai-hist` requirement/);
});

// --------------------------------------------------------- check-resolution

test("cargo tree roots parse, pre-release and build metadata included", () => {
  assert.deepEqual(parseTreeRoot("ai-hist v0.31.0 (/ws/crates/ai-hist)\nrelayburn-sdk v5.0.0 (/b)\n"), {
    version: "0.31.0",
    source: "/ws/crates/ai-hist",
  });
  assert.deepEqual(parseTreeRoot("ai-hist v1.0.0-rc.1+build.5 (/ws/crates/ai-hist)\n"), {
    version: "1.0.0-rc.1+build.5",
    source: "/ws/crates/ai-hist",
  });
  assert.deepEqual(parseTreeRoot("ai-hist v0.30.2\n"), { version: "0.30.2", source: null });
  assert.equal(parseTreeRoot("serde v1.0.0\n"), null);
});

test("resolution verdicts name what went wrong", () => {
  const ok = resolutionVerdict({ status: 0, stdout: `ai-hist v0.32.0-alpha.1 (${P})\n`, stderr: "" }, P);
  assert.equal(ok.ok, true);

  const registry = resolutionVerdict({ status: 0, stdout: "ai-hist v0.30.2\n", stderr: "" }, P);
  assert.match(registry.message, /from the registry, not this checkout/);

  const elsewhere = resolutionVerdict({ status: 0, stdout: "ai-hist v0.31.0 (/other)\n", stderr: "" }, P);
  assert.match(elsewhere.message, /from \/other, not this checkout/);

  const two = resolutionVerdict(
    { status: 101, stdout: "", stderr: "error: There are multiple `ai-hist` packages in your project" },
    P,
  );
  assert.match(two.message, /two ai-hist packages/);

  const missing = resolutionVerdict({ status: 101, stdout: "", stderr: "did not match any packages" }, P);
  assert.match(missing.message, /cargo tree -i ai-hist failed/);
});

test("check-resolution runs cargo tree in burn with the feature when it exists", (t) => {
  const burn = tempDir(t);
  const { io, calls, logs } = fakeIo(t, {
    ...metadataHandler(withDep({ targets: [{ name: "relayhistory_parity", kind: ["test"] }] })),
    "cargo tree": () => ({ status: 0, stdout: "ai-hist v0.30.2\n" }),
  });
  assert.equal(checkResolution({ "burn-dir": burn, "ai-hist-path": P }, io), 1);
  const tree = calls.find((c) => c.args[0] === "tree");
  assert.deepEqual(tree.args, [
    "tree", "-p", "relayburn-sdk", "--features", "relayhistory-source", "-i", "ai-hist", "--prefix", "none",
  ]);
  assert.equal(tree.options.cwd, burn);
  assert.match(logs.at(-1), /^::error::burn resolved ai-hist v0\.30\.2 from the registry/);
});

// --------------------------------------------------------------- run-parity

test("countTestsRun sums passed and failed across test binaries", () => {
  const out = [
    "running 3 tests",
    "test result: ok. 3 passed; 0 failed; 1 ignored; 0 measured; 200 filtered out; finished in 0.1s",
    "test result: FAILED. 1 passed; 2 failed; 0 ignored; 0 measured; 0 filtered out",
  ].join("\n");
  assert.equal(countTestsRun(out), 6);
  assert.equal(countTestsRun("test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 9 filtered out"), 0);
});

test("run-parity fails on a red suite and on a filter that matched nothing", (t) => {
  const burn = tempDir(t);
  const target = withDep({ targets: [{ name: "relayhistory_parity", kind: ["test"] }] });
  const green = fakeIo(t, {
    ...metadataHandler(target),
    "cargo test": () => ({ status: 0, stdout: "test result: ok. 12 passed; 0 failed; 0 ignored;" }),
  });
  assert.equal(runParity({ "burn-dir": burn }, green.io), 0);

  const red = fakeIo(t, { ...metadataHandler(target), "cargo test": () => ({ status: 101, stdout: "" }) });
  assert.equal(runParity({ "burn-dir": burn }, red.io), 1);

  const empty = fakeIo(t, {
    ...metadataHandler(target),
    "cargo test": () => ({ status: 0, stdout: "test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 4 filtered out" }),
  });
  assert.equal(runParity({ "burn-dir": burn }, empty.io), 1);
  assert.match(empty.logs.at(-1), /ran zero tests/);

  const none = fakeIo(t, metadataHandler(BURN_TODAY));
  assert.equal(runParity({ "burn-dir": burn }, none.io), 1);
});

// ------------------------------------------------------------------ tripwire

test("git ls-remote exit codes map to tagged, untagged and a thrown error", (t) => {
  for (const [status, expected] of [
    [0, true],
    [2, false],
  ]) {
    const { io, calls } = fakeIo(t, { "git ls-remote": () => ({ status }) });
    assert.equal(tagExists(io, "https://example/burn.git", "v5"), expected);
    assert.deepEqual(calls[0].args, ["ls-remote", "--exit-code", "--tags", "https://example/burn.git", "refs/tags/v5"]);
  }
  const { io } = fakeIo(t, { "git ls-remote": () => ({ status: 128, stderr: "Could not resolve host" }) });
  assert.throws(() => tagExists(io, "u", "v5"), /status 128: Could not resolve host/);
});

test("parser symbols are found by #562's acceptance grep, line by line", () => {
  assert.deepEqual(
    scanParserSymbols([
      { rel: "relayburn-sdk/src/reader/codex.rs", text: "pub fn parse_codex_session() {}\nfn span_tree() {}" },
      { rel: "relayburn-sdk/src/ingest/fs_events.rs", text: "use notify::Watcher;" },
      { rel: "relayburn-sdk/src/reader/codex/span_tree.rs", text: "fn build() {}" },
    ]),
    [
      "relayburn-sdk/src/reader/codex.rs:1: pub fn parse_codex_session() {}",
      "relayburn-sdk/src/ingest/fs_events.rs:1: use notify::Watcher;",
    ],
  );
});

const READY = { status: "ready" };

test("the tripwire is inactive until the cutover tag exists, parsers or not", () => {
  const verdict = tripwireVerdict({
    tag: DEFAULT_CUTOVER_TAG, tagExists: false, symbols: ["a"], readerFiles: READER_FILES, plan: { status: "absent" },
  });
  assert.equal(verdict.status, "inactive");
  assert.match(verdict.message, /relayburn-sdk-v5\.0\.0/);
});

test("after the cutover: symbols fail, a missing suite fails, reader file names only warn", () => {
  const base = { tag: "t", tagExists: true, symbols: [], readerFiles: [], plan: READY };
  assert.equal(tripwireVerdict({ ...base, symbols: ["x.rs:1: parse_claude_session"] }).status, "fail");
  const noSuite = tripwireVerdict({ ...base, plan: { status: "no-suite", reason: "nothing to run" } });
  assert.equal(noSuite.status, "fail");
  assert.match(noSuite.message, /no relayhistory parity suite/);
  assert.equal(tripwireVerdict({ ...base, readerFiles: [READER_FILES[1]] }).status, "warn");
  assert.equal(tripwireVerdict(base).status, "pass");
});

test("tripwire end to end over a burn checkout on disk", (t) => {
  const burn = tempDir(t);
  write(burn, "crates/relayburn-sdk/src/reader/codex.rs", "mod span_tree;\n");
  write(burn, "crates/relayburn-sdk/src/reader/codex/span_tree.rs", "fn build() {}\n");
  write(burn, "crates/relayburn-sdk/src/ingest/backend/relayhistory/tests.rs", "#[test]\nfn parity() {}\n");
  write(burn, "crates/relayburn-sdk/target/x.rs", "parse_claude_session\n");
  const handlers = (tagStatus) => ({
    ...metadataHandler(metadata({ dependencies: [{ name: "ai-hist" }] })),
    "git ls-remote": () => ({ status: tagStatus }),
  });

  const clean = fakeIo(t, handlers(0));
  assert.equal(tripwire({ "burn-dir": burn, "cutover-tag": "" }, clean.io), 0);
  assert.match(clean.logs[0], /^::warning::.*reader\/codex\.rs/);
  assert.equal(clean.calls.find((c) => c.cmd === "git").args.at(-1), `refs/tags/${DEFAULT_CUTOVER_TAG}`);

  write(burn, "crates/relayburn-cli/src/harnesses/claude.rs", "fn parse_claude_session() {}\n");
  const dirty = fakeIo(t, handlers(0));
  assert.equal(tripwire({ "burn-dir": burn }, dirty.io), 1);
  assert.match(dirty.logs[0], /^::error::.*crates\/relayburn-cli\/src\/harnesses\/claude\.rs:1/);

  const early = fakeIo(t, handlers(2));
  assert.equal(tripwire({ "burn-dir": burn }, early.io), 0);
  assert.match(early.logs[0], /^::notice::/);
});

// ----------------------------------------------------------------- workflows

function jobSteps(workflow, job) {
  const start = workflow.indexOf(`\n  ${job}:\n`);
  assert.notEqual(start, -1, `missing ${job} job`);
  const next = workflow.slice(start + 1).search(/\n  [\w-]+:\n/);
  const block = next === -1 ? workflow.slice(start) : workflow.slice(start, start + 1 + next);
  return block.split(/\n      - /).slice(1);
}

test("every drift step after the probe is gated on ready, and every step before it on relevance", async () => {
  const ci = await readFile(new URL("../.github/workflows/ci.yml", import.meta.url), "utf8");
  const steps = jobSteps(ci, "burn-contract-drift");
  const relevant = steps.findIndex((s) => s.includes("id: relevant"));
  const probe = steps.findIndex((s) => s.includes("id: probe"));
  assert.ok(relevant >= 0 && probe > relevant);
  assert.match(steps[relevant], /relevant-change/);
  for (const step of steps.slice(relevant + 1, probe + 1)) {
    assert.match(step, /if: steps\.relevant\.outputs\.run == 'true'/, step);
  }
  assert.match(steps[probe], /parity-probe --burn-dir burn/);
  const gated = steps.slice(probe + 1);
  assert.ok(gated.length >= 4);
  for (const step of gated) {
    assert.match(step, /if: steps\.probe\.outputs\.ready == 'true'/, step);
  }
  const commands = gated.join("\n");
  for (const cmd of ["pin-ai-hist", "check-resolution", "run-parity"]) {
    assert.match(commands, new RegExp(`burn-guardrails\\.mjs ${cmd}`));
  }
  assert.match(ci, /ref: \$\{\{ vars\.BURN_REF \|\| 'main' \}\}/);
  assert.match(ci, /fetch-depth: 2/);
});

test("the tripwire is scheduled, never on PRs, bounded, and files one issue", async () => {
  const wf = await readFile(new URL("../.github/workflows/burn-reader-tripwire.yml", import.meta.url), "utf8");
  assert.match(wf, /schedule:/);
  assert.doesNotMatch(wf, /pull_request/);
  assert.match(wf, /timeout-minutes: \d+/);
  assert.match(wf, /issues: write/);
  assert.match(wf, /if: failure\(\) && github\.event_name == 'schedule'/);
  assert.match(wf, /node scripts\/burn-guardrails\.mjs tripwire --burn-dir burn/);
  assert.ok(wf.includes(DEFAULT_CUTOVER_TAG), "workflow default tag matches the script's");
});

import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import {
  aggregateRounds,
  baselinesFromReport,
  evaluateGate,
  mergeBaselines,
  renderMarkdown,
} from "./benchmark-latency-lib.mjs";
import { findLatencyExecutable, withNotifyDevDependency } from "./benchmark-latency.mjs";

const here = dirname(fileURLToPath(import.meta.url));

// p50 is half the p99 throughout, so a test can steer either.
const summary = (p99, extra = {}) => ({ n: 100, p50: p99 / 2, p90: p99 * 0.8, p99, max: p99 * 1.2, ...extra });

const thresholds = {
  policy: { maxRegression: 0.1, trackedMaxRegression: 0.25, regressionFloorMs: 0.05 },
  budgets: {
    "stages@30d.fs_event": { stage: "fs", p99Ms: 1.0, enforced: true },
    "stages@30d.ingest_hook": {
      stage: "txn",
      p99Ms: 1.5,
      enforced: false,
      known: { "linux-x64": { p50: 20, p99: 200 } },
    },
  },
  baselines: {
    "linux-x64": {
      measuredOn: { cpusSeen: ["test"] },
      metrics: { "stages@30d.fs_event": { p50: 0.1, p99: 0.2 } },
    },
  },
};

const report = (metrics, { base, cpu = "test" } = {}) => ({
  profile: "gate",
  rounds: 1,
  release: true,
  machine: { platform: "linux", arch: "x64", cpu, cores: 4 },
  runs: [{ phase: "stages", days: 30, result: { metrics }, ...(base ? { base: { metrics: base } } : {}) }],
});

const gate = (metrics, options, platform = "linux-x64", t = thresholds) =>
  evaluateGate(report(metrics, options), t, platform);
const status = (result, rule) => result.checks.find((c) => c.rule === rule)?.status;

test("an enforced budget at or over its p99 fails", () => {
  const result = gate({ fs_event: summary(1.0) });
  assert.equal(result.passed, false);
  assert.equal(status(result, "budget"), "fail");
});

test("a budget enforced on a list of classes fails only on those", () => {
  const perPlatform = {
    ...thresholds,
    budgets: { "stages@30d.fs_event": { stage: "fs", p99Ms: 1.0, enforced: ["linux-x64"] } },
  };
  assert.equal(gate({ fs_event: summary(16) }, {}, "linux-x64", perPlatform).passed, false);
  const mac = gate({ fs_event: summary(16) }, {}, "darwin-arm64", perPlatform);
  assert.equal(mac.passed, true);
  assert.equal(status(mac, "budget"), "xfail");
});

test("an enforced stage: 10% plus the floor over the stored p99 fails without a base", () => {
  // 0.2 * 1.1 + 0.05 = 0.27
  assert.equal(gate({ fs_event: summary(0.27) }).passed, true);
  assert.equal(gate({ fs_event: summary(0.28) }).passed, false);
});

test("an enforced stage: against a base, the base decides and stored numbers only advise", () => {
  const slowRunner = gate({ fs_event: summary(0.5) }, { base: { fs_event: summary(0.48) } });
  assert.equal(slowRunner.passed, true, "a slower runner moved both sides");
  assert.equal(status(slowRunner, "vs stored p99"), "warn");
  const regressed = gate({ fs_event: summary(0.5) }, { base: { fs_event: summary(0.3) } });
  assert.equal(regressed.passed, false);
  assert.equal(status(regressed, "vs base p99"), "fail");
});

test("a tracked stage over budget is an expected failure, and stays green", () => {
  const result = gate({ ingest_hook: summary(40) });
  assert.equal(result.passed, true);
  assert.equal(status(result, "budget"), "xfail");
});

test("a tracked stage that meets its budget is flagged for enforcement", () => {
  const result = gate({ ingest_hook: summary(1.2) });
  assert.equal(status(result, "budget"), "xpass");
  assert.match(result.warnings.join("\n"), /stages@30d.ingest_hook is within its 1.5 ms budget/);
});

test("a tracked stage getting worse fails only when it is over both the base and its known number", () => {
  // known p50 20 -> 20 * 1.1 + 0.05 = 22.05; base p50 16 -> 16 * 1.25 + 0.05 = 20.05
  const base = { ingest_hook: summary(32) };
  // p50 21: over the base by > 25%, under the known number -> noise, not worse.
  const noise = gate({ ingest_hook: summary(42) }, { base });
  assert.equal(noise.passed, true);
  assert.equal(status(noise, "vs base p50"), "warn");
  // p50 23: over both -> worse.
  const worse = gate({ ingest_hook: summary(46) }, { base });
  assert.equal(worse.passed, false);
  assert.equal(status(worse, "vs base p50"), "fail");
  assert.equal(status(worse, "vs known p50"), "fail");
  // p50 23 against a base of 22: over the known number only -> a slower runner.
  assert.equal(gate({ ingest_hook: summary(46) }, { base: { ingest_hook: summary(44) } }).passed, true);
});

test("a tracked stage's tail does not decide", () => {
  // p99 400 against a known p99 of 200, p50 unchanged.
  assert.equal(gate({ ingest_hook: summary(400, { p50: 20 }) }).passed, true);
});

test("without a base, a tracked stage is held to its known number on a recorded CPU only", () => {
  assert.equal(gate({ ingest_hook: summary(46) }).passed, false);
  const otherCpu = gate({ ingest_hook: summary(46) }, { cpu: "other" });
  assert.equal(otherCpu.passed, true);
  assert.match(otherCpu.warnings[0], /is not one the linux-x64 baselines were recorded on/);
});

test("the regression floor grows to a share of the stage's budget", () => {
  const withShare = { ...thresholds, policy: { ...thresholds.policy, regressionFloorBudgetShare: 0.1 } };
  // fs_event budget 1.0 -> floor 0.1: 0.2 * 1.1 + 0.1 = 0.32
  assert.equal(gate({ fs_event: summary(0.32) }, {}, "linux-x64", withShare).passed, true);
  assert.equal(gate({ fs_event: summary(0.33) }, {}, "linux-x64", withShare).passed, false);
});

test("with neither a base nor stored numbers, only budgets are checked", () => {
  const result = gate({ fs_event: summary(0.5) }, {}, "darwin-arm64");
  assert.equal(result.passed, true);
  assert.deepEqual(result.checks.map((c) => c.rule), ["budget"]);
  assert.match(result.warnings[0], /only budgets are checked/);
});

test("a budgeted stage with no samples fails", () => {
  const result = gate({ fs_event: { n: 0 } });
  assert.equal(result.passed, false);
  assert.equal(status(result, "samples"), "fail");
});

test("rounds fold to the median percentile, keeping the worst round visible", () => {
  const folded = aggregateRounds([
    { phase: "stages", metrics: { x: summary(1) } },
    { phase: "stages", metrics: { x: summary(9) } },
    { phase: "stages", metrics: { x: summary(2) } },
  ]);
  assert.equal(folded.metrics.x.p99, 2);
  assert.equal(folded.metrics.x.worstP99, 9);
  assert.equal(folded.metrics.x.worstP50, 4.5);
  assert.equal(folded.metrics.x.rounds, 3);
  assert.equal(folded.phase, "stages");
});

test("recording splits enforced baselines from tracked known numbers, worst round, rounded up", () => {
  const recorded = baselinesFromReport(
    report({
      fs_event: summary(0.2, { worstP50: 0.12, worstP99: 0.2341 }),
      ingest_hook: summary(30),
      open_db: summary(4),
    }),
    thresholds.budgets,
    "linux-x64",
  );
  assert.deepEqual(recorded.baselines, { "stages@30d.fs_event": { p50: 0.12, p99: 0.235 } });
  assert.deepEqual(recorded.known, { "stages@30d.ingest_hook": { p50: 15, p99: 30 } });
  const merged = mergeBaselines({ "stages@30d.fs_event": { p50: 0.1, p99: 0.3 } }, recorded.baselines);
  assert.deepEqual(merged["stages@30d.fs_event"], { p50: 0.12, p99: 0.3 });
});

test("the markdown names every metric, the known-over-budget list and the verdict", () => {
  const r = report({ fs_event: summary(0.2), ingest_hook: summary(19) });
  const text = renderMarkdown(r, evaluateGate(r, thresholds, "linux-x64"), thresholds);
  assert.match(text, /`stages@30d.fs_event`/);
  assert.match(text, /1\.500 \(tracked\)/);
  assert.match(text, /Known over budget on linux-x64/);
  assert.match(text, /\| txn \| `stages@30d.ingest_hook` \| 1\.500 \| 20\.000 \| 200\.000 \| 9\.500 \| 19\.000 \|/);
  assert.match(text, /\*\*Gate: pass\*\*/);
});

test("the harness executable is found in cargo's message stream", () => {
  const stdout = [
    "not json",
    JSON.stringify({ reason: "compiler-artifact", target: { name: "sync_bench" }, executable: "/a" }),
    JSON.stringify({ reason: "compiler-artifact", target: { name: "latency_bench" }, executable: "/b" }),
  ].join("\n");
  assert.equal(findLatencyExecutable(stdout), "/b");
});

test("a base manifest gains the harness's notify dev-dependency exactly once", () => {
  const without = '[package]\nname = "x"\n\n[dev-dependencies]\nlibc = "0.2"\n';
  const once = withNotifyDevDependency(without);
  assert.match(once, /\[dev-dependencies\]\nnotify = /);
  assert.equal(withNotifyDevDependency(once), once);
  assert.match(withNotifyDevDependency('[package]\nname = "x"\n'), /\[dev-dependencies\]\nnotify = /);
});

test("every budget names a run the gate profile measures, and every tracked one has known numbers", () => {
  const file = JSON.parse(readFileSync(join(here, "latency-thresholds.json"), "utf8"));
  const runs = new Set(file.profiles.gate.runs.map((run) => `${run.phase}@${run.days}d`));
  for (const [id, budget] of Object.entries(file.budgets)) {
    assert.ok(runs.has(id.split(".")[0]), `${id} is not measured by the gate profile`);
    for (const platform of ["linux-x64", "darwin-arm64"]) {
      const enforced = Array.isArray(budget.enforced) ? budget.enforced.includes(platform) : budget.enforced;
      if (!enforced) assert.ok(budget.known?.[platform], `${id} is tracked on ${platform} with no known numbers`);
    }
  }
});

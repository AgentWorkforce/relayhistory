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
import { findLatencyExecutable } from "./benchmark-latency.mjs";

const here = dirname(fileURLToPath(import.meta.url));

const summary = (p99, extra = {}) => ({ n: 100, p50: p99 / 2, p90: p99 * 0.8, p99, max: p99 * 1.2, ...extra });

const thresholds = {
  policy: { maxRegression: 0.1, regressionFloorMs: 0.05 },
  budgets: {
    "stages@30d.fs_event": { stage: "fs", p99Ms: 1.0, enforced: true },
    "stages@30d.ingest_hook": { stage: "txn", p99Ms: 1.5, enforced: false },
  },
  baselines: {
    "linux-x64": {
      metrics: {
        "stages@30d.fs_event": { p99: 0.2 },
        "stages@30d.ingest_hook": { p99: 20 },
      },
    },
  },
};

const report = (metrics) => ({
  profile: "gate",
  rounds: 1,
  release: true,
  machine: { platform: "linux", arch: "x64", cpu: "test", cores: 4 },
  runs: [{ phase: "stages", days: 30, result: { metrics } }],
});

test("an enforced budget at or over its p99 fails", () => {
  const gate = evaluateGate(report({ fs_event: summary(1.0) }), thresholds, "linux-x64");
  assert.equal(gate.passed, false);
  assert.equal(gate.checks.find((c) => c.rule === "budget").status, "fail");
});

test("a tracked budget over its p99 warns and stays green", () => {
  const gate = evaluateGate(report({ ingest_hook: summary(19) }), thresholds, "linux-x64");
  assert.equal(gate.passed, true);
  assert.equal(gate.checks.find((c) => c.rule === "budget").status, "warn");
});

test("a budget enforced on a list of classes fails only on those", () => {
  const perPlatform = {
    ...thresholds,
    budgets: { "stages@30d.fs_event": { stage: "fs", p99Ms: 1.0, enforced: ["linux-x64"] } },
  };
  const r = report({ fs_event: summary(16) });
  assert.equal(evaluateGate(r, perPlatform, "linux-x64").passed, false);
  const mac = evaluateGate(r, perPlatform, "darwin-arm64");
  assert.equal(mac.passed, true);
  assert.equal(mac.checks[0].status, "warn");
});

test("a p99 more than 10% plus the floor over baseline fails", () => {
  // 20 * 1.1 + 0.05 = 22.05
  assert.equal(evaluateGate(report({ ingest_hook: summary(22.05) }), thresholds, "linux-x64").passed, true);
  assert.equal(evaluateGate(report({ ingest_hook: summary(22.06) }), thresholds, "linux-x64").passed, false);
});

test("the floor keeps microsecond jitter on a tiny stage from failing", () => {
  // 0.2 * 1.1 = 0.22, but 0.26 is within the 0.05 ms floor.
  assert.equal(evaluateGate(report({ fs_event: summary(0.26) }), thresholds, "linux-x64").passed, true);
  assert.equal(evaluateGate(report({ fs_event: summary(0.28) }), thresholds, "linux-x64").passed, false);
});

test("a platform without baselines gets budgets only, and a warning", () => {
  const gate = evaluateGate(report({ ingest_hook: summary(500) }), thresholds, "darwin-arm64");
  assert.equal(gate.passed, true);
  assert.equal(gate.checks.filter((c) => c.rule === "regression").length, 0);
  assert.match(gate.warnings[0], /no stored baselines for darwin-arm64/);
});

test("rounds fold to the median percentile, keeping the worst round visible", () => {
  const folded = aggregateRounds([
    { phase: "stages", metrics: { x: summary(1) } },
    { phase: "stages", metrics: { x: summary(9) } },
    { phase: "stages", metrics: { x: summary(2) } },
  ]);
  assert.equal(folded.metrics.x.p99, 2);
  assert.equal(folded.metrics.x.worstP99, 9);
  assert.equal(folded.metrics.x.rounds, 3);
  assert.equal(folded.phase, "stages");
});

test("recorded baselines take the worst round, round up, and never tighten when merged", () => {
  const recorded = baselinesFromReport(
    report({ fs_event: summary(0.2, { worstP99: 0.2341 }), open_db: summary(4) }),
    thresholds.budgets,
  );
  // open_db has no budget, so it is reported but never gated.
  assert.deepEqual(recorded, { "stages@30d.fs_event": { p99: 0.235 } });
  const merged = mergeBaselines({ "stages@30d.fs_event": { p99: 0.3 } }, recorded);
  assert.equal(merged["stages@30d.fs_event"].p99, 0.3);
});

test("the markdown table names every metric and the verdict", () => {
  const r = report({ fs_event: summary(0.2), ingest_hook: summary(19) });
  const text = renderMarkdown(r, evaluateGate(r, thresholds, "linux-x64"), thresholds);
  assert.match(text, /`stages@30d.fs_event`/);
  assert.match(text, /1\.500 \(tracked\)/);
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

test("every budget names a run the gate profile measures", () => {
  const file = JSON.parse(readFileSync(join(here, "latency-thresholds.json"), "utf8"));
  const runs = new Set(file.profiles.gate.runs.map((run) => `${run.phase}@${run.days}d`));
  for (const id of Object.keys(file.budgets)) {
    assert.ok(runs.has(id.split(".")[0]), `${id} is not measured by the gate profile`);
  }
});

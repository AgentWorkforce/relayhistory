import assert from "node:assert/strict";
import { mkdtemp, readFile, rm, readdir } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import {
  CALIBRATION_CLAMP,
  calibrationFactor,
  claudeTranscript,
  codexRollout,
  createRng,
  evaluateGate,
  planStore,
  renderMarkdownTable,
  REPORT_COLUMNS,
} from "./benchmark-sync-lib.mjs";
import { generateStore } from "./gen-synthetic-history.mjs";
import { CALIBRATION_PHASE, PHASE_ORDER, findHarnessExecutable } from "./benchmark-sync.mjs";

const thresholds = JSON.parse(
  await readFile(new URL("./benchmark-thresholds.json", import.meta.url), "utf8"),
);

test("the same seed produces the same store and a different seed does not", async () => {
  const root = await mkdtemp(join(tmpdir(), "sync-bench-seed-"));
  try {
    const plan = planStore({ seed: 7, targetBytes: 64 * 1024, turns: 2, sources: ["claude"] });
    const first = await generateStore(plan, join(root, "a"));
    const second = await generateStore(plan, join(root, "b"));
    assert.equal(first.storeBytes, second.storeBytes);
    assert.equal(first.sessionCount, second.sessionCount);
    const read = async (manifest, index) => readFile(manifest.sessions[index].path, "utf8");
    assert.equal(await read(first, 0), await read(second, 0));

    const other = await generateStore(planStore({ ...plan, seed: 8 }), join(root, "c"));
    assert.notEqual(await read(first, 0), await read(other, 0));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("the manifest counts match what actually landed on disk", async () => {
  const root = await mkdtemp(join(tmpdir(), "sync-bench-manifest-"));
  try {
    const plan = planStore({
      seed: 3, targetBytes: 96 * 1024, turns: 2,
      sources: ["claude", "codex", "cursor", "grok"], largeSessionBytes: 24 * 1024,
    });
    const manifest = await generateStore(plan, join(root, "home"));
    assert.ok(manifest.storeBytes >= plan.targetBytes, "the store reached its target");
    assert.equal(manifest.sessions.length, manifest.sessionCount);
    // Every source the plan asked for is represented, and the hydration target
    // is the largest Claude transcript rather than whichever landed first.
    const sources = new Set(manifest.sessions.map((session) => session.source));
    assert.deepEqual([...sources].sort(), ["claude", "codex", "cursor", "grok"]);
    const largestClaude = manifest.sessions
      .filter((session) => session.source === "claude")
      .reduce((best, session) => (session.bytes > best.bytes ? session : best));
    assert.equal(manifest.hydrationTarget.sessionId, largestClaude.sessionId);
    assert.ok(manifest.hydrationTarget.bytes >= 24 * 1024);
    // `storeFiles` counts files, not directories.
    const claudeProject = join(root, "home", ".claude/projects", plan.project);
    const entries = await readdir(claudeProject);
    assert.ok(entries.length > 0);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("generated records carry the fields the ingest parsers read", () => {
  const plan = planStore({ seed: 11, sources: ["claude"], toolResults: 2 });
  const rng = createRng(plan.seed);
  const lines = claudeTranscript(plan, "abc", rng, { turns: 1, baseMs: Date.UTC(2026, 0, 1) })
    .trim().split("\n").map((line) => JSON.parse(line));
  assert.equal(lines.length, 3);
  for (const record of lines) {
    assert.equal(record.sessionId, "abc");
    assert.ok(typeof record.timestamp === "string");
    assert.ok(record.message.content.length > 0);
  }
  const assistant = lines[1].message.content.map((block) => block.type);
  assert.deepEqual(assistant, ["thinking", "text", "tool_use", "tool_use"]);
  const results = lines[2].message.content;
  assert.equal(results.length, 2);
  assert.equal(results[0].type, "tool_result");
  assert.equal(results[0].tool_use_id, lines[1].message.content[2].id);

  const codex = codexRollout(plan, "def", createRng(plan.seed), { turns: 1, baseMs: Date.UTC(2026, 0, 1) })
    .trim().split("\n").map((line) => JSON.parse(line));
  assert.deepEqual(codex.map((record) => record.type), [
    "session_meta", "turn_context", "event_msg", "event_msg",
  ]);
  assert.equal(codex[0].payload.id, "def");
});

function report(phases, calibrationMs = 100) {
  return { phases, calibrationMs };
}

const gateThresholds = {
  policy: {
    minRecordsPerSecondFactor: 0.5,
    maxElapsedMsFactor: 2,
    maxPeakRssFactor: 1.5,
    absoluteFloors: { elapsedMs: 40 },
  },
  profiles: {
    demo: {
      calibrationMs: 100,
      phases: {
        cold_sync: { recordsPerSecond: 600, peakRssBytes: 10_000_000 },
        unchanged_sync: { elapsedMs: 10 },
      },
    },
  },
};

test("the gate passes a clean run and fails a 3x slowdown", () => {
  const clean = report([
    { phase: "cold_sync", recordsPerSecond: 610, peakRssBytes: 10_100_000 },
    { phase: "unchanged_sync", elapsedMs: 11 },
  ]);
  assert.equal(evaluateGate(clean, gateThresholds, "demo").ok, true);

  const slow = report([
    { phase: "cold_sync", recordsPerSecond: 610 / 3, peakRssBytes: 10_100_000 },
    { phase: "unchanged_sync", elapsedMs: 11 },
  ]);
  const verdict = evaluateGate(slow, gateThresholds, "demo");
  assert.equal(verdict.ok, false);
  assert.match(verdict.failures.join("\n"), /cold_sync\.recordsPerSecond/);
});

test("a 1.5x memory regression fails and a smaller one does not", () => {
  const within = report([
    { phase: "cold_sync", recordsPerSecond: 610, peakRssBytes: 14_000_000 },
    { phase: "unchanged_sync", elapsedMs: 11 },
  ]);
  assert.equal(evaluateGate(within, gateThresholds, "demo").ok, true);
  const over = report([
    { phase: "cold_sync", recordsPerSecond: 610, peakRssBytes: 16_000_000 },
    { phase: "unchanged_sync", elapsedMs: 11 },
  ]);
  assert.match(evaluateGate(over, gateThresholds, "demo").failures.join("\n"), /peakRssBytes/);
});

test("an absolute floor loosens a ceiling and never tightens one", () => {
  // 10 ms baseline x2 = 20 ms, raised to the 40 ms floor: 30 ms passes.
  const noisy = report([
    { phase: "cold_sync", recordsPerSecond: 610, peakRssBytes: 10_100_000 },
    { phase: "unchanged_sync", elapsedMs: 30 },
  ]);
  assert.equal(evaluateGate(noisy, gateThresholds, "demo").ok, true);
  const past = report([
    { phase: "cold_sync", recordsPerSecond: 610, peakRssBytes: 10_100_000 },
    { phase: "unchanged_sync", elapsedMs: 41 },
  ]);
  assert.equal(evaluateGate(past, gateThresholds, "demo").ok, false);
  // The floor must not rescue a throughput floor, which is a minimum.
  const floored = { ...gateThresholds, policy: { ...gateThresholds.policy, absoluteFloors: { recordsPerSecond: 1 } } };
  const starved = report([
    { phase: "cold_sync", recordsPerSecond: 10, peakRssBytes: 10_100_000 },
    { phase: "unchanged_sync", elapsedMs: 11 },
  ]);
  assert.equal(evaluateGate(starved, floored, "demo").ok, false);
});

test("a phase that produced nothing is a failure, not a silent pass", () => {
  const missing = report([{ phase: "cold_sync", recordsPerSecond: 610, peakRssBytes: 10_100_000 }]);
  const verdict = evaluateGate(missing, gateThresholds, "demo");
  assert.equal(verdict.ok, false);
  assert.match(verdict.failures.join("\n"), /unchanged_sync.*no measurement/);

  const unmeasured = report([
    { phase: "cold_sync", recordsPerSecond: 610, peakRssBytes: null },
    { phase: "unchanged_sync", elapsedMs: 11 },
  ]);
  assert.match(
    evaluateGate(unmeasured, gateThresholds, "demo").failures.join("\n"),
    /did not measure peakRssBytes/,
  );

  assert.equal(evaluateGate(report([]), gateThresholds, "nope").ok, false);
  assert.equal(
    evaluateGate(report([]), { policy: {}, profiles: { empty: { phases: {} } } }, "empty").ok,
    false,
  );
});

test("a slower machine is normalized away, but a slower code path is not", () => {
  // Everything three times slower, including the reference workload: a busy or
  // smaller runner, not a regression.
  const busy = report([
    { phase: "cold_sync", recordsPerSecond: 610 / 3, peakRssBytes: 10_100_000 },
    { phase: "unchanged_sync", elapsedMs: 11 * 3 },
  ], 300);
  const verdict = evaluateGate(busy, gateThresholds, "demo");
  assert.equal(verdict.ok, true, verdict.failures.join("; "));
  assert.equal(verdict.calibration.raw, 3);

  // The same three-times-slower phases while the reference is unchanged: that
  // is the code, and it has to stay red.
  const regressed = report([
    { phase: "cold_sync", recordsPerSecond: 610 / 3, peakRssBytes: 10_100_000 },
    { phase: "unchanged_sync", elapsedMs: 11 * 3 },
  ], 100);
  assert.equal(evaluateGate(regressed, gateThresholds, "demo").ok, false);
});

test("the calibration factor is clamped and reports when it was", () => {
  assert.equal(calibrationFactor(100, 100).factor, 1);
  assert.equal(calibrationFactor(100, 100).clamped, false);
  const wild = calibrationFactor(100_000, 100);
  assert.equal(wild.factor, CALIBRATION_CLAMP.max);
  assert.equal(wild.clamped, true);
  assert.equal(calibrationFactor(1, 100).factor, CALIBRATION_CLAMP.min);
  for (const bad of [[0, 100], [100, 0], [NaN, 100], [undefined, 100]]) {
    assert.equal(calibrationFactor(...bad), null);
  }
  // A profile that stores a calibration baseline must get one back.
  const missing = { phases: [{ phase: "cold_sync", recordsPerSecond: 610, peakRssBytes: 1 }] };
  assert.match(
    evaluateGate(missing, gateThresholds, "demo").failures.join("\n"),
    /stores a calibration baseline but this run measured none/,
  );
});

test("the committed thresholds file is usable by the gate", () => {
  assert.ok(thresholds.policy.minRecordsPerSecondFactor > 0);
  assert.ok(thresholds.policy.maxPeakRssFactor >= 1);
  const gateProfile = thresholds.profiles["ci-debug"];
  assert.ok(gateProfile, "the PR gate profile exists");
  assert.ok(gateProfile.measuredOn?.commit, "the gate baseline records where it came from");
  assert.ok(gateProfile.calibrationMs > 0, "the gate baseline carries its reference workload");
  assert.deepEqual(Object.keys(gateProfile.phases).sort(), [...PHASE_ORDER].sort());
  assert.ok(!PHASE_ORDER.includes(CALIBRATION_PHASE), "calibration is not a gated phase");
  const known = new Set(["recordsPerSecond", "elapsedMs", "peakRssBytes"]);
  for (const [phase, metrics] of Object.entries(gateProfile.phases)) {
    assert.ok(Object.keys(metrics).length > 0, `${phase} stores at least one metric`);
    for (const [metric, value] of Object.entries(metrics)) {
      assert.ok(known.has(metric), `${phase}.${metric} is a metric the gate understands`);
      assert.ok(Number.isFinite(value) && value > 0, `${phase}.${metric} is a positive number`);
    }
  }
});

test("the harness executable is read from cargo's artifact stream", () => {
  const stream = [
    "warning: something",
    JSON.stringify({ reason: "compiler-artifact", target: { name: "ai-hist" }, executable: "/bad/ai-hist" }),
    JSON.stringify({ reason: "compiler-artifact", target: { name: "sync_bench" }, executable: null }),
    JSON.stringify({ reason: "compiler-artifact", target: { name: "sync_bench" }, executable: "/good/sync_bench-abc" }),
    JSON.stringify({ reason: "build-finished", success: true }),
  ].join("\n");
  assert.equal(findHarnessExecutable(stream), "/good/sync_bench-abc");
  assert.equal(findHarnessExecutable("no json here"), null);
});

test("every report row has one cell per column header", () => {
  const rendered = renderMarkdownTable(report([
    { phase: "cold_sync", storeBytes: 1024, elapsedMs: 12.5, records: 3, recordsPerSecond: 240, megabytesPerSecond: 0.1, bytesRead: null, peakRssBytes: 10, dbBytes: 5, walBytes: 0 },
  ])).split("\n");
  assert.equal(rendered.length, 3);
  for (const line of rendered) {
    assert.equal(line.split("|").length - 2, REPORT_COLUMNS.length);
  }
  assert.match(rendered[2], /\| — \|/, "an unmeasured field renders as a dash, not a zero");
});

test("the harness the driver runs is the file this repository ships", async () => {
  const harness = new URL("../crates/ai-hist-cli/tests/sync_bench.rs", import.meta.url);
  const source = await readFile(harness, "utf8");
  for (const phase of [...PHASE_ORDER, CALIBRATION_PHASE]) {
    assert.ok(source.includes(`"${phase}"`), `sync_bench.rs implements the ${phase} phase`);
  }
  assert.ok(fileURLToPath(harness).endsWith("sync_bench.rs"));
});

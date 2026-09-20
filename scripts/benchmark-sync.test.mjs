import assert from "node:assert/strict";
import { mkdtemp, readFile, rm, readdir, stat } from "node:fs/promises";
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
  unsupportedPhases,
} from "./benchmark-sync-lib.mjs";
import { generateStore, opencodeAvailable } from "./gen-synthetic-history.mjs";
import {
  CALIBRATION_PHASE, PHASE_ORDER, failureFooter, findHarnessExecutable, updatedProfile,
} from "./benchmark-sync.mjs";

const warningText = (verdict) => (verdict.warnings ?? []).map((w) => w.message).join("\n");

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
    assert.equal(manifest.fileSessionCount, manifest.sessions.length);
    // The total is every session the store holds, not only the file-backed
    // ones: an OpenCode store's rows are sessions too, and a count that leaves
    // them out understates every report built from this manifest.
    assert.equal(manifest.sessionCount, manifest.fileSessionCount + manifest.opencodeSessions);
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

test("the oversized session follows --sources instead of forcing Claude in", async () => {
  const root = await mkdtemp(join(tmpdir(), "sync-bench-sources-"));
  try {
    const plan = planStore({
      seed: 5, targetBytes: 48 * 1024, turns: 2,
      sources: ["codex"], largeSessionBytes: 24 * 1024,
    });
    const manifest = await generateStore(plan, join(root, "home"));
    // A plan that did not ask for Claude must not get a Claude transcript: it
    // would be ingested by `sync`, counted in the throughput, and reported as
    // a codex-only measurement.
    await assert.rejects(
      () => stat(join(root, "home", ".claude")),
      /ENOENT/,
      "no Claude tree is written for a codex-only plan",
    );
    assert.ok(manifest.sessions.every((session) => session.source === "codex"));
    assert.equal(manifest.hydrationTarget.source, "codex");
    // The generator scales a turn count from a probe and trims nothing, so the
    // oversized session lands near the request rather than on it. What has to
    // hold is that it is close and that it dominates every other session —
    // otherwise per-transcript cost is not what the hydration phases measure.
    assert.ok(
      manifest.hydrationTarget.bytes >= 0.9 * plan.largeSessionBytes,
      `oversized session is ${manifest.hydrationTarget.bytes} B, wanted ~${plan.largeSessionBytes}`,
    );
    const others = manifest.sessions
      .filter((session) => session.sessionId !== manifest.hydrationTarget.sessionId);
    assert.ok(others.length > 0, "the plan also wrote ordinary sessions");
    assert.ok(others.every((session) => session.bytes < manifest.hydrationTarget.bytes));
    // `append_one_record` in the harness writes a Claude-shaped record and
    // there is no codex equivalent, so this plan has no incremental target.
    assert.equal(manifest.incrementalTarget, null);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("a phase that needs Claude is refused before anything is measured", () => {
  const codexOnly = planStore({ sources: ["codex"] });
  const refused = unsupportedPhases(PHASE_ORDER, { plan: codexOnly });
  assert.deepEqual(refused.map(({ phase }) => phase), ["incremental_sync"]);
  assert.match(refused[0].reason, /needs a `claude` source/);
  assert.match(refused[0].reason, /this plan has codex/);
  // Everything else is provider-agnostic and must not be refused.
  assert.deepEqual(
    unsupportedPhases(
      PHASE_ORDER.filter((phase) => phase !== "incremental_sync"),
      { plan: codexOnly },
    ),
    [],
  );
  assert.deepEqual(unsupportedPhases(PHASE_ORDER, { plan: planStore({}) }), []);
  assert.deepEqual(unsupportedPhases([], { plan: codexOnly }), []);
});

test("listing Claude is not the same as writing one, and the manifest decides", async () => {
  const root = await mkdtemp(join(tmpdir(), "sync-bench-empty-claude-"));
  try {
    // `claude` is in --sources, but the oversized codex session already meets
    // the byte target, so the round-robin loop never runs and no Claude
    // transcript is ever written. Checking the source list alone accepts
    // `incremental_sync` here and only discovers the truth after generating.
    const plan = planStore({
      seed: 4, sources: ["codex", "claude"],
      targetBytes: 32 * 1024, largeSessionBytes: 64 * 1024, turns: 2,
    });
    const manifest = await generateStore(plan, join(root, "home"));
    assert.deepEqual(
      manifest.sessions.map((session) => session.source), ["codex"],
      "the premise: nothing Claude was written despite Claude being requested",
    );
    assert.equal(manifest.incrementalTarget, null);

    const refused = unsupportedPhases(["cold_sync", "incremental_sync"], { manifest });
    assert.deepEqual(refused.map(({ phase }) => phase), ["incremental_sync"]);
    assert.match(refused[0].reason, /generated store has no Claude transcript/);
    // Phases the store can serve are not caught by the same check.
    assert.deepEqual(unsupportedPhases(["cold_sync", "hydrate_cold"], { manifest }), []);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("a phase order that cannot provide its own setup is refused", () => {
  const plan = planStore({});
  // The reported bug: `incremental_sync` runs first and reaches the harness
  // with no database, because validation only looked at the set of phases.
  const backwards = unsupportedPhases(["incremental_sync", "cold_sync"], { plan });
  assert.deepEqual(backwards.map(({ phase }) => phase), ["incremental_sync"]);
  assert.match(backwards[0].reason, /needs `cold_sync` to run before it/);

  assert.deepEqual(unsupportedPhases(["cold_sync", "incremental_sync"], { plan }), []);
  assert.deepEqual(unsupportedPhases(PHASE_ORDER, { plan }), []);

  // Every dependent phase, alone, is missing its setup.
  for (const phase of PHASE_ORDER.filter((name) => name !== "cold_sync")) {
    const alone = unsupportedPhases([phase], { plan });
    assert.deepEqual(alone.map((entry) => entry.phase), [phase], `${phase} alone is refused`);
    assert.match(alone[0].reason, /cold_sync/);
  }

  // A repeated phase is malformed: the second `cold_sync` would find the
  // database its predecessor created.
  assert.match(
    unsupportedPhases(["cold_sync", "cold_sync"], { plan })[0].reason,
    /more than once/,
  );
});

test("OpenCode sessions are counted, not dropped from the total", async (t) => {
  if (!(await opencodeAvailable())) {
    t.skip("node:sqlite is unavailable on this Node; the OpenCode fixture cannot be written");
    return;
  }
  const root = await mkdtemp(join(tmpdir(), "sync-bench-opencode-"));
  try {
    const plan = planStore({
      seed: 9, targetBytes: 48 * 1024, turns: 2,
      sources: ["claude", "opencode"], largeSessionBytes: 0,
    });
    const manifest = await generateStore(plan, join(root, "home"));
    assert.ok(manifest.opencodeSessions >= 10, "the OpenCode store was written");
    assert.equal(manifest.fileSessionCount, manifest.sessions.length);
    assert.equal(manifest.sessionCount, manifest.fileSessionCount + manifest.opencodeSessions);
    assert.ok(
      manifest.sessionCount > manifest.sessions.length,
      "the total exceeds the file-backed list once an OpenCode store exists",
    );
    await stat(join(root, "home", ".local/share/opencode/opencode.db"));
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

// The tests below that exercise scaling set `calibration: "applied"`
// explicitly, because the committed policy no longer applies it by default.
const gateThresholds = {
  policy: {
    minRecordsPerSecondFactor: 0.5,
    maxElapsedMsFactor: 2,
    maxPeakRssFactor: 1.5,
    calibration: "applied",
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

test("a phase-specific floor only loosens the named phase", () => {
  const targeted = {
    policy: {
      ...gateThresholds.policy,
      absoluteFloors: { elapsedMs: 40 },
      absoluteFloorsByPhase: { unchanged_sync: { elapsedMs: 120 } },
    },
    profiles: {
      demo: {
        calibrationMs: 100,
        phases: {
          unchanged_sync: { elapsedMs: 10 },
          hydrate_unchanged: { elapsedMs: 15 },
        },
      },
    },
  };
  const run = report([
    { phase: "unchanged_sync", elapsedMs: 100 },
    { phase: "hydrate_unchanged", elapsedMs: 41 },
  ]);
  const verdict = evaluateGate(run, targeted, "demo");
  assert.equal(verdict.ok, false);
  const failures = verdict.failures.join("\n");
  assert.match(failures, /hydrate_unchanged\.elapsedMs/);
  assert.doesNotMatch(failures, /unchanged_sync\.elapsedMs/);
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

test("the committed gate accepts both observed CI runners and still catches 3x", () => {
  // The two real `verify` runs on this branch, replayed against the committed
  // thresholds. One of them used to fail on a completely healthy runner.
  const runs = {
    "6973P-C": {
      calibrationMs: 129.0,
      machine: { cpu: "Intel(R) Xeon(R) 6973P-C" },
      phases: [
        { phase: "cold_sync", recordsPerSecond: 478.6, peakRssBytes: 12644352 },
        { phase: "incremental_sync", elapsedMs: 423.3, peakRssBytes: 10297344 },
        { phase: "unchanged_sync", elapsedMs: 47.1, peakRssBytes: 9687040 },
        { phase: "hydrate_cold", recordsPerSecond: 406.3, peakRssBytes: 10895360 },
        { phase: "hydrate_unchanged", elapsedMs: 10.5, peakRssBytes: 8708096 },
      ],
    },
    "8573C": {
      calibrationMs: 166.8,
      machine: { cpu: "INTEL(R) XEON(R) PLATINUM 8573C" },
      phases: [
        { phase: "cold_sync", recordsPerSecond: 631.0, peakRssBytes: 12476416 },
        { phase: "incremental_sync", elapsedMs: 272.9, peakRssBytes: 10395648 },
        { phase: "unchanged_sync", elapsedMs: 50.8, peakRssBytes: 9646080 },
        { phase: "hydrate_cold", recordsPerSecond: 296.7, peakRssBytes: 10936320 },
        { phase: "hydrate_unchanged", elapsedMs: 14.1, peakRssBytes: 8634368 },
      ],
    },
  };
  const tripled = (run) => ({
    ...run,
    phases: run.phases.map((phase) => ({
      ...phase,
      ...(phase.recordsPerSecond === undefined
        ? {} : { recordsPerSecond: phase.recordsPerSecond / 3 }),
      ...(phase.elapsedMs === undefined ? {} : { elapsedMs: phase.elapsedMs * 3 }),
    })),
  });
  for (const [cpu, run] of Object.entries(runs)) {
    const healthy = evaluateGate(run, thresholds, "ci-debug");
    assert.equal(healthy.ok, true, `${cpu} healthy: ${healthy.failures.join("; ")}`);
    assert.deepEqual(healthy.warnings, [], `${cpu} is a known baseline CPU, so nothing to warn`);
    // Every margin at or above the 2x the policy asks for, on both CPUs.
    for (const check of healthy.checks.filter((c) => c.metric !== "peakRssBytes")) {
      const margin = check.metric === "recordsPerSecond"
        ? check.value / check.bound : check.bound / check.value;
      assert.ok(margin >= 2, `${cpu} ${check.phase}.${check.metric} margin ${margin.toFixed(2)}x`);
    }
    assert.equal(evaluateGate(tripled(run), thresholds, "ci-debug").ok, false,
      `${cpu} must go red on a 3x regression`);
  }
});

test("the committed thresholds tolerate the observed healthy unchanged_sync jitter", () => {
  const verdict = evaluateGate({
    calibrationMs: 166.8,
    machine: { cpu: "INTEL(R) XEON(R) PLATINUM 8573C" },
    phases: [
      { phase: "cold_sync", recordsPerSecond: 631.0, peakRssBytes: 12476416 },
      { phase: "incremental_sync", elapsedMs: 272.9, peakRssBytes: 11726848 },
      { phase: "unchanged_sync", elapsedMs: 113.0, peakRssBytes: 9646080 },
      { phase: "hydrate_cold", recordsPerSecond: 296.7, peakRssBytes: 10936320 },
      { phase: "hydrate_unchanged", elapsedMs: 14.1, peakRssBytes: 8634368 },
    ],
  }, thresholds, "ci-debug");
  assert.equal(verdict.ok, true, verdict.failures.join("; "));
});

test("re-measuring keeps the machine metadata the bounds depend on", () => {
  const before = structuredClone(thresholds);
  const report = {
    commit: "abc1234",
    generatedAt: "2026-09-21T00:00:00.000Z",
    cargoProfile: "debug",
    calibrationMs: 151.2,
    machine: { cpu: "AMD EPYC 7763", cores: 4, platform: "linux", arch: "x64", rustc: "rustc 1.98.1" },
    phases: [
      { phase: "cold_sync", recordsPerSecond: 500.7, peakRssBytes: 12_000_000 },
      { phase: "incremental_sync", elapsedMs: 400.2, peakRssBytes: 10_000_000 },
      { phase: "unchanged_sync", elapsedMs: 49.4, peakRssBytes: 9_000_000 },
      { phase: "hydrate_cold", recordsPerSecond: 310.9, peakRssBytes: 10_000_000 },
      { phase: "hydrate_unchanged", elapsedMs: 13.6, peakRssBytes: 8_000_000 },
    ],
  };
  const next = updatedProfile(before.profiles["ci-debug"], report);

  // The schema the contract test and the unknown-CPU warning both rely on.
  assert.ok(next.measuredOn.machineClass, "machineClass survives a re-measure");
  assert.ok(Array.isArray(next.measuredOn.cpusSeen));
  assert.ok(next.measuredOn.cpusSeen.includes("AMD EPYC 7763"), "the new CPU is recorded");
  for (const cpu of before.profiles["ci-debug"].measuredOn.cpusSeen) {
    assert.ok(next.measuredOn.cpusSeen.includes(cpu), `${cpu} is not forgotten`);
  }
  assert.equal(next.measuredOn.runs.length, before.profiles["ci-debug"].measuredOn.runs.length + 1);
  const appended = next.measuredOn.runs.at(-1);
  assert.equal(appended.commit, "abc1234");
  assert.equal(appended.cpu, "AMD EPYC 7763");
  assert.equal(appended.calibrationMs, 151.2);

  // Baselines are rounded away from the bound, so the run they came from passes.
  assert.equal(next.phases.cold_sync.recordsPerSecond, 500);
  assert.equal(next.phases.incremental_sync.elapsedMs, 401);
  assert.equal(next.calibrationMs, 151.2);

  // And the warning still works against the rewritten profile.
  const rewritten = { ...before, profiles: { ...before.profiles, "ci-debug": next } };
  const elsewhere = {
    calibrationMs: 151.2,
    machine: { cpu: "Apple M2 Max" },
    phases: report.phases,
  };
  assert.match(warningText(evaluateGate(elsewhere, rewritten, "ci-debug")), /Apple M2 Max/);
  assert.deepEqual(evaluateGate(report, rewritten, "ci-debug").warnings, []);
});

test("a source listed twice is refused instead of quietly shrinking the store", () => {
  assert.throws(
    () => planStore({ sources: ["claude", "claude"] }),
    /listed more than once|duplicate/i,
  );
  assert.throws(() => planStore({ sources: ["codex", "claude", "codex"] }), /codex/);
  // Positive control: a distinct pair is still fine.
  assert.deepEqual(planStore({ sources: ["claude", "codex"] }).sources, ["claude", "codex"]);
});

test("an OpenCode store counts against the byte target, not on top of it", async (t) => {
  if (!(await opencodeAvailable())) {
    t.skip("node:sqlite is unavailable on this Node; the OpenCode fixture cannot be written");
    return;
  }
  const root = await mkdtemp(join(tmpdir(), "sync-bench-oc-budget-"));
  try {
    const targetBytes = 512 * 1024;
    const withOpencode = planStore({
      seed: 12, targetBytes, turns: 2, sources: ["claude", "opencode"], largeSessionBytes: 0,
    });
    const manifest = await generateStore(withOpencode, join(root, "home"));
    assert.ok(manifest.opencodeSessions > 0, "the OpenCode store was written");
    // The requested corpus size is the whole store, SQLite included. Otherwise
    // two runs with equal targets measure different amounts of work.
    assert.ok(
      manifest.storeBytes <= targetBytes * 1.25,
      `store is ${manifest.storeBytes} B against a ${targetBytes} B target`,
    );
    assert.ok(manifest.storeBytes >= targetBytes * 0.75, "and is not far short either");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("the failure footer names the warning that fired, and claims no more", () => {
  const profile = thresholds.profiles["ci-debug"];
  const known = profile.measuredOn.cpusSeen[0];
  const calibration = { measuredMs: 300, baselineMs: 147.9, raw: 2.03, factor: 2.03 };

  // A known CPU that merely ran the reference slowly. That is load, not new
  // hardware, so the footer must not say the run was off the baseline class —
  // a red here is more likely contention or a genuine regression.
  const bandOnly = failureFooter(
    { warnings: [{ kind: "calibration-band", message: "…", raw: 2.03, knownCpu: true }], calibration },
    profile,
  );
  assert.match(bandOnly, /took 2\.03x as long/);
  assert.match(bandOnly, new RegExp(known.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")));
  assert.doesNotMatch(bandOnly, /not (on )?one of them|different machine class/i);
  assert.match(bandOnly, /load|contention/i);

  // An unfamiliar CPU: here the machine-class claim is the right one.
  const unknownCpu = failureFooter(
    { warnings: [{ kind: "unknown-cpu", message: "…", cpu: "Apple M2 Max" }], calibration: null },
    profile,
  );
  assert.match(unknownCpu, /Apple M2 Max/);
  assert.match(unknownCpu, /not one of them/i);
  assert.match(unknownCpu, /as likely to be the hardware as the code/);

  // Both: the hardware claim wins, because it is the stronger explanation.
  const both = failureFooter(
    {
      warnings: [
        { kind: "unknown-cpu", message: "…", cpu: "Apple M2 Max" },
        { kind: "calibration-band", message: "…", raw: 2.03, knownCpu: false },
      ],
      calibration,
    },
    profile,
  );
  assert.match(both, /Apple M2 Max/);
  assert.match(both, /not one of them/i);

  // No warnings at all: no hardware talk of any kind.
  const clean = failureFooter({ warnings: [], calibration: null }, profile);
  assert.doesNotMatch(clean, /hardware|machine class/i);
});

test("unfamiliar hardware is called out without failing the gate", () => {
  const onKnownCpu = {
    calibrationMs: 147.9,
    machine: { cpu: "Intel(R) Xeon(R) 6973P-C" },
    phases: [{ phase: "cold_sync", recordsPerSecond: 479, peakRssBytes: 1 }],
  };
  // Same numbers, different CPU: a warning, and only a warning.
  const elsewhere = { ...onKnownCpu, machine: { cpu: "Apple M2 Max" } };
  const verdict = evaluateGate(elsewhere, thresholds, "ci-debug");
  assert.match(warningText(verdict), /this is Apple M2 Max/);
  assert.ok(
    !verdict.failures.some((failure) => /Apple M2 Max/.test(failure)),
    "an unfamiliar CPU is never itself a failure",
  );
  assert.deepEqual(evaluateGate(onKnownCpu, thresholds, "ci-debug").warnings, []);
});

test("a calibration far from the baseline runs is reported, not applied", () => {
  const diagnostic = {
    policy: { minRecordsPerSecondFactor: 0.5, calibration: "diagnostic",
      calibrationWarnBand: { min: 0.7, max: 1.4 } },
    profiles: { demo: { calibrationMs: 100, phases: { cold_sync: { recordsPerSecond: 600 } } } },
  };
  // Twice as long on the reference, but the measurement itself is healthy:
  // green, with the discrepancy said out loud.
  const report = { calibrationMs: 200, phases: [{ phase: "cold_sync", recordsPerSecond: 610 }] };
  const verdict = evaluateGate(report, diagnostic, "demo");
  assert.equal(verdict.ok, true);
  assert.equal(verdict.calibrationApplied, false);
  assert.equal(verdict.checks[0].normalized, 610, "the reading is not scaled");
  assert.match(warningText(verdict), /2\.00x as long/);

  // The same thresholds with the factor switched back on scale it instead.
  const applied = { ...diagnostic, policy: { ...diagnostic.policy, calibration: "applied" } };
  const scaled = evaluateGate(report, applied, "demo");
  assert.equal(scaled.calibrationApplied, true);
  assert.equal(scaled.checks[0].normalized, 1220);
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
  // The baselines must say which machines produced them: bounds are absolute
  // numbers, so where they came from is part of what they mean.
  const measuredOn = gateProfile.measuredOn;
  assert.ok(measuredOn?.machineClass, "the gate baseline names the machine class");
  assert.ok(measuredOn.cpusSeen?.length >= 1, "and the CPUs it was measured on");
  assert.ok(measuredOn.runs?.length >= 1, "and the runs it came from");
  for (const run of measuredOn.runs) {
    assert.ok(run.commit && run.run, "each baseline run is identified by commit and run id");
    assert.ok(measuredOn.cpusSeen.includes(run.cpu), "and names a CPU the profile lists");
  }
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

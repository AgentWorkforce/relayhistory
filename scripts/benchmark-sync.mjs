// Full-sync and hydration throughput benchmark, and the CI regression gate.
//
//   node scripts/benchmark-sync.mjs --gate                  # fast CI subset
//   node scripts/benchmark-sync.mjs --profile full --output docs/sync-bench.md
//
// This script owns orchestration only. It generates a deterministic synthetic
// store with `gen-synthetic-history.mjs`, runs one phase of the Rust harness
// (`crates/ai-hist-cli/tests/sync_bench.rs`) per child process so each phase's
// peak RSS is its own, and applies the thresholds in
// `scripts/benchmark-thresholds.json`.
//
// The measurement itself is deliberately not here: `sync` and `hydrate_session`
// have no command-line surface, and driving them through the N-API addon would
// measure the binding as much as the engine.

import { spawnSync } from "node:child_process";
import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { arch, cpus, platform, tmpdir, totalmem } from "node:os";
import { dirname, extname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { generateStore } from "./gen-synthetic-history.mjs";
import {
  FILE_SOURCES,
  baselinesFromReport,
  evaluateGate,
  formatBytes,
  planStore,
  renderMarkdownTable,
  unsupportedPhases,
} from "./benchmark-sync-lib.mjs";

const here = dirname(fileURLToPath(import.meta.url));
const repositoryRoot = resolve(here, "..");
const THRESHOLDS = join(here, "benchmark-thresholds.json");

/**
 * The reference workload, measured beside every run so the gate can divide out
 * how fast or how busy the machine is. See `calibration()` in the harness.
 */
export const CALIBRATION_PHASE = "calibration";

/** Phases in the order they must run: each one leans on the previous state. */
export const PHASE_ORDER = [
  "cold_sync",
  "incremental_sync",
  "unchanged_sync",
  "hydrate_cold",
  "hydrate_unchanged",
];

/** Metrics written back by `--update-baselines`, per phase. */
const BASELINE_METRICS = {
  cold_sync: ["recordsPerSecond", "peakRssBytes"],
  incremental_sync: ["elapsedMs", "peakRssBytes"],
  unchanged_sync: ["elapsedMs", "peakRssBytes"],
  hydrate_cold: ["recordsPerSecond", "peakRssBytes"],
  hydrate_unchanged: ["elapsedMs", "peakRssBytes"],
};

function option(argv, name, fallback) {
  const prefix = `--${name}=`;
  const inline = argv.find((argument) => argument.startsWith(prefix));
  if (inline) return inline.slice(prefix.length);
  const index = argv.indexOf(`--${name}`);
  if (index >= 0 && argv[index + 1] && !argv[index + 1].startsWith("--")) return argv[index + 1];
  return fallback;
}

function flag(argv, name) {
  return argv.includes(`--${name}`);
}

function git(...args) {
  const result = spawnSync("git", ["-C", repositoryRoot, ...args], { encoding: "utf8" });
  return result.status === 0 ? result.stdout.trim() : null;
}

function machine() {
  const rustc = spawnSync("rustc", ["--version"], { encoding: "utf8" });
  return {
    platform: platform(),
    arch: arch(),
    cpu: cpus()[0]?.model ?? "unknown",
    cores: cpus().length,
    memoryBytes: totalmem(),
    node: process.version,
    rustc: rustc.status === 0 ? rustc.stdout.trim() : null,
  };
}

/**
 * Build the harness once and return the path of its test executable.
 *
 * Going through `cargo test` for every phase costs several seconds of
 * freshness checking each time, which would dwarf the phases it is timing. In
 * CI this call is a no-op: `cargo test --workspace --all-features` has already
 * built the same target in the same profile.
 */
export function buildHarness(context) {
  const args = [];
  if (context.toolchain) args.push(`+${context.toolchain}`);
  // The selector mirrors CI's `cargo test --workspace --all-features`. A
  // narrower one (`-p ai-hist-cli`) resolves a different feature list for
  // `ai-hist`, which changes the fingerprint and rebuilds the dependency tree
  // instead of reusing what the preceding step just built. For the same reason
  // nothing here overrides `CARGO_INCREMENTAL`.
  args.push(
    "test", "--workspace", "--all-features", "--test", "sync_bench", "--no-run",
    "--message-format", "json-render-diagnostics",
  );
  if (context.cargoProfile === "release") args.push("--release");
  const result = spawnSync("cargo", args, {
    cwd: repositoryRoot,
    encoding: "utf8",
    maxBuffer: 64 * 1024 * 1024,
  });
  if (result.error) throw new Error(`cargo could not be started: ${result.error.message}`);
  if (result.status !== 0) {
    throw new Error(`building the benchmark harness failed:\n${result.stdout}\n${result.stderr}`);
  }
  const executable = findHarnessExecutable(result.stdout);
  if (!executable) {
    throw new Error(`cargo built no sync_bench executable:\n${result.stdout}\n${result.stderr}`);
  }
  return executable;
}

/** Pull the `sync_bench` test executable out of cargo's JSON message stream. */
export function findHarnessExecutable(stdout) {
  let found = null;
  for (const line of stdout.split("\n")) {
    if (!line.startsWith("{")) continue;
    let message;
    try {
      message = JSON.parse(line);
    } catch {
      continue;
    }
    if (message.reason !== "compiler-artifact") continue;
    if (message.target?.name !== "sync_bench") continue;
    if (typeof message.executable === "string") found = message.executable;
  }
  return found;
}

/**
 * Run one phase in its own process.
 *
 * `--exact benchmark_phase` keeps it to the single ignored test, which is what
 * makes `getrusage(RUSAGE_SELF)` a per-phase figure and makes the harness's
 * own `set_var("HOME", …)` safe.
 */
function runPhase(phase, context) {
  const reportPath = join(context.work, `${phase}.json`);
  const env = {
    ...process.env,
    AI_HIST_BENCH_PHASE: phase,
    AI_HIST_BENCH_HOME: context.storeRoot,
    AI_HIST_BENCH_DB: context.dbPath,
    AI_HIST_BENCH_REPORT: reportPath,
    AI_HIST_BENCH_APPEND: context.manifest.incrementalTarget?.path ?? "",
    AI_HIST_BENCH_SESSION: context.manifest.hydrationTarget
      ? `${context.manifest.hydrationTarget.source}:${context.manifest.hydrationTarget.sessionId}`
      : "",
  };
  // The harness resolves these into paths. An empty one would surface as a
  // confusing failure inside the timed region, so it stops here instead.
  for (const [variable, needed] of [
    ["AI_HIST_BENCH_APPEND", phase === "incremental_sync"],
    ["AI_HIST_BENCH_SESSION", phase.startsWith("hydrate_")],
  ]) {
    if (needed && !env[variable]) {
      throw new Error(
        `phase ${phase} needs ${variable}, and the generated store produced none. `
        + "Check the manifest's target for this phase.",
      );
    }
  }
  const started = Date.now();
  const result = spawnSync(
    context.harness,
    ["--ignored", "--exact", "--nocapture", "benchmark_phase"],
    { cwd: repositoryRoot, env, encoding: "utf8" },
  );
  const wallMs = Date.now() - started;
  if (result.error) throw new Error(`the harness could not be started: ${result.error.message}`);
  if (result.status !== 0) {
    throw new Error(
      `phase ${phase} failed (exit ${result.status})\n${result.stdout ?? ""}\n${result.stderr ?? ""}`,
    );
  }
  if (!existsSync(reportPath)) {
    throw new Error(`phase ${phase} produced no report at ${reportPath}\n${result.stdout ?? ""}`);
  }
  return { ...JSON.parse(readFileSync(reportPath, "utf8")), harnessWallMs: wallMs };
}

/** Turn `unsupportedPhases` findings into one actionable failure. */
function refuseUnmeasurable(problems) {
  if (problems.length === 0) return;
  throw new Error(
    `this run cannot measure ${problems.length} of the requested phases:\n`
    + problems.map(({ phase, reason }) => `  - ${phase}: ${reason}`).join("\n")
    + `\nRun the phases in their documented order (${PHASE_ORDER.join(", ")}), `
    + "add the missing source to --sources, or drop the phase from --phases.",
  );
}

function loadThresholds() {
  return JSON.parse(readFileSync(THRESHOLDS, "utf8"));
}

/** Resolve the store shape: the profile's, unless the command line overrides it. */
export function resolveStore(profile, argv) {
  const stored = profile?.store ?? {};
  const number = (name, fallback) => {
    const raw = option(argv, name, undefined);
    if (raw === undefined) return fallback;
    const value = Number(raw);
    if (!Number.isSafeInteger(value) || value < 0) throw new Error(`--${name} must be an integer`);
    return value;
  };
  const sources = option(argv, "sources", undefined);
  return planStore({
    seed: number("seed", stored.seed ?? 176),
    sources: sources ? sources.split(",").filter(Boolean) : (stored.sources ?? [...FILE_SOURCES]),
    targetBytes: number("target-bytes", stored.targetBytes ?? 6 * 1024 * 1024),
    largeSessionBytes: number("large-session-bytes", stored.largeSessionBytes ?? 1024 * 1024),
    turns: number("turns", stored.turns ?? 12),
    toolResults: number("tool-results", stored.toolResults ?? 2),
    toolResultBytes: number("tool-result-bytes", stored.toolResultBytes ?? 1024),
  });
}

function renderReport(report) {
  const lines = [
    `Commit: ${report.commit ?? "unknown"}`,
    `Machine: ${report.machine.cpu} (${report.machine.cores} cores, ${formatBytes(report.machine.memoryBytes)}), ${report.machine.platform}/${report.machine.arch}`,
    `Toolchain: ${report.machine.rustc ?? "unknown"} (${report.cargoProfile} profile), Node ${report.machine.node}`,
    `Store: ${formatBytes(report.store.storeBytes)} across ${report.store.storeFiles} files / ${report.store.sessionCount} sessions (seed ${report.store.plan.seed})`,
    `Calibration: ${report.calibrationMs?.toFixed(1) ?? "—"} ms`,
    "",
    renderMarkdownTable(report),
  ];
  return `${lines.join("\n")}\n`;
}

async function main(argv) {
  // Re-render a report that was already measured. Used by the dispatch
  // workflow to put the table in its job summary without measuring twice.
  const reportOnly = option(argv, "report-only", undefined);
  if (reportOnly) {
    process.stdout.write(renderReport(JSON.parse(readFileSync(resolve(reportOnly), "utf8"))));
    return;
  }
  const thresholds = loadThresholds();
  const profileName = option(argv, "profile", "ci-debug");
  const profile = thresholds.profiles?.[profileName];
  if (!profile) {
    throw new Error(
      `unknown profile "${profileName}"; known: ${Object.keys(thresholds.profiles ?? {}).join(", ")}`,
    );
  }
  const plan = resolveStore(profile, argv);
  const work = resolve(option(argv, "work", join(tmpdir(), `relayhistory-sync-bench-${process.pid}`)));
  const keep = flag(argv, "keep");
  const phases = (option(argv, "phases", profile.phasesOrder?.join(",") ?? PHASE_ORDER.join(",")))
    .split(",")
    .filter(Boolean);
  for (const phase of phases) {
    if (!PHASE_ORDER.includes(phase)) throw new Error(`unknown phase ${phase}`);
  }
  // Refuse what the request alone already rules out, before generating a store
  // for it: a phase whose setup the order cannot provide, a phase listed twice,
  // a provider the plan never asked for.
  refuseUnmeasurable(unsupportedPhases(phases, { plan }));
  const repeat = Number(option(argv, "repeat", profile.repeat ?? 1));
  if (!Number.isSafeInteger(repeat) || repeat < 1) throw new Error("--repeat must be >= 1");
  const context = {
    work,
    storeRoot: join(work, "home"),
    dbPath: join(work, "ai-history.db"),
    cargoProfile: option(argv, "cargo-profile", profile.cargoProfile ?? "debug"),
    toolchain: option(argv, "toolchain", process.env.BENCH_CARGO_TOOLCHAIN ?? ""),
  };
  rmSync(work, { recursive: true, force: true });
  mkdirSync(work, { recursive: true });

  // Generate before building. The store costs well under a second and the
  // harness can cost minutes, so the check below — the one that reads what
  // actually landed — should not be paid for with a compile first.
  const manifest = await generateStore(plan, context.storeRoot);
  writeFileSync(join(work, "manifest.json"), `${JSON.stringify(manifest, null, 2)}\n`, "utf8");
  context.manifest = manifest;
  // A source named in `--sources` is not a promise that a session of it was
  // written: when the oversized session alone meets the byte target the
  // round-robin loop never runs. This reading, not the plan's, decides.
  refuseUnmeasurable(unsupportedPhases(phases, { manifest }));

  context.harness = buildHarness(context);

  // Setup is done; from here on the clock covers measurement.
  const started = Date.now();
  const samples = [];
  try {
    for (let round = 0; round < repeat; round += 1) {
      // Each round needs a database that does not exist yet, because
      // `cold_sync` is only cold once. The store itself is regenerated too so
      // a round never inherits the previous round's appended record.
      if (round > 0) await generateStore(plan, context.storeRoot);
      rmSync(context.dbPath, { force: true });
      rmSync(`${context.dbPath}-wal`, { force: true });
      rmSync(`${context.dbPath}-shm`, { force: true });
      rmSync(join(work, ".sync-state.json"), { force: true });
      for (const phase of [CALIBRATION_PHASE, ...phases]) {
        samples.push({ round, ...runPhase(phase, context) });
      }
    }
  } finally {
    if (!keep) rmSync(work, { recursive: true, force: true });
  }
  // Report the fastest round of each phase. Interference on a shared runner
  // only ever makes a phase slower, so the best sample is the one that
  // describes the code rather than the neighbours; taking the whole row from
  // one round keeps records, bytes and RSS internally consistent.
  const fastest = (phase) => samples
    .filter((sample) => sample.phase === phase)
    .reduce((best, sample) => (sample.elapsedMs < best.elapsedMs ? sample : best));
  const measured = phases.map(fastest);
  const calibration = fastest(CALIBRATION_PHASE);

  const report = {
    generatedAt: new Date().toISOString(),
    commit: git("rev-parse", "HEAD"),
    dirty: git("status", "--porcelain") !== "",
    profile: profileName,
    cargoProfile: context.cargoProfile,
    machine: machine(),
    store: {
      plan,
      storeBytes: manifest.storeBytes,
      storeFiles: manifest.storeFiles,
      sessionCount: manifest.sessionCount,
      hydrationTargetBytes: manifest.hydrationTarget?.bytes ?? null,
    },
    totalWallMs: Date.now() - started,
    repeat,
    calibrationMs: calibration.elapsedMs,
    phases: measured,
    samples,
  };

  if (flag(argv, "update-baselines")) {
    const next = structuredClone(thresholds);
    next.profiles[profileName].calibrationMs = Number(report.calibrationMs.toFixed(1));
    next.profiles[profileName].phases = baselinesFromReport(report, BASELINE_METRICS);
    next.profiles[profileName].measuredOn = {
      commit: report.commit,
      machine: `${report.machine.cpu} (${report.machine.cores} cores), ${report.machine.platform}/${report.machine.arch}`,
      rustc: report.machine.rustc,
      cargoProfile: report.cargoProfile,
      at: report.generatedAt,
    };
    writeFileSync(THRESHOLDS, `${JSON.stringify(next, null, 2)}\n`, "utf8");
    process.stdout.write(`baselines for profile "${profileName}" written to ${THRESHOLDS}\n`);
  }

  const output = option(argv, "output", undefined);
  if (output) {
    const path = resolve(output);
    const rendered = extname(path).toLowerCase() === ".md"
      ? renderReport(report)
      : `${JSON.stringify(report, null, 2)}\n`;
    writeFileSync(path, rendered, "utf8");
    process.stdout.write(`benchmark report written to ${path}\n`);
  } else {
    // Printed in gate mode too: a red gate is far easier to read next to the
    // numbers that produced it than as a bare threshold violation.
    process.stdout.write(renderReport(report));
    process.stdout.write("\n");
  }

  if (!flag(argv, "gate")) return;

  const verdict = evaluateGate(report, thresholds, profileName);
  const seconds = (report.totalWallMs / 1000).toFixed(1);
  if (verdict.calibration) {
    const { measuredMs, baselineMs, raw, factor, clamped } = verdict.calibration;
    process.stdout.write(
      `calibration: ${measuredMs.toFixed(1)} ms here vs ${baselineMs.toFixed(1)} ms ` +
      `on the baseline machine — this run is ${raw.toFixed(2)}x its speed` +
      `${clamped ? `, clamped to ${factor.toFixed(2)}x` : ""}. ` +
      "Throughput and elapsed checks below are normalized by that.\n\n",
    );
  }
  for (const check of verdict.checks) {
    const shown = check.normalized === check.value
      ? check.value.toFixed(check.metric === "peakRssBytes" ? 0 : 1)
      : `${check.value.toFixed(1)} -> ${check.normalized.toFixed(1)}`;
    process.stdout.write(
      `${check.ok ? "ok  " : "FAIL"} ${check.phase}.${check.metric}: ${shown} ` +
      `(baseline ${check.baseline}, bound ${check.bound.toFixed(0)})\n`,
    );
  }
  process.stdout.write(
    `\n${verdict.checks.length} checks over ${report.phases.length} phases in ${seconds}s ` +
    `(profile "${profileName}", ${report.cargoProfile} build)\n`,
  );
  if (!verdict.ok) {
    process.stderr.write(`\nsync/hydration throughput gate FAILED:\n`);
    for (const failure of verdict.failures) process.stderr.write(`  - ${failure}\n`);
    process.stderr.write(
      "\nIf this is an intended cost, re-measure with " +
      `\`node scripts/benchmark-sync.mjs --profile ${profileName} --update-baselines\` ` +
      "and say why in the pull request.\n",
    );
    process.exitCode = 1;
    return;
  }
  process.stdout.write("sync/hydration throughput gate passed\n");
}

if (process.argv[1] && resolve(process.argv[1]) === resolve(fileURLToPath(import.meta.url))) {
  main(process.argv.slice(2)).catch((error) => {
    console.error(error instanceof Error ? error.message : error);
    process.exitCode = 1;
  });
}

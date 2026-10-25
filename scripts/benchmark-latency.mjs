// Session-visible latency benchmark, and the 5 ms CI gate.
//
//   node scripts/benchmark-latency.mjs --gate                    # what CI runs
//   node scripts/benchmark-latency.mjs --profile full --output latency.json --markdown latency.md
//   node scripts/benchmark-latency.mjs --report-only latency.json  # re-render / re-gate a saved run
//   node scripts/benchmark-latency.mjs --gate --update-baselines   # on the baseline runner class
//
// This script owns orchestration only: it builds the release harness
// (`crates/ai-hist-cli/tests/latency_bench.rs`) once, runs each run of the
// profile in `scripts/latency-thresholds.json` as its own process for each
// round, folds the rounds together, and applies the budgets and baselines.
// See docs/benchmarks.md, "Session-visible latency".

import { spawnSync } from "node:child_process";
import { mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { arch, cpus, platform, totalmem } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
  aggregateRounds,
  baselinesFromReport,
  evaluateGate,
  mergeBaselines,
  platformClass,
  renderMarkdown,
  runId,
} from "./benchmark-latency-lib.mjs";

const here = dirname(fileURLToPath(import.meta.url));
const repositoryRoot = resolve(here, "..");
const THRESHOLDS = join(here, "latency-thresholds.json");

function option(argv, name, fallback) {
  const prefix = `--${name}=`;
  const inline = argv.find((argument) => argument.startsWith(prefix));
  if (inline) return inline.slice(prefix.length);
  const index = argv.indexOf(`--${name}`);
  if (index >= 0 && argv[index + 1] && !argv[index + 1].startsWith("--")) return argv[index + 1];
  return fallback;
}

const flag = (argv, name) => argv.includes(`--${name}`);

function machine() {
  return {
    platform: platform(),
    arch: arch(),
    cpu: cpus()[0]?.model ?? "unknown",
    cores: cpus().length,
    memoryBytes: totalmem(),
    node: process.version,
  };
}

/** Pull the `latency_bench` test executable out of cargo's JSON messages. */
export function findLatencyExecutable(stdout) {
  let found = null;
  for (const line of stdout.split("\n")) {
    if (!line.startsWith("{")) continue;
    let message;
    try {
      message = JSON.parse(line);
    } catch {
      continue;
    }
    if (message.reason !== "compiler-artifact" || message.target?.name !== "latency_bench") continue;
    if (typeof message.executable === "string") found = message.executable;
  }
  return found;
}

/**
 * Release only: the budgets are release-build numbers on the machine that
 * will run the desktop, and an unoptimized parser is several times slower.
 */
function buildHarness() {
  const result = spawnSync(
    "cargo",
    ["test", "--release", "-p", "ai-hist-cli", "--test", "latency_bench", "--no-run",
      "--message-format", "json-render-diagnostics"],
    { cwd: repositoryRoot, encoding: "utf8", maxBuffer: 64 * 1024 * 1024 },
  );
  if (result.error) throw new Error(`cargo could not be started: ${result.error.message}`);
  if (result.status !== 0) {
    throw new Error(`building the latency harness failed:\n${result.stdout}\n${result.stderr}`);
  }
  const executable = findLatencyExecutable(result.stdout);
  if (!executable) throw new Error(`cargo built no latency_bench executable:\n${result.stderr}`);
  return executable;
}

function runOnce(executable, run, profile, workDir) {
  const reportPath = join(workDir, `report-${runId(run)}.json`);
  rmSync(reportPath, { force: true });
  const env = {
    ...process.env,
    AI_HIST_LATENCY_PHASE: run.phase,
    AI_HIST_LATENCY_DAYS: String(run.days),
    AI_HIST_LATENCY_SESSIONS_PER_DAY: String(run.sessionsPerDay ?? profile.sessionsPerDay),
    AI_HIST_LATENCY_TURNS: String(profile.turns),
    AI_HIST_LATENCY_SAMPLES: String(run.samples ?? profile.samples),
    AI_HIST_LATENCY_SWEEP_SAMPLES: String(profile.sweepSamples ?? 20),
    AI_HIST_LATENCY_RETAIN_DAYS: String(profile.retainDays),
    AI_HIST_LATENCY_DIR: workDir,
    AI_HIST_LATENCY_REPORT: reportPath,
  };
  const result = spawnSync(executable, ["--ignored", "--exact", "latency_phase", "--nocapture"], {
    cwd: repositoryRoot,
    env,
    encoding: "utf8",
    maxBuffer: 64 * 1024 * 1024,
  });
  if (result.status !== 0) {
    throw new Error(`${runId(run)} failed (exit ${result.status})\n${result.stdout}\n${result.stderr}`);
  }
  return JSON.parse(readFileSync(reportPath, "utf8"));
}

function measure(profileName, thresholds, rounds) {
  const profile = thresholds.profiles[profileName];
  if (!profile) throw new Error(`unknown profile ${profileName}`);
  const executable = buildHarness();
  // Under target/, on the disk the build used: never the system temp dir,
  // which is tmpfs on some Linux hosts and would make every fsync free.
  const workDir = resolve(process.env.AI_HIST_LATENCY_DIR ?? join(repositoryRoot, "target/latency-bench"));
  rmSync(workDir, { recursive: true, force: true });
  mkdirSync(workDir, { recursive: true });
  const runs = [];
  for (const run of profile.runs) {
    const results = [];
    for (let round = 0; round < rounds; round++) {
      const started = Date.now();
      results.push(runOnce(executable, run, profile, workDir));
      process.stderr.write(`${runId(run)} round ${round + 1}/${rounds}: ${Date.now() - started} ms\n`);
    }
    runs.push({ ...run, result: aggregateRounds(results) });
  }
  rmSync(workDir, { recursive: true, force: true });
  return {
    profile: profileName,
    rounds,
    release: true,
    machine: machine(),
    commit: spawnSync("git", ["-C", repositoryRoot, "rev-parse", "HEAD"], { encoding: "utf8" }).stdout?.trim(),
    measuredAt: new Date().toISOString(),
    runs,
  };
}

async function main(argv) {
  const thresholds = JSON.parse(readFileSync(THRESHOLDS, "utf8"));
  const profileName = option(argv, "profile", flag(argv, "gate") ? "gate" : "full");
  const reportOnly = option(argv, "report-only", null);
  const rounds = Number(option(argv, "rounds", thresholds.profiles[profileName]?.rounds ?? 1));
  const report = reportOnly
    ? JSON.parse(readFileSync(reportOnly, "utf8"))
    : measure(profileName, thresholds, rounds);
  const output = option(argv, "output", null);
  if (output) writeFileSync(output, `${JSON.stringify(report, null, 2)}\n`);

  const cls = platformClass(report.machine.platform, report.machine.arch);
  if (flag(argv, "update-baselines")) {
    const previous = thresholds.baselines?.[cls];
    thresholds.baselines ??= {};
    thresholds.baselines[cls] = {
      measuredOn: {
        cpusSeen: [...new Set([...(previous?.measuredOn?.cpusSeen ?? []), report.machine.cpu])],
        commit: report.commit,
        measuredAt: report.measuredAt,
        profile: report.profile,
      },
      metrics: mergeBaselines(previous?.metrics, baselinesFromReport(report, thresholds.budgets)),
    };
    writeFileSync(THRESHOLDS, `${JSON.stringify(thresholds, null, 2)}\n`);
    process.stderr.write(`baselines for ${cls} written to ${THRESHOLDS}\n`);
  }

  const gate = evaluateGate(report, thresholds, cls);
  const markdown = renderMarkdown(report, gate, thresholds);
  const markdownPath = option(argv, "markdown", null);
  if (markdownPath) writeFileSync(markdownPath, markdown);
  process.stdout.write(markdown);
  if (flag(argv, "gate") && !gate.passed) {
    process.stderr.write("\nsession-visible latency gate FAILED:\n");
    for (const check of gate.checks.filter((c) => c.status === "fail")) {
      process.stderr.write(`  - ${check.id}: p99 ${check.value} ms over ${check.rule} bound ${check.bound} ms\n`);
    }
    process.stderr.write(
      "\nIf this is an intended cost, re-measure with `--gate --update-baselines` on the " +
        "runner class and say why in the pull request; a budget is a product limit, not a knob.\n",
    );
    process.exitCode = 1;
  }
}

if (process.argv[1] && resolve(process.argv[1]) === resolve(fileURLToPath(import.meta.url))) {
  main(process.argv.slice(2)).catch((error) => {
    console.error(error instanceof Error ? error.message : error);
    process.exitCode = 1;
  });
}

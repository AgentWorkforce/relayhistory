// Base-versus-head benchmark: the pull-request gate for small steady-state
// regressions.
//
//   node scripts/benchmark-ab.mjs --base ../base --head . [--rounds 2] [--output ab.json]
//
// `benchmark-sync.mjs --gate` compares one run against stored absolute
// baselines with 2x margins, which is what a shared runner's noise allows. It
// cannot see a 5% regression. This script measures the base commit and the
// head commit on the same runner, against the same generated store, and
// compares them with each other instead.
//
// Each phase runs under `valgrind --tool=cachegrind --cache-sim=no`, and the
// metric is instructions executed (`Ir`). An instruction count does not move
// with the neighbours' load, so a 2% bound is meaningful on a shared runner
// where a wall clock is not. Without valgrind (`--metric cpu`, the default off
// Linux) the harness's user+system CPU time is used instead and the rounds are
// interleaved base/head so drift lands on both sides.
//
// Each side runs its own `sync_bench` harness, built from its own checkout:
// the base harness measures the base code. The store is generated once, by the
// head's generator, and copied fresh for every side and round.

import { spawnSync } from "node:child_process";
import { cpSync, existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { generateStore } from "./gen-synthetic-history.mjs";
import { planStore } from "./benchmark-sync-lib.mjs";

/** Phases in run order; each leans on the state the previous one left. */
export const AB_PHASES = ["cold_sync", "incremental_sync", "unchanged_sync", "hydrate_cold", "hydrate_unchanged"];

/**
 * The bound on head/base per phase. Steady-state phases -- the ones every
 * sync tick and every repeat read pays -- may not get slower. Cold phases may
 * grow with evidence a change newly captures, so they are reported, not gated.
 */
export const AB_BOUNDS = {
  incremental_sync: 1.02,
  unchanged_sync: 1.02,
  hydrate_unchanged: 1.02,
};

/** The store: small enough for cachegrind, every provider the harness reads. */
export const AB_STORE = {
  seed: 176,
  sources: ["claude", "codex", "cursor", "grok", "opencode"],
  targetBytes: 2 * 1024 * 1024,
  largeSessionBytes: 512 * 1024,
  turns: 12,
  toolResults: 2,
  toolResultBytes: 1024,
};

function option(argv, name, fallback) {
  const index = argv.indexOf(`--${name}`);
  if (index >= 0 && argv[index + 1] && !argv[index + 1].startsWith("--")) return argv[index + 1];
  const inline = argv.find((a) => a.startsWith(`--${name}=`));
  return inline ? inline.slice(name.length + 3) : fallback;
}

/** Build one checkout's harness and return its test executable. */
export function buildHarness(checkout) {
  const result = spawnSync(
    "cargo",
    ["test", "--workspace", "--all-features", "--release", "--test", "sync_bench", "--no-run",
      "--message-format", "json-render-diagnostics"],
    { cwd: checkout, encoding: "utf8", maxBuffer: 64 * 1024 * 1024 },
  );
  if (result.status !== 0) throw new Error(`building ${checkout} failed:\n${result.stderr}`);
  let executable = null;
  for (const line of result.stdout.split("\n")) {
    if (!line.startsWith("{")) continue;
    try {
      const message = JSON.parse(line);
      if (message.reason === "compiler-artifact" && message.target?.name === "sync_bench" && message.executable) {
        executable = message.executable;
      }
    } catch {
      // cargo interleaves non-JSON lines; skip them.
    }
  }
  if (!executable) throw new Error(`${checkout} built no sync_bench executable`);
  return executable;
}

/** Instructions executed, from a cachegrind output file's `summary:` line. */
export function cachegrindInstructions(text) {
  const events = text.match(/^events:\s+(.+)$/m)?.[1]?.trim().split(/\s+/);
  const summary = text.match(/^summary:\s+(.+)$/m)?.[1]?.trim().split(/\s+/);
  if (!events || !summary) return null;
  const index = events.indexOf("Ir");
  return index >= 0 ? Number(summary[index]) : null;
}

function runPhase(side, phase, context) {
  const report = join(context.work, `${side.name}-${phase}.json`);
  rmSync(report, { force: true });
  const env = {
    ...process.env,
    AI_HIST_BENCH_PHASE: phase,
    AI_HIST_BENCH_HOME: side.home,
    AI_HIST_BENCH_DB: side.db,
    AI_HIST_BENCH_REPORT: report,
    AI_HIST_BENCH_APPEND: side.append,
    AI_HIST_BENCH_SESSION: context.session,
  };
  const args = ["--ignored", "--exact", "--nocapture", "benchmark_phase"];
  let command = side.harness;
  let commandArgs = args;
  const cg = join(context.work, `${side.name}-${phase}.cachegrind`);
  if (context.metric === "instructions") {
    command = "valgrind";
    commandArgs = ["--tool=cachegrind", "--cache-sim=no", `--cachegrind-out-file=${cg}`, side.harness, ...args];
  }
  const result = spawnSync(command, commandArgs, { env, encoding: "utf8", maxBuffer: 64 * 1024 * 1024 });
  if (result.status !== 0 || !existsSync(report)) {
    throw new Error(`${side.name} ${phase} failed (exit ${result.status})\n${result.stdout}\n${result.stderr}`);
  }
  const measured = JSON.parse(readFileSync(report, "utf8"));
  const instructions = context.metric === "instructions"
    ? cachegrindInstructions(readFileSync(cg, "utf8"))
    : null;
  return { instructions, cpuMs: measured.cpuMs ?? null, elapsedMs: measured.elapsedMs, dbBytes: measured.dbBytes };
}

const median = (values) => {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.floor(sorted.length / 2)];
};

/**
 * The number a sample is compared on: instructions when measured, else CPU
 * time when both sides' harness reports it (one that predates `cpuMs` does
 * not), else wall time.
 */
export function metricOf(samples) {
  if (samples.every((s) => typeof s.instructions === "number")) return "instructions";
  if (samples.every((s) => typeof s.cpuMs === "number")) return "cpuMs";
  return "elapsedMs";
}

/** Compare per-phase medians; a ratio above its bound is a failure. */
export function compare(samples, bounds = AB_BOUNDS) {
  const metric = metricOf(samples);
  samples = samples.map((s) => ({ ...s, value: s[metric] }));
  const rows = [];
  for (const phase of AB_PHASES) {
    const base = samples.filter((s) => s.side === "base" && s.phase === phase);
    const head = samples.filter((s) => s.side === "head" && s.phase === phase);
    if (base.length === 0 || head.length === 0) continue;
    const baseValue = median(base.map((s) => s.value));
    const headValue = median(head.map((s) => s.value));
    const ratio = headValue / baseValue;
    const bound = bounds[phase];
    rows.push({
      phase, base: baseValue, head: headValue, ratio, bound: bound ?? null,
      baseDbBytes: median(base.map((s) => s.dbBytes)), headDbBytes: median(head.map((s) => s.dbBytes)),
      ok: bound === undefined || ratio <= bound,
    });
  }
  return { metric, rows, ok: rows.every((row) => row.ok) };
}

export function renderComparison({ metric, rows, ok }) {
  const unit = { instructions: "instructions", cpuMs: "CPU ms", elapsedMs: "wall ms" }[metric];
  const shown = (value) => (metric === "instructions"
    ? Math.round(value).toLocaleString("en-US")
    : value.toFixed(1));
  const lines = [
    `| phase | base (${unit}) | head | head/base | bound | DB bytes base -> head | |`,
    "|---|---:|---:|---:|---:|---|---|",
    ...rows.map((row) => `| ${row.phase} | ${shown(row.base)} | ${shown(row.head)} | `
      + `${((row.ratio - 1) * 100).toFixed(2)}% | ${row.bound ? `${((row.bound - 1) * 100).toFixed(0)}%` : "reported"} | `
      + `${row.baseDbBytes.toLocaleString("en-US")} -> ${row.headDbBytes.toLocaleString("en-US")} | ${row.ok ? "" : "**FAIL**"}`),
    "",
    ok ? "base/head benchmark passed" : "base/head benchmark FAILED: a steady-state phase got slower than its bound",
  ];
  return `${lines.join("\n")}\n`;
}

async function main(argv) {
  const base = resolve(option(argv, "base", "../base"));
  const head = resolve(option(argv, "head", "."));
  const rounds = Number(option(argv, "rounds", "2"));
  const metric = option(argv, "metric", process.platform === "linux" ? "instructions" : "cpu");
  const work = resolve(option(argv, "work", join(tmpdir(), `relayhistory-ab-${process.pid}`)));
  rmSync(work, { recursive: true, force: true });
  mkdirSync(work, { recursive: true });
  const pristine = join(work, "pristine");
  const manifest = await generateStore(planStore(AB_STORE), pristine);
  const session = `${manifest.hydrationTarget.source}:${manifest.hydrationTarget.sessionId}`;
  const harnesses = { base: buildHarness(base), head: buildHarness(head) };
  const samples = [];
  for (let round = 0; round < rounds; round += 1) {
    const order = round % 2 === 0 ? ["base", "head"] : ["head", "base"];
    for (const name of order) {
      const root = join(work, name);
      rmSync(root, { recursive: true, force: true });
      mkdirSync(root, { recursive: true });
      cpSync(pristine, join(root, "home"), { recursive: true });
      const side = {
        name, harness: harnesses[name], home: join(root, "home"), db: join(root, "ai-history.db"),
        append: manifest.incrementalTarget.path.replace(pristine, join(root, "home")),
      };
      for (const phase of AB_PHASES) {
        samples.push({ side: name, phase, round, ...runPhase(side, phase, { work, session, metric }) });
      }
    }
  }
  const verdict = compare(samples);
  const rendered = renderComparison(verdict);
  process.stdout.write(rendered);
  const output = option(argv, "output", undefined);
  if (output) writeFileSync(resolve(output), `${JSON.stringify({ samples, ...verdict }, null, 2)}\n`);
  if (process.env.GITHUB_STEP_SUMMARY) writeFileSync(process.env.GITHUB_STEP_SUMMARY, rendered, { flag: "a" });
  if (!argv.includes("--keep")) rmSync(work, { recursive: true, force: true });
  if (!verdict.ok) process.exitCode = 1;
}

if (process.argv[1] && resolve(process.argv[1]) === resolve(new URL(import.meta.url).pathname)) {
  main(process.argv.slice(2)).catch((error) => {
    console.error(error instanceof Error ? error.message : error);
    process.exitCode = 1;
  });
}

// Session-visible latency benchmark, and the 5 ms CI gate.
//
//   node scripts/benchmark-latency.mjs --gate --base origin/main  # what CI runs on a PR
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
import { chmodSync, copyFileSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
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
 * `targetDir` is shared between the head and base builds so the base build
 * reuses every compiled dependency and rebuilds only the workspace crates.
 */
function buildHarness(sourceDir, targetDir, label) {
  const result = spawnSync(
    "cargo",
    ["test", "--release", "-p", "ai-hist-cli", "--test", "latency_bench", "--no-run",
      "--message-format", "json-render-diagnostics"],
    {
      cwd: sourceDir,
      env: { ...process.env, CARGO_TARGET_DIR: targetDir },
      encoding: "utf8",
      maxBuffer: 64 * 1024 * 1024,
    },
  );
  if (result.error) throw new Error(`cargo could not be started: ${result.error.message}`);
  if (result.status !== 0) {
    throw new Error(`building the latency harness in ${sourceDir} failed:\n${result.stdout}\n${result.stderr}`);
  }
  const executable = findLatencyExecutable(result.stdout);
  if (!executable) throw new Error(`cargo built no latency_bench executable:\n${result.stderr}`);
  // Copied out at once: cargo hashes a workspace member by its path relative
  // to the workspace root, so the base worktree's build writes the very same
  // file name into the shared target dir and would replace the head's binary.
  const kept = join(targetDir, "latency-bin", label);
  mkdirSync(dirname(kept), { recursive: true });
  copyFileSync(executable, kept);
  chmodSync(kept, 0o755);
  return kept;
}

const HARNESS = "crates/ai-hist-cli/tests/latency_bench.rs";
const CLI_MANIFEST = "crates/ai-hist-cli/Cargo.toml";
const NOTIFY_DEV_DEP =
  'notify = { version = "8", default-features = false, features = ["macos_fsevent"] }';

/**
 * Give a base manifest the harness's one extra dev-dependency, unless it
 * already has it. Exported for the test; the base is a throwaway worktree.
 */
export function withNotifyDevDependency(manifest) {
  const section = manifest.indexOf("[dev-dependencies]");
  const next = section < 0 ? -1 : manifest.indexOf("\n[", section + 1);
  const devDeps = section < 0 ? "" : manifest.slice(section, next < 0 ? undefined : next);
  if (/^notify\s*=/m.test(devDeps)) return manifest;
  if (section < 0) return `${manifest.trimEnd()}\n\n[dev-dependencies]\n${NOTIFY_DEV_DEP}\n`;
  const at = section + "[dev-dependencies]".length;
  return `${manifest.slice(0, at)}\n${NOTIFY_DEV_DEP}${manifest.slice(at)}`;
}

/**
 * A detached worktree of `ref` carrying this checkout's harness. The harness
 * uses only the crate's public API, so it measures the base's library with
 * the head's measurement code: a base that predates the harness can still be
 * compared, and a change to the harness itself moves both sides at once.
 */
function prepareBase(ref) {
  const dir = join(repositoryRoot, "target/latency-base");
  spawnSync("git", ["-C", repositoryRoot, "worktree", "remove", "--force", dir], { encoding: "utf8" });
  rmSync(dir, { recursive: true, force: true });
  const added = spawnSync("git", ["-C", repositoryRoot, "worktree", "add", "--detach", dir, ref], {
    encoding: "utf8",
  });
  if (added.status !== 0) throw new Error(`git worktree add ${ref} failed:\n${added.stderr}`);
  copyFileSync(join(repositoryRoot, HARNESS), join(dir, HARNESS));
  const manifest = join(dir, CLI_MANIFEST);
  writeFileSync(manifest, withNotifyDevDependency(readFileSync(manifest, "utf8")));
  return dir;
}

function removeBase(dir) {
  spawnSync("git", ["-C", repositoryRoot, "worktree", "remove", "--force", dir], { encoding: "utf8" });
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
    AI_HIST_LATENCY_FS_SAMPLES: String(profile.fsSamples ?? 1000),
    AI_HIST_LATENCY_RETAIN_DAYS: String(profile.retainDays),
    AI_HIST_LATENCY_PRUNE_WINDOW_MS: String(profile.pruneWindowMs ?? 10000),
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

function measure(profileName, thresholds, rounds, baseRef) {
  const profile = thresholds.profiles[profileName];
  if (!profile) throw new Error(`unknown profile ${profileName}`);
  const targetDir = resolve(process.env.CARGO_TARGET_DIR ?? join(repositoryRoot, "target"));
  const head = buildHarness(repositoryRoot, targetDir, "head");
  const baseDir = baseRef ? prepareBase(baseRef) : null;
  const base = baseDir ? buildHarness(baseDir, targetDir, "base") : null;
  // Under target/, on the disk the build used: never the system temp dir,
  // which is tmpfs on some Linux hosts and would make every fsync free. Head
  // and base keep separate stores: a cached store built by one library is
  // not a store the other necessarily reads without migrating.
  // A directory of its own inside the configured one, so the cleanup below
  // never touches anything the caller keeps there.
  const root = join(
    resolve(process.env.AI_HIST_LATENCY_DIR ?? join(repositoryRoot, "target")),
    "latency-bench",
  );
  rmSync(root, { recursive: true, force: true });
  const headDir = join(root, "head");
  const baseWork = join(root, "base");
  mkdirSync(headDir, { recursive: true });
  if (base) mkdirSync(baseWork, { recursive: true });
  const runs = [];
  for (const run of profile.runs) {
    const results = [];
    const baseResults = [];
    for (let round = 0; round < rounds; round++) {
      // Interleaved, so a runner that slows down part-way slows both sides.
      if (base) baseResults.push(runOnce(base, run, profile, baseWork));
      const started = Date.now();
      results.push(runOnce(head, run, profile, headDir));
      process.stderr.write(`${runId(run)} round ${round + 1}/${rounds}: ${Date.now() - started} ms\n`);
    }
    runs.push({
      ...run,
      result: aggregateRounds(results),
      ...(base ? { base: aggregateRounds(baseResults) } : {}),
    });
  }
  rmSync(root, { recursive: true, force: true });
  if (baseDir) removeBase(baseDir);
  const rev = (ref) =>
    spawnSync("git", ["-C", repositoryRoot, "rev-parse", ref], { encoding: "utf8" }).stdout?.trim();
  return {
    profile: profileName,
    rounds,
    release: true,
    machine: machine(),
    commit: rev("HEAD"),
    baseCommit: baseRef ? rev(baseRef) : null,
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
    : measure(profileName, thresholds, rounds, option(argv, "base", null));
  const output = option(argv, "output", null);
  if (output) writeFileSync(output, `${JSON.stringify(report, null, 2)}\n`);

  const cls = platformClass(report.machine.platform, report.machine.arch);
  if (flag(argv, "update-baselines")) {
    const recorded = baselinesFromReport(report, thresholds.budgets, cls);
    const previous = thresholds.baselines?.[cls];
    thresholds.baselines ??= {};
    thresholds.baselines[cls] = {
      measuredOn: {
        cpusSeen: [...new Set([...(previous?.measuredOn?.cpusSeen ?? []), report.machine.cpu])],
        commit: report.commit,
        measuredAt: report.measuredAt,
        profile: report.profile,
      },
      metrics: mergeBaselines(previous?.metrics, recorded.baselines),
    };
    for (const [id, numbers] of Object.entries(recorded.known)) {
      const budget = thresholds.budgets[id];
      budget.known ??= {};
      budget.known[cls] = mergeBaselines({ [id]: budget.known[cls] }, { [id]: numbers })[id];
    }
    writeFileSync(THRESHOLDS, `${JSON.stringify(thresholds, null, 2)}\n`);
    process.stderr.write(`baselines and known numbers for ${cls} written to ${THRESHOLDS}\n`);
  }

  const gate = evaluateGate(report, thresholds, cls);
  const markdown = renderMarkdown(report, gate, thresholds);
  const markdownPath = option(argv, "markdown", null);
  if (markdownPath) writeFileSync(markdownPath, markdown);
  process.stdout.write(markdown);
  if (flag(argv, "gate") && !gate.passed) {
    process.stderr.write("\nsession-visible latency gate FAILED:\n");
    for (const check of gate.checks.filter((c) => c.status === "fail")) {
      process.stderr.write(`  - ${check.id}: ${check.value} ms over the ${check.rule} bound of ${check.bound} ms\n`);
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

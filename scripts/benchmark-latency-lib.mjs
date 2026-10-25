// Pure helpers for scripts/benchmark-latency.mjs: run naming, aggregation
// across rounds, the gate, and the Markdown table. No I/O, so
// benchmark-latency.test.mjs can exercise every rule directly.

/** `stages@30d` — a phase at one store size. */
export function runId(run) {
  return `${run.phase}@${run.days}d`;
}

/** `stages@30d.ingest_hook` — one metric of one run. */
export function metricId(run, metric) {
  return `${runId(run)}.${metric}`;
}

/** The baseline class a machine belongs to: `darwin-arm64`, `linux-x64`. */
export function platformClass(platform, arch) {
  return `${platform}-${arch}`;
}

function median(values) {
  const sorted = [...values].sort((a, b) => a - b);
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2;
}

/**
 * Fold the rounds of one run into one summary per metric.
 *
 * Each round is a separate process with its own nearest-rank percentiles; the
 * reported value of each percentile is the median across rounds, so one
 * noisy round (a runner neighbour, a page-cache miss) cannot move the gate on
 * its own. `worstP99` keeps the largest round so the table still shows it.
 */
export function aggregateRounds(rounds) {
  const metrics = {};
  for (const name of Object.keys(rounds[0]?.metrics ?? {})) {
    const series = rounds.map((round) => round.metrics[name]).filter((m) => m && m.n > 0);
    if (series.length === 0) {
      metrics[name] = { n: 0 };
      continue;
    }
    metrics[name] = {
      n: series.reduce((sum, m) => sum + m.n, 0),
      p50: median(series.map((m) => m.p50)),
      p90: median(series.map((m) => m.p90)),
      p99: median(series.map((m) => m.p99)),
      max: Math.max(...series.map((m) => m.max)),
      worstP50: Math.max(...series.map((m) => m.p50)),
      worstP99: Math.max(...series.map((m) => m.p99)),
      rounds: series.length,
    };
  }
  const { metrics: _ignored, ...rest } = rounds[0] ?? {};
  return { ...rest, metrics };
}

/**
 * Whether a budget fails the gate on this platform class. `enforced` is
 * `true` (everywhere), `false` (tracked everywhere), or the list of classes it
 * is enforced on: a stage can meet its budget on one platform and not yet on
 * another -- inotify does, FSEvents does not -- and holding the platform that
 * meets it is worth more than waiting for both.
 */
export function budgetEnforced(budget, platform) {
  return Array.isArray(budget.enforced) ? budget.enforced.includes(platform) : budget.enforced === true;
}

/**
 * The percentile a regression is judged on. An enforced stage is held to
 * its p99, the number the budget is about. A tracked stage -- one the code
 * does not meet yet -- is judged on its p50: its tail is lock waits and WAL
 * checkpoints that land on whichever samples they land on (the hook ingest's
 * p99 was 257 ms in one job and 31 ms in the next, p50 23-24 ms in both), so
 * a 10% bound on that tail measures luck, while the p50 is the cost the fix
 * for that stage will move.
 */
export function regressionPercentile(budget, platform) {
  return budgetEnforced(budget, platform) ? "p99" : "p50";
}

/**
 * The absolute allowance added to every regression bound: the policy floor,
 * or `regressionFloorBudgetShare` of the stage's own budget when that is
 * larger. A slowdown smaller than a tenth of what the stage may spend is not
 * one a pull request should fail on, and below that size a same-runner
 * comparison still sees the raw-event tail move by a few hundred
 * microseconds on a busy machine with no code change at all.
 */
export function regressionFloor(policy, budget) {
  return Math.max(policy.regressionFloorMs, (policy.regressionFloorBudgetShare ?? 0) * budget.p99Ms);
}

/**
 * Apply budgets and the regression rule to an aggregated report.
 *
 * Every rule produces `ok`, `warn` or `fail` per budgeted metric -- the
 * stages of the 5 ms path; everything else in the table is diagnostic:
 *
 * - **Budget.** p99 at or above the budget fails if the budget is enforced on
 *   this platform class (see `budgetEnforced`), and warns otherwise: a stage
 *   the code does not meet yet is tracked, so the gate can land before the
 *   fix does, and the fix flips it to enforced.
 * - **Regression against base.** When the report carries a base measurement
 *   (`--base`: the same harness over the base commit's library, interleaved
 *   round by round on the same runner), the head fails if its
 *   `regressionPercentile` exceeds the base's by more than `maxRegression`
 *   plus `regressionFloorMs`. This is the rule that decides: the first CI
 *   runs showed the same metric 20-40% apart on two runners of one class,
 *   so only a comparison made on one machine can see 10%.
 * - **Regression against stored baselines.** Advisory (`warn`) whenever a
 *   base comparison exists, and whenever this CPU is not one the baselines
 *   were recorded on; it fails only without a base, on a recorded CPU --
 *   the `main` push and a manual run, where there is nothing to compare with.
 *
 * Every bound adds `regressionFloor`: 10% of a 0.1 ms stage is 10 us, which
 * is scheduler jitter rather than code.
 */
export function evaluateGate(report, thresholds, platform) {
  const policy = thresholds.policy;
  const stored = thresholds.baselines?.[platform] ?? null;
  const cpuKnown = stored?.measuredOn?.cpusSeen?.includes(report.machine?.cpu) ?? false;
  const checks = [];
  const warnings = [];
  const anyBase = report.runs.some((run) => run.base);
  if (!anyBase && !stored) {
    warnings.push(
      `no base comparison and no stored baselines for ${platform}; only budgets are checked.`,
    );
  } else if (!anyBase && !cpuKnown) {
    warnings.push(
      `${report.machine?.cpu} is not one the ${platform} baselines were recorded on ` +
        `(${stored.measuredOn?.cpusSeen?.join(", ")}); the stored-baseline check is advisory.`,
    );
  }
  const bound = (value, budget) =>
    Math.round((value * (1 + policy.maxRegression) + regressionFloor(policy, budget)) * 1000) / 1000;
  for (const run of report.runs) {
    for (const [name, measured] of Object.entries(run.result.metrics)) {
      const id = metricId(run, name);
      const budget = thresholds.budgets?.[id];
      if (!budget || !measured || measured.n === 0) continue;
      checks.push({
        id,
        rule: "budget",
        value: measured.p99,
        bound: budget.p99Ms,
        status: measured.p99 < budget.p99Ms ? "ok" : budgetEnforced(budget, platform) ? "fail" : "warn",
        stage: budget.stage,
      });
      const percentile = regressionPercentile(budget, platform);
      const base = run.base?.metrics?.[name];
      if (base && base.n > 0) {
        const limit = bound(base[percentile], budget);
        checks.push({
          id,
          rule: `vs base ${percentile}`,
          value: measured[percentile],
          bound: limit,
          baseline: base[percentile],
          status: measured[percentile] > limit ? "fail" : "ok",
        });
      }
      const recorded = stored?.metrics?.[id]?.[percentile];
      if (typeof recorded === "number") {
        const limit = bound(recorded, budget);
        const over = measured[percentile] > limit;
        checks.push({
          id,
          rule: `vs stored ${percentile}`,
          value: measured[percentile],
          bound: limit,
          baseline: recorded,
          status: !over ? "ok" : anyBase || !cpuKnown ? "warn" : "fail",
        });
      }
    }
  }
  return {
    passed: checks.every((check) => check.status !== "fail"),
    checks,
    warnings,
  };
}

/**
 * Baselines from an aggregated report, for the budgeted metrics: each
 * metric's p50 and p99, the *worst round's* where the report has rounds,
 * rounded up to the next microsecond, so a stored bound is never stricter
 * than the run that produced it.
 */
export function baselinesFromReport(report, budgets) {
  const metrics = {};
  const up = (value) => Math.ceil(value * 1000) / 1000;
  for (const run of report.runs) {
    for (const [name, measured] of Object.entries(run.result.metrics)) {
      const id = metricId(run, name);
      if (!measured || measured.n === 0 || !budgets?.[id]) continue;
      metrics[id] = {
        p50: up(measured.worstP50 ?? measured.p50),
        p99: up(measured.worstP99 ?? measured.p99),
      };
    }
  }
  return metrics;
}

/**
 * Keep the worse of two baseline sets, metric by metric, so recording on a
 * second runner of the same class never tightens a bound the first one set.
 */
export function mergeBaselines(previous, next) {
  const merged = { ...(previous ?? {}) };
  for (const [id, value] of Object.entries(next)) {
    merged[id] = {
      p50: Math.max(value.p50 ?? 0, merged[id]?.p50 ?? 0),
      p99: Math.max(value.p99, merged[id]?.p99 ?? 0),
    };
  }
  return merged;
}

const fmt = (value) => (typeof value === "number" ? value.toFixed(3) : "—");

/** The per-stage table published in the PR and in `$GITHUB_STEP_SUMMARY`. */
export function renderMarkdown(report, gate, thresholds) {
  const lines = [];
  const m = report.machine;
  lines.push(`### Session-visible latency — ${m.platform}-${m.arch}, ${m.cpu} (${m.cores} cores)`);
  lines.push("");
  lines.push(
    `Profile \`${report.profile}\`, ${report.rounds} round(s) per run, release build: ${report.release}. ` +
      `Each value is the median across rounds of that round's nearest-rank percentile, in ms.`,
  );
  lines.push("");
  const hasBase = report.runs.some((run) => run.base);
  if (hasBase) lines.push(`Base: \`${report.baseCommit}\`, measured interleaved on the same runner.`, "");
  lines.push(
    `| Metric | Budget p99 | p50 | p90 | p99 | worst-round p99 | max | n |${hasBase ? " base p50 | base p99 |" : ""} Gate |`,
  );
  lines.push(`|---|---:|---:|---:|---:|---:|---:|---:|${hasBase ? "---:|---:|" : ""}:--|`);
  const byId = new Map();
  for (const check of gate.checks) {
    const list = byId.get(check.id) ?? [];
    list.push(check);
    byId.set(check.id, list);
  }
  for (const run of report.runs) {
    for (const [name, measured] of Object.entries(run.result.metrics)) {
      const id = metricId(run, name);
      const budget = thresholds.budgets?.[id];
      const verdict = (byId.get(id) ?? [])
        .map((check) => `${check.status} (${check.rule} ${fmt(check.bound)})`)
        .join(", ");
      const platform = `${m.platform}-${m.arch}`;
      const tracked = budget && !budgetEnforced(budget, platform) ? " (tracked)" : "";
      lines.push(
        `| \`${id}\` | ${budget ? fmt(budget.p99Ms) + tracked : ""} | ` +
          `${fmt(measured.p50)} | ${fmt(measured.p90)} | ${fmt(measured.p99)} | ${fmt(measured.worstP99)} | ` +
          `${fmt(measured.max)} | ${measured.n ?? 0} |` +
          (hasBase ? ` ${fmt(run.base?.metrics?.[name]?.p50)} | ${fmt(run.base?.metrics?.[name]?.p99)} |` : "") +
          ` ${verdict} |`,
      );
    }
  }
  lines.push("");
  const scalars = [];
  for (const run of report.runs) {
    const { metrics: _m, store, ...rest } = run.result;
    const keep = Object.entries(rest).filter(
      ([key]) => !["phase", "samples", "turns", "release", "phaseWallMs", "watchDefaults"].includes(key),
    );
    const storeText = store
      ? `${store.sessions} sessions, ${(store.dbBytes / 1048576).toFixed(1)} MiB db`
      : "";
    scalars.push(
      `- \`${runId(run)}\` (${storeText}): ` + keep.map(([k, v]) => `${k}=${JSON.stringify(v)}`).join(", "),
    );
  }
  lines.push(...scalars);
  for (const warning of gate.warnings) lines.push("", `> warning: ${warning}`);
  lines.push("", gate.passed ? "**Gate: pass**" : "**Gate: FAIL**");
  return `${lines.join("\n")}\n`;
}

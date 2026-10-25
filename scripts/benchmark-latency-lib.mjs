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
      worstP99: Math.max(...series.map((m) => m.p99)),
      rounds: series.length,
    };
  }
  const { metrics: _ignored, ...rest } = rounds[0] ?? {};
  return { ...rest, metrics };
}

/**
 * Apply budgets and baselines to an aggregated report.
 *
 * Two independent rules, each producing `ok`, `warn` or `fail` per metric:
 *
 * - **Budget.** A metric listed in `thresholds.budgets` fails when its p99 is
 *   at or above the budget, if the budget is `enforced`. An unenforced
 *   budget is one the code does not meet yet: a breach is a `warn` and the
 *   run stays green, so the gate can land before the fix does and is flipped
 *   to enforced by the change that brings the stage under budget.
 * - **Regression.** A metric with a stored baseline for this platform class
 *   fails when its p99 exceeds `baseline × (1 + maxRegression)` plus
 *   `regressionFloorMs`. The floor exists because a 10% bound on a 0.1 ms
 *   number is 10 µs, which is scheduler jitter rather than code.
 *
 * A platform with no baselines gets the budget rule alone and a warning
 * saying so, rather than a failure: a baseline belongs to the machine class
 * that produced it.
 */
export function evaluateGate(report, thresholds, platform) {
  const policy = thresholds.policy;
  const baselines = thresholds.baselines?.[platform]?.metrics ?? null;
  const checks = [];
  const warnings = [];
  if (!baselines) {
    warnings.push(
      `no stored baselines for ${platform}; only budgets are checked. ` +
        `Record them with --update-baselines on that runner class.`,
    );
  }
  for (const run of report.runs) {
    for (const [name, measured] of Object.entries(run.result.metrics)) {
      const id = metricId(run, name);
      if (!measured || measured.n === 0) continue;
      const budget = thresholds.budgets?.[id];
      if (budget) {
        const over = measured.p99 >= budget.p99Ms;
        checks.push({
          id,
          rule: "budget",
          value: measured.p99,
          bound: budget.p99Ms,
          status: !over ? "ok" : budget.enforced ? "fail" : "warn",
          stage: budget.stage,
        });
      }
      const baseline = baselines?.[id];
      if (baseline && typeof baseline.p99 === "number") {
        const bound = baseline.p99 * (1 + policy.maxRegression) + policy.regressionFloorMs;
        checks.push({
          id,
          rule: "regression",
          value: measured.p99,
          bound: Math.round(bound * 1000) / 1000,
          baseline: baseline.p99,
          status: measured.p99 > bound ? "fail" : "ok",
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
 * Baselines from an aggregated report: each metric's p99, rounded up to the
 * next microsecond so the stored bound is never stricter than the run that
 * produced it.
 */
export function baselinesFromReport(report) {
  const metrics = {};
  for (const run of report.runs) {
    for (const [name, measured] of Object.entries(run.result.metrics)) {
      if (!measured || measured.n === 0) continue;
      metrics[metricId(run, name)] = { p99: Math.ceil(measured.p99 * 1000) / 1000 };
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
    merged[id] = { p99: Math.max(value.p99, merged[id]?.p99 ?? 0) };
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
  lines.push("| Metric | Budget p99 | p50 | p90 | p99 | worst-round p99 | max | n | Gate |");
  lines.push("|---|---:|---:|---:|---:|---:|---:|---:|:--|");
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
      lines.push(
        `| \`${id}\` | ${budget ? fmt(budget.p99Ms) + (budget.enforced ? "" : " (tracked)") : ""} | ` +
          `${fmt(measured.p50)} | ${fmt(measured.p90)} | ${fmt(measured.p99)} | ${fmt(measured.worstP99)} | ` +
          `${fmt(measured.max)} | ${measured.n ?? 0} | ${verdict} |`,
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

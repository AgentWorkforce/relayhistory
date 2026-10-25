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
 * a bound on that tail measures luck, while the p50 is the cost the fix for
 * that stage will move.
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

const round3 = (value) => Math.round(value * 1000) / 1000;

/**
 * Apply the budgets and regression rules to an aggregated report.
 *
 * Only budgeted metrics -- the stages of the 5 ms path -- are gated; the
 * rest of the table is diagnostic. A budgeted metric is either **enforced**
 * on this platform class (see `budgetEnforced`) or **tracked**: a known
 * over-budget stage whose current numbers are recorded in its budget's
 * `known` block.
 *
 * Enforced stage:
 * - p99 at or above the budget fails.
 * - p99 more than `maxRegression` plus the floor over the base commit,
 *   measured interleaved on this runner (`--base`), fails.
 * - p99 over the stored baseline fails only with no base to compare with and
 *   on a CPU the baselines were recorded on; otherwise it is advisory.
 *
 * Tracked stage:
 * - Over budget is `xfail`: expected, reported, not failing. Under budget is
 *   `xpass`, a warning that names the stage as ready to enforce.
 * - Getting worse fails. "Worse" is the p50 more than `trackedMaxRegression`
 *   over the same-runner base *and* more than `maxRegression` over its
 *   recorded known p50 -- both, because on identical code these stages
 *   moved 12-27% between interleaved base and head runs (lock waits behind a
 *   prune, the debounce clock), so either test alone fails healthy pull
 *   requests. Without a base, the known number alone decides on a recorded
 *   CPU and advises elsewhere.
 *
 * Every bound adds `regressionFloor`.
 */
export function evaluateGate(report, thresholds, platform) {
  const policy = thresholds.policy;
  const stored = thresholds.baselines?.[platform] ?? null;
  const cpuKnown = stored?.measuredOn?.cpusSeen?.includes(report.machine?.cpu) ?? false;
  const checks = [];
  const warnings = [];
  const anyBase = report.runs.some((run) => run.base);
  if (!anyBase && !stored) {
    warnings.push(`no base comparison and no stored baselines for ${platform}; only budgets are checked.`);
  } else if (!anyBase && !cpuKnown) {
    warnings.push(
      `${report.machine?.cpu} is not one the ${platform} baselines were recorded on ` +
        `(${stored.measuredOn?.cpusSeen?.join(", ")}); stored and known numbers are advisory.`,
    );
  }
  const over = (value, reference, factor, budget) => {
    const limit = round3(reference * (1 + factor) + regressionFloor(policy, budget));
    return { limit, over: value > limit };
  };
  for (const run of report.runs) {
    for (const [name, measured] of Object.entries(run.result.metrics)) {
      const id = metricId(run, name);
      const budget = thresholds.budgets?.[id];
      if (!budget || !measured || measured.n === 0) continue;
      const enforced = budgetEnforced(budget, platform);
      const withinBudget = measured.p99 < budget.p99Ms;
      checks.push({
        id,
        rule: "budget",
        value: measured.p99,
        bound: budget.p99Ms,
        status: withinBudget ? (enforced ? "ok" : "xpass") : enforced ? "fail" : "xfail",
        stage: budget.stage,
      });
      if (!enforced && withinBudget) {
        warnings.push(`${id} is within its ${budget.p99Ms} ms budget on ${platform}: enforce it there.`);
      }
      const percentile = regressionPercentile(budget, platform);
      const value = measured[percentile];
      const base = run.base?.metrics?.[name];
      const hasBase = base && base.n > 0;
      if (enforced) {
        if (hasBase) {
          const vs = over(value, base[percentile], policy.maxRegression, budget);
          checks.push({ id, rule: `vs base ${percentile}`, value, bound: vs.limit, baseline: base[percentile],
            status: vs.over ? "fail" : "ok" });
        }
        const recorded = stored?.metrics?.[id]?.[percentile];
        if (typeof recorded === "number") {
          const vs = over(value, recorded, policy.maxRegression, budget);
          checks.push({ id, rule: `vs stored ${percentile}`, value, bound: vs.limit, baseline: recorded,
            status: !vs.over ? "ok" : hasBase || !cpuKnown ? "warn" : "fail" });
        }
        continue;
      }
      const known = budget.known?.[platform]?.[percentile];
      const vsKnown = typeof known === "number" ? over(value, known, policy.maxRegression, budget) : null;
      const vsBase = hasBase ? over(value, base[percentile], policy.trackedMaxRegression, budget) : null;
      const worse = vsBase
        ? vsBase.over && (vsKnown ? vsKnown.over : true)
        : Boolean(vsKnown?.over && cpuKnown);
      const status = (isOver) => (!isOver ? "ok" : worse ? "fail" : "warn");
      if (vsBase) {
        checks.push({ id, rule: `vs base ${percentile}`, value, bound: vsBase.limit, baseline: base[percentile],
          status: status(vsBase.over) });
      }
      if (vsKnown) {
        checks.push({ id, rule: `vs known ${percentile}`, value, bound: vsKnown.limit, baseline: known,
          status: status(vsKnown.over) });
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
 * What `--update-baselines` records from an aggregated report, for the
 * budgeted metrics only: each one's p50 and p99 -- the *worst round's* where
 * the report has rounds -- rounded up to the next microsecond, so a stored
 * number is never stricter than the run that produced it. Enforced stages go
 * to `baselines`; tracked ones are the `known` numbers in their budget.
 */
export function baselinesFromReport(report, budgets, platform) {
  const baselines = {};
  const known = {};
  const up = (value) => Math.ceil(value * 1000) / 1000;
  for (const run of report.runs) {
    for (const [name, measured] of Object.entries(run.result.metrics)) {
      const id = metricId(run, name);
      const budget = budgets?.[id];
      if (!measured || measured.n === 0 || !budget) continue;
      const numbers = {
        p50: up(measured.worstP50 ?? measured.p50),
        p99: up(measured.worstP99 ?? measured.p99),
      };
      (budgetEnforced(budget, platform) ? baselines : known)[id] = numbers;
    }
  }
  return { baselines, known };
}

/**
 * Keep the worse of two sets of recorded numbers, metric by metric, so a
 * second run of the same class never tightens a bound the first one set.
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
      const trackedMark =
        budget && !budgetEnforced(budget, `${m.platform}-${m.arch}`) ? " (tracked)" : "";
      lines.push(
        `| \`${id}\` | ${budget ? fmt(budget.p99Ms) + trackedMark : ""} | ` +
          `${fmt(measured.p50)} | ${fmt(measured.p90)} | ${fmt(measured.p99)} | ${fmt(measured.worstP99)} | ` +
          `${fmt(measured.max)} | ${measured.n ?? 0} |` +
          (hasBase ? ` ${fmt(run.base?.metrics?.[name]?.p50)} | ${fmt(run.base?.metrics?.[name]?.p99)} |` : "") +
          ` ${verdict} |`,
      );
    }
  }
  const platform = `${m.platform}-${m.arch}`;
  const tracked = report.runs.flatMap((run) =>
    Object.entries(run.result.metrics)
      .map(([name, measured]) => [metricId(run, name), measured])
      .filter(([id]) => thresholds.budgets?.[id] && !budgetEnforced(thresholds.budgets[id], platform)),
  );
  if (tracked.length) {
    lines.push("", `#### Known over budget on ${platform} (tracked: expected to fail, must not get worse)`, "");
    lines.push("| Stage | Metric | Budget p99 | known p50 | known p99 | now p50 | now p99 |");
    lines.push("|---|---|---:|---:|---:|---:|---:|");
    for (const [id, measured] of tracked) {
      const budget = thresholds.budgets[id];
      const known = budget.known?.[platform];
      lines.push(
        `| ${budget.stage} | \`${id}\` | ${fmt(budget.p99Ms)} | ${fmt(known?.p50)} | ${fmt(known?.p99)} | ` +
          `${fmt(measured.p50)} | ${fmt(measured.p99)} |`,
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

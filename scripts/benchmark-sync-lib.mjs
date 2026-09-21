// Shapes, planning, and gate arithmetic for the sync/hydration throughput
// benchmark. Everything here is pure: no filesystem, no child processes, no
// clock. `gen-synthetic-history.mjs` turns these records into a store on disk
// and `benchmark-sync.mjs` runs the measurement and applies `evaluateGate`.
//
// The record shapes are modelled on what the parsers in
// `crates/ai-hist/src/ingest.rs` actually read, not on any captured transcript:
// nothing real is committed, and a seed makes every store byte-for-byte
// reproducible. When the fixture corpus from #192 lands, `recordShapes` is the
// single place to re-point at it.

/** Deterministic 32-bit PRNG. Same seed, same store, on every machine. */
export function createRng(seed) {
  if (!Number.isSafeInteger(seed) || seed < 0) {
    throw new Error(`seed must be a non-negative safe integer, got ${seed}`);
  }
  let state = (seed + 0x6d2b79f5) >>> 0;
  return function next() {
    state = (state + 0x6d2b79f5) >>> 0;
    let t = state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** Pick one element of `values`, deterministically for a given rng position. */
export function pick(rng, values) {
  if (values.length === 0) throw new Error("pick needs a non-empty list");
  return values[Math.min(values.length - 1, Math.floor(rng() * values.length))];
}

const TOOL_NAMES = ["Read", "Edit", "Write", "Bash", "Grep", "Glob", "Task"];
const MODELS = ["claude-opus-4", "claude-sonnet-4", "claude-haiku-4"];
const WORDS = [
  "sync", "catalog", "transcript", "hydrate", "session", "provider", "ledger",
  "cursor", "digest", "rollout", "evidence", "throughput", "baseline", "gate",
];

/** Filler of an exact byte length, built from a stable word list. */
export function filler(rng, bytes) {
  if (bytes <= 0) return "";
  let text = "";
  while (text.length < bytes) text += `${pick(rng, WORDS)} `;
  return text.slice(0, bytes);
}

export const SOURCES = ["claude", "codex", "cursor", "grok", "opencode"];
export const FILE_SOURCES = ["claude", "codex", "cursor", "grok"];

/**
 * Normalize generator options into a plan. `targetBytes` is the size the
 * generator grows the store to by adding whole sessions round-robin; the
 * manifest records what was actually written.
 */
export function planStore(options = {}) {
  const plan = {
    seed: options.seed ?? 176,
    sources: options.sources ?? [...FILE_SOURCES],
    targetBytes: options.targetBytes ?? 8 * 1024 * 1024,
    turns: options.turns ?? 12,
    toolResults: options.toolResults ?? 2,
    toolResultBytes: options.toolResultBytes ?? 1024,
    largeSessionBytes: options.largeSessionBytes ?? 0,
    project: options.project ?? "relayhistory-bench",
  };
  const seen = new Set();
  for (const source of plan.sources) {
    if (!SOURCES.includes(source)) throw new Error(`unknown source ${source}`);
    // A repeated source generates the same session ids twice per ordinal: the
    // second write replaces the first file while both are counted, so the store
    // silently falls short of `targetBytes` and the manifest overstates it.
    if (seen.has(source)) {
      throw new Error(
        `source \`${source}\` is listed more than once. Each source is written once per `
        + "round, so a duplicate would overwrite its own files while still being counted, "
        + "leaving the store smaller than the byte target it reports.",
      );
    }
    seen.add(source);
  }
  if (plan.sources.length === 0) throw new Error("at least one source is required");
  if (plan.targetBytes <= 0) throw new Error("targetBytes must be positive");
  if (plan.turns <= 0) throw new Error("turns must be positive");
  return plan;
}

function timestamp(baseMs, step) {
  return new Date(baseMs + step * 1000).toISOString();
}

/**
 * One Claude project transcript: user prompt, assistant thinking/text/tool_use,
 * then a user record carrying tool_result blocks. `ingest_claude_transcript`
 * derives a history row, session events, tool calls and file edits from exactly
 * these blocks, so the shape exercises every write the hot loop performs.
 */
export function claudeTranscript(plan, sessionId, rng, { turns = plan.turns, baseMs } = {}) {
  const cwd = `/work/${plan.project}`;
  const lines = [];
  let uuid = 0;
  const nextUuid = () => `${sessionId}-${(uuid++).toString(36).padStart(6, "0")}`;
  let previous = null;
  for (let turn = 0; turn < turns; turn++) {
    const model = pick(rng, MODELS);
    const promptUuid = nextUuid();
    lines.push(JSON.stringify({
      sessionId, uuid: promptUuid, parentUuid: previous, cwd, gitBranch: "main",
      version: "1.2.3", type: "user",
      message: { role: "user", content: [{ type: "text", text: `turn ${turn}: ${filler(rng, 180)}` }] },
      timestamp: timestamp(baseMs, turn * 3),
    }));
    const assistantUuid = nextUuid();
    const toolUses = [];
    for (let index = 0; index < plan.toolResults; index++) {
      const name = pick(rng, TOOL_NAMES);
      toolUses.push({
        type: "tool_use",
        id: `toolu_${assistantUuid}_${index}`,
        name,
        input: name === "Bash"
          ? { command: `rg --files ${pick(rng, WORDS)}` }
          : { file_path: `${cwd}/src/${pick(rng, WORDS)}.rs`, content: filler(rng, 120) },
      });
    }
    lines.push(JSON.stringify({
      sessionId, uuid: assistantUuid, parentUuid: promptUuid, cwd, gitBranch: "main",
      type: "assistant",
      message: {
        role: "assistant", model,
        usage: { input_tokens: 1200 + turn, output_tokens: 300 + turn },
        content: [
          { type: "thinking", thinking: filler(rng, 220) },
          { type: "text", text: filler(rng, 260) },
          ...toolUses,
        ],
      },
      timestamp: timestamp(baseMs, turn * 3 + 1),
    }));
    const resultUuid = nextUuid();
    lines.push(JSON.stringify({
      sessionId, uuid: resultUuid, parentUuid: assistantUuid, cwd, gitBranch: "main",
      type: "user",
      message: {
        role: "user",
        content: toolUses.map((use) => ({
          type: "tool_result",
          tool_use_id: use.id,
          content: [{ type: "text", text: filler(rng, plan.toolResultBytes) }],
        })),
      },
      timestamp: timestamp(baseMs, turn * 3 + 2),
    }));
    previous = resultUuid;
  }
  return `${lines.join("\n")}\n`;
}

/** One `~/.claude/history.jsonl` prompt-log line, read by `parse_claude`. */
export function claudeHistoryLine(plan, sessionId, rng, baseMs) {
  return `${JSON.stringify({
    display: `turn 0: ${filler(rng, 60)}`,
    sessionId,
    project: `/work/${plan.project}`,
    timestamp: baseMs,
  })}\n`;
}

/** A Codex rollout: session_meta, turn_context, then alternating event_msg rows. */
export function codexRollout(plan, sessionId, rng, { turns = plan.turns, baseMs } = {}) {
  const cwd = `/work/${plan.project}`;
  const lines = [
    JSON.stringify({
      timestamp: timestamp(baseMs, 0), type: "session_meta",
      payload: {
        id: sessionId, cwd, originator: "codex_cli_rs", cli_version: "0.148.0",
        workspace_roots: [cwd],
        git: { branch: "dev", repository_url: "git@github.com:acme/bench.git", commit_hash: "abc1234" },
      },
    }),
    JSON.stringify({
      timestamp: timestamp(baseMs, 1), type: "turn_context",
      payload: { model: "gpt-5-codex" },
    }),
  ];
  for (let turn = 0; turn < turns; turn++) {
    lines.push(JSON.stringify({
      timestamp: timestamp(baseMs, turn * 3 + 2), type: "event_msg",
      payload: { type: "user_message", message: `turn ${turn}: ${filler(rng, 180)}` },
    }));
    lines.push(JSON.stringify({
      timestamp: timestamp(baseMs, turn * 3 + 3), type: "event_msg",
      payload: { type: "agent_message", message: filler(rng, 480 + plan.toolResultBytes) },
    }));
  }
  return `${lines.join("\n")}\n`;
}

/** One `~/.codex/history.jsonl` line, read by `parse_codex`. */
export function codexHistoryLine(plan, sessionId, rng, baseMs) {
  return `${JSON.stringify({
    session_id: sessionId,
    text: `turn 0: ${filler(rng, 60)}`,
    ts: Math.floor(baseMs / 1000),
  })}\n`;
}

/** A Cursor agent transcript. */
export function cursorTranscript(plan, sessionId, rng, { turns = plan.turns, baseMs } = {}) {
  const lines = [];
  for (let turn = 0; turn < turns; turn++) {
    lines.push(JSON.stringify({
      role: "user",
      message: {
        content: [{
          type: "text",
          text: `<user_query>\nturn ${turn}: ${filler(rng, 160)}\n</user_query>`,
        }],
      },
    }));
    lines.push(JSON.stringify({
      role: "assistant",
      message: { content: [{ type: "text", text: filler(rng, 420 + plan.toolResultBytes) }] },
    }));
  }
  void baseMs;
  return `${lines.join("\n")}\n`;
}

/** A Grok session: a `summary.json` plus a `chat_history.jsonl`. */
export function grokSession(plan, sessionId, rng, { turns = plan.turns, baseMs } = {}) {
  const summary = `${JSON.stringify({
    info: { id: sessionId, cwd: `/work/${plan.project}` },
    created_at: timestamp(baseMs, 0),
    updated_at: timestamp(baseMs, turns * 2),
    head_branch: "trunk",
  })}\n`;
  const lines = [];
  for (let turn = 0; turn < turns; turn++) {
    lines.push(JSON.stringify({
      type: "user",
      content: [{ type: "text", text: `turn ${turn}: ${filler(rng, 160)}` }],
    }));
    lines.push(JSON.stringify({
      type: "assistant",
      content: [{ type: "text", text: filler(rng, 400 + plan.toolResultBytes) }],
    }));
  }
  return { summary, chat: `${lines.join("\n")}\n` };
}

// The 1 KiB record an incremental sync is measured against is written by the
// harness (`append_one_record` in `crates/ai-hist-cli/tests/sync_bench.rs`), so
// that it lands outside the timed region and in the same process that times it.

/**
 * What each phase needs before it can be measured.
 *
 * `needs` names phases that must already have run in this invocation —
 * everything but `cold_sync` reads a database `cold_sync` created, and
 * `cold_sync` itself insists on a database that does not exist yet, so a list
 * is not a set: order is part of whether it can run at all.
 *
 * `target` names the manifest field the phase is pointed at. `source` is the
 * provider that field can only come from — `append_one_record` writes a
 * Claude-shaped record and no other provider has an equivalent yet, which is
 * why `incremental_sync` alone carries one. Letting a Claude transcript be
 * smuggled into a store that did not ask for one is not the alternative: that
 * would put a whole provider into the ingested byte count while the report
 * still called the run codex-only.
 */
export const PHASE_REQUIREMENTS = {
  cold_sync: { needs: [] },
  incremental_sync: {
    needs: ["cold_sync"],
    target: "incrementalTarget",
    source: "claude",
    reason: "the harness appends a Claude-shaped record and no other provider has an equivalent",
  },
  unchanged_sync: { needs: ["cold_sync"] },
  hydrate_cold: { needs: ["cold_sync"], target: "hydrationTarget" },
  hydrate_unchanged: { needs: ["cold_sync"], target: "hydrationTarget" },
};

/**
 * Phases this run cannot measure, as `{ phase, reason }`.
 *
 * Call it twice. With `plan`, before generating anything, it catches what the
 * request alone already rules out. With `manifest`, after generating and
 * before the first phase runs, it catches what the store actually contains —
 * and that reading is the authoritative one, because a source appearing in
 * `--sources` is not a promise that a session of it was written. When the
 * oversized session alone meets the byte target the round-robin loop never
 * runs, so `--sources codex,claude` can produce a store with no Claude
 * transcript in it at all.
 */
export function unsupportedPhases(phases, { plan, manifest } = {}) {
  const list = phases ?? [];
  const available = new Set(plan?.sources ?? []);
  const seen = new Set();
  const problems = [];
  for (const phase of list) {
    const requirement = PHASE_REQUIREMENTS[phase];
    const add = (reason) => problems.push({ phase, reason });
    if (seen.has(phase)) {
      add("is listed more than once; each phase leaves state the next one reads, "
        + "so running it twice in one invocation is not repeatable");
      continue;
    }
    seen.add(phase);
    if (!requirement) continue;
    const missing = (requirement.needs ?? []).filter((prerequisite) => !seen.has(prerequisite));
    if (missing.length > 0) {
      add(`needs ${missing.map((name) => `\`${name}\``).join(" and ")} to run before it `
        + "in this invocation, and the requested order does not");
      continue;
    }
    // The manifest is the ground truth; the plan is only a cheap early guess.
    if (manifest) {
      if (requirement.target && !manifest[requirement.target]) {
        add(requirement.source
          ? `the generated store has no ${requirement.source[0].toUpperCase()}`
            + `${requirement.source.slice(1)} transcript to use as its \`${requirement.target}\``
            + `, though the plan listed \`${requirement.source}\` among its sources`
          : `the generated store produced no \`${requirement.target}\``);
      }
      continue;
    }
    if (requirement.source && !available.has(requirement.source)) {
      add(`needs a \`${requirement.source}\` source (${requirement.reason}); `
        + `this plan has ${[...available].join(", ") || "no sources"}`);
    }
  }
  return problems;
}

// ---------------------------------------------------------------------------
// reporting and the gate
// ---------------------------------------------------------------------------

export function formatBytes(value) {
  if (value === null || value === undefined) return "—";
  const units = ["B", "KiB", "MiB", "GiB"];
  let scaled = Number(value);
  let unit = 0;
  while (scaled >= 1024 && unit + 1 < units.length) {
    scaled /= 1024;
    unit += 1;
  }
  return unit === 0 ? `${value} B` : `${scaled.toFixed(1)} ${units[unit]}`;
}

export function formatNumber(value) {
  if (value === null || value === undefined || Number.isNaN(value)) return "—";
  return Number(value).toLocaleString("en-US", { maximumFractionDigits: 0 });
}

/**
 * How long this machine took on the reference workload, relative to the runs
 * the baselines were recorded on. Above 1 is slower, below 1 is faster.
 *
 * This started out as a *corrector*: scale each measurement by it, and a
 * throughput floor recorded on one machine would mean something on another.
 * Two GitHub runners disproved that. On `ubuntu-latest` the pool contains at
 * least two CPUs, and they do not agree about which of them is faster:
 *
 *     measurement            6973P-C   8573C    faster   spread
 *     calibration (time)       129.0   166.8      A       1.29x
 *     cold_sync (rec/s)        478.6   631.0      B       1.32x
 *     incremental (time)       423.3   272.9      B       1.55x
 *     unchanged (time)          47.1    50.8      A       1.08x
 *     hydrate_cold (rec/s)     406.3   296.7      A       1.37x
 *     hydrate_unchanged (time)  10.5    14.1      A       1.34x
 *
 * The calibration agrees with three phases and contradicts two — and the two
 * it contradicts, `cold_sync` and `incremental_sync`, are exactly the ones a
 * scaled comparison failed on a healthy runner. No single scalar can fix that,
 * because the phases themselves disagree about which machine is faster; it is
 * a property of the hardware, not of the reference's composition.
 *
 * So the factor is now a **diagnostic**, not a decision, unless
 * `policy.calibration` is set to `"applied"`. Contention, which is the case it
 * does track, is absorbed instead by the plain 2x throughput and time margins
 * — those margins *are* the contention allowance, and stacking a second,
 * sometimes anti-correlated corrector on top made the gate worse.
 */
export const CALIBRATION_CLAMP = { min: 0.2, max: 8 };

/**
 * How far the bounds may be widened on a runner the baselines were never
 * measured on — and no further.
 *
 * GitHub added an AMD EPYC 7763 to the `ubuntu-latest` pool after these
 * baselines were taken on two Intel Xeons. The gate already said so
 * ("a failure here may be the hardware rather than the code") and then failed
 * the pull request anyway, which blocked three of them on 2026-09-21 with
 * every other check green. Off the baseline class the bounds are therefore
 * widened rather than merely annotated — but only ever widened, only by a
 * measured amount, and never past this cap, so a genuinely regressed run
 * cannot hide behind a slow runner. 2x on top of the policy's own 2x/0.5x
 * margins leaves a 4x regression red on any machine.
 */
export const OFF_CLASS_CALIBRATION_CAP = 2;

/**
 * The factor a bound derived from a stored baseline is widened by off class.
 *
 * It is the calibration ratio, clamped to `[1, cap]`. The lower clamp is the
 * important half: below 1 the reference says this machine is *faster* than the
 * baseline machines, and tightening a ceiling on that reading is exactly the
 * corrector that failed a healthy runner in #202. This one may only loosen.
 */
export function offClassScaleFor(raw, cap = OFF_CLASS_CALIBRATION_CAP) {
  if (!Number.isFinite(raw) || raw <= 1) return 1;
  return Math.min(raw, cap);
}

/** Outside this band, the baselines probably came from different hardware. */
export const CALIBRATION_WARN_BAND = { min: 0.7, max: 1.4 };

export function calibrationFactor(measuredMs, baselineMs) {
  if (!Number.isFinite(measuredMs) || !Number.isFinite(baselineMs)) return null;
  if (measuredMs <= 0 || baselineMs <= 0) return null;
  const raw = measuredMs / baselineMs;
  return {
    raw,
    factor: Math.min(CALIBRATION_CLAMP.max, Math.max(CALIBRATION_CLAMP.min, raw)),
    clamped: raw < CALIBRATION_CLAMP.min || raw > CALIBRATION_CLAMP.max,
  };
}

/**
 * Compare one measured report against stored thresholds.
 *
 * Every metric the profile stores a baseline for is checked, and a phase the
 * profile names but the report does not carry is a failure rather than a skip:
 * a harness that silently ran nothing must not read as a pass.
 *
 * Throughput and elapsed time are first scaled by the calibration factor, so
 * what is compared is "what this phase would have cost on the machine the
 * baseline came from". Peak RSS is not scaled: memory does not get bigger
 * because the box is busy.
 *
 * On a CPU that is not one of `measuredOn.cpusSeen` the bounds are widened —
 * see `OFF_CLASS_CALIBRATION_CAP`, and the two rules below. On the baseline
 * class none of it runs and the verdict is exactly what it was before.
 */
export function evaluateGate(report, thresholds, profileName) {
  const profile = thresholds?.profiles?.[profileName];
  if (!profile) {
    return {
      ok: false,
      profile: profileName,
      checks: [],
      failures: [`no threshold profile named "${profileName}" in the thresholds file`],
    };
  }
  const policy = thresholds.policy ?? {};
  const floors = policy.absoluteFloors ?? {};
  const phaseFloors = policy.absoluteFloorsByPhase ?? {};
  const factors = {
    // `scale` turns a measurement into what it would have been on the baseline
    // machine: a busy box reports less throughput and more elapsed time.
    recordsPerSecond: {
      direction: "min",
      factor: policy.minRecordsPerSecondFactor ?? 0.5,
      scale: (value, load) => value * load,
    },
    peakRssBytes: { direction: "max", factor: policy.maxPeakRssFactor ?? 1.5 },
    elapsedMs: {
      direction: "max",
      factor: policy.maxElapsedMsFactor ?? 2,
      scale: (value, load) => value / load,
    },
  };
  const measured = new Map((report.phases ?? []).map((phase) => [phase.phase, phase]));
  const checks = [];
  const failures = [];
  const warnings = [];
  const calibration = calibrationFactor(report.calibrationMs, profile.calibrationMs);
  // Applying the factor is opt-in; see CALIBRATION_CLAMP for why. Measuring it
  // is not, because it is how a baseline taken on other hardware is spotted.
  const applied = policy.calibration === "applied";
  if (profile.calibrationMs && !calibration) {
    const complaint = "the profile stores a calibration baseline but this run measured none";
    if (applied) failures.push(`${complaint}; measurements cannot be scaled without it`);
    else {
      warnings.push({
        kind: "no-calibration",
        message: `${complaint}, so there is no reading on the machine this ran on`,
      });
    }
  }
  // Warnings carry a `kind` because they mean different things and a reader
  // acts on them differently: an unfamiliar CPU says the bounds may not belong
  // to this machine at all, while a reference that merely ran slow on a CPU the
  // baselines came from is usually load. Collapsing the two into "something is
  // off with the hardware" is how a real regression gets excused.
  //
  // A timing ratio is also a weak proxy for "is this the hardware the baselines
  // came from". The profile records the CPUs it was measured on, so ask that
  // directly rather than inferring it.
  const cpu = report.machine?.cpu;
  const cpusSeen = profile.measuredOn?.cpusSeen;
  const offClass = Boolean(
    cpu && Array.isArray(cpusSeen) && cpusSeen.length > 0 && !cpusSeen.includes(cpu),
  );
  if (offClass) {
    warnings.push({
      kind: "unknown-cpu",
      cpu,
      message: `these baselines were measured on ${cpusSeen.join(" and ")}, and this is `
        + `${cpu}. Bounds are absolute numbers from those machines, so a failure here may `
        + "be the hardware rather than the code; compare against a run on the baseline class "
        + "before treating it as a regression.",
    });
  }
  const band = policy.calibrationWarnBand ?? CALIBRATION_WARN_BAND;
  if (calibration && (calibration.raw < band.min || calibration.raw > band.max)) {
    const knownCpu = cpu && Array.isArray(cpusSeen) && cpusSeen.includes(cpu);
    warnings.push({
      kind: "calibration-band",
      raw: calibration.raw,
      knownCpu: Boolean(knownCpu),
      message: `the reference workload took ${calibration.raw.toFixed(2)}x as long here as in `
        + `the baseline runs, outside the expected ${band.min}x-${band.max}x. `
        + (knownCpu
          // Same CPU, different pace: the machine is busy, not different.
          ? "This is a CPU the baselines were measured on, so that is load rather than "
            + "different hardware."
          : "The baselines may have been recorded on different hardware; re-measure them "
            + "on this machine class before reading a failure below as a regression."),
    });
  }
  const load = applied ? (calibration?.factor ?? 1) : 1;
  // Widening off class and normalizing by `load` are two answers to the same
  // question, so only one of them is ever applied. `policy.calibration:
  // "applied"` already divides every measurement by the reference; doing both
  // would count the same machine twice.
  // A cap below 1 would turn the widening into a tightening, which is the one
  // thing this must never do, so a misconfigured one falls back to the default
  // rather than quietly making the gate stricter off class.
  const configuredCap = Number(policy.offClassCalibrationCap ?? OFF_CLASS_CALIBRATION_CAP);
  const cap = Number.isFinite(configuredCap) && configuredCap >= 1
    ? configuredCap
    : OFF_CLASS_CALIBRATION_CAP;
  const offClassScale = offClass && !applied ? offClassScaleFor(calibration?.raw, cap) : 1;
  const advisories = [];
  for (const [phaseName, baselines] of Object.entries(profile.phases ?? {})) {
    const phase = measured.get(phaseName);
    if (!phase) {
      failures.push(`phase "${phaseName}" has a stored baseline but produced no measurement`);
      continue;
    }
    for (const [metric, baseline] of Object.entries(baselines)) {
      const rule = factors[metric];
      if (!rule) {
        failures.push(`phase "${phaseName}" stores an unknown metric "${metric}"`);
        continue;
      }
      const value = phase[metric];
      if (value === null || value === undefined || Number.isNaN(Number(value))) {
        failures.push(`phase "${phaseName}" did not measure ${metric}`);
        continue;
      }
      // A ceiling on a very small baseline is dominated by runner noise rather
      // than by the code under test, so an absolute floor can raise it. The
      // floor only ever loosens a ceiling; it never tightens one, and it never
      // applies to a throughput floor.
      const floor = phaseFloors[phaseName]?.[metric] ?? floors[metric] ?? 0;
      const proportional = baseline * rule.factor;
      const bound = rule.direction === "max" ? Math.max(proportional, floor) : proportional;
      // Where the bound came from decides how much room an off-class runner
      // gets, because the two kinds of bound say different things:
      //
      //   * `baseline x factor` is a proportional statement about the code, so
      //     the calibration — a reading of how fast this machine is — is the
      //     right correction, capped.
      //   * an absolute floor is a runner-noise allowance measured on the
      //     baseline class. Off that class there is no measured noise floor at
      //     all, and the calibration does not describe walk-and-stat jitter:
      //     PR #204 spent 179 ms on `unchanged_sync` against a 120 ms floor
      //     while the reference said only 1.36x. Such a check gets the full
      //     cap, and a breach inside it is advisory rather than fatal.
      //
      // Memory gets neither. RSS does not grow because the CPU is slower, and
      // its floor is a blow-up guard that is valid on any machine.
      const fromFloor = rule.direction === "max" && floor > proportional;
      const widen = offClass && !applied && metric !== "peakRssBytes"
        ? (fromFloor ? cap : offClassScale)
        : 1;
      const effectiveBound = rule.direction === "max" ? bound * widen : bound / widen;
      const normalized = rule.scale && applied ? rule.scale(Number(value), load) : Number(value);
      const ok = rule.direction === "min"
        ? normalized >= effectiveBound
        : normalized <= effectiveBound;
      const breachesRaw = rule.direction === "min" ? normalized < bound : normalized > bound;
      const advisory = Boolean(ok && fromFloor && widen > 1 && breachesRaw);
      const limit = rule.direction === "min" ? "floor" : "ceiling";
      checks.push({
        phase: phaseName, metric, value: Number(value), normalized, baseline, bound,
        effectiveBound, widened: widen, advisory, ok,
      });
      if (advisory) {
        advisories.push(
          `${phaseName}.${metric} = ${formatNumber(value)} is past its ${formatNumber(bound)} `
          + `${limit}, which is a noise allowance measured on ${cpusSeen.join(" and ")}. `
          + `This ran on ${cpu}, so it is reported rather than failed, up to the `
          + `${formatNumber(effectiveBound)} the ${cap.toFixed(2)}x off-class cap allows.`,
        );
      }
      if (!ok) {
        failures.push(
          `${phaseName}.${metric} = ${formatNumber(value)}` +
          (rule.scale && applied ? ` (${formatNumber(normalized)} machine-normalized)` : "") +
          `, baseline ${formatNumber(baseline)}, ` +
          `${limit} ${formatNumber(bound)}` +
          (widen > 1
            ? ` (off-class ${limit} ${formatNumber(effectiveBound)}, widened ${widen.toFixed(2)}x)`
            : ""),
        );
      }
    }
  }
  if (checks.length === 0 && failures.length === 0) {
    failures.push(`threshold profile "${profileName}" checked nothing`);
  }
  return {
    ok: failures.length === 0,
    profile: profileName,
    calibrationApplied: applied,
    offClass,
    offClassScale,
    offClassCap: cap,
    advisories,
    calibration: calibration
      ? { measuredMs: report.calibrationMs, baselineMs: profile.calibrationMs, ...calibration }
      : null,
    checks,
    failures,
    warnings,
  };
}

/** The committed baseline table's row order and headers. */
export const REPORT_COLUMNS = [
  ["phase", "Phase"],
  ["storeBytes", "Store"],
  ["elapsedMs", "Time"],
  ["records", "Records"],
  ["recordsPerSecond", "Records/s"],
  ["megabytesPerSecond", "MB/s"],
  ["bytesRead", "Bytes read"],
  ["peakRssBytes", "Peak RSS"],
  ["dbBytes", "DB"],
  ["walBytes", "WAL"],
];

export function renderMarkdownTable(report) {
  const cell = (phase, key) => {
    const value = phase[key];
    if (value === null || value === undefined) return "—";
    switch (key) {
      case "phase": return `\`${value}\``;
      case "storeBytes": case "bytesRead": case "peakRssBytes": case "dbBytes": case "walBytes":
        return formatBytes(value);
      case "elapsedMs": return `${Number(value).toFixed(1)} ms`;
      case "megabytesPerSecond": return Number(value).toFixed(1);
      default: return formatNumber(value);
    }
  };
  const header = `| ${REPORT_COLUMNS.map(([, label]) => label).join(" | ")} |`;
  const rule = `|${REPORT_COLUMNS.map(([key]) => (key === "phase" ? "---" : "---:")).join("|")}|`;
  const rows = (report.phases ?? []).map(
    (phase) => `| ${REPORT_COLUMNS.map(([key]) => cell(phase, key)).join(" | ")} |`,
  );
  return [header, rule, ...rows].join("\n");
}

/**
 * Round a baseline away from the bound it produces: a throughput floor down, a
 * time or memory ceiling up. Rounding the other way makes the limit fractionally
 * stricter than the run it was measured from, which is how a baseline taken from
 * a healthy run can fail that very run.
 */
export function roundBaseline(metric, value) {
  return metric === "recordsPerSecond" ? Math.floor(value) : Math.ceil(value);
}

/** Baselines shaped for `scripts/benchmark-thresholds.json` from a measured run. */
export function baselinesFromReport(report, metrics) {
  const phases = {};
  for (const phase of report.phases ?? []) {
    const wanted = metrics?.[phase.phase];
    if (!wanted) continue;
    const entry = {};
    for (const metric of wanted) {
      const value = phase[metric];
      if (value === null || value === undefined) continue;
      entry[metric] = roundBaseline(metric, Number(value));
    }
    if (Object.keys(entry).length > 0) phases[phase.phase] = entry;
  }
  return phases;
}

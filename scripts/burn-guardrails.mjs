#!/usr/bin/env node
// Cross-repo guardrails between relayhistory and its downstream consumer
// AgentWorkforce/burn (#183, #184). Commands, in the order ci.yml's
// `burn-contract-drift` job runs them:
//
//   relevant-change
//     Is this push/PR a change burn could observe (crates/ai-hist/**, the
//     workspace manifest, or this guardrail itself)? Writes `run=true|false`.
//     A PR diffs its merge commit against the base; a push diffs the whole
//     pushed range (event `before`..HEAD). Anything that cannot be decided
//     (no parent commit, a new branch, a manual run) runs.
//
//   parity-probe --burn-dir <dir>
//     Does burn have a relayhistory parity suite to run? Writes
//     `ready=true|false`. It arms on burn's `relayburn-sdk` depending on
//     `ai-hist`. The suite is the `relayhistory_parity` test target when there
//     is one (#183's spelling), otherwise the unit tests under a `relayhistory`
//     module path in the lib (burn #557's spelling:
//     src/ingest/backend/relayhistory/tests.rs). No dependency yet is a
//     notice; a dependency with no suite is a warning. Neither fails the job:
//     that is the tripwire's job once burn's cutover is tagged.
//
//   pin-ai-hist --burn-dir <dir> --ai-hist-path <path>
//     Rewrites every `ai-hist` requirement in burn's manifests to a path
//     dependency on this checkout. `[patch.crates-io]` only applies when the
//     workspace version satisfies burn's pin, which it stops doing after every
//     relayhistory release; the question here is "does burn main pass against
//     this code", not "against this version number".
//
//   check-resolution --burn-dir <dir> --ai-hist-path <path>
//     Proves the `ai-hist` burn resolves is this checkout, with a specific
//     error for each way it might not be.
//
//   run-parity --burn-dir <dir>
//     Runs the suite the probe found and fails if it ran zero tests.
//
//   tripwire --burn-dir <dir> [--cutover-tag <tag>] [--repo <url>]
//     burn-reader-tripwire.yml. Inactive until burn's cutover tag exists.
//     After it, fails if burn's crates/ still contain the harness-parser
//     symbols burn #562's acceptance greps for, or if burn has no parity
//     suite. Leftover reader file names are a warning only: #562 keeps
//     reader/{claude,codex}/span_tree.rs and may rename reader/.
//
// Every command takes an `io` of `{ run, env, log }` so the tests can drive
// them without cargo, git or the network.

import { spawnSync } from "node:child_process";
import { appendFileSync, existsSync, readdirSync, readFileSync, realpathSync, writeFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

export const BURN_REPO_URL = "https://github.com/AgentWorkforce/burn.git";

// burn #562 ships the cutover as the 5.0.0 major. burn tags each crate
// release separately; the readers live in relayburn-sdk, so its tag is the
// one that means "the readers were deleted in a release".
export const DEFAULT_CUTOVER_TAG = "relayburn-sdk-v5.0.0";

export const SDK_PACKAGE = "relayburn-sdk";
export const SDK_DIR = "crates/relayburn-sdk";
export const PARITY_FEATURE = "relayhistory-source";
export const PARITY_TEST = "relayhistory_parity";
export const MODULE_FILTER = "relayhistory";

// burn #562's acceptance grep: none of these may remain in burn's crates/.
export const PARSER_SYMBOLS = /parse_claude_session|parse_codex_session|parse_opencode_session|notify::/;

// Secondary signal only; see the header.
export const READER_FILES = [
  "crates/relayburn-sdk/src/reader/claude.rs",
  "crates/relayburn-sdk/src/reader/codex.rs",
  "crates/relayburn-sdk/src/reader/opencode.rs",
];

// A change under any of these can change what burn observes.
export const RELEVANT_PATHS = [
  "crates/ai-hist/",
  "Cargo.toml",
  ".github/workflows/ci.yml",
  "scripts/burn-guardrails.mjs",
];

// ---------------------------------------------------------------- plumbing

export function defaultRun(cmd, args, options = {}) {
  const result = spawnSync(cmd, args, { encoding: "utf8", maxBuffer: 256 << 20, ...options });
  if (result.error) throw result.error;
  return { status: result.status, stdout: result.stdout ?? "", stderr: result.stderr ?? "" };
}

export function defaultIo() {
  return { run: defaultRun, env: process.env, log: (line) => console.log(line) };
}

export function parseArgs(argv) {
  const [command, ...rest] = argv;
  const options = {};
  for (let i = 0; i < rest.length; i += 2) {
    const key = rest[i];
    const value = rest[i + 1];
    if (!key?.startsWith("--") || value === undefined || value.startsWith("--")) {
      throw new Error(`expected --name value pairs, got ${JSON.stringify(rest.slice(i))}`);
    }
    options[key.slice(2)] = value;
  }
  return { command, options };
}

export function setOutput(io, name, value) {
  const file = io.env.GITHUB_OUTPUT;
  if (file) appendFileSync(file, `${name}=${value}\n`);
}

// GitHub annotations are one line; the message is plain prose.
function annotate(io, level, message) {
  io.log(`::${level}::${message.replace(/\r?\n/g, " ")}`);
}

function need(options, ...names) {
  for (const name of names) {
    if (!options[name]) throw new Error(`missing --${name}`);
  }
}

/** Relative paths of files under `root` for which `keep(rel)` holds. */
export function walkFiles(root, keep) {
  const out = [];
  const visit = (dir) => {
    let entries;
    try {
      entries = readdirSync(dir, { withFileTypes: true });
    } catch {
      return;
    }
    for (const entry of entries) {
      if (entry.name === "target" || entry.name === ".git" || entry.name === "node_modules") continue;
      const abs = path.join(dir, entry.name);
      if (entry.isDirectory()) visit(abs);
      else if (entry.isFile()) {
        const rel = path.relative(root, abs).split(path.sep).join("/");
        if (keep(rel)) out.push(rel);
      }
    }
  };
  visit(root);
  return out.sort();
}

function readFiles(root, rels) {
  return rels.map((rel) => ({ rel, text: readFileSync(path.join(root, rel), "utf8") }));
}

// --------------------------------------------------------- relevant-change

export function isRelevantChange(files) {
  return files.some((file) =>
    RELEVANT_PATHS.some((p) => (p.endsWith("/") ? file.startsWith(p) : file === p)),
  );
}

/**
 * The push event's `before` commit, present locally, or null when there is
 * none to diff against (a new branch, an unreadable payload, a failed fetch).
 */
export function pushBefore(io) {
  let before;
  try {
    before = JSON.parse(readFileSync(io.env.GITHUB_EVENT_PATH, "utf8")).before;
  } catch {
    return null;
  }
  if (typeof before !== "string" || !/^[0-9a-f]{40,64}$/.test(before) || /^0+$/.test(before)) return null;
  if (io.run("git", ["cat-file", "-e", `${before}^{commit}`]).status === 0) return before;
  const fetched = io.run("git", ["fetch", "--no-tags", "--depth=1", "origin", before]);
  return fetched.status === 0 ? before : null;
}

export function relevantChange(options, io) {
  const event = io.env.GITHUB_EVENT_NAME;
  if (event !== "pull_request" && event !== "push") {
    setOutput(io, "run", "true");
    io.log(`Event \`${event ?? "local"}\`: running the burn contract-drift check unconditionally.`);
    return 0;
  }
  // A pull_request checkout is the merge commit; its first parent is the
  // base branch tip, so HEAD^1..HEAD is exactly the PR's change. A push can
  // carry several commits, so it diffs from the event's `before` (the previous
  // tip of the branch), fetching it when the shallow checkout lacks it.
  let base = "HEAD^1";
  if (event === "push") {
    base = pushBefore(io);
    if (!base) {
      setOutput(io, "run", "true");
      annotate(io, "notice", "Could not determine the pushed range; running the burn contract-drift check.");
      return 0;
    }
  }
  const diff = io.run("git", ["diff", "--name-only", base, "HEAD"]);
  if (diff.status !== 0) {
    setOutput(io, "run", "true");
    annotate(io, "notice", `Could not diff against ${base}; running the burn contract-drift check.`);
    return 0;
  }
  const files = diff.stdout.split("\n").filter(Boolean);
  const relevant = isRelevantChange(files);
  setOutput(io, "run", relevant ? "true" : "false");
  if (!relevant) {
    annotate(
      io,
      "notice",
      `burn contract-drift skipped: nothing under ${RELEVANT_PATHS.join(", ")} changed.`,
    );
  }
  return 0;
}

// ------------------------------------------------------------ parity plan

/** Lib files whose module path contains `relayhistory` and hold tests. */
export function moduleParityFiles(files) {
  return files
    .filter(({ rel, text }) => rel.includes(MODULE_FILTER) && /#\[(?:[\w:]+::)?test\b/.test(text))
    .map(({ rel }) => rel);
}

/**
 * What to run, from `cargo metadata --no-deps` and the lib's relayhistory
 * test files. `status` is `absent` (no ai-hist dependency), `no-suite`
 * (dependency, nothing to run) or `ready`.
 */
export function parityPlan(metadata, moduleFiles = []) {
  const pkg = (metadata?.packages ?? []).find((p) => p.name === SDK_PACKAGE);
  if (!pkg) {
    return { status: "absent", reason: `burn has no \`${SDK_PACKAGE}\` package`, args: null };
  }
  const hasDep = (pkg.dependencies ?? []).some((d) => d.name === "ai-hist");
  if (!hasDep) {
    return {
      status: "absent",
      reason: `\`${SDK_PACKAGE}\` does not depend on \`ai-hist\` yet (burn #557)`,
      args: null,
    };
  }
  // After the cutover the feature may be folded away (burn #562); only
  // pass it while it exists.
  const features = Object.hasOwn(pkg.features ?? {}, PARITY_FEATURE) ? ["--features", PARITY_FEATURE] : [];
  const hasTarget = (pkg.targets ?? []).some(
    (t) => t.name === PARITY_TEST && (t.kind ?? []).includes("test"),
  );
  if (hasTarget) {
    return {
      status: "ready",
      mode: "target",
      args: ["test", "-p", SDK_PACKAGE, ...features, "--test", PARITY_TEST],
    };
  }
  if (moduleFiles.length > 0) {
    return {
      status: "ready",
      mode: "module",
      moduleFiles,
      args: ["test", "-p", SDK_PACKAGE, ...features, "--lib", "--", MODULE_FILTER],
    };
  }
  return {
    status: "no-suite",
    reason:
      `\`${SDK_PACKAGE}\` depends on \`ai-hist\` but has neither a \`${PARITY_TEST}\` test ` +
      `target nor tests under a \`${MODULE_FILTER}\` module path`,
    args: null,
  };
}

function cargoMetadata(burnDir, io) {
  const result = io.run("cargo", [
    "metadata",
    "--no-deps",
    "--format-version",
    "1",
    "--manifest-path",
    path.join(burnDir, "Cargo.toml"),
  ]);
  if (result.status !== 0) {
    throw new Error(`cargo metadata failed for ${burnDir}:\n${result.stderr}`);
  }
  return JSON.parse(result.stdout);
}

function planFor(burnDir, io) {
  const srcDir = path.join(burnDir, SDK_DIR, "src");
  const rels = walkFiles(srcDir, (rel) => rel.endsWith(".rs") && rel.includes(MODULE_FILTER));
  return parityPlan(cargoMetadata(burnDir, io), moduleParityFiles(readFiles(srcDir, rels)));
}

export function parityProbe(options, io) {
  need(options, "burn-dir");
  const plan = planFor(options["burn-dir"], io);
  setOutput(io, "ready", plan.status === "ready" ? "true" : "false");
  if (plan.status === "ready") {
    io.log(`burn parity suite found (${plan.mode}): cargo ${plan.args.join(" ")}`);
  } else if (plan.status === "absent") {
    annotate(
      io,
      "notice",
      `burn contract-drift check not armed: ${plan.reason}. It arms itself when burn depends on ai-hist.`,
    );
  } else {
    annotate(
      io,
      "warning",
      `burn contract-drift check has nothing to run: ${plan.reason}. burn #557 owes that suite; ` +
        "without it this check cannot see drift.",
    );
  }
  return 0;
}

// -------------------------------------------------------------- pin-ai-hist

function sectionName(line) {
  const m = /^\s*\[\[?\s*([^\]]+?)\s*\]\]?\s*(?:#.*)?$/.exec(line);
  return m ? m[1].replace(/\s+/g, "").replace(/"/g, "") : null;
}

/**
 * Rewrite every `ai-hist` requirement in one Cargo.toml to a path dependency
 * on `aiHistPath`, keeping its other keys (features, optional,
 * default-features). `workspace = true` entries are left to the workspace
 * root, which is rewritten the same way.
 */
export function rewriteAiHistRequirement(text, aiHistPath) {
  const pathValue = `path = ${JSON.stringify(aiHistPath)}`;
  let section = null;
  let changed = 0;
  const lines = text.split("\n").map((line) => {
    const name = sectionName(line);
    if (name !== null) {
      section = name;
      return line;
    }
    if (section === null) return line;
    const depTable = /(^|\.)(dev-|build-)?dependencies$/.test(section);
    const aiHistTable = /(^|\.)(dev-|build-)?dependencies\.ai-hist$/.test(section);
    if (aiHistTable) {
      const next = line.replace(/^(\s*)version\s*=\s*"[^"]*"/, `$1${pathValue}`);
      if (next !== line) changed += 1;
      return next;
    }
    if (!depTable) return line;
    const key = /^\s*("?)([\w-]+)\1\s*=\s*(.*)$/.exec(line);
    if (!key) return line;
    const [, , depName, value] = key;
    const isAiHist = depName === "ai-hist" || /package\s*=\s*"ai-hist"/.test(value);
    if (!isAiHist) return line;
    if (value.startsWith('"')) {
      changed += 1;
      return line.replace(/=\s*"[^"]*"/, `= { ${pathValue} }`);
    }
    if (/workspace\s*=\s*true/.test(value)) return line;
    const next = line.replace(/version\s*=\s*"[^"]*"/, pathValue);
    if (next !== line) changed += 1;
    return next;
  });
  return { text: lines.join("\n"), changed };
}

export function pinAiHist(options, io) {
  need(options, "burn-dir", "ai-hist-path");
  const burnDir = options["burn-dir"];
  const aiHistPath = path.resolve(options["ai-hist-path"]);
  const rewritten = [];
  for (const rel of walkFiles(burnDir, (r) => r === "Cargo.toml" || r.endsWith("/Cargo.toml"))) {
    const file = path.join(burnDir, rel);
    const { text, changed } = rewriteAiHistRequirement(readFileSync(file, "utf8"), aiHistPath);
    if (changed > 0) {
      writeFileSync(file, text);
      rewritten.push(rel);
    }
  }
  if (rewritten.length === 0) {
    annotate(
      io,
      "error",
      "Found no `ai-hist` requirement in burn's manifests to point at this checkout, though the probe " +
        "found one. The manifest uses a form rewriteAiHistRequirement does not know; extend it.",
    );
    return 1;
  }
  io.log(`Pointed ai-hist at ${aiHistPath} in: ${rewritten.join(", ")}`);
  return 0;
}

// --------------------------------------------------------- check-resolution

const SEMVER = String.raw`\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?`;
const TREE_LINE = new RegExp(String.raw`^ai-hist v(${SEMVER})(?: \(([^)]+)\))?(?: \(\*\))?$`);

/** Parse the first line of `cargo tree -i ai-hist --prefix none`. */
export function parseTreeRoot(stdout) {
  const first = stdout.split("\n").find((line) => line.trim() !== "");
  const m = first ? TREE_LINE.exec(first.trim()) : null;
  return m ? { version: m[1], source: m[2] ?? null } : null;
}

export function resolutionVerdict({ status, stdout, stderr }, expectedPath, samePath = (a, b) => a === b) {
  if (status !== 0) {
    if (/multiple .*ai-hist/i.test(stderr) || /There are multiple `ai-hist` packages/.test(stderr)) {
      return {
        ok: false,
        message:
          "burn's graph has two ai-hist packages: a requirement pin-ai-hist did not rewrite still pulls " +
          `one from the registry. cargo: ${stderr.trim()}`,
      };
    }
    return { ok: false, message: `cargo tree -i ai-hist failed in burn: ${stderr.trim()}` };
  }
  const root = parseTreeRoot(stdout);
  if (!root) {
    return { ok: false, message: `unrecognised cargo tree output: ${stdout.split("\n")[0]}` };
  }
  if (root.source === null) {
    return {
      ok: false,
      message:
        `burn resolved ai-hist v${root.version} from the registry, not this checkout. ` +
        "The requirement rewrite did not take effect; see the pin-ai-hist step.",
    };
  }
  if (!samePath(root.source, expectedPath)) {
    return {
      ok: false,
      message: `burn resolved ai-hist v${root.version} from ${root.source}, not this checkout (${expectedPath}).`,
    };
  }
  return { ok: true, message: `burn resolves ai-hist v${root.version} from ${root.source}.` };
}

function realSame(a, b) {
  try {
    return realpathSync(a) === realpathSync(b);
  } catch {
    return path.resolve(a) === path.resolve(b);
  }
}

export function checkResolution(options, io) {
  need(options, "burn-dir", "ai-hist-path");
  const burnDir = options["burn-dir"];
  const plan = planFor(burnDir, io);
  const features = plan.args?.includes("--features") ? ["--features", PARITY_FEATURE] : [];
  const tree = io.run(
    "cargo",
    ["tree", "-p", SDK_PACKAGE, ...features, "-i", "ai-hist", "--prefix", "none"],
    { cwd: burnDir },
  );
  const verdict = resolutionVerdict(tree, path.resolve(options["ai-hist-path"]), realSame);
  if (!verdict.ok) {
    annotate(io, "error", verdict.message);
    return 1;
  }
  io.log(verdict.message);
  return 0;
}

// --------------------------------------------------------------- run-parity

/** Tests that ran (passed + failed) across every `test result:` line. */
export function countTestsRun(stdout) {
  let ran = 0;
  for (const m of stdout.matchAll(/^test result: \w+\. (\d+) passed; (\d+) failed;/gm)) {
    ran += Number(m[1]) + Number(m[2]);
  }
  return ran;
}

export function runParity(options, io) {
  need(options, "burn-dir");
  const burnDir = options["burn-dir"];
  const plan = planFor(burnDir, io);
  if (plan.status !== "ready") {
    annotate(io, "error", `run-parity called but burn has no parity suite: ${plan.reason}.`);
    return 1;
  }
  io.log(`cargo ${plan.args.join(" ")}`);
  const result = io.run("cargo", plan.args, { cwd: burnDir, stdio: ["ignore", "pipe", "inherit"] });
  io.log(result.stdout);
  if (result.status !== 0) {
    annotate(io, "error", "burn's relayhistory parity suite fails against this checkout; see the test output above.");
    return 1;
  }
  const ran = countTestsRun(result.stdout);
  if (ran === 0) {
    annotate(
      io,
      "error",
      `burn's parity suite ran zero tests (cargo ${plan.args.join(" ")}); the filter or target no longer matches.`,
    );
    return 1;
  }
  io.log(`burn parity suite: ${ran} tests ran against this checkout.`);
  return 0;
}

// ----------------------------------------------------------------- tripwire

/** Lines in burn's crates/ that match burn #562's acceptance grep. */
export function scanParserSymbols(files) {
  const hits = [];
  for (const { rel, text } of files) {
    text.split("\n").forEach((line, i) => {
      if (PARSER_SYMBOLS.test(line)) hits.push(`${rel}:${i + 1}: ${line.trim()}`);
    });
  }
  return hits;
}

export function tagExists(io, repo, tag) {
  // --exit-code: 0 when the ref matched, 2 when nothing matched. Anything
  // else is a transport failure, and a tripwire that cannot see burn must
  // not report green.
  const result = io.run("git", ["ls-remote", "--exit-code", "--tags", repo, `refs/tags/${tag}`]);
  if (result.status === 0) return true;
  if (result.status === 2) return false;
  throw new Error(`git ls-remote ${repo} failed with status ${result.status}: ${result.stderr.trim()}`);
}

/**
 * `inactive` before the cutover tag exists. After it: `fail` on parser
 * symbols or a missing parity suite, `warn` on leftover reader file names
 * alone, `pass` otherwise.
 */
export function tripwireVerdict({ tag, tagExists: tagged, symbols, readerFiles, plan }) {
  if (!tagged) {
    return {
      status: "inactive",
      message:
        `burn has no \`${tag}\` tag yet, so its harness parsers are still expected ` +
        `(${symbols.length} matching lines in crates/). The tripwire arms itself once burn's ` +
        "cutover release (burn #562) is tagged.",
    };
  }
  const failures = [];
  if (symbols.length > 0) {
    const shown = symbols.slice(0, 10).join("; ");
    failures.push(
      `burn's crates/ still contain harness-parser symbols after \`${tag}\` ` +
        `(${symbols.length} lines, e.g. ${shown}). relayhistory is the single owner of session ` +
        "parsing (docs/decisions/2026-09-19-relayhistory-owns-session-sourcing.md).",
    );
  }
  if (plan.status !== "ready") {
    failures.push(
      `burn has no relayhistory parity suite after \`${tag}\`: ${plan.reason}. Without it the ` +
        "contract-drift job in relayhistory CI cannot see drift.",
    );
  }
  if (failures.length > 0) return { status: "fail", message: failures.join(" ") };
  if (readerFiles.length > 0) {
    return {
      status: "warn",
      message:
        `No parser symbols remain, but these reader files still exist after \`${tag}\`: ` +
        `${readerFiles.join(", ")}. Check they hold no log parsing.`,
    };
  }
  return { status: "pass", message: `burn has no harness parsers after \`${tag}\`, and a parity suite.` };
}

export function tripwire(options, io) {
  need(options, "burn-dir");
  const burnDir = options["burn-dir"];
  if (!existsSync(path.join(burnDir, SDK_DIR))) {
    throw new Error(`${burnDir} is not a burn checkout (no ${SDK_DIR})`);
  }
  const tag = options["cutover-tag"] || DEFAULT_CUTOVER_TAG;
  const cratesDir = path.join(burnDir, "crates");
  const symbols = scanParserSymbols(
    readFiles(cratesDir, walkFiles(cratesDir, (rel) => rel.endsWith(".rs"))),
  ).map((hit) => `crates/${hit}`);
  const verdict = tripwireVerdict({
    tag,
    tagExists: tagExists(io, options.repo || BURN_REPO_URL, tag),
    symbols,
    readerFiles: READER_FILES.filter((file) => existsSync(path.join(burnDir, file))),
    plan: planFor(burnDir, io),
  });
  const level = { fail: "error", warn: "warning", inactive: "notice" }[verdict.status];
  if (level) annotate(io, level, verdict.message);
  else io.log(verdict.message);
  return verdict.status === "fail" ? 1 : 0;
}

// --------------------------------------------------------------------- main

export const COMMANDS = {
  "relevant-change": relevantChange,
  "parity-probe": parityProbe,
  "pin-ai-hist": pinAiHist,
  "check-resolution": checkResolution,
  "run-parity": runParity,
  tripwire,
};

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const { command, options } = parseArgs(process.argv.slice(2));
  const handler = COMMANDS[command];
  if (!handler) {
    console.error(`usage: burn-guardrails.mjs <${Object.keys(COMMANDS).join("|")}> [--name value ...]`);
    process.exit(2);
  }
  process.exitCode = handler(options, defaultIo());
}

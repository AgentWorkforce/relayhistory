#!/usr/bin/env node
// Cross-repo guardrails between relayhistory and its downstream consumer
// AgentWorkforce/burn. Two commands, one per workflow:
//
//   parity-probe --burn-dir <dir>
//     Used by the `burn-contract-drift` job in ci.yml (#183). Reads burn's
//     `cargo metadata --no-deps` and decides whether burn's relayhistory
//     parity suite exists yet: a `relayburn-sdk` package with a
//     `relayhistory-source` feature, an `ai-hist` dependency, and a test
//     target named `relayhistory_parity` (burn #557). Writes `ready=true` or
//     `ready=false` to $GITHUB_OUTPUT and always exits 0 -- a missing suite
//     is a notice, not a failure, so the job does not fail every PR before
//     burn lands it. Once it exists the job runs it and becomes a real gate.
//
//   tripwire --burn-dir <dir> --cutover-tag <tag> [--repo <git url>]
//     Used by .github/workflows/burn-reader-tripwire.yml (#184). Until burn's
//     cutover release tag exists it prints a notice and exits 0. After it
//     exists, it fails if burn main's harness readers have reappeared
//     (`reader/{claude,codex,opencode}.rs` under crates/relayburn-sdk/src/):
//     relayhistory is the single owner of session parsing, and a second
//     parser growing back in burn is the regression this catches.
//
// The decision functions are exported pure so burn-guardrails.test.mjs can
// cover them without cargo, git or the network.

import { spawnSync } from "node:child_process";
import { appendFileSync, existsSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

export const BURN_REPO_URL = "https://github.com/AgentWorkforce/burn.git";

// burn #562 ships the cutover as the 5.0.0 major. burn tags each crate
// release separately; the readers live in relayburn-sdk, so its tag is the
// one that means "the readers were deleted in a release".
export const DEFAULT_CUTOVER_TAG = "relayburn-sdk-v5.0.0";

export const SDK_PACKAGE = "relayburn-sdk";
export const PARITY_FEATURE = "relayhistory-source";
export const PARITY_TEST = "relayhistory_parity";

export const READER_FILES = [
  "crates/relayburn-sdk/src/reader/claude.rs",
  "crates/relayburn-sdk/src/reader/codex.rs",
  "crates/relayburn-sdk/src/reader/opencode.rs",
];

/**
 * Decide from `cargo metadata --no-deps` output whether burn's parity suite
 * can be run as `cargo test -p relayburn-sdk --features relayhistory-source
 * --test relayhistory_parity`.
 */
export function parityReadiness(metadata) {
  const pkg = (metadata?.packages ?? []).find((p) => p.name === SDK_PACKAGE);
  if (!pkg) {
    return { ready: false, missing: [`package \`${SDK_PACKAGE}\``] };
  }
  const missing = [];
  if (!Object.hasOwn(pkg.features ?? {}, PARITY_FEATURE)) {
    missing.push(`feature \`${PARITY_FEATURE}\` in ${SDK_PACKAGE}`);
  }
  if (!(pkg.dependencies ?? []).some((d) => d.name === "ai-hist")) {
    missing.push(`an \`ai-hist\` dependency in ${SDK_PACKAGE}`);
  }
  const hasTest = (pkg.targets ?? []).some(
    (t) => t.name === PARITY_TEST && (t.kind ?? []).includes("test"),
  );
  if (!hasTest) {
    missing.push(`test target \`${PARITY_TEST}\` in ${SDK_PACKAGE}`);
  }
  return { ready: missing.length === 0, missing };
}

/** The reader files present under a burn checkout. */
export function reappearedReaders(burnDir, exists = existsSync) {
  return READER_FILES.filter((file) => exists(path.join(burnDir, file)));
}

/**
 * `inactive` before the cutover tag exists, `pass` or `fail` after.
 */
export function tripwireVerdict({ tag, tagExists, present }) {
  if (!tagExists) {
    return {
      status: "inactive",
      message:
        `burn has no \`${tag}\` tag yet, so its harness readers are still expected ` +
        `(${present.length} of ${READER_FILES.length} present). The tripwire arms ` +
        "itself once burn's cutover release (burn #562) is tagged.",
    };
  }
  if (present.length > 0) {
    return {
      status: "fail",
      message:
        `burn main contains harness readers after its cutover release \`${tag}\`: ` +
        `${present.join(", ")}. relayhistory is the single owner of session parsing ` +
        "(docs/decisions/2026-09-19-relayhistory-owns-session-sourcing.md); a " +
        "parser belongs here, and burn reads evidence through ai-hist's SessionStore.",
    };
  }
  return {
    status: "pass",
    message: `burn main has no harness readers after \`${tag}\`.`,
  };
}

function parseArgs(argv) {
  const [command, ...rest] = argv;
  const options = {};
  for (let i = 0; i < rest.length; i += 2) {
    const key = rest[i];
    const value = rest[i + 1];
    if (!key?.startsWith("--") || value === undefined) {
      throw new Error(`expected --name value pairs, got ${JSON.stringify(rest.slice(i))}`);
    }
    options[key.slice(2)] = value;
  }
  return { command, options };
}

function setOutput(name, value) {
  if (process.env.GITHUB_OUTPUT) {
    appendFileSync(process.env.GITHUB_OUTPUT, `${name}=${value}\n`);
  }
}

// GitHub annotations are one line; the message is plain prose.
function annotate(level, message) {
  console.log(`::${level}::${message}`);
}

function run(cmd, args, options = {}) {
  const result = spawnSync(cmd, args, { encoding: "utf8", maxBuffer: 64 << 20, ...options });
  if (result.error) throw result.error;
  return result;
}

function parityProbe({ "burn-dir": burnDir }) {
  if (!burnDir) throw new Error("parity-probe needs --burn-dir");
  const result = run("cargo", [
    "metadata",
    "--no-deps",
    "--format-version",
    "1",
    "--manifest-path",
    path.join(burnDir, "Cargo.toml"),
  ]);
  if (result.status !== 0) {
    process.stderr.write(result.stderr);
    throw new Error(`cargo metadata failed for ${burnDir}`);
  }
  const { ready, missing } = parityReadiness(JSON.parse(result.stdout));
  setOutput("ready", ready ? "true" : "false");
  if (ready) {
    console.log(`burn has the relayhistory parity suite; running it against this checkout.`);
  } else {
    annotate(
      "notice",
      `Contract-drift check skipped: burn main does not have its relayhistory parity suite yet ` +
        `(missing ${missing.join("; ")}; burn #557). The job runs it automatically once it lands.`,
    );
  }
}

function tagExists(repo, tag) {
  // --exit-code: 0 when the ref matched, 2 when nothing matched. Anything
  // else is a transport failure, and a tripwire that cannot see burn must
  // not report green.
  const result = run("git", ["ls-remote", "--exit-code", "--tags", repo, `refs/tags/${tag}`]);
  if (result.status === 0) return true;
  if (result.status === 2) return false;
  process.stderr.write(result.stderr);
  throw new Error(`git ls-remote ${repo} failed with status ${result.status}`);
}

function tripwire({ "burn-dir": burnDir, "cutover-tag": tag, repo = BURN_REPO_URL }) {
  if (!burnDir) throw new Error("tripwire needs --burn-dir");
  if (!existsSync(path.join(burnDir, "crates/relayburn-sdk"))) {
    throw new Error(`${burnDir} is not a burn checkout (no crates/relayburn-sdk)`);
  }
  const cutoverTag = tag || DEFAULT_CUTOVER_TAG;
  const verdict = tripwireVerdict({
    tag: cutoverTag,
    tagExists: tagExists(repo, cutoverTag),
    present: reappearedReaders(burnDir),
  });
  if (verdict.status === "fail") {
    annotate("error", verdict.message);
    process.exitCode = 1;
  } else if (verdict.status === "inactive") {
    annotate("notice", verdict.message);
  } else {
    console.log(verdict.message);
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const { command, options } = parseArgs(process.argv.slice(2));
  if (command === "parity-probe") parityProbe(options);
  else if (command === "tripwire") tripwire(options);
  else {
    console.error("usage: burn-guardrails.mjs parity-probe --burn-dir <dir>");
    console.error("       burn-guardrails.mjs tripwire --burn-dir <dir> [--cutover-tag <tag>] [--repo <url>]");
    process.exit(2);
  }
}

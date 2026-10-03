#!/usr/bin/env node
/**
 * Cut the curated `[Unreleased]` block of CHANGELOG.md into a released
 * heading, then restore a bare `[Unreleased]`.
 *
 * Curated entries are authoritative (see AGENTS.md "Changelog"). Commit
 * subjects since the previous release tag are only a fallback when nothing is
 * pending, and the release fails if they imply a larger bump than `--version`.
 * A release with neither still gets a heading, so the next release compares
 * against it. Run from the repository root.
 *
 * With `--released-from <sha> --pending-since <sha>`, persist a cut made at
 * <released-from> onto a branch that moved since the release's source tree
 * <pending-since>: entries the branch gained meanwhile stay pending.
 *
 * Usage:
 *   node scripts/cut-changelog.mjs --version 0.35.0 [--date 2026-10-03] [--dry-run]
 *   node scripts/cut-changelog.mjs --version 0.35.0 --released-from <sha> --pending-since <sha>
 */

import { execFileSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import {
  TAG_PREFIX,
  bodyFromCommitSubjects,
  carryReleaseCut,
  cutChangelog,
  impliedLevel,
  latestReleasedVersion,
  levelAtLeast,
  releaseLevel,
  updateComparisonReferences,
} from "./release-changelog.mjs";

const FILE = "CHANGELOG.md";
const args = process.argv.slice(2);
const dryRun = args.includes("--dry-run");
const flag = (name) => {
  const index = args.indexOf(`--${name}`);
  return index === -1 ? undefined : args[index + 1];
};

const version = flag("version");
if (!version || !/^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$/.test(version)) {
  console.error("usage: cut-changelog.mjs --version <x.y.z> [--date <yyyy-mm-dd>] [--dry-run]");
  process.exit(1);
}
const date = flag("date") ?? new Date().toISOString().slice(0, 10);
if (
  !/^\d{4}-\d{2}-\d{2}$/.test(date) ||
  Number.isNaN(Date.parse(`${date}T00:00:00Z`)) ||
  new Date(`${date}T00:00:00Z`).toISOString().slice(0, 10) !== date
) {
  console.error(`invalid --date: ${date}`);
  process.exit(1);
}

const warn = (message) =>
  console.warn(process.env.GITHUB_ACTIONS ? `::warning::${message}` : `warning: ${message}`);

/** Conventional commit subjects since the previous release tag, if reachable. */
function commitSubjects(previousVersion) {
  const tag = `${TAG_PREFIX}${previousVersion}`;
  try {
    return execFileSync("git", ["log", `${tag}..HEAD`, "--no-merges", "--pretty=format:%s"], {
      encoding: "utf8",
      stdio: ["ignore", "pipe", "ignore"],
    }).split("\n");
  } catch {
    warn(`${tag} is not reachable; commit-subject fallback disabled`);
    return [];
  }
}

const releasedFrom = flag("released-from");
const pendingSince = flag("pending-since");
if (Boolean(releasedFrom) !== Boolean(pendingSince)) {
  console.error("--released-from and --pending-since go together");
  process.exit(1);
}

const original = readFileSync(FILE, "utf8");

if (releasedFrom) {
  const show = (sha) => execFileSync("git", ["show", `${sha}:${FILE}`], { encoding: "utf8" });
  const updated = carryReleaseCut(original, {
    version,
    released: show(releasedFrom),
    start: show(pendingSince),
  });
  if (!dryRun) writeFileSync(FILE, updated);
  console.log(`${FILE}: carried the [${version}] cut; entries added since ${pendingSince.slice(0, 12)} stay pending`);
  process.exit(0);
}

const previousVersion = latestReleasedVersion(original);
if (previousVersion === version) {
  console.log(`${FILE}: already cut for ${version}`);
  process.exit(0);
}

const fallback = previousVersion ? bodyFromCommitSubjects(commitSubjects(previousVersion)) : "";
const result = cutChangelog(original, {
  version,
  date,
  fallback: fallback || "No user-facing changes.",
});

if (previousVersion) {
  const actual = releaseLevel(previousVersion, version);
  if (!result.curated && fallback) {
    // Nothing curated, so the commit subjects are the release notes, and
    // check-release-changelog.mjs had no level to hold the version to.
    const implied = impliedLevel(fallback, previousVersion);
    if (!levelAtLeast(actual, implied)) {
      console.error(
        `${FILE}: commits since ${TAG_PREFIX}${previousVersion} need a ${implied} release, but ${version} is ${actual}; curate [Unreleased] or release a larger bump`,
      );
      process.exit(1);
    }
  }
  if (result.level && !levelAtLeast(actual, result.level)) {
    warn(`${FILE} pending entries are marked ${result.level} but ${version} is a ${actual} bump`);
  }
}

const updated = previousVersion
  ? updateComparisonReferences(result.changelog, { version, previousVersion })
  : result.changelog;
if (!dryRun) writeFileSync(FILE, updated);
const source = result.curated ? "curated" : fallback ? "from commit subjects" : "no user-facing changes";
console.log(`${FILE}: cut [${version}] - ${date} (${source})${dryRun ? " (dry run)" : ""}`);

#!/usr/bin/env node
/**
 * Cut the curated `[Unreleased]` block of CHANGELOG.md into a released
 * heading, then restore a bare `[Unreleased]`.
 *
 * Curated entries are authoritative (see AGENTS.md "Changelog"). Commit
 * subjects since the previous release tag are only a fallback when nothing is
 * pending. Run from the repository root.
 *
 * Usage:
 *   node scripts/cut-changelog.mjs --version 0.35.0 [--date 2026-10-03] [--dry-run]
 */

import { execFileSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import {
  TAG_PREFIX,
  bodyFromCommitSubjects,
  cutChangelog,
  latestReleasedVersion,
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
if (!version || !/^\d+\.\d+\.\d+$/.test(version)) {
  console.error("usage: cut-changelog.mjs --version <x.y.z> [--date <yyyy-mm-dd>] [--dry-run]");
  process.exit(1);
}
const date = flag("date") ?? new Date().toISOString().slice(0, 10);
if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
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

const original = readFileSync(FILE, "utf8");
const previousVersion = latestReleasedVersion(original);
if (previousVersion === version) {
  console.log(`${FILE}: already cut for ${version}`);
  process.exit(0);
}

const result = cutChangelog(original, {
  version,
  date,
  fallback: previousVersion ? bodyFromCommitSubjects(commitSubjects(previousVersion)) : "",
});
if (!result.cut) {
  warn(`${FILE}: nothing pending and no conventional commits since ${previousVersion}; left unchanged`);
  process.exit(0);
}

let updated = result.changelog;
if (previousVersion) {
  updated = updateComparisonReferences(updated, { version, previousVersion });
  // The pending heading records the SemVer impact of what is being released.
  // check-release-changelog.mjs rejects a smaller bump before publishing; this
  // only surfaces it when the cut runs on its own.
  const rank = { Patch: 0, Minor: 1, Major: 2 };
  const actual = releaseLevel(previousVersion, version);
  if (result.level && rank[result.level] > rank[actual]) {
    warn(`${FILE} pending entries are marked ${result.level} but ${version} is a ${actual} bump`);
  }
}

if (!dryRun) writeFileSync(FILE, updated);
console.log(
  `${FILE}: cut [${version}] - ${date} (${result.curated ? "curated" : "from commit subjects"})${dryRun ? " (dry run)" : ""}`,
);

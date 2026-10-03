#!/usr/bin/env node
// Fail a release whose version is smaller than the pending changelog level,
// or whose pending entries do not declare one. Run from the repository root.
//
// Usage: node scripts/check-release-changelog.mjs --version <x.y.z>

import { readFileSync } from "node:fs";
import { assertChangelogSemver } from "./release-changelog.mjs";

const args = process.argv.slice(2);
const index = args.indexOf("--version");
const version = index === -1 ? undefined : args[index + 1];
if (!version) {
  console.error("usage: check-release-changelog.mjs --version <x.y.z>");
  process.exit(2);
}

try {
  const result = assertChangelogSemver(readFileSync("CHANGELOG.md", "utf8"), version);
  console.log(
    `changelog ok: ${result.latestVersion} -> ${version} (${result.actualLevel}; pending ${result.pendingLevel ?? "none"})`,
  );
} catch (error) {
  console.error(error.message);
  process.exit(1);
}

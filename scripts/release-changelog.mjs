// The changelog half of the release contract. `CHANGELOG.md` curates pending
// entries under `[Unreleased - Patch|Minor|Major]`; the publish workflow checks
// that the release version is at least that level (check-release-changelog.mjs)
// and then moves the entries under the released version (cut-changelog.mjs).
// See AGENTS.md "Changelog".

export const REPOSITORY = "https://github.com/AgentWorkforce/relayhistory";
export const TAG_PREFIX = "sdk-ts-v";

const LEVEL_RANK = { Patch: 0, Minor: 1, Major: 2 };
const SEMVER = /^(\d+)\.(\d+)\.(\d+)(?:-([0-9A-Za-z.-]+))?$/;
export const UNRELEASED =
  /^## \[Unreleased(?: - (Patch|Minor|Major))?\][ \t]*\n([\s\S]*?)(?=^## \[|^\[[^\]]+\]:\s|(?![\s\S]))/m;
const RELEASED = /^## \[(\d+\.\d+\.\d+)\]/m;

const SECTION_BY_TYPE = new Map([
  ["feat", "Added"],
  ["fix", "Fixed"],
  ["perf", "Changed"],
  ["revert", "Changed"],
  ["deprecate", "Deprecated"],
  ["deprecated", "Deprecated"],
  ["remove", "Removed"],
  ["removed", "Removed"],
  ["security", "Security"],
]);

const SECTION_ORDER = [
  "Breaking Changes",
  "Added",
  "Changed",
  "Deprecated",
  "Removed",
  "Fixed",
  "Security",
];

export function parseVersion(value) {
  const match = SEMVER.exec(value);
  if (!match) throw new Error(`Invalid release version: ${value}`);
  return {
    major: Number(match[1]),
    minor: Number(match[2]),
    patch: Number(match[3]),
    prerelease: match[4],
  };
}

export function releaseLevel(fromVersion, toVersion) {
  const from = parseVersion(fromVersion);
  const to = parseVersion(toVersion);
  const order =
    to.major - from.major || to.minor - from.minor || to.patch - from.patch;
  if (order <= 0) {
    throw new Error(
      `Release target ${toVersion} must be newer than ${fromVersion}`,
    );
  }
  if (to.major !== from.major) return "Major";
  if (to.minor !== from.minor) return "Minor";
  return "Patch";
}

/** The newest released version heading, or undefined. */
export function latestReleasedVersion(changelog) {
  return RELEASED.exec(changelog)?.[1];
}

/**
 * Validate the pending block against the version about to be released: a
 * non-empty block must declare its level, and the release must be at least
 * that level. A changelog already cut for `targetVersion` must have nothing
 * left pending.
 */
export function assertChangelogSemver(changelog, targetVersion) {
  const pending = UNRELEASED.exec(changelog);
  if (!pending) throw new Error("CHANGELOG.md has no [Unreleased] heading");

  const pendingLevel = pending[1];
  const pendingBody = pending[2].trim();
  if (pendingBody && !pendingLevel) {
    throw new Error(
      "Non-empty [Unreleased] must declare Patch, Minor, or Major",
    );
  }

  const latest = latestReleasedVersion(changelog);
  if (!latest) throw new Error("CHANGELOG.md has no stable release heading");
  if (latest === targetVersion) {
    if (pendingBody) {
      throw new Error(
        `CHANGELOG.md already contains ${targetVersion} but still has pending entries`,
      );
    }
    return { latestVersion: latest, pendingLevel, actualLevel: "Released" };
  }
  const actualLevel = releaseLevel(latest, targetVersion);
  if (pendingLevel && LEVEL_RANK[actualLevel] < LEVEL_RANK[pendingLevel]) {
    throw new Error(
      `CHANGELOG.md requires a ${pendingLevel} release, but ${latest} -> ${targetVersion} is ${actualLevel}`,
    );
  }
  return { latestVersion: latest, pendingLevel, actualLevel };
}

/** Commit subjects grouped into Keep a Changelog sections. */
export function bodyFromCommitSubjects(subjects) {
  const sections = new Map(SECTION_ORDER.map((section) => [section, []]));

  for (const subject of subjects) {
    const parsed = subject
      .trim()
      .match(/^([a-z]+)(?:\(([^)]*)\))?(!)?:\s*(.+)$/i);
    if (!parsed) continue;
    const [, typeRaw, scope = "", bang, titleRaw] = parsed;
    const type = typeRaw.toLowerCase();
    if (type === "chore" && /^release\b/i.test(titleRaw)) continue;
    if (type === "chore" && scope.toLowerCase() === "release") continue;

    const section = bang ? "Breaking Changes" : SECTION_BY_TYPE.get(type);
    // chore/docs/ci/test/build/style/refactor are not part of the release
    // narrative.
    if (!section) continue;

    const title = titleRaw
      .replace(/\s*\(#\d+[^)]*\)/g, "")
      .replace(/\s+/g, " ")
      .trim();
    if (!title) continue;

    const entry = `${title.charAt(0).toUpperCase()}${title.slice(1)}`;
    const entries = sections.get(section);
    if (!entries.includes(entry)) entries.push(entry);
  }

  const lines = [];
  for (const section of SECTION_ORDER) {
    const entries = sections.get(section);
    if (entries.length === 0) continue;
    lines.push(`### ${section}`, "");
    for (const entry of entries) lines.push(`- ${entry}`);
    lines.push("");
  }
  return lines.join("\n").trimEnd();
}

/**
 * Move the pending entries (or `fallback` when none are curated) under
 * `## [version] - date` and restore a bare `## [Unreleased]`.
 * @returns {{ changelog: string, cut: boolean, curated: boolean, level: string | null }}
 */
export function cutChangelog(changelog, { version, date, fallback = "" }) {
  const match = UNRELEASED.exec(changelog);
  if (!match) throw new Error("CHANGELOG.md has no [Unreleased] heading");

  const curated = match[2].trim();
  const body = curated || fallback.trim();
  if (!body) return { changelog, cut: false, curated: false, level: null };

  const start = match.index;
  const end = start + match[0].length;
  return {
    changelog:
      changelog.slice(0, start) +
      `## [Unreleased]\n\n## [${version}] - ${date}\n\n${body}\n\n` +
      changelog.slice(end),
    cut: true,
    curated: Boolean(curated),
    level: match[1] ?? null,
  };
}

/**
 * Point `[Unreleased]` at the new release tag and add the new version's
 * comparison link above the existing ones.
 */
export function updateComparisonReferences(
  changelog,
  { version, previousVersion },
) {
  parseVersion(version);
  parseVersion(previousVersion);
  const escaped = version.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const definitions = [
    `[Unreleased]: ${REPOSITORY}/compare/${TAG_PREFIX}${version}...HEAD`,
    `[${version}]: ${REPOSITORY}/compare/${TAG_PREFIX}${previousVersion}...${TAG_PREFIX}${version}`,
  ];

  const kept = changelog.split("\n").filter((line) => {
    if (/^\[Unreleased(?: - (?:Patch|Minor|Major))?\]:/.test(line)) return false;
    return !new RegExp(`^\\[${escaped}\\]:`).test(line);
  });
  const firstDefinition = kept.findIndex((line) => /^\[[^\]]+\]:\s+\S+/.test(line));
  kept.splice(firstDefinition === -1 ? kept.length : firstDefinition, 0, ...definitions);
  return `${kept.join("\n").replace(/\n*$/, "")}\n`;
}

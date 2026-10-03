import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { describe, it } from "node:test";
import { fileURLToPath } from "node:url";
import {
  assertChangelogSemver,
  bodyFromCommitSubjects,
  carryReleaseCut,
  cutChangelog,
  impliedLevel,
  updateComparisonReferences,
} from "./release-changelog.mjs";

const scripts = dirname(fileURLToPath(import.meta.url));
const REPO = "https://github.com/AgentWorkforce/relayhistory";

function changelog(level = "Minor", body = "### Added\n\n- Feature") {
  const suffix = level ? ` - ${level}` : "";
  return `# Changelog\n\n## [Unreleased${suffix}]\n\n${body}\n\n## [0.34.1] - 2026-10-03\n\n### Fixed\n\n- Previous\n\n[Unreleased]: ${REPO}/compare/sdk-ts-v0.34.1...HEAD\n[0.34.1]: ${REPO}/compare/sdk-ts-v0.34.0...sdk-ts-v0.34.1\n`;
}

describe("assertChangelogSemver", () => {
  it("accepts a release at or above the pending level", () => {
    assert.equal(assertChangelogSemver(changelog(), "0.35.0").actualLevel, "Minor");
    assert.equal(assertChangelogSemver(changelog("Patch"), "1.0.0").actualLevel, "Major");
  });

  it("rejects a release smaller than the pending level", () => {
    assert.throws(
      () => assertChangelogSemver(changelog(), "0.34.2"),
      /requires a Minor release, but 0.34.1 -> 0.34.2 is Patch/,
    );
  });

  it("rejects pending entries without a level", () => {
    assert.throws(() => assertChangelogSemver(changelog(null), "0.34.2"), /must declare/);
  });

  it("accepts an empty pending block at any level", () => {
    assert.equal(assertChangelogSemver(changelog(null, ""), "0.34.2").actualLevel, "Patch");
  });

  it("accepts a changelog already cut for the target", () => {
    const cut = cutChangelog(changelog(), { version: "0.35.0", date: "2026-10-04" });
    assert.equal(assertChangelogSemver(cut.changelog, "0.35.0").actualLevel, "Released");
  });

  it("rejects a version with leading zeros", () => {
    assert.throws(() => assertChangelogSemver(changelog(), "0.35.00"), /Invalid release version/);
  });

  it("rejects a target that is not newer", () => {
    assert.throws(() => assertChangelogSemver(changelog(), "0.34.0"), /must be newer/);
  });
});

describe("cutChangelog", () => {
  it("moves curated entries under the release and restores a bare heading", () => {
    const result = cutChangelog(changelog(), { version: "0.35.0", date: "2026-10-04" });
    assert.equal(result.cut, true);
    assert.equal(result.curated, true);
    assert.equal(result.level, "Minor");
    assert.match(
      result.changelog,
      /## \[Unreleased\]\n\n## \[0\.35\.0\] - 2026-10-04\n\n### Added\n\n- Feature\n\n## \[0\.34\.1\]/,
    );
  });

  it("uses the fallback only when nothing is curated", () => {
    const empty = changelog(null, "");
    assert.equal(cutChangelog(empty, { version: "0.34.2", date: "2026-10-04" }).cut, false);
    const result = cutChangelog(empty, {
      version: "0.34.2",
      date: "2026-10-04",
      fallback: "### Fixed\n\n- From commits",
    });
    assert.equal(result.curated, false);
    assert.match(result.changelog, /## \[0\.34\.2\] - 2026-10-04\n\n### Fixed\n\n- From commits/);
  });

  it("does not swallow link definitions into an empty pending block", () => {
    const bare = `# Changelog\n\n## [Unreleased]\n\n[Unreleased]: ${REPO}/compare/sdk-ts-v0.1.0...HEAD\n`;
    assert.equal(cutChangelog(bare, { version: "0.1.1", date: "2026-10-04" }).cut, false);
  });
});

describe("bodyFromCommitSubjects", () => {
  it("groups conventional subjects and drops release and housekeeping commits", () => {
    assert.equal(
      bodyFromCommitSubjects([
        "feat(sdk): add searchPage (#67)",
        "fix: keep cursor order",
        "feat!: drop delivery",
        "chore: release 0.34.1",
        "docs: tidy",
        "Merge pull request #1",
      ]),
      "### Breaking Changes\n\n- Drop delivery\n\n### Added\n\n- Add searchPage\n\n### Fixed\n\n- Keep cursor order",
    );
  });
});

describe("updateComparisonReferences", () => {
  it("repoints [Unreleased] and adds the new release link", () => {
    const updated = updateComparisonReferences(changelog(), {
      version: "0.35.0",
      previousVersion: "0.34.1",
    });
    assert.match(
      updated,
      new RegExp(
        `\\[Unreleased\\]: ${REPO}/compare/sdk-ts-v0\\.35\\.0\\.\\.\\.HEAD\\n` +
          `\\[0\\.35\\.0\\]: ${REPO}/compare/sdk-ts-v0\\.34\\.1\\.\\.\\.sdk-ts-v0\\.35\\.0\\n` +
          `\\[0\\.34\\.1\\]:`,
      ),
    );
    assert.equal(updated.match(/^\[Unreleased\]:/gm).length, 1);
  });
});

function cutCli(dir, version) {
  return spawnSync(
    process.execPath,
    [join(scripts, "cut-changelog.mjs"), "--version", version, "--date", "2026-10-04"],
    { cwd: dir, encoding: "utf8" },
  );
}

function repoWithCommits(subjects) {
  const dir = mkdtempSync(join(tmpdir(), "relayhistory-changelog-git-"));
  const git = (...args) => {
    const result = spawnSync("git", ["-C", dir, ...args], { encoding: "utf8" });
    assert.equal(result.status, 0, result.stderr);
  };
  git("init", "-q");
  git("config", "user.name", "test");
  git("config", "user.email", "test@example.com");
  writeFileSync(join(dir, "CHANGELOG.md"), changelog(null, ""));
  git("add", "CHANGELOG.md");
  git("commit", "-q", "-m", "chore: release 0.34.1");
  git("tag", "sdk-ts-v0.34.1");
  for (const subject of subjects) git("commit", "-q", "--allow-empty", "-m", subject);
  return dir;
}

describe("cut-changelog.mjs", () => {
  it("refuses a release smaller than the commit subjects imply", () => {
    const dir = repoWithCommits(["feat!: drop the old search API", "fix: tidy"]);
    const patch = cutCli(dir, "0.34.2");
    assert.equal(patch.status, 1);
    assert.match(patch.stderr, /need a Minor release, but 0\.34\.2 is Patch/);
    assert.equal(readFileSync(join(dir, "CHANGELOG.md"), "utf8"), changelog(null, ""));

    const minor = cutCli(dir, "0.35.0");
    assert.equal(minor.status, 0, minor.stderr);
    assert.match(readFileSync(join(dir, "CHANGELOG.md"), "utf8"), /## \[0\.35\.0\] - 2026-10-04\n\n### Breaking Changes\n\n- Drop the old search API/);
  });

  it("still records a release with no user-facing changes", () => {
    const dir = repoWithCommits(["chore: update packaging"]);
    const result = cutCli(dir, "0.34.2");
    assert.equal(result.status, 0, result.stderr);
    const cut = readFileSync(join(dir, "CHANGELOG.md"), "utf8");
    assert.match(cut, /## \[Unreleased\]\n\n## \[0\.34\.2\] - 2026-10-04\n\nNo user-facing changes\.\n\n## \[0\.34\.1\]/);
    assert.match(cut, /^\[Unreleased\]: .*sdk-ts-v0\.34\.2\.\.\.HEAD$/m);
  });

  it("cuts CHANGELOG.md in the working directory and is idempotent", () => {
    const dir = mkdtempSync(join(tmpdir(), "relayhistory-changelog-"));
    writeFileSync(join(dir, "CHANGELOG.md"), changelog());
    const run = () =>
      spawnSync(process.execPath, [join(scripts, "cut-changelog.mjs"), "--version", "0.35.0", "--date", "2026-10-04"], {
        cwd: dir,
        encoding: "utf8",
      });

    const first = run();
    assert.equal(first.status, 0, first.stderr);
    const cut = readFileSync(join(dir, "CHANGELOG.md"), "utf8");
    assert.match(cut, /^## \[Unreleased\]\n\n## \[0\.35\.0\] - 2026-10-04$/m);
    assert.match(cut, /^\[0\.35\.0\]: .*sdk-ts-v0\.34\.1\.\.\.sdk-ts-v0\.35\.0$/m);

    const second = run();
    assert.equal(second.status, 0, second.stderr);
    assert.equal(readFileSync(join(dir, "CHANGELOG.md"), "utf8"), cut);
  });
});

describe("impliedLevel", () => {
  it("treats a breaking change as Minor before 1.0 and Major after", () => {
    assert.equal(impliedLevel("### Breaking Changes\n\n- Drop\n\n### Fixed\n\n- Fix", "0.34.1"), "Minor");
    assert.equal(impliedLevel("### Breaking Changes\n\n- Drop", "1.2.0"), "Major");
    assert.equal(impliedLevel("### Added\n\n- New", "0.34.1"), "Minor");
    assert.equal(impliedLevel("### Fixed\n\n- Fix", "0.34.1"), "Patch");
  });
});

describe("carryReleaseCut", () => {
  const start = changelog("Patch", "### Fixed\n\n- Shipped fix");
  const released = cutChangelog(start, { version: "0.34.2", date: "2026-10-04" }).changelog;

  it("keeps entries added after the release source pending, at the branch level", () => {
    const upstream = changelog("Minor", "### Added\n\n- New feature\n\n### Fixed\n\n- Shipped fix\n- Later fix\n  continued");
    const carried = carryReleaseCut(upstream, { version: "0.34.2", released, start });
    assert.match(
      carried,
      /## \[Unreleased - Minor\]\n\n### Added\n\n- New feature\n\n### Fixed\n\n- Later fix\n  continued\n\n## \[0\.34\.2\] - 2026-10-04\n\n### Fixed\n\n- Shipped fix\n\n## \[0\.34\.1\]/,
    );
    assert.match(carried, /^\[Unreleased\]: .*sdk-ts-v0\.34\.2\.\.\.HEAD$/m);
    assert.equal(carryReleaseCut(carried, { version: "0.34.2", released, start }), carried);
  });

  it("does not keep a shipped feature's level for a carried fix", () => {
    const minorStart = changelog("Minor", "### Added\n\n- Add search");
    const minorReleased = cutChangelog(minorStart, { version: "0.35.0", date: "2026-10-04" }).changelog;
    const upstream = changelog("Minor", "### Added\n\n- Add search\n\n### Fixed\n\n- Fix search");
    const carried = carryReleaseCut(upstream, { version: "0.35.0", released: minorReleased, start: minorStart });
    assert.match(carried, /## \[Unreleased - Patch\]\n\n### Fixed\n\n- Fix search\n\n## \[0\.35\.0\]/);

    const ambiguous = changelog("Minor", "### Added\n\n- Add search\n\n### Changed\n\n- Faster search");
    assert.match(
      carryReleaseCut(ambiguous, { version: "0.35.0", released: minorReleased, start: minorStart }),
      /## \[Unreleased - Minor\]\n\n### Changed\n\n- Faster search/,
    );
  });

  it("levels a breaking change carried past 1.0.0 as Major", () => {
    const majorStart = changelog("Major", "### Breaking Changes\n\n- Stable API");
    const majorReleased = cutChangelog(majorStart, { version: "1.0.0", date: "2026-10-04" }).changelog;
    const upstream = changelog("Major", "### Breaking Changes\n\n- Stable API\n- Drop old flag");
    const carried = carryReleaseCut(upstream, { version: "1.0.0", released: majorReleased, start: majorStart });
    assert.match(carried, /## \[Unreleased - Major\]\n\n### Breaking Changes\n\n- Drop old flag\n\n## \[1\.0\.0\]/);
    assert.throws(() => assertChangelogSemver(carried, "1.1.0"), /requires a Major release/);
  });

  it("keeps a bullet whose text shipped under another section", () => {
    const upstream = changelog("Patch", "### Fixed\n\n- Shipped fix\n\n### Rust API\n\n- Shipped fix");
    assert.match(
      carryReleaseCut(upstream, { version: "0.34.2", released, start }),
      /## \[Unreleased - Patch\]\n\n### Rust API\n\n- Shipped fix\n\n## \[0\.34\.2\]/,
    );
  });

  it("leaves a bare [Unreleased] when the branch gained nothing pending", () => {
    const upstream = changelog("Patch", "### Fixed\n\n- Shipped fix").replace("# Changelog", "# Changelog\n\nEdited intro.");
    const carried = carryReleaseCut(upstream, { version: "0.34.2", released, start });
    assert.match(carried, /Edited intro\.\n\n## \[Unreleased\]\n\n## \[0\.34\.2\] - 2026-10-04\n\n### Fixed\n\n- Shipped fix\n/);
  });
});

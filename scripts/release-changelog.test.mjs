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
  cutChangelog,
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

describe("cut-changelog.mjs", () => {
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

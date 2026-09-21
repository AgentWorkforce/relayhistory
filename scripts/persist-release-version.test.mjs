import assert from "node:assert/strict";
import test from "node:test";
import { spawnSync } from "node:child_process";
import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const scripts = dirname(fileURLToPath(import.meta.url));
const persist = join(scripts, "persist-release-version.sh");

function git(cwd, ...args) {
  const result = spawnSync("git", ["-C", cwd, ...args], { encoding: "utf8" });
  if (result.status !== 0) {
    throw new Error(
      `git ${args.join(" ")} failed:\n${result.stdout}\n${result.stderr}`,
    );
  }
  return result.stdout.trim();
}

function persistRelease(cwd, branch, startSha, extraEnv = {}) {
  return spawnSync("bash", [persist, branch, startSha], {
    cwd,
    encoding: "utf8",
    env: { ...process.env, ...extraEnv },
  });
}

async function stageRepos() {
  const root = await mkdtemp(join(tmpdir(), "persist-release-"));
  const origin = join(root, "origin.git");
  const work = join(root, "work");
  const other = join(root, "other");

  git(root, "init", "--bare", origin);
  git(root, "clone", origin, work);
  git(work, "config", "user.name", "persist-test");
  git(work, "config", "user.email", "persist-test@example.com");
  await writeFile(join(work, "version.txt"), "0.21.0\n");
  git(work, "add", "version.txt");
  git(work, "commit", "-m", "start");
  git(work, "branch", "-M", "main");
  git(work, "push", "-u", "origin", "main");
  git(origin, "symbolic-ref", "HEAD", "refs/heads/main");
  const startSha = git(work, "rev-parse", "HEAD");

  git(root, "clone", "--branch", "main", origin, other);
  git(other, "config", "user.name", "persist-test");
  git(other, "config", "user.email", "persist-test@example.com");

  return { origin, work, other, startSha };
}

test("rebases the version commit when the branch advanced with an unrelated file", async () => {
  const { work, other, startSha } = await stageRepos();

  await writeFile(join(work, "version.txt"), "0.21.2\n");
  git(work, "add", "version.txt");
  git(work, "commit", "-m", "chore: release 0.21.2");

  await writeFile(join(other, "unrelated.txt"), "from a merged PR\n");
  git(other, "add", "unrelated.txt");
  git(other, "commit", "-m", "feat: land during publish");
  git(other, "push", "origin", "main");

  const result = persistRelease(work, "main", startSha);
  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
  assert.match(result.stdout, /rebasing the version commit/);

  git(work, "fetch", "origin", "main");
  const files = git(work, "ls-tree", "-r", "--name-only", "origin/main");
  assert.equal(files, "unrelated.txt\nversion.txt");
  assert.equal(
    git(work, "show", "origin/main:version.txt"),
    "0.21.2",
  );
  assert.equal(
    git(work, "show", "origin/main:unrelated.txt"),
    "from a merged PR",
  );
});

test("fast-forwards the version commit when the branch has not moved", async () => {
  const { work, startSha } = await stageRepos();
  await writeFile(join(work, "version.txt"), "0.21.2\n");
  git(work, "add", "version.txt");
  git(work, "commit", "-m", "chore: release 0.21.2");

  const result = persistRelease(work, "main", startSha);
  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
  assert.doesNotMatch(result.stdout, /rebasing the version commit/);
  assert.equal(git(work, "rev-parse", "origin/main"), git(work, "rev-parse", "HEAD"));
});

test("leaves the advanced branch alone when there is no version commit", async () => {
  const { work, other, startSha } = await stageRepos();
  await writeFile(join(other, "unrelated.txt"), "from a merged PR\n");
  git(other, "add", "unrelated.txt");
  git(other, "commit", "-m", "feat: land during publish");
  git(other, "push", "origin", "main");
  const remoteSha = git(other, "rev-parse", "HEAD");

  const result = persistRelease(work, "main", startSha);
  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
  assert.match(result.stdout, /not rewriting main/);
  git(work, "fetch", "origin", "main");
  assert.equal(git(work, "rev-parse", "origin/main"), remoteSha);
});

test("fails closed when the version commit conflicts with the new tip", async () => {
  const { work, other, startSha } = await stageRepos();
  await writeFile(join(work, "version.txt"), "0.21.2\n");
  git(work, "add", "version.txt");
  git(work, "commit", "-m", "chore: release 0.21.2");
  const versionSha = git(work, "rev-parse", "HEAD");

  await writeFile(join(other, "version.txt"), "conflicting edit\n");
  git(other, "add", "version.txt");
  git(other, "commit", "-m", "feat: touch the version file");
  git(other, "push", "origin", "main");
  const remoteSha = git(other, "rev-parse", "HEAD");

  const result = persistRelease(work, "main", startSha, { VERSION: "0.21.2" });
  assert.equal(result.status, 1);
  assert.match(result.stderr, /does not apply cleanly/);
  assert.match(result.stderr, /sdk-ts-v0\.21\.2/);
  git(work, "fetch", "origin", "main");
  assert.equal(git(work, "rev-parse", "origin/main"), remoteSha);
  assert.equal(git(work, "rev-parse", "HEAD"), versionSha);
});

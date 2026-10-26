//! `session.repo` derivation against real repositories. Needs `git` on PATH;
//! each test skips (passes) without it, since the derivation itself returns
//! `None` there by design.

use std::path::Path;
use std::process::Command;

use relayhistory_object_store::manifest::repo::{root_commit, session_repo, RootCommitCache};

fn git(dir: &Path, args: &[&str]) -> String {
    // Isolated from the developer's git config (signing, hooks, templates).
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args([
            "-c",
            "commit.gpgsign=false",
            "-c",
            "tag.gpgsign=false",
            "-c",
            "core.hooksPath=/dev/null",
        ])
        .args(args)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_AUTHOR_NAME", "t")
        .env("GIT_AUTHOR_EMAIL", "t@example.com")
        .env("GIT_COMMITTER_NAME", "t")
        .env("GIT_COMMITTER_EMAIL", "t@example.com")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("HOME", dir)
        .output()
        .expect("git runs");
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

fn have_git() -> bool {
    Command::new("git")
        .arg("--version")
        .output()
        .is_ok_and(|o| o.status.success())
}

fn repo_with_commits(dir: &Path, n: usize) -> String {
    git(dir, &["init", "-q", "-b", "main"]);
    for i in 0..n {
        std::fs::write(dir.join("f"), i.to_string()).unwrap();
        git(dir, &["add", "f"]);
        git(dir, &["commit", "-q", "-m", &format!("c{i}")]);
    }
    git(dir, &["rev-list", "--max-parents=0", "HEAD"])
}

#[test]
fn root_commit_is_the_first_commit_and_is_cached() {
    if !have_git() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let root = repo_with_commits(dir.path(), 3);
    assert_eq!(root_commit(dir.path()).as_deref(), Some(root.as_str()));
    // From a subdirectory too.
    std::fs::create_dir(dir.path().join("sub")).unwrap();
    assert_eq!(
        root_commit(&dir.path().join("sub")).as_deref(),
        Some(root.as_str())
    );

    let cache = RootCommitCache::new();
    assert_eq!(cache.get(dir.path()).as_deref(), Some(root.as_str()));
    // Cached: a new history after the first lookup is not re-read.
    std::fs::remove_dir_all(dir.path().join(".git")).unwrap();
    assert_eq!(cache.get(dir.path()).as_deref(), Some(root.as_str()));
}

#[test]
fn several_roots_pick_the_smallest() {
    if !have_git() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    repo_with_commits(dir.path(), 1);
    git(dir.path(), &["checkout", "-q", "--orphan", "other"]);
    std::fs::write(dir.path().join("g"), "x").unwrap();
    git(dir.path(), &["add", "g"]);
    git(dir.path(), &["commit", "-q", "-m", "other root"]);
    git(dir.path(), &["checkout", "-q", "main"]);
    git(
        dir.path(),
        &[
            "merge",
            "-q",
            "--allow-unrelated-histories",
            "-m",
            "join",
            "other",
        ],
    );
    let mut roots: Vec<String> = git(dir.path(), &["rev-list", "--max-parents=0", "HEAD"])
        .lines()
        .map(str::to_string)
        .collect();
    assert_eq!(roots.len(), 2);
    roots.sort();
    assert_eq!(root_commit(dir.path()), Some(roots[0].clone()));
}

#[test]
fn shallow_clones_empty_repos_and_non_repos_have_no_root() {
    if !have_git() {
        return;
    }
    let src = tempfile::tempdir().unwrap();
    repo_with_commits(src.path(), 3);
    let parent = tempfile::tempdir().unwrap();
    let url = format!("file://{}", src.path().display());
    git(
        parent.path(),
        &["clone", "-q", "--depth", "1", &url, "shallow"],
    );
    assert_eq!(
        root_commit(&parent.path().join("shallow")),
        None,
        "shallow clone"
    );

    let empty = tempfile::tempdir().unwrap();
    git(empty.path(), &["init", "-q"]);
    assert_eq!(root_commit(empty.path()), None, "no commits");

    let plain = tempfile::tempdir().unwrap();
    assert_eq!(root_commit(plain.path()), None, "not a repository");
}

#[test]
fn session_repo_drops_non_canonical_remotes_and_bad_roots() {
    let root = "0123456789abcdef0123456789abcdef01234567".to_string();
    let r = session_repo(Some("github.com/acme/api"), Some(root.clone())).unwrap();
    assert_eq!(r.remote_url.as_deref(), Some("github.com/acme/api"));
    assert_eq!(r.root_commit.as_deref(), Some(root.as_str()));
    // A raw remote (possibly with credentials) is never sent.
    let r = session_repo(
        Some("https://bot:token@github.com/acme/api.git"),
        Some(root.clone()),
    )
    .unwrap();
    assert_eq!(r.remote_url, None);
    assert_eq!(r.root_commit.as_deref(), Some(root.as_str()));
    // Remote alone (shallow clone).
    assert!(session_repo(Some("github.com/acme/api"), None)
        .unwrap()
        .root_commit
        .is_none());
    // Nothing usable: no block at all.
    assert!(session_repo(Some("/home/me/src/api"), Some("nope".into())).is_none());
    assert!(session_repo(None, None).is_none());
}

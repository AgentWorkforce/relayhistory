//! `root_commit` ignores repository-selection variables inherited from the
//! host (a delivery job started from a git hook has `GIT_DIR` set). Its own
//! test binary, because it sets process environment.

use std::path::Path;
use std::process::Command;

use relayhistory_object_store::manifest::repo::root_commit;

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
        .env_remove("GIT_DIR")
        .output()
        .expect("git runs");
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

fn repo(dir: &Path, content: &str) -> String {
    git(dir, &["init", "-q"]);
    std::fs::write(dir.join("f"), content).unwrap();
    git(dir, &["add", "f"]);
    git(dir, &["commit", "-q", "-m", content]);
    git(dir, &["rev-list", "--max-parents=0", "HEAD"])
}

#[test]
fn an_inherited_git_dir_does_not_redirect_the_lookup() {
    if !Command::new("git")
        .arg("--version")
        .output()
        .is_ok_and(|o| o.status.success())
    {
        return;
    }
    let wanted = tempfile::tempdir().unwrap();
    let other = tempfile::tempdir().unwrap();
    let wanted_root = repo(wanted.path(), "wanted");
    let other_root = repo(other.path(), "other");
    assert_ne!(wanted_root, other_root);
    std::env::set_var("GIT_DIR", other.path().join(".git"));
    std::env::set_var("GIT_WORK_TREE", other.path());
    let got = root_commit(wanted.path());
    std::env::remove_var("GIT_DIR");
    std::env::remove_var("GIT_WORK_TREE");
    assert_eq!(got, Some(wanted_root));
}

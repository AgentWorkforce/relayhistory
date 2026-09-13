//! Shared Git process operations used by explicit linkage and the CLI.
use anyhow::{Context, Result};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
pub fn sh_single_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

pub fn git_repo_root(repo: &Path) -> Result<PathBuf> {
    let out = git_stdout(repo, &["rev-parse", "--show-toplevel"])?;
    Ok(PathBuf::from(out.trim()))
}

pub fn git_path(repo: &Path, path: &str) -> Result<PathBuf> {
    let out = git_stdout(repo, &["rev-parse", "--git-path", path])?;
    let resolved = PathBuf::from(out.trim());
    if resolved.is_absolute() {
        Ok(resolved)
    } else {
        Ok(repo.join(resolved))
    }
}

pub fn git_branch(repo: &Path) -> Result<String> {
    let out = git_stdout(repo, &["rev-parse", "--abbrev-ref", "HEAD"])?;
    let branch = out.trim();
    anyhow::ensure!(branch != "HEAD" && !branch.is_empty(), "detached HEAD");
    Ok(branch.to_string())
}

pub fn git_commit_time_ms(repo: &Path, commit: &str) -> Result<i64> {
    let out = git_stdout(repo, &["show", "-s", "--format=%ct", commit])?;
    Ok(out.trim().parse::<i64>()? * 1000)
}

pub fn git_commit_files(repo: &Path, commit: &str) -> Result<Vec<String>> {
    let out = git_stdout(
        repo,
        &[
            "diff-tree",
            "--root",
            "--no-commit-id",
            "--name-only",
            "-r",
            commit,
        ],
    )?;
    Ok(out
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect())
}

pub fn git_commit_numstat(repo: &Path, commit: &str) -> Result<Vec<Value>> {
    let out = git_stdout(
        repo,
        &[
            "diff-tree",
            "--root",
            "--numstat",
            "--no-commit-id",
            "-r",
            commit,
        ],
    )?;
    Ok(out
        .lines()
        .filter_map(|line| {
            let mut parts = line.split('\t');
            let additions = parts.next()?;
            let deletions = parts.next()?;
            let path = parts.next()?;
            Some(json!({
                "path": path,
                "additions": additions.parse::<i64>().ok(),
                "deletions": deletions.parse::<i64>().ok(),
            }))
        })
        .collect())
}

pub fn git_stdout(repo: &Path, args: &[&str]) -> Result<String> {
    let out = std::process::Command::new("git")
        .current_dir(repo)
        .args(args)
        .output()
        .with_context(|| format!("running git {}", args.join(" ")))?;
    anyhow::ensure!(
        out.status.success(),
        "git {} failed: {}",
        args.join(" "),
        String::from_utf8_lossy(&out.stderr).trim()
    );
    Ok(String::from_utf8_lossy(&out.stdout).to_string())
}

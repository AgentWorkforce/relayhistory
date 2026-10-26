//! Deriving `session.repo` on the delivery side.
//!
//! The host builds `session.*` when it creates a session's manifest. For repo
//! identity it passes what it already knows from
//! `ai_hist::project_identity` — the canonical remote (`project_key` when the
//! method is `Remote`) and the git work-tree root — and this module adds the
//! root commit.
//!
//! The root commit needs `git`: computing it means walking history through
//! packfiles. That is why it lives here, in the delivery plugin, and not in
//! `project_identity`, which never runs a subprocess because it sits on the
//! ingest path. Delivery is asynchronous and off every local read, the answer
//! never changes for a repository, and [`RootCommitCache`] runs `git` once per
//! work tree. Any failure (no `git` on `PATH`, not a repository, no commits,
//! a shallow clone) yields `None`, and the server then identifies the repo by
//! its remote alone. Blocking: call from `spawn_blocking` in async code.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Mutex;

use super::types::SessionRepo;
use super::validate::canonical_remote_shape;

/// `session.repo` from a canonical remote and a root commit, or `None` when
/// neither is usable. A remote that is not canonical `host/path` is dropped
/// rather than sent: a raw remote can carry credentials.
pub fn session_repo(remote_url: Option<&str>, root_commit: Option<String>) -> Option<SessionRepo> {
    let remote_url = remote_url
        .map(str::trim)
        .filter(|r| canonical_remote_shape(r))
        .map(str::to_string);
    let root_commit = root_commit.filter(|r| is_commit_id(r));
    (remote_url.is_some() || root_commit.is_some()).then(|| SessionRepo {
        remote_url,
        root_commit,
        extra: Default::default(),
    })
}

/// The repository's root commit: the smallest of `git rev-list
/// --max-parents=0 HEAD` (a repository can have several roots), or `None` for
/// a shallow clone or on any failure.
pub fn root_commit(work_tree: &Path) -> Option<String> {
    let shallow = git(work_tree, &["rev-parse", "--is-shallow-repository"])?;
    if shallow.trim() != "false" {
        return None;
    }
    git(work_tree, &["rev-list", "--max-parents=0", "HEAD"])?
        .lines()
        .map(str::trim)
        .filter(|line| is_commit_id(line))
        .min()
        .map(str::to_string)
}

/// Root commits per work tree, so `git` runs once per repository. Owned by the
/// caller (one per delivery job), never process-global.
#[derive(Debug, Default)]
pub struct RootCommitCache {
    by_work_tree: Mutex<HashMap<PathBuf, Option<String>>>,
}

impl RootCommitCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, work_tree: &Path) -> Option<String> {
        let mut cache = self
            .by_work_tree
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        cache
            .entry(work_tree.to_path_buf())
            .or_insert_with(|| root_commit(work_tree))
            .clone()
    }
}

fn is_commit_id(s: &str) -> bool {
    (s.len() == 40 || s.len() == 64) && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

fn git(work_tree: &Path, args: &[&str]) -> Option<String> {
    let out = Command::new("git")
        .arg("-C")
        .arg(work_tree)
        .args(args)
        .env("GIT_TERMINAL_PROMPT", "0")
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).into_owned())
}

//! Explicit-session Git linkage for the npm SDK. No network runs in post-commit.
use crate::{
    git_branch, git_commit_files, git_commit_numstat, git_commit_time_ms, git_path, git_repo_root,
    git_stdout, sh_single_quote,
};
use ai_hist_core::open_db;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{fs, path::Path};

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GitLinkOptions {
    pub repo: String,
    pub session_id: String,
    pub source: Option<String>,
    pub db_path: String,
    pub pr_url: Option<String>,
}

fn pr_ref(raw: &str) -> Result<String> {
    let url = url::Url::parse(raw)?;
    let segments: Vec<_> = url.path().trim_start_matches('/').split('/').collect();
    anyhow::ensure!(
        url.scheme() == "https"
            && url.host_str() == Some("github.com")
            && url.username().is_empty()
            && url.password().is_none()
            && url.port().is_none()
            && url.query().is_none()
            && url.fragment().is_none()
            && segments.len() == 4
            && segments[2] == "pull"
            && segments[3]
                .parse::<u64>()
                .is_ok_and(|n| n > 0 && n.to_string() == segments[3])
            && segments[..2].iter().all(|s| !s.is_empty()
                && s.chars()
                    .all(|c| c.is_ascii_alphanumeric() || "_.-".contains(c))),
        "prUrl must be https://github.com/OWNER/REPO/pull/NUMBER"
    );
    Ok(format!("{}/{}#{}", segments[0], segments[1], segments[3]))
}

fn resolve_source(options: &GitLinkOptions) -> Result<String> {
    anyhow::ensure!(
        !options.session_id.trim().is_empty(),
        "sessionId must not be empty"
    );
    let conn = open_db(Path::new(&options.db_path))?;
    let mut stmt = conn.prepare("SELECT DISTINCT source FROM sessions WHERE session_id = ? UNION SELECT DISTINCT source FROM history WHERE session_id = ?")?;
    let sources = stmt
        .query_map([&options.session_id, &options.session_id], |r| {
            r.get::<_, String>(0)
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if let Some(source) = &options.source {
        anyhow::ensure!(
            sources.contains(source),
            "session not found for requested source; sync or hydrate it first"
        );
        return Ok(source.clone());
    }
    anyhow::ensure!(
        sources.len() == 1,
        "session must exist with exactly one source; sync first or pass source explicitly"
    );
    Ok(sources[0].clone())
}

/// Resolve a path whose tail may not exist yet: canonicalize the nearest existing
/// ancestor, then apply the remaining components so `..` cannot escape it.
fn resolve_against_existing(path: &Path) -> Result<std::path::PathBuf> {
    let Some(base) = path.ancestors().find(|candidate| candidate.exists()) else {
        return Ok(path.to_path_buf());
    };
    let mut resolved = base.canonicalize()?;
    for component in path
        .strip_prefix(base)
        .unwrap_or(Path::new(""))
        .components()
    {
        match component {
            std::path::Component::ParentDir => {
                resolved.pop();
            }
            std::path::Component::CurDir => {}
            other => resolved.push(other.as_os_str()),
        }
    }
    Ok(resolved)
}

pub fn install(mut options: GitLinkOptions, node: &str, sdk_url: &str) -> Result<String> {
    let root = git_repo_root(Path::new(&options.repo))?;
    options.repo = root.display().to_string();
    options.db_path = fs::canonicalize(&options.db_path)?.display().to_string();
    options.source = Some(resolve_source(&options)?);
    if options.pr_url.is_none() {
        // Resolve an existing PR once at install time. The post-commit path is
        // deliberately offline. A repository override also works without gh.
        options.pr_url = git_stdout(&root, &["config", "--get", "ai-hist.pr-url"])
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        if options.pr_url.is_none() {
            options.pr_url = std::process::Command::new("gh")
                .current_dir(&root)
                .env("GH_PROMPT_DISABLED", "1")
                .args(["pr", "view", "--json", "url", "--jq", ".url"])
                .output()
                .ok()
                .filter(|out| out.status.success())
                .and_then(|out| String::from_utf8(out.stdout).ok())
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty());
        }
    }
    if let Some(url) = &options.pr_url {
        pr_ref(url)?;
    }
    let hook = git_path(&root, "hooks/post-commit")?;
    let common = git_stdout(&root, &["rev-parse", "--git-common-dir"])?;
    let common = root.join(common.trim()).canonicalize()?;
    let hook_parent = hook.parent().context("missing hook parent")?;
    // Resolve before comparing: an unresolved `..` can satisfy the lexical
    // containment check while `create_dir_all` lands outside the repository.
    let resolved_parent = resolve_against_existing(hook_parent)?;
    anyhow::ensure!(resolved_parent.starts_with(&common),
        "Git uses an external shared core.hooksPath; refusing to change another repository's hooks. Configure a repository-local hooks directory first");
    // Linked worktrees share the common dir's hooks, but this hook embeds one
    // fixed sessionId and repo. Installing from a worktree would attribute every
    // other worktree's commits to this session, so refuse instead of corrupting
    // the linkage we exist to record.
    let git_dir = git_stdout(&root, &["rev-parse", "--git-dir"])?;
    let git_dir = root.join(git_dir.trim()).canonicalize()?;
    anyhow::ensure!(
        git_dir == common,
        "this is a linked Git worktree and its hooks are shared with the main worktree; \
         install from the main worktree instead"
    );
    let script = hook.with_file_name("ai-hist-post-commit.mjs");
    fs::create_dir_all(hook.parent().context("missing hook parent")?)?;
    let body = format!(
        "import {{ linkGitCommit }} from {};\nawait linkGitCommit({});\n",
        serde_json::to_string(sdk_url)?,
        serde_json::to_string(&options)?
    );
    fs::write(&script, body)?;
    let marker = "# ai-hist SDK hook";
    let backup = hook.with_file_name("post-commit.before-ai-hist");
    // A compiled or non-UTF-8 hook still deserves preservation: only a missing
    // file means there is nothing to back up. Decoding failure is not emptiness.
    let needs_backup = match fs::read_to_string(&hook) {
        Ok(text) => !text.is_empty() && !text.contains(marker),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(_) => true,
    };
    if needs_backup {
        anyhow::ensure!(
            !backup.exists(),
            "hook backup already exists; refusing to overwrite it"
        );
        fs::rename(&hook, &backup)?;
    }
    // Wrap the original instead of appending after a possible `exit 0`. Retain
    // its interpreter and exit code, and record our link even if it exits early.
    let previous = if backup.exists() {
        format!(
            "{} \"$@\"\nprevious_status=$?\n",
            sh_single_quote(&backup.display().to_string())
        )
    } else {
        "previous_status=0\n".to_string()
    };
    fs::write(&hook, format!("#!/bin/sh\n{marker}\n{previous}{} {} || echo 'ai-hist: commit linkage failed' >&2\nexit \"$previous_status\"\n", sh_single_quote(node), sh_single_quote(&script.display().to_string())))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&hook, fs::Permissions::from_mode(0o755))?;
    }
    Ok(json!({"hookPath": hook.display().to_string(), "prUrl": options.pr_url}).to_string())
}

pub fn link(options: GitLinkOptions) -> Result<String> {
    let source = resolve_source(&options)?;
    let root = git_repo_root(Path::new(&options.repo))?;
    let sha = git_stdout(&root, &["rev-parse", "--verify", "HEAD^{commit}"])?
        .trim()
        .to_owned();
    let branch = git_branch(&root).ok();
    let commit_ms = git_commit_time_ms(&root, &sha)?;
    let files = serde_json::to_string(&git_commit_files(&root, &sha)?)?;
    let numstat = serde_json::to_string(&git_commit_numstat(&root, &sha)?)?;
    let mut evidence = json!({"commit_time_ms": commit_ms});
    if let Some(url) = &options.pr_url {
        evidence["github_pr"] = json!({"system":"github", "id":pr_ref(url)?, "url":url});
    }
    let conn = open_db(Path::new(&options.db_path))?;
    conn.execute("INSERT INTO session_commit_links (source, session_id, repo, branch, commit_sha, note_ref, match_method, confidence, files_json, numstat_json, evidence_json, created_at_ms) VALUES (?,?,?,?,?,'refs/notes/ai-hist','explicit_session',1,?,?,?,?) ON CONFLICT(source, session_id, commit_sha, match_method) DO NOTHING",
        rusqlite::params![source, options.session_id, root.display().to_string(), branch, sha, files, numstat, evidence.to_string(), chrono::Utc::now().timestamp_millis()])?;
    let note = format!("ai-hist:{}:{}", source, options.session_id);
    let prior = git_stdout(&root, &["notes", "--ref=ai-hist", "show", &sha]).unwrap_or_default();
    if !prior.lines().any(|line| line == note) {
        git_stdout(
            &root,
            &["notes", "--ref=ai-hist", "append", "-m", &note, &sha],
        )?;
    }
    Ok(sha)
}

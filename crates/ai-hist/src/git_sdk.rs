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
            && segments[3].parse::<u64>().is_ok_and(|n| n > 0)
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

pub fn install(mut options: GitLinkOptions, node: &str, sdk_url: &str) -> Result<String> {
    let root = git_repo_root(Path::new(&options.repo))?;
    options.repo = root.display().to_string();
    options.db_path = fs::canonicalize(&options.db_path)?.display().to_string();
    options.source = Some(resolve_source(&options)?);
    if let Some(url) = &options.pr_url {
        pr_ref(url)?;
    }
    let hook = git_path(&root, "hooks/post-commit")?;
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
    let existing = fs::read_to_string(&hook).unwrap_or_default();
    if !existing.is_empty() && !existing.contains(marker) {
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
    Ok(hook.display().to_string())
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

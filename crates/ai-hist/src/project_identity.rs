//! Canonical project identity for a working directory.
//!
//! Consumers group sessions and events by project. A filesystem path is not a
//! usable key for that: every worktree, every checkout and every subdirectory
//! of one repository is a different path, so `/Users/a/proj` and
//! `/home/b/proj` fragment into two projects even when they are the same
//! repository. The git remote is the one identifier that survives all three,
//! so the canonical key is the `origin` remote canonicalized to `host/path`,
//! and the directory path is only the fallback when there is no remote.
//!
//! The rules here are a deliberate port of burn's
//! `crates/relayburn-sdk/src/reader/git.rs` (itself a port of
//! `packages/reader/src/git.ts`). burn's `--group-by project` and
//! RelayHistory's `project_key` have to agree on the same checkout or a
//! cross-tool rollup silently splits, so the parsing, canonicalization and
//! worktree handling are matched vector for vector — see the tests at the
//! bottom of this file, which carry burn's own cases verbatim plus the ones
//! issue #175 pins.
//!
//! No subprocess runs. `git` is not guaranteed to be on `PATH` in the contexts
//! that ingest history (a hook, a daemon, a Node addon), spawning it per
//! session is far more expensive than reading one small file, and a subprocess
//! that fails is indistinguishable from a repository with no remote. `.git` is
//! read directly, including the `gitdir:` pointer file a linked worktree uses
//! and the `commondir` indirection that points back at the main checkout whose
//! `config` actually holds the remote.
//!
//! One deliberate difference from burn: burn returns `project_key: None` when
//! nothing resolves, keeping the raw `cwd` in a separate `project` field.
//! RelayHistory stores a single column, so [`ProjectIdentity::project_key`] is
//! always populated and [`ProjectIdentity::method`] says which of the two it
//! is. A consumer that needs burn's exact shape reads `method`, it does not
//! guess from whether the string looks like a path.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{LazyLock, Mutex};

use serde::{Deserialize, Serialize};

/// How a [`ProjectIdentity::project_key`] was arrived at.
///
/// Recorded alongside the key because the three are not interchangeable: a
/// remote key is canonical and comparable across machines, a path fallback is
/// only meaningful on the machine that produced it, and an inherited key is
/// the parent's identity standing in for a child that resolved to nothing.
/// The merge order in [`crate::store`] is exactly this ranking, so a later,
/// weaker observation can never overwrite a stronger one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProjectKeyMethod {
    /// Canonicalized `origin` remote: `host/owner/repo`.
    Remote,
    /// No remote was resolvable; the key is the working directory itself.
    #[serde(rename = "path")]
    PathFallback,
    /// Adopted from the delegating parent session.
    Inherited,
}

impl ProjectKeyMethod {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Remote => "remote",
            Self::PathFallback => "path",
            Self::Inherited => "inherited",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "remote" => Some(Self::Remote),
            "path" => Some(Self::PathFallback),
            "inherited" => Some(Self::Inherited),
            _ => None,
        }
    }
}

/// The canonical identity of the project a working directory belongs to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectIdentity {
    /// `host/owner/repo` when a remote resolved, else the working directory.
    pub project_key: String,
    /// The raw `origin` URL as `.git/config` spells it, when there was one.
    pub repo_url: Option<String>,
    /// Work-tree root that owns the `.git` entry, when one was found.
    pub git_root: Option<PathBuf>,
    pub method: ProjectKeyMethod,
}

/// Resolve the identity of `cwd`, reading at most two small files.
pub fn project_identity(cwd: &Path) -> ProjectIdentity {
    let fallback_key = cwd.to_string_lossy().to_string();
    let Some(found) = find_git_dir(cwd) else {
        return ProjectIdentity {
            project_key: fallback_key,
            repo_url: None,
            git_root: None,
            method: ProjectKeyMethod::PathFallback,
        };
    };
    let repo_url = fs::read_to_string(found.git_dir.join("config"))
        .ok()
        .and_then(|text| {
            parse_git_config(&text)
                .get("remote \"origin\"")
                .and_then(|section| section.get("url"))
                .cloned()
        });
    match repo_url
        .as_deref()
        .and_then(canonicalize_remote_url)
        .filter(|key| !key.is_empty())
    {
        Some(project_key) => ProjectIdentity {
            project_key,
            repo_url,
            git_root: Some(found.work_tree),
            method: ProjectKeyMethod::Remote,
        },
        // A repository with no usable `origin` is still a repository: the git
        // root is reported so a caller can say so, but the key falls back to
        // the directory, exactly as burn's resolver does.
        None => ProjectIdentity {
            project_key: fallback_key,
            repo_url,
            git_root: Some(found.work_tree),
            method: ProjectKeyMethod::PathFallback,
        },
    }
}

/// The raw `origin` remote URL for `cwd`, without canonicalizing it.
///
/// The cloud outbox needs the URL itself (it derives an `owner/repo` slug on
/// its own terms), so it reads it through here rather than shelling out to
/// `git remote get-url`.
pub fn origin_remote_url(cwd: &Path) -> Option<String> {
    project_identity(cwd).repo_url
}

/// Pick the identity for a catalog row that may already carry a provider-
/// recorded remote.
///
/// Codex writes `session_meta.payload.git.repository_url`; that is the
/// provider's own statement about the repository and it is preferred over
/// resolving the working directory, which may not even exist any more by the
/// time the transcript is read. Returns `None` only when neither input says
/// anything.
pub fn identity_for(
    cwd: Option<&str>,
    repo_url: Option<&str>,
) -> Option<(String, ProjectKeyMethod)> {
    if let Some(key) = repo_url
        .map(str::trim)
        .filter(|url| !url.is_empty())
        .and_then(canonicalize_remote_url)
    {
        return Some((key, ProjectKeyMethod::Remote));
    }
    let cwd = cwd.map(str::trim).filter(|cwd| !cwd.is_empty())?;
    let resolved = resolve_project_identity(cwd);
    Some((resolved.project_key, resolved.method))
}

/// Cached resolver. Construct one per scope to avoid sharing a process-global
/// cache; the free [`resolve_project_identity`] uses a global one.
#[derive(Debug, Default)]
pub struct ProjectIdentityResolver {
    cache: Mutex<HashMap<String, ProjectIdentity>>,
}

impl ProjectIdentityResolver {
    pub fn new() -> Self {
        Self::default()
    }

    /// Resolve `cwd`, consulting and populating the cache. The lock is held
    /// across the filesystem walk so concurrent callers naming the same
    /// directory only walk it once.
    pub fn resolve(&self, cwd: &str) -> ProjectIdentity {
        let mut cache = self.cache.lock().unwrap_or_else(|poisoned| {
            // A panic in a previous resolve left the cache intact — it is a
            // plain map. Refusing every later lookup would be worse than
            // continuing with a possibly stale entry.
            poisoned.into_inner()
        });
        if let Some(hit) = cache.get(cwd) {
            return hit.clone();
        }
        let resolved = project_identity(Path::new(cwd));
        cache.insert(cwd.to_string(), resolved.clone());
        resolved
    }

    pub fn clear(&self) {
        if let Ok(mut cache) = self.cache.lock() {
            cache.clear();
        }
    }
}

static GLOBAL_RESOLVER: LazyLock<ProjectIdentityResolver> =
    LazyLock::new(ProjectIdentityResolver::new);

/// Resolve `cwd` through a process-global cache. One sync pass touches the
/// same handful of directories thousands of times.
pub fn resolve_project_identity(cwd: &str) -> ProjectIdentity {
    GLOBAL_RESOLVER.resolve(cwd)
}

struct FoundGitDir {
    /// Directory holding `config` — `.git`, or a worktree's common dir.
    git_dir: PathBuf,
    /// Directory whose `.git` entry was found.
    work_tree: PathBuf,
}

fn find_git_dir(start: &Path) -> Option<FoundGitDir> {
    let mut dir: PathBuf = start.canonicalize().unwrap_or_else(|_| start.to_path_buf());
    // Bounded like burn's walk: a symlink loop or a pathological path must
    // cost a fixed number of `stat` calls, not an unbounded climb.
    for _ in 0..100 {
        let candidate = dir.join(".git");
        if let Ok(meta) = fs::metadata(&candidate) {
            if meta.is_dir() {
                return Some(FoundGitDir {
                    git_dir: candidate,
                    work_tree: dir,
                });
            }
            if meta.is_file() {
                if let Some(resolved) = resolve_worktree_git_dir(&candidate) {
                    return Some(FoundGitDir {
                        git_dir: resolved,
                        work_tree: dir,
                    });
                }
            }
        }
        match dir.parent() {
            Some(parent) if parent != dir => dir = parent.to_path_buf(),
            _ => return None,
        }
    }
    None
}

/// Follow a linked worktree's `.git` pointer file to the directory holding the
/// shared `config`.
///
/// A worktree's own gitdir has no `config` of its own — the remote lives in
/// the main checkout, which `commondir` names. Without the second hop every
/// session run from a worktree would fall back to its path and split away from
/// the repository it belongs to.
fn resolve_worktree_git_dir(git_file: &Path) -> Option<PathBuf> {
    let text = fs::read_to_string(git_file).ok()?;
    let raw = gitdir_pointer(&text)?;
    let raw_path = Path::new(raw);
    let gitdir = if raw_path.is_absolute() {
        raw_path.to_path_buf()
    } else {
        git_file.parent()?.join(raw_path)
    };
    if let Ok(text) = fs::read_to_string(gitdir.join("commondir")) {
        let trimmed = text.trim();
        if !trimmed.is_empty() {
            let common = Path::new(trimmed);
            return Some(if common.is_absolute() {
                common.to_path_buf()
            } else {
                gitdir.join(common)
            });
        }
    }
    Some(gitdir)
}

/// The payload of the first `gitdir:` line, trimmed. Equivalent to burn's
/// `(?m)^gitdir:\s*(.+?)\s*$`, which requires a non-empty remainder.
fn gitdir_pointer(text: &str) -> Option<&str> {
    text.lines().find_map(|line| {
        let rest = line.strip_prefix("gitdir:")?;
        let trimmed = rest.trim_matches(|c: char| c.is_whitespace());
        (!trimmed.is_empty()).then_some(trimmed)
    })
}

/// Parse `.git/config` text into `{section -> {key -> value}}`.
///
/// Handles `[section]` and `[section "subsection"]` headers, skips `#` / `;`
/// comment and blank lines, and strips inline comments that fall outside
/// quotes. Deliberately not a full git-config implementation: only enough to
/// read `remote "origin"`'s `url`, and byte-for-byte the same subset burn
/// reads so the two cannot disagree about a config they both parse.
pub fn parse_git_config(text: &str) -> HashMap<String, HashMap<String, String>> {
    let mut out: HashMap<String, HashMap<String, String>> = HashMap::new();
    let mut current: Option<String> = None;
    for raw_line in text.split('\n') {
        let line = raw_line
            .trim_end_matches('\r')
            .trim_start_matches([' ', '\t'])
            .trim_end_matches([' ', '\t']);
        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }
        if line.starts_with('[') && line.ends_with(']') {
            let raw = line[1..line.len() - 1].trim();
            let name = section_name(raw);
            out.entry(name.clone()).or_default();
            current = Some(name);
            continue;
        }
        let Some(section) = current.as_ref() else {
            continue;
        };
        let Some(eq) = line.find('=') else { continue };
        let key = line[..eq].trim().to_string();
        if key.is_empty() {
            continue;
        }
        let value = strip_inline_comment(line[eq + 1..].trim());
        out.entry(section.clone()).or_default().insert(key, value);
    }
    out
}

/// Normalize a section header body, mirroring `^([A-Za-z0-9._-]+)\s+"(.*)"$`.
/// A header that does not match that shape is kept verbatim, as burn keeps it.
fn section_name(raw: &str) -> String {
    let Some(quote) = raw.find('"') else {
        return raw.to_string();
    };
    if !raw.ends_with('"') || raw.len() < quote + 2 {
        return raw.to_string();
    }
    let (head, tail) = raw.split_at(quote);
    let name = head.trim_end_matches(char::is_whitespace);
    // `\s+` requires at least one separator, and the name must be non-empty
    // and made only of the allowed characters.
    if name.len() == head.len()
        || name.is_empty()
        || !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
    {
        return raw.to_string();
    }
    // `(.*)` is greedy, so the subsection runs from the first quote to the
    // last one, embedded quotes and all.
    let subsection = &tail[1..tail.len() - 1];
    format!("{name} \"{subsection}\"")
}

fn strip_inline_comment(value: &str) -> String {
    let mut out = String::new();
    let mut in_quotes = false;
    for ch in value.chars() {
        if ch == '"' {
            in_quotes = !in_quotes;
            continue;
        }
        if !in_quotes && (ch == '#' || ch == ';') {
            break;
        }
        out.push(ch);
    }
    out.trim().to_string()
}

/// Canonicalize a remote URL into `host/path`, lowercasing the host and
/// preserving owner/repo case. `None` for anything that is not recognizably a
/// git URL — a malformed remote must not become a project key.
pub fn canonicalize_remote_url(url: &str) -> Option<String> {
    let trimmed = url.trim();
    // burn's patterns are anchored with a non-multiline `$` and `.` does not
    // match a newline, so a value spanning lines matches neither branch.
    if trimmed.is_empty() || trimmed.contains('\n') || trimmed.contains('\r') {
        return None;
    }

    if let Some((host, path)) = split_scp_like(trimmed) {
        let path_part = strip_dot_git(path.trim_start_matches('/').trim_end_matches('/'));
        if path_part.is_empty() {
            return None;
        }
        return Some(format!("{}/{}", host.to_lowercase(), path_part));
    }

    if let Some(rest) = strip_url_scheme(trimmed) {
        let after_auth = match rest.find('@') {
            Some(idx) => &rest[idx + 1..],
            None => rest,
        };
        let slash = after_auth.find('/')?;
        let host = strip_port(&after_auth[..slash]).to_lowercase();
        if host.is_empty() {
            return None;
        }
        let path_part = strip_dot_git(after_auth[slash + 1..].trim_end_matches('/'));
        if path_part.is_empty() {
            return None;
        }
        return Some(format!("{host}/{path_part}"));
    }

    None
}

/// `^(?:[A-Za-z0-9_-]+)@([^:\s]+):(.+)$` — the scp-like `git@host:owner/repo`
/// form, which has no scheme and so cannot be parsed as a URL.
fn split_scp_like(input: &str) -> Option<(&str, &str)> {
    let at = input.find('@')?;
    let user = &input[..at];
    if user.is_empty()
        || !user
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-'))
    {
        return None;
    }
    let rest = &input[at + 1..];
    let colon = rest.find(':')?;
    let host = &rest[..colon];
    let path = &rest[colon + 1..];
    if host.is_empty() || host.chars().any(char::is_whitespace) || path.is_empty() {
        return None;
    }
    Some((host, path))
}

/// `^([A-Za-z][A-Za-z0-9+.\-]*)://(.+)$` — returns everything after `://`.
fn strip_url_scheme(input: &str) -> Option<&str> {
    let sep = input.find("://")?;
    let scheme = &input[..sep];
    let mut chars = scheme.chars();
    let first = chars.next()?;
    if !first.is_ascii_alphabetic() {
        return None;
    }
    if !chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '.' | '-')) {
        return None;
    }
    let rest = &input[sep + 3..];
    (!rest.is_empty()).then_some(rest)
}

fn strip_dot_git(path: &str) -> String {
    path.strip_suffix(".git").unwrap_or(path).to_string()
}

fn strip_port(host: &str) -> &str {
    match host.find(':') {
        Some(idx) => &host[..idx],
        None => host,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    // ---- canonicalization: burn's own vectors, kept verbatim -------------

    #[test]
    fn canonicalize_scp_form() {
        assert_eq!(
            canonicalize_remote_url("git@github.com:AgentWorkforce/burn.git").as_deref(),
            Some("github.com/AgentWorkforce/burn"),
        );
    }

    #[test]
    fn canonicalize_https_with_dot_git() {
        assert_eq!(
            canonicalize_remote_url("https://github.com/AgentWorkforce/burn.git").as_deref(),
            Some("github.com/AgentWorkforce/burn"),
        );
    }

    #[test]
    fn canonicalize_https_without_dot_git_with_subgroup() {
        assert_eq!(
            canonicalize_remote_url("https://gitlab.com/group/sub/repo").as_deref(),
            Some("gitlab.com/group/sub/repo"),
        );
    }

    #[test]
    fn canonicalize_https_with_user() {
        assert_eq!(
            canonicalize_remote_url("https://user:token@github.com/foo/bar.git").as_deref(),
            Some("github.com/foo/bar"),
        );
    }

    #[test]
    fn canonicalize_ssh_with_port() {
        assert_eq!(
            canonicalize_remote_url("ssh://git@github.com:22/AgentWorkforce/burn.git").as_deref(),
            Some("github.com/AgentWorkforce/burn"),
        );
    }

    #[test]
    fn canonicalize_lowercases_host_only() {
        assert_eq!(
            canonicalize_remote_url("git@GitHub.COM:AgentWorkforce/Burn.git").as_deref(),
            Some("github.com/AgentWorkforce/Burn"),
        );
    }

    #[test]
    fn canonicalize_returns_none_on_junk() {
        assert_eq!(canonicalize_remote_url(""), None);
        assert_eq!(canonicalize_remote_url("not a url"), None);
        assert_eq!(canonicalize_remote_url("https://example.com/"), None);
    }

    #[test]
    fn canonicalize_strips_trailing_slash() {
        assert_eq!(
            canonicalize_remote_url("https://github.com/foo/bar/").as_deref(),
            Some("github.com/foo/bar"),
        );
    }

    // ---- the vectors issue #175 pins across burn and relayhistory --------

    #[test]
    fn issue_175_shared_vectors() {
        // `git@github.com:Org/Repo.git` — scp form, host lowercased, owner and
        // repo case preserved, `.git` stripped.
        assert_eq!(
            canonicalize_remote_url("git@github.com:Org/Repo.git").as_deref(),
            Some("github.com/Org/Repo"),
        );
        // `https://github.com/org/repo` — no `.git` suffix to strip.
        assert_eq!(
            canonicalize_remote_url("https://github.com/org/repo").as_deref(),
            Some("github.com/org/repo"),
        );
        // `ssh://git@host:2222/org/repo.git` — the port is not part of the key.
        assert_eq!(
            canonicalize_remote_url("ssh://git@host:2222/org/repo.git").as_deref(),
            Some("host/org/repo"),
        );
    }

    #[test]
    fn canonicalize_git_protocol_and_scp_with_leading_slash() {
        assert_eq!(
            canonicalize_remote_url("git://github.com/foo/bar.git").as_deref(),
            Some("github.com/foo/bar"),
        );
        assert_eq!(
            canonicalize_remote_url("git@github.com:/foo/bar.git").as_deref(),
            Some("github.com/foo/bar"),
        );
    }

    #[test]
    fn canonicalize_rejects_a_multiline_value() {
        assert_eq!(
            canonicalize_remote_url("https://github.com/foo/bar\nx"),
            None
        );
    }

    // ---- git config parsing ---------------------------------------------

    #[test]
    fn parse_simple_sections() {
        let cfg = parse_git_config(
            "\n[core]\n\trepositoryformatversion = 0\n[remote \"origin\"]\n\turl = git@github.com:foo/bar.git\n\tfetch = +refs/heads/*:refs/remotes/origin/*\n",
        );
        assert_eq!(cfg["core"]["repositoryformatversion"], "0");
        assert_eq!(
            cfg["remote \"origin\""]["url"],
            "git@github.com:foo/bar.git"
        );
    }

    #[test]
    fn parse_ignores_comments_and_blanks() {
        let cfg = parse_git_config(
            "\n# a comment\n; another comment\n[remote \"origin\"]\n\turl = https://github.com/foo/bar ; inline comment\n",
        );
        assert_eq!(
            cfg["remote \"origin\""]["url"],
            "https://github.com/foo/bar"
        );
    }

    #[test]
    fn parse_keeps_a_non_subsection_header_verbatim() {
        let cfg = parse_git_config("[branch]\n\tx = 1\n");
        assert_eq!(cfg["branch"]["x"], "1");
    }

    // ---- resolution ------------------------------------------------------

    #[test]
    fn resolves_remote_from_a_parent_directory() {
        let root = tempdir().unwrap();
        let git_dir = root.path().join(".git");
        fs::create_dir_all(&git_dir).unwrap();
        fs::write(
            git_dir.join("config"),
            "[remote \"origin\"]\n\turl = git@github.com:Org/Repo.git\n",
        )
        .unwrap();
        let nested = root.path().join("packages").join("a");
        fs::create_dir_all(&nested).unwrap();

        let identity = project_identity(&nested);
        assert_eq!(identity.project_key, "github.com/Org/Repo");
        assert_eq!(identity.method, ProjectKeyMethod::Remote);
        assert_eq!(
            identity.repo_url.as_deref(),
            Some("git@github.com:Org/Repo.git")
        );
        assert_eq!(
            identity.git_root.as_deref(),
            Some(root.path().canonicalize().unwrap().as_path())
        );
    }

    #[test]
    fn no_remote_falls_back_to_the_path() {
        let dir = tempdir().unwrap();
        let identity = project_identity(dir.path());
        assert_eq!(identity.project_key, dir.path().to_string_lossy());
        assert_eq!(identity.method, ProjectKeyMethod::PathFallback);
        assert_eq!(identity.repo_url, None);
        assert_eq!(identity.git_root, None);
    }

    #[test]
    fn a_repository_without_an_origin_falls_back_to_the_path() {
        let root = tempdir().unwrap();
        let git_dir = root.path().join(".git");
        fs::create_dir_all(&git_dir).unwrap();
        fs::write(git_dir.join("config"), "[core]\n\tbare = false\n").unwrap();

        let identity = project_identity(root.path());
        assert_eq!(identity.method, ProjectKeyMethod::PathFallback);
        assert_eq!(identity.project_key, root.path().to_string_lossy());
        // The repository was found even though the key fell back.
        assert!(identity.git_root.is_some());
    }

    #[test]
    fn worktree_gitdir_pointer_resolves_through_commondir() {
        let root = tempdir().unwrap();
        let common_git = root.path().join("main").join(".git");
        fs::create_dir_all(&common_git).unwrap();
        fs::write(
            common_git.join("config"),
            "[remote \"origin\"]\n\turl = https://github.com/foo/bar\n",
        )
        .unwrap();
        let worktree_dir = common_git.join("worktrees").join("branch-a");
        fs::create_dir_all(&worktree_dir).unwrap();
        fs::write(worktree_dir.join("commondir"), "../..\n").unwrap();

        let worktree = root.path().join("worktree-a");
        fs::create_dir_all(&worktree).unwrap();
        fs::write(
            worktree.join(".git"),
            format!("gitdir: {}\n", worktree_dir.display()),
        )
        .unwrap();

        let identity = project_identity(&worktree);
        assert_eq!(identity.project_key, "github.com/foo/bar");
        assert_eq!(identity.method, ProjectKeyMethod::Remote);
    }

    #[test]
    fn two_paths_with_the_same_remote_produce_one_key() {
        let make = |name: &str| {
            let root = tempdir().unwrap();
            let git_dir = root.path().join(name).join(".git");
            fs::create_dir_all(&git_dir).unwrap();
            fs::write(
                git_dir.join("config"),
                "[remote \"origin\"]\n\turl = git@github.com:Org/Repo.git\n",
            )
            .unwrap();
            let key = project_identity(&root.path().join(name)).project_key;
            (root, key)
        };
        let (_a, key_a) = make("proj");
        let (_b, key_b) = make("proj-elsewhere");
        assert_eq!(key_a, "github.com/Org/Repo");
        assert_eq!(key_a, key_b);
    }

    #[test]
    fn resolver_memoizes() {
        let resolver = ProjectIdentityResolver::new();
        let root = tempdir().unwrap();
        let git_dir = root.path().join(".git");
        fs::create_dir_all(&git_dir).unwrap();
        fs::write(
            git_dir.join("config"),
            "[remote \"origin\"]\n\turl = git@github.com:foo/bar.git\n",
        )
        .unwrap();
        let key = root.path().to_string_lossy().to_string();
        let first = resolver.resolve(&key);
        fs::write(
            git_dir.join("config"),
            "[remote \"origin\"]\n\turl = git@github.com:zzz/zzz.git\n",
        )
        .unwrap();
        assert_eq!(resolver.resolve(&key).project_key, first.project_key);
        resolver.clear();
        assert_eq!(resolver.resolve(&key).project_key, "github.com/zzz/zzz");
    }

    #[test]
    fn identity_for_prefers_a_provider_recorded_remote() {
        let dir = tempdir().unwrap();
        let cwd = dir.path().to_string_lossy().to_string();
        // The directory has no repository at all, so only the recorded remote
        // can produce a canonical key.
        let (key, method) =
            identity_for(Some(&cwd), Some("git@github.com:Org/Repo.git")).expect("identity");
        assert_eq!(key, "github.com/Org/Repo");
        assert_eq!(method, ProjectKeyMethod::Remote);
    }

    #[test]
    fn identity_for_ignores_an_unparseable_recorded_remote() {
        let dir = tempdir().unwrap();
        let cwd = dir.path().to_string_lossy().to_string();
        let (key, method) = identity_for(Some(&cwd), Some("not-a-git-remote")).expect("identity");
        assert_eq!(key, cwd);
        assert_eq!(method, ProjectKeyMethod::PathFallback);
    }

    #[test]
    fn identity_for_needs_at_least_one_input() {
        assert!(identity_for(None, None).is_none());
        assert!(identity_for(Some("   "), Some("")).is_none());
    }

    #[test]
    fn method_round_trips_through_its_stored_string() {
        for method in [
            ProjectKeyMethod::Remote,
            ProjectKeyMethod::PathFallback,
            ProjectKeyMethod::Inherited,
        ] {
            assert_eq!(ProjectKeyMethod::parse(method.as_str()), Some(method));
        }
        assert_eq!(ProjectKeyMethod::parse("nonsense"), None);
    }
}

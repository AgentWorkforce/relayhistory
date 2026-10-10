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
//! session is far more expensive than reading a few small files, and a
//! subprocess that fails is indistinguishable from a repository with no
//! remote. `.git` is read directly, including the `gitdir:` pointer file a
//! linked worktree uses and the `commondir` indirection that points back at
//! the main checkout whose `config` actually holds the remote.
//!
//! Reading the repository's own `config` is not enough, because git does not
//! resolve a remote from it alone. The system and global scopes are read and
//! merged in git's precedence order, and `include` / `includeIf` directives
//! are followed, because the rewrite that makes a remote recognizable is
//! overwhelmingly configured *outside* the repository: one
//! `url."git@github.com:".insteadOf = https://github.com/` in `~/.gitconfig`
//! covers every repository on the machine. A reader that skipped it saw
//! `gh:Org/Repo.git` as an unrecognizable remote and fell back to a path key —
//! fragmenting exactly the sessions this key exists to merge, on precisely
//! the machines whose owners had configured git most carefully.
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

mod git_config;

use git_config::{apply_insteadof, load_git_config, remote_url};
#[cfg(test)]
use git_config::{
    head_branch, include_condition_holds, load_config_file, merge_config, wildmatch, IncludeContext,
};
pub use git_config::{parse_git_config, GitConfig};

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
    let config = load_git_config(&found.git_dir, &found.head_dir);
    let repo_url = remote_url(&config, "origin").map(|url| apply_insteadof(&config, url));
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
///
/// The cache is only valid for the length of one acquisition pass, and callers
/// must open a pass with [`begin_acquisition_pass`]. A repository that gains
/// an `origin`, or has one changed, would otherwise keep its stale identity
/// for as long as the process lives — which for `ai-hist watch`, the Node
/// addon or a desktop host is indefinitely, and the wrong answer would look
/// exactly like a correct one.
pub fn resolve_project_identity(cwd: &str) -> ProjectIdentity {
    GLOBAL_RESOLVER.resolve(cwd)
}

/// Discard everything the process-global resolver has cached.
///
/// Called at the start of every sync, discovery and hydration pass. Within one
/// pass the filesystem is treated as fixed — that is what makes the cache
/// sound, and it is why the pass boundary is where it has to be dropped. A
/// long-lived host therefore sees a repository's new remote on its next pass
/// rather than on its next restart.
pub fn begin_acquisition_pass() {
    GLOBAL_RESOLVER.clear();
}

struct FoundGitDir {
    /// Directory holding `config` — `.git`, or a worktree's common dir.
    git_dir: PathBuf,
    /// Directory holding `HEAD`. The same one for an ordinary checkout, and
    /// the *per-worktree* git dir for a linked worktree, which is the whole
    /// point of keeping them apart: a worktree exists to be on a different
    /// branch from the checkout whose `config` it shares, so asking the common
    /// dir what branch we are on answers about the wrong tree — and an
    /// `includeIf "onbranch:"` rewrite git would apply here would be skipped,
    /// dropping the repository back to a path key.
    head_dir: PathBuf,
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
                    git_dir: candidate.clone(),
                    head_dir: candidate,
                    work_tree: dir,
                });
            }
            if meta.is_file() {
                if let Some((common, head)) = resolve_worktree_git_dir(&candidate) {
                    return Some(FoundGitDir {
                        git_dir: common,
                        head_dir: head,
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

/// Follow a linked worktree's `.git` pointer file, returning the directory
/// holding the shared `config` and the one holding this worktree's `HEAD`.
///
/// A worktree's own gitdir has no `config` of its own — the remote lives in
/// the main checkout, which `commondir` names. Without that hop every session
/// run from a worktree would fall back to its path and split away from the
/// repository it belongs to. `HEAD`, though, lives in the per-worktree dir and
/// has to be read there: the branch is the one thing a worktree does *not*
/// share.
fn resolve_worktree_git_dir(git_file: &Path) -> Option<(PathBuf, PathBuf)> {
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
            let common = if common.is_absolute() {
                common.to_path_buf()
            } else {
                gitdir.join(common)
            };
            return Some((common, gitdir));
        }
    }
    Some((gitdir.clone(), gitdir))
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
    // `git@[2001:db8::1]:path` is legal scp-like syntax, and the address is
    // full of colons: the separator is the one *after* the closing bracket.
    let colon = match rest.strip_prefix('[') {
        Some(_) => rest
            .find(']')
            .map(|end| end + 1)
            .filter(|at| rest[*at..].starts_with(':'))?,
        None => rest.find(':')?,
    };
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

/// The host part of an authority, with an optional `:port` removed.
///
/// A bracketed IPv6 literal is consumed through its matching `]` first,
/// because its address is full of colons: cutting at the first one turns
/// `[2001:db8::1]:2222` into `[2001`, which is not a host, and turns every
/// address sharing a first group into the same key. The brackets are kept, so
/// the host stays unambiguous and cannot be confused with a path.
fn strip_port(host: &str) -> &str {
    if host.starts_with('[') {
        return match host.find(']') {
            Some(end) => &host[..=end],
            // Unterminated: not an authority this module can take apart, so
            // leave it whole rather than inventing a host from half of it.
            None => host,
        };
    }
    match host.find(':') {
        Some(idx) => &host[..idx],
        None => host,
    }
}

/// The effective value of a genuinely single-valued key: the last written, as
/// git resolves it.
#[cfg(test)]
fn single<'a>(section: &'a HashMap<String, Vec<String>>, key: &str) -> Option<&'a String> {
    section
        .get(&key.to_ascii_lowercase())
        .and_then(|values| values.last())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    // ---- configuration git reads from outside the repository ------------

    /// One config file and everything it includes, in file order.
    fn config_chain(path: &Path, context: &IncludeContext) -> GitConfig {
        let mut out = GitConfig::new();
        load_config_file(path, context, 0, &mut out);
        out
    }

    #[test]
    fn wildmatch_keeps_a_single_star_inside_one_component() {
        assert!(wildmatch("/work/*/.git", "/work/app/.git", false));
        assert!(!wildmatch("/work/*/.git", "/work/team/app/.git", false));
        assert!(wildmatch("**/work/**", "/home/me/work/app/.git", false));
        assert!(wildmatch("/work/**", "/work/.git", false));
        assert!(wildmatch("/work/a?c/.git", "/work/abc/.git", false));
        assert!(!wildmatch("/Work/**", "/work/app/.git", false));
        assert!(wildmatch("/Work/**", "/work/app/.git", true));
    }

    #[test]
    fn a_later_scope_wins_a_single_value_and_adds_to_a_multi_valued_one() {
        let mut merged =
            parse_git_config("[url \"a\"]\n\tinsteadOf = one:\n[core]\n\teditor = first\n");
        merge_config(
            &mut merged,
            parse_git_config("[url \"a\"]\n\tinsteadOf = two:\n[core]\n\tEDITOR = second\n"),
        );
        assert_eq!(
            single(merged.get("core").unwrap(), "editor").map(String::as_str),
            Some("second"),
            "the later scope must win a genuinely single-valued key, whatever its spelling"
        );
        let mut rewrites = merged.get("url \"a\"").unwrap()["insteadof"].clone();
        rewrites.sort();
        assert_eq!(
            rewrites,
            vec!["one:".to_string(), "two:".to_string()],
            "a rewrite configured in one scope must not delete another scope's"
        );
    }

    /// A remote's URL is a *list*, and its identity is the head of that list.
    ///
    /// Verified against git 2.43 rather than assumed, because this replaced a
    /// `git remote get-url origin` subprocess:
    ///
    /// ```text
    /// [remote "origin"]
    ///     url = https://github.com/acme/main.git
    ///     url = https://mirror.example/acme/main.git
    /// $ git remote get-url origin      → https://github.com/acme/main.git
    /// $ git config --get remote.origin.url
    ///                                  → https://mirror.example/acme/main.git
    /// ```
    ///
    /// Taking the last — the generic single-valued rule, which is what
    /// `config --get` applies and what this module used to do — names the
    /// mirror. Every session in the repository would then be filed under a
    /// project whose name is a host nobody pushes to.
    #[test]
    fn a_remote_resolves_to_its_first_url_not_its_last() {
        let config = parse_git_config(
            "[remote \"origin\"]\n\
             \turl = https://github.com/acme/main.git\n\
             \turl = https://mirror.example/acme/main.git\n",
        );
        assert_eq!(
            remote_url(&config, "origin").map(String::as_str),
            Some("https://github.com/acme/main.git")
        );
        assert_eq!(
            canonicalize_remote_url(remote_url(&config, "origin").unwrap()).as_deref(),
            Some("github.com/acme/main")
        );

        // Spelled two different ways, the order between them still holds:
        // folding the names is what keeps them one list.
        let mixed = parse_git_config(
            "[remote \"origin\"]\n\
             \tURL = https://first.example/a.git\n\
             \turl = https://second.example/b.git\n",
        );
        assert_eq!(
            remote_url(&mixed, "origin").map(String::as_str),
            Some("https://first.example/a.git"),
            "git returns the first URL written, whichever way it is spelled"
        );

        // And across scopes, which accumulate in the same list: git 2.43
        // answers `get-url` with the global one here, surprising as that is.
        let mut scoped = parse_git_config(
            "[remote \"origin\"]\n\turl = https://global.example/acme/global.git\n",
        );
        merge_config(
            &mut scoped,
            parse_git_config("[remote \"origin\"]\n\turl = https://github.com/acme/main.git\n"),
        );
        assert_eq!(
            remote_url(&scoped, "origin").map(String::as_str),
            Some("https://global.example/acme/global.git")
        );

        // A rewrite still applies to whichever URL was selected.
        let shorthand = parse_git_config(
            "[remote \"origin\"]\n\turl = gh:acme/main.git\n\turl = gh:acme/mirror.git\n\
             [url \"https://github.com/\"]\n\tinsteadOf = gh:\n",
        );
        assert_eq!(
            apply_insteadof(&shorthand, remote_url(&shorthand, "origin").unwrap()),
            "https://github.com/acme/main.git"
        );
    }

    /// An IPv6 authority is full of colons, and a port is only the last one.
    #[test]
    fn ipv6_remotes_keep_their_whole_address() {
        assert_eq!(
            canonicalize_remote_url("ssh://git@[2001:db8::1]:2222/acme/app.git").as_deref(),
            Some("[2001:db8::1]/acme/app"),
        );
        assert_eq!(
            canonicalize_remote_url("ssh://git@[2001:db8::1]/acme/app.git").as_deref(),
            Some("[2001:db8::1]/acme/app"),
        );
        assert_eq!(
            canonicalize_remote_url("https://[2001:db8::2]/acme/app.git").as_deref(),
            Some("[2001:db8::2]/acme/app"),
        );
        // Two addresses that share a first group must not collide, which is
        // what cutting at the first colon did to every one of them.
        assert_ne!(
            canonicalize_remote_url("ssh://git@[2001:db8::1]:2222/acme/app.git"),
            canonicalize_remote_url("ssh://git@[2001:db8::2]:2222/acme/app.git"),
        );
        // The scp-like form takes brackets too.
        assert_eq!(
            canonicalize_remote_url("git@[2001:db8::1]:acme/app.git").as_deref(),
            Some("[2001:db8::1]/acme/app"),
        );
        // And the ordinary cases are untouched.
        assert_eq!(
            canonicalize_remote_url("ssh://git@host:2222/org/repo.git").as_deref(),
            Some("host/org/repo"),
        );
        assert_eq!(
            canonicalize_remote_url("ssh://git@192.0.2.10:2222/org/repo.git").as_deref(),
            Some("192.0.2.10/org/repo"),
        );
        assert_eq!(
            canonicalize_remote_url("https://github.com/Org/Repo").as_deref(),
            Some("github.com/Org/Repo"),
        );
    }

    /// `include.path` is followed, and the including file still wins.
    #[test]
    fn an_included_file_is_read_and_overridden_by_its_includer() {
        let temp = tempdir().unwrap();
        let root = temp.path();
        fs::write(
            root.join("identity"),
            "[url \"https://github.com/\"]\n\tinsteadOf = gh:\n[remote \"origin\"]\n\turl = from-include\n",
        )
        .unwrap();
        // `origin` is set *before* the include, so the included file's value
        // is the one git resolves. Reading the file into a map and merging it
        // over its includes would invert that and answer with a URL
        // `git remote get-url origin` does not return.
        fs::write(
            root.join("config"),
            "[remote \"origin\"]\n\turl = before-the-include\n\
             [include]\n\tpath = identity\n",
        )
        .unwrap();
        let context = IncludeContext {
            git_dir: root.to_path_buf(),
            branch: None,
        };
        let config = config_chain(&root.join("config"), &context);
        assert_eq!(
            single(config.get("remote \"origin\"").unwrap(), "url").map(String::as_str),
            Some("from-include"),
            "an include must override a value written before its line"
        );
        assert_eq!(
            apply_insteadof(&config, "gh:Org/Repo.git"),
            "https://github.com/Org/Repo.git",
            "a rewrite defined in an included file was not applied"
        );

        // And the other way round: a value after the include wins, because
        // that is where git would apply it.
        fs::write(
            root.join("config"),
            "[include]\n\tpath = identity\n\
             [remote \"origin\"]\n\turl = after-the-include\n",
        )
        .unwrap();
        let config = config_chain(&root.join("config"), &context);
        assert_eq!(
            single(config.get("remote \"origin\"").unwrap(), "url").map(String::as_str),
            Some("after-the-include"),
            "a value written after an include must override it"
        );

        // Two includes, and the later one wins — which is only true if each is
        // expanded where it is written rather than collected and sorted.
        fs::write(
            root.join("second"),
            "[remote \"origin\"]\n\turl = from-the-second-include\n",
        )
        .unwrap();
        fs::write(
            root.join("config"),
            "[include]\n\tpath = second\n\
             [include]\n\tpath = identity\n",
        )
        .unwrap();
        let config = config_chain(&root.join("config"), &context);
        assert_eq!(
            single(config.get("remote \"origin\"").unwrap(), "url").map(String::as_str),
            Some("from-include"),
            "the last include written must win, not whichever sorts last"
        );
    }

    /// `includeIf "gitdir:"` is evaluated against the repository, not guessed.
    #[test]
    fn a_conditional_include_is_applied_only_where_its_condition_holds() {
        let temp = tempdir().unwrap();
        let root = temp.path();
        fs::write(
            root.join("work-identity"),
            "[url \"git@github.com:acme/\"]\n\tinsteadOf = acme:\n",
        )
        .unwrap();
        fs::write(
            root.join("config"),
            "[includeIf \"gitdir:/srv/work/\"]\n\tpath = work-identity\n",
        )
        .unwrap();

        let inside = IncludeContext {
            git_dir: PathBuf::from("/srv/work/app/.git"),
            branch: None,
        };
        let applied = config_chain(&root.join("config"), &inside);
        assert_eq!(
            apply_insteadof(&applied, "acme:thing.git"),
            "git@github.com:acme/thing.git"
        );

        let elsewhere = IncludeContext {
            git_dir: PathBuf::from("/srv/other/app/.git"),
            branch: None,
        };
        let skipped = config_chain(&root.join("config"), &elsewhere);
        assert_eq!(
            apply_insteadof(&skipped, "acme:thing.git"),
            "acme:thing.git",
            "a condition that does not hold must not pull the file in"
        );
    }

    /// An include cycle must cost a bounded number of reads, not the process.
    #[test]
    fn an_include_cycle_terminates() {
        let temp = tempdir().unwrap();
        let root = temp.path();
        fs::write(root.join("a"), "[include]\n\tpath = b\n").unwrap();
        fs::write(
            root.join("b"),
            "[include]\n\tpath = a\n[remote \"origin\"]\n\turl = looped\n",
        )
        .unwrap();
        let context = IncludeContext {
            git_dir: root.to_path_buf(),
            branch: None,
        };
        let config = config_chain(&root.join("a"), &context);
        assert_eq!(
            single(config.get("remote \"origin\"").unwrap(), "url").map(String::as_str),
            Some("looped")
        );
    }

    /// A value continued onto the next line is one value, as git reads it.
    ///
    /// Two checkouts of one repository whose configs differ only in where the
    /// line happens to wrap would otherwise key differently — the wrapped one
    /// keeping a trailing backslash in its URL, which canonicalizes to
    /// something no other machine produces. Verified against git 2.43:
    ///
    /// ```text
    ///     url = https://github.com/acme/long\
    /// /path.git
    /// $ git remote get-url origin → https://github.com/acme/long/path.git
    /// ```
    #[test]
    fn a_continued_value_is_joined_the_way_git_joins_it() {
        let config = parse_git_config(
            "[remote \"origin\"]\n\turl = https://github.com/acme/long\\\n/path.git\n",
        );
        assert_eq!(
            remote_url(&config, "origin").map(String::as_str),
            Some("https://github.com/acme/long/path.git")
        );

        // The continuation may fall inside quotes.
        let quoted = parse_git_config(
            "[remote \"origin\"]\n\turl = \"https://github.com/acme/\\\nquoted.git\"\n",
        );
        assert_eq!(
            remote_url(&quoted, "origin").map(String::as_str),
            Some("https://github.com/acme/quoted.git")
        );

        // A *doubled* backslash is a literal one and ends the line, so the
        // variable below it stays a variable of its own.
        let escaped = parse_git_config("[test]\n\tvalue = a\\\\\n\tother = b\n");
        let section = escaped.get("test").expect("section");
        assert_eq!(section["value"], vec!["a\\".to_string()]);
        assert_eq!(section["other"], vec!["b".to_string()]);

        // Escapes and quoting, on both sides of a quote.
        let values = parse_git_config(
            "[test]\n\
             \tquoted = \"a\\\"b\"\n\
             \thash = \"a#b\"\n\
             \tcomment = a#b\n\
             \tspaces = a  b\n\
             \ttab = a\tb\n",
        );
        let section = values.get("test").expect("section");
        assert_eq!(section["quoted"], vec!["a\"b".to_string()]);
        assert_eq!(section["hash"], vec!["a#b".to_string()]);
        assert_eq!(section["comment"], vec!["a".to_string()]);
        assert_eq!(section["spaces"], vec!["a  b".to_string()]);
        assert_eq!(section["tab"], vec!["a b".to_string()]);
    }

    /// The same repository, wrapped and unwrapped, is one project.
    #[test]
    fn a_continued_remote_resolves_to_the_same_key_as_the_plain_one() {
        let temp = tempdir().unwrap();
        let write_repo = |name: &str, config: &str| {
            let work = temp.path().join(name);
            let git = work.join(".git");
            fs::create_dir_all(&git).unwrap();
            fs::write(git.join("config"), config).unwrap();
            project_identity(&work)
        };

        let plain = write_repo(
            "plain",
            "[remote \"origin\"]\n\turl = https://github.com/acme/long/path.git\n",
        );
        let wrapped = write_repo(
            "wrapped",
            "[remote \"origin\"]\n\turl = https://github.com/acme/long\\\n/path.git\n",
        );
        assert_eq!(
            (plain.project_key.as_str(), plain.method),
            ("github.com/acme/long/path", ProjectKeyMethod::Remote),
            "the control: an ordinary single-line URL still resolves as before"
        );
        assert_eq!(
            wrapped.project_key, plain.project_key,
            "a wrapped config split one repository into two projects"
        );
        assert_eq!(wrapped.method, ProjectKeyMethod::Remote);
    }

    /// git's legacy `[section.subsection]` header, folded onto one spelling.
    ///
    /// A repository written before the quoted form — or by a tool that still
    /// uses this one — has an `origin` that a lookup for `remote "origin"`
    /// simply misses, and the whole repository falls back to a path key.
    #[test]
    fn a_dotted_section_header_is_the_same_section_as_the_quoted_form() {
        let config = parse_git_config("[remote.origin]\n\turl = git@github.com:acme/app.git\n");
        assert_eq!(
            remote_url(&config, "origin").map(String::as_str),
            Some("git@github.com:acme/app.git")
        );

        // The dotted form folds entirely: git answers `remote.origin.url` for
        // `[remote.ORIGIN]` and `[Remote.Origin]` alike.
        for header in ["[remote.ORIGIN]", "[Remote.Origin]", "[REMOTE.ORIGIN]"] {
            let config =
                parse_git_config(&format!("{header}\n\turl = git@github.com:acme/app.git\n"));
            assert_eq!(
                remote_url(&config, "origin").map(String::as_str),
                Some("git@github.com:acme/app.git"),
                "{header} must be the origin remote"
            );
        }

        // The quoted form does not fold its subsection, which is the
        // distinction that makes this a fold rather than a lowercase.
        let quoted =
            parse_git_config("[remote \"ORIGIN\"]\n\turl = git@github.com:acme/other.git\n");
        assert_eq!(remote_url(&quoted, "origin"), None);
        assert_eq!(
            remote_url(&quoted, "ORIGIN").map(String::as_str),
            Some("git@github.com:acme/other.git")
        );

        // A body git itself rejects is not taken apart into a subsection this
        // module invented: `git` calls `[url.https://github.com/]` a bad
        // config line, so it stays a plain section name.
        let odd = parse_git_config("[url.https://github.com/]\n\tinsteadOf = gh:\n");
        assert_eq!(apply_insteadof(&odd, "gh:Org/Repo.git"), "gh:Org/Repo.git");
    }

    /// A repository whose `origin` is written the legacy way still resolves.
    #[test]
    fn a_dotted_remote_resolves_end_to_end() {
        let temp = tempdir().unwrap();
        let work = temp.path().join("app");
        let git = work.join(".git");
        fs::create_dir_all(&git).unwrap();
        fs::write(
            git.join("config"),
            "[core]\n\tbare = false\n[remote.origin]\n\turl = git@github.com:acme/app.git\n",
        )
        .unwrap();
        let resolved = project_identity(&work);
        assert_eq!(
            (resolved.project_key.as_str(), resolved.method),
            ("github.com/acme/app", ProjectKeyMethod::Remote),
        );
    }

    /// `config.worktree`, when the repository has turned that extension on.
    ///
    /// Checked against git 2.43, because the interesting part is what it does
    /// *not* change. git reads the file after the shared config, so its
    /// values win a single-valued key and its rewrites add to the shared
    /// ones — but a remote's URL list still begins with the shared entry, so
    /// `git remote get-url origin` keeps answering with the shared URL:
    ///
    /// ```text
    /// shared config     remote.origin.url = https://shared.example/…
    /// config.worktree   remote.origin.url = https://per-worktree.example/…
    /// $ git remote get-url origin        → https://shared.example/…
    /// $ git remote get-url --all origin  → shared, then per-worktree
    /// $ git config --get remote.origin.url → https://per-worktree.example/…
    /// ```
    ///
    /// So the reach of this file here is every other key, `insteadOf`
    /// included — which is exactly the kind of setting a worktree carries.
    #[test]
    fn a_worktree_config_is_read_when_the_extension_is_enabled() {
        let temp = tempdir().unwrap();
        let root = temp.path();
        let work = root.join("app");
        let git = work.join(".git");
        fs::create_dir_all(&git).unwrap();
        fs::write(
            git.join("config"),
            "[remote \"origin\"]\n\turl = wt:acme/app.git\n",
        )
        .unwrap();
        fs::write(
            git.join("config.worktree"),
            "[url \"https://worktree.example/\"]\n\tinsteadOf = wt:\n",
        )
        .unwrap();

        // Off by default: the file is there and must be ignored.
        assert_eq!(
            project_identity(&work).method,
            ProjectKeyMethod::PathFallback,
            "config.worktree must not be read without extensions.worktreeConfig"
        );

        fs::write(
            git.join("config"),
            "[extensions]\n\tworktreeConfig = true\n\
             [remote \"origin\"]\n\turl = wt:acme/app.git\n",
        )
        .unwrap();
        assert_eq!(
            project_identity(&work).project_key,
            "worktree.example/acme/app",
            "an enabled worktree config was not read"
        );

        // A value git reads as false leaves it unread again.
        fs::write(
            git.join("config"),
            "[extensions]\n\tworktreeConfig = false\n\
             [remote \"origin\"]\n\turl = wt:acme/app.git\n",
        )
        .unwrap();
        assert_eq!(
            project_identity(&work).method,
            ProjectKeyMethod::PathFallback,
        );

        // And a linked worktree reads *its own* file, not the main
        // checkout's: the extension is shared, the file is not.
        fs::write(
            git.join("config"),
            "[extensions]\n\tworktreeConfig = true\n\
             [remote \"origin\"]\n\turl = wt:acme/app.git\n",
        )
        .unwrap();
        let worktree_git = git.join("worktrees/feature");
        fs::create_dir_all(&worktree_git).unwrap();
        fs::write(worktree_git.join("HEAD"), "ref: refs/heads/feature\n").unwrap();
        fs::write(worktree_git.join("commondir"), "../..\n").unwrap();
        fs::write(
            worktree_git.join("config.worktree"),
            "[url \"https://feature.example/\"]\n\tinsteadOf = wt:\n",
        )
        .unwrap();
        let linked = root.join("app-feature");
        fs::create_dir_all(&linked).unwrap();
        fs::write(
            linked.join(".git"),
            format!("gitdir: {}\n", worktree_git.display()),
        )
        .unwrap();
        assert_eq!(
            project_identity(&linked).project_key,
            "feature.example/acme/app",
            "a linked worktree must read its own config.worktree"
        );
        assert_eq!(
            project_identity(&work).project_key,
            "worktree.example/acme/app",
            "and the main checkout keeps reading its own"
        );
    }

    /// A worktree is on its own branch, and `onbranch:` has to know that.
    ///
    /// A linked worktree shares the repository's `config` and keeps its own
    /// `HEAD`. Reading the branch from the shared directory answers about the
    /// main checkout — a different branch, which is the entire reason the
    /// worktree exists — so a conditional include git applies here would be
    /// skipped and the repository would drop back to a path key.
    #[test]
    fn a_linked_worktree_resolves_an_onbranch_include_against_its_own_branch() {
        let temp = tempdir().unwrap();
        let root = temp.path();

        // The main checkout, on `main`, whose config carries the rewrite
        // behind an `onbranch:feature` condition.
        let main_tree = root.join("app");
        let git = main_tree.join(".git");
        fs::create_dir_all(&git).unwrap();
        fs::write(git.join("HEAD"), "ref: refs/heads/main\n").unwrap();
        fs::write(
            root.join("feature-identity"),
            "[url \"git@github.com:acme/\"]\n\tinsteadOf = acme:\n",
        )
        .unwrap();
        fs::write(
            git.join("config"),
            format!(
                "[remote \"origin\"]\n\turl = acme:app.git\n\
                 [includeIf \"onbranch:feature\"]\n\tpath = {}\n",
                root.join("feature-identity").display()
            ),
        )
        .unwrap();

        // The linked worktree, on `feature`, sharing that config through
        // `commondir` and keeping its own `HEAD`.
        let worktree_git = git.join("worktrees/feature");
        fs::create_dir_all(&worktree_git).unwrap();
        fs::write(worktree_git.join("HEAD"), "ref: refs/heads/feature\n").unwrap();
        fs::write(worktree_git.join("commondir"), "../..\n").unwrap();
        let worktree = root.join("app-feature");
        fs::create_dir_all(&worktree).unwrap();
        fs::write(
            worktree.join(".git"),
            format!("gitdir: {}\n", worktree_git.display()),
        )
        .unwrap();

        // On `main` the condition does not hold, so the shorthand stays
        // unresolvable — which is also what makes the next assertion mean
        // something.
        assert_eq!(
            project_identity(&main_tree).method,
            ProjectKeyMethod::PathFallback
        );
        let resolved = project_identity(&worktree);
        assert_eq!(
            (resolved.project_key.as_str(), resolved.method),
            ("github.com/acme/app", ProjectKeyMethod::Remote),
            "the worktree was asked about the main checkout's branch"
        );

        // --- and `gitdir:` is the worktree's own git dir, not the shared one --
        //
        // The same mistake in the other direction: matched against `commondir`
        // every worktree looks like the main checkout, so an include scoped to
        // the main checkout applies to all of them and one scoped to a
        // worktree applies to none.
        fs::write(
            git.join("config"),
            format!(
                "[remote \"origin\"]\n\turl = acme:app.git\n\
                 [includeIf \"gitdir:{}/worktrees/\"]\n\tpath = {}\n",
                git.display(),
                root.join("feature-identity").display()
            ),
        )
        .unwrap();
        assert_eq!(
            project_identity(&worktree).project_key,
            "github.com/acme/app",
            "a condition scoped to the worktree's own git dir was not applied"
        );
        assert_eq!(
            project_identity(&main_tree).method,
            ProjectKeyMethod::PathFallback,
            "a worktree-scoped condition must not apply to the main checkout"
        );
    }

    #[test]
    fn an_onbranch_condition_reads_head() {
        let temp = tempdir().unwrap();
        let git_dir = temp.path().join(".git");
        fs::create_dir_all(&git_dir).unwrap();
        fs::write(git_dir.join("HEAD"), "ref: refs/heads/release/1.x\n").unwrap();
        assert_eq!(head_branch(&git_dir).as_deref(), Some("release/1.x"));
        let context = IncludeContext {
            git_dir: git_dir.clone(),
            branch: head_branch(&git_dir),
        };
        assert!(include_condition_holds(
            "onbranch:release/",
            &git_dir.join("config"),
            &context
        ));
        assert!(!include_condition_holds(
            "onbranch:main",
            &git_dir.join("config"),
            &context
        ));
        // A detached HEAD names no branch, and an unevaluated condition is
        // false rather than assumed.
        fs::write(
            git_dir.join("HEAD"),
            "9fceb02d0ae598e95dc970b74767f19372d61af8\n",
        )
        .unwrap();
        assert_eq!(head_branch(&git_dir), None);
    }

    #[test]
    fn a_condition_this_module_does_not_evaluate_is_false() {
        let temp = tempdir().unwrap();
        let context = IncludeContext {
            git_dir: temp.path().to_path_buf(),
            branch: Some("main".to_string()),
        };
        assert!(!include_condition_holds(
            "hasconfig:remote.*.url:https://github.com/**",
            &temp.path().join("config"),
            &context
        ));
    }

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
        assert_eq!(
            cfg["core"]["repositoryformatversion"],
            vec!["0".to_string()]
        );
        assert_eq!(
            single(&cfg["remote \"origin\""], "url").map(String::as_str),
            Some("git@github.com:foo/bar.git")
        );
    }

    #[test]
    fn parse_ignores_comments_and_blanks() {
        let cfg = parse_git_config(
            "\n# a comment\n; another comment\n[remote \"origin\"]\n\turl = https://github.com/foo/bar ; inline comment\n",
        );
        assert_eq!(
            single(&cfg["remote \"origin\""], "url").map(String::as_str),
            Some("https://github.com/foo/bar")
        );
    }

    #[test]
    fn parse_keeps_a_non_subsection_header_verbatim() {
        let cfg = parse_git_config("[branch]\n\tx = 1\n");
        assert_eq!(cfg["branch"]["x"], vec!["1".to_string()]);
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

    /// `git remote get-url origin` expands `insteadOf`, and the cloud outbox
    /// used to read the remote through exactly that command. A reader that
    /// skipped the rewrite would see `gh:Org/Repo.git`, fail to canonicalize
    /// it, and fall back to a path — reintroducing the fragmentation the key
    /// exists to end.
    #[test]
    fn insteadof_rewrites_are_expanded_like_git_does() {
        let root = tempdir().unwrap();
        let git_dir = root.path().join(".git");
        fs::create_dir_all(&git_dir).unwrap();
        fs::write(
            git_dir.join("config"),
            "[remote \"origin\"]\n\turl = gh:Org/Repo.git\n\
             [url \"https://github.com/\"]\n\tinsteadOf = gh:\n",
        )
        .unwrap();

        let identity = project_identity(root.path());
        assert_eq!(identity.project_key, "github.com/Org/Repo");
        assert_eq!(identity.method, ProjectKeyMethod::Remote);
        assert_eq!(
            identity.repo_url.as_deref(),
            Some("https://github.com/Org/Repo.git"),
            "the recorded remote is the expanded one, as `git remote get-url` reports it"
        );
    }

    #[test]
    fn insteadof_takes_the_longest_matching_prefix() {
        let config = parse_git_config(
            "[url \"https://github.com/\"]\n\tinsteadOf = gh:\n\
             [url \"https://github.com/me/\"]\n\tinsteadOf = gh:me/\n",
        );
        assert_eq!(
            apply_insteadof(&config, "gh:me/Repo.git"),
            "https://github.com/me/Repo.git"
        );
        assert_eq!(
            apply_insteadof(&config, "gh:Other/Repo.git"),
            "https://github.com/Other/Repo.git"
        );
        // A remote that matches no rewrite is returned untouched.
        assert_eq!(
            apply_insteadof(&config, "git@github.com:Org/Repo.git"),
            "git@github.com:Org/Repo.git"
        );
    }

    /// Git treats `insteadOf` as multi-valued: one `url` section may carry
    /// several rewrites. A config map that kept only the last would drop the
    /// others silently, and every remote they covered would fall back to a
    /// path key -- resolved-looking, and wrong.
    #[test]
    fn several_insteadof_values_in_one_section_all_apply() {
        let config = parse_git_config(
            "[url \"https://github.com/\"]\n\tinsteadOf = gh:\n\tinsteadOf = github:\n",
        );
        assert_eq!(
            apply_insteadof(&config, "gh:Org/Repo.git"),
            "https://github.com/Org/Repo.git"
        );
        assert_eq!(
            apply_insteadof(&config, "github:Org/Repo.git"),
            "https://github.com/Org/Repo.git",
            "the second value must not have been overwritten by the first"
        );
    }

    #[test]
    fn several_insteadof_values_still_take_the_longest_match() {
        let config = parse_git_config(
            "[url \"https://example.invalid/\"]\n\tinsteadOf = gh:\n\tinsteadOf = x:\n\
             [url \"https://github.com/me/\"]\n\tinsteadOf = gh:me/\n",
        );
        assert_eq!(
            apply_insteadof(&config, "gh:me/Repo.git"),
            "https://github.com/me/Repo.git"
        );
    }

    /// The counterpart rule for a single-valued key: git takes the last one.
    #[test]
    fn a_repeated_remote_url_takes_the_last_value() {
        let config = parse_git_config(
            "[remote \"origin\"]\n\turl = git@github.com:Org/Old.git\n\
             \turl = git@github.com:Org/New.git\n",
        );
        assert_eq!(
            single(&config["remote \"origin\""], "url").map(String::as_str),
            Some("git@github.com:Org/New.git")
        );
    }

    /// Git's case rules are not uniform: section and variable names are
    /// case-insensitive, subsection names are case-sensitive. A reader that
    /// matched `remote "origin"` and `url` literally would see no remote in a
    /// perfectly ordinary hand-edited config and fall back to a path key --
    /// resolved-looking, and wrong.
    #[test]
    fn mixed_case_sections_and_variables_still_resolve() {
        let root = tempdir().unwrap();
        let git_dir = root.path().join(".git");
        fs::create_dir_all(&git_dir).unwrap();
        fs::write(
            git_dir.join("config"),
            "[Remote \"origin\"]\n\tURL = gh:Org/Repo.git\n\
             [URL \"https://github.com/\"]\n\tInsteadOf = gh:\n",
        )
        .unwrap();

        let identity = project_identity(root.path());
        assert_eq!(identity.project_key, "github.com/Org/Repo");
        assert_eq!(identity.method, ProjectKeyMethod::Remote);
    }

    #[test]
    fn a_section_name_is_lowercased_while_its_subsection_is_not() {
        let config = parse_git_config("[Remote \"Origin\"]\n\tUrl = git@github.com:Org/Repo.git\n");
        // The section name folds...
        assert!(config.contains_key("remote \"Origin\""));
        // ...and the subsection does not: `Origin` is a different remote from
        // `origin`, which is why this cannot simply lowercase the whole header.
        assert!(!config.contains_key("remote \"origin\""));
        assert_eq!(
            single(&config["remote \"Origin\""], "URL").map(String::as_str),
            Some("git@github.com:Org/Repo.git"),
            "variable names are case-insensitive"
        );
    }

    /// The corollary: a remote whose subsection is not exactly `origin` is not
    /// the origin, so the key falls back rather than silently adopting it.
    #[test]
    fn a_differently_cased_subsection_is_a_different_remote() {
        let root = tempdir().unwrap();
        let git_dir = root.path().join(".git");
        fs::create_dir_all(&git_dir).unwrap();
        fs::write(
            git_dir.join("config"),
            "[remote \"Origin\"]\n\turl = git@github.com:Org/Repo.git\n",
        )
        .unwrap();
        assert_eq!(
            project_identity(root.path()).method,
            ProjectKeyMethod::PathFallback
        );
    }

    #[test]
    fn pushinsteadof_does_not_change_the_identity_of_a_repository() {
        let config = parse_git_config("[url \"ssh://git@github.com/\"]\n\tpushInsteadOf = gh:\n");
        assert_eq!(
            apply_insteadof(&config, "gh:Org/Repo.git"),
            "gh:Org/Repo.git"
        );
    }

    /// A process that stays up across many acquisition passes -- `watch`, the
    /// Node addon, a desktop host -- must not keep answering from a checkout's
    /// state at the first one. A repository that gains an `origin` would
    /// otherwise carry a path key for the life of the process, and the stale
    /// answer is indistinguishable from a correct one.
    #[test]
    fn a_new_acquisition_pass_sees_a_repository_that_gained_a_remote() {
        let root = tempdir().unwrap();
        let cwd = root.path().to_string_lossy().to_string();
        // Seed the global cache with the directory as it stands: no remote.
        assert_eq!(
            resolve_project_identity(&cwd).method,
            ProjectKeyMethod::PathFallback
        );

        let git_dir = root.path().join(".git");
        fs::create_dir_all(&git_dir).unwrap();
        fs::write(
            git_dir.join("config"),
            "[remote \"origin\"]\n\turl = git@github.com:Org/Repo.git\n",
        )
        .unwrap();

        // Deliberately no assertion that the stale answer survives until the
        // pass ends: the global resolver is shared with every other test in
        // this binary, so "still cached" is not something one test can claim
        // without asserting something about the others. `resolver_memoizes`
        // pins the caching itself on a resolver it owns. What matters here is
        // the recovery, and clearing is idempotent, so a concurrent clear can
        // only make this pass sooner.
        begin_acquisition_pass();
        let identity = resolve_project_identity(&cwd);
        assert_eq!(identity.project_key, "github.com/Org/Repo");
        assert_eq!(identity.method, ProjectKeyMethod::Remote);
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

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

/// Git's configuration as it applies inside `git_dir`: the system scope,
/// then the global scopes, then the repository's own, with `include` and
/// `includeIf` expanded.
///
/// Precedence is expressed by append order — [`single`] takes the last value —
/// so a repository's `remote.origin.url` overrides a global one, while
/// multi-valued keys such as `insteadOf` accumulate across every scope, which
/// is what git does with them. Includes are expanded at the position of their
/// own line, for the same reason and with the same consequence if they are
/// not.
///
/// `head_dir` is separate from `git_dir` because a linked worktree shares the
/// repository's `config` and keeps its own `HEAD`: the branch an
/// `includeIf "onbranch:"` asks about is the worktree's, not the main
/// checkout's.
fn load_git_config(git_dir: &Path, head_dir: &Path) -> GitConfig {
    let context = IncludeContext {
        git_dir: head_dir
            .canonicalize()
            .unwrap_or_else(|_| head_dir.to_path_buf()),
        branch: head_branch(head_dir),
    };
    let mut merged = GitConfig::new();
    if let Some(system) = system_config_path() {
        load_config_file(&system, &context, 0, &mut merged);
    }
    for global in global_config_paths() {
        load_config_file(&global, &context, 0, &mut merged);
    }
    load_config_file(&git_dir.join("config"), &context, 0, &mut merged);
    merged
}

/// What an `includeIf` condition is asked about.
struct IncludeContext {
    git_dir: PathBuf,
    branch: Option<String>,
}

/// Git's own cap on include nesting. A configuration that exceeds it is
/// malformed for git too, so stopping is the same answer git gives.
const MAX_INCLUDE_DEPTH: usize = 10;

/// Read one config file into `out`, expanding each include **where its line
/// appears**.
///
/// Appending in file order is what makes [`single`] — which takes the last
/// value — agree with git: a variable set before an `include` is overridden by
/// the included file, and one set after it overrides the include. Reading the
/// whole file into a map first and merging it over its includes inverts that
/// for every key, `remote.origin.url` included, so the reader would answer
/// with a URL `git remote get-url origin` does not return.
fn load_config_file(path: &Path, context: &IncludeContext, depth: usize, out: &mut GitConfig) {
    let Ok(text) = fs::read_to_string(path) else {
        return;
    };
    read_git_config(
        &text,
        &mut |section: &str, entry: Option<(&str, String)>| {
            let Some((key, value)) = entry else {
                out.entry(section.to_string()).or_default();
                return;
            };
            if depth < MAX_INCLUDE_DEPTH && key.eq_ignore_ascii_case("path") {
                if let Some(included) = included_path(section, &value, path, context) {
                    load_config_file(&included, context, depth + 1, out);
                }
            }
            out.entry(section.to_string())
                .or_default()
                .entry(key.to_string())
                .or_default()
                .push(value);
        },
    );
}

/// The file an `include.path` / `includeIf.<condition>.path` line pulls in,
/// when its section is an include at all and its condition holds here.
fn included_path(
    section: &str,
    value: &str,
    from: &Path,
    context: &IncludeContext,
) -> Option<PathBuf> {
    let applies = if section == "include" {
        true
    } else {
        let condition = section
            .strip_prefix("includeif \"")
            .and_then(|rest| rest.strip_suffix('"'))?;
        include_condition_holds(condition, from, context)
    };
    applies.then(|| resolve_config_path(value, from))?
}

/// Evaluate one `includeIf` condition.
///
/// `gitdir:` / `gitdir/i:` match the repository's own `.git` directory, and
/// `onbranch:` the branch `HEAD` points at. `hasconfig:remote.*.url:` is
/// deliberately not evaluated: git resolves it against the configuration read
/// so far, which is a different and order-dependent question, and treating an
/// unevaluated condition as *false* only ever leaves a remote unrewritten —
/// the same answer this module gave before it followed includes at all.
fn include_condition_holds(condition: &str, from: &Path, context: &IncludeContext) -> bool {
    let (keyword, pattern) = match condition.split_once(':') {
        Some(split) => split,
        None => return false,
    };
    match keyword {
        "gitdir" => gitdir_condition(pattern, from, context, false),
        "gitdir/i" => gitdir_condition(pattern, from, context, true),
        "onbranch" => {
            let Some(branch) = context.branch.as_deref() else {
                return false;
            };
            let pattern = if pattern.ends_with('/') {
                format!("{pattern}**")
            } else {
                pattern.to_string()
            };
            wildmatch(&pattern, branch, false)
        }
        _ => false,
    }
}

fn gitdir_condition(pattern: &str, from: &Path, context: &IncludeContext, fold_case: bool) -> bool {
    // git's documented expansions, in order: `~/` is the home directory, `./`
    // is relative to the including file, a pattern that is not anchored at all
    // matches at any depth, and a trailing `/` matches everything below.
    let mut expanded = if let Some(rest) = pattern.strip_prefix("~/") {
        match home_dir() {
            Some(home) => format!("{}/{rest}", home.to_string_lossy()),
            None => return false,
        }
    } else if let Some(rest) = pattern.strip_prefix("./") {
        match from.parent() {
            Some(dir) => format!("{}/{rest}", dir.to_string_lossy()),
            None => return false,
        }
    } else if pattern.starts_with('/') {
        pattern.to_string()
    } else {
        format!("**/{pattern}")
    };
    if expanded.ends_with('/') {
        expanded.push_str("**");
    }
    let git_dir = context.git_dir.to_string_lossy().replace('\\', "/");
    wildmatch(&expanded, &git_dir, fold_case)
}

/// The branch `HEAD` names, when it names one.
fn head_branch(git_dir: &Path) -> Option<String> {
    let text = fs::read_to_string(git_dir.join("HEAD")).ok()?;
    let reference = text.trim().strip_prefix("ref:")?.trim();
    Some(reference.strip_prefix("refs/heads/")?.to_string())
}

/// Resolve a `path` value: `~` for the home directory, and anything relative
/// taken from the including file's directory, as git does.
fn resolve_config_path(value: &str, from: &Path) -> Option<PathBuf> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    if let Some(rest) = value.strip_prefix("~/") {
        return Some(home_dir()?.join(rest));
    }
    let path = Path::new(value);
    if path.is_absolute() {
        return Some(path.to_path_buf());
    }
    Some(from.parent()?.join(path))
}

/// `$GIT_CONFIG_SYSTEM`, or `/etc/gitconfig`, unless `$GIT_CONFIG_NOSYSTEM`
/// says to read neither — the same three rules git applies.
fn system_config_path() -> Option<PathBuf> {
    if std::env::var_os("GIT_CONFIG_NOSYSTEM").is_some_and(|value| {
        let value = value.to_string_lossy().to_ascii_lowercase();
        !matches!(value.as_str(), "" | "0" | "false" | "no" | "off")
    }) {
        return None;
    }
    if let Some(explicit) = std::env::var_os("GIT_CONFIG_SYSTEM") {
        let path = PathBuf::from(explicit);
        return (!path.as_os_str().is_empty()).then_some(path);
    }
    Some(PathBuf::from("/etc/gitconfig"))
}

/// The global scope: `$GIT_CONFIG_GLOBAL` when set, otherwise
/// `$XDG_CONFIG_HOME/git/config` and then `~/.gitconfig`, which git reads in
/// that order so the home file wins.
fn global_config_paths() -> Vec<PathBuf> {
    if let Some(explicit) = std::env::var_os("GIT_CONFIG_GLOBAL") {
        let path = PathBuf::from(explicit);
        // git treats `/dev/null` as "no global config"; any unreadable path
        // has the same effect here, since the read simply yields nothing.
        return if path.as_os_str().is_empty() {
            Vec::new()
        } else {
            vec![path]
        };
    }
    let mut out = Vec::new();
    let xdg = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .filter(|path| !path.as_os_str().is_empty())
        .or_else(|| home_dir().map(|home| home.join(".config")));
    if let Some(xdg) = xdg {
        out.push(xdg.join("git/config"));
    }
    if let Some(home) = home_dir() {
        out.push(home.join(".gitconfig"));
    }
    out
}

fn home_dir() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .filter(|path| !path.as_os_str().is_empty())
}

/// Append `incoming` over `base`, keeping every value.
///
/// Append rather than replace because git's scopes do not replace one another:
/// a single-valued key resolves to the last one written, which [`single`]
/// takes, while a multi-valued one — `insteadOf`, the reason this function
/// exists — is the union of every scope that sets it.
#[cfg(test)]
fn merge_config(base: &mut GitConfig, incoming: GitConfig) {
    for (section, entries) in incoming {
        let target = base.entry(section).or_default();
        for (key, values) in entries {
            target.entry(key).or_default().extend(values);
        }
    }
}

/// git's `wildmatch` with `WM_PATHNAME`, which is what config conditions are
/// matched with: `*` and `?` stay inside one path component, `**` crosses
/// them.
fn wildmatch(pattern: &str, text: &str, fold_case: bool) -> bool {
    let (pattern, text) = if fold_case {
        (pattern.to_lowercase(), text.to_lowercase())
    } else {
        (pattern.to_string(), text.to_string())
    };
    wildmatch_bytes(pattern.as_bytes(), text.as_bytes())
}

fn wildmatch_bytes(pattern: &[u8], text: &[u8]) -> bool {
    let mut p = 0;
    let mut t = 0;
    while p < pattern.len() {
        match pattern[p] {
            b'*' => {
                let double = pattern.get(p + 1) == Some(&b'*');
                let rest = if double {
                    &pattern[p + 2..]
                } else {
                    &pattern[p + 1..]
                };
                // `**/` consumes whole components, including none of them.
                let rest = if double && rest.first() == Some(&b'/') {
                    if wildmatch_bytes(&rest[1..], &text[t..]) {
                        return true;
                    }
                    rest
                } else {
                    rest
                };
                let mut at = t;
                loop {
                    if wildmatch_bytes(rest, &text[at..]) {
                        return true;
                    }
                    if at >= text.len() {
                        return false;
                    }
                    if !double && text[at] == b'/' {
                        return false;
                    }
                    at += 1;
                }
            }
            b'?' => {
                if t >= text.len() || text[t] == b'/' {
                    return false;
                }
                p += 1;
                t += 1;
            }
            literal => {
                if t >= text.len() || text[t] != literal {
                    return false;
                }
                p += 1;
                t += 1;
            }
        }
    }
    t == text.len()
}

/// Parse `.git/config` text into `{section -> {key -> value}}`.
///
/// Handles `[section]` and `[section "subsection"]` headers, skips `#` / `;`
/// comment and blank lines, and strips inline comments that fall outside
/// quotes. Deliberately not a full git-config implementation: only enough to
/// read `remote "origin"`'s `url`, and byte-for-byte the same subset burn
/// reads so the two cannot disagree about a config they both parse.
pub fn parse_git_config(text: &str) -> GitConfig {
    let mut out: GitConfig = HashMap::new();
    read_git_config(
        text,
        &mut |section: &str, entry: Option<(&str, String)>| match entry {
            // A header with no variables under it is still a section git saw.
            None => {
                out.entry(section.to_string()).or_default();
            }
            Some((key, value)) => out
                .entry(section.to_string())
                .or_default()
                .entry(key.to_string())
                .or_default()
                .push(value),
        },
    );
    out
}

/// What [`read_git_config`] reports as it walks a file: a section header, then
/// each variable written under it.
trait ConfigVisitor {
    fn visit(&mut self, section: &str, entry: Option<(&str, String)>);
}

impl<F: FnMut(&str, Option<(&str, String)>)> ConfigVisitor for F {
    fn visit(&mut self, section: &str, entry: Option<(&str, String)>) {
        self(section, entry)
    }
}

/// Walk a config file's variables **in the order they are written**.
///
/// The order is not a detail: an `include` takes effect where its line
/// appears, so a variable written before it can be overridden by the included
/// file and one written after it overrides the include. Collecting the file
/// into a map first and merging afterwards loses exactly that, and for
/// `remote.origin.url` it loses the identity of the repository — the reader
/// would answer with a URL `git remote get-url origin` does not return.
///
/// `visit` is called with the current section and either `None` when a section
/// header opens, or `Some((key, value))` for each variable. A trait rather than
/// a closure type so the signature stays readable; every caller passes a
/// closure.
fn read_git_config(text: &str, visit: &mut dyn ConfigVisitor) {
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
            visit.visit(&name, None);
            current = Some(name);
            continue;
        }
        let Some(section) = current.as_ref() else {
            continue;
        };
        let Some(eq) = line.find('=') else { continue };
        let key = line[..eq].trim();
        if key.is_empty() {
            continue;
        }
        let value = strip_inline_comment(line[eq + 1..].trim());
        // Variable names are case-insensitive in git, so they are folded here
        // rather than compared case-insensitively at each lookup. Folding is
        // what keeps `URL` and `url` in *one* list in the order they were
        // written: two differently spelled keys would otherwise be two
        // separate lists, and which came first in the file would be
        // unknowable — and for a remote's URL that is the difference between
        // the repository and one of its mirrors.
        visit.visit(section, Some((&key.to_ascii_lowercase(), value)));
    }
}

/// A parsed `.git/config`: `{section -> {key -> values}}`.
///
/// Keys are multi-valued because git's are. `url.<base>.insteadOf` is the case
/// that matters here — one `url` section may carry several rewrites, and a map
/// keeping only the last would silently drop all but one, leaving every remote
/// the others covered to fall back to a path key.
///
/// Single-valued keys like `remote.origin.url` take the last entry, which is
/// git's own rule for them.
///
/// Section names arrive lowercased (see [`section_name`]) and subsection names
/// verbatim, and variable names lowercased too, matching git's own case rules.
/// `URL` and `url` are therefore one key — and one *ordered* list, which is
/// what lets a remote's first URL be identified as such.
pub type GitConfig = HashMap<String, HashMap<String, Vec<String>>>;

/// The URL a remote resolves to: the **first** one configured for it.
///
/// `remote.<name>.url` is not a single-valued key. Git accumulates every value
/// into the remote's URL list, in the order it reads them — across scopes as
/// well as within a file — and `git remote get-url <name>` prints the first;
/// the rest are mirrors that `--all` lists. Taking the last, as a single-
/// valued key would, names the mirror instead of the repository. Verified
/// against git 2.43 rather than assumed, because this helper replaced a
/// `git remote get-url origin` subprocess and a disagreement here is a
/// silently different project key:
///
/// ```text
/// [remote "origin"]
///     url = https://github.com/acme/main.git
///     url = https://mirror.example/acme/main.git
/// $ git remote get-url origin
/// https://github.com/acme/main.git
/// ```
///
/// (`git config --get remote.origin.url` does answer with the last — it
/// applies the generic single-valued rule and knows nothing about remotes.
/// The identity of a remote is what `get-url` reports.)
///
/// Variable names are lowercased when parsed, so `URL` and `url` are one list
/// in the order they were written rather than two that lose their order
/// against each other.
fn remote_url<'a>(config: &'a GitConfig, remote: &str) -> Option<&'a String> {
    config
        .get(&format!("remote \"{remote}\""))?
        .get("url")?
        .first()
}

/// The effective value of a genuinely single-valued key: the last written, as
/// git resolves it.
#[cfg(test)]
fn single<'a>(section: &'a HashMap<String, Vec<String>>, key: &str) -> Option<&'a String> {
    section
        .get(&key.to_ascii_lowercase())
        .and_then(|values| values.last())
}

/// Normalize a section header body, mirroring `^([A-Za-z0-9._-]+)\s+"(.*)"$`.
/// A header that does not match that shape is kept verbatim, as burn keeps it.
/// Normalize a section header body.
///
/// Git's case rules are not uniform and the difference is load-bearing here:
/// **section names are case-insensitive, subsection names are case-sensitive**.
/// So `[Remote "origin"]` is the same section as `[remote "origin"]`, while
/// `[remote "Origin"]` is a different remote entirely. The name is lowercased
/// and the subsection kept verbatim, which lets every lookup below use one
/// spelling without flattening a distinction git makes.
///
/// A header that is not the `name "subsection"` shape keeps its whole body,
/// lowercased — it is a plain section name, and those are case-insensitive too.
fn section_name(raw: &str) -> String {
    let Some(quote) = raw.find('"') else {
        return raw.to_ascii_lowercase();
    };
    if !raw.ends_with('"') || raw.len() < quote + 2 {
        return raw.to_ascii_lowercase();
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
        return raw.to_ascii_lowercase();
    }
    // `(.*)` is greedy, so the subsection runs from the first quote to the
    // last one, embedded quotes and all.
    let subsection = &tail[1..tail.len() - 1];
    format!("{} \"{subsection}\"", name.to_ascii_lowercase())
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

/// Expand a `url.<base>.insteadOf` rewrite over a configured remote.
///
/// `git remote get-url origin` performs this rewrite, so a repository whose
/// `origin` is stored as `gh:Org/Repo.git` with
/// `url."https://github.com/".insteadOf = gh:` reports the expanded URL. Any
/// reader that skips it sees an unrecognizable remote and falls back to the
/// working directory — which is the fragmentation a canonical key exists to
/// prevent, and would have been a regression in the cloud outbox, whose
/// previous subprocess did expand it.
///
/// Git's rule is longest-match-wins, so a configuration with both `gh:` and
/// `gh:me/` rewrites resolves against the more specific one. `pushInsteadOf`
/// is deliberately not applied: it changes only where pushes go, never the
/// identity of the repository being read.
///
/// This is a superset of burn's `reader/git.rs`, which does not implement the
/// rewrite. The two still agree on every remote that needs no rewriting; for
/// one that does, burn falls back to a path key and this returns the canonical
/// one. That is a strictly better answer rather than a disagreement to
/// preserve, and the burn characterization issue should adopt the same rule.
fn apply_insteadof(config: &GitConfig, url: &str) -> String {
    let mut best: Option<(&str, &str)> = None;
    for (section, entries) in config {
        let Some(base) = section
            .strip_prefix("url \"")
            .and_then(|rest| rest.strip_suffix('"'))
        else {
            continue;
        };
        // Every `insteadOf` in the section, not merely one: git treats the key
        // as multi-valued, so one base may carry several rewrites. Names are
        // folded when parsed, so the several spellings hand-edited files use
        // are already one key here.
        let prefixes = entries.get("insteadof").into_iter().flatten();
        for prefix in prefixes {
            if prefix.is_empty() || !url.starts_with(prefix.as_str()) {
                continue;
            }
            if best.is_none_or(|(longest, _)| prefix.len() > longest.len()) {
                best = Some((prefix.as_str(), base));
            }
        }
    }
    match best {
        Some((prefix, base)) => format!("{base}{}", &url[prefix.len()..]),
        None => url.to_string(),
    }
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

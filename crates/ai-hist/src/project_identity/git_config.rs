use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

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
pub(super) fn load_git_config(git_dir: &Path, head_dir: &Path) -> GitConfig {
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
    // The repository's own scope is read into a map of its own so that
    // `extensions.worktreeConfig` can be asked of *it*: extensions are
    // repository-scoped in git, and a global one must not switch on a
    // per-worktree file.
    let mut repository = GitConfig::new();
    load_config_file(&git_dir.join("config"), &context, 0, &mut repository);
    let worktree_config = config_is_true(&repository, "extensions", "worktreeconfig");
    merge_config(&mut merged, repository);
    if worktree_config {
        // Read after the shared config, which is where git reads it: its
        // values win a single-valued key and its rewrites add to the shared
        // ones. Note what this does *not* change — a remote's URL list still
        // begins with the shared entry, so `get-url` (and this module) answer
        // with the shared URL even when a worktree appends its own. Checked
        // against git 2.43; the worktree file's reach here is every other
        // key, `insteadOf` included.
        load_config_file(&head_dir.join("config.worktree"), &context, 0, &mut merged);
    }
    merged
}

/// A git boolean, as git reads one: `true`, `yes`, `on` and `1` are true, and
/// anything else — including an unset key — is false.
fn config_is_true(config: &GitConfig, section: &str, key: &str) -> bool {
    config
        .get(section)
        .and_then(|entries| entries.get(key))
        .and_then(|values| values.last())
        .is_some_and(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "true" | "yes" | "on" | "1"
            )
        })
}

/// What an `includeIf` condition is asked about.
pub(super) struct IncludeContext {
    pub(super) git_dir: PathBuf,
    pub(super) branch: Option<String>,
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
pub(super) fn load_config_file(
    path: &Path,
    context: &IncludeContext,
    depth: usize,
    out: &mut GitConfig,
) {
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
pub(super) fn include_condition_holds(
    condition: &str,
    from: &Path,
    context: &IncludeContext,
) -> bool {
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
pub(super) fn head_branch(git_dir: &Path) -> Option<String> {
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
pub(super) fn merge_config(base: &mut GitConfig, incoming: GitConfig) {
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
pub(super) fn wildmatch(pattern: &str, text: &str, fold_case: bool) -> bool {
    let (pattern, text) = if fold_case {
        (pattern.to_lowercase(), text.to_lowercase())
    } else {
        (pattern.to_string(), text.to_string())
    };
    wildmatch_bytes(pattern.as_bytes(), text.as_bytes())
}

fn wildmatch_bytes(pattern: &[u8], text: &[u8]) -> bool {
    let mut t = 0;
    for (p, &token) in pattern.iter().enumerate() {
        if token == b'*' {
            return wildmatch_star(&pattern[p..], &text[t..]);
        }
        match text.get(t) {
            Some(&byte) if token_matches_byte(token, byte) => t += 1,
            _ => return false,
        }
    }
    t == text.len()
}

/// One non-`*` pattern byte against one text byte: `?` is any byte but `/`.
fn token_matches_byte(token: u8, byte: u8) -> bool {
    if token == b'?' {
        byte != b'/'
    } else {
        byte == token
    }
}

/// Match `pattern`, which starts at a `*` or `**`, against `text`.
fn wildmatch_star(pattern: &[u8], text: &[u8]) -> bool {
    let double = pattern.get(1) == Some(&b'*');
    let rest = if double { &pattern[2..] } else { &pattern[1..] };
    // `**/` consumes whole components, including none of them.
    if double && rest.first() == Some(&b'/') && wildmatch_bytes(&rest[1..], text) {
        return true;
    }
    let mut at = 0;
    loop {
        if wildmatch_bytes(rest, &text[at..]) {
            return true;
        }
        if at >= text.len() || (!double && text[at] == b'/') {
            return false;
        }
        at += 1;
    }
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
    for logical in logical_config_lines(text) {
        let line = logical
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
        let value = parse_config_value(&line[eq + 1..]);
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

/// Join physical lines into the logical ones git reads.
///
/// A value may be continued onto the next line with a trailing backslash, and
/// git joins the two with nothing between them. Iterating physical lines
/// instead leaves the backslash in the value: a remote written as
///
/// ```text
///     url = https://github.com/acme/long\
/// /path.git
/// ```
///
/// yields `github.com/acme/long\` here while `git remote get-url origin`
/// yields `https://github.com/acme/long/path.git` — so the same repository
/// splits into two projects depending on how its config happens to be
/// wrapped. Verified against git 2.43.
///
/// The backslash only continues when it is not itself escaped, so a trailing
/// `\\` ends the line and leaves a literal backslash in the value, which git
/// also does.
///
/// One documented difference: git rejects an entire file whose *section
/// header* is continued this way (`fatal: bad config line`), while this joins
/// the header and carries on. Refusing everything would throw away every
/// rewrite in a global file over one malformed line, which is a worse failure
/// than reading it.
fn logical_config_lines(text: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut pending: Option<String> = None;
    for raw in text.split('\n') {
        let line = raw.trim_end_matches('\r');
        let continued = trailing_backslashes(line) % 2 == 1;
        let body = if continued {
            &line[..line.len() - 1]
        } else {
            line
        };
        match pending.as_mut() {
            Some(buffer) => buffer.push_str(body),
            None => pending = Some(body.to_string()),
        }
        if !continued {
            out.push(pending.take().unwrap_or_default());
        }
    }
    if let Some(last) = pending {
        out.push(last);
    }
    out
}

fn trailing_backslashes(line: &str) -> usize {
    line.chars().rev().take_while(|c| *c == '\\').count()
}

/// Read one variable's value the way git reads it.
///
/// Quoted runs keep their whitespace and treat `#` and `;` as ordinary
/// characters; outside quotes those start a comment. `\n`, `\t`, `\b`, `\"` and
/// `\\` are escapes on both sides of a quote. Leading and trailing whitespace
/// is dropped, and each whitespace character *inside* the value becomes a
/// single space — a tab between two words comes back as one space, while two
/// spaces stay two, which is what git 2.43 does.
///
/// An unrecognized escape keeps the character that follows it rather than
/// rejecting the line: git errors there, and a reader whose job is to answer
/// "which project is this" should not decide a whole file is unreadable over
/// one variable it will probably never look at.
fn parse_config_value(raw: &str) -> String {
    let mut out = String::new();
    let mut pending_space = 0usize;
    let mut in_quotes = false;
    let mut chars = raw.chars();
    while let Some(ch) = chars.next() {
        match ch {
            '\\' => {
                let Some(escaped) = chars.next() else { break };
                let decoded = match escaped {
                    'n' => '\n',
                    't' => '\t',
                    'b' => '\u{8}',
                    other => other,
                };
                flush_spaces(&mut out, &mut pending_space);
                out.push(decoded);
            }
            '"' => in_quotes = !in_quotes,
            '#' | ';' if !in_quotes => break,
            c if c.is_whitespace() && !in_quotes => pending_space += 1,
            c => {
                flush_spaces(&mut out, &mut pending_space);
                out.push(c);
            }
        }
    }
    out
}

/// Emit whitespace that was held back, now that something follows it.
///
/// Held back rather than written as it is read, so that whitespace at the end
/// of a value disappears with it — and dropped entirely before the first
/// character, which is what makes `key = value` and `key=value` the same.
fn flush_spaces(out: &mut String, pending: &mut usize) {
    if !out.is_empty() {
        out.extend(std::iter::repeat_n(' ', *pending));
    }
    *pending = 0;
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
pub(super) fn remote_url<'a>(config: &'a GitConfig, remote: &str) -> Option<&'a String> {
    config
        .get(&format!("remote \"{remote}\""))?
        .get("url")?
        .first()
}

/// Normalize a section header body.
///
/// Git's case rules are not uniform and the difference is load-bearing here:
/// **section names are case-insensitive, subsection names are case-sensitive**.
/// So `[Remote "origin"]` is the same section as `[remote "origin"]`, while
/// `[remote "Origin"]` is a different remote entirely. The name is lowercased
/// and the subsection kept verbatim, which lets every lookup below use one
/// spelling without flattening a distinction git makes.
///
/// git also accepts the legacy `[section.subsection]` spelling, and there the
/// *whole* header folds — `[remote.Origin]` and `[remote.origin]` are the same
/// remote, while quoted `[remote "Origin"]` is a different one. Checked
/// against git 2.43 rather than assumed, since the two forms have to land on
/// one internal spelling for a lookup to find either:
///
/// ```text
/// [remote.origin] url = …dotted…      $ git remote get-url origin → …dotted…
/// [remote.ORIGIN] url = …upper…       $ git config --get remote.origin.url
///                                       → …upper…  (same remote)
/// [remote "ORIGIN"] url = …quoted…    $ git config --get remote.ORIGIN.url
///                                       → …quoted… (a different remote)
/// ```
///
/// A header that is neither shape keeps its whole body, lowercased — it is a
/// plain section name, and those are case-insensitive too.
fn section_name(raw: &str) -> String {
    let Some(quote) = raw.find('"') else {
        return dotted_section_name(raw);
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

/// The legacy `[section.subsection]` form, folded onto the same internal
/// spelling the quoted form produces.
///
/// git accepts only `[A-Za-z0-9.-]` in this form and rejects the line
/// otherwise ("bad config line"), so a body outside that set is left as a
/// plain section name rather than being taken apart into a subsection this
/// module invented. The split is at the *first* dot: everything after it is
/// the subsection, dots and all.
fn dotted_section_name(raw: &str) -> String {
    let folded = raw.to_ascii_lowercase();
    let Some(dot) = folded.find('.') else {
        return folded;
    };
    if !folded
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-'))
    {
        return folded;
    }
    let (name, rest) = folded.split_at(dot);
    let subsection = &rest[1..];
    if name.is_empty() || subsection.is_empty() {
        return folded;
    }
    format!("{name} \"{subsection}\"")
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
pub(super) fn apply_insteadof(config: &GitConfig, url: &str) -> String {
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

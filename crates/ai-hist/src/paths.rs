//! Local provider locations shared by discovery, ingestion, and applications.
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

fn env_dir(var: &str) -> Option<PathBuf> {
    std::env::var_os(var).and_then(|value| {
        if value.to_string_lossy().trim().is_empty() {
            None
        } else {
            Some(PathBuf::from(value))
        }
    })
}

pub fn claude_config_dir(home: &Path) -> PathBuf {
    env_dir("CLAUDE_CONFIG_DIR").unwrap_or_else(|| home.join(".claude"))
}

pub fn codex_home(home: &Path) -> PathBuf {
    env_dir("CODEX_HOME").unwrap_or_else(|| home.join(".codex"))
}

pub fn grok_home(home: &Path) -> PathBuf {
    env_dir("GROK_HOME").unwrap_or_else(|| home.join(".grok"))
}

/// Where Muse Code keeps its session logs: `$XDG_DATA_HOME/muse/sessions`,
/// or `~/.local/share/muse/sessions` when `XDG_DATA_HOME` is unset. Muse uses
/// the same XDG layout on every platform.
pub fn muse_sessions_dir(home: &Path) -> PathBuf {
    match env_dir("XDG_DATA_HOME") {
        Some(data) => data.join("muse/sessions"),
        None => default_muse_sessions_dir(home),
    }
}

/// [`muse_sessions_dir`] with nothing read from the environment.
pub(crate) fn default_muse_sessions_dir(home: &Path) -> PathBuf {
    home.join(".local/share/muse/sessions")
}

pub fn opencode_db_path(home: &Path) -> PathBuf {
    env_dir("OPENCODE_DB")
        .unwrap_or_else(|| home.join(".local/share/opencode/opencode.db"))
}

/// Where OpenCode's legacy JSON tree lives: `session/<scope>/<id>.json`,
/// `message/<sessionId>/*.json`, `part/<messageId>/*.json`.
pub fn opencode_storage_dir(home: &Path) -> PathBuf {
    env_dir("OPENCODE_STORAGE_DIR")
        .unwrap_or_else(|| home.join(".local/share/opencode/storage"))
}

/// Where every local provider keeps its sessions.
///
/// One value, resolved once, drives `sync`, `hydrate`, `watch` and the
/// advertised watch roots alike — the same tree is scanned, hydrated and
/// watched, or a session the sweep catalogued cannot be hydrated afterwards.
/// Build it with [`ProviderRoots::from_env`] (the CLI's rules: the process
/// environment's `CLAUDE_CONFIG_DIR`, `CODEX_HOME`, `GROK_HOME`,
/// `XDG_DATA_HOME` (for Muse Code), `OPENCODE_DB`, `OPENCODE_STORAGE_DIR` and `TRAJECTORY_ROOT` override the
/// defaults under `home`) or [`ProviderRoots::from_home`] (the defaults under
/// `home`, with nothing read from the environment — what a test or an
/// embedder with its own layout wants). The environment is read **once**,
/// here; nothing on the sync, hydrate or watch paths consults it again.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ProviderRoots {
    /// The home the file-backed providers are rooted at.
    pub home: PathBuf,
    /// Claude Code configuration root (`~/.claude`).
    pub claude: PathBuf,
    /// Codex state root (`~/.codex`).
    pub codex: PathBuf,
    /// Grok state root (`~/.grok`).
    pub grok: PathBuf,
    /// Muse Code session logs (`~/.local/share/muse/sessions`).
    pub muse: PathBuf,
    /// The OpenCode SQLite store.
    pub opencode_db: PathBuf,
    /// OpenCode's legacy `storage/` JSON tree, read only when there is no
    /// `opencode.db`.
    pub opencode_storage_dir: PathBuf,
    /// Where trajectory records are read from. `Some` names the roots
    /// outright — `TRAJECTORY_ROOT` under [`ProviderRoots::from_env`], or
    /// whatever an embedder sets — each entry a `.trajectories` directory or
    /// a single JSON file; `None` derives them by finding `.trajectories`
    /// directories under `<home>/Projects` at scan time, so one created
    /// after the roots were built is still picked up.
    pub trajectory_roots: Option<Vec<PathBuf>>,
    /// Whether these roots came from environment overrides, so a sweep can
    /// say when a configured root does not exist rather than silently
    /// scanning nothing.
    pub use_env_roots: bool,
}

impl ProviderRoots {
    /// Roots under `home`, with the process environment's provider overrides
    /// applied — the CLI's resolution.
    pub fn from_env(home: PathBuf) -> Self {
        Self {
            claude: claude_config_dir(&home),
            codex: codex_home(&home),
            grok: grok_home(&home),
            muse: muse_sessions_dir(&home),
            opencode_db: opencode_db_path(&home),
            opencode_storage_dir: opencode_storage_dir(&home),
            trajectory_roots: trajectory_roots_from_env(),
            use_env_roots: true,
            home,
        }
    }

    /// Roots under `home` and the given OpenCode store, reading nothing from
    /// the environment.
    pub fn from_home(home: PathBuf, opencode_db: PathBuf) -> Self {
        let opencode_storage_dir = opencode_db
            .parent()
            .map(|parent| parent.join("storage"))
            .unwrap_or_else(|| home.join(".local/share/opencode/storage"));
        Self {
            claude: home.join(".claude"),
            codex: home.join(".codex"),
            grok: home.join(".grok"),
            muse: default_muse_sessions_dir(&home),
            home,
            opencode_db,
            opencode_storage_dir,
            trajectory_roots: None,
            use_env_roots: false,
        }
    }
}

/// `TRAJECTORY_ROOT` as a list of roots: a `PATH`-style list whose empty
/// entries are dropped. `None` when the variable is unset, which means
/// "derive from `<home>/Projects`"; `Some(vec![])` when it is set to nothing
/// but empties, which means "no trajectory roots at all" — the variable was
/// the operator's answer, and it said none.
fn trajectory_roots_from_env() -> Option<Vec<PathBuf>> {
    let raw = std::env::var_os("TRAJECTORY_ROOT")?;
    Some(
        std::env::split_paths(&raw)
            .filter(|part| !part.as_os_str().is_empty())
            .collect(),
    )
}

pub fn default_opencode_db_path() -> PathBuf {
    opencode_db_path(&home_dir())
}

pub fn default_opencode_storage_dir() -> PathBuf {
    opencode_storage_dir(&home_dir())
}

pub fn home_dir() -> PathBuf {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
}

#[cfg(test)]
mod tests {
    #[test]
    fn provider_roots_have_one_owner() {
        for (name, source) in [
            ("discover.rs", include_str!("discover.rs")),
            ("ingest.rs", include_str!("ingest.rs")),
            ("ingest/hydrate.rs", include_str!("ingest/hydrate.rs")),
        ] {
            let production = source.split("\n#[cfg(test)]").next().unwrap();
            for literal in [
                "join(\".claude",
                "join(\".codex",
                "join(\".grok",
                "join(\".local/share/muse",
            ] {
                assert!(
                    !production.contains(literal),
                    "{name} builds a provider root directly with {literal}"
                );
            }
        }
    }
}

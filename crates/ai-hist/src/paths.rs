//! Local provider locations shared by discovery, ingestion, and applications.
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

/// The Devin CLI data directory: `$XDG_DATA_HOME/devin/cli`, defaulting to
/// `~/.local/share/devin/cli`. It holds `sessions.db` plus the per-session
/// `transcripts/<id>.json` exports.
pub fn devin_cli_dir(home: &Path) -> PathBuf {
    env_dir("XDG_DATA_HOME")
        .map(|data| data.join("devin").join("cli"))
        .unwrap_or_else(|| home.join(".local/share/devin/cli"))
}

/// The home-relative default, ignoring `XDG_DATA_HOME` — for hosts that hand
/// every provider root in explicitly rather than through the process env.
pub(crate) fn devin_cli_dir_under(home: &Path) -> PathBuf {
    home.join(".local/share/devin/cli")
}

#[derive(Clone, Debug)]
pub(crate) struct ProviderRoots {
    pub home: PathBuf,
    pub claude: PathBuf,
    pub codex: PathBuf,
    pub grok: PathBuf,
    /// Devin CLI data directory (`sessions.db` plus `transcripts/`).
    pub devin: PathBuf,
    pub opencode_db: PathBuf,
    pub opencode_storage_dir: PathBuf,
    pub use_env_roots: bool,
}

impl ProviderRoots {
    pub(crate) fn from_env(home: PathBuf) -> Self {
        Self {
            claude: claude_config_dir(&home),
            codex: codex_home(&home),
            grok: grok_home(&home),
            devin: devin_cli_dir(&home),
            opencode_db: opencode_db_path(&home),
            opencode_storage_dir: opencode_storage_dir(&home),
            use_env_roots: true,
            home,
        }
    }

    pub(crate) fn from_home(home: PathBuf, opencode_db: PathBuf) -> Self {
        let opencode_storage_dir = opencode_db
            .parent()
            .map(|parent| parent.join("storage"))
            .unwrap_or_else(|| home.join(".local/share/opencode/storage"));
        Self {
            claude: home.join(".claude"),
            codex: home.join(".codex"),
            grok: home.join(".grok"),
            devin: devin_cli_dir_under(&home),
            home,
            opencode_db,
            opencode_storage_dir,
            use_env_roots: false,
        }
    }
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
            for literal in ["join(\".claude", "join(\".codex", "join(\".grok"] {
                assert!(
                    !production.contains(literal),
                    "{name} builds a provider root directly with {literal}"
                );
            }
        }
    }
}

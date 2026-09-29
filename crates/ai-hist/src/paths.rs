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

/// Whether `name` is an OpenCode SQLite store: `opencode.db`, or a
/// channel-suffixed `opencode-<channel>.db` such as `opencode-stable.db` or
/// `opencode-nightly.db`.
///
/// OpenCode writes one database per release channel (`getChannelPath` in its
/// `storage/db.ts`): `latest` and `beta`, and anyone with
/// `OPENCODE_DISABLE_CHANNEL_DB=1`, use `opencode.db`; every other channel gets
/// its own file beside it. `<channel>` is drawn from the `[a-zA-Z0-9._-]`
/// class OpenCode normalizes channel names to. The `-wal`, `-shm` and
/// `-journal` sidecars do not end in `.db`, so they are never mistaken for a
/// store.
pub(crate) fn is_opencode_db_filename(name: &str) -> bool {
    let Some(stem) = name.strip_suffix(".db") else {
        return false;
    };
    if stem == "opencode" {
        return true;
    }
    let Some(channel) = stem.strip_prefix("opencode-") else {
        return false;
    };
    !channel.is_empty()
        && channel
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

/// Every OpenCode SQLite store to read, given the configured one.
///
/// `pinned` means the store was named outright (`OPENCODE_DB`, or an explicit
/// path): exactly that file is read. Otherwise every channel database in its
/// directory is read too. The configured file comes first when it exists, then
/// the channel files in name order, so a session present in more than one
/// store resolves to the same file on every path that asks. Only existing
/// regular files are returned (a symlink counts when it resolves to one), and
/// the directory is listed at call time: a channel database OpenCode creates
/// while a watch loop runs is picked up by the next scan.
///
/// A directory that exists but cannot be listed yields only the configured
/// store; callers that must not mistake that for "there are no channel
/// databases" ask [`list_opencode_db_files`] instead.
pub(crate) fn opencode_db_files(configured: &Path, pinned: bool) -> Vec<PathBuf> {
    list_opencode_db_files(configured, pinned).stores
}

/// [`opencode_db_files`], plus whether the channel directory could be listed.
pub(crate) struct OpencodeDbListing {
    /// The stores found, in [`opencode_db_files`] order.
    pub stores: Vec<PathBuf>,
    /// The directory whose listing failed, and why. `None` when it was listed,
    /// when the store is pinned, or when the directory does not exist (then
    /// there is nothing beside the configured store to miss).
    pub unlisted: Option<(PathBuf, std::io::Error)>,
}

/// See [`opencode_db_files`]. An unlistable directory is reported rather than
/// read as empty, so a sweep that could see only the configured store does not
/// record the channel databases beside it as absent.
pub(crate) fn list_opencode_db_files(configured: &Path, pinned: bool) -> OpencodeDbListing {
    let mut stores = Vec::new();
    if configured.is_file() {
        stores.push(configured.to_path_buf());
    }
    let unlisted = |stores, dir: &Path, error| OpencodeDbListing {
        stores,
        unlisted: Some((dir.to_path_buf(), error)),
    };
    if pinned {
        return OpencodeDbListing {
            stores,
            unlisted: None,
        };
    }
    let dir = match configured.parent() {
        Some(dir) if dir.as_os_str().is_empty() => Path::new("."),
        Some(dir) => dir,
        None => {
            return OpencodeDbListing {
                stores,
                unlisted: None,
            }
        }
    };
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return OpencodeDbListing {
                stores,
                unlisted: None,
            }
        }
        Err(error) => return unlisted(stores, dir, error),
    };
    let mut channels = Vec::new();
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => return unlisted(stores, dir, error),
        };
        if !entry
            .file_name()
            .to_str()
            .is_some_and(is_opencode_db_filename)
        {
            continue;
        }
        // Same directory, so the same name is the configured store itself.
        if Some(entry.file_name().as_os_str()) == configured.file_name() {
            continue;
        }
        let path = entry.path();
        if path.is_file() {
            channels.push(path);
        }
    }
    channels.sort();
    stores.extend(channels);
    OpencodeDbListing {
        stores,
        unlisted: None,
    }
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
    /// The OpenCode SQLite store. Unless [`Self::opencode_db_pinned`], every
    /// channel database beside it (`opencode-stable.db`,
    /// `opencode-nightly.db`, ...) is read as well.
    pub opencode_db: PathBuf,
    /// Whether `opencode_db` was named outright, so it is the only OpenCode
    /// database read. [`ProviderRoots::from_env`] sets this when
    /// `OPENCODE_DB` is set; the default reads every channel database in the
    /// store's directory.
    #[serde(default)]
    pub opencode_db_pinned: bool,
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
            opencode_db_pinned: env_dir("OPENCODE_DB").is_some(),
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
            opencode_db_pinned: false,
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
    use super::*;

    #[test]
    fn opencode_db_filenames_match_the_channel_rule() {
        for name in [
            "opencode.db",
            "opencode-stable.db",
            "opencode-nightly.db",
            "opencode-v1.2_rc-3.db",
        ] {
            assert!(is_opencode_db_filename(name), "{name}");
        }
        for name in [
            "opencode.db-wal",
            "opencode.db-shm",
            "opencode-nightly.db-wal",
            "opencode-nightly.db-journal",
            "opencode-.db",
            "opencode-a b.db",
            "opencodex.db",
            "other.db",
            "opencode",
        ] {
            assert!(!is_opencode_db_filename(name), "{name}");
        }
    }

    #[test]
    fn channel_databases_are_found_beside_the_configured_store_unless_pinned() {
        let dir = tempfile::tempdir().unwrap();
        let default = dir.path().join("opencode.db");
        for name in [
            "opencode.db",
            "opencode-nightly.db",
            "opencode-nightly.db-wal",
            "opencode-stable.db",
            "notes.db",
        ] {
            std::fs::write(dir.path().join(name), b"").unwrap();
        }
        std::fs::create_dir(dir.path().join("opencode-dir.db")).unwrap();
        assert_eq!(
            opencode_db_files(&default, false),
            vec![
                default.clone(),
                dir.path().join("opencode-nightly.db"),
                dir.path().join("opencode-stable.db"),
            ]
        );
        assert_eq!(opencode_db_files(&default, true), vec![default.clone()]);
        let stable = dir.path().join("opencode-stable.db");
        assert_eq!(opencode_db_files(&stable, true), vec![stable.clone()]);

        // No default store at all: the channel files are still the stores.
        std::fs::remove_file(&default).unwrap();
        assert_eq!(
            opencode_db_files(&default, false),
            vec![
                dir.path().join("opencode-nightly.db"),
                dir.path().join("opencode-stable.db"),
            ]
        );
        assert!(opencode_db_files(&default, true).is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn an_unlistable_channel_directory_is_reported_not_read_as_empty() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let default = dir.path().join("opencode.db");
        std::fs::write(&default, b"").unwrap();
        std::fs::write(dir.path().join("opencode-nightly.db"), b"").unwrap();
        // Searchable but not listable: the configured store still opens.
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o300)).unwrap();
        let listable = std::fs::read_dir(dir.path()).is_ok();
        let listing = list_opencode_db_files(&default, false);
        let pinned = list_opencode_db_files(&default, true);
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        if listable {
            // Running as root: permissions do not bind, nothing to observe.
            return;
        }
        assert_eq!(listing.stores, vec![default.clone()]);
        assert_eq!(
            listing.unlisted.map(|(path, _)| path),
            Some(dir.path().to_path_buf())
        );
        assert!(pinned.unlisted.is_none(), "a pinned store lists nothing");
        // A directory that does not exist has nothing in it to miss.
        let missing = dir.path().join("absent/opencode.db");
        assert!(list_opencode_db_files(&missing, false).unlisted.is_none());
    }

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

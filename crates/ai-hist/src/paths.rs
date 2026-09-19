//! Local provider locations shared by discovery, ingestion, and applications.
use std::path::PathBuf;
pub fn default_opencode_db_path() -> PathBuf {
    std::env::var_os("OPENCODE_DB")
        .map(PathBuf::from)
        .unwrap_or_else(|| home_dir().join(".local/share/opencode/opencode.db"))
}

/// Where OpenCode's legacy JSON tree lives: `session/<scope>/<id>.json`,
/// `message/<sessionId>/*.json`, `part/<messageId>/*.json`.
///
/// Newer OpenCode releases write `opencode.db` instead and leave this tree
/// behind, so it is only read when there is no SQLite store.
pub fn default_opencode_storage_dir() -> PathBuf {
    std::env::var_os("OPENCODE_STORAGE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| home_dir().join(".local/share/opencode/storage"))
}

pub fn home_dir() -> PathBuf {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
}

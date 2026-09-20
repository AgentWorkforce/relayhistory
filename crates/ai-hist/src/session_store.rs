//! Embedder entry point. Cargo semver is the contract; there is no separate
//! Rust contract-version constant.
use crate::ingest::{sync_local_at, sync_local_at_with_home};
use crate::store::{
    default_db_path, open_db, open_db_readonly, session_user_turns_page, SessionEventCursor,
    SessionUserTurnPage,
};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::path::PathBuf;

/// Recoverable failure from [`SessionStore`].
#[derive(Debug)]
#[non_exhaustive]
pub struct Error {
    message: String,
}

impl Error {
    pub(crate) fn from_anyhow(error: anyhow::Error) -> Self {
        Self {
            message: format!("{error:#}"),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for Error {}

impl From<anyhow::Error> for Error {
    fn from(error: anyhow::Error) -> Self {
        Self::from_anyhow(error)
    }
}

/// Coding-agent source that produced a session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "lowercase")]
pub enum Source {
    Claude,
    Codex,
    Cursor,
    Grok,
    Relay,
    Trajectory,
    #[serde(rename = "opencode")]
    OpenCode,
}

impl Source {
    /// Canonical lowercase identifier stored in the ledger.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Claude => "claude",
            Self::Codex => "codex",
            Self::Cursor => "cursor",
            Self::Grok => "grok",
            Self::Relay => "relay",
            Self::Trajectory => "trajectory",
            Self::OpenCode => "opencode",
        }
    }
}

/// How to open a [`SessionStore`].
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct StoreOptions {
    pub db_path: Option<PathBuf>,
    pub home: Option<PathBuf>,
    pub read_only: bool,
}

/// How to run a local ingest.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct SyncOptions {}

/// A session the store can name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum SessionRef {
    Id { source: Source, session_id: String },
    Path { source: Source, path: PathBuf },
}

/// Result of [`SessionStore::sync`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct SyncReport {
    pub changed: Vec<SessionRef>,
}

/// The single public entry point for embedding `ai-hist` from Rust.
pub struct SessionStore {
    db_path: PathBuf,
    home: Option<PathBuf>,
    read_only: bool,
}

impl SessionStore {
    /// Open (and create, unless `read_only`) the local history database.
    pub fn open(opts: StoreOptions) -> Result<Self, Error> {
        let db_path = resolve_db_path(&opts);
        if opts.read_only {
            let _ = open_db_readonly(&db_path)?;
        } else {
            let _ = open_db(&db_path)?;
        }
        Ok(Self {
            db_path,
            home: opts.home,
            read_only: opts.read_only,
        })
    }

    /// Full local scan into this store.
    pub fn sync(&self, _opts: SyncOptions) -> Result<SyncReport, Error> {
        if self.read_only {
            return Err(Error {
                message: "SessionStore is read-only".to_string(),
            });
        }
        let _ran = match &self.home {
            Some(home) => sync_local_at_with_home(&self.db_path, home)?,
            None => sync_local_at(&self.db_path)?,
        };
        Ok(SyncReport {
            changed: Vec::new(),
        })
    }

    /// Read one bounded page of user turns and their ordered text/tool-result
    /// blocks without exposing a raw SQLite connection.
    pub fn session_user_turns_page(
        &self,
        source: Source,
        session_id: &str,
        limit: i64,
        after: Option<&SessionEventCursor>,
    ) -> Result<SessionUserTurnPage, Error> {
        let conn = open_db_readonly(&self.db_path)?;
        session_user_turns_page(&conn, source.as_str(), session_id, limit, after)
            .map_err(Error::from_anyhow)
    }
}

fn resolve_db_path(opts: &StoreOptions) -> PathBuf {
    if let Some(path) = &opts.db_path {
        return path.clone();
    }
    match &opts.home {
        None => default_db_path(),
        Some(home) => home.join(".local/share/ai-hist/ai-history.db"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_creates_the_database() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("ai-history.db");
        SessionStore::open(StoreOptions {
            db_path: Some(db.clone()),
            ..StoreOptions::default()
        })
        .unwrap();
        assert!(db.exists());
    }

    #[test]
    fn user_turns_are_readable_through_the_public_facade() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("ai-history.db");
        let store = SessionStore::open(StoreOptions {
            db_path: Some(db.clone()),
            ..StoreOptions::default()
        })
        .unwrap();
        let conn = open_db(&db).unwrap();
        conn.execute(
            "INSERT INTO session_events \
             (source, session_id, message_id, ts_ms, role, kind, text, event_uid) \
             VALUES ('claude', 's1', 'm1', 10, 'user', 'text', 'hello', 'e1')",
            [],
        )
        .unwrap();

        let page = store
            .session_user_turns_page(Source::Claude, "s1", 10, None)
            .unwrap();
        assert_eq!(page.user_turns.len(), 1);
        assert_eq!(page.user_turns[0].blocks[0].byte_len, 5);
        assert!(page.next_cursor.is_none());
    }
}

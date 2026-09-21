//! Embedder entry point. Cargo semver is the contract; there is no separate
//! Rust contract-version constant.
use crate::ingest::{sync_local_at, sync_local_at_with_home};
use crate::session_usage::{
    session_requests_page, session_usage_summary, SessionRequestCursor, SessionRequestPage,
    SessionUsageSummary,
};
use crate::store::{
    default_db_path, open_db, open_db_readonly, schema_is_event_read_current,
    schema_is_evidence_read_current, schema_is_usage_read_current, session_markers_page,
    session_user_turns_page, SessionEventCursor, SessionEvidenceCursor, SessionMarkerPage,
    SessionUserTurnPage,
};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::path::{Path, PathBuf};

/// Recoverable failure from [`SessionStore`].
#[derive(Debug)]
#[non_exhaustive]
pub struct Error {
    message: String,
    stale_schema: bool,
}

impl Error {
    pub(crate) fn from_anyhow(error: anyhow::Error) -> Self {
        Self {
            message: format!("{error:#}"),
            stale_schema: false,
        }
    }

    /// A read-only store refused a database written before the schema this
    /// version reads. The remedy is named in the message: open it writable
    /// once, which migrates it.
    fn stale_schema(db_path: &Path, what: &str) -> Self {
        Self {
            message: format!(
                "{} predates the {what} schema this version reads; \
                 open it writable once (or run a sync) to migrate it",
                db_path.display()
            ),
            stale_schema: true,
        }
    }

    /// Whether this failure is a read-only store refusing a database it
    /// would have to migrate first.
    ///
    /// Exposed as a fact rather than left to the message text, because a
    /// caller that is allowed to write has a remedy for exactly this failure
    /// and for no other: reopening writable migrates a stale database, but
    /// it also takes the writer lock and runs initialization, which is the
    /// wrong answer to a query that failed for any other reason.
    pub fn is_stale_schema(&self) -> bool {
        self.stale_schema
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
    ///
    /// A writable open migrates the database to the shape this version reads.
    /// A read-only one cannot, so it checks instead and fails here, naming the
    /// remedy: the alternative is an open that succeeds and a first read that
    /// dies on `no such column` somewhere inside a query, which tells the
    /// caller nothing about what to do. The napi layer answers the same
    /// mismatch by reopening writable and migrating; a read-only embedder has
    /// asked not to be written for, so it is told rather than upgraded behind
    /// its back.
    pub fn open(opts: StoreOptions) -> Result<Self, Error> {
        let db_path = resolve_db_path(&opts);
        if opts.read_only {
            let conn = open_db_readonly(&db_path)?;
            if !schema_is_event_read_current(&conn)? {
                return Err(Error::stale_schema(&db_path, "session-event"));
            }
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
                stale_schema: false,
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

    /// Read one bounded page of a session's markers, oldest first.
    ///
    /// Markers are the records the normalized event model cannot carry --
    /// compaction and summary boundaries, provider system rows, non-text
    /// content blocks, agent lifecycle events. An embedder that syncs them
    /// needs a supported way to read them back; without one this table is
    /// write-only for everyone outside this workspace, and the only reachable
    /// alternative is a hand-written query against a schema that is explicitly
    /// not a contract.
    ///
    /// Unlike the user-turn page, the schema check is made here rather than at
    /// `open`: the marker page index arrived after `SessionStore` shipped, so
    /// gating `open` on it would turn "cannot read markers" into "cannot open
    /// this database at all" for a caller that never asks for one. A read-only
    /// store over a database written before this schema is told what to do
    /// instead of being served an unindexed scan -- or `no such table`.
    pub fn session_markers_page(
        &self,
        source: Source,
        session_id: &str,
        limit: i64,
        after: Option<&SessionEvidenceCursor>,
    ) -> Result<SessionMarkerPage, Error> {
        let conn = open_db_readonly(&self.db_path)?;
        if !schema_is_evidence_read_current(&conn).map_err(Error::from_anyhow)? {
            return Err(Error::stale_schema(&self.db_path, "session-marker"));
        }
        session_markers_page(&conn, source.as_str(), session_id, limit, after)
            .map_err(Error::from_anyhow)
    }

    /// Read one bounded page of a session's model requests, oldest first.
    ///
    /// The grouping is what makes this worth a facade method rather than a
    /// query an embedder writes itself: one API call is several stored rows
    /// for every provider here, by a different rule for each, and counting
    /// rows reports a session as costing several times what it did.
    pub fn session_requests_page(
        &self,
        source: Source,
        session_id: &str,
        limit: i64,
        after: Option<&SessionRequestCursor>,
    ) -> Result<SessionRequestPage, Error> {
        let conn = open_db_readonly(&self.db_path)?;
        if !schema_is_usage_read_current(&conn).map_err(Error::from_anyhow)? {
            return Err(Error::stale_schema(&self.db_path, "session-usage"));
        }
        session_requests_page(&conn, source.as_str(), session_id, limit, after)
            .map_err(Error::from_anyhow)
    }

    /// The whole-session usage rollup, or `None` when no request was recorded.
    ///
    /// `None` means the session has no requests at all. A session whose usage
    /// could not be established answers `Some` with `usage: None` and the
    /// diagnostics saying why — the two are different answers and the facade
    /// keeps them apart.
    pub fn session_usage(
        &self,
        source: Source,
        session_id: &str,
    ) -> Result<Option<SessionUsageSummary>, Error> {
        let conn = open_db_readonly(&self.db_path)?;
        if !schema_is_usage_read_current(&conn).map_err(Error::from_anyhow)? {
            return Err(Error::stale_schema(&self.db_path, "session-usage"));
        }
        session_usage_summary(&conn, source.as_str(), session_id).map_err(Error::from_anyhow)
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

    /// Usage is reachable without a raw connection, through the same store a
    /// caller already has. Exporting only the connection-taking functions left
    /// an embedder with no supported way to ask what a session cost.
    #[test]
    fn usage_is_readable_through_the_public_facade() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("ai-history.db");
        let store = SessionStore::open(StoreOptions {
            db_path: Some(db.clone()),
            ..StoreOptions::default()
        })
        .unwrap();

        // A session that was never recorded is `None`, not an empty rollup.
        assert!(store
            .session_usage(Source::Codex, "missing")
            .unwrap()
            .is_none());
        let page = store
            .session_requests_page(Source::Codex, "missing", 10, None)
            .unwrap();
        assert!(page.requests.is_empty());
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

    #[test]
    fn a_read_only_open_rejects_a_database_it_cannot_migrate() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("ai-history.db");
        // A database written before the tool-result fidelity columns existed.
        // A read-only handle never runs `init_db`, so without the check at
        // `open` the store would hand back a handle whose first user-turn read
        // dies inside a SELECT on `no such column: event_source`.
        {
            let conn = rusqlite::Connection::open(&db).unwrap();
            conn.execute_batch(
                "CREATE TABLE session_events (
                     id INTEGER PRIMARY KEY AUTOINCREMENT,
                     source TEXT NOT NULL,
                     session_id TEXT NOT NULL,
                     message_id TEXT,
                     ts_ms INTEGER NOT NULL,
                     role TEXT NOT NULL,
                     kind TEXT NOT NULL,
                     text TEXT,
                     event_uid TEXT NOT NULL,
                     UNIQUE(source, session_id, event_uid)
                 );
                 INSERT INTO session_events \
                 (source, session_id, message_id, ts_ms, role, kind, text, event_uid) \
                 VALUES ('claude', 's1', 'm1', 10, 'user', 'text', 'hello', 'e1');",
            )
            .unwrap();
        }

        let refused = SessionStore::open(StoreOptions {
            db_path: Some(db.clone()),
            read_only: true,
            ..StoreOptions::default()
        });
        let error = match refused {
            Ok(_) => panic!("a read-only open cannot migrate, so it must refuse"),
            Err(error) => error,
        };
        assert!(
            error.is_stale_schema(),
            "the refusal is typed, so a caller with a remedy can apply it"
        );
        assert!(
            !Error::from(anyhow::anyhow!("no such column: x")).is_stale_schema(),
            "an ordinary query failure is not a stale schema"
        );
        let message = error.to_string();
        assert!(
            message.contains("predates the session-event schema"),
            "the error names the mismatch: {message}",
        );
        assert!(
            message.contains("writable"),
            "the error names the remedy: {message}",
        );

        // The same database opened writable migrates and reads, so the refusal
        // is about what a read-only handle can do, not about the database.
        let store = SessionStore::open(StoreOptions {
            db_path: Some(db.clone()),
            ..StoreOptions::default()
        })
        .unwrap();
        let page = store
            .session_user_turns_page(Source::Claude, "s1", 10, None)
            .unwrap();
        assert_eq!(page.user_turns.len(), 1);

        // And a migrated database opens read-only, so the check does not
        // simply refuse every read-only caller.
        SessionStore::open(StoreOptions {
            db_path: Some(db),
            read_only: true,
            ..StoreOptions::default()
        })
        .unwrap();
    }
}

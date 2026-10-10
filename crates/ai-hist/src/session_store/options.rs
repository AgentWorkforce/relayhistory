//! The by-value option builders the [`SessionStore`] operations take, and
//! the stop and progress controls they carry.

use super::{Error, ProviderRoots, Source};
#[cfg(doc)]
use super::{SessionStore, SourceCapabilities};
use crate::ingest::{with_capture_observer, with_capture_token, CaptureProgress};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// How to open a [`SessionStore`].
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct StoreOptions {
    /// The database. Defaults to `$AI_HIST_DB`, then the XDG data path, or
    /// `<home>/.local/share/ai-hist/ai-history.db` when `home` is set.
    pub db_path: Option<PathBuf>,
    /// Provider home to scan instead of the process `HOME`. Ignored when
    /// `roots` is set. Provider roots derived from it still honour
    /// `CLAUDE_CONFIG_DIR`, `CODEX_HOME`, `GROK_HOME` and `OPENCODE_DB` when
    /// they are set, exactly as the CLI does.
    pub home: Option<PathBuf>,
    /// Exactly where each provider keeps its sessions, resolved by the
    /// caller. `None` derives them from `home` (or the process `HOME`) with
    /// the environment overrides applied, as [`ProviderRoots::from_env`]
    /// does. One resolution drives `sync`, `hydrate`, `watch` and
    /// [`SourceCapabilities::watch_roots`] alike.
    pub roots: Option<ProviderRoots>,
    /// Never write. `discover`, `sync`, `hydrate`, `watch`, `forget_evidence`
    /// and `compact` return [`Error::UnsupportedOperation`]; a database older than the shape this
    /// version reads is refused at `open` rather than failing inside a query.
    pub read_only: bool,
}

impl StoreOptions {
    /// Open exactly this database; see [`StoreOptions::db_path`].
    #[must_use]
    pub fn db_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.db_path = Some(path.into());
        self
    }

    /// Scan this provider home; see [`StoreOptions::home`].
    #[must_use]
    pub fn home(mut self, home: impl Into<PathBuf>) -> Self {
        self.home = Some(home.into());
        self
    }

    /// Read providers from exactly these roots; see [`StoreOptions::roots`].
    #[must_use]
    pub fn roots(mut self, roots: ProviderRoots) -> Self {
        self.roots = Some(roots);
        self
    }

    /// Never write; see [`StoreOptions::read_only`].
    #[must_use]
    pub fn read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
        self
    }
}

/// Cooperative stop for [`SessionStore::discover`], [`SessionStore::sync`]
/// and [`SessionStore::hydrate`]. Clones share one flag, so a token handed to
/// a call on one thread is stopped from another.
///
/// A stopped call returns [`Error::Cancelled`] at the next provider, file or
/// record boundary. Committed chunks stay; the unfinished transaction rolls
/// back and the next call resumes from its checkpoint.
#[derive(Debug, Clone, Default)]
pub struct StopToken(Arc<AtomicBool>);

impl StopToken {
    pub fn new() -> Self {
        Self::default()
    }

    /// Stop every call holding this token or a clone of it. Idempotent.
    pub fn stop(&self) {
        self.0.store(true, Ordering::SeqCst);
    }

    pub fn is_stopped(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}

/// Receives content-free [`CaptureProgress`] — a source name and file
/// counts, never paths or session contents — while a sweep reads files.
/// Called on the thread running the sweep.
#[derive(Clone)]
pub struct ProgressObserver(Arc<dyn Fn(CaptureProgress) + Send + Sync>);

impl ProgressObserver {
    pub fn new(observer: impl Fn(CaptureProgress) + Send + Sync + 'static) -> Self {
        Self(Arc::new(observer))
    }
}

impl fmt::Debug for ProgressObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProgressObserver")
    }
}

/// Run `work` with the caller's stop and progress installed for this thread.
pub(crate) fn controlled<T>(
    stop: Option<&StopToken>,
    progress: Option<&ProgressObserver>,
    work: impl FnOnce() -> Result<T, Error>,
) -> Result<T, Error> {
    let stop = stop.cloned();
    let stoppable = move || match stop {
        Some(token) => with_capture_token(token, || Ok(work())),
        None => Ok(work()),
    };
    let outcome = match progress.cloned() {
        Some(observer) => with_capture_observer(move |value| (observer.0)(value), stoppable),
        None => stoppable(),
    };
    outcome.map_err(Error::sync)?
}

/// How to run a shallow catalog sweep.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct DiscoveryOptions {
    /// Restrict to these sources. `None` means every local source;
    /// `Some(vec![])` admits none and reads nothing.
    pub sources: Option<Vec<Source>>,
    /// Cap on rows read, newest first across providers. `None` (the default)
    /// reads the whole catalog; a caller repeating a capped sweep only ever
    /// sees the same newest sessions.
    pub limit: Option<usize>,
    /// Stops the sweep at the next provider or file boundary. See
    /// [`StopToken`].
    #[serde(skip)]
    pub stop: Option<StopToken>,
}

impl DiscoveryOptions {
    /// Read only these sources; see [`DiscoveryOptions::sources`].
    #[must_use]
    pub fn sources(mut self, sources: impl IntoIterator<Item = Source>) -> Self {
        self.sources = Some(sources.into_iter().collect());
        self
    }

    /// Read at most this many rows; see [`DiscoveryOptions::limit`].
    #[must_use]
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Stop when this token is stopped; see [`DiscoveryOptions::stop`].
    #[must_use]
    pub fn stop(mut self, stop: StopToken) -> Self {
        self.stop = Some(stop);
        self
    }
}

/// How to run a local sweep.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SyncOptions {
    /// Bypass the stat-only source fingerprint and walk every provider even
    /// when nothing appears to have moved. The watch loop sets it for
    /// filesystem-event ticks; a caller repairing a store it does not trust
    /// sets it too.
    pub force: bool,
    /// How long to wait for another process's `SyncRunLock` before returning
    /// [`Error::SyncLocked`]. `0` (the default) tries once. The lock is
    /// re-tried every 100 ms while the budget lasts; a budget above seven
    /// days is treated as seven days.
    pub lock_timeout_ms: u64,
    /// Stops the sweep at the next provider, file or record boundary, and
    /// ends a wait for the lock. See [`StopToken`].
    #[serde(skip)]
    pub stop: Option<StopToken>,
    /// Receives [`CaptureProgress`] as the sweep reads each provider's files.
    /// OpenCode counts its SQLite database as one file, or one session file
    /// per session in the legacy JSON tree. Unchanged files count as processed.
    #[serde(skip)]
    pub progress: Option<ProgressObserver>,
    /// Sweep only these sources. `None` (the default) is the full sweep of
    /// every local source; `Some(vec![])` reads nothing and reports
    /// `swept: false`.
    ///
    /// A scoped sweep always reads its sources, whatever `force` says: the
    /// stat-only fingerprint the fast path compares is a statement about
    /// every source, so a scoped sweep neither consults nor stores it, and
    /// the next unforced full sweep still sees whatever moved in the sources
    /// it left out. [`SessionStore::watch`] scopes each filesystem-event
    /// tick this way to the sources whose roots fired. Absent from stored
    /// JSON written before the field existed, which means a full sweep.
    #[serde(default)]
    pub sources: Option<Vec<Source>>,
}

impl SyncOptions {
    /// Sweep only these sources; see [`SyncOptions::sources`].
    #[must_use]
    pub fn sources(mut self, sources: impl IntoIterator<Item = Source>) -> Self {
        self.sources = Some(sources.into_iter().collect());
        self
    }

    /// Walk every provider; see [`SyncOptions::force`].
    #[must_use]
    pub fn force(mut self, force: bool) -> Self {
        self.force = force;
        self
    }

    /// Wait this long for the lock; see [`SyncOptions::lock_timeout_ms`].
    #[must_use]
    pub fn lock_timeout_ms(mut self, lock_timeout_ms: u64) -> Self {
        self.lock_timeout_ms = lock_timeout_ms;
        self
    }

    /// Stop when this token is stopped; see [`SyncOptions::stop`].
    #[must_use]
    pub fn stop(mut self, stop: StopToken) -> Self {
        self.stop = Some(stop);
        self
    }

    /// Report file progress here; see [`SyncOptions::progress`].
    #[must_use]
    pub fn progress(mut self, progress: ProgressObserver) -> Self {
        self.progress = Some(progress);
        self
    }
}

/// How to hydrate one session.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct HydrateOptions {
    /// Also hydrate the session's bounded related transcripts — Claude
    /// subagent sidecars beside it, Codex child rollouts. Defaults to `true`.
    pub include_related: bool,
    /// Stops the hydration at the next file or record boundary. See
    /// [`StopToken`].
    #[serde(skip)]
    pub stop: Option<StopToken>,
}

impl Default for HydrateOptions {
    fn default() -> Self {
        Self {
            include_related: true,
            stop: None,
        }
    }
}

impl HydrateOptions {
    /// Also hydrate related transcripts; see
    /// [`HydrateOptions::include_related`].
    #[must_use]
    pub fn include_related(mut self, include_related: bool) -> Self {
        self.include_related = include_related;
        self
    }

    /// Stop when this token is stopped; see [`HydrateOptions::stop`].
    #[must_use]
    pub fn stop(mut self, stop: StopToken) -> Self {
        self.stop = Some(stop);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sync_options_json_without_sources_decodes_as_a_full_sweep() {
        let options: SyncOptions =
            serde_json::from_str(r#"{"force":true,"lock_timeout_ms":250}"#).unwrap();
        assert!(options.force);
        assert_eq!(options.lock_timeout_ms, 250);
        assert!(options.sources.is_none());
    }
}

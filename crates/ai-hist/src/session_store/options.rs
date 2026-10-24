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
    /// Exact watcher paths or a bounded reconciliation slice. `None` means
    /// unrestricted; `Some(vec![])` reads no providers. Provider-owned path
    /// rules remain authoritative. With no `sources` hint, the SDK selects
    /// only providers whose watch roots own these paths.
    #[serde(default)]
    pub paths: Option<Vec<PathBuf>>,
    /// Cap on rows read, newest first across providers. `None` (the default)
    /// reads the whole catalog; a caller repeating a capped sweep only ever
    /// sees the same newest sessions.
    pub limit: Option<usize>,
    /// Stops the sweep at the next provider or file boundary. See
    /// [`StopToken`].
    #[serde(skip)]
    pub stop: Option<StopToken>,
    /// Keep a working directory whose first component under the user home is
    /// `Desktop`, `Documents`, or `Downloads` as that raw path. Discovery
    /// does not canonicalize it, probe `.git`, or read Git config. A
    /// provider-recorded `repo_url` is still preferred and does not read the
    /// directory. On macOS those three names match case-insensitively;
    /// elsewhere the match is case-sensitive. `~/Desktop-backup` and
    /// `~/Projects` are not protected. A `..` component is not normalized
    /// away, so a path that still textually sits under one of those folders
    /// is protected. Default off: sync, hydrate, and any caller that leaves
    /// this unset resolve every directory from the filesystem as before.
    /// Absent from stored JSON written before the field existed.
    #[serde(default)]
    pub skip_protected_folder_identity: bool,
}

impl DiscoveryOptions {
    pub(crate) fn reads_nothing(&self) -> bool {
        self.sources.as_ref().is_some_and(Vec::is_empty)
            || self.paths.as_ref().is_some_and(Vec::is_empty)
    }

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

    /// Skip filesystem project identity under Desktop, Documents, and
    /// Downloads; see [`DiscoveryOptions::skip_protected_folder_identity`].
    #[must_use]
    pub fn skip_protected_folder_identity(mut self, skip: bool) -> Self {
        self.skip_protected_folder_identity = skip;
        self
    }

    pub(crate) fn discover_options(&self) -> crate::discover::DiscoverOptions {
        crate::discover::DiscoverOptions {
            scope: super::SessionScope::Local,
            sources: self
                .sources
                .iter()
                .flatten()
                .map(|source| source.as_str().to_string())
                .collect(),
            limit: self.limit,
            skip_protected_folder_identity: self.skip_protected_folder_identity,
        }
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
    /// Maximum age, in days, of a Codex thread whose rollout is gone before
    /// its SQLite projection is skipped. `None` uses
    /// `AI_HIST_CODEX_SQLITE_MAX_AGE_DAYS`, then 30 days. `Some(0)` disables
    /// the age cutoff. Each sweep still reads at most 50 projected threads.
    #[serde(default)]
    pub codex_sqlite_max_age_days: Option<u64>,
    /// Exact filesystem paths reported by an embedder's watcher. `None` means
    /// no path restriction; `Some(vec![])` reads no providers. An explicit
    /// `sources` hint bypasses inference; otherwise the SDK derives owning
    /// providers from their watch roots.
    /// Partial passes never publish a full-source fingerprint.
    #[serde(default)]
    pub paths: Option<Vec<PathBuf>>,
}

impl SyncOptions {
    /// Sweep only these sources; see [`SyncOptions::sources`].
    #[must_use]
    pub fn sources(mut self, sources: impl IntoIterator<Item = Source>) -> Self {
        self.sources = Some(sources.into_iter().collect());
        self
    }

    /// Restrict this sweep to changed paths; see [`SyncOptions::paths`].
    #[must_use]
    pub fn paths(mut self, paths: impl IntoIterator<Item = PathBuf>) -> Self {
        self.paths = Some(paths.into_iter().collect());
        self
    }

    /// Walk every provider; see [`SyncOptions::force`].
    #[must_use]
    pub fn force(mut self, force: bool) -> Self {
        self.force = force;
        self
    }

    /// Set the Codex SQLite-only thread cutoff; see
    /// [`SyncOptions::codex_sqlite_max_age_days`].
    #[must_use]
    pub fn codex_sqlite_max_age_days(mut self, days: u64) -> Self {
        self.codex_sqlite_max_age_days = Some(days);
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

/// What one tick of [`super::SessionStore::watch`] runs.
///
/// [`WatchTick::Sync`] is the default, including options serialized before
/// the field existed. [`WatchTick::Discover`] reads the shallow catalog and
/// does not parse transcripts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum WatchTick {
    /// The locked capture [`super::SessionStore::sync`] runs.
    #[default]
    Sync,
    /// A shallow [`super::SessionStore::discover`] of the sources the tick
    /// names. No evidence is written and no transcript is parsed.
    Discover,
}

pub(super) fn resolve_db_path(opts: &StoreOptions) -> PathBuf {
    if let Some(path) = &opts.db_path {
        return path.clone();
    }
    match &opts.home {
        None => crate::store::default_db_path(),
        Some(home) => home.join(".local/share/ai-hist/ai-history.db"),
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

    #[test]
    fn discovery_options_without_the_protected_folder_field_stay_off() {
        let options: DiscoveryOptions = serde_json::from_str(r#"{"limit":3}"#).unwrap();
        assert!(!options.skip_protected_folder_identity);
        assert_eq!(options.limit, Some(3));
        let enabled = DiscoveryOptions::default().skip_protected_folder_identity(true);
        assert!(enabled.skip_protected_folder_identity);
    }
}

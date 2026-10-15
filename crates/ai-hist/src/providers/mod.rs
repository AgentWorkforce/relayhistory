//! Internal admission seam for provider-owned acquisition. No runtime loading.
#![allow(dead_code)] // Optional adapter hooks are exercised by the test provider until pilots land.
use crate::discover::WatchRoot;
use crate::hydrate::HydrateSessionOptions;
use crate::{MessageIdOrigin, NormalizedUsage, ProviderRoots, UsageAccounting, UsageError};
use anyhow::Result;
use rusqlite::Connection;
use serde_json::{Map, Value};
use std::path::{Path, PathBuf};

include!("generated_modules.rs");
mod acquisition;
pub(crate) mod file_history;
mod sweep;
pub(crate) use acquisition::*;
pub(crate) use sweep::sweep_generation;

/// Provider code owns capability declarations beside the implementation.
#[derive(Clone, Copy)]
pub(crate) struct ProviderMetadata {
    pub markers: bool,
    pub message_ids: MessageIdOrigin,
    pub usage: Option<UsageRegistration>,
}
#[derive(Clone, Copy)]
pub(crate) struct UsageRegistration {
    pub accounting: UsageAccounting,
    pub normalize: fn(&Value) -> std::result::Result<Option<NormalizedUsage>, UsageError>,
    pub splits_requests: bool,
}

/// Additional intake hooks for new providers. Existing parsers retain their
/// existing paths; adding a new source never grows those legacy switches.
#[derive(Clone, Copy)]
pub(crate) struct ProviderAdapter {
    /// Must change when normalization changes on unchanged provider bytes.
    pub generation: &'static str,
    /// Captures immutable payload and its stamp together, after root validation.
    pub acquire:
        fn(&HydrateSessionOptions, &Path, &AcquisitionContext<'_>) -> Result<AcquiredSession>,
    /// Resolved exactly once when ProviderRoots is constructed.
    pub resolve_roots: fn(&Path, bool) -> ResolvedProviderRoots,
    /// Full or source-scoped sync. Writes only through the existing store APIs.
    pub sweep: fn(SweepContext<'_>) -> Result<SweepResult>,
    /// Watch roots using the same root resolver as discovery/acquisition.
    pub watch_roots: fn(&ProviderRoots) -> Vec<WatchRoot>,
}

/// Paths and identity provenance frozen together by one resolver invocation.
pub(crate) struct ResolvedProviderRoots {
    pub paths: Vec<PathBuf>,
    pub default_root: Option<PathBuf>,
}
impl From<Vec<PathBuf>> for ResolvedProviderRoots {
    fn from(paths: Vec<PathBuf>) -> Self {
        Self {
            paths,
            default_root: None,
        }
    }
}
pub(crate) type ProviderRootMaps = (
    std::collections::BTreeMap<String, Vec<PathBuf>>,
    std::collections::BTreeMap<String, PathBuf>,
);

pub(crate) struct SweepContext<'a> {
    pub conn: &'a Connection,
    pub state: &'a mut Map<String, Value>,
    pub roots: &'a ProviderRoots,
    /// Optional changed paths; None requests the complete source.
    pub paths: Option<&'a crate::ingest::path_scope::PathScope>,
    pub repairs: &'a crate::ingest::SweepRepairs,
}
#[derive(Default)]
pub(crate) struct SweepResult {
    pub inserted: usize,
    /// Partial per-file failure forbids caching a complete-source fingerprint.
    pub unread: u64,
}
struct Registration {
    id: &'static str,
    metadata: ProviderMetadata,
    adapter: Option<ProviderAdapter>,
}
include!("generated_registry.rs");

pub(crate) fn metadata(id: &str) -> Option<&'static ProviderMetadata> {
    REGISTRATIONS
        .iter()
        .find(|r| r.id == id)
        .map(|r| &r.metadata)
}
pub(crate) fn adapter(id: &str) -> Option<&'static ProviderAdapter> {
    REGISTRATIONS.iter().find(|r| r.id == id)?.adapter.as_ref()
}
pub(crate) fn adapters() -> impl Iterator<Item = (&'static str, &'static ProviderAdapter)> {
    REGISTRATIONS
        .iter()
        .filter_map(|r| r.adapter.as_ref().map(|adapter| (r.id, adapter)))
}

/// The sources whose events, tool calls, file edits and catalog row a *sweep*
/// can put back.
///
/// The marker is a repair guard, so it may only promise what the sweep can
/// honour. Codex rollouts and Claude transcripts are re-read and re-ingested
/// by this sweep, so a loss in one is repairable and belongs in the marker.
/// Adding a source here means adding its repair path in the same change.
///
/// Muse Code joined with its parser: every session is a whole-transcript
/// read, and a sweep re-reads a session this marker names short, or the
/// parent whose tree holds a short subagent.
pub(crate) const REPAIRABLE_EVENT_SOURCES: &[&str] =
    &["claude", "codex", "opencode", "muse", "devin"];

/// The sources whose `history` and `session_markers` rows a sweep can replay.
///
/// Cursor-backed flat logs (`~/.claude/history.jsonl`, `~/.codex/history.jsonl`)
/// sit at EOF and cannot be safely rewound, so counting them would disarm the
/// fast path forever over a loss it could not undo. Store-backed providers
/// re-read their own database on each sync, so their history and markers are
/// as repairable as their events.
pub(crate) const REPLAYABLE_HISTORY_SOURCES: &[&str] = &["devin", "opencode"];

/// `REPAIRABLE_EVENT_SOURCES` as a bound parameter, so the grouped reads and
/// the shortfall scan cannot drift apart.
pub(crate) fn repairable_event_sources() -> rusqlite::types::Value {
    rusqlite::types::Value::Text(
        serde_json::to_string(&crate::providers::repairable_sources(
            REPAIRABLE_EVENT_SOURCES,
            false,
        ))
        .unwrap_or_else(|_| "[]".to_string()),
    )
}

pub(crate) fn replayable_history_sources() -> rusqlite::types::Value {
    rusqlite::types::Value::Text(
        serde_json::to_string(&crate::providers::repairable_sources(
            REPLAYABLE_HISTORY_SOURCES,
            true,
        ))
        .unwrap_or_else(|_| "[]".to_string()),
    )
}

pub(crate) fn can_repair_source(source: &str, history: bool) -> bool {
    repairable_sources(
        if history {
            REPLAYABLE_HISTORY_SOURCES
        } else {
            REPAIRABLE_EVENT_SOURCES
        },
        history,
    )
    .contains(&source)
}
pub(crate) fn repairable_sources(legacy: &[&'static str], history: bool) -> Vec<&'static str> {
    let mut sources = legacy.to_vec();
    sources.extend(crate::providers::adapters().filter_map(|(id, _)| {
        let kinds = crate::declared_evidence_kinds(id);
        let capable = if history {
            kinds.contains(&crate::EvidenceKind::History)
                || kinds.contains(&crate::EvidenceKind::SessionMarker)
        } else {
            // Every adapter can restore its catalog, even one without events.
            true
        };
        capable.then_some(id)
    }));
    sources
}

const USAGE_COUNT: usize = {
    let mut count = 0;
    let mut i = 0;
    while i < REGISTRATIONS.len() {
        if REGISTRATIONS[i].metadata.usage.is_some() {
            count += 1;
        }
        i += 1;
    }
    count
};
const USAGE_IDS: [&str; USAGE_COUNT] = {
    let mut ids = [""; USAGE_COUNT];
    let mut i = 0;
    let mut next = 0;
    while i < REGISTRATIONS.len() {
        if REGISTRATIONS[i].metadata.usage.is_some() {
            ids[next] = REGISTRATIONS[i].id;
            next += 1;
        }
        i += 1;
    }
    ids
};
pub(crate) const NORMALIZABLE_SOURCES: &[&str] = &USAGE_IDS;

/// Resolve an explicit override only while constructing ProviderRoots.
pub(crate) fn resolve_root(home: &Path, use_env: bool, env: &str, relative: &str) -> PathBuf {
    resolve_root_value(
        home,
        use_env.then(|| std::env::var_os(env)).flatten(),
        relative,
    )
}
fn resolve_root_value(home: &Path, value: Option<std::ffi::OsString>, relative: &str) -> PathBuf {
    value
        .filter(|v| !v.to_string_lossy().trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| home.join(relative))
}
pub(crate) fn resolve_roots(home: &Path, use_env: bool) -> ProviderRootMaps {
    let mut paths = std::collections::BTreeMap::new();
    let mut defaults = std::collections::BTreeMap::new();
    for (id, adapter) in adapters() {
        let resolved = (adapter.resolve_roots)(home, use_env);
        if let Some(default) = resolved.default_root {
            defaults.insert(id.to_owned(), default);
        }
        paths.insert(id.to_owned(), resolved.paths);
    }
    (paths, defaults)
}

pub(crate) fn legacy_watch_roots(source: &str, roots: &ProviderRoots) -> Vec<WatchRoot> {
    let providers = crate::sources::catalog::shallow_providers();
    let Some(provider) = providers
        .iter()
        .find(|provider| provider.source() == source)
    else {
        return Vec::new();
    };
    crate::discover::watch_roots(
        std::slice::from_ref(provider),
        &crate::discover::ProviderRoots {
            home: &roots.home,
            claude: &roots.claude,
            codex: &roots.codex,
            grok: &roots.grok,
            muse: &roots.muse,
            devin: &roots.devin,
            opencode_db: &roots.opencode_db,
            opencode_db_pinned: roots.opencode_db_pinned,
        },
    )
}

/// A provider-only normalization upgrade resets the hydration byte cursor as
/// well as invalidating its content stamp, without a global parser bump.
pub(crate) fn generation_changed(source: &str, previous: Option<&str>) -> bool {
    adapter(source).is_some_and(|a| {
        previous.is_some_and(|stamp| !stamp.starts_with(&format!("{}:", a.generation)))
    })
}

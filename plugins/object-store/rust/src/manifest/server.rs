//! Server helpers: the server-owned section and the pointer-row fields.

use std::time::Duration;

use super::types::{EffectiveState, Lifecycle, Manifest, Normalized, Server};
use super::update::now_rfc3339;

/// Lifecycle thresholds (spec defaults: idle after 24 h, abandoned after
/// 14 days without a raw change).
#[derive(Debug, Clone)]
pub struct LifecyclePolicy {
    pub idle_after: Duration,
    pub abandoned_after: Duration,
}

impl Default for LifecyclePolicy {
    fn default() -> Self {
        Self {
            idle_after: Duration::from_secs(24 * 60 * 60),
            abandoned_after: Duration::from_secs(14 * 24 * 60 * 60),
        }
    }
}

/// The lifecycle the server should record now, computed from the
/// client-owned `raw.updated_at`. `archived` is set by retention and is kept.
pub fn lifecycle_for(
    m: &Manifest,
    now: chrono::DateTime<chrono::Utc>,
    policy: &LifecyclePolicy,
) -> Lifecycle {
    if m.lifecycle() == Lifecycle::Archived {
        return Lifecycle::Archived;
    }
    let last = chrono::DateTime::parse_from_rfc3339(&m.raw.updated_at)
        .map(|t| t.with_timezone(&chrono::Utc))
        .unwrap_or(now);
    let quiet = (now - last).to_std().unwrap_or_default();
    if quiet >= policy.abandoned_after {
        Lifecycle::Abandoned
    } else if quiet >= policy.idle_after {
        Lifecycle::Idle
    } else {
        Lifecycle::Active
    }
}

/// A copy of the current server section (or a fresh one), for editing.
pub fn server_section(m: &Manifest) -> Server {
    m.server.clone().unwrap_or_else(|| {
        let now = now_rfc3339();
        Server {
            updated_at: now.clone(),
            normalized: None,
            lifecycle: Lifecycle::Active,
            lifecycle_at: now,
            lifecycle_reason: None,
            retention: None,
            extra: Default::default(),
        }
    })
}

/// Server section with `lifecycle` updated; `None` if it would not change
/// (so the caller can skip the write).
pub fn with_lifecycle(
    m: &Manifest,
    lifecycle: Lifecycle,
    reason: Option<String>,
) -> Option<Server> {
    let mut s = server_section(m);
    if m.server.is_some() && s.lifecycle == lifecycle {
        return None;
    }
    let now = now_rfc3339();
    s.lifecycle = lifecycle;
    s.lifecycle_at = now.clone();
    s.lifecycle_reason = reason;
    s.updated_at = now;
    Some(s)
}

/// Server section recording a finished transform of the content it read.
/// Pass the manifest the transform **read**, not a later one, so a stale
/// build is visibly stale.
pub fn with_normalized(
    read: &Manifest,
    current: &Manifest,
    version: u32,
    manifest_key: Option<String>,
) -> Server {
    let mut s = server_section(current);
    let now = now_rfc3339();
    s.normalized = Some(Normalized {
        version,
        source_content_sha256: read.raw.content_sha256.clone(),
        source_epoch: read.raw.epoch,
        built_at: now.clone(),
        manifest_key,
        extra: Default::default(),
    });
    s.updated_at = now;
    s
}

/// Whether the transform must (re)run: no normalized output yet, or it was
/// built from different raw content. The server's own writes never trigger it.
pub fn needs_transform(m: &Manifest) -> bool {
    !m.normalized_is_current()
}

/// Columns for the Supabase `sessions` pointer row. `manifest_etag` comes
/// from the object version read, not from the manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PointerFields {
    pub workspace_id: String,
    pub machine_id: String,
    pub harness: String,
    pub session_id: String,
    pub started_at: String,
    pub ended_at: Option<String>,
    pub state: EffectiveState,
    pub lifecycle: Lifecycle,
    pub trace_id: String,
    pub manifest_revision: u64,
    pub raw_bytes: u64,
    pub stored_bytes: u64,
    pub chunk_count: u64,
    pub raw_epoch: u32,
    pub content_sha256: String,
    pub normalized_version: Option<u32>,
    pub normalized_stale: bool,
}

pub fn pointer_fields(m: &Manifest) -> PointerFields {
    let normalized = m.server.as_ref().and_then(|s| s.normalized.as_ref());
    PointerFields {
        workspace_id: m.session.workspace_id.clone(),
        machine_id: m.session.machine_id.clone(),
        harness: m.session.harness.clone(),
        session_id: m.session.session_id.clone(),
        started_at: m.session.started_at.clone(),
        ended_at: m.session.ended_at.clone(),
        state: m.effective_state(),
        lifecycle: m.lifecycle(),
        trace_id: m.trace_id.clone(),
        manifest_revision: m.revision,
        raw_bytes: m.raw.raw_bytes,
        stored_bytes: m.raw.stored_bytes,
        chunk_count: m.raw.chunk_count,
        raw_epoch: m.raw.epoch,
        content_sha256: m.raw.content_sha256.clone(),
        normalized_version: normalized.map(|n| n.version),
        normalized_stale: normalized
            .is_some_and(|n| n.source_content_sha256 != m.raw.content_sha256),
    }
}

//! Passive OpenClaw agent-database acquisition. See plans/history-providers/openclaw.md.
use super::*;
use crate::discover::{Candidate, DiscoveryEnv, ScanEnv, ShallowSession, ShallowSessionProvider};
use crate::hydrate::{IngestOutcome, SelectedIngest, SelectedIngestResult};
use crate::sources::catalog::{Discovery, Fixtures, Hydration, LocalSource, Relationships};
use crate::EvidenceKind;
use serde::{Deserialize, Serialize};
mod cold;
mod events;
mod format;
mod legacy;
mod legacy_sweep;
mod normalize;
mod retire;
mod roots;
mod schema;
mod storage;
mod stores;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod tests_legacy;
#[cfg(test)]
mod tests_review;
mod tree;
const ID: &str = "openclaw";
pub(crate) const LOCAL_SOURCE: LocalSource = LocalSource {
    id: ID,
    discovery: Discovery::Provider(|| Box::new(Provider)),
    hydration: Hydration::Parser(ingest),
    transcript_roots: |roots| roots.for_provider(ID).to_vec(),
    relationships: Relationships::NONE,
    resume: None,
    fixtures: Fixtures::Dir(ID),
};
pub(crate) const METADATA: ProviderMetadata = ProviderMetadata {
    markers: true,
    message_ids: MessageIdOrigin::Provider,
    usage: Some(UsageRegistration {
        accounting: UsageAccounting::PerMessage,
        normalize: super::pi::usage::normalize,
        splits_requests: false,
    }),
};
pub(crate) const ADAPTER: Option<ProviderAdapter> = Some(ProviderAdapter {
    generation: GENERATION,
    acquire,
    sweep,
    resolve_roots: roots::resolve,
    watch_roots: stores::watch_roots,
});
const GENERATION: &str = "openclaw_agent24_v2";
#[derive(Debug, Serialize, Deserialize)]
struct Session {
    id: String,
    native_id: String,
    agent_id: String,
    session_key: String,
    channel: Option<String>,
    window_channel: Option<String>,
    account_id: Option<String>,
    chat_type: Option<String>,
    scope: Option<String>,
    reason: Option<String>,
    previous_session_id: Option<String>,
    parent_session_key: Option<String>,
    spawned_by: Option<String>,
    created_via: Option<String>,
    harness: Option<String>,
    model_provider: Option<String>,
    model: Option<String>,
    status: Option<String>,
    title: Option<String>,
    started: i64,
    latest: i64,
    ended: Option<i64>,
    /// Hot rows: count, last sequence and rewrite watermark generation.
    event_count: Option<i64>,
    last_seq: Option<i64>,
    rewrite: Option<String>,
    /// Present when the transcript lives only in a cold archive.
    cold: Option<ColdArchive>,
    /// Agent-database schema; None for a legacy JSONL transcript.
    schema_version: Option<i64>,
    /// Read from the pre-2026.8 JSONL store rather than the agent database.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    legacy: bool,
    /// A JSONL transcript whose final record was still being written: its
    /// complete records only add evidence and never retire any.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    partial: bool,
    /// Change stamp of this session's rows; drives incremental sweeps.
    stamp: String,
    events: Vec<Event>,
}
#[derive(Debug, Serialize, Deserialize)]
struct ColdArchive {
    generation: String,
    name: String,
    sha256: String,
    event_count: i64,
    raw_bytes: i64,
    archive_bytes: i64,
    last_seq: i64,
    archived_at: i64,
    storage: String,
}
#[derive(Debug, Serialize, Deserialize)]
struct Event {
    seq: i64,
    created_at: i64,
    /// On the session's active branch (`session_transcript_active_events`).
    active: bool,
    entry: Value,
}
struct Provider;
impl ShallowSessionProvider for Provider {
    fn source(&self) -> &'static str {
        ID
    }
    fn evidence_kinds(&self) -> &'static [EvidenceKind] {
        &[
            EvidenceKind::History,
            EvidenceKind::SessionEvent,
            EvidenceKind::ToolCall,
            EvidenceKind::SessionMarker,
        ]
    }
    fn enumerate(&self, env: &DiscoveryEnv<'_>, limit: Option<usize>) -> Result<Vec<Candidate>> {
        storage::enumerate(&env.provider_roots, limit)
    }
    fn fingerprint_inputs(&self, env: &DiscoveryEnv<'_>) -> Result<Vec<Candidate>> {
        stores::fingerprint_inputs(&env.provider_roots)
    }
    fn read_shallow(
        &self,
        scan: &ScanEnv<'_>,
        _: Option<&Connection>,
        c: &Candidate,
    ) -> Result<Option<ShallowSession>> {
        storage::shallow(scan, c)
    }
}
fn acquire(
    options: &HydrateSessionOptions,
    path: &Path,
    context: &AcquisitionContext<'_>,
) -> Result<AcquiredSession> {
    storage::capture(context, path, &options.session_id)
}
fn ingest(ctx: SelectedIngest<'_>) -> Result<SelectedIngestResult> {
    let snapshot = ctx
        .provider_session
        .ok_or_else(|| anyhow::anyhow!("missing OpenClaw snapshot"))?;
    let session: Session = serde_json::from_value(snapshot.payload.clone())?;
    let path = ctx
        .path
        .ok_or_else(|| anyhow::anyhow!("missing OpenClaw path"))?;
    legacy::complete_or_append(ctx.conn, &session, path)?;
    Ok((
        IngestOutcome {
            records: snapshot.records,
            ..Default::default()
        },
        vec![],
        None,
    ))
}

const STAMPS: &str = "openclaw_session_stamps";
#[cfg(not(test))]
const MAX_SWEEP: usize = 100_000;
/// Small enough for a test to exceed.
#[cfg(test)]
const MAX_SWEEP: usize = 4;
fn sweep(mut ctx: SweepContext<'_>) -> Result<SweepResult> {
    let mut result = SweepResult::default();
    // Only an unread database withholds deletion proof; a failed session
    // inside a readable one says nothing about other databases.
    let mut unread_databases = 0u64;
    let mut cache = ctx
        .state
        .get(STAMPS)
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let databases = stores::databases(ctx.roots)?;
    if ctx.paths.is_none() {
        let visited: std::collections::BTreeSet<_> = databases
            .iter()
            .map(|p| p.to_string_lossy().into_owned())
            .collect();
        cache.retain(|path, _| visited.contains(path));
    }
    for path in &databases {
        if !retire::in_scope(path, ctx.paths) {
            continue;
        }
        crate::ingest::check_capture_cancelled()?;
        let key = path.to_string_lossy().into_owned();
        let previous = match cache.remove(&key) {
            Some(Value::Object(map)) => map,
            _ => Map::new(),
        };
        match sweep_database(&ctx, path, &previous) {
            Ok((inserted, unread, stamps)) => {
                result.inserted += inserted;
                result.unread += unread;
                cache.insert(key, Value::Object(stamps));
            }
            Err(error) => {
                crate::ingest::check_capture_cancelled()?;
                // A busy, locked or unreadable live database is unread, not
                // empty: its prior evidence and cursor stay in place.
                eprintln!("OpenClaw {}: {error:#}", path.display());
                result.unread += 1;
                unread_databases += 1;
                cache.insert(key, Value::Object(previous));
            }
        }
    }
    ctx.state.insert(STAMPS.into(), Value::Object(cache));
    // After every database pass: a session a database now holds is claimed
    // by it before its JSONL transcript is considered.
    let (inserted, unread_transcripts, unread_stores) = legacy_sweep::sweep(&mut ctx)?;
    result.inserted += inserted;
    result.unread += unread_transcripts;
    retire::retire_missing_locators(&ctx, unread_databases == 0, !unread_stores)?;
    Ok(result)
}

/// Normalize every session of one database whose stamp changed, then retire
/// sessions the complete snapshot no longer holds. One read snapshot covers
/// the listing and every re-read session; each session commits atomically.
fn sweep_database(
    ctx: &SweepContext<'_>,
    path: &Path,
    previous: &Map<String, Value>,
) -> Result<(usize, u64, Map<String, Value>)> {
    let context = AcquisitionContext::new(ID, ctx.roots);
    let mut stamps = Map::new();
    let mut inserted = 0;
    let mut unread = 0;
    let sessions = storage::read(&context, path, |conn, schema| {
        let mut sessions = storage::sessions(conn, ctx.roots, path, schema, MAX_SWEEP + 1, None)?;
        // A truncated listing is not a complete snapshot: retiring against it
        // would delete sessions past the bound.
        anyhow::ensure!(
            sessions.len() <= MAX_SWEEP,
            "OpenClaw database exceeds {MAX_SWEEP} sessions"
        );
        for s in &mut sessions {
            crate::ingest::check_capture_cancelled()?;
            let stamp = format!("{GENERATION}/{}", s.stamp);
            let cursor = previous.get(&s.id);
            let repair = ctx.repairs.contains(ID, &s.id) || !present(ctx.conn, &s.id)?;
            if !repair && cursor.and_then(|c| c["stamp"].as_str()) == Some(&stamp) {
                stamps.insert(s.id.clone(), cursor.unwrap().clone());
                continue;
            }
            let from = cursor
                .filter(|_| !repair)
                .map(|c| appended_from(conn, s, c))
                .transpose()?
                .flatten();
            let normalized = normalize_one(ctx, &context, (conn, schema), path, s, from);
            // Each session's events are released before the next is read.
            s.events = vec![];
            match normalized {
                Ok(n) => inserted += n,
                Err(error) => {
                    crate::ingest::check_capture_cancelled()?;
                    // One malformed session keeps its prior evidence and its
                    // previous cursor; the rest of the database still syncs.
                    eprintln!(
                        "OpenClaw {} session {:?}: {error:#}",
                        path.display(),
                        s.native_id
                    );
                    unread += 1;
                    if let Some(cursor) = cursor {
                        stamps.insert(s.id.clone(), cursor.clone());
                    }
                    continue;
                }
            }
            stamps.insert(s.id.clone(), cursor_for(conn, s, stamp)?);
        }
        Ok(sessions)
    })?;
    let tx = ctx.conn.unchecked_transaction()?;
    retire::retire_absent(&tx, path, &sessions)?;
    tx.commit()?;
    Ok((inserted, unread, stamps))
}
/// Events re-read before the previous last sequence: in-place annotations
/// (delivery and mirror fields) land on the newest rows.
const TAIL: usize = 32;

/// Read and normalize one changed session: only its grown tail when `from`
/// proves an append, else the whole transcript. One ledger transaction.
fn normalize_one(
    ctx: &SweepContext<'_>,
    context: &AcquisitionContext<'_>,
    (conn, schema): (&Connection, &schema::Schema),
    path: &Path,
    s: &mut Session,
    from: Option<i64>,
) -> Result<usize> {
    s.events = match from {
        Some(seq) => events::tail(conn, schema, &s.native_id, seq)?,
        None => events::events(context, conn, path, schema, s)?,
    };
    let tx = ctx.conn.unchecked_transaction()?;
    let n = match from {
        Some(_) => normalize::append(&tx, s, path)?,
        None => normalize::session(&tx, s, path)?,
    };
    tx.commit()?;
    Ok(n)
}

/// The first sequence to re-read when the session only grew since `cursor`:
/// same rewrite watermark, still hot, every new row past the old end, and the
/// old tail's entry IDs unchanged. Anything else needs a complete read, which
/// also retires evidence the transcript dropped.
fn appended_from(conn: &Connection, s: &Session, cursor: &Value) -> Result<Option<i64>> {
    let (Some(count), Some(last)) = (s.event_count, s.last_seq) else {
        return Ok(None);
    };
    let tail: Vec<(i64, String)> =
        serde_json::from_value(cursor["tail"].clone()).unwrap_or_default();
    let (Some(old_count), Some(old_last), Some(first)) = (
        cursor["count"].as_i64(),
        cursor["last_seq"].as_i64(),
        tail.first().map(|t| t.0),
    ) else {
        return Ok(None);
    };
    let grew = s.cold.is_none()
        && cursor["rewrite"].as_str() == s.rewrite.as_deref()
        && cursor["stamp"]
            .as_str()
            .is_some_and(|c| c.starts_with(GENERATION))
        && count - old_count == last - old_last
        && last >= old_last;
    if !grew || events::ids_at(conn, &s.native_id, first, old_last)? != tail {
        return Ok(None);
    }
    Ok(Some(first))
}
fn cursor_for(conn: &Connection, s: &Session, stamp: String) -> Result<Value> {
    let tail = match s.cold {
        Some(_) => vec![],
        None => events::tail_ids(conn, &s.native_id, TAIL)?,
    };
    Ok(serde_json::json!({
        "stamp": stamp,
        "count": s.event_count,
        "last_seq": s.last_seq,
        "rewrite": s.rewrite,
        "tail": tail,
    }))
}
fn present(conn: &Connection, id: &str) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sessions WHERE source=?1 AND session_id=?2 AND raw_path IS NOT NULL)",
        [ID, id],
        |r| r.get(0),
    )?)
}

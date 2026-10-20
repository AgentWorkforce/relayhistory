//! Passive OpenClaw agent-database acquisition. See plans/history-providers/openclaw.md.
use super::*;
use crate::discover::{Candidate, DiscoveryEnv, ScanEnv, ShallowSession, ShallowSessionProvider};
use crate::hydrate::{IngestOutcome, SelectedIngest, SelectedIngestResult};
use crate::sources::catalog::{Discovery, Fixtures, Hydration, LocalSource, Relationships};
use crate::EvidenceKind;
use serde::{Deserialize, Serialize};
mod cold;
mod conversation;
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
const GENERATION: &str = "openclaw_agent24_v3";
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
    /// For a tail re-read: whether the newest user turn before the tail came
    /// from a person.
    #[serde(default, skip)]
    prompted: bool,
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
    let path = ctx
        .path
        .ok_or_else(|| anyhow::anyhow!("missing OpenClaw path"))?;
    // A database capture is a conversation; a JSONL capture is one transcript.
    if snapshot.payload.get("windows").is_some() {
        let conv: conversation::Conversation = serde_json::from_value(snapshot.payload.clone())?;
        normalize::session(ctx.conn, &conv, path)?;
    } else {
        let session: Session = serde_json::from_value(snapshot.payload.clone())?;
        legacy::complete_or_append(ctx.conn, session, path)?;
    }
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

/// Normalize every conversation of one database whose stamp changed, then
/// retire conversations the complete snapshot no longer holds. One read
/// snapshot covers the listing and every re-read window; each conversation
/// commits atomically.
fn sweep_database(
    ctx: &SweepContext<'_>,
    path: &Path,
    previous: &Map<String, Value>,
) -> Result<(usize, u64, Map<String, Value>)> {
    let context = AcquisitionContext::new(ID, ctx.roots);
    let mut stamps = Map::new();
    let mut inserted = 0;
    let mut unread = 0;
    // A failed conversation keeps its windows' rows under any earlier
    // identity too (a per-window ID before generation v3).
    let mut kept = vec![];
    let mut ids = storage::read(&context, path, |conn, schema| {
        let windows = storage::sessions(conn, ctx.roots, path, schema, MAX_SWEEP + 1, None)?;
        // A truncated listing is not a complete snapshot: retiring against it
        // would delete conversations past the bound.
        anyhow::ensure!(
            windows.len() <= MAX_SWEEP,
            "OpenClaw database exceeds {MAX_SWEEP} sessions"
        );
        let mut conversations = conversation::group(windows);
        for conv in &mut conversations {
            crate::ingest::check_capture_cancelled()?;
            let stamp = format!("{GENERATION}/{}", conv.stamp());
            let cursor = previous.get(&conv.id);
            let repair = ctx.repairs.contains(ID, &conv.id) || !present(ctx.conn, &conv.id)?;
            if !repair && cursor.and_then(|c| c["stamp"].as_str()) == Some(&stamp) {
                stamps.insert(conv.id.clone(), cursor.unwrap().clone());
                continue;
            }
            let plan = cursor
                .filter(|_| !repair)
                .map(|c| plan_append(conn, conv, c))
                .transpose()?
                .flatten();
            let normalized = normalize_one(ctx, &context, (conn, schema), path, conv, plan);
            // Each conversation's events are released before the next is read.
            for window in &mut conv.windows {
                window.events = vec![];
            }
            match normalized {
                Ok(n) => inserted += n,
                Err(error) => {
                    crate::ingest::check_capture_cancelled()?;
                    // One malformed conversation keeps its prior evidence and
                    // its previous cursor; the rest of the database still syncs.
                    eprintln!(
                        "OpenClaw {} session {:?}: {error:#}",
                        path.display(),
                        conv.key
                    );
                    unread += 1;
                    kept.extend(conv.window_ids());
                    if let Some(cursor) = cursor {
                        stamps.insert(conv.id.clone(), cursor.clone());
                    }
                    continue;
                }
            }
            stamps.insert(conv.id.clone(), cursor_for(conn, conv, stamp)?);
        }
        Ok(conversations.into_iter().map(|c| c.id).collect::<Vec<_>>())
    })?;
    ids.extend(kept);
    let tx = ctx.conn.unchecked_transaction()?;
    retire::retire_absent(&tx, path, &ids)?;
    tx.commit()?;
    Ok((inserted, unread, stamps))
}
/// Events re-read before a window's previous last sequence: in-place
/// annotations (delivery and mirror fields) land on the newest rows.
const TAIL: usize = 32;
/// How one window of a grown conversation is re-read.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Read {
    Unchanged,
    From(i64),
    Whole,
}

/// Read and normalize one changed conversation: only grown tails and new
/// windows when `plan` proves an append, else every window. One ledger
/// transaction.
fn normalize_one(
    ctx: &SweepContext<'_>,
    context: &AcquisitionContext<'_>,
    (conn, schema): (&Connection, &schema::Schema),
    path: &Path,
    conv: &mut conversation::Conversation,
    plan: Option<Vec<Read>>,
) -> Result<usize> {
    for (i, window) in conv.windows.iter_mut().enumerate() {
        window.events = match plan.as_ref().map_or(Read::Whole, |p| p[i]) {
            Read::Unchanged => vec![],
            Read::From(seq) => {
                window.prompted = events::prompted_before(conn, schema, &window.native_id, seq)?;
                events::tail(conn, schema, &window.native_id, seq)?
            }
            Read::Whole => events::events(context, conn, path, schema, window)?,
        };
    }
    let tx = ctx.conn.unchecked_transaction()?;
    let n = match plan {
        Some(_) => normalize::append(&tx, conv, path)?,
        None => normalize::session(&tx, conv, path)?,
    };
    tx.commit()?;
    Ok(n)
}

/// How to re-read a conversation that only grew since `cursor`: every
/// previous window still present, each unchanged, appended (same rewrite
/// watermark, still hot, new rows past the old end, the old tail's entry IDs
/// intact) or new. Anything else needs a complete read, which also retires
/// evidence the conversation dropped.
fn plan_append(
    conn: &Connection,
    conv: &conversation::Conversation,
    cursor: &Value,
) -> Result<Option<Vec<Read>>> {
    let Some(previous) = cursor["windows"].as_object() else {
        return Ok(None);
    };
    let current: std::collections::BTreeSet<&str> =
        conv.windows.iter().map(|w| w.native_id.as_str()).collect();
    if !cursor["stamp"]
        .as_str()
        .is_some_and(|c| c.starts_with(GENERATION))
        || previous.keys().any(|id| !current.contains(id.as_str()))
    {
        return Ok(None);
    }
    let mut plan = vec![];
    for window in &conv.windows {
        let step = match previous.get(&window.native_id) {
            None => Read::Whole,
            Some(old) if old["stamp"].as_str() == Some(&window.stamp) => Read::Unchanged,
            Some(old) => match appended_from(conn, window, old)? {
                Some(seq) => Read::From(seq),
                None => return Ok(None),
            },
        };
        plan.push(step);
    }
    Ok(Some(plan))
}
/// The first sequence to re-read when one window only grew since `cursor`.
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
        && count - old_count == last - old_last
        && last >= old_last;
    if !grew || events::ids_at(conn, &s.native_id, first, old_last)? != tail {
        return Ok(None);
    }
    Ok(Some(first))
}
fn cursor_for(
    conn: &Connection,
    conv: &conversation::Conversation,
    stamp: String,
) -> Result<Value> {
    let mut windows = Map::new();
    for s in &conv.windows {
        let tail = match s.cold {
            Some(_) => vec![],
            None => events::tail_ids(conn, &s.native_id, TAIL)?,
        };
        windows.insert(
            s.native_id.clone(),
            serde_json::json!({
                "stamp": s.stamp,
                "count": s.event_count,
                "last_seq": s.last_seq,
                "rewrite": s.rewrite,
                "tail": tail,
            }),
        );
    }
    Ok(serde_json::json!({"stamp": stamp, "windows": windows}))
}
fn present(conn: &Connection, id: &str) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sessions WHERE source=?1 AND session_id=?2 AND raw_path IS NOT NULL)",
        [ID, id],
        |r| r.get(0),
    )?)
}

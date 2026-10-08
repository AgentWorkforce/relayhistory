//! Every Codex `token_count` counter reaches the public read surface verbatim.
//!
//! A turn whose rollout reports usage through `event_msg/token_count` without
//! an assistant message still billed those tokens. Each populated snapshot is
//! a `usage_snapshot` marker on [`SessionEvidence::markers`]: the provider's
//! `info` as written (`null` included), stamped with the Codex `turn_id` it
//! fell inside, in read order.
//! Snapshots are cumulative and never differenced here, so every one of them
//! stays readable for a consumer that does its own accounting.

use ai_hist::{
    Marker, ProviderRoots, SessionEvidence, SessionQuery, SessionRef, SessionStore, Source,
    StoreOptions, SyncOptions,
};
use serde_json::{json, Value};
use std::fs;
use std::path::Path;

fn fixture(name: &str) -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/codex")
        .join(name);
    fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

// `StoreOptions` is `#[non_exhaustive]`, so an outside crate builds it field
// by field; this test is written as that crate.
#[allow(clippy::field_reassign_with_default)]
fn synced(rollout: &str, session_id: &str) -> (tempfile::TempDir, SessionEvidence) {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    let day = home.join(".codex/sessions/2026/04/20");
    fs::create_dir_all(&day).unwrap();
    fs::write(
        day.join("rollout-2026-04-20T00-00-00-fixture.jsonl"),
        rollout,
    )
    .unwrap();
    let mut options = StoreOptions::default();
    options.db_path = Some(home.join("ai-history.db"));
    options.roots = Some(ProviderRoots::from_home(
        home.to_path_buf(),
        home.join(".local/share/opencode/opencode.db"),
    ));
    let store = SessionStore::open(options).expect("open");
    store.sync(SyncOptions::default()).expect("sync");
    let evidence = store
        .session(
            &SessionRef::id(Source::Codex, session_id),
            SessionQuery::default(),
        )
        .expect("read")
        .expect("session is cataloged");
    (dir, evidence)
}

fn usage_snapshots(evidence: &SessionEvidence) -> Vec<&Marker> {
    evidence
        .markers
        .iter()
        .filter(|marker| marker.kind == "usage_snapshot")
        .collect()
}

/// `(turn_id, payload)` for every snapshot, in transcript order.
fn snapshots(evidence: &SessionEvidence) -> Vec<(Option<&str>, Value)> {
    usage_snapshots(evidence)
        .into_iter()
        .map(|marker| {
            assert_eq!(marker.subkind.as_deref(), Some("token_count"));
            let raw: Value = serde_json::from_str(marker.raw_payload().expect("raw payload"))
                .expect("raw payload is JSON");
            (marker.turn_id.as_deref(), raw)
        })
        .collect()
}

fn line_info(rollout: &str, line: usize) -> Value {
    let value: Value = serde_json::from_str(rollout.lines().nth(line).unwrap()).unwrap();
    value["payload"]["info"].clone()
}

/// burn's `simple-turn`: a turn whose only usage evidence is a `token_count`.
#[test]
fn a_turn_with_no_assistant_message_keeps_its_token_count() {
    let rollout = fixture("simple-turn.jsonl");
    let (_dir, evidence) = synced(&rollout, "sess_simple_1");
    eprintln!(
        "simple-turn: requests={} usage={:?} snapshots={:?}",
        evidence.requests.len(),
        evidence.usage,
        snapshots(&evidence)
    );
    // Both snapshots, in read order: the `info: null` one Codex writes before
    // the turn has spent anything, then the populated one, as written.
    assert_eq!(
        snapshots(&evidence),
        vec![
            (Some("turn_simple_1"), Value::Null),
            (Some("turn_simple_1"), line_info(&rollout, 4)),
        ]
    );
    assert_eq!(
        snapshots(&evidence)[1].1["total_token_usage"],
        json!({"input_tokens":1000,"cached_input_tokens":400,"output_tokens":120,"reasoning_output_tokens":30,"total_tokens":1120})
    );
}

/// burn's verbatim `compaction`: two turns either side of a `compacted`
/// record, neither with an assistant message. Both cumulative snapshots stay
/// readable, so the consumer can difference them itself.
#[test]
fn every_cumulative_snapshot_of_an_assistantless_rollout_is_kept() {
    let rollout = fixture("compaction-usage-only.jsonl");
    let (_dir, evidence) = synced(&rollout, "sess_codex_compact");
    eprintln!(
        "compaction-usage-only: requests={} usage={:?} snapshots={:?}",
        evidence.requests.len(),
        evidence.usage,
        snapshots(&evidence)
    );
    assert_eq!(
        snapshots(&evidence),
        vec![
            (Some("turn_compact_1"), line_info(&rollout, 3)),
            (Some("turn_compact_2"), line_info(&rollout, 9)),
        ]
    );
}

/// burn's `session-meta-relationships`: a snapshot that omits `total_tokens`
/// is stored as written, not completed.
#[test]
fn a_snapshot_is_stored_as_written_not_completed() {
    let rollout = fixture("session-meta-relationships.jsonl");
    let (_dir, evidence) = synced(&rollout, "sess_meta_child");
    eprintln!(
        "session-meta-relationships: requests={} usage={:?} snapshots={:?}",
        evidence.requests.len(),
        evidence.usage,
        snapshots(&evidence)
    );
    let snapshots = snapshots(&evidence);
    assert_eq!(
        snapshots,
        vec![(Some("turn_meta_1"), line_info(&rollout, 4))]
    );
    assert!(snapshots[0].1["total_token_usage"]
        .get("total_tokens")
        .is_none());
}

/// A rollout with assistant messages keeps its per-request deltas exactly as
/// before; the snapshots sit beside them as markers and are never added into
/// `requests` or `usage`.
#[test]
fn snapshots_do_not_change_per_request_usage() {
    let rollout = fixture("compaction.jsonl");
    let (_dir, evidence) = synced(&rollout, "sess_codex_compact");
    eprintln!(
        "compaction: requests={:?} usage={:?} snapshots={:?}",
        evidence
            .requests
            .iter()
            .map(|r| r.usage.as_ref().map(|u| u.provider_total_tokens))
            .collect::<Vec<_>>(),
        evidence.usage.as_ref().and_then(|u| u.usage.as_ref()),
        snapshots(&evidence)
    );
    assert_eq!(snapshots(&evidence).len(), 2);
    let summary = evidence.usage.expect("usage summary");
    assert_eq!(summary.total_request_count, 2);
    // The second snapshot's cumulative total: the deltas still sum to it, so
    // nothing was counted twice.
    assert_eq!(
        summary.usage.expect("totals").provider_total_tokens,
        Some(6950)
    );
}

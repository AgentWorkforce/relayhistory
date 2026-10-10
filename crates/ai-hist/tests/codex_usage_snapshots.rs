//! Every Codex `token_count` counter and `turn_context` record reaches the
//! public read surface verbatim.
//!
//! A turn whose rollout reports usage through `event_msg/token_count` without
//! an assistant message still billed those tokens. Each populated snapshot is
//! a `usage_snapshot` marker on [`SessionEvidence::markers`]: the provider's
//! `info`, typed in [`Marker::usage_snapshot`] (none for `info: null`),
//! stamped with the Codex `turn_id` it fell inside, in read order.
//! Snapshots are cumulative and never differenced here, so every one of them
//! stays readable for a consumer that does its own accounting.

use ai_hist::{
    ChangeOp, ChangeQuery, DiscoveryOptions, EvidenceRow, ForgetOptions, ForgetScope,
    HydrateOptions, Marker, ProviderRoots, SessionEvidence, SessionQuery, SessionRef, SessionStore,
    Source, StoreOptions, SyncOptions, Watermark,
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
fn staged(rollout: &str) -> (tempfile::TempDir, SessionStore) {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    let day = home.join(".codex/sessions/2026/04/20");
    fs::create_dir_all(&day).unwrap();
    fs::write(
        day.join("rollout-2026-04-20T00-00-00-fixture.jsonl"),
        rollout,
    )
    .unwrap();
    let options = StoreOptions::default()
        .db_path(home.join("ai-history.db"))
        .roots(ProviderRoots::from_home(
            home.to_path_buf(),
            home.join(".local/share/opencode/opencode.db"),
        ));
    let store = SessionStore::open(options).expect("open");
    (dir, store)
}

fn synced(rollout: &str, session_id: &str) -> (tempfile::TempDir, SessionEvidence) {
    let (dir, store) = staged(rollout);
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
            // The counters are typed; nothing is left to parse.
            assert_eq!(marker.payload, None);
            assert_eq!(marker.raw_payload(), None);
            // `info: null` has no snapshot.
            let info = marker
                .usage_snapshot
                .as_ref()
                .map_or(Value::Null, |snapshot| snapshot.to_value());
            (marker.turn_id.as_deref(), info)
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

/// burn's `compaction` (session id renamed): two turns either side of a `compacted`
/// record, neither with an assistant message. Both cumulative snapshots stay
/// readable, so the consumer can difference them itself.
#[test]
fn every_cumulative_snapshot_of_an_assistantless_rollout_is_kept() {
    let rollout = fixture("compaction-usage-only.jsonl");
    let (_dir, evidence) = synced(&rollout, "sess_codex_compact_usage_only");
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
    // Each request keeps its own delta: 3200 from zero, then 6950 - 3200.
    assert_eq!(
        evidence
            .requests
            .iter()
            .map(|request| request.usage.as_ref().and_then(|u| u.provider_total_tokens))
            .collect::<Vec<_>>(),
        vec![Some(3200), Some(3750)]
    );
    let summary = evidence.usage.expect("usage summary");
    assert_eq!(summary.total_request_count, 2);
    // The second snapshot's cumulative total: the deltas still sum to it, so
    // nothing was counted twice.
    assert_eq!(
        summary.usage.expect("totals").provider_total_tokens,
        Some(6950)
    );
}

/// burn's `simple-turn` again: its only assistant-free turn still names its
/// model, through the `turn_context` the provider wrote, kept whole.
#[test]
fn a_turn_with_no_assistant_message_keeps_its_turn_context() {
    let rollout = fixture("simple-turn.jsonl");
    let (_dir, evidence) = synced(&rollout, "sess_simple_1");
    let contexts: Vec<(Option<&str>, Value)> = evidence
        .markers
        .iter()
        .filter(|marker| marker.kind == "turn_context")
        .map(|marker| {
            assert_eq!(marker.subkind.as_deref(), Some("turn_context"));
            (
                marker.turn_id.as_deref(),
                serde_json::from_str(marker.raw_payload().expect("raw payload")).unwrap(),
            )
        })
        .collect();
    eprintln!("simple-turn: turn_context markers={contexts:?}");
    let written: Value = serde_json::from_str(rollout.lines().nth(1).unwrap()).unwrap();
    assert_eq!(
        contexts,
        vec![(Some("turn_simple_1"), written["payload"].clone())]
    );
    assert_eq!(contexts[0].1["model"], "gpt-5.4");
}

/// Token counters reach the consumer exactly as the provider wrote them. The
/// marker payload bound cuts long strings and containers past 32 entries;
/// it never touches a number, so a counter at `u64::MAX`, one above
/// `i64::MAX`, a zero, and even a malformed fractional or negative one are
/// all stored unchanged. Key order is not preserved and is not part of the
/// contract.
#[test]
fn counters_are_never_altered_by_the_marker_bound() {
    let info = json!({
        "total_token_usage": {
            "input_tokens": u64::MAX,
            "cached_input_tokens": 9_223_372_036_854_775_808u64,
            "cache_write_input_tokens": 0,
            "output_tokens": 1.5,
            "reasoning_output_tokens": -3,
            "total_tokens": 18_446_744_073_709_551_000u64
        },
        "last_token_usage": {"input_tokens": 1, "output_tokens": 2, "total_tokens": 3},
        "model_context_window": 400_000
    });
    let rollout = [
        json!({"timestamp":"2026-04-20T00:00:00.000Z","type":"session_meta","payload":{"id":"sess_counters","cwd":"/tmp/project"}}),
        json!({"timestamp":"2026-04-20T00:00:00.100Z","type":"turn_context","payload":{"turn_id":"turn_c","cwd":"/tmp/project","model":"gpt-5.4"}}),
        json!({"timestamp":"2026-04-20T00:00:01.000Z","type":"event_msg","payload":{"type":"token_count","info":info}}),
    ]
    .iter()
    .map(|line| format!("{line}\n"))
    .collect::<String>();
    let (_dir, evidence) = synced(&rollout, "sess_counters");
    assert_eq!(snapshots(&evidence), vec![(Some("turn_c"), info)]);
    let total = usage_snapshots(&evidence)[0]
        .usage_snapshot
        .as_ref()
        .and_then(|snapshot| snapshot.total_token_usage.as_ref())
        .expect("typed total");
    assert_eq!(
        total.input_tokens.as_ref().and_then(Value::as_u64),
        Some(u64::MAX)
    );
    assert_eq!(
        total.output_tokens.as_ref().and_then(Value::as_f64),
        Some(1.5)
    );
    assert_eq!(
        total
            .reasoning_output_tokens
            .as_ref()
            .and_then(Value::as_i64),
        Some(-3)
    );
}

/// State markers survive a serde round trip unchanged, `info: null` included:
/// that snapshot stores no payload, so it cannot read back as `Some(Null)`
/// before serialization and `None` after it.
#[test]
fn state_markers_round_trip_through_serde() {
    let rollout = fixture("simple-turn.jsonl");
    let (_dir, evidence) = synced(&rollout, "sess_simple_1");
    let null_snapshot = usage_snapshots(&evidence)[0];
    assert_eq!(null_snapshot.raw_payload(), None);
    assert_eq!(null_snapshot.payload, None);
    assert_eq!(null_snapshot.usage_snapshot, None);
    assert!(usage_snapshots(&evidence)[1].usage_snapshot.is_some());
    let encoded = serde_json::to_string(&evidence.markers).unwrap();
    let decoded: Vec<Marker> = serde_json::from_str(&encoded).unwrap();
    assert_eq!(decoded, evidence.markers);
}

/// A payload stored as the `info` object itself -- by a development build
/// before the compact form -- reads back typed all the same.
#[test]
fn an_info_object_payload_reads_back_typed() {
    let rollout = fixture("simple-turn.jsonl");
    let (dir, evidence) = synced(&rollout, "sess_simple_1");
    let expected = snapshots(&evidence);
    let conn = rusqlite::Connection::open(dir.path().join("ai-history.db")).unwrap();
    let rewritten = conn
        .execute(
            "UPDATE session_markers SET payload_json = ?1 \
             WHERE kind = 'usage_snapshot' AND payload_json IS NOT NULL",
            [serde_json::to_string(&line_info(&rollout, 4)).unwrap()],
        )
        .unwrap();
    assert_eq!(rewritten, 1);
    drop(conn);
    let mut options = StoreOptions::default();
    options.db_path = Some(dir.path().join("ai-history.db"));
    options.read_only = true;
    let reread = SessionStore::open(options)
        .unwrap()
        .session(
            &SessionRef::id(Source::Codex, "sess_simple_1"),
            SessionQuery::default(),
        )
        .unwrap()
        .unwrap();
    assert_eq!(snapshots(&reread), expected);
}

/// A rollout whose `turn_context` records are the `configs` given, one turn
/// each, every turn closed, with Codex's own turn ids. Its `task_started`
/// records write no `root_turn_id`, as older Codex builds do.
fn configured_rollout(session_id: &str, configs: &[Value]) -> String {
    rollout_with_roots(session_id, configs, false)
}

/// As [`configured_rollout`], with `task_started` writing the turn's
/// `root_turn_id` when `rooted`, as current Codex does.
fn rollout_with_roots(session_id: &str, configs: &[Value], rooted: bool) -> String {
    let mut lines = vec![
        json!({"timestamp":"2026-04-20T00:00:00.000Z","type":"session_meta","payload":{"id":session_id,"cwd":"/tmp/project"}}),
    ];
    for (n, config) in configs.iter().enumerate() {
        let turn = format!("turn_{n}");
        let at = |second: usize| format!("2026-04-20T00:{n:02}:{second:02}.000Z");
        let mut context = config.clone();
        context["turn_id"] = json!(turn);
        let mut started = json!({"type":"task_started","turn_id":turn});
        if rooted {
            started["root_turn_id"] = context["root_turn_id"].clone();
        }
        lines.push(json!({"timestamp":at(0),"type":"event_msg","payload":started}));
        lines.push(json!({"timestamp":at(1),"type":"turn_context","payload":context}));
        lines.push(json!({"timestamp":at(2),"type":"event_msg","payload":{"type":"user_message","message":format!("prompt {n}")}}));
        lines.push(json!({"timestamp":at(3),"type":"event_msg","payload":{"type":"task_complete","turn_id":turn}}));
    }
    lines.iter().map(|line| format!("{line}\n")).collect()
}

/// `(turn_id, payload)` of every `turn_context` marker, in read order.
fn turn_contexts(evidence: &SessionEvidence) -> Vec<(String, Value)> {
    evidence
        .markers
        .iter()
        .filter(|marker| marker.kind == "turn_context")
        .map(|marker| {
            (
                marker.turn_id.clone().expect("turn id"),
                serde_json::from_str(marker.raw_payload().expect("payload")).unwrap(),
            )
        })
        .collect()
}

/// The `turn_context` Codex writes, minus the bulk that never changes.
fn base_config() -> Value {
    json!({
        "root_turn_id": "root_a",
        "cwd": "/tmp/project",
        "current_date": "2026-04-20",
        "timezone": "America/Los_Angeles",
        "approval_policy": "never",
        "sandbox_policy": {"type": "danger-full-access"},
        "permission_profile": {"network": {"enabled": true}},
        "model": "gpt-5.4",
        "collaboration_mode": {"mode": "default", "settings": {"model": "gpt-5.4", "reasoning_effort": "high"}},
        "effort": "high",
        "summary": "auto"
    })
}

fn with(key: &str, value: Value) -> Value {
    let mut config = base_config();
    config[key] = value;
    config
}

/// Codex restates its whole configuration on every turn. A record that
/// repeats the previous one apart from its `turn_id` stores nothing: the
/// session keeps one marker, at the turn it took effect, whole, and every
/// later turn's configuration is that marker.
#[test]
fn unchanged_turn_contexts_store_one_marker() {
    let rollout = configured_rollout("sess_repeat", &vec![base_config(); 5]);
    let (_dir, evidence) = synced(&rollout, "sess_repeat");
    let mut expected = base_config();
    expected["turn_id"] = json!("turn_0");
    assert_eq!(
        turn_contexts(&evidence),
        vec![("turn_0".to_string(), expected)]
    );
    // The repeats leave no fallback marker of their own.
    assert!(evidence
        .markers
        .iter()
        .all(|marker| marker.kind != "unknown"));
}

/// Any change other than the turn id is a new configuration and a new
/// marker -- model, effort, cwd, the date, a nested setting, and the root
/// turn when the turn's `task_started` does not keep it -- and returning to an
/// earlier configuration is a change too.
#[test]
fn a_changed_setting_stores_a_new_marker() {
    let configs = vec![
        base_config(),
        base_config(),
        with("model", json!("gpt-5.5")),
        with("effort", json!("low")),
        with("cwd", json!("/tmp/other")),
        with("current_date", json!("2026-04-21")),
        with("root_turn_id", json!("root_b")),
        with(
            "collaboration_mode",
            json!({"mode": "plan", "settings": {"model": "gpt-5.4", "reasoning_effort": "high"}}),
        ),
        base_config(),
        base_config(),
    ];
    let rollout = configured_rollout("sess_changes", &configs);
    let (_dir, evidence) = synced(&rollout, "sess_changes");
    let stored = turn_contexts(&evidence);
    let expected: Vec<(String, Value)> = [0usize, 2, 3, 4, 5, 6, 7, 8]
        .into_iter()
        .map(|n| {
            let mut config = configs[n].clone();
            config["turn_id"] = json!(format!("turn_{n}"));
            (format!("turn_{n}"), config)
        })
        .collect();
    assert_eq!(stored, expected);

    // Every turn's configuration is the latest marker at or before it.
    let marker_ts: Vec<i64> = evidence
        .markers
        .iter()
        .filter(|marker| marker.kind == "turn_context")
        .map(|marker| marker.ts_ms.expect("ts"))
        .collect();
    assert!(marker_ts.windows(2).all(|pair| pair[0] < pair[1]));

    // The stored markers survive a serde round trip unchanged.
    let encoded = serde_json::to_string(&evidence.markers).unwrap();
    let decoded: Vec<Marker> = serde_json::from_str(&encoded).unwrap();
    assert_eq!(decoded, evidence.markers);
}

/// Current Codex writes `root_turn_id` on `task_started` too, and that marker
/// keeps it: a delegated thread whose root moved to its next turn, with
/// nothing else changed, stores no new `turn_context`, and every turn still
/// reads its own root.
#[test]
fn a_root_kept_by_task_started_is_not_a_configuration_change() {
    let configs = vec![
        base_config(),
        with("root_turn_id", json!("root_b")),
        with("root_turn_id", json!("root_c")),
        {
            let mut config = with("root_turn_id", json!("root_c"));
            config["model"] = json!("gpt-5.5");
            config
        },
    ];
    let rollout = rollout_with_roots("sess_roots", &configs, true);
    let (_dir, evidence) = synced(&rollout, "sess_roots");
    assert_eq!(
        turn_contexts(&evidence)
            .iter()
            .map(|(turn, config)| (turn.as_str(), config["root_turn_id"].as_str().unwrap()))
            .collect::<Vec<_>>(),
        vec![("turn_0", "root_a"), ("turn_3", "root_c")]
    );
    let roots: Vec<(&str, Value)> = evidence
        .markers
        .iter()
        .filter(|marker| marker.kind == "task_started")
        .map(|marker| {
            (
                marker.turn_id.as_deref().unwrap(),
                marker.payload.clone().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        roots,
        vec![
            ("turn_0", json!({"root_turn_id": "root_a"})),
            ("turn_1", json!({"root_turn_id": "root_b"})),
            ("turn_2", json!({"root_turn_id": "root_c"})),
            ("turn_3", json!({"root_turn_id": "root_c"})),
        ]
    );
}

/// `sync`, `hydrate` and the change feed agree on which `turn_context`
/// records are stored.
#[test]
fn sync_hydrate_and_the_change_feed_store_the_same_turn_contexts() {
    let configs = vec![
        base_config(),
        base_config(),
        with("model", json!("gpt-5.5")),
        with("model", json!("gpt-5.5")),
    ];
    let rollout = configured_rollout("sess_paths", &configs);
    let (_synced_dir, synced_evidence) = synced(&rollout, "sess_paths");
    let by_sync = turn_contexts(&synced_evidence);
    assert_eq!(
        by_sync
            .iter()
            .map(|(turn, _)| turn.as_str())
            .collect::<Vec<_>>(),
        vec!["turn_0", "turn_2"]
    );

    // A store that never syncs: discovery catalogs the rollout and
    // hydration alone reads it.
    let (_dir, store) = staged(&rollout);
    store
        .discover(DiscoveryOptions::default())
        .expect("discover");
    store
        .hydrate(
            &SessionRef::id(Source::Codex, "sess_paths"),
            HydrateOptions::default(),
        )
        .expect("hydrate");
    let hydrated = store
        .session(
            &SessionRef::id(Source::Codex, "sess_paths"),
            SessionQuery::default(),
        )
        .unwrap()
        .expect("hydrated session");
    assert_eq!(turn_contexts(&hydrated), by_sync);

    let fed: Vec<(String, Value)> = store
        .changes_since(Watermark::START, ChangeQuery::default())
        .unwrap()
        .map(|change| change.unwrap())
        .filter_map(|change| match change.op {
            ChangeOp::Upsert(EvidenceRow::SessionMarker(marker))
                if marker.kind == "turn_context" =>
            {
                Some((
                    marker.turn_id.clone().unwrap(),
                    serde_json::from_str(marker.payload_json.as_deref().unwrap()).unwrap(),
                ))
            }
            _ => None,
        })
        .collect();
    assert_eq!(fed, by_sync);

    // Forgotten and hydrated back, the session holds the same markers.
    let session = SessionRef::id(Source::Codex, "sess_paths");
    store
        .forget_evidence(
            ForgetScope::Sessions(vec![session.clone()]),
            ForgetOptions::default(),
        )
        .expect("forget");
    let forgotten = store
        .session(&session, SessionQuery::default())
        .unwrap()
        .expect("still cataloged");
    assert!(turn_contexts(&forgotten).is_empty());
    store
        .hydrate(&session, HydrateOptions::default())
        .expect("hydrate again");
    let restored = store
        .session(&session, SessionQuery::default())
        .unwrap()
        .expect("restored");
    assert_eq!(turn_contexts(&restored), by_sync);
}

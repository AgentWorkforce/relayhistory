//! Public Cortex Code CLI 1.2.18 history acceptance from its native offline writer.
mod provider_support;
use ai_hist::{
    DiscoveryOptions, HydrateOptions, ProviderRoots, Role, SessionQuery, SessionRef, SessionStore,
    Source, StoreOptions, SyncOptions,
};
use std::path::{Path, PathBuf};
const ID: &str = "11111111-1111-4111-8111-111111111111";
const META: &str = include_str!("fixtures/cortex/native/.snowflake/cortex/conversations/11111111-1111-4111-8111-111111111111.json");
const HISTORY: &str =
    include_str!("fixtures/cortex/native/.snowflake/cortex/conversations/11111111-1111-4111-8111-111111111111.history.jsonl");
const FLAT_ID: &str = "22222222-2222-4222-8222-222222222222";
const FLAT_META: &str =
    include_str!("fixtures/cortex/native/.snowflake/cortex/conversations/22222222-2222-4222-8222-222222222222.json");
const FLAT_HISTORY: &str =
    include_str!("fixtures/cortex/native/.snowflake/cortex/conversations/22222222-2222-4222-8222-222222222222.history.jsonl");
const EMPTY_ID: &str = "55555555-5555-4555-8555-555555555555";
const EMPTY_META: &str = include_str!("fixtures/cortex/native/.snowflake/cortex/conversations/55555555-5555-4555-8555-555555555555.json");
const EMPTY_BEFORE: &str =
    include_str!("../../../plans/history-providers/fixtures/cortex/empty-before.jsonl");
fn stage(home: &Path) -> (PathBuf, PathBuf) {
    let dir = home.join(".snowflake/cortex/conversations");
    std::fs::create_dir_all(&dir).unwrap();
    let meta = dir.join(format!("{ID}.json"));
    let side = dir.join(format!("{ID}.history.jsonl"));
    std::fs::write(&meta, META).unwrap();
    std::fs::write(&side, HISTORY).unwrap();
    (meta, side)
}
fn open(home: &Path) -> SessionStore {
    SessionStore::open(
        StoreOptions::default()
            .db_path(home.join("cortex.db"))
            .roots(ProviderRoots::from_home(
                home.into(),
                home.join("opencode.db"),
            )),
    )
    .unwrap()
}
#[test]
fn native_writer_history_reaches_public_facade() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path());
    let evidence = provider_support::assert_history_conformance(
        home.path(),
        provider_support::HistoryCase {
            source: Source::Cortex,
            session_id: ID,
            messages: &[
                (Role::User, "Create a demo table"),
                (Role::Assistant, "I will inspect the schema."),
                (Role::Assistant, "Done."),
            ],
        },
    );
    assert_eq!(evidence.tool_calls.len(), 1);
    assert_eq!(evidence.tool_results.len(), 1);
    assert_eq!(evidence.tool_calls[0].tool_use_id, "tool-1");
    assert_eq!(
        evidence.tool_results[0].tool_use_id.as_deref(),
        Some("tool-1")
    );
    assert_eq!(evidence.prompts.len(), 1, "tool result is not human prompt");
    assert!(evidence.messages.iter().all(|m| m.usage.is_none()));
    let ids: Vec<_> = evidence
        .messages
        .iter()
        .filter_map(|m| m.message_id.as_deref())
        .collect();
    let oracle: serde_json::Value = serde_json::from_str(include_str!(
        "../../../plans/history-providers/fixtures/cortex/native-reader-oracle.json"
    ))
    .unwrap();
    let native: Vec<_> = oracle
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, native, "native restart reader order");
}
#[test]
fn native_flat_writer_reaches_public_facade() {
    let home = tempfile::tempdir().unwrap();
    let dir = home.path().join(".snowflake/cortex/conversations");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join(format!("{FLAT_ID}.json")), FLAT_META).unwrap();
    std::fs::write(dir.join(format!("{FLAT_ID}.history.jsonl")), FLAT_HISTORY).unwrap();
    let evidence = provider_support::assert_history_conformance(
        home.path(),
        provider_support::HistoryCase {
            source: Source::Cortex,
            session_id: FLAT_ID,
            messages: &[(Role::User, "Check status"), (Role::Assistant, "Checking.")],
        },
    );
    assert_eq!(evidence.tool_calls[0].tool_use_id, "flat-tool-1");
    assert_eq!(
        evidence.tool_results[0].tool_use_id.as_deref(),
        Some("flat-tool-1")
    );
}
#[test]
fn native_empty_sidecar_rewrite_retires_stale_prompt() {
    let home = tempfile::tempdir().unwrap();
    let dir = home.path().join(".snowflake/cortex/conversations");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join(format!("{EMPTY_ID}.json")), EMPTY_META).unwrap();
    let side = dir.join(format!("{EMPTY_ID}.history.jsonl"));
    std::fs::write(&side, EMPTY_BEFORE).unwrap();
    let store = open(home.path());
    let reference = SessionRef::id(Source::Cortex, EMPTY_ID);
    let sweep = || {
        store
            .sync(SyncOptions::default().sources([Source::Cortex]).force(true))
            .unwrap()
    };
    sweep();
    assert_eq!(
        store
            .session(&reference, SessionQuery::default())
            .unwrap()
            .unwrap()
            .prompts
            .len(),
        1
    );
    std::fs::write(&side, b"").unwrap();
    sweep();
    let after = store
        .session(&reference, SessionQuery::default())
        .unwrap()
        .unwrap();
    assert!(after.prompts.is_empty());
    assert!(after.messages.is_empty());
}
#[test]
fn sidecar_append_partial_tail_and_rewrite() {
    let home = tempfile::tempdir().unwrap();
    let (_, side) = stage(home.path());
    let store = open(home.path());
    store
        .discover(DiscoveryOptions::default().sources([Source::Cortex]))
        .unwrap();
    let reference = SessionRef::id(Source::Cortex, ID);
    store
        .hydrate(&reference, HydrateOptions::default())
        .unwrap();
    let prompts = || {
        store
            .session(&reference, SessionQuery::default())
            .unwrap()
            .unwrap()
            .prompts
            .len()
    };
    assert_eq!(prompts(), 1);
    let extra = r#"{"role":"user","id":"user-2","content":[{"type":"text","text":"Second turn","is_user_prompt":true}],"user_sent_time":"2026-10-21T10:00:04.000Z"}"#;
    std::fs::write(&side, format!("{HISTORY}{}", &extra[..30])).unwrap();
    assert!(store
        .hydrate(&reference, HydrateOptions::default())
        .is_err());
    assert_eq!(prompts(), 1);
    std::fs::write(&side, format!("{HISTORY}{extra}\n")).unwrap();
    store
        .hydrate(&reference, HydrateOptions::default())
        .unwrap();
    assert_eq!(prompts(), 2);
    std::fs::write(&side, HISTORY).unwrap();
    store
        .hydrate(&reference, HydrateOptions::default())
        .unwrap();
    assert_eq!(prompts(), 1);
}
#[test]
fn metadata_rename_and_missing_sidecar_are_visible() {
    let home = tempfile::tempdir().unwrap();
    let (meta, side) = stage(home.path());
    let store = open(home.path());
    let discovered = store
        .discover(DiscoveryOptions::default().sources([Source::Cortex]))
        .unwrap();
    assert_eq!(discovered.discovered, 1);
    let reference = SessionRef::id(Source::Cortex, ID);
    store
        .hydrate(&reference, HydrateOptions::default())
        .unwrap();
    let mut value: serde_json::Value = serde_json::from_str(META).unwrap();
    value["title"] = "Renamed".into();
    std::fs::write(&meta, serde_json::to_vec(&value).unwrap()).unwrap();
    store
        .hydrate(&reference, HydrateOptions::default())
        .unwrap();
    let read = store
        .session(&reference, SessionQuery::default())
        .unwrap()
        .unwrap();
    assert_eq!(read.session.title.as_deref(), Some("Renamed"));
    std::fs::remove_file(side).unwrap();
    let missing = store
        .discover(DiscoveryOptions::default().sources([Source::Cortex]))
        .unwrap();
    assert!(missing
        .diagnostics
        .iter()
        .any(|d| d.message.contains("CORTEX_MISSING_HISTORY")));
}
#[test]
fn both_native_files_must_disappear_before_local_retirement() {
    let home = tempfile::tempdir().unwrap();
    let (meta, side) = stage(home.path());
    let store = open(home.path());
    let reference = SessionRef::id(Source::Cortex, ID);
    let sweep = || {
        store
            .sync(SyncOptions::default().sources([Source::Cortex]).force(true))
            .unwrap()
    };
    sweep();
    assert!(store
        .session(&reference, SessionQuery::default())
        .unwrap()
        .is_some());
    std::fs::remove_file(&meta).unwrap();
    sweep();
    assert!(
        store
            .session(&reference, SessionQuery::default())
            .unwrap()
            .is_some(),
        "remaining sidecar is historical authority"
    );
    std::fs::remove_file(&side).unwrap();
    sweep();
    assert!(
        store
            .session(&reference, SessionQuery::default())
            .unwrap()
            .is_none(),
        "native wipe removes both files"
    );
}
#[test]
fn missing_root_preserves_prior_local_evidence() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path());
    let store = open(home.path());
    let reference = SessionRef::id(Source::Cortex, ID);
    let sweep = || {
        store
            .sync(SyncOptions::default().sources([Source::Cortex]).force(true))
            .unwrap()
    };
    sweep();
    std::fs::rename(
        home.path().join(".snowflake/cortex/conversations"),
        home.path().join("moved-conversations"),
    )
    .unwrap();
    sweep();
    assert!(
        store
            .session(&reference, SessionQuery::default())
            .unwrap()
            .is_some(),
        "missing root is not proof of deletion"
    );
}
#[test]
fn malformed_and_oversized_siblings_do_not_hide_native_session() {
    let home = tempfile::tempdir().unwrap();
    let (meta, _) = stage(home.path());
    let dir = meta.parent().unwrap();
    std::fs::write(dir.join("broken.json"), b"{unclosed").unwrap();
    let big_id = "44444444-4444-4444-8444-444444444444";
    let mut big_meta: serde_json::Value = serde_json::from_str(META).unwrap();
    big_meta["session_id"] = big_id.into();
    std::fs::write(
        dir.join(format!("{big_id}.json")),
        serde_json::to_vec(&big_meta).unwrap(),
    )
    .unwrap();
    std::fs::write(
        dir.join(format!("{big_id}.history.jsonl")),
        vec![b' '; 8 * 1024 * 1024 + 1],
    )
    .unwrap();
    let huge_meta_id = "66666666-6666-4666-8666-666666666666";
    std::fs::write(
        dir.join(format!("{huge_meta_id}.json")),
        vec![b' '; 8 * 1024 * 1024 + 1],
    )
    .unwrap();
    let store = open(home.path());
    let report = store
        .discover(DiscoveryOptions::default().sources([Source::Cortex]))
        .unwrap();
    assert!(report.discovered >= 1);
    let reference = SessionRef::id(Source::Cortex, ID);
    assert!(store
        .session(&reference, SessionQuery::default())
        .unwrap()
        .is_some());
}
#[test]
fn two_native_roots_cannot_claim_one_cortex_session_id() {
    let home = tempfile::tempdir().unwrap();
    let (meta, side) = stage(home.path());
    let second = tempfile::tempdir().unwrap();
    let second_dir = second.path().join("conversations");
    std::fs::create_dir_all(&second_dir).unwrap();
    std::fs::copy(&meta, second_dir.join(meta.file_name().unwrap())).unwrap();
    std::fs::copy(&side, second_dir.join(side.file_name().unwrap())).unwrap();
    let mut roots = serde_json::to_value(ProviderRoots::from_home(
        home.path().into(),
        home.path().join("opencode.db"),
    ))
    .unwrap();
    roots["provider_roots"]["cortex"] = serde_json::json!([meta.parent().unwrap(), second_dir]);
    let store = SessionStore::open(
        StoreOptions::default()
            .db_path(home.path().join("collision.db"))
            .roots(serde_json::from_value(roots).unwrap()),
    )
    .unwrap();
    let report = store
        .discover(DiscoveryOptions::default().sources([Source::Cortex]))
        .unwrap();
    assert!(report
        .diagnostics
        .iter()
        .any(|diagnostic| diagnostic.message.contains("CORTEX_ID_COLLISION")));
}
#[cfg(unix)]
#[test]
fn escaped_native_metadata_preserves_prior_evidence() {
    use std::os::unix::fs::symlink;
    let home = tempfile::tempdir().unwrap();
    let (meta, side) = stage(home.path());
    let store = open(home.path());
    let reference = SessionRef::id(Source::Cortex, ID);
    store
        .sync(SyncOptions::default().sources([Source::Cortex]).force(true))
        .unwrap();
    let outside = tempfile::tempdir().unwrap();
    let escaped = outside.path().join("foreign.json");
    std::fs::write(&escaped, META).unwrap();
    std::fs::remove_file(&meta).unwrap();
    std::fs::remove_file(&side).unwrap();
    symlink(&escaped, &meta).unwrap();
    let _ = store.sync(SyncOptions::default().sources([Source::Cortex]).force(true));
    assert!(store
        .session(&reference, SessionQuery::default())
        .unwrap()
        .is_some());
}
#[test]
fn last_native_tool_result_status_survives_event_order_replay() {
    let home = tempfile::tempdir().unwrap();
    let (_, side) = stage(home.path());
    let store = open(home.path());
    let reference = SessionRef::id(Source::Cortex, ID);
    store
        .discover(DiscoveryOptions::default().sources([Source::Cortex]))
        .unwrap();
    store
        .hydrate(&reference, HydrateOptions::default())
        .unwrap();
    let replacement = r#"{"role":"user","id":"tool-result-1","user_sent_time":"2026-10-21T10:00:03.000Z","content":[{"type":"tool_result","tool_result":{"tool_use_id":"tool-1","status":"failed","content":[{"type":"text","text":"permission denied"}]}}]}"#;
    std::fs::write(&side, format!("{HISTORY}{replacement}\n")).unwrap();
    store
        .hydrate(&reference, HydrateOptions::default())
        .unwrap();
    let read = store
        .session(&reference, SessionQuery::default())
        .unwrap()
        .unwrap();
    assert_eq!(read.tool_calls[0].is_error, Some(true));
    std::fs::write(&side, HISTORY).unwrap();
    store
        .hydrate(&reference, HydrateOptions::default())
        .unwrap();
    let read = store
        .session(&reference, SessionQuery::default())
        .unwrap()
        .unwrap();
    assert_eq!(read.tool_calls[0].is_error, Some(false));
}
#[cfg(all(feature = "unstable-internal", feature = "export"))]
#[test]
fn public_export_intake_then_local_wipe_preserves_remote_share() {
    use ai_hist::export::{ExportLimits, ExportSelection, ExportSnapshot};
    use ai_hist::{
        EvidenceKind, EvidenceRecord, NormalizedSourceEvidence, ObservationKey, SessionLocation,
        ShallowSession, SourceEvidenceSubmission, SourceObservation, SourceObservationBatch,
    };
    let origin = tempfile::tempdir().unwrap();
    stage(origin.path());
    let source = open(origin.path());
    source
        .sync(SyncOptions::default().sources([Source::Cortex]).force(true))
        .unwrap();
    let reference = SessionRef::id(Source::Cortex, ID);
    let original = source
        .session(&reference, SessionQuery::default())
        .unwrap()
        .unwrap();
    let kinds = vec![
        EvidenceKind::SessionEvent,
        EvidenceKind::SessionMarker,
        EvidenceKind::ToolCall,
    ];
    let mut export = ExportSnapshot::open(
        rusqlite::Connection::open(origin.path().join("cortex.db")).unwrap(),
        &ExportSelection {
            sources: vec!["cortex".into()],
            kinds: kinds.iter().map(|kind| kind.as_str().into()).collect(),
            ..Default::default()
        },
        &ExportLimits::default(),
        60_000,
        1_000,
    )
    .unwrap();
    let mut cursor = Some(export.handle().cursor);
    let mut records = Vec::new();
    while let Some(value) = cursor {
        let page = export.page(&value, 1_000).unwrap();
        cursor = page.next_cursor;
        for record in page.records {
            let mut payload = record.payload.as_object().unwrap().clone();
            payload.remove("id");
            payload.remove("location");
            records.push(EvidenceRecord {
                kind: serde_json::from_value(serde_json::json!(record.kind)).unwrap(),
                payload,
                record_id: Some(record.record_id),
                revision_id: Some(record.revision_id),
            });
        }
    }
    let destination = tempfile::tempdir().unwrap();
    let store = open(destination.path());
    let key = ObservationKey {
        source: "cortex".into(),
        session_id: ID.into(),
        location: SessionLocation::Remote,
        connector_id: "fixture-export".into(),
        connector_instance: "default".into(),
    };
    store
        .apply_source_observations(SourceObservationBatch::new(
            "fixture-export",
            "default",
            SessionLocation::Remote,
            vec![SourceObservation {
                session: ShallowSession {
                    source: "cortex".into(),
                    session_id: ID.into(),
                    source_stamp: Some("export-1".into()),
                    ..Default::default()
                },
                raw_locator: None,
            }],
        ))
        .unwrap();
    let revision = store.source_observation(&key).unwrap().revision.unwrap();
    store
        .apply_source_evidence(
            SourceEvidenceSubmission::new(
                key,
                revision,
                NormalizedSourceEvidence {
                    source_stamp: "export-1".into(),
                    source_bytes: 1,
                    covered_kinds: kinds,
                    records,
                },
            )
            .include_related(false),
        )
        .unwrap();
    assert_eq!(
        store
            .session(&reference, SessionQuery::default())
            .unwrap()
            .unwrap()
            .messages,
        original.messages
    );
    let (meta, side) = stage(destination.path());
    store
        .sync(SyncOptions::default().sources([Source::Cortex]).force(true))
        .unwrap();
    store
        .apply_source_observations(SourceObservationBatch::new(
            "fixture-export",
            "default",
            SessionLocation::Remote,
            vec![SourceObservation {
                session: ShallowSession {
                    source: "cortex".into(),
                    session_id: ID.into(),
                    source_stamp: Some("export-2".into()),
                    ..Default::default()
                },
                raw_locator: Some("remote-cortex-locator".into()),
            }],
        ))
        .unwrap();
    std::fs::remove_file(meta).unwrap();
    std::fs::remove_file(side).unwrap();
    store
        .sync(SyncOptions::default().sources([Source::Cortex]).force(true))
        .unwrap();
    assert_eq!(
        store
            .session(&reference, SessionQuery::default())
            .unwrap()
            .unwrap()
            .messages,
        original.messages
    );
    let conn = rusqlite::Connection::open(destination.path().join("cortex.db")).unwrap();
    let local: i64 = conn.query_row("SELECT count(*) FROM session_observations WHERE source='cortex' AND session_id=?1 AND location='local'", [ID], |row| row.get(0)).unwrap();
    assert_eq!(local, 0);
    let raw: Option<String> = conn
        .query_row(
            "SELECT raw_path FROM sessions WHERE source='cortex' AND session_id=?1",
            [ID],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(raw.as_deref(), Some("remote-cortex-locator"));
}

mod provider_support;
#[cfg(feature = "export")]
use ai_hist::export::{ExportLimits, ExportSelection, ExportSnapshot, SessionIdentity};
use ai_hist::{
    BlockKind, DiscoveryOptions, HydrateOptions, ProviderRoots, Role, SessionLocation,
    SessionQuery, SessionRef, SessionStore, ShallowSession, Source, SourceObservation,
    SourceObservationBatch, StoreOptions, SyncOptions,
};
#[cfg(feature = "export")]
use ai_hist::{
    EvidenceKind, EvidenceRecord, NormalizedSourceEvidence, ObservationKey,
    SourceEvidenceSubmission,
};
use provider_support::{assert_history_conformance, HistoryCase};

const SID: &str = "default:32303236313032325f31";

fn open(home: &std::path::Path) -> SessionStore {
    SessionStore::open(
        StoreOptions::default()
            .db_path(home.join("ledger.db"))
            .roots(ProviderRoots::from_home(
                home.into(),
                home.join("opencode.db"),
            )),
    )
    .unwrap()
}

fn stage(home: &std::path::Path, sql: &str) {
    let dir = home.join(".local/share/goose/sessions");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("sessions.db");
    if path.exists() {
        std::fs::remove_file(&path).unwrap();
    }
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.execute_batch(sql).unwrap();
    assert_eq!(
        conn.query_row("SELECT count(*) FROM pragma_foreign_key_check", [], |r| r
            .get::<_, i64>(
            0
        ))
        .unwrap(),
        0
    );
}

fn hydrate(store: &SessionStore) -> ai_hist::SessionEvidence {
    store
        .discover(DiscoveryOptions::default().sources([Source::Goose]))
        .unwrap();
    let reference = SessionRef::id(Source::Goose, SID);
    store
        .hydrate(&reference, HydrateOptions::default())
        .unwrap();
    store
        .session(&reference, SessionQuery::default())
        .unwrap()
        .unwrap()
}

#[test]
fn goose_native_schema17_public_history_conformance() {
    let home = tempfile::tempdir().unwrap();
    let parent = home.path().join(".local/share/goose/sessions");
    std::fs::create_dir_all(&parent).unwrap();
    let native = rusqlite::Connection::open(parent.join("sessions.db")).unwrap();
    native
        .execute_batch(include_str!("fixtures/goose/schema17.sql"))
        .unwrap();
    let violations: i64 = native
        .query_row("SELECT count(*) FROM pragma_foreign_key_check", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(
        violations, 0,
        "native fixture must have no broken foreign keys"
    );
    drop(native);
    let evidence = assert_history_conformance(
        home.path(),
        HistoryCase {
            source: Source::Goose,
            session_id: SID,
            messages: &[
                (Role::User, "Find the Goose fixture needle"),
                (Role::Assistant, "Found the Goose fixture needle."),
                (Role::User, "Continue after restart."),
                (Role::Assistant, "The historical answer is still available."),
            ],
        },
    );
    assert_eq!(evidence.tool_calls.len(), 1);
    assert_eq!(evidence.tool_calls[0].tool_use_id, "call-native-1");
    assert_eq!(evidence.tool_results.len(), 1);
    assert_eq!(
        evidence.tool_results[0].tool_use_id.as_deref(),
        Some("call-native-1")
    );
    assert!(evidence
        .requests
        .iter()
        .all(|request| request.usage.is_none()));
    let oracle: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/goose/schema17-official-export.json")).unwrap();
    assert_eq!(oracle["conversation"].as_array().unwrap().len(), 6);
    assert_eq!(oracle["id"], "20261022_1");
}

#[test]
fn goose_interleaved_native_blocks_keep_order_and_stable_ids_after_reopen() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), include_str!("fixtures/goose/schema17.sql"));
    // Derived format variant: Goose persists an ordered content_json array.
    // Put a tool between two text runs and thinking before both, while keeping
    // the native writer's message ID and tool-request identity.
    let blocks = serde_json::json!([
        {"type":"thinking","thinking":"Inspect first."},
        {"type":"text","text":"Before the tool."},
        {"type":"toolRequest","id":"call-native-1","toolCall":{"status":"success","value":{"name":"read_file"}}},
        {"type":"text","text":"After the tool."}
    ]);
    let interleaved = blocks.to_string();
    let native =
        rusqlite::Connection::open(home.path().join(".local/share/goose/sessions/sessions.db"))
            .unwrap();
    native
        .execute(
            "UPDATE messages SET content_json=?1 WHERE message_id='assistant-native-1'",
            [&interleaved],
        )
        .unwrap();
    drop(native);
    let reference = SessionRef::id(Source::Goose, SID);
    let store = open(home.path());
    let first = hydrate(&store);
    let target = first
        .messages
        .iter()
        .find(|message| message.message_id.as_deref() == Some("assistant-native-1"))
        .unwrap();
    let kinds = target
        .blocks
        .iter()
        .map(|block| block.kind)
        .collect::<Vec<_>>();
    assert_eq!(
        kinds,
        [
            BlockKind::Thinking,
            BlockKind::Text,
            BlockKind::ToolUse,
            BlockKind::Text
        ]
    );
    let ids = target
        .blocks
        .iter()
        .map(|block| block.event_uid.clone())
        .collect::<Vec<_>>();
    assert_eq!(ids.len(), 4);
    drop(store);
    let reopened = open(home.path());
    reopened
        .discover(DiscoveryOptions::default().sources([Source::Goose]))
        .unwrap();
    reopened
        .hydrate(&reference, HydrateOptions::default())
        .unwrap();
    let second = reopened
        .session(&reference, SessionQuery::default())
        .unwrap()
        .unwrap();
    let target = second
        .messages
        .iter()
        .find(|message| message.message_id.as_deref() == Some("assistant-native-1"))
        .unwrap();
    assert_eq!(
        target
            .blocks
            .iter()
            .map(|block| block.kind)
            .collect::<Vec<_>>(),
        kinds
    );
    assert_eq!(
        target
            .blocks
            .iter()
            .map(|block| block.event_uid.clone())
            .collect::<Vec<_>>(),
        ids
    );
    drop(reopened);
    // The captured upstream replace_conversation changes SQLite row IDs while
    // keeping this message ID. Reapply only the derived interleaved content.
    stage(home.path(), include_str!("fixtures/goose/rewritten.sql"));
    let native =
        rusqlite::Connection::open(home.path().join(".local/share/goose/sessions/sessions.db"))
            .unwrap();
    native
        .execute(
            "UPDATE messages SET content_json=?1 WHERE message_id='assistant-native-1'",
            [&interleaved],
        )
        .unwrap();
    drop(native);
    let rewritten = open(home.path());
    let after = hydrate(&rewritten);
    let target = after
        .messages
        .iter()
        .find(|message| message.message_id.as_deref() == Some("assistant-native-1"))
        .unwrap();
    assert_eq!(
        target
            .blocks
            .iter()
            .map(|block| block.kind)
            .collect::<Vec<_>>(),
        kinds
    );
    assert_eq!(
        target
            .blocks
            .iter()
            .map(|block| block.event_uid.clone())
            .collect::<Vec<_>>(),
        ids
    );
    let retained_tools = after.tool_calls.clone();
    let reordered = serde_json::json!([
        {"type":"thinking","thinking":"Inspect first."},
        {"type":"toolRequest","id":"call-native-1","toolCall":{"status":"success","value":{"name":"read_file"}}},
        {"type":"text","text":"After the tool."}
    ]);
    let native =
        rusqlite::Connection::open(home.path().join(".local/share/goose/sessions/sessions.db"))
            .unwrap();
    native
        .execute(
            "UPDATE messages SET content_json=?1 WHERE message_id='assistant-native-1'",
            [reordered.to_string()],
        )
        .unwrap();
    drop(native);
    let changed = hydrate(&rewritten);
    let target = changed
        .messages
        .iter()
        .find(|message| message.message_id.as_deref() == Some("assistant-native-1"))
        .unwrap();
    assert_eq!(
        target
            .blocks
            .iter()
            .map(|block| block.kind)
            .collect::<Vec<_>>(),
        [BlockKind::Thinking, BlockKind::ToolUse, BlockKind::Text]
    );
    assert_eq!(target.blocks[0].text.as_deref(), Some("Inspect first."));
    assert_eq!(
        target.blocks[1].tool_use_id.as_deref(),
        Some("call-native-1")
    );
    assert_eq!(target.blocks[2].text.as_deref(), Some("After the tool."));
    assert_eq!(changed.tool_calls, retained_tools);
}

#[test]
fn goose_multiblock_user_prompt_survives_reopen_and_retires_on_rewrite() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), include_str!("fixtures/goose/schema17.sql"));
    let native_path = home.path().join(".local/share/goose/sessions/sessions.db");
    let first = serde_json::json!([
        {"type":"text","text":"First part of prompt"},
        {"type":"image","mimeType":"image/png"},
        {"type":"text","text":"Second part of prompt"}
    ]);
    let write_user = |content: &serde_json::Value| {
        let native = rusqlite::Connection::open(&native_path).unwrap();
        native
            .execute(
                "UPDATE messages SET content_json=?1 WHERE message_id='user-native-1'",
                [content.to_string()],
            )
            .unwrap();
    };
    let prompts =
        || {
            let conn = rusqlite::Connection::open(home.path().join("ledger.db")).unwrap();
            let mut stmt = conn.prepare(
            "SELECT prompt FROM history WHERE source='goose' AND session_id=?1 ORDER BY prompt",
        ).unwrap();
            stmt.query_map([SID], |row| row.get::<_, String>(0))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap()
        };
    write_user(&first);
    let store = open(home.path());
    let evidence = hydrate(&store);
    assert_eq!(
        evidence
            .messages
            .iter()
            .find(|message| message.message_id.as_deref() == Some("user-native-1"))
            .unwrap()
            .blocks
            .iter()
            .map(|block| block.text.as_deref())
            .collect::<Vec<_>>(),
        [Some("First part of prompt"), Some("Second part of prompt")]
    );
    assert!(prompts().contains(&"First part of prompt\nSecond part of prompt".into()));
    #[cfg(feature = "unstable-internal")]
    {
        let conn = rusqlite::Connection::open(home.path().join("ledger.db")).unwrap();
        let hits = ai_hist::search(
            &conn,
            &["Second".into()],
            false,
            &ai_hist::QueryFilter {
                source: Some("goose".into()),
                ..Default::default()
            },
        )
        .unwrap();
        assert!(hits
            .iter()
            .any(|hit| hit.prompt == "First part of prompt\nSecond part of prompt"));
    }
    drop(store);
    let reopened = open(home.path());
    reopened
        .discover(DiscoveryOptions::default().sources([Source::Goose]))
        .unwrap();
    reopened
        .hydrate(
            &SessionRef::id(Source::Goose, SID),
            HydrateOptions::default(),
        )
        .unwrap();
    assert!(prompts().contains(&"First part of prompt\nSecond part of prompt".into()));
    drop(reopened);
    stage(home.path(), include_str!("fixtures/goose/rewritten.sql"));
    write_user(&first);
    let rewritten = open(home.path());
    hydrate(&rewritten);
    assert!(prompts().contains(&"First part of prompt\nSecond part of prompt".into()));
    let removed = serde_json::json!([{"type":"text","text":"First part of prompt"}]);
    write_user(&removed);
    hydrate(&rewritten);
    let current = prompts();
    assert!(current.contains(&"First part of prompt".into()));
    assert!(!current.contains(&"First part of prompt\nSecond part of prompt".into()));
    let many = serde_json::Value::Array(
        (0..12)
            .map(|index| serde_json::json!({"type":"text","text":format!("part-{index}")}))
            .collect(),
    );
    write_user(&many);
    hydrate(&rewritten);
    assert!(prompts().contains(
        &(0..12)
            .map(|index| format!("part-{index}"))
            .collect::<Vec<_>>()
            .join("\n")
    ));
}

#[cfg(feature = "export")]
#[test]
fn goose_remote_opaque_event_uid_does_not_abort_local_prompt_reconciliation() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), include_str!("fixtures/goose/schema17.sql"));
    let native =
        rusqlite::Connection::open(home.path().join(".local/share/goose/sessions/sessions.db"))
            .unwrap();
    let ts: i64 = native
        .query_row(
            "SELECT created_timestamp * 1000 FROM messages WHERE message_id='user-native-1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    drop(native);
    let store = open(home.path());
    let remote_sid = "unrelated-remote-goose";
    store
        .apply_source_observations(SourceObservationBatch::new(
            "opaque-connector",
            "fixture",
            SessionLocation::Remote,
            vec![SourceObservation {
                session: ShallowSession {
                    source: "goose".into(),
                    session_id: remote_sid.into(),
                    source_stamp: Some("opaque-stamp".into()),
                    ..Default::default()
                },
                raw_locator: None,
            }],
        ))
        .unwrap();
    let key = ObservationKey {
        source: "goose".into(),
        session_id: remote_sid.into(),
        location: SessionLocation::Remote,
        connector_id: "opaque-connector".into(),
        connector_instance: "fixture".into(),
    };
    let revision = store.source_observation(&key).unwrap().revision.unwrap();
    let payload = serde_json::json!({
        "source":"goose", "session_id":remote_sid, "event_uid":"connector:opaque",
        "role":"user", "kind":"text", "ts_ms":ts, "text":"Unrelated remote prompt"
    });
    let null_text = serde_json::json!({
        "source":"goose", "session_id":remote_sid, "event_uid":"connector:null-text",
        "role":"user", "kind":"text", "ts_ms":ts
    });
    store
        .apply_source_evidence(
            SourceEvidenceSubmission::new(
                key,
                revision,
                NormalizedSourceEvidence {
                    source_stamp: "opaque-stamp".into(),
                    source_bytes: 0,
                    covered_kinds: vec![EvidenceKind::SessionEvent],
                    records: vec![
                        EvidenceRecord {
                            kind: EvidenceKind::SessionEvent,
                            payload: payload.as_object().unwrap().clone(),
                            record_id: Some("remote:opaque".into()),
                            revision_id: Some("remote:1".into()),
                        },
                        EvidenceRecord {
                            kind: EvidenceKind::SessionEvent,
                            payload: null_text.as_object().unwrap().clone(),
                            record_id: Some("remote:null-text".into()),
                            revision_id: Some("remote:1".into()),
                        },
                    ],
                },
            )
            .include_related(false),
        )
        .unwrap();
    let native = hydrate(&store);
    assert!(native.messages.iter().any(|message| message
        .blocks
        .iter()
        .any(|block| block.text.as_deref() == Some("Find the Goose fixture needle"))));
    let remote = store
        .session(
            &SessionRef::id(Source::Goose, remote_sid),
            SessionQuery::default(),
        )
        .unwrap()
        .unwrap();
    assert!(remote.messages.iter().any(|message| message
        .blocks
        .iter()
        .any(|block| block.text.as_deref() == Some("Unrelated remote prompt"))));
}

#[test]
fn goose_native_replace_reorders_stable_ids_and_retires_removed_result() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), include_str!("fixtures/goose/schema17.sql"));
    let store = open(home.path());
    let before = hydrate(&store);
    assert_eq!(before.tool_results.len(), 1);
    // Native replace_conversation retains message IDs but deletes and reinserts
    // SQLite rows. Align its recorded seconds with the original writer to
    // exercise same-millisecond order repair independently of clock drift.
    stage(home.path(), include_str!("fixtures/goose/rewritten.sql"));
    let native =
        rusqlite::Connection::open(home.path().join(".local/share/goose/sessions/sessions.db"))
            .unwrap();
    native.execute("UPDATE messages SET created_timestamp=1792642821 WHERE message_id IN ('user-native-1','assistant-native-1','assistant-native-2')", []).unwrap();
    native.execute("UPDATE messages SET created_timestamp=1792642824 WHERE message_id IN ('user-native-2','assistant-native-3')", []).unwrap();
    drop(native);
    let after = hydrate(&store);
    assert_eq!(after.tool_calls.len(), 1);
    assert_eq!(after.tool_calls[0].is_error, None);
    assert!(after.tool_results.is_empty());
    let ids: Vec<_> = after
        .messages
        .iter()
        .map(|message| message.message_id.as_deref().unwrap())
        .collect();
    assert_eq!(
        ids,
        [
            "user-native-1",
            "assistant-native-2",
            "assistant-native-1",
            "user-native-2",
            "assistant-native-3"
        ]
    );
    let oracle: serde_json::Value = serde_json::from_str(include_str!(
        "fixtures/goose/rewritten-official-export.json"
    ))
    .unwrap();
    let official_ids: Vec<_> = oracle["conversation"]
        .as_array()
        .unwrap()
        .iter()
        .map(|message| message["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, official_ids);
    assert!(after
        .messages
        .iter()
        .flat_map(|message| &message.blocks)
        .any(|block| block.text.as_deref() == Some("The revised historical answer is available.")));
    drop(store);
    let reopened = open(home.path());
    let again = reopened
        .session(&SessionRef::id(Source::Goose, SID), SessionQuery::default())
        .unwrap()
        .unwrap();
    assert_eq!(after.messages, again.messages);
    assert_eq!(after.tool_calls, again.tool_calls);
    assert_eq!(after.tool_results, again.tool_results);
}

#[test]
fn goose_rewrite_cannot_replace_remote_owned_evidence() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), include_str!("fixtures/goose/schema17.sql"));
    let store = open(home.path());
    let prior = hydrate(&store);
    let ledger = rusqlite::Connection::open(home.path().join("ledger.db")).unwrap();
    ledger
        .execute(
            "UPDATE session_events SET location='remote' WHERE source='goose' AND session_id=?1 AND message_id='assistant-native-2'",
            [SID],
        )
        .unwrap();
    drop(ledger);
    stage(home.path(), include_str!("fixtures/goose/rewritten.sql"));
    store
        .discover(DiscoveryOptions::default().sources([Source::Goose]))
        .unwrap();
    let reference = SessionRef::id(Source::Goose, SID);
    let error = store
        .hydrate(&reference, HydrateOptions::default())
        .unwrap_err();
    assert!(error.to_string().contains("SHARED"), "{error}");
    let after = store
        .session(&reference, SessionQuery::default())
        .unwrap()
        .unwrap();
    assert_eq!(after.messages, prior.messages);
    assert_eq!(after.tool_calls, prior.tool_calls);
    assert_eq!(after.tool_results, prior.tool_results);
}

#[test]
fn goose_native_deletion_retires_local_observation_and_keeps_remote_catalog() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), include_str!("fixtures/goose/schema17.sql"));
    let store = open(home.path());
    hydrate(&store);
    store
        .apply_source_observations(SourceObservationBatch::new(
            "goose-remote",
            "fixture",
            SessionLocation::Remote,
            vec![SourceObservation {
                session: ShallowSession {
                    source: "goose".into(),
                    session_id: SID.into(),
                    title: Some("Remote Goose title".into()),
                    first_prompt: Some("Remote Goose prompt".into()),
                    source_stamp: Some("remote-stamp".into()),
                    ..Default::default()
                },
                raw_locator: Some("remote-goose-locator".into()),
            }],
        ))
        .unwrap();
    std::fs::remove_file(home.path().join(".local/share/goose/sessions/sessions.db")).unwrap();
    store.sync(SyncOptions::default().force(true)).unwrap();
    let conn = rusqlite::Connection::open(home.path().join("ledger.db")).unwrap();
    let local: i64 = conn
        .query_row("SELECT count(*) FROM session_observations WHERE source='goose' AND session_id=?1 AND location='local'", [SID], |r| r.get(0))
        .unwrap();
    assert_eq!(local, 0);
    let remote: i64 = conn
        .query_row("SELECT count(*) FROM session_observations WHERE source='goose' AND session_id=?1 AND location='remote'", [SID], |r| r.get(0))
        .unwrap();
    assert_eq!(remote, 1);
    let path: Option<String> = conn
        .query_row(
            "SELECT raw_path FROM sessions WHERE source='goose' AND session_id=?1",
            [SID],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(path.as_deref(), Some("remote-goose-locator"));
    let remaining = store
        .session(&SessionRef::id(Source::Goose, SID), SessionQuery::default())
        .unwrap()
        .unwrap();
    assert!(remaining.messages.is_empty());
    assert!(remaining.tool_calls.is_empty());
    assert!(remaining.tool_results.is_empty());
}

#[test]
fn goose_success_envelope_with_error_result_is_errored() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), include_str!("fixtures/goose/schema17.sql"));
    let native =
        rusqlite::Connection::open(home.path().join(".local/share/goose/sessions/sessions.db"))
            .unwrap();
    native.execute("UPDATE messages SET content_json=json_set(content_json,'$[0].toolResult.value.isError',json('true')) WHERE message_id='tool-native-1'", []).unwrap();
    drop(native);
    let store = open(home.path());
    let evidence = hydrate(&store);
    assert_eq!(evidence.tool_calls[0].is_error, Some(true));
    assert_eq!(
        evidence.tool_results[0].result_status.as_deref(),
        Some("errored")
    );
}

#[test]
fn goose_malformed_native_rewrite_keeps_prior_public_evidence() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), include_str!("fixtures/goose/schema17.sql"));
    let store = open(home.path());
    let prior = hydrate(&store);
    let native =
        rusqlite::Connection::open(home.path().join(".local/share/goose/sessions/sessions.db"))
            .unwrap();
    native
        .execute(
            "UPDATE messages SET content_json='{' WHERE message_id='assistant-native-2'",
            [],
        )
        .unwrap();
    drop(native);
    store
        .discover(DiscoveryOptions::default().sources([Source::Goose]))
        .unwrap();
    let reference = SessionRef::id(Source::Goose, SID);
    assert!(store
        .hydrate(&reference, HydrateOptions::default())
        .is_err());
    let after = store
        .session(&reference, SessionQuery::default())
        .unwrap()
        .unwrap();
    assert_eq!(after.messages, prior.messages);
    assert_eq!(after.tool_calls, prior.tool_calls);
    assert_eq!(after.tool_results, prior.tool_results);
}

#[cfg(feature = "export")]
#[test]
fn goose_native_export_remote_intake_retains_evidence_on_order_conflict() {
    let origin = tempfile::tempdir().unwrap();
    stage(origin.path(), include_str!("fixtures/goose/schema17.sql"));
    let original_store = open(origin.path());
    let original = hydrate(&original_store);
    let kinds = vec![
        EvidenceKind::History,
        EvidenceKind::SessionEvent,
        EvidenceKind::ToolCall,
        EvidenceKind::SessionMarker,
    ];
    let mut snapshot = ExportSnapshot::open(
        rusqlite::Connection::open(origin.path().join("ledger.db")).unwrap(),
        &ExportSelection {
            sessions: vec![SessionIdentity {
                source: "goose".into(),
                session_id: SID.into(),
            }],
            kinds: kinds.iter().map(|kind| kind.as_str().into()).collect(),
            ..Default::default()
        },
        &ExportLimits {
            max_batch_records: 1,
            ..Default::default()
        },
        60_000,
        1_000,
    )
    .unwrap();
    let mut cursor = Some(snapshot.handle().cursor);
    let mut records = Vec::new();
    while let Some(value) = cursor {
        let page = snapshot.page(&value, 1_000).unwrap();
        cursor = page.next_cursor;
        records.extend(page.records);
    }
    assert!(!records.is_empty());
    let records: Vec<EvidenceRecord> = records
        .into_iter()
        .map(|record| {
            let mut payload = record.payload.as_object().unwrap().clone();
            payload.remove("id");
            payload.remove("location");
            EvidenceRecord {
                kind: serde_json::from_value(serde_json::json!(record.kind)).unwrap(),
                payload,
                record_id: Some(record.record_id),
                revision_id: Some(record.revision_id),
            }
        })
        .collect();
    let receiver_home = tempfile::tempdir().unwrap();
    let receiver = open(receiver_home.path());
    receiver
        .apply_source_observations(SourceObservationBatch::new(
            "goose-transport",
            "fixture",
            SessionLocation::Remote,
            vec![SourceObservation {
                session: ShallowSession {
                    source: "goose".into(),
                    session_id: SID.into(),
                    source_stamp: Some("goose-native-archive".into()),
                    ..Default::default()
                },
                raw_locator: None,
            }],
        ))
        .unwrap();
    let key = ObservationKey {
        source: "goose".into(),
        session_id: SID.into(),
        location: SessionLocation::Remote,
        connector_id: "goose-transport".into(),
        connector_instance: "fixture".into(),
    };
    let revision = receiver.source_observation(&key).unwrap().revision.unwrap();
    let evidence = NormalizedSourceEvidence {
        source_stamp: "goose-native-archive".into(),
        source_bytes: 0,
        covered_kinds: kinds,
        records,
    };
    receiver
        .apply_source_evidence(
            SourceEvidenceSubmission::new(key.clone(), revision, evidence.clone())
                .include_related(false),
        )
        .unwrap();
    let reference = SessionRef::id(Source::Goose, SID);
    let remote = receiver
        .session(&reference, SessionQuery::default())
        .unwrap()
        .unwrap();
    // Shared source intake currently stores an identity set rather than a
    // durable native ordinal: https://github.com/AgentWorkforce/relayhistory/issues/532.
    // Verify every imported message while leaving native order to local reads.
    let mut imported_blocks: Vec<_> = remote
        .messages
        .iter()
        .flat_map(|message| {
            message.blocks.iter().map(|block| {
                (
                    message.message_id.clone(),
                    serde_json::to_string(block).unwrap(),
                )
            })
        })
        .collect();
    let mut native_blocks: Vec<_> = original
        .messages
        .iter()
        .flat_map(|message| {
            message.blocks.iter().map(|block| {
                (
                    message.message_id.clone(),
                    serde_json::to_string(block).unwrap(),
                )
            })
        })
        .collect();
    imported_blocks.sort();
    native_blocks.sort();
    assert_eq!(imported_blocks, native_blocks);
    assert_eq!(remote.messages.len(), original.messages.len());
    assert_eq!(remote.tool_calls, original.tool_calls);
    assert_eq!(remote.tool_results, original.tool_results);
    let revision = receiver.source_observation(&key).unwrap().revision.unwrap();
    let repeated = receiver
        .apply_source_evidence(
            SourceEvidenceSubmission::new(key, revision, evidence).include_related(false),
        )
        .unwrap();
    assert_eq!(repeated.status, "unchanged");
    assert_eq!(
        receiver
            .session(&reference, SessionQuery::default())
            .unwrap()
            .unwrap()
            .messages,
        remote.messages
    );
    drop(receiver);
    let receiver = open(receiver_home.path());
    stage(
        receiver_home.path(),
        include_str!("fixtures/goose/schema17.sql"),
    );
    receiver
        .discover(DiscoveryOptions::default().sources([Source::Goose]))
        .unwrap();
    let failure = receiver
        .hydrate(&reference, HydrateOptions::default())
        .unwrap_err();
    assert!(format!("{failure:#}").contains("FILE_HISTORY_SHARED_ORDER_CONFLICT"));
    let retained = receiver
        .session(&reference, SessionQuery::default())
        .unwrap()
        .unwrap();
    assert_eq!(retained.messages, remote.messages);
    assert_eq!(retained.tool_calls, remote.tool_calls);
    assert_eq!(retained.tool_results, remote.tool_results);
}

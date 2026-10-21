//! Junie CLI 3110.7 native writer capture and post-restart official reader.
mod provider_support;
use ai_hist::{
    CatalogQuery, DiscoveryOptions, HydrateOptions, ProviderRoots, Role, SessionQuery, SessionRef,
    SessionStore, Source, StoreOptions,
};
use provider_support::{assert_history_conformance, HistoryCase};
use std::path::{Path, PathBuf};

const SID: &str = "session-261021-120000-demo";

fn stage(home: &Path) -> PathBuf {
    fn copy(from: &Path, to: &Path) {
        std::fs::create_dir_all(to).unwrap();
        for entry in std::fs::read_dir(from).unwrap() {
            let entry = entry.unwrap();
            let target = to.join(entry.file_name());
            if entry.path().is_dir() {
                copy(&entry.path(), &target);
            } else {
                std::fs::copy(entry.path(), target).unwrap();
            }
        }
    }
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/junie/native");
    copy(&fixture, home);
    home.join(".junie/sessions").join(SID).join("events.jsonl")
}

fn open(home: &Path) -> SessionStore {
    SessionStore::open(
        StoreOptions::default()
            .db_path(home.join("junie.db"))
            .roots(ProviderRoots::from_home(
                home.into(),
                home.join("opencode.db"),
            )),
    )
    .unwrap()
}

fn evidence(store: &SessionStore) -> ai_hist::SessionEvidence {
    store
        .session(&SessionRef::id(Source::Junie, SID), SessionQuery::default())
        .unwrap()
        .unwrap()
}

#[test]
fn junie_public_history_conformance() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path());
    let evidence = assert_history_conformance(
        home.path(),
        HistoryCase {
            source: Source::Junie,
            session_id: SID,
            messages: &[
                (Role::User, "Hello from synthetic Junie"),
                (Role::Assistant, "The synthetic file is blue."),
                (Role::User, "Continue after restart"),
                (Role::Assistant, "The child confirmed blue."),
            ],
        },
    );
    assert_eq!(evidence.tool_calls.len(), 1);
    assert_eq!(evidence.tool_calls[0].tool_use_id, "step-tool-1");
    assert_eq!(evidence.tool_results.len(), 1);
    assert!(evidence
        .messages
        .iter()
        .flat_map(|m| &m.blocks)
        .any(|b| b.text.as_deref() == Some("blue")
            && b.tool_use_id.as_deref() == Some("step-tool-1")));
    assert!(evidence
        .requests
        .iter()
        .all(|request| request.usage.is_none()));
    assert_eq!(evidence.markers.len(), 8);
}

#[test]
fn append_rewrite_partial_tail_and_index_metadata_are_truthful() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path());
    let store = open(home.path());
    store
        .discover(DiscoveryOptions::default().sources([Source::Junie]))
        .unwrap();
    store
        .hydrate(
            &SessionRef::id(Source::Junie, SID),
            HydrateOptions::default(),
        )
        .unwrap();
    store
        .hydrate(
            &SessionRef::id(Source::Junie, SID),
            HydrateOptions::default(),
        )
        .unwrap();
    let before = evidence(&store).messages;
    let original = std::fs::read_to_string(&path).unwrap();
    std::fs::write(&path, format!("{original}{{\"kind\":")).unwrap();
    store
        .hydrate(
            &SessionRef::id(Source::Junie, SID),
            HydrateOptions::default(),
        )
        .unwrap();
    assert_eq!(evidence(&store).messages, before);
    std::fs::write(
        &path,
        original.replace("The child confirmed blue.", "A rewritten native result."),
    )
    .unwrap();
    store
        .hydrate(
            &SessionRef::id(Source::Junie, SID),
            HydrateOptions::default(),
        )
        .unwrap();
    assert!(evidence(&store)
        .messages
        .iter()
        .flat_map(|m| &m.blocks)
        .any(|b| b.text.as_deref() == Some("A rewritten native result.")));
    let index = home.path().join(".junie/sessions/index.jsonl");
    let text = std::fs::read_to_string(&index)
        .unwrap()
        .replace("Synthetic Junie capture", "Renamed native title");
    std::fs::write(index, text).unwrap();
    store
        .discover(DiscoveryOptions::default().sources([Source::Junie]))
        .unwrap();
    store
        .hydrate(
            &SessionRef::id(Source::Junie, SID),
            HydrateOptions::default(),
        )
        .unwrap();
    let mut query = CatalogQuery::default();
    query.sources = Some(vec![Source::Junie]);
    let row = store.sessions(query).next().unwrap().unwrap();
    assert_eq!(row.title.as_deref(), Some("Renamed native title"));
}

#[test]
fn invalid_native_row_preserves_prior_evidence() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path());
    let store = open(home.path());
    store
        .discover(DiscoveryOptions::default().sources([Source::Junie]))
        .unwrap();
    store
        .hydrate(
            &SessionRef::id(Source::Junie, SID),
            HydrateOptions::default(),
        )
        .unwrap();
    let before = evidence(&store).messages;
    let original = std::fs::read_to_string(&path).unwrap();
    std::fs::write(
        &path,
        original.replace("\"timestampMs\":", "\"invalidTimestamp\":"),
    )
    .unwrap();
    assert!(store
        .hydrate(
            &SessionRef::id(Source::Junie, SID),
            HydrateOptions::default()
        )
        .is_err());
    assert_eq!(evidence(&store).messages, before);
}

#[test]
fn terminal_native_tool_status_without_output_keeps_observed_success() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path());
    let rows = std::fs::read_to_string(&path)
        .unwrap()
        .replace(",\"output\":\"blue\"", "");
    std::fs::write(&path, rows).unwrap();
    let store = open(home.path());
    store
        .discover(DiscoveryOptions::default().sources([Source::Junie]))
        .unwrap();
    store
        .hydrate(
            &SessionRef::id(Source::Junie, SID),
            HydrateOptions::default(),
        )
        .unwrap();
    let result = evidence(&store);
    assert_eq!(result.tool_calls[0].is_error, Some(false));
    assert!(result.tool_results.is_empty());
}

#[test]
fn optional_index_partial_tail_keeps_native_journal_readable_and_oversize_is_diagnostic() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path());
    let index = home.path().join(".junie/sessions/index.jsonl");
    let original = std::fs::read_to_string(&index).unwrap();
    std::fs::write(&index, format!("{original}{{\"sessionId\":")).unwrap();
    let store = open(home.path());
    store
        .discover(DiscoveryOptions::default().sources([Source::Junie]))
        .unwrap();
    store
        .hydrate(
            &SessionRef::id(Source::Junie, SID),
            HydrateOptions::default(),
        )
        .unwrap();
    assert_eq!(evidence(&store).tool_calls[0].tool_use_id, "step-tool-1");
    std::fs::write(&index, vec![b' '; 8 * 1024 * 1024 + 1]).unwrap();
    assert!(store
        .hydrate(
            &SessionRef::id(Source::Junie, SID),
            HydrateOptions::default()
        )
        .is_err());
    assert_eq!(evidence(&store).tool_calls[0].tool_use_id, "step-tool-1");
}

#[test]
fn discovery_limit_selects_newest_journal_after_enumerating_all_directories() {
    let home = tempfile::tempdir().unwrap();
    let original = stage(home.path());
    let second = original
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("session-zz-newer");
    std::fs::create_dir_all(&second).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(20));
    std::fs::copy(&original, second.join("events.jsonl")).unwrap();
    let store = open(home.path());
    store
        .discover(
            DiscoveryOptions::default()
                .sources([Source::Junie])
                .limit(1),
        )
        .unwrap();
    let mut query = CatalogQuery::default();
    query.sources = Some(vec![Source::Junie]);
    let rows = store
        .sessions(query)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].session_id, "session-zz-newer");
}

#[test]
fn explicit_roots_isolate_equal_native_ids_and_empty_default_is_safe() {
    let home = tempfile::tempdir().unwrap();
    let empty = open(home.path());
    assert_eq!(
        empty
            .discover(DiscoveryOptions::default().sources([Source::Junie]))
            .unwrap()
            .discovered,
        0
    );
    let native = stage(home.path());
    let root_a = home.path().join("alternate-a/sessions");
    let root_b = home.path().join("alternate-b/sessions");
    for root in [&root_a, &root_b] {
        let target = root.join(SID).join("events.jsonl");
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        std::fs::copy(&native, target).unwrap();
        assert!(
            !root.join("index.jsonl").exists(),
            "optional metadata is absent"
        );
    }
    let mut roots = serde_json::to_value(ProviderRoots::from_home(
        home.path().into(),
        home.path().join("opencode.db"),
    ))
    .unwrap();
    roots["provider_roots"]["junie"] = serde_json::json!([root_a, root_b]);
    roots["provider_default_roots"]
        .as_object_mut()
        .unwrap()
        .remove("junie");
    let roots: ProviderRoots = serde_json::from_value(roots).unwrap();
    let store = SessionStore::open(
        StoreOptions::default()
            .db_path(home.path().join("isolated.db"))
            .roots(roots),
    )
    .unwrap();
    assert_eq!(
        store
            .discover(DiscoveryOptions::default().sources([Source::Junie]))
            .unwrap()
            .discovered,
        2
    );
    let mut query = CatalogQuery::default();
    query.sources = Some(vec![Source::Junie]);
    let ids = store
        .sessions(query)
        .map(|row| row.unwrap().session_id)
        .collect::<Vec<_>>();
    assert_eq!(ids.len(), 2);
    assert_ne!(ids[0], ids[1]);
    assert!(ids
        .iter()
        .all(|id| id.starts_with("root:") && id.ends_with(SID)));
    for id in ids {
        store
            .hydrate(
                &SessionRef::id(Source::Junie, id.clone()),
                HydrateOptions::default(),
            )
            .unwrap();
        assert!(!store
            .session(&SessionRef::id(Source::Junie, id), SessionQuery::default())
            .unwrap()
            .unwrap()
            .messages
            .is_empty());
    }
}

#[test]
fn remote_evidence_accepts_matching_local_share_but_rejects_conflicting_rewrite() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path());
    let store = open(home.path());
    let reference = SessionRef::id(Source::Junie, SID);
    store
        .discover(DiscoveryOptions::default().sources([Source::Junie]))
        .unwrap();
    store
        .hydrate(&reference, HydrateOptions::default())
        .unwrap();
    let conn = ai_hist::open_db(&home.path().join("junie.db")).unwrap();
    for table in ["session_events", "session_markers", "tool_calls"] {
        conn.execute(
            &format!("UPDATE {table} SET location='remote' WHERE source='junie' AND session_id=?1"),
            [SID],
        )
        .unwrap();
    }
    let before = evidence(&store);
    use std::io::Write;
    std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(b"\n")
        .unwrap();
    store
        .hydrate(&reference, HydrateOptions::default())
        .unwrap();
    assert_eq!(evidence(&store).messages, before.messages);
    for table in ["session_events", "session_markers", "tool_calls"] {
        let count: i64 = conn.query_row(&format!("SELECT count(*) FROM {table} WHERE source='junie' AND session_id=?1 AND location!='both'"), [SID], |r| r.get(0)).unwrap();
        assert_eq!(count, 0, "{table} did not gain a local share");
    }
    let original = std::fs::read_to_string(&path).unwrap();
    std::fs::write(
        &path,
        original.replace("The child confirmed blue.", "A conflicting local rewrite."),
    )
    .unwrap();
    assert!(store
        .hydrate(&reference, HydrateOptions::default())
        .is_err());
    assert_eq!(evidence(&store).messages, before.messages);
}

#[test]
fn exported_junie_evidence_roundtrips_through_public_intake_then_local_hydrate() {
    use ai_hist::export::{ExportLimits, ExportSelection, ExportSnapshot};
    use ai_hist::{
        EvidenceKind, EvidenceRecord, NormalizedSourceEvidence, ObservationKey, SessionLocation,
        ShallowSession, SourceEvidenceSubmission, SourceObservation, SourceObservationBatch,
    };
    let origin = tempfile::tempdir().unwrap();
    let origin_path = stage(origin.path());
    // Extend the pinned native row shape only for this ordering regression:
    // a same-millisecond bucket crossing ordinal 9 must survive public transport.
    let base = std::fs::read_to_string(&origin_path).unwrap();
    let mut added = String::new();
    let last: serde_json::Value = serde_json::from_str(base.lines().last().unwrap()).unwrap();
    for n in 8..14 {
        let mut row = last.clone();
        row["event"]["agentEvent"]["stepId"] = format!("same-ms-step-{n}").into();
        row["event"]["agentEvent"]["result"] = format!("Same-ms result {n}").into();
        added.push_str(&serde_json::to_string(&row).unwrap());
        added.push('\n');
    }
    std::fs::write(&origin_path, format!("{base}{added}")).unwrap();
    let source = open(origin.path());
    source
        .discover(DiscoveryOptions::default().sources([Source::Junie]))
        .unwrap();
    source
        .hydrate(
            &SessionRef::id(Source::Junie, SID),
            HydrateOptions::default(),
        )
        .unwrap();
    let kinds = vec![
        EvidenceKind::SessionEvent,
        EvidenceKind::SessionMarker,
        EvidenceKind::ToolCall,
    ];
    let mut snapshot = ExportSnapshot::open(
        rusqlite::Connection::open(origin.path().join("junie.db")).unwrap(),
        &ExportSelection {
            sources: vec!["junie".into()],
            kinds: kinds.iter().map(|kind| kind.as_str().into()).collect(),
            ..Default::default()
        },
        &ExportLimits::default(),
        60_000,
        1_000,
    )
    .unwrap();
    let mut cursor = Some(snapshot.handle().cursor);
    let mut records = Vec::new();
    while let Some(value) = cursor {
        let page = snapshot.page(&value, 1_000).unwrap();
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
    assert!(records
        .iter()
        .any(|record| record.kind == EvidenceKind::ToolCall));
    let receiver = tempfile::tempdir().unwrap();
    let store = open(receiver.path());
    let key = ObservationKey {
        source: "junie".into(),
        session_id: SID.into(),
        location: SessionLocation::Remote,
        connector_id: "fixture-remote".into(),
        connector_instance: "default".into(),
    };
    store
        .apply_source_observations(SourceObservationBatch::new(
            "fixture-remote",
            "default",
            SessionLocation::Remote,
            vec![SourceObservation {
                session: ShallowSession {
                    source: "junie".into(),
                    session_id: SID.into(),
                    source_stamp: Some("remote-junie-fixture".into()),
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
                    source_stamp: "remote-junie-fixture".into(),
                    source_bytes: 1,
                    covered_kinds: kinds,
                    records,
                },
            )
            .include_related(false),
        )
        .unwrap();
    assert_eq!(evidence(&store).tool_calls[0].tool_use_id, "step-tool-1");
    let receiver_path = stage(receiver.path());
    std::fs::copy(&origin_path, receiver_path).unwrap();
    store
        .discover(DiscoveryOptions::default().sources([Source::Junie]))
        .unwrap();
    store
        .hydrate(
            &SessionRef::id(Source::Junie, SID),
            HydrateOptions::default(),
        )
        .unwrap();
    assert_eq!(evidence(&store).messages, evidence(&source).messages);
    assert_eq!(evidence(&store).tool_calls, evidence(&source).tool_calls);
}

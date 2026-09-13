use ai_hist_core::{
    observations::ObservationKey,
    source_evidence::{EvidenceKind, EvidenceRecord},
    SessionLocation,
};
use ai_hist_engine::{source_intake::*, ShallowSession};
use anyhow::Result;
use serde_json::json;
fn key(instance: &str) -> ObservationKey {
    ObservationKey {
        source: "claude".into(),
        session_id: "s".into(),
        location: SessionLocation::Remote,
        connector_id: "plugin".into(),
        connector_instance: instance.into(),
    }
}
fn observe(path: &std::path::Path, instance: &str) -> Result<()> {
    apply_source_observations(ApplyObservationsRequest {
        db_path: Some(path.into()),
        connector_id: "plugin".into(),
        connector_instance: instance.into(),
        location: SessionLocation::Remote,
        observations: vec![ShallowSession {
            source: "claude".into(),
            session_id: "s".into(),
            raw_path: Some(format!("instance:{instance}")),
            source_stamp: Some("listing1".into()),
            ..Default::default()
        }],
    })?;
    Ok(())
}
fn state(path: &std::path::Path, instance: &str) -> Result<ObservationState> {
    get_source_observation(ObservationRequest {
        db_path: Some(path.into()),
        key: key(instance),
    })
}
fn event(uid: &str, text: &str) -> EvidenceRecord {
    EvidenceRecord{kind:EvidenceKind::SessionEvent,payload:json!({"id":999,"source":"claude","session_id":"s","event_uid":uid,"role":"assistant","kind":"text","ts_ms":1,"text":text}).as_object().unwrap().clone(),record_id:Some(format!("upstream:{uid}")),revision_id:Some("upstream:1".into())}
}
fn apply(
    path: &std::path::Path,
    instance: &str,
    revision: String,
    covered: Vec<EvidenceKind>,
    records: Vec<EvidenceRecord>,
) -> Result<ai_hist_engine::HydrateSessionResult> {
    apply_source_evidence(ApplyEvidenceRequest {
        db_path: Some(path.into()),
        key: key(instance),
        expected_revision: revision,
        evidence: NormalizedSourceEvidence {
            source_stamp: "full1".into(),
            source_bytes: 20,
            covered_kinds: covered,
            records,
        },
    })
}
#[test]
fn revisions_fence_stale_results_and_instances_are_independent() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("history.db");
    assert!(state(&path, "a")?.revision.is_none());
    assert!(!path.exists());
    observe(&path, "a")?;
    observe(&path, "b")?;
    let first = state(&path, "a")?.revision.unwrap();
    let other = state(&path, "b")?.revision.unwrap();
    apply(
        &path,
        "a",
        first.clone(),
        vec![EvidenceKind::SessionEvent],
        vec![event("shared", "a"), event("only-a", "a")],
    )?;
    let second = state(&path, "a")?.revision.unwrap();
    assert_ne!(first, second);
    let error = apply(
        &path,
        "a",
        first,
        vec![EvidenceKind::SessionEvent],
        vec![event("stale", "stale")],
    )
    .unwrap_err();
    assert!(error.to_string().contains("SOURCE_REVISION_CONFLICT"));
    assert_eq!(state(&path, "a")?.revision, Some(second));
    apply(
        &path,
        "b",
        other,
        vec![EvidenceKind::SessionEvent],
        vec![event("shared", "b"), event("only-b", "b")],
    )?;
    let conn = ai_hist_core::open_db(&path)?;
    let text: String = conn.query_row(
        "SELECT text FROM session_events WHERE event_uid='shared'",
        [],
        |r| r.get(0),
    )?;
    assert_eq!(text, "a");
    apply(
        &path,
        "a",
        state(&path, "a")?.revision.unwrap(),
        vec![EvidenceKind::SessionEvent],
        vec![],
    )?;
    let text: String = conn.query_row(
        "SELECT text FROM session_events WHERE event_uid='shared'",
        [],
        |r| r.get(0),
    )?;
    assert_eq!(text, "b");
    let count: i64 = conn.query_row(
        "SELECT count(*) FROM session_events WHERE event_uid IN ('only-a','stale')",
        [],
        |r| r.get(0),
    )?;
    assert_eq!(count, 0);
    let id: i64 = conn.query_row("SELECT id FROM session_events LIMIT 1", [], |r| r.get(0))?;
    assert_ne!(id, 999);
    Ok(())
}
#[test]
fn partial_kind_update_preserves_tools_and_atomic_failure_preserves_revision() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("history.db");
    observe(&path, "a")?;
    let tool=EvidenceRecord{kind:EvidenceKind::ToolCall,payload:json!({"source":"claude","session_id":"s","tool_use_id":"tool","name":"Read","args_json":"{}"}).as_object().unwrap().clone(),record_id:None,revision_id:None};
    apply(
        &path,
        "a",
        state(&path, "a")?.revision.unwrap(),
        vec![EvidenceKind::SessionEvent, EvidenceKind::ToolCall],
        vec![event("one", "one"), tool],
    )?;
    apply(
        &path,
        "a",
        state(&path, "a")?.revision.unwrap(),
        vec![EvidenceKind::SessionEvent],
        vec![],
    )?;
    let conn = ai_hist_core::open_db(&path)?;
    assert_eq!(
        conn.query_row("SELECT count(*) FROM tool_calls", [], |r| r
            .get::<_, i64>(0))?,
        1
    );
    let revision = state(&path, "a")?.revision.unwrap();
    conn.execute_batch("CREATE TRIGGER fail_source BEFORE INSERT ON session_events BEGIN SELECT RAISE(ABORT,'capture capacity'); END;")?;
    assert!(apply(
        &path,
        "a",
        revision.clone(),
        vec![EvidenceKind::SessionEvent],
        vec![event("fail", "fail")]
    )
    .is_err());
    assert_eq!(state(&path, "a")?.revision, Some(revision));
    assert_eq!(
        conn.query_row("SELECT count(*) FROM session_events", [], |r| r
            .get::<_, i64>(0))?,
        0
    );
    Ok(())
}
#[test]
fn malformed_complete_batch_never_creates_database() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("history.db");
    let mut bad = event("one", "one");
    bad.payload.insert("session_id".into(), json!("other"));
    assert!(apply(
        &path,
        "a",
        "v1:1".into(),
        vec![EvidenceKind::SessionEvent],
        vec![bad]
    )
    .unwrap_err()
    .to_string()
    .contains("INVALID_ARGUMENT"));
    assert!(!path.exists());
    let mut request = ApplyObservationsRequest {
        db_path: Some(path.clone()),
        connector_id: "plugin".into(),
        connector_instance: "a".into(),
        location: SessionLocation::Remote,
        observations: vec![ShallowSession {
            source: "claude".into(),
            session_id: "s".into(),
            ..Default::default()
        }],
    };
    request.observations.push(ShallowSession {
        source: "unknown".into(),
        session_id: "other".into(),
        ..Default::default()
    });
    assert!(apply_source_observations(request).is_err());
    assert!(!path.exists());
    Ok(())
}
#[test]
fn claude_snapshots_use_same_reconciliation_for_both_scan_orders() -> Result<()> {
    use ai_hist_engine::sources::{normalize_source_evidence, AcquiredEvidence};
    for order in [["a", "b"], ["b", "a"]] {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("history.db");
        observe(&path, "a")?;
        observe(&path, "b")?;
        for instance in order {
            let records = vec![
                json!({"sessionId":"s","uuid":"shared","type":"assistant","timestamp":"2026-01-01T00:00:00Z","message":{"role":"assistant","content":[{"type":"text","text":instance},{"type":"tool_use","id":format!("tool-{instance}"),"name":"Read","input":{"file_path":"a.rs"}}]}}),
            ];
            let evidence = normalize_source_evidence(
                "claude",
                "s",
                AcquiredEvidence::ClaudeFull {
                    records,
                    source_stamp: "raw1".into(),
                    source_bytes: 100,
                },
            )?;
            assert!(evidence
                .records
                .iter()
                .any(|r| r.kind == EvidenceKind::ToolCall));
            apply_source_evidence(ApplyEvidenceRequest {
                db_path: Some(path.clone()),
                key: key(instance),
                expected_revision: state(&path, instance)?.revision.unwrap(),
                evidence,
            })?;
        }
        let conn = ai_hist_core::open_db(&path)?;
        let text: String = conn.query_row(
            "SELECT text FROM session_events WHERE kind='text'",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(text, "a");
        let empty = normalize_source_evidence(
            "claude",
            "s",
            AcquiredEvidence::ClaudeFull {
                records: vec![],
                source_stamp: "raw2".into(),
                source_bytes: 0,
            },
        )?;
        apply_source_evidence(ApplyEvidenceRequest {
            db_path: Some(path.clone()),
            key: key("a"),
            expected_revision: state(&path, "a")?.revision.unwrap(),
            evidence: empty,
        })?;
        assert_eq!(
            conn.query_row(
                "SELECT count(*) FROM tool_calls WHERE tool_use_id='tool-a'",
                [],
                |r| r.get::<_, i64>(0)
            )?,
            0
        );
        assert_eq!(
            conn.query_row(
                "SELECT count(*) FROM tool_calls WHERE tool_use_id='tool-b'",
                [],
                |r| r.get::<_, i64>(0)
            )?,
            1
        );
        let text: String = conn.query_row(
            "SELECT text FROM session_events WHERE kind='text'",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(text, "b");
    }
    Ok(())
}

#[test]
fn local_acquisition_after_remote_protects_changed_and_identical_canonical_records() -> Result<()> {
    for changed in [false, true] {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("history.db");
        observe(&path, "a")?;
        apply(
            &path,
            "a",
            state(&path, "a")?.revision.unwrap(),
            vec![EvidenceKind::SessionEvent],
            vec![event("shared", "remote")],
        )?;
        let conn = ai_hist_core::open_db(&path)?;
        let local = if changed { "local" } else { "remote" };
        conn.execute(
            "UPDATE session_events SET text=? WHERE event_uid='shared'",
            [local],
        )?;
        // Ordinary local acquisition establishes its executing connector identity,
        // but a pre-existing canonical row alone cannot prove per-record ownership.
        ai_hist_core::observations::upsert(
            &conn,
            &ai_hist_core::observations::SessionObservation {
                key: ObservationKey {
                    location: SessionLocation::Local,
                    connector_id: "claude".into(),
                    connector_instance: "default".into(),
                    ..key("a")
                },
                raw_locator: Some("local.jsonl".into()),
                source_stamp: Some("local1".into()),
                discovery_state: "full".into(),
                access_state: "available".into(),
                updated_ms: 1,
            },
        )?;
        apply(
            &path,
            "a",
            state(&path, "a")?.revision.unwrap(),
            vec![EvidenceKind::SessionEvent],
            vec![event("shared", "changed remote")],
        )?;
        assert_eq!(
            conn.query_row(
                "SELECT text FROM session_events WHERE event_uid='shared'",
                [],
                |r| r.get::<_, String>(0)
            )?,
            local
        );
        apply(
            &path,
            "a",
            state(&path, "a")?.revision.unwrap(),
            vec![EvidenceKind::SessionEvent],
            vec![],
        )?;
        assert_eq!(
            conn.query_row(
                "SELECT text FROM session_events WHERE event_uid='shared'",
                [],
                |r| r.get::<_, String>(0)
            )?,
            local
        );
    }
    Ok(())
}

#[test]
fn observation_revision_is_monotonic_even_on_same_timestamp_and_recreation() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("history.db");
    observe(&path, "a")?;
    let conn = ai_hist_core::open_db(&path)?;
    let mut observation = state(&path, "a")?.observation.unwrap();
    observation.updated_ms = 7;
    ai_hist_core::observations::upsert(&conn, &observation)?;
    let first = state(&path, "a")?.revision.unwrap();
    observation.raw_locator = Some("updated".into());
    ai_hist_core::observations::upsert(&conn, &observation)?;
    let second = state(&path, "a")?.revision.unwrap();
    assert_ne!(first, second);
    conn.execute(
        "DELETE FROM session_observations WHERE source='claude' AND session_id='s'",
        [],
    )?;
    ai_hist_core::observations::upsert(&conn, &observation)?;
    let third = state(&path, "a")?.revision.unwrap();
    assert_ne!(first, third);
    assert_ne!(second, third);
    Ok(())
}

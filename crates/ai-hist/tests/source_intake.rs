use ai_hist::{
    observations::ObservationKey,
    source_evidence::{EvidenceKind, EvidenceRecord},
    SessionLocation,
};
use ai_hist::{source_intake::*, ShallowSession};
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
        }
        .into()],
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
) -> Result<ai_hist::HydrateSessionResult> {
    apply_scoped(path, instance, revision, covered, records, None)
}
fn apply_scoped(
    path: &std::path::Path,
    instance: &str,
    revision: String,
    covered: Vec<EvidenceKind>,
    records: Vec<EvidenceRecord>,
    include_related: Option<bool>,
) -> Result<ai_hist::HydrateSessionResult> {
    apply_source_evidence(ApplyEvidenceRequest {
        db_path: Some(path.into()),
        key: key(instance),
        expected_revision: revision,
        include_related,
        evidence: NormalizedSourceEvidence {
            source_stamp: "full1".into(),
            source_bytes: 20,
            covered_kinds: covered,
            records,
        },
    })
}
fn record(kind: EvidenceKind, payload: serde_json::Value, id: &str) -> EvidenceRecord {
    EvidenceRecord {
        kind,
        payload: payload.as_object().unwrap().clone(),
        record_id: Some(format!("upstream:{id}")),
        revision_id: Some("upstream:1".into()),
    }
}

/// One record of every kind in `FULL_SESSION_KINDS`.
fn full_session_records() -> Vec<EvidenceRecord> {
    vec![
        record(
            EvidenceKind::History,
            json!({"source":"claude","session_id":"s","prompt":"question","timestamp_ms":1}),
            "prompt",
        ),
        event("answer", "answer"),
        record(
            EvidenceKind::ToolCall,
            json!({"source":"claude","session_id":"s","tool_use_id":"tool-1","name":"Edit"}),
            "tool",
        ),
        record(
            EvidenceKind::FileEdit,
            json!({"source":"claude","session_id":"s","tool_use_id":"tool-1","file_path":"/work/a.rs","tool_name":"Edit"}),
            "edit",
        ),
        record(
            EvidenceKind::Relationship,
            json!({"source":"claude","parent_session_id":"s","relationship_uid":"child-1","child_session_id":"child","relationship":"delegated","identity_status":"observed","evidence_kind":"transcript","created_ms":1,"updated_ms":1}),
            "delegation",
        ),
    ]
}

/// The retained snapshot accumulates every kind this connector has ever
/// covered, which is what the stored `discovery_state` is about. The result's
/// `coverage` answers a different question -- what *this* acquisition examined
/// -- and reporting the accumulated set let a later `include_related: false`
/// snapshot inherit `relationship` from an earlier one and read as `full`
/// despite the opt-out.
#[test]
fn declining_related_evidence_does_not_inherit_coverage_from_an_earlier_acquisition() -> Result<()>
{
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("history.db");
    observe(&path, "a")?;

    let related = apply(
        &path,
        "a",
        state(&path, "a")?.revision.unwrap(),
        vec![
            EvidenceKind::History,
            EvidenceKind::SessionEvent,
            EvidenceKind::ToolCall,
            EvidenceKind::FileEdit,
            EvidenceKind::Relationship,
        ],
        full_session_records(),
    )?;
    assert_eq!(related.capability, "full");
    assert!(related.coverage.contains(&EvidenceKind::Relationship));

    // The same observation, acquired again without delegation evidence.
    let thread_only = apply_scoped(
        &path,
        "a",
        state(&path, "a")?.revision.unwrap(),
        vec![
            EvidenceKind::History,
            EvidenceKind::SessionEvent,
            EvidenceKind::ToolCall,
            EvidenceKind::FileEdit,
        ],
        full_session_records()
            .into_iter()
            .filter(|record| record.kind != EvidenceKind::Relationship)
            .collect(),
        Some(false),
    )?;
    assert!(
        !thread_only.coverage.contains(&EvidenceKind::Relationship),
        "coverage is this acquisition's, not the accumulated snapshot's: {:?}",
        thread_only.coverage
    );
    assert_eq!(
        thread_only.coverage,
        vec![
            EvidenceKind::History,
            EvidenceKind::SessionEvent,
            EvidenceKind::ToolCall,
            EvidenceKind::FileEdit,
        ]
    );
    assert_eq!(thread_only.capability, "partial");
    // The normalized plugin path names what it left out too, in the same words
    // the local path uses. A `partial` capability with nothing naming the
    // absent kinds is most of the way back to the defect this contract removes.
    let partial = thread_only
        .diagnostics
        .iter()
        .find(|diagnostic| diagnostic.code == "HYDRATION_PARTIAL_COVERAGE")
        .expect("a partial plugin snapshot names the kinds it does not cover");
    assert_eq!(
        partial.message,
        "claude evidence covers history, session_event, tool_call, file_edit; \
         this hydration produces no relationship \
         (include_related is off, so delegation evidence is not read)"
    );
    // The acquisition's own diagnostic survives beside it.
    assert!(thread_only
        .diagnostics
        .iter()
        .any(|diagnostic| diagnostic.code == "SOURCE_EVIDENCE_INDEXED"));
    // The earlier acquisition's delegation record is not withdrawn -- not
    // covering a kind is not a claim that it is gone -- so the snapshot as a
    // whole is still complete and the stored state stays `full`. That is the
    // distinction the two fields carry, and it is why reporting the stored
    // state as `coverage` was wrong rather than merely redundant.
    assert_eq!(thread_only.discovery_state, "full");
    // The opt-out reaches intake, not only the connector's snapshot: a request
    // that declined related evidence must not come back listing related
    // sessions, even though the rows an earlier acquisition contributed are
    // still there.
    assert!(
        thread_only.related_session_ids.is_empty(),
        "{:?}",
        thread_only.related_session_ids
    );
    assert_eq!(thread_only.evidence.related_sessions, 0);

    // Asking for it again restores the claim, so the assertions above are about
    // the acquisition and not about evidence that stopped existing.
    let again = apply(
        &path,
        "a",
        state(&path, "a")?.revision.unwrap(),
        vec![
            EvidenceKind::History,
            EvidenceKind::SessionEvent,
            EvidenceKind::ToolCall,
            EvidenceKind::FileEdit,
            EvidenceKind::Relationship,
        ],
        full_session_records(),
    )?;
    assert_eq!(again.capability, "full");
    assert!(again.coverage.contains(&EvidenceKind::Relationship));
    assert!(
        !again
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "HYDRATION_PARTIAL_COVERAGE"),
        "complete coverage names nothing as absent"
    );
    // Positive control for the two assertions above: with related evidence
    // requested the same store does report the child, so their emptiness is
    // about the option rather than about a relationship that never landed.
    assert_eq!(again.related_session_ids, vec!["child".to_string()]);
    assert_eq!(again.evidence.related_sessions, 1);
    Ok(())
}

/// Coverage is acquisition metadata, so a snapshot that covers more than the
/// last one is a different result even when the rows and the stamp are
/// identical -- the capability it reports has changed. Calling that `unchanged`
/// invites a consumer to skip the upgrade.
#[test]
fn expanding_coverage_without_new_rows_is_not_an_unchanged_result() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("history.db");
    observe(&path, "a")?;
    let thread_kinds = vec![
        EvidenceKind::History,
        EvidenceKind::SessionEvent,
        EvidenceKind::ToolCall,
        EvidenceKind::FileEdit,
    ];
    let rows = || {
        full_session_records()
            .into_iter()
            .filter(|record| record.kind != EvidenceKind::Relationship)
            .collect::<Vec<_>>()
    };

    let first = apply_scoped(
        &path,
        "a",
        state(&path, "a")?.revision.unwrap(),
        thread_kinds.clone(),
        rows(),
        Some(false),
    )?;
    assert_eq!(first.status, "hydrated");
    assert_eq!(first.capability, "partial");

    // Control: the identical acquisition, repeated. Same rows, same stamp, same
    // coverage -- genuinely unchanged, and it must stay that way or the
    // assertion below would pass for a result that simply never reports it.
    let repeated = apply_scoped(
        &path,
        "a",
        state(&path, "a")?.revision.unwrap(),
        thread_kinds.clone(),
        rows(),
        Some(false),
    )?;
    assert_eq!(repeated.status, "unchanged");
    assert_eq!(repeated.capability, "partial");

    // The same rows and stamp again, but now covering delegation too. No row is
    // added -- this session simply has no child -- yet the capability rises.
    let upgraded = apply_scoped(
        &path,
        "a",
        state(&path, "a")?.revision.unwrap(),
        vec![
            EvidenceKind::History,
            EvidenceKind::SessionEvent,
            EvidenceKind::ToolCall,
            EvidenceKind::FileEdit,
            EvidenceKind::Relationship,
        ],
        rows(),
        Some(true),
    )?;
    assert_eq!(upgraded.capability, "full");
    assert_ne!(
        upgraded.status, "unchanged",
        "coverage grew and the capability rose, so the result is not unchanged"
    );
    Ok(())
}

/// The checkpoint records what the acquisition did. Storing a literal `false`
/// made it disagree with a hydration that did index delegation.
#[test]
fn the_checkpoint_records_whether_related_evidence_was_acquired() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("history.db");
    observe(&path, "a")?;

    apply_scoped(
        &path,
        "a",
        state(&path, "a")?.revision.unwrap(),
        vec![
            EvidenceKind::History,
            EvidenceKind::SessionEvent,
            EvidenceKind::ToolCall,
            EvidenceKind::FileEdit,
            EvidenceKind::Relationship,
        ],
        full_session_records(),
        Some(true),
    )?;
    assert!(
        state(&path, "a")?.checkpoint.unwrap().include_related,
        "an acquisition that indexed delegation records that it did"
    );

    // Control: declining it stores false, so the field tracks the request
    // rather than being pinned either way.
    apply_scoped(
        &path,
        "a",
        state(&path, "a")?.revision.unwrap(),
        vec![EvidenceKind::History, EvidenceKind::SessionEvent],
        full_session_records()
            .into_iter()
            .filter(|record| {
                matches!(
                    record.kind,
                    EvidenceKind::History | EvidenceKind::SessionEvent
                )
            })
            .collect(),
        Some(false),
    )?;
    assert!(!state(&path, "a")?.checkpoint.unwrap().include_related);
    Ok(())
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
    let conn = ai_hist::open_db(&path)?;
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
    let conn = ai_hist::open_db(&path)?;
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
        }
        .into()],
    };
    request.observations.push(
        ShallowSession {
            source: "unknown".into(),
            session_id: "other".into(),
            ..Default::default()
        }
        .into(),
    );
    assert!(apply_source_observations(request).is_err());
    assert!(!path.exists());
    Ok(())
}
#[test]
fn claude_snapshots_use_same_reconciliation_for_both_scan_orders() -> Result<()> {
    use ai_hist::sources::{normalize_source_evidence, AcquiredEvidence};
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
                include_related: None,
                evidence,
            })?;
        }
        let conn = ai_hist::open_db(&path)?;
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
            include_related: None,
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
        let conn = ai_hist::open_db(&path)?;
        let local = if changed { "local" } else { "remote" };
        conn.execute(
            "UPDATE session_events SET text=? WHERE event_uid='shared'",
            [local],
        )?;
        // Ordinary local acquisition establishes its executing connector identity,
        // but a pre-existing canonical row alone cannot prove per-record ownership.
        ai_hist::observations::upsert(
            &conn,
            &ai_hist::observations::SessionObservation {
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
    let conn = ai_hist::open_db(&path)?;
    let mut observation = state(&path, "a")?.observation.unwrap();
    observation.updated_ms = 7;
    ai_hist::observations::upsert(&conn, &observation)?;
    let first = state(&path, "a")?.revision.unwrap();
    observation.raw_locator = Some("updated".into());
    ai_hist::observations::upsert(&conn, &observation)?;
    let second = state(&path, "a")?.revision.unwrap();
    assert_ne!(first, second);
    conn.execute(
        "DELETE FROM session_observations WHERE source='claude' AND session_id='s'",
        [],
    )?;
    ai_hist::observations::upsert(&conn, &observation)?;
    let third = state(&path, "a")?.revision.unwrap();
    assert_ne!(first, third);
    assert_ne!(second, third);
    Ok(())
}

#[test]
fn external_observation_preserves_opaque_locator_separately_from_display_path() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("history.db");
    let request: ApplyObservationsRequest = serde_json::from_value(
        json!({"db_path":path,"connector_id":"plugin","connector_instance":"a","location":"remote","observations":[{"source":"claude","session_id":"s","raw_locator":"opaque-handle","raw_path":"https://display.example/s","source_stamp":"listing"}]}),
    )?;
    apply_source_observations(request)?;
    assert_eq!(
        state(&path, "a")?
            .observation
            .unwrap()
            .raw_locator
            .as_deref(),
        Some("opaque-handle")
    );
    let conn = ai_hist::open_db(&path)?;
    assert_eq!(
        conn.query_row("SELECT raw_path FROM sessions", [], |row| row
            .get::<_, String>(0))?,
        "https://display.example/s"
    );
    Ok(())
}

#[test]
fn complete_normalized_snapshot_round_trips_all_six_kinds_and_removes_only_owned_rows() -> Result<()>
{
    use EvidenceKind::*;
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("history.db");
    observe(&path, "a")?;
    let record = |kind, payload: serde_json::Value| EvidenceRecord {
        kind,
        payload: payload.as_object().unwrap().clone(),
        record_id: Some("upstream-row".into()),
        revision_id: Some("revision-7".into()),
    };
    let records = vec![
        record(
            History,
            json!({"id":12345,"source":"claude","session_id":"s","timestamp_ms":1,"prompt":"hello","prompt_hash":"untrusted"}),
        ),
        event("event", "response"),
        record(
            ToolCall,
            json!({"source":"claude","session_id":"s","tool_use_id":"tool","name":"Edit","args_json":"{}"}),
        ),
        record(
            FileEdit,
            json!({"source":"claude","session_id":"s","tool_use_id":"tool","file_path":"a.rs","tool_name":"Edit","structured_patch_json":"[]"}),
        ),
        record(
            Relationship,
            json!({"source":"claude","parent_session_id":"s","relationship_uid":"child","child_session_id":"child-s","relationship":"delegated","identity_status":"observed","evidence_kind":"provider","created_ms":1,"updated_ms":1}),
        ),
        record(
            CommitLink,
            json!({"source":"claude","session_id":"s","repo":"repo","commit_sha":"abcdef","match_method":"explicit","confidence":1.0,"created_at_ms":1}),
        ),
    ];
    let kinds = vec![
        History,
        SessionEvent,
        ToolCall,
        FileEdit,
        Relationship,
        CommitLink,
    ];
    let result = apply(
        &path,
        "a",
        state(&path, "a")?.revision.unwrap(),
        kinds.clone(),
        records,
    )?;
    assert_eq!(result.capability, "full");
    let conn = ai_hist::open_db(&path)?;
    for table in [
        "history",
        "session_events",
        "tool_calls",
        "file_edits",
        "session_relationships",
        "session_commit_links",
    ] {
        assert_eq!(
            conn.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r
                .get::<_, i64>(0))?,
            1
        );
    }
    let hash: String = conn.query_row("SELECT prompt_hash FROM history", [], |r| r.get(0))?;
    assert_eq!(hash, ai_hist::prompt_hash("hello"));
    let id: i64 = conn.query_row("SELECT id FROM history", [], |r| r.get(0))?;
    assert_ne!(id, 12345);
    apply(
        &path,
        "a",
        state(&path, "a")?.revision.unwrap(),
        kinds,
        vec![],
    )?;
    for table in [
        "history",
        "session_events",
        "tool_calls",
        "file_edits",
        "session_relationships",
        "session_commit_links",
    ] {
        assert_eq!(
            conn.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r
                .get::<_, i64>(0))?,
            0
        );
    }
    Ok(())
}

#[test]
fn legacy_migration_retains_unknown_canonical_evidence_without_claiming_connector_ownership(
) -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("history.db");
    {
        let conn = ai_hist::open_db(&path)?;
        conn.execute("INSERT INTO sessions(source,session_id,raw_path,discovery_state) VALUES('claude','s','legacy-display','full')",[])?;
        ai_hist::upsert_session_presence(
            &conn,
            "claude",
            "s",
            SessionLocation::Remote,
            Some("legacy-display"),
            Some("legacy-stamp"),
            Some("full"),
        )?;
        conn.execute("INSERT INTO session_events(source,session_id,event_uid,role,kind,ts_ms,text) VALUES('claude','s','shared','assistant','text',1,'legacy content'),('claude','s','removed-upstream','assistant','text',1,'unknown owner')",[])?;
        conn.execute(
            "DELETE FROM schema_migrations WHERE name='connector_observations_v1'",
            [],
        )?;
    }
    {
        let conn = ai_hist::open_db(&path)?;
        let legacy = ai_hist::observations::list(&conn, "claude", "s")?;
        assert_eq!(legacy.len(), 1);
        assert_eq!(legacy[0].key.connector_id, "legacy-unknown");
        assert!(ai_hist::observations::checkpoint(&conn, &legacy[0].key)?.is_none());
    }
    observe(&path, "a")?;
    apply(
        &path,
        "a",
        state(&path, "a")?.revision.unwrap(),
        vec![EvidenceKind::SessionEvent],
        vec![event("shared", "fresh connector"), event("new", "owned")],
    )?;
    let conn = ai_hist::open_db(&path)?;
    assert_eq!(
        conn.query_row(
            "SELECT text FROM session_events WHERE event_uid='shared'",
            [],
            |r| r.get::<_, String>(0)
        )?,
        "legacy content"
    );
    let snapshot = ai_hist::observations::evidence(&conn, &key("a"))?.unwrap();
    assert!(snapshot.to_string().contains("fresh connector"));
    apply(
        &path,
        "a",
        state(&path, "a")?.revision.unwrap(),
        vec![EvidenceKind::SessionEvent],
        vec![],
    )?;
    assert_eq!(
        conn.query_row("SELECT count(*) FROM session_events", [], |r| r
            .get::<_, i64>(0))?,
        2
    );
    drop(conn);
    let conn = ai_hist::open_db(&path)?;
    assert_eq!(ai_hist::observations::list(&conn, "claude", "s")?.len(), 2);
    Ok(())
}

#[test]
fn ownership_revocation_does_not_mutate_sibling_acquisition_state() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("history.db");
    for instance in ["a", "b"] {
        observe(&path, instance)?;
        apply(
            &path,
            instance,
            state(&path, instance)?.revision.unwrap(),
            vec![EvidenceKind::SessionEvent],
            vec![event("shared", "remote")],
        )?;
    }
    let conn = ai_hist::open_db(&path)?;
    let sibling_revision = state(&path, "b")?.revision.unwrap();
    let sibling_evidence = ai_hist::observations::evidence(&conn, &key("b"))?;
    ai_hist::observations::upsert(
        &conn,
        &ai_hist::observations::SessionObservation {
            key: ObservationKey {
                location: SessionLocation::Local,
                connector_id: "claude".into(),
                ..key("local")
            },
            raw_locator: Some("local.jsonl".into()),
            source_stamp: Some("local1".into()),
            discovery_state: "full".into(),
            access_state: "available".into(),
            updated_ms: 1,
        },
    )?;
    // Stand in for bounded delivery capture: acquiring a must not recapture b.
    conn.execute_batch("CREATE TRIGGER reject_sibling_evidence DELETE ON observation_evidence WHEN OLD.connector_instance='b' BEGIN SELECT RAISE(ABORT,'unexpected sibling capture'); END;")?;
    apply(
        &path,
        "a",
        state(&path, "a")?.revision.unwrap(),
        vec![EvidenceKind::SessionEvent],
        vec![],
    )?;
    assert_eq!(
        state(&path, "b")?.revision.as_deref(),
        Some(sibling_revision.as_str())
    );
    assert_eq!(
        ai_hist::observations::evidence(&conn, &key("b"))?,
        sibling_evidence
    );
    conn.execute_batch("DROP TRIGGER reject_sibling_evidence; DELETE FROM session_observations WHERE location='local';")?;
    // Revocation survives reopen and disappearance of ambiguous local provenance.
    drop(conn);
    apply(
        &path,
        "b",
        sibling_revision,
        vec![EvidenceKind::SessionEvent],
        vec![],
    )?;
    let conn = ai_hist::open_db(&path)?;
    assert_eq!(
        conn.query_row(
            "SELECT text FROM session_events WHERE event_uid='shared'",
            [],
            |r| r.get::<_, String>(0)
        )?,
        "remote"
    );
    // A later remote refresh still cannot reclaim the protected canonical row.
    apply(
        &path,
        "b",
        state(&path, "b")?.revision.unwrap(),
        vec![EvidenceKind::SessionEvent],
        vec![event("shared", "remote replacement")],
    )?;
    assert_eq!(
        conn.query_row(
            "SELECT text FROM session_events WHERE event_uid='shared'",
            [],
            |r| r.get::<_, String>(0)
        )?,
        "remote"
    );
    conn.execute(
        "DELETE FROM sessions WHERE source='claude' AND session_id='s'",
        [],
    )?;
    assert_eq!(
        conn.query_row(
            "SELECT count(*) FROM canonical_evidence_protection",
            [],
            |r| r.get::<_, i64>(0)
        )?,
        0
    );
    Ok(())
}

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

/// The mirror of the widening case. The accumulated snapshot kinds only ever
/// grow, so comparing against them catches an acquisition that covers more and
/// misses one that covers less: a later `include_related: false` pass reports
/// `partial` while still calling itself `unchanged`, and a consumer that skips
/// work on `unchanged` keeps the earlier `full` ranking.
#[test]
fn narrowing_coverage_is_not_an_unchanged_result_either() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("history.db");
    observe(&path, "a")?;
    let all_kinds = vec![
        EvidenceKind::History,
        EvidenceKind::SessionEvent,
        EvidenceKind::ToolCall,
        EvidenceKind::FileEdit,
        EvidenceKind::Relationship,
    ];

    let full = apply_scoped(
        &path,
        "a",
        state(&path, "a")?.revision.unwrap(),
        all_kinds.clone(),
        full_session_records(),
        Some(true),
    )?;
    assert_eq!(full.capability, "full");

    // Control: the identical pass again is genuinely unchanged.
    let repeated = apply_scoped(
        &path,
        "a",
        state(&path, "a")?.revision.unwrap(),
        all_kinds,
        full_session_records(),
        Some(true),
    )?;
    assert_eq!(repeated.status, "unchanged");

    // Same stamp, same rows, but this acquisition declined delegation. The
    // accumulated set still contains it, so only comparing against that would
    // call this unchanged while the capability drops.
    let narrowed = apply_scoped(
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
    assert_eq!(narrowed.capability, "partial");
    assert_ne!(
        narrowed.status, "unchanged",
        "coverage narrowed and the capability fell, so the result is not unchanged"
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
/// Remote Claude hydration must carry the provider's request identity.
///
/// `ClaudeFull` evidence is parsed in an isolated database and then projected
/// back out through the evidence row contract. While that projection omitted
/// `request_id` / `provider_message_id`, a remotely hydrated session arrived
/// with null identities: its four records read as four `record-id` requests,
/// each flagged unresolved, and the session reported no total at all — the
/// same multiplied-then-withheld shape the local path was fixed to avoid.
#[test]
fn remote_claude_hydration_preserves_the_provider_request_identity() -> Result<()> {
    use ai_hist::sources::{normalize_source_evidence, AcquiredEvidence};
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("history.db");
    observe(&path, "a")?;
    // One API request the provider split across two records, exactly as
    // Claude writes a multi-block turn.
    let records = (0..2)
        .map(|index| {
            json!({
                "sessionId": "s",
                "uuid": format!("rec-{index}"),
                "requestId": "req_remote",
                "type": "assistant",
                "timestamp": "2026-01-01T00:00:00Z",
                "message": {
                    "role": "assistant",
                    "id": "msg_remote",
                    "model": "claude-test",
                    "usage": {"input_tokens": 3, "output_tokens": 11},
                    "content": [{"type": "text", "text": format!("part {index}")}],
                },
            })
        })
        .collect::<Vec<_>>();
    let evidence = normalize_source_evidence(
        "claude",
        "s",
        AcquiredEvidence::ClaudeFull {
            records,
            source_stamp: "raw1".into(),
            source_bytes: 100,
        },
    )?;
    // The identity survives the projection itself, before anything applies it.
    let carried = evidence
        .records
        .iter()
        .filter(|record| record.kind == EvidenceKind::SessionEvent)
        .filter(|record| {
            record.payload.get("request_id").and_then(|v| v.as_str()) == Some("req_remote")
        })
        .count();
    assert_eq!(carried, 2, "both event rows carry the provider request id");

    apply_source_evidence(ApplyEvidenceRequest {
        db_path: Some(path.clone()),
        key: key("a"),
        expected_revision: state(&path, "a")?.revision.unwrap(),
        include_related: None,
        evidence,
    })?;

    let conn = ai_hist::open_db(&path)?;
    let stored: i64 = conn.query_row(
        "SELECT COUNT(*) FROM session_events \
         WHERE request_id = 'req_remote' AND provider_message_id = 'msg_remote'",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(stored, 2, "applying the evidence keeps both identities");

    // And the point of carrying them: one request, one total.
    let page = ai_hist::session_requests_page(&conn, "claude", "s", 50, None)?;
    assert_eq!(page.requests.len(), 1);
    assert_eq!(page.requests[0].request_key, "request-id:req_remote");
    assert!(page.requests[0].diagnostics.is_empty());
    let summary = ai_hist::session_usage_summary(&conn, "claude", "s")?
        .expect("a hydrated session has a summary");
    assert_eq!(summary.usage.as_ref().unwrap().output_tokens, 11);
    Ok(())
}

/// A remote `ClaudeFull` snapshot is parsed by the same local parser, into a
/// temporary database, and then projected back out as evidence records. The
/// projection is what decides which tables survive that round trip, so a table
/// missing from it is written during normalization and silently thrown away
/// before anything durable sees it.
///
/// Markers were missing, so remote Claude hydration produced none at all --
/// the same class of gap #194 hit with its new columns.
#[test]
fn claude_remote_hydration_carries_session_markers() -> Result<()> {
    use ai_hist::sources::{normalize_source_evidence, AcquiredEvidence};
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("history.db");
    observe(&path, "a")?;

    let records = vec![
        json!({"sessionId":"s","uuid":"u-asst","type":"assistant","timestamp":"2026-01-01T00:00:00Z",
               "message":{"role":"assistant","model":"m","content":[{"type":"text","text":"hi"}],
                          "usage":{"cache_read_input_tokens":9000}}}),
        json!({"sessionId":"s","uuid":"s-compact","type":"system","subtype":"compact_boundary",
               "timestamp":"2026-01-01T00:00:01Z"}),
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
    assert!(
        evidence
            .records
            .iter()
            .any(|r| r.kind == EvidenceKind::SessionMarker),
        "the projection must carry what the parser wrote"
    );

    apply_source_evidence(ApplyEvidenceRequest {
        db_path: Some(path.clone()),
        key: key("a"),
        expected_revision: state(&path, "a")?.revision.unwrap(),
        evidence,
        include_related: None,
    })?;

    let conn = ai_hist::open_db(&path)?;
    let (kind, payload): (String, Option<String>) = conn.query_row(
        "SELECT kind, payload_json FROM session_markers WHERE source='claude' AND session_id='s'",
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    assert_eq!(kind, "compaction_boundary");
    assert!(
        payload
            .as_deref()
            .is_some_and(|payload| payload.contains("9000")),
        "the marker keeps the context it recorded: {payload:?}"
    );

    // A later complete snapshot without that record must take the marker back
    // out again, exactly as it does for every other evidence table.
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
        include_related: None,
    })?;
    assert_eq!(
        conn.query_row("SELECT count(*) FROM session_markers", [], |r| r
            .get::<_, i64>(0))?,
        0,
        "a marker the snapshot no longer carries must not survive it"
    );
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

/// A plugin submitting a continuity relationship must reach the database with
/// `origin_session_id` intact.
///
/// The normalized contract's `Spec.columns` is the one list validation,
/// persistence, equality and the `read_session` projection all read, so
/// omitting the column did not degrade gracefully: the whole record was
/// refused with `unsupported session_relationships column origin_session_id`,
/// and a `fork` could not be submitted at all.
#[test]
fn a_submitted_continuity_relationship_round_trips_its_origin() -> Result<()> {
    use EvidenceKind::Relationship;

    let dir = tempfile::tempdir()?;
    let path = dir.path().join("history.db");
    observe(&path, "a")?;
    let record = EvidenceRecord {
        kind: Relationship,
        payload: json!({
            "source": "claude",
            "parent_session_id": "s",
            "relationship_uid": "fork:branch",
            "child_session_id": "branch",
            "relationship": "fork",
            "identity_status": "observed",
            "evidence_kind": "claude_transcript_continuity",
            "evidence_locator": "/tmp/branch.jsonl",
            "evidence_ref": "sharedSessionId",
            "origin_session_id": "s",
            "child_has_events": false,
            "created_ms": 1,
            "updated_ms": 1,
        })
        .as_object()
        .unwrap()
        .clone(),
        record_id: None,
        revision_id: None,
    };
    apply(
        &path,
        "a",
        state(&path, "a")?.revision.unwrap(),
        vec![Relationship],
        vec![record.clone()],
    )?;

    let conn = ai_hist::open_db(&path)?;
    let stored: (String, String, Option<String>) = conn.query_row(
        "SELECT relationship, parent_session_id, origin_session_id \
         FROM session_relationships WHERE relationship_uid = 'fork:branch'",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    assert_eq!(
        stored,
        ("fork".to_string(), "s".to_string(), Some("s".to_string()))
    );

    // The same submission again is recognized as equal rather than rewritten,
    // which is the equality path reading the same column list.
    apply(
        &path,
        "a",
        state(&path, "a")?.revision.unwrap(),
        vec![Relationship],
        vec![record],
    )?;
    let rows: i64 = conn.query_row(
        "SELECT COUNT(*) FROM session_relationships WHERE relationship_uid = 'fork:branch'",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(rows, 1);
    Ok(())
}

/// A plugin snapshot must not leave the canonical project key null or stale.
///
/// `apply_normalized` writes `session_events` rows straight from an adapter's
/// payload and commits. The adapter is free to omit `project_key` -- an older
/// one does not know the field exists -- or to report one that no longer
/// matches the session. Without a refresh inside that transaction, those rows
/// are the one place in the database where the identity is missing, and for an
/// adapter that never reports again the gap never closes. It has to be inside
/// the transaction, not after the commit, or every reader in between is served
/// the gap.
#[test]
fn a_plugin_snapshot_leaves_every_event_carrying_the_session_key() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("history.db");
    observe(&path, "one")?;

    let conn = ai_hist::open_db(&path)?;
    // The session's identity, as discovery would have resolved it.
    conn.execute(
        "UPDATE sessions SET cwd = '/work/app', project_key = 'github.com/Org/Repo', \
         project_key_method = 'remote' WHERE source = 'claude' AND session_id = 's'",
        [],
    )?;
    // A legacy event already in the store with no key at all.
    conn.execute(
        "INSERT INTO session_events (source, session_id, event_uid, ts_ms, role, kind, text) \
         VALUES ('claude', 's', 'legacy', 1, 'user', 'text', 'older row')",
        [],
    )?;
    let revision = state(&path, "one")?
        .revision
        .expect("an observed session has a revision");

    // The adapter reports one event and says nothing about the project.
    apply(
        &path,
        "one",
        revision,
        vec![EvidenceKind::SessionEvent],
        vec![event("fresh", "from the plugin")],
    )?;

    let keys: Vec<(String, Option<String>)> = conn
        .prepare("SELECT event_uid, project_key FROM session_events ORDER BY event_uid")?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<rusqlite::Result<_>>()?;
    assert_eq!(
        keys,
        vec![
            ("fresh".to_string(), Some("github.com/Org/Repo".to_string())),
            (
                "legacy".to_string(),
                Some("github.com/Org/Repo".to_string())
            ),
        ],
        "the snapshot must leave both the new and the legacy row keyed"
    );
    Ok(())
}

/// Plugin intake is an acquisition pass, so it must open one.
///
/// The project-identity cache is process-global and only sound for the length
/// of a pass. The Node addon serves request after request from one long-lived
/// host, so without a boundary here the answer it reconciles against is
/// whatever the *first* request happened to see: a repository cloned, moved or
/// given an `origin` an hour ago is still filed under the directory it used to
/// be, and that stale key is shaped exactly like a correct one.
#[test]
fn plugin_intake_opens_a_new_acquisition_pass() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("history.db");
    let work = directory.path().join("work/app");
    std::fs::create_dir_all(&work)?;
    observe(&path, "one")?;

    let conn = ai_hist::open_db(&path)?;
    conn.execute(
        "UPDATE sessions SET cwd = ? WHERE source = 'claude' AND session_id = 's'",
        rusqlite::params![work.to_string_lossy()],
    )?;

    // A pass earlier in the life of this process resolved the directory while
    // it was not yet a repository, and that answer is in the cache.
    ai_hist::project_identity::begin_acquisition_pass();
    assert_eq!(
        ai_hist::project_identity::resolve_project_identity(&work.to_string_lossy()).method,
        ai_hist::project_identity::ProjectKeyMethod::PathFallback
    );

    // Then the checkout gains an origin, as checkouts do.
    let git_dir = work.join(".git");
    std::fs::create_dir_all(&git_dir)?;
    std::fs::write(
        git_dir.join("config"),
        "[remote \"origin\"]\n\turl = git@github.com:acme/app.git\n",
    )?;

    let revision = state(&path, "one")?
        .revision
        .expect("an observed session has a revision");
    apply(
        &path,
        "one",
        revision,
        vec![EvidenceKind::SessionEvent],
        vec![event("fresh", "from the plugin")],
    )?;

    let key: (Option<String>, Option<String>) = conn.query_row(
        "SELECT project_key, project_key_method FROM sessions \
         WHERE source = 'claude' AND session_id = 's'",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    assert_eq!(
        key,
        (
            Some("github.com/acme/app".to_string()),
            Some("remote".to_string())
        ),
        "intake reconciled against a directory state from an earlier request"
    );
    Ok(())
}

/// Enriching an adapter's event must not take it away from the adapter.
///
/// `project_key` and `project_key_method` travel in the record so a snapshot
/// round-trips what the emitting side knew, but they are *derived* canonical
/// state: `refresh_project_identity` rewrites them, inside the very
/// transaction that stores the adapter's snapshot. Comparing them in the
/// ownership check therefore reads the enrichment as an external edit — the
/// connector's own event is protected against the connector, and from then on
/// a changed text is never applied and an omitted record is never deleted.
/// The adapter goes on reporting into a record it no longer owns, and nothing
/// says so.
#[test]
fn enrichment_does_not_revoke_the_adapter_s_ownership_of_its_event() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("history.db");
    observe(&path, "one")?;

    let conn = ai_hist::open_db(&path)?;
    // The session has a canonical key, so the refresh has something to stamp.
    conn.execute(
        "UPDATE sessions SET cwd = '/work/app', project_key = 'github.com/Org/Repo', \
         project_key_method = 'remote' WHERE source = 'claude' AND session_id = 's'",
        [],
    )?;

    let revision = |instance: &str| -> Result<String> {
        Ok(state(&path, instance)?
            .revision
            .expect("an observed session has a revision"))
    };
    let text_of = |uid: &str| -> Option<String> {
        conn.query_row(
            "SELECT text FROM session_events WHERE event_uid = ?",
            rusqlite::params![uid],
            |row| row.get(0),
        )
        .ok()
    };

    // The adapter says nothing about the project, as an older one would not.
    apply(
        &path,
        "one",
        revision("one")?,
        vec![EvidenceKind::SessionEvent],
        vec![event("e1", "first")],
    )?;
    assert_eq!(
        conn.query_row(
            "SELECT project_key FROM session_events WHERE event_uid = 'e1'",
            [],
            |row| row.get::<_, Option<String>>(0),
        )?,
        Some("github.com/Org/Repo".to_string()),
        "the premise is that intake enriches the event it just stored"
    );

    // Same record, new text. The adapter still owns it, so this must land.
    apply(
        &path,
        "one",
        revision("one")?,
        vec![EvidenceKind::SessionEvent],
        vec![event("e1", "second")],
    )?;
    assert_eq!(
        text_of("e1").as_deref(),
        Some("second"),
        "an update from the owning adapter was refused after its own event was enriched"
    );

    // And the adapter dropping the record must delete it.
    apply(
        &path,
        "one",
        revision("one")?,
        vec![EvidenceKind::SessionEvent],
        vec![],
    )?;
    assert_eq!(
        text_of("e1"),
        None,
        "a record the adapter stopped reporting was kept, because enrichment had \
         quietly revoked its ownership"
    );

    // --- the control: a real external edit still revokes -----------------
    //
    // The protection exists for a reason, and this proves the fix did not
    // disarm it: a *non-derived* column changed outside the connector means
    // something else owns the row now, and the connector may no longer delete
    // it.
    apply(
        &path,
        "one",
        revision("one")?,
        vec![EvidenceKind::SessionEvent],
        vec![event("e2", "from the adapter")],
    )?;
    conn.execute(
        "UPDATE session_events SET text = 'edited locally' WHERE event_uid = 'e2'",
        [],
    )?;
    apply(
        &path,
        "one",
        revision("one")?,
        vec![EvidenceKind::SessionEvent],
        vec![],
    )?;
    assert_eq!(
        text_of("e2").as_deref(),
        Some("edited locally"),
        "an externally edited row must be protected from the connector that used to own it"
    );
    Ok(())
}

/// A connector cannot submit a marker payload the local parser could not write.
///
/// `payload_json` is a bounded projection: every string at 128 characters,
/// every container at 32 entries, recursively. That bound is applied by the
/// parsers, and `EvidenceKind::SessionMarker` put the same column on the public
/// normalized-evidence contract — where the generic validator accepts any
/// string and stores it verbatim. So a contributed marker could hold what a
/// parsed one cannot, in the one place the bound exists to defend.
///
/// Rejected rather than silently bounded, which is how this boundary treats
/// every other out-of-contract value: an unknown `result_status`, a negative
/// `payload_bytes`, a tool-result field on a row that is not one. The boundary
/// derives (`prompt_hash`) and fills absent columns with null, but it never
/// rewrites a value a connector supplied — doing so here would store something
/// the connector did not send and cannot reconcile against.
#[test]
fn a_contributed_marker_payload_obeys_the_same_bound() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("history.db");
    observe(&path, "a")?;

    let marker = |payload: serde_json::Value| -> EvidenceRecord {
        EvidenceRecord {
            kind: EvidenceKind::SessionMarker,
            payload: json!({
                "source": "claude",
                "session_id": "s",
                "marker_uid": "m1",
                "kind": "unknown",
                "payload_json": payload.to_string(),
            })
            .as_object()
            .unwrap()
            .clone(),
            record_id: Some("upstream:m1".into()),
            revision_id: Some("upstream:1".into()),
        }
    };

    let huge = "x".repeat(5_000);
    let rejected = apply(
        &path,
        "a",
        state(&path, "a")?.revision.unwrap(),
        vec![EvidenceKind::SessionMarker],
        vec![marker(json!({ "nested": { "deeper": huge } }))],
    );
    let message = format!(
        "{:#}",
        rejected.expect_err("an oversized payload is refused")
    );
    assert!(
        message.contains("INVALID_ARGUMENT"),
        "refused as a contract violation, like every other one: {message}"
    );

    // Malformed JSON is refused too: the column's contract is that it is
    // bounded, and text nothing can parse cannot be shown to be.
    let broken = EvidenceRecord {
        kind: EvidenceKind::SessionMarker,
        payload: json!({
            "source": "claude",
            "session_id": "s",
            "marker_uid": "m2",
            "kind": "unknown",
            "payload_json": "{not json",
        })
        .as_object()
        .unwrap()
        .clone(),
        record_id: Some("upstream:m2".into()),
        revision_id: Some("upstream:1".into()),
    };
    assert!(apply(
        &path,
        "a",
        state(&path, "a")?.revision.unwrap(),
        vec![EvidenceKind::SessionMarker],
        vec![broken],
    )
    .is_err());

    // Positive control: a payload within the bound round-trips untouched, so
    // this rejects what breaks the contract rather than markers in general.
    apply(
        &path,
        "a",
        state(&path, "a")?.revision.unwrap(),
        vec![EvidenceKind::SessionMarker],
        vec![marker(json!({ "tokens_before_compact": 9000 }))],
    )?;
    let conn = ai_hist::open_db(&path)?;
    let stored: String = conn.query_row(
        "SELECT payload_json FROM session_markers WHERE source='claude' AND session_id='s'",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&stored)?,
        json!({ "tokens_before_compact": 9000 })
    );
    Ok(())
}

/// An empty `kind` is not a kind.
///
/// `kind` is in the marker spec's `required` list, but that check only asks
/// whether the field is present and non-null -- so `""` passed, and the
/// NOT NULL column stored it happily. A marker whose classification is the
/// empty string is indistinguishable from one whose classifier failed, which
/// is the state this table exists to make impossible.
///
/// Refused rather than rewritten, consistent with every other out-of-contract
/// value at this boundary.
#[test]
fn a_contributed_marker_needs_a_real_kind() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("history.db");
    observe(&path, "a")?;

    let marker = |kind: &str, uid: &str| -> EvidenceRecord {
        EvidenceRecord {
            kind: EvidenceKind::SessionMarker,
            payload: json!({
                "source": "claude",
                "session_id": "s",
                "marker_uid": uid,
                "kind": kind,
            })
            .as_object()
            .unwrap()
            .clone(),
            record_id: Some(format!("upstream:{uid}")),
            revision_id: Some("upstream:1".into()),
        }
    };

    let refused = apply(
        &path,
        "a",
        state(&path, "a")?.revision.unwrap(),
        vec![EvidenceKind::SessionMarker],
        vec![marker("", "m-empty")],
    );
    let message = format!("{:#}", refused.expect_err("an empty kind is refused"));
    assert!(message.contains("INVALID_ARGUMENT"), "{message}");

    // Positive control: `unknown` is a real classification and is accepted,
    // so this refuses empty rather than refusing unclassified.
    apply(
        &path,
        "a",
        state(&path, "a")?.revision.unwrap(),
        vec![EvidenceKind::SessionMarker],
        vec![marker("unknown", "m-unknown")],
    )?;
    let conn = ai_hist::open_db(&path)?;
    let stored: String = conn.query_row(
        "SELECT kind FROM session_markers WHERE source='claude' AND session_id='s'",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(stored, "unknown");
    Ok(())
}

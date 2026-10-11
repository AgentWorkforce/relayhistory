use super::HistoryCase;
use ai_hist::export::{ExportLimits, ExportSelection, ExportSnapshot, SessionIdentity};
use ai_hist::{SessionEvidence, SessionStore};
use std::collections::BTreeSet;

pub(super) fn assert_export(
    store: &SessionStore,
    case: &HistoryCase<'_>,
    evidence: &SessionEvidence,
) {
    let selection = ExportSelection {
        sessions: vec![SessionIdentity {
            source: case.source.as_str().to_string(),
            session_id: case.session_id.to_string(),
        }],
        kinds: vec!["session_event".to_string()],
        ..Default::default()
    };
    // Force multiple pages; default limits would hide cursor/duplication bugs
    // in ordinary two-message fixtures.
    let limits = ExportLimits {
        max_batch_records: 1,
        ..Default::default()
    };
    let now = chrono::Utc::now().timestamp_millis();
    let conn = rusqlite::Connection::open_with_flags(
        store.db_path(),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .expect("open crate-owned export snapshot");
    let mut snapshot = ExportSnapshot::open(conn, &selection, &limits, 60_000, now)
        .expect("start selected export");
    let mut cursor = Some(snapshot.handle().cursor);
    let mut ids = BTreeSet::new();
    while let Some(value) = cursor {
        let page = snapshot.page(&value, now).expect("read export page");
        cursor = page.next_cursor;
        for record in page.records {
            assert_eq!(record.source, case.source.as_str());
            assert_eq!(record.session_id.as_deref(), Some(case.session_id));
            assert_eq!(record.kind, "session_event");
            let id = record.payload["event_uid"]
                .as_str()
                .expect("export native identity");
            assert!(ids.insert(id.to_string()), "export repeated an event");
        }
    }
    let expected: BTreeSet<_> = evidence
        .messages
        .iter()
        .flat_map(|message| &message.blocks)
        .map(|block| block.event_uid.clone())
        .collect();
    assert_eq!(
        ids, expected,
        "export preserves all acquired event identities"
    );
}

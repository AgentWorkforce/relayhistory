//! Keep Droid's native decimal block ordinal when identity-keyed reconciliation
//! writes a snapshot. The ledger identity itself never changes.
use crate::source_evidence::{EvidenceKind, EvidenceRecord};
use serde_json::json;

pub(super) fn write_key(record: &EvidenceRecord) -> (String, u64, String) {
    let identity = record.identity();
    if record.kind == EvidenceKind::SessionEvent && record.payload["source"] == "droid" {
        if let Some((message, ordinal)) = record.payload["event_uid"]
            .as_str()
            .and_then(|uid| uid.rsplit_once(':'))
        {
            if record
                .payload
                .get("message_id")
                .and_then(|value| value.as_str())
                == Some(message)
            {
                if let Ok(index) = ordinal.parse::<u64>() {
                    if ordinal == index.to_string() {
                        let group = json!([
                            record.kind,
                            ["droid", record.payload["session_id"], message]
                        ])
                        .to_string();
                        return (group, index, identity);
                    }
                }
            }
        }
    }
    (identity.clone(), 0, identity)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn event(source: &str, message: &str, uid: &str) -> EvidenceRecord {
        EvidenceRecord {
            kind: EvidenceKind::SessionEvent,
            payload: json!({"source":source,"session_id":"s","message_id":message,"event_uid":uid})
                .as_object()
                .unwrap()
                .clone(),
            record_id: None,
            revision_id: None,
        }
    }
    #[test]
    fn only_canonical_droid_block_ordinals_change_insertion_order() {
        let tenth = event("droid", "message:with:colons", "message:with:colons:10");
        let second = event("droid", "message:with:colons", "message:with:colons:2");
        let identity = tenth.identity();
        assert!(write_key(&second) < write_key(&tenth));
        assert_eq!(tenth.identity(), identity);
        let mut unbound = event("droid", "message", "message:10");
        unbound.payload.remove("message_id");
        assert_eq!(
            write_key(&unbound),
            (unbound.identity(), 0, unbound.identity())
        );
        for row in [
            event("claude", "message", "message:10"),
            event("droid", "different", "message:10"),
            event("droid", "message", "message:010"),
            event("droid", "message", "message:18446744073709551616"),
        ] {
            assert_eq!(write_key(&row), (row.identity(), 0, row.identity()));
        }
    }
}

use super::*;

#[test]
fn malformed_interior_record_is_not_a_partial_tail() {
    let bytes = b"{\"kind\":\"UserPromptEvent\",\"prompt\":\"hi\",\"timestampMs\":1}\n{bad}\n";
    assert!(format::parse(bytes).is_err());
    let partial = b"{\"kind\":\"UserPromptEvent\",\"prompt\":\"hi\",\"timestampMs\":1}\n{\"kind\":";
    let (rows, incomplete) = format::parse(partial).unwrap();
    assert_eq!(rows.len(), 1);
    assert!(incomplete);
    let complete_json_without_newline = b"{\"kind\":\"UserPromptEvent\",\"prompt\":\"hi\",\"timestampMs\":1}\n{\"kind\":\"UserPromptEvent\",\"prompt\":\"not committed\",\"timestampMs\":2}";
    let (rows, incomplete) = format::parse(complete_json_without_newline).unwrap();
    assert_eq!(
        rows.len(),
        1,
        "an unterminated append has no commit boundary"
    );
    assert!(incomplete);
}

#[test]
fn unknown_native_event_is_retained_without_inventing_a_message() {
    let (rows, partial) =
        format::parse(b"{\"kind\":\"FutureEvent\",\"timestampMs\":1,\"secretNewField\":true}\n")
            .unwrap();
    assert!(!partial);
    assert_eq!(rows.len(), 1);
    assert!(format::human_text(&rows[0]).is_none());
}

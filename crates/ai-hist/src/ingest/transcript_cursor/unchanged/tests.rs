use super::*;
use rusqlite::Connection;
use std::{
    fs,
    path::{Path, PathBuf},
};

fn fixture() -> (
    tempfile::TempDir,
    Connection,
    PathBuf,
    TranscriptCursorState,
) {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("archive.jsonl");
    fs::write(&path, "{\"message\":\"aaaa\"}\n".repeat(128)).unwrap();
    let conn = Connection::open(temp.path().join("history.db")).unwrap();
    crate::init_db(&conn).unwrap();
    let (file, _) = whole_file_cursor(&path).unwrap();
    let mut metadata = file.clone();
    metadata.unchanged_since_ms += 17;
    let cursor = TranscriptCursorState {
        file: Some(file),
        claude: Some(ClaudeCursorState {
            scan: Some(ClaudeScanState {
                file: Some(metadata),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    };
    save(&conn, &path, &cursor);
    (temp, conn, path, cursor)
}
fn key(path: &Path) -> CursorKey<'_> {
    CursorKey::Locator {
        source: "claude",
        locator: path.to_str().unwrap(),
    }
}
fn save(conn: &Connection, path: &Path, cursor: &TranscriptCursorState) {
    store_cursor(conn, &key(path), cursor).unwrap();
}
fn bytes(cursor: &TranscriptCursorState) -> u64 {
    prefix_window_bytes(cursor.file.as_ref().unwrap().offset)
}

#[test]
fn identical_record_and_metadata_proofs_hash_the_provider_window_once() {
    let (_temp, conn, path, cursor) = fixture();
    let before = load_cursor_with_document(&conn, &key(&path)).unwrap().1;
    reset_validation_meter();
    assert!(transcript_unchanged_readonly(&conn, "claude", &path).unwrap());
    assert_eq!(
        validation_meter(),
        bytes(&cursor),
        "equal proof inputs must not reopen and hash the same window for metadata"
    );
    assert_eq!(
        load_cursor_with_document(&conn, &key(&path)).unwrap().1,
        before,
        "reuse must not adopt or settle a cursor"
    );
}

#[test]
fn a_different_metadata_hash_is_independently_checked() {
    let (_temp, conn, path, mut cursor) = fixture();
    cursor
        .claude
        .as_mut()
        .unwrap()
        .scan
        .as_mut()
        .unwrap()
        .file
        .as_mut()
        .unwrap()
        .prefix_hash = "stale metadata".into();
    save(&conn, &path, &cursor);
    reset_validation_meter();
    assert!(!transcript_unchanged_readonly(&conn, "claude", &path).unwrap());
    assert_eq!(validation_meter(), 2 * bytes(&cursor));
}

#[test]
fn different_metadata_offsets_keep_their_own_window_proof() {
    let (_temp, conn, path, mut cursor) = fixture();
    let offset = cursor.file.as_ref().unwrap().offset / 2;
    let mut file = fs::File::open(&path).unwrap();
    let metadata = cursor
        .claude
        .as_mut()
        .unwrap()
        .scan
        .as_mut()
        .unwrap()
        .file
        .as_mut()
        .unwrap();
    metadata.offset = offset;
    metadata.prefix_hash = prefix_window_digest(&mut file, offset).unwrap();
    save(&conn, &path, &cursor);
    reset_validation_meter();
    assert!(transcript_unchanged_readonly(&conn, "claude", &path).unwrap());
    assert_eq!(
        validation_meter(),
        bytes(&cursor) + prefix_window_bytes(offset)
    );
}

#[test]
fn missing_or_rotated_metadata_identity_does_not_share_the_record_proof() {
    for missing in [true, false] {
        let (_temp, conn, path, mut cursor) = fixture();
        if missing {
            cursor.claude.as_mut().unwrap().scan = None;
        } else {
            cursor
                .claude
                .as_mut()
                .unwrap()
                .scan
                .as_mut()
                .unwrap()
                .file
                .as_mut()
                .unwrap()
                .inode = Some(u64::MAX);
        }
        save(&conn, &path, &cursor);
        let valid = transcript_unchanged_readonly(&conn, "claude", &path).unwrap();
        assert_eq!(valid, !missing && !cfg!(unix));
    }
}

#[test]
fn same_stat_rewrites_and_held_open_appends_still_require_acquisition() {
    {
        let (_temp, conn, path, _cursor) = fixture();
        assert!(transcript_unchanged_readonly(&conn, "claude", &path).unwrap());
        let modified = fs::metadata(&path).unwrap().modified().unwrap();
        let original = fs::read_to_string(&path).unwrap();
        fs::write(&path, original.replace("aaaa", "bbbb")).unwrap();
        fs::File::open(&path)
            .unwrap()
            .set_modified(modified)
            .unwrap();
        assert!(!transcript_unchanged_readonly(&conn, "claude", &path).unwrap());
    }
    {
        let (_temp, conn, path, _cursor) = fixture();
        assert!(transcript_unchanged_readonly(&conn, "claude", &path).unwrap());
        use std::io::Write;
        let mut writer = fs::OpenOptions::new().append(true).open(&path).unwrap();
        writer.write_all(b"{\"partial\":").unwrap();
        writer.flush().unwrap();
        assert!(!transcript_unchanged_readonly(&conn, "claude", &path).unwrap());
    }
}

#[test]
fn each_read_observes_a_foreign_cursor_writer() {
    let (_temp, conn, path, mut cursor) = fixture();
    assert!(transcript_unchanged_readonly(&conn, "claude", &path).unwrap());
    let writer = Connection::open(conn.path().unwrap()).unwrap();
    cursor
        .claude
        .as_mut()
        .unwrap()
        .scan
        .as_mut()
        .unwrap()
        .file
        .as_mut()
        .unwrap()
        .prefix_hash = "foreign cursor".into();
    save(&writer, &path, &cursor);
    assert!(!transcript_unchanged_readonly(&conn, "claude", &path).unwrap());
}

#[test]
fn changed_size_encoding_or_stored_hash_keep_independent_validation() {
    for field in ["size", "compressed", "stored_hash"] {
        let (_temp, conn, path, mut cursor) = fixture();
        let metadata = cursor
            .claude
            .as_mut()
            .unwrap()
            .scan
            .as_mut()
            .unwrap()
            .file
            .as_mut()
            .unwrap();
        match field {
            "size" => metadata.size += 1,
            "compressed" => metadata.compressed = true,
            _ => metadata.stored_hash = Some("different stored hash".into()),
        }
        save(&conn, &path, &cursor);
        reset_validation_meter();
        assert!(transcript_unchanged_readonly(&conn, "claude", &path).unwrap());
        assert_eq!(
            validation_meter(),
            2 * bytes(&cursor),
            "{field} must not share a proof"
        );
    }
}

#[test]
fn a_newer_metadata_mtime_and_legacy_missing_record_cursor_are_not_shared() {
    let (_temp, conn, path, mut cursor) = fixture();
    cursor
        .claude
        .as_mut()
        .unwrap()
        .scan
        .as_mut()
        .unwrap()
        .file
        .as_mut()
        .unwrap()
        .mtime_ns += 1;
    save(&conn, &path, &cursor);
    assert!(!transcript_unchanged_readonly(&conn, "claude", &path).unwrap());
    cursor.file = None;
    save(&conn, &path, &cursor);
    assert!(!transcript_unchanged_readonly(&conn, "claude", &path).unwrap());
}

#[test]
fn mutation_after_metadata_check_is_checked_by_the_first_provider_read() {
    let (_temp, conn, path, _cursor) = fixture();
    let rewrite = path.clone();
    let modified = fs::metadata(&path).unwrap().modified().unwrap();
    set_before_prefix_read_for_test(Some(Box::new(move || {
        let text = fs::read_to_string(&rewrite).unwrap();
        fs::write(&rewrite, text.replace("aaaa", "bbbb")).unwrap();
        fs::File::open(&rewrite)
            .unwrap()
            .set_modified(modified)
            .unwrap();
    })));
    let result = transcript_unchanged_readonly(&conn, "claude", &path);
    set_before_prefix_read_for_test(None);
    assert!(!result.unwrap());
}

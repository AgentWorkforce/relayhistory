//! Cursor hydration: a transcript, read from its byte-offset records, and a
//! chat Cursor kept only in its own chat store, rebuilt whole.

use super::*;

pub(super) fn ingest_cursor(
    conn: &Connection,
    options: &HydrateSessionOptions,
    _target: &CatalogTarget,
    path: &Path,
) -> Result<(Vec<HydrationDiagnostic>, u64)> {
    let project = crate::ingest::cursor::project_dir_name(path).map(decode_cursor_project);
    let mtime_ms = file_modified_ms(path).unwrap_or(0);
    // Targeted hydration always re-reads the whole transcript, so it is a
    // rebuild: clear every row a previous read left before writing the new
    // one. Upserting on top is not enough. Cursor evidence is keyed on the
    // record's byte offset, and a rewritten transcript reuses those offsets
    // for different records, so the rows an earlier generation wrote past the
    // new end of the file would survive as tool calls and edits this session
    // never made. `history` cannot be upserted at all, because a prompt's
    // identity includes a timestamp an earlier parser took from the mtime.
    //
    // The read happens inside the caller's transaction, and it propagates its
    // error, so a transcript that has vanished or turned unreadable since the
    // snapshot rolls this delete back rather than committing an empty session.
    clear_cursor_session_evidence(conn, &options.session_id)?;
    let outcome = ingest_cursor_transcript(
        conn,
        path,
        &options.session_id,
        project.as_deref(),
        mtime_ms,
        0,
        // Hydration re-reads the whole file. The Cursor byte checkpoint is
        // advanced after this transaction commits, so a later incremental
        // sync does not re-insert the untimed prompts this pass just wrote.
        u64::MAX,
    )?;
    // Authoritative about both ends of the window, having just read the whole
    // file: an expanding merge would keep a mtime endpoint an earlier parser
    // wrote, which MAX() can never retract.
    upsert_session_rebuilt(
        conn,
        &SessionCatalogRow {
            session_id: &options.session_id,
            source: "cursor",
            cwd: project.as_deref(),
            git_branch: None,
            first_ts: outcome.first_ts_ms.unwrap_or(mtime_ms),
            last_ts: outcome.last_ts_ms.unwrap_or(mtime_ms),
            last_assistant_text: outcome.last_assistant_text.as_deref(),
            raw_path: Some(&path.to_string_lossy()),
        },
    )?;
    if options.include_related {
        // Both ends: a subagent's edge to its parent, and a parent's edge to
        // every subagent filed under it, so `relationship` coverage is what
        // this hydration claims whichever of the two was selected.
        crate::ingest::cursor::record_subagent_delegation(conn, path)?;
        if let Some(session_dir) = path.parent() {
            for (child, _, _) in
                crate::ingest::scoped_files::cursor_subagent_transcripts(session_dir, "")?
            {
                crate::ingest::cursor::record_subagent_delegation(conn, &child)?;
            }
        }
    }
    Ok((cursor_diagnostics(&outcome), outcome.consumed_through))
}

/// What a Cursor transcript could not establish on its own.
///
/// Both codes describe an absence in the provider's records, not a failure of
/// this run: reporting them is the difference between "Cursor does not write
/// this" and "RelayHistory did not read it".
fn cursor_diagnostics(outcome: &CursorTranscriptOutcome) -> Vec<HydrationDiagnostic> {
    let mut diagnostics = Vec::new();
    if outcome.used_mtime_fallback {
        diagnostics.push(HydrationDiagnostic {
            code: "CURSOR_TIMESTAMP_FROM_MTIME".to_string(),
            message: "cursor records carry no timestamp field; events in turns with no readable \
                      <timestamp> tag are stamped with the transcript file mtime"
                .to_string(),
            duration_ms: None,
            source_bytes: None,
            records_parsed: None,
        });
    }
    if outcome.subagent_calls > 0 {
        diagnostics.push(HydrationDiagnostic {
            code: "CURSOR_SUBAGENT_SPAWN_UNLINKED".to_string(),
            message: format!(
                "cursor recorded {} subagent spawn call(s) but writes no child transcript id \
                 into them; a call is not matched to the subagent transcript it started, \
                 which is linked by its subagents/ location instead",
                outcome.subagent_calls
            ),
            duration_ms: None,
            source_bytes: None,
            records_parsed: None,
        });
    }
    diagnostics
}

/// Targeted hydration of a chat Cursor kept only in its chat store: the whole
/// chat, rebuilt, as the sync pass does. A store has no byte checkpoint.
pub(super) fn ingest_chat_store(
    conn: &Connection,
    options: &HydrateSessionOptions,
    path: &Path,
    records: i64,
) -> Result<SelectedIngestResult> {
    // A chat that has gained a transcript is the transcript's now; this row
    // still names the store until discovery runs again.
    if crate::ingest::cursor_store::superseded_by_transcript(path)? {
        return Err(hydration_error(
            "SESSION_SOURCE_MISMATCH",
            format!(
                "Cursor catalog row points at the chat store {}, but the chat now has a \
                 transcript; run discoverSessions() again to re-establish its provenance",
                path.display()
            ),
        ));
    }
    let store = crate::ingest::cursor_store::read_chat_store(path)?.ok_or_else(|| {
        hydration_error(
            "SESSION_SOURCE_UNAVAILABLE",
            "Cursor chat store holds no conversation",
        )
    })?;
    if store.session_id != options.session_id {
        return Err(hydration_error(
            "SESSION_SOURCE_MISMATCH",
            "Cursor chat store identity does not match the catalog row",
        ));
    }
    let fallback_ms = store.updated_ms.or(store.created_ms).unwrap_or(0);
    clear_cursor_session_evidence(conn, &options.session_id)?;
    let outcome = ingest_cursor_transcript(
        conn,
        crate::ingest::cursor_store::Records::Rendered(&store.transcript),
        &options.session_id,
        store.cwd.as_deref(),
        fallback_ms,
        0,
        u64::MAX,
    )?;
    upsert_session_rebuilt(
        conn,
        &SessionCatalogRow {
            session_id: &options.session_id,
            source: "cursor",
            cwd: store.cwd.as_deref(),
            git_branch: None,
            first_ts: crate::ingest::cursor_store::chat_start(
                &outcome,
                store.created_ms,
                fallback_ms,
            ),
            last_ts: crate::ingest::cursor_store::chat_end(&outcome, store.updated_ms, fallback_ms),
            last_assistant_text: outcome.last_assistant_text.as_deref(),
            raw_path: Some(&path.to_string_lossy()),
        },
    )?;
    let mut diagnostics = cursor_diagnostics(&outcome);
    if store.skipped_blobs > 0 {
        diagnostics.push(HydrationDiagnostic {
            code: "CURSOR_CHAT_STORE_BLOB_UNREADABLE".to_string(),
            message: format!(
                "{} message blob(s) in the Cursor chat store were missing or not readable JSON \
                 (possibly encrypted) and were skipped",
                store.skipped_blobs
            ),
            duration_ms: None,
            source_bytes: None,
            records_parsed: None,
        });
    }
    let outcome = IngestOutcome {
        records: records.max(i64::try_from(store.records).unwrap_or(i64::MAX)),
        bytes_read: path.metadata().map_or(0, |metadata| metadata.len() as i64),
        ..Default::default()
    };
    Ok((outcome, diagnostics, None))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::cursor_store::test_support::{tool_chat, write_store};

    fn catalog_store(conn: &Connection, session_id: &str, path: &Path) {
        let locator = path.to_string_lossy().into_owned();
        conn.execute(
            "INSERT INTO sessions (source, session_id, raw_path, discovery_state) \
             VALUES ('cursor', ?, ?, 'shallow')",
            params![session_id, locator],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO session_presences \
             (source, session_id, location, raw_locator, source_stamp, discovery_state) \
             VALUES ('cursor', ?, 'local', ?, 'v2:test-discovery-stamp', 'shallow')",
            params![session_id, locator],
        )
        .unwrap();
    }

    fn options(session_id: &str) -> HydrateSessionOptions {
        HydrateSessionOptions {
            source: "cursor".to_string(),
            session_id: session_id.to_string(),
            scope: SessionScope::Local,
            include_related: true,
        }
    }

    #[test]
    fn a_cursor_chat_with_no_transcript_hydrates_from_its_chat_store() {
        let dir = tempfile::tempdir().unwrap();
        let store = write_store(
            dir.path(),
            "ws",
            "cur-store",
            "/work/app",
            &tool_chat("read note.txt"),
            1_795_042_344_825,
        )
        .unwrap();
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_store(&conn, "cur-store", &store);
        drop(conn);

        let result = hydrate_session_at_with_home(&db, &options("cur-store"), dir.path()).unwrap();
        assert_eq!(result.status, "hydrated", "{:?}", result.diagnostics);
        assert!(result.evidence.events > 0, "{:?}", result.evidence);
        assert_eq!(result.evidence.tool_calls, 1, "{:?}", result.evidence);
        let conn = open_db(&db).unwrap();
        let (cwd, last): (Option<String>, Option<String>) = conn
            .query_row(
                "SELECT cwd, last_assistant_text FROM sessions \
                 WHERE source = 'cursor' AND session_id = 'cur-store'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(cwd.as_deref(), Some("/work/app"));
        assert_eq!(last.as_deref(), Some("note.txt says hello"));
        let prompts: Vec<String> = conn
            .prepare(
                "SELECT prompt FROM history WHERE source = 'cursor' AND session_id = 'cur-store'",
            )
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(prompts, vec!["read note.txt".to_string()]);
        let results = crate::session_events(&conn, "cur-store", Some("cursor"))
            .unwrap()
            .into_iter()
            .filter(|event| event.kind == "tool_result")
            .count();
        assert_eq!(results, 1);

        // Unchanged store: nothing to do, nothing duplicated.
        let again = hydrate_session_at_with_home(&db, &options("cur-store"), dir.path()).unwrap();
        assert_eq!(again.status, "unchanged");
    }

    #[test]
    fn a_store_whose_chat_gained_a_transcript_is_not_hydrated_from_the_store() {
        let dir = tempfile::tempdir().unwrap();
        let store = write_store(dir.path(), "ws", "cur-store", "/w", &tool_chat("a"), 5).unwrap();
        let transcript = dir
            .path()
            .join(".cursor/projects/w/agent-transcripts/cur-store/cur-store.jsonl");
        fs::create_dir_all(transcript.parent().unwrap()).unwrap();
        fs::write(&transcript, "{}\n").unwrap();
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_store(&conn, "cur-store", &store);
        drop(conn);
        let error = hydrate_session_at_with_home(&db, &options("cur-store"), dir.path())
            .unwrap_err()
            .to_string();
        assert!(error.contains("SESSION_SOURCE_MISMATCH"), "{error}");
    }
}

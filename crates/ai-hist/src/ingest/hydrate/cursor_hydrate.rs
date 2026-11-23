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

/// Targeted hydration of a session Cursor kept only in one of its own stores
/// (the editor database or a chat store), or `None` for a transcript.
pub(super) fn store_only(
    conn: &Connection,
    options: &HydrateSessionOptions,
    path: &Path,
    records: i64,
) -> Option<Result<SelectedIngestResult>> {
    if crate::ingest::cursor_store::editor::is_locator(path) {
        return Some(ingest_editor_composer(conn, options, path));
    }
    crate::ingest::cursor_store::is_chat_store(path)
        .then(|| ingest_chat_store(conn, options, path, records))
}

/// Targeted hydration of a chat Cursor kept only in its chat store: the whole
/// chat, rebuilt, as the sync pass does. A store has no byte checkpoint.
pub(super) fn ingest_chat_store(
    conn: &Connection,
    options: &HydrateSessionOptions,
    path: &Path,
    records: i64,
) -> Result<SelectedIngestResult> {
    // A chat that has gained a transcript, or a newer copy in another
    // workspace, is no longer this store's; the row still names it until
    // discovery runs again.
    if crate::ingest::cursor_store::superseded(path)? {
        return Err(hydration_error(
            "SESSION_SOURCE_MISMATCH",
            format!(
                "Cursor catalog row points at the chat store {}, which no longer owns the \
                 chat (it has a transcript or a newer copy); run discoverSessions() again to \
                 re-establish its provenance",
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
    for diagnostic in &mut diagnostics {
        if diagnostic.code == "CURSOR_TIMESTAMP_FROM_MTIME" {
            diagnostic.code = "CURSOR_TIMESTAMP_FROM_STORE".to_string();
            diagnostic.message = "cursor records carry no timestamp field; events in turns with \
                                  no readable <timestamp> tag are stamped with the chat store's \
                                  recorded update time"
                .to_string();
        }
    }
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

/// The snapshot of a composer the Cursor editor kept only in its database,
/// or `None` when the row names something else. The locator must name the
/// editor database under this home: anything else is provenance a
/// rediscovery has to re-establish, never a path to open.
pub(super) fn editor_snapshot(
    options: &HydrateSessionOptions,
    target: &CatalogTarget,
    roots: &crate::ProviderRoots,
) -> Result<Option<SourceSnapshot>> {
    let Some(locator) = target.locator.as_deref() else {
        return Ok(None);
    };
    let Some((db, id)) = crate::ingest::cursor_store::editor::parse_locator(locator) else {
        return Ok(None);
    };
    if id != options.session_id
        || db != crate::ingest::cursor_store::editor::state_db_path(&roots.home)
    {
        return Err(hydration_error(
            "SESSION_SOURCE_MISMATCH",
            format!(
                "Cursor catalog row names {locator}, not this session in the editor database; \
                 run discoverSessions() again to re-establish its provenance"
            ),
        ));
    }
    let gone = || {
        hydration_error(
            "SESSION_SOURCE_UNAVAILABLE",
            format!(
                "Cursor editor conversation '{id}' is no longer in {}",
                db.display()
            ),
        )
    };
    if !db.is_file() {
        return Err(gone());
    }
    let stamp = crate::ingest::cursor_store::editor::composer_stamp(&db, id)?.ok_or_else(gone)?;
    Ok(Some(SourceSnapshot {
        // A transcript or chat store appearing changes no row of the
        // database, so whether it still owns the composer is in the stamp.
        stamp: format!(
            "{stamp}|superseded:{}",
            crate::ingest::cursor_store::editor::superseded(&roots.home, id)?
        ),
        bytes: 0,
        records: SnapshotRecords::Counted(0),
        path: Some(PathBuf::from(locator)),
        claude_transcript: None,
        devin_session: None,
        provider_session: None,
        claude_subagents: Vec::new(),
        scanned_bytes: 0,
        scanned_superseded: false,
        stamped_cursors: Vec::new(),
        opencode_layout: None,
        codex_relationship_complete: true,
    }))
}

/// Targeted hydration of a composer the Cursor editor kept only in its
/// database: the whole conversation, rebuilt, as the sync pass does.
pub(super) fn ingest_editor_composer(
    conn: &Connection,
    options: &HydrateSessionOptions,
    locator: &Path,
) -> Result<SelectedIngestResult> {
    let text = locator.to_string_lossy();
    let (db, id) = crate::ingest::cursor_store::editor::parse_locator(&text)
        .context("a Cursor editor locator that does not parse")?;
    let home = db
        .ancestors()
        .find(|dir| crate::ingest::cursor_store::editor::state_db_path(dir) == db)
        .map(Path::to_path_buf);
    if let Some(home) = home {
        if crate::ingest::cursor_store::editor::superseded(&home, id)? {
            return Err(hydration_error(
                "SESSION_SOURCE_MISMATCH",
                format!(
                    "Cursor catalog row points at the editor conversation {text}, which now has \
                     a transcript or chat store; run discoverSessions() again to re-establish \
                     its provenance"
                ),
            ));
        }
    }
    let composer =
        crate::ingest::cursor_store::editor::read_composer(&db, id)?.ok_or_else(|| {
            hydration_error(
                "SESSION_SOURCE_UNAVAILABLE",
                "Cursor editor conversation holds no messages",
            )
        })?;
    anyhow::ensure!(
        composer.session_id == options.session_id,
        "SESSION_SOURCE_MISMATCH: Cursor editor conversation identity does not match the catalog row"
    );
    let (_, records, outcome) =
        crate::ingest::cursor_store::editor::rebuild(conn, &text, &composer)?;
    let mut diagnostics = cursor_diagnostics(&outcome);
    for diagnostic in &mut diagnostics {
        if diagnostic.code == "CURSOR_TIMESTAMP_FROM_MTIME" {
            diagnostic.code = "CURSOR_TIMESTAMP_FROM_STORE".to_string();
            diagnostic.message = "some Cursor editor messages carry no creation time; they are \
                                  stamped with the conversation's recorded update time"
                .to_string();
        }
    }
    if composer.skipped_bubbles > 0 {
        diagnostics.push(HydrationDiagnostic {
            code: "CURSOR_EDITOR_BUBBLE_UNREADABLE".to_string(),
            message: format!(
                "{} message(s) the Cursor editor conversation lists were missing or not readable \
                 JSON and were skipped",
                composer.skipped_bubbles
            ),
            duration_ms: None,
            source_bytes: None,
            records_parsed: None,
        });
    }
    let outcome = IngestOutcome {
        records,
        bytes_read: i64::try_from(composer.transcript.len()).unwrap_or(i64::MAX),
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

    fn catalog_editor(conn: &Connection, session_id: &str, locator: &str) {
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

    #[test]
    fn a_cursor_editor_conversation_hydrates_from_the_editor_database() {
        use crate::ingest::cursor_store::editor::test_support::*;
        let dir = tempfile::tempdir().unwrap();
        let (path, writer) = open_state_db(dir.path()).unwrap();
        write_composer(
            &writer,
            "ed-1",
            "Read the note",
            "/work/app",
            &tool_conversation("read note.txt", 1_795_042_000_000),
            1_795_041_000_000,
        )
        .unwrap();
        close_idle(&path, writer).unwrap();
        let locator = crate::ingest::cursor_store::editor::locator(&path, "ed-1");
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_editor(&conn, "ed-1", &locator);
        drop(conn);

        let result = hydrate_session_at_with_home(&db, &options("ed-1"), dir.path()).unwrap();
        assert_eq!(result.status, "hydrated", "{:?}", result.diagnostics);
        assert_eq!(result.evidence.tool_calls, 2, "{:?}", result.evidence);
        let conn = open_db(&db).unwrap();
        let (cwd, title, last): (Option<String>, Option<String>, Option<String>) = conn
            .query_row(
                "SELECT cwd, title, last_assistant_text FROM sessions \
                 WHERE source = 'cursor' AND session_id = 'ed-1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(cwd.as_deref(), Some("/work/app"));
        assert_eq!(title.as_deref(), Some("Read the note"));
        assert_eq!(last.as_deref(), Some("note.txt says hello"));
        drop(conn);

        let again = hydrate_session_at_with_home(&db, &options("ed-1"), dir.path()).unwrap();
        assert_eq!(again.status, "unchanged");

        // A transcript appears: the row no longer names the composer's source.
        let transcript = dir
            .path()
            .join(".cursor/projects/w/agent-transcripts/ed-1/ed-1.jsonl");
        fs::create_dir_all(transcript.parent().unwrap()).unwrap();
        fs::write(&transcript, "{}\n").unwrap();
        let error = hydrate_session_at_with_home(&db, &options("ed-1"), dir.path())
            .unwrap_err()
            .to_string();
        assert!(error.contains("SESSION_SOURCE_MISMATCH"), "{error}");
    }

    #[test]
    fn an_editor_locator_for_another_database_is_not_opened() {
        let dir = tempfile::tempdir().unwrap();
        let elsewhere = dir.path().join("elsewhere/state.vscdb");
        let locator = crate::ingest::cursor_store::editor::locator(&elsewhere, "ed-1");
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_editor(&conn, "ed-1", &locator);
        drop(conn);
        let error = hydrate_session_at_with_home(&db, &options("ed-1"), dir.path())
            .unwrap_err()
            .to_string();
        assert!(error.contains("SESSION_SOURCE_MISMATCH"), "{error}");
    }

    #[test]
    fn an_older_copy_of_a_chat_is_not_hydrated() {
        let dir = tempfile::tempdir().unwrap();
        let older = write_store(dir.path(), "ws-a", "cur-store", "/w", &tool_chat("a"), 5).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(20));
        write_store(dir.path(), "ws-b", "cur-store", "/w", &tool_chat("b"), 6).unwrap();
        let db = dir.path().join("history.db");
        let conn = open_db(&db).unwrap();
        catalog_store(&conn, "cur-store", &older);
        drop(conn);
        let error = hydrate_session_at_with_home(&db, &options("cur-store"), dir.path())
            .unwrap_err()
            .to_string();
        assert!(error.contains("SESSION_SOURCE_MISMATCH"), "{error}");
    }
}

//! The cheap change stamp a hydration takes of a session's provider source
//! before deciding whether anything needs parsing.

use super::*;

pub(super) fn source_snapshot(
    conn: &Connection,
    options: &HydrateSessionOptions,
    target: &CatalogTarget,
    roots: &crate::ProviderRoots,
    claude_snapshot: Option<ClaudeTranscriptSnapshot>,
) -> Result<SourceSnapshot> {
    if let Some(refusal) = crate::sources::catalog::hydration_refusal(&options.source) {
        return Err(hydration_error("HYDRATION_UNSUPPORTED", refusal));
    }
    match options.source.as_str() {
        "opencode" => opencode_store_snapshot(options, target, roots),
        "devin" => devin_store_snapshot(options, target, roots),
        _ => file_source_snapshot(conn, options, target, roots, claude_snapshot),
    }
}

fn opencode_store_snapshot(
    options: &HydrateSessionOptions,
    target: &CatalogTarget,
    roots: &crate::ProviderRoots,
) -> Result<SourceSnapshot> {
    let locator = target.locator.as_deref().ok_or_else(|| {
        hydration_error(
            "SESSION_SOURCE_UNAVAILABLE",
            "OpenCode catalog row has no store provenance; run discoverSessions() again",
        )
    })?;
    let path = PathBuf::from(locator);
    let earlier = match opencode_source(&path, roots)? {
        OpencodeSource::TreeSession => return opencode_json_tree_snapshot(options, &path),
        OpencodeSource::Store { earlier } => earlier,
    };
    // A session held by more than one channel store belongs to the first
    // (discovery and sync both claim it there). If an earlier store has
    // gained this session since the row was cataloged, the row names a
    // superseded copy: sync now reads the earlier one, so hydrating this
    // one would import evidence sync disagrees with. An earlier store that
    // cannot be read claims nothing, exactly as in sync.
    for earlier in &earlier {
        if crate::ingest::opencode::sqlite_store_holds_session(earlier, &options.session_id)
            .unwrap_or(false)
        {
            return Err(opencode_superseded(&path, earlier));
        }
    }
    let src = Connection::open_with_flags(
        &path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_URI,
    )?;
    let columns = src
        .prepare("SELECT name FROM pragma_table_info('session')")?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<std::collections::BTreeSet<_>>>()?;
    let created = if columns.contains("time_created") {
        "time_created"
    } else {
        "NULL"
    };
    let updated = if columns.contains("time_updated") {
        "time_updated"
    } else {
        "NULL"
    };
    let stamp_sql = format!(
        "SELECT printf('%lld:%lld', COALESCE({created}, 0), \
            COALESCE({updated}, {created}, 0)) FROM session WHERE id = ?"
    );
    let stamp = src
        .query_row(&stamp_sql, [&options.session_id], |row| {
            row.get::<_, String>(0)
        })
        .optional()?
        .ok_or_else(|| {
            hydration_error(
                "SESSION_SOURCE_UNAVAILABLE",
                format!("OpenCode session '{}' no longer exists", options.session_id),
            )
        })?;
    Ok(SourceSnapshot {
        stamp,
        bytes: 0,
        // The ingestion query remains session-keyed. Avoid a second count
        // query here so checkpoint resolution never scans the provider's
        // complete `part` table on older stores missing its usual index.
        records: SnapshotRecords::Counted(0),
        path: Some(path),
        claude_transcript: None,
        devin_session: None,
        claude_subagents: Vec::new(),
        scanned_bytes: 0,
        scanned_superseded: false,
        stamped_cursors: Vec::new(),
        opencode_layout: Some(OpencodeIngestLayout::Sqlite),
        codex_relationship_complete: true,
    })
}

fn devin_store_snapshot(
    options: &HydrateSessionOptions,
    target: &CatalogTarget,
    roots: &crate::ProviderRoots,
) -> Result<SourceSnapshot> {
    let configured_dir = &roots.devin;
    let configured_path = crate::ingest::devin::sessions_db_path(configured_dir);
    let locator = target.locator.as_deref().ok_or_else(|| {
        hydration_error(
            "SESSION_SOURCE_UNAVAILABLE",
            "Devin catalog row has no store provenance; run discoverSessions() again",
        )
    })?;
    let path = PathBuf::from(locator);
    // A store that is gone is unavailable, not a provenance mismatch:
    // either the row names the configured path itself, or there is no
    // configured store left for it to have been superseded by.
    if !path.is_file() && (path == configured_path || !configured_path.is_file()) {
        return Err(hydration_error(
            "SESSION_SOURCE_UNAVAILABLE",
            format!("Devin source {} is unavailable", path.display()),
        ));
    }
    // The catalog locator is the store path itself, so a row can only
    // name the configured `sessions.db` — anything else is provenance a
    // rediscovery must re-establish, never a path to open.
    let resolved = fs::canonicalize(&path).ok();
    if resolved.is_none() || resolved != fs::canonicalize(&configured_path).ok() {
        return Err(hydration_error(
            "SESSION_SOURCE_MISMATCH",
            format!(
                "Devin catalog store {} does not match configured store {}",
                path.display(),
                configured_path.display()
            ),
        ));
    }
    let mut src = crate::store::open_db_readonly(&path)?;
    crate::ingest::devin::register_stamp_fn(&src)?;
    let snapshot = src.transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)?;
    let stamp = crate::ingest::devin::session_stamp(
        &snapshot,
        &options.session_id,
        &crate::ingest::devin::transcripts_dir(configured_dir),
    )?
    .ok_or_else(|| {
        hydration_error(
            "SESSION_SOURCE_UNAVAILABLE",
            format!(
                "Devin session '{}' no longer exists or is hidden",
                options.session_id
            ),
        )
    })?;
    snapshot.commit()?;
    Ok(SourceSnapshot {
        stamp,
        bytes: 0,
        // A changed pass replaces this with the count of the rows it
        // captures. An unchanged pass reads the stored checkpoint count.
        records: SnapshotRecords::Counted(0),
        path: Some(path),
        claude_transcript: None,
        devin_session: None,
        claude_subagents: Vec::new(),
        scanned_bytes: 0,
        scanned_superseded: false,
        stamped_cursors: Vec::new(),
        opencode_layout: None,
        codex_relationship_complete: true,
    })
}

/// The snapshot of a file-backed provider: the session's own source plus,
/// with related evidence, the child transcripts folded into its stamp.
fn file_source_snapshot(
    conn: &Connection,
    options: &HydrateSessionOptions,
    target: &CatalogTarget,
    roots: &crate::ProviderRoots,
    claude_snapshot: Option<ClaudeTranscriptSnapshot>,
) -> Result<SourceSnapshot> {
    let locator = target.locator.as_deref().ok_or_else(|| {
        hydration_error(
            "SESSION_SOURCE_UNAVAILABLE",
            "catalog row has no local provider locator; run discoverSessions() again",
        )
    })?;
    let path = PathBuf::from(locator);
    let captured_claude = captured_claude_snapshot(options, &path, roots, claude_snapshot)?;
    let (bytes, records, stamp) = file_source_inventory(options, &path, captured_claude.as_ref())?;
    let mut stamped = RelatedStamp {
        stamp,
        bytes,
        scanned_bytes: 0,
        stamped_cursors: Vec::new(),
    };
    if options.source == "codex" {
        // Identity comes from the rollout's first record, and reading it is
        // unavoidable work this hydration did.
        stamped.scanned_bytes += read_codex_session_meta_counted(&path)?.1 as i64;
    }
    let mut subagents = Vec::new();
    let mut scanned_superseded = false;
    if options.source == "claude" && options.include_related {
        (subagents, scanned_superseded) =
            stamped.fold_claude_subagents(conn, &path, &options.session_id)?;
    }
    let mut codex_relationship_complete = true;
    if options.source == "codex" && options.include_related {
        codex_relationship_complete =
            stamped.fold_codex_children(conn, &path, &options.session_id)?;
    }
    Ok(SourceSnapshot {
        stamp: stamped.stamp,
        bytes: stamped.bytes,
        records,
        path: Some(path),
        claude_transcript: captured_claude,
        devin_session: None,
        claude_subagents: subagents,
        scanned_bytes: stamped.scanned_bytes,
        scanned_superseded,
        stamped_cursors: stamped.stamped_cursors,
        // Every other provider is file-backed with one layout; only OpenCode
        // has a choice to record here.
        opencode_layout: None,
        codex_relationship_complete,
    })
}

/// The lifecycle hook's Claude bytes, checked against the catalog locator;
/// without them, the live source must still exist under its provider root.
fn captured_claude_snapshot(
    options: &HydrateSessionOptions,
    path: &Path,
    roots: &crate::ProviderRoots,
    claude_snapshot: Option<ClaudeTranscriptSnapshot>,
) -> Result<Option<ClaudeTranscriptSnapshot>> {
    let captured = if options.source == "claude" {
        claude_snapshot
    } else {
        anyhow::ensure!(
            claude_snapshot.is_none(),
            "SESSION_SOURCE_MISMATCH: Claude snapshot supplied for another provider"
        );
        None
    };
    if let Some(snapshot) = captured.as_ref() {
        anyhow::ensure!(
            snapshot.path == path,
            "SESSION_SOURCE_MISMATCH: Claude hook snapshot does not match the catalog locator"
        );
        // The hook validated this path against the configured Claude root
        // immediately before opening the snapshot. Do not canonicalize the
        // live path again here: rotation may remove it after the bytes were
        // safely captured, and those bytes are the evidence being hydrated.
    } else {
        if !path.is_file() {
            return Err(hydration_error(
                "SESSION_SOURCE_UNAVAILABLE",
                format!(
                    "provider source {} disappeared after discovery",
                    path.display()
                ),
            ));
        }
        validate_provider_path(&options.source, path, roots)?;
    }
    Ok(captured)
}

/// The session source's own size, record count and change stamp.
///
/// Grok's source is a directory, not a file. Its inventory is metadata
/// only — the same walk as its change stamp, so the two can never describe
/// different sets of files — and the record count is deferred, because
/// taking it means reading an update stream that is routinely megabytes
/// and a run that decides nothing changed has no use for it.
fn file_source_inventory(
    options: &HydrateSessionOptions,
    path: &Path,
    captured_claude: Option<&ClaudeTranscriptSnapshot>,
) -> Result<(i64, SnapshotRecords, String)> {
    Ok(if options.source == "grok" {
        let inventory = grok_source_inventory(path)?;
        (
            inventory.bytes,
            SnapshotRecords::DeferredGrok(path.to_path_buf()),
            inventory.stamp,
        )
    } else if options.source == "muse" && options.include_related {
        // With related evidence, a Muse session is its transcript plus the
        // subagent logs beside it; the stamp and the counts cover all of them,
        // so a child that grew after the parent's last record is still a
        // change. Without it, the transcript alone, like any other file.
        let mut bytes = 0i64;
        let mut records = 0i64;
        for file in muse_session_files(path)? {
            bytes += file.metadata()?.len() as i64;
            records += complete_jsonl_records(&file)?;
        }
        (
            bytes,
            SnapshotRecords::Counted(records),
            muse_session_stamp(path)?,
        )
    } else if let Some(snapshot) = captured_claude {
        // The hook already read these bytes; nothing here opens the file.
        (
            snapshot.text.len() as i64,
            SnapshotRecords::Counted(snapshot.records()),
            snapshot.stamp.clone(),
        )
    } else if matches!(options.source.as_str(), "claude" | "codex") {
        // Counted by the pass that parses, not by a walk over the whole file
        // before it: that walk is exactly the cost cursors remove.
        (
            path.metadata()?.len() as i64,
            SnapshotRecords::Counted(0),
            file_stamp(path)?,
        )
    } else {
        (
            path.metadata()?.len() as i64,
            SnapshotRecords::Counted(complete_jsonl_records(path)?),
            file_stamp(path)?,
        )
    })
}

/// A file-backed stamp and its byte totals as related files fold into it.
struct RelatedStamp {
    stamp: String,
    bytes: i64,
    scanned_bytes: i64,
    stamped_cursors: Vec<(String, PathBuf)>,
}

impl RelatedStamp {
    /// Fold in the session's Claude subagent sidecars and their metadata;
    /// returns the sidecars and whether any walk over them was superseded.
    fn fold_claude_subagents(
        &mut self,
        conn: &Connection,
        path: &Path,
        session_id: &str,
    ) -> Result<(Vec<ClaudeSubagentEvidence>, bool)> {
        let (subagents, sidecar_bytes, sidecar_superseded) =
            claude_subagents(conn, path, session_id)?;
        self.scanned_bytes += sidecar_bytes as i64;
        for evidence in &subagents {
            crate::ingest::check_capture_cancelled()?;
            self.stamp.push('|');
            self.stamp.push_str(&file_stamp(&evidence.path)?);
            self.bytes += evidence.path.metadata()?.len() as i64;
            self.stamped_cursors
                .push(("claude".to_string(), evidence.path.clone()));
            // The metadata sidecar describes the child — its type, model and
            // spawn depth, and the tool use that started it — so a sidecar
            // that arrives or changes on its own is still new evidence.
            let metadata = claude_subagent_meta_path(&evidence.path);
            if metadata.is_file() {
                self.stamp.push('|');
                self.stamp.push_str(&file_stamp(&metadata)?);
                self.bytes += metadata.metadata()?.len() as i64;
                self.stamped_cursors.push((
                    crate::ingest::CLAUDE_SUBAGENT_META_SOURCE.to_string(),
                    metadata,
                ));
            }
        }
        Ok((subagents, sidecar_superseded))
    }

    /// Fold in the session's Codex child rollouts; returns whether the
    /// bounded child search covered every relationship.
    fn fold_codex_children(
        &mut self,
        conn: &Connection,
        path: &Path,
        session_id: &str,
    ) -> Result<bool> {
        let complete = match path.parent() {
            Some(directory) => codex_child_scan_complete(directory)?,
            None => false,
        };
        if !complete {
            self.stamp.push_str("|codex-relationships-limited");
        }
        // Finding the children means reading one head record from every
        // sibling rollout in the directory, whether or not it turns out to be
        // one. That is provider I/O this hydration did.
        let (children, enumeration_bytes) = codex_children_counted(conn, path, session_id)?;
        self.scanned_bytes += enumeration_bytes as i64;
        for child in children {
            crate::ingest::check_capture_cancelled()?;
            self.stamp.push('|');
            self.stamp.push_str(&file_stamp(&child)?);
            self.bytes += child.metadata()?.len() as i64;
            self.stamped_cursors.push(("codex".to_string(), child));
        }
        Ok(complete)
    }
}

//! The names Codex gives its threads.
//!
//! Codex keeps a thread's name out of the rollout: a rename (or the name the
//! app generates after the first turn) appends `{"id", "thread_name",
//! "updated_at"}` to `session_index.jsonl` beside the rollout trees, and the
//! last line for an id is its current name. A rename therefore moves this file
//! and not the rollout, so the names are applied in a pass of their own rather
//! than by the rollout reader, whose stamp would not notice.

use super::*;

/// The sync-state key holding the stamp of the index the last pass applied.
const CODEX_SESSION_INDEX_STATE_KEY: &str = "codex_session_index";

pub(crate) fn session_index_path(root: &Path) -> PathBuf {
    root.join("session_index.jsonl")
}

/// The current name of every thread the index names, last line wins. A line
/// that does not parse, or names no thread, is skipped: the index is a cache
/// Codex rebuilds, never evidence worth failing a sweep over.
pub(crate) fn read_thread_names(path: &Path) -> Result<HashMap<String, String>> {
    let reader = BufReader::new(fs::File::open(path)?);
    let mut names = HashMap::new();
    for line in reader.lines() {
        let line = line?;
        let Ok(value) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        let (Some(id), Some(name)) = (
            value
                .get("id")
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty()),
            value
                .get("thread_name")
                .and_then(Value::as_str)
                .map(crate::discover::excerpt)
                .filter(|name| !name.is_empty()),
        ) else {
            continue;
        };
        names.insert(id.to_string(), name);
    }
    Ok(names)
}

/// Sets `title` on the Codex sessions the index names, and returns how many
/// rows changed.
///
/// Reads the index when it moved since the last pass, or when this sweep
/// touched a Codex session that still has no title (a thread named before its
/// rollout was first catalogued has a row to name only now). An unmoved index
/// is not opened for a session that already has its name: the bytes cannot
/// have changed, and re-parsing the whole index on every rollout append is
/// the cost this skips. An unchanged title is not rewritten, so a read that
/// does happen does not restamp the row.
pub(crate) fn apply_codex_thread_names(
    conn: &Connection,
    state: &mut Map<String, Value>,
    root: &Path,
    touched: &HashSet<String>,
) -> Result<usize> {
    let path = session_index_path(root);
    if !path.exists() {
        return Ok(0);
    }
    let stamp = file_stamp(&path)?;
    let moved = state
        .get(CODEX_SESSION_INDEX_STATE_KEY)
        .and_then(Value::as_str)
        != Some(&stamp);
    if !moved && !touched_session_needs_name(conn, touched)? {
        return Ok(0);
    }
    let names = read_thread_names(&path)?;
    let mut changed = 0;
    for (id, name) in &names {
        if (moved || touched.contains(id)) && set_session_title(conn, "codex", id, Some(name))? {
            changed += 1;
        }
    }
    if moved {
        state.insert(CODEX_SESSION_INDEX_STATE_KEY.to_string(), json!(stamp));
    }
    Ok(changed)
}

/// Whether any touched session still has no title, so the unmoved index has
/// a name to apply. A session that already has one was named from this same
/// index; the rollout reader does not write titles.
fn touched_session_needs_name(conn: &Connection, touched: &HashSet<String>) -> Result<bool> {
    let mut untitled = conn.prepare_cached(
        "SELECT 1 FROM sessions WHERE source = 'codex' AND session_id = ?1 AND title IS NULL",
    )?;
    for id in touched {
        if untitled.exists(params![id])? {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn title(conn: &Connection, id: &str) -> Option<String> {
        conn.query_row(
            "SELECT title FROM sessions WHERE source = 'codex' AND session_id = ?1",
            [id],
            |row| row.get(0),
        )
        .unwrap()
    }

    #[test]
    fn the_last_name_for_a_thread_wins_and_an_unmoved_index_is_not_reread() {
        let dir = tempfile::tempdir().unwrap();
        let conn = crate::open_db(&dir.path().join("ai-history.db")).unwrap();
        conn.execute_batch(
            "INSERT INTO sessions (session_id, source) VALUES ('t1', 'codex'), ('t2', 'codex');",
        )
        .unwrap();
        let root = dir.path().join("codex");
        fs::create_dir_all(&root).unwrap();
        fs::write(
            session_index_path(&root),
            concat!(
                "{\"id\":\"t1\",\"thread_name\":\"First\",\"updated_at\":\"2026-10-01T00:00:00Z\"}\n",
                "not json\n",
                "{\"id\":\"t2\",\"thread_name\":\"  \"}\n",
                "{\"id\":\"t1\",\"thread_name\":\"Renamed\",\"updated_at\":\"2026-10-02T00:00:00Z\"}\n",
            ),
        )
        .unwrap();
        let mut state = Map::new();

        let changed = apply_codex_thread_names(&conn, &mut state, &root, &HashSet::new()).unwrap();
        assert_eq!(changed, 1);
        assert_eq!(title(&conn, "t1").as_deref(), Some("Renamed"));
        assert_eq!(title(&conn, "t2"), None);

        // Unmoved, and nothing touched: no read, nothing written.
        conn.execute(
            "UPDATE sessions SET title = NULL WHERE session_id = 't1'",
            [],
        )
        .unwrap();
        apply_codex_thread_names(&conn, &mut state, &root, &HashSet::new()).unwrap();
        assert_eq!(title(&conn, "t1"), None);

        // A touched session is named even though the index did not move.
        let touched = HashSet::from(["t1".to_string()]);
        let changed = apply_codex_thread_names(&conn, &mut state, &root, &touched).unwrap();
        assert_eq!(changed, 1);
        assert_eq!(title(&conn, "t1").as_deref(), Some("Renamed"));
    }

    /// The index bytes did not change and the session already has the name
    /// they carry, so the file is not opened. Mode `0o000` makes a read fail;
    /// a pass that still parsed the index would return that error.
    #[cfg(unix)]
    #[test]
    fn an_unmoved_index_is_not_opened_for_a_session_that_already_has_its_name() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let conn = crate::open_db(&dir.path().join("ai-history.db")).unwrap();
        conn.execute(
            "INSERT INTO sessions (session_id, source) VALUES ('t1', 'codex')",
            [],
        )
        .unwrap();
        let root = dir.path().join("codex");
        fs::create_dir_all(&root).unwrap();
        let index = session_index_path(&root);
        fs::write(
            &index,
            "{\"id\":\"t1\",\"thread_name\":\"Named\",\"updated_at\":\"2026-10-01T00:00:00Z\"}\n",
        )
        .unwrap();
        let mut state = Map::new();
        apply_codex_thread_names(&conn, &mut state, &root, &HashSet::new()).unwrap();
        assert_eq!(title(&conn, "t1").as_deref(), Some("Named"));

        fs::set_permissions(&index, fs::Permissions::from_mode(0o0)).unwrap();
        let touched = HashSet::from(["t1".to_string()]);
        let changed = apply_codex_thread_names(&conn, &mut state, &root, &touched).unwrap();
        assert_eq!(changed, 0);
        assert_eq!(title(&conn, "t1").as_deref(), Some("Named"));
    }
}

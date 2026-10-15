//! The paginated Codex history, used only when its rollout is unavailable or incomplete.
use anyhow::{ensure, Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use crate::discover::{Candidate, ScanEnv, ShallowSession};
use crate::ingest::{EventRow, RawMessageFacts, SessionCatalogRow, ToolCallRef};
use crate::providers::{sqlite_snapshot, AcquiredSession, AcquisitionContext};
use crate::{HistoryEntry, NewSessionMarker};

const MAX_THREADS: usize = 100_000;
const ITEM_PAGE: i64 = 512;
const MAX_ITEMS: usize = 100_000;

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Thread {
    pub(crate) id: String,
    pub(super) rollout_path: Option<String>,
    pub(super) created: i64,
    pub(super) updated: i64,
    pub(super) source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) thread_source: Option<String>,
    pub(super) cwd: Option<String>,
    pub(super) branch: Option<String>,
    pub(super) repo: Option<String>,
    pub(super) title: Option<String>,
    pub(super) name: Option<String>,
    pub(super) first_user_message: Option<String>,
    pub(super) originator: Option<String>,
    pub(super) version: Option<String>,
    pub(super) model: Option<String>,
    pub(super) tokens_used: Option<i64>,
    pub(super) archived: bool,
    pub(super) agent_nickname: Option<String>,
    pub(super) agent_role: Option<String>,
    pub(super) next_ordinal: i64,
    pub(super) next_offset: i64,
}

impl Thread {
    fn is_child(&self) -> bool {
        let source = self
            .source
            .as_deref()
            .and_then(|s| serde_json::from_str::<Value>(s).ok());
        crate::ingest::codex_is_subagent(
            Some(&json!({"source": source, "thread_source": self.thread_source})),
            &self.id,
        )
    }
    fn stamp(&self) -> String {
        format!(
            "sqlite:{}:{}:{}",
            self.updated, self.next_ordinal, self.next_offset
        )
    }
    fn rollout_complete(&self) -> bool {
        self.rollout_path.as_deref().is_some_and(|path| {
            crate::ingest::transcript_file::metadata(Path::new(path))
                .is_ok_and(|meta| meta.is_file() && meta.len() >= self.next_offset as u64)
        })
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) struct Capture {
    pub(crate) thread: Thread,
    pub(super) items: Vec<Item>,
    pub(super) turns: Vec<Turn>,
    pub(super) edges: Vec<Edge>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) related: Vec<Capture>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) related_rollouts: Vec<RelatedRollout>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub(super) related_incomplete: bool,
}

fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Serialize, Deserialize)]
pub(crate) struct RelatedRollout {
    pub(crate) id: String,
    pub(crate) path: PathBuf,
    pub(super) stamp: String,
}

#[derive(Serialize, Deserialize)]
pub(super) struct Item {
    pub(super) id: String,
    pub(super) turn_id: Option<String>,
    pub(super) ts: i64,
    pub(super) kind: String,
    pub(super) value: Value,
}

#[derive(Serialize, Deserialize)]
pub(super) struct Turn {
    pub(super) id: String,
    pub(super) ts: Option<i64>,
    pub(super) error: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub(super) struct Edge {
    pub(super) parent: String,
    pub(super) child: String,
    pub(super) status: Option<String>,
}

pub(crate) struct DatabaseSelection {
    pub(crate) pair: Option<(PathBuf, PathBuf)>,
    pub(crate) diagnostic: Option<String>,
}

/// Only the known schema pair is admitted. A newer or incomplete pair is
/// skipped so an unrelated Codex rollout remains discoverable and ingestible.
pub(crate) fn database_selection(root: &Path) -> Result<DatabaseSelection> {
    type VersionedPath = (Option<PathBuf>, Option<String>);
    fn versioned(root: &Path, prefix: &str, expected: u32) -> Result<VersionedPath> {
        if !root.exists() {
            return Ok((None, None));
        }
        let mut found = None;
        let mut unsupported = None;
        for entry in std::fs::read_dir(root)? {
            let name = entry?.file_name();
            let Some(name) = name.to_str() else { continue };
            let Some(version) = name
                .strip_prefix(prefix)
                .and_then(|s| s.strip_suffix(".sqlite"))
            else {
                continue;
            };
            let Ok(version) = version.parse::<u32>() else {
                continue;
            };
            if version > expected {
                unsupported = Some(format!(
                    "unsupported Codex {prefix}{version}.sqlite; supported version: {expected}"
                ));
            }
            if version == expected {
                found = Some(root.join(name));
            }
        }
        Ok((found, unsupported))
    }
    let (state, state_warning) = versioned(root, "state_", 5)?;
    let (history, history_warning) = versioned(root, "thread_history_", 1)?;
    let diagnostic = state_warning.or(history_warning);
    let (pair, diagnostic) = match (state, history, diagnostic) {
        (_, _, Some(warning)) => (None, Some(warning)),
        (None, None, None) => (None, None),
        (Some(state), Some(history), None) => (Some((state, history)), None),
        _ => (
            None,
            Some(format!(
                "incomplete Codex SQLite history pair under {}",
                root.display()
            )),
        ),
    };
    Ok(DatabaseSelection { pair, diagnostic })
}

pub(crate) fn databases(root: &Path) -> Result<Option<(PathBuf, PathBuf)>> {
    Ok(database_selection(root)?.pair)
}

/// Refresh a saved rollout observation against the current projection. Query
/// only this thread and its checkpoint; capture reads items after selection.
pub(crate) fn current_locator(
    roots: &crate::ProviderRoots,
    id: &str,
    observed: &str,
) -> Result<Option<String>> {
    let Some((state, history)) = databases(&roots.codex)? else {
        return Ok(None);
    };
    let context = AcquisitionContext::new("codex", roots);
    let rollout_path: Option<Option<String>> = sqlite_snapshot(&context, &state, |db| {
        db.query_row(
            "SELECT rollout_path FROM threads WHERE id=?1 AND history_mode='paginated'",
            [id],
            |row| row.get(0),
        )
        .optional()
        .map_err(Into::into)
    })?;
    let Some(rollout_path) = rollout_path else {
        return Ok(None);
    };
    // Catalog ownership wins when another live rollout uses this session id.
    // Only the observed file's own projection may replace it with SQLite.
    if crate::ingest::transcript_file::exists(Path::new(observed))
        && rollout_path
            .as_deref()
            .is_none_or(|path| Path::new(path) != Path::new(observed))
    {
        return Ok(None);
    }
    let offset: Option<i64> = sqlite_snapshot(&context, &history, |db| {
        db.query_row(
            "SELECT next_rollout_byte_offset FROM thread_history_projection_state WHERE thread_id=?1",
            [id],
            |row| row.get(0),
        )
        .optional()
        .map_err(Into::into)
    })?;
    let Some(offset) = offset else {
        return Ok(None);
    };
    ensure!(offset >= 0, "invalid Codex rollout projection offset");
    if let Some(path) = rollout_path {
        let path = PathBuf::from(path);
        if crate::ingest::transcript_file::metadata(&path)
            .is_ok_and(|metadata| metadata.is_file() && metadata.len() >= offset as u64)
        {
            context.validate(&path)?;
            return Ok(Some(path.to_string_lossy().into_owned()));
        }
    }
    Ok(Some(locator(&state, id)))
}

/// A cataloged rollout yields to SQLite only when that same thread's
/// projection extends beyond the rollout. A sibling with the same id retains
/// catalog ownership. A malformed or unreadable locator cannot take it over.
pub(crate) fn incomplete_projection_owns_rollout(incoming: &str, observed: &str) -> bool {
    let Some((state, id)) = parse_locator(incoming) else {
        return false;
    };
    if state
        .file_name()
        .is_none_or(|name| name != "state_5.sqlite")
    {
        return false;
    }
    let Some(root) = state.parent() else {
        return false;
    };
    let observed_path = Path::new(observed);
    if !observed_path.starts_with(root.join("sessions"))
        && !observed_path.starts_with(root.join("archived_sessions"))
    {
        return false;
    }
    let Some(home) = root.parent() else {
        return false;
    };
    let mut roots = crate::ProviderRoots::from_home(
        home.to_path_buf(),
        home.join(".local/share/opencode/opencode.db"),
    );
    roots.codex = root.to_path_buf();
    current_locator(&roots, id, observed).is_ok_and(|current| current.as_deref() == Some(incoming))
}

fn projection(
    context: &AcquisitionContext<'_>,
    path: &Path,
) -> Result<HashMap<String, (i64, i64)>> {
    sqlite_snapshot(context, path, |db| {
        let mut stmt = db.prepare("SELECT thread_id,next_rollout_ordinal,next_rollout_byte_offset FROM thread_history_projection_state LIMIT 100001")?;
        let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get(1)?, r.get(2)?)))?;
        let mut map = HashMap::new();
        for row in rows {
            let (id, ordinal, offset) = row?;
            ensure!(
                map.len() < MAX_THREADS,
                "Codex projection exceeds safety limit"
            );
            map.insert(id, (ordinal, offset));
        }
        Ok(map)
    })
}

fn thread_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<Thread> {
    Ok(Thread {
        id: r.get(0)?,
        rollout_path: r.get(1)?,
        created: r.get(2)?,
        updated: r.get(3)?,
        source: r.get(4)?,
        thread_source: r.get(18)?,
        cwd: r.get(5)?,
        branch: r.get(6)?,
        repo: r.get(7)?,
        title: r.get(8)?,
        name: r.get(9)?,
        first_user_message: r.get(10)?,
        originator: r.get(11)?,
        version: r.get(12)?,
        model: r.get(13)?,
        tokens_used: r.get(14)?,
        archived: r.get::<_, i64>(15)? != 0,
        agent_nickname: r.get(16)?,
        agent_role: r.get(17)?,
        next_ordinal: 0,
        next_offset: 0,
    })
}

const THREAD_SQL: &str = "SELECT id,rollout_path,created_at_ms,updated_at_ms,source,cwd,git_branch,git_origin_url,title,name,first_user_message,originator,cli_version,model,tokens_used,archived,agent_nickname,agent_role,thread_source FROM threads WHERE history_mode='paginated'";

fn threads(context: &AcquisitionContext<'_>, state: &Path, history: &Path) -> Result<Vec<Thread>> {
    let offsets = projection(context, history)?;
    sqlite_snapshot(context, state, |db| {
        let mut stmt = db.prepare(&format!("{THREAD_SQL} LIMIT 100001"))?;
        let mut out = Vec::new();
        for (scanned, row) in stmt.query_map([], thread_row)?.enumerate() {
            ensure!(
                scanned < MAX_THREADS,
                "Codex thread discovery exceeds safety limit"
            );
            let mut thread = row?;
            if let Some((ordinal, offset)) = offsets.get(&thread.id) {
                thread.next_ordinal = *ordinal;
                thread.next_offset = *offset;
            }
            if !thread.rollout_complete() {
                out.push(thread);
            }
        }
        Ok(out)
    })
}

pub(crate) fn locator(state: &Path, id: &str) -> String {
    format!("sqlite:{}#{id}", state.display())
}

pub(crate) fn parse_locator(locator: &str) -> Option<(PathBuf, &str)> {
    let (path, id) = locator.strip_prefix("sqlite:")?.rsplit_once('#')?;
    (!id.is_empty()).then(|| (PathBuf::from(path), id))
}

mod discovery;
pub(crate) use discovery::{enumerate, fingerprint, shallow};

fn items(db: &Connection, id: &str) -> Result<(Vec<Item>, Vec<Turn>)> {
    let mut items = Vec::new();
    let mut after = -1_i64;
    loop {
        let mut stmt = db.prepare("SELECT item_id,turn_id,rollout_ordinal,created_at_ms,item_type,item_json FROM thread_items WHERE thread_id=?1 AND rollout_ordinal>?2 ORDER BY rollout_ordinal LIMIT ?3")?;
        let page = stmt
            .query_map(params![id, after, ITEM_PAGE], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get(1)?,
                    r.get::<_, i64>(2)?,
                    r.get(3)?,
                    r.get::<_, String>(4)?,
                    r.get::<_, String>(5)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        if page.is_empty() {
            break;
        }
        for (item_id, turn_id, ordinal, ts, kind, raw) in page {
            ensure!(
                items.len() < MAX_ITEMS,
                "Codex thread item capture exceeds safety limit"
            );
            let value: Value = serde_json::from_str(&raw).context("malformed Codex thread item")?;
            ensure!(
                value.get("id").and_then(Value::as_str) == Some(item_id.as_str()),
                "Codex item ID mismatch"
            );
            after = ordinal;
            items.push(Item {
                id: item_id,
                turn_id,
                ts,
                kind,
                value,
            });
        }
    }
    let mut stmt = db.prepare("SELECT turn_id,started_at,error_json FROM thread_turns WHERE thread_id=? ORDER BY rollout_ordinal LIMIT 100001")?;
    let turns = stmt
        .query_map([id], |r| {
            Ok(Turn {
                id: r.get(0)?,
                ts: r.get(1)?,
                error: r.get(2)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    ensure!(
        turns.len() <= MAX_ITEMS,
        "Codex turn capture exceeds safety limit"
    );
    Ok((items, turns))
}

fn thread_and_edges(
    context: &AcquisitionContext<'_>,
    state: &Path,
    id: &str,
) -> Result<(Thread, Vec<Edge>)> {
    sqlite_snapshot(context, state, |db| {
        let thread = db
            .query_row(&format!("{THREAD_SQL} AND id=?1"), [id], thread_row)
            .optional()?
            .context("SESSION_SOURCE_UNAVAILABLE: Codex thread missing")?;
        let mut stmt = db.prepare("SELECT parent_thread_id,child_thread_id,status FROM thread_spawn_edges WHERE parent_thread_id=?1 OR child_thread_id=?1 ORDER BY parent_thread_id,child_thread_id LIMIT 100001")?;
        let edges = stmt
            .query_map([id], |r| {
                Ok(Edge {
                    parent: r.get(0)?,
                    child: r.get(1)?,
                    status: r.get(2)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        ensure!(
            edges.len() <= MAX_THREADS,
            "Codex edges exceed safety limit"
        );
        Ok((thread, edges))
    })
}

pub(crate) fn capture(
    roots: &crate::ProviderRoots,
    state: &Path,
    id: &str,
) -> Result<AcquiredSession> {
    let (current_state, history) =
        databases(&roots.codex)?.context("Codex SQLite history unavailable")?;
    ensure!(
        std::fs::canonicalize(state)? == std::fs::canonicalize(&current_state)?,
        "SESSION_SOURCE_MISMATCH: stale Codex SQLite locator"
    );
    let context = AcquisitionContext::new("codex", roots);
    let (mut thread, edges) = thread_and_edges(&context, state, id)?;
    let (offset, items, turns) = sqlite_snapshot(&context, &history, |db| {
        let offset = db
            .query_row("SELECT next_rollout_ordinal,next_rollout_byte_offset FROM thread_history_projection_state WHERE thread_id=?1", [id], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)))
            .optional()?;
        let (items, turns) = items(db, id)?;
        Ok((offset, items, turns))
    })?;
    if let Some((ordinal, offset)) = offset {
        thread.next_ordinal = ordinal;
        thread.next_offset = offset;
    }
    ensure!(
        !thread.rollout_complete(),
        "SESSION_SOURCE_MISMATCH: Codex rollout is complete"
    );
    let records = (items.len() + turns.len()) as i64;
    let payload = serde_json::to_value(Capture {
        thread,
        items,
        turns,
        edges,
        related: Vec::new(),
        related_rollouts: Vec::new(),
        related_incomplete: false,
    })?;
    let mut read_paths = vec![state.to_path_buf(), history.clone()];
    for path in [state, history.as_path()] {
        for suffix in ["-wal", "-shm", "-journal"] {
            let sidecar = PathBuf::from(format!("{}{suffix}", path.display()));
            if sidecar.exists() {
                context.validate(&sidecar)?;
                read_paths.push(sidecar);
            }
        }
    }
    Ok(AcquiredSession::new(payload, 0, records, read_paths))
}

mod related;
pub(crate) use related::{capture_related, restored_rollout};

mod normalize;
pub(crate) use normalize::{
    claim_history_log_prompt, normalize, normalize_with_relationships, retire, retire_if_changed,
};

pub(crate) fn decode(acquired: &AcquiredSession) -> Result<Capture> {
    serde_json::from_value(acquired.payload.clone()).map_err(Into::into)
}

pub(crate) fn sync(
    conn: &Connection,
    state: &mut serde_json::Map<String, Value>,
    root: &Path,
    repairs: &crate::ingest::SweepRepairs,
) -> Result<(usize, Vec<String>)> {
    let Some((state_db, history)) = databases(root)? else {
        return Ok((0, Vec::new()));
    };
    let home = root.parent().unwrap_or(root).to_path_buf();
    let mut roots = crate::ProviderRoots::from_home(
        home.clone(),
        home.join(".local/share/opencode/opencode.db"),
    );
    roots.codex = root.to_path_buf();
    let context = AcquisitionContext::new("codex", &roots);
    let mut stamps = state
        .get("codex_sqlite_v2")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let candidates = threads(&context, &state_db, &history)?;
    let active: HashSet<&str> = candidates.iter().map(|t| t.id.as_str()).collect();
    stamps.retain(|id, _| active.contains(id.as_str()));
    let mut inserted = 0;
    let mut read = Vec::new();
    for t in candidates {
        crate::ingest::check_capture_cancelled()?;
        let stamp = t.stamp();
        if stamps.get(&t.id).and_then(Value::as_str) == Some(&stamp)
            && !repairs.contains("codex", &t.id)
        {
            continue;
        }
        let result = (|| -> Result<usize> {
            let acquired = capture(&roots, &state_db, &t.id)?;
            let captured = decode(&acquired)?;
            let tx = conn.unchecked_transaction()?;
            retire_if_changed(&tx, &captured, true)?;
            let count = normalize(&tx, &captured, &state_db)?;
            tx.commit()?;
            Ok(count)
        })();
        match result {
            Ok(count) => inserted += count,
            Err(error) if error.is::<crate::ingest::CaptureCancelled>() => return Err(error),
            Err(error) => {
                eprintln!(
                    "ai-hist: [codex] skipping SQLite thread {}: {error:#}",
                    t.id
                );
                continue;
            }
        }
        read.push(t.id.clone());
        stamps.insert(t.id, json!(stamp));
    }
    state.insert("codex_sqlite_v2".into(), Value::Object(stamps));
    Ok((inserted, read))
}

#[cfg(test)]
mod tests;

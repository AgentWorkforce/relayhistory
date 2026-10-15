//! The paginated Codex history, used only when its rollout is unavailable or incomplete.
use anyhow::{bail, ensure, Context, Result};
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
    pub(super) id: String,
    pub(super) rollout_path: Option<String>,
    pub(super) created: i64,
    pub(super) updated: i64,
    pub(super) source: Option<String>,
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
        self.source
            .as_deref()
            .is_some_and(|s| s.contains("\"subagent\""))
    }
    fn stamp(&self) -> String {
        format!(
            "sqlite:{}:{}:{}",
            self.updated, self.next_ordinal, self.next_offset
        )
    }
    fn rollout_complete(&self) -> bool {
        self.rollout_path.as_deref().is_some_and(|path| {
            std::fs::metadata(path).is_ok_and(|meta| meta.len() >= self.next_offset as u64)
        })
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) struct Capture {
    pub(super) thread: Thread,
    pub(super) items: Vec<Item>,
    pub(super) turns: Vec<Turn>,
    pub(super) edges: Vec<Edge>,
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

/// Only the known schema pair is admitted. A higher version is a new contract.
pub(crate) fn databases(root: &Path) -> Result<Option<(PathBuf, PathBuf)>> {
    fn versioned(root: &Path, prefix: &str, expected: u32) -> Result<Option<PathBuf>> {
        if !root.exists() {
            return Ok(None);
        }
        let mut found = None;
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
                bail!("unsupported Codex {prefix}{version}.sqlite; supported version: {expected}");
            }
            if version == expected {
                found = Some(root.join(name));
            }
        }
        Ok(found)
    }
    match (
        versioned(root, "state_", 5)?,
        versioned(root, "thread_history_", 1)?,
    ) {
        (None, None) => Ok(None),
        (Some(state), Some(history)) => Ok(Some((state, history))),
        _ => bail!(
            "incomplete Codex SQLite history pair under {}",
            root.display()
        ),
    }
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

const THREAD_SQL: &str = "SELECT id,rollout_path,created_at_ms,updated_at_ms,source,cwd,git_branch,git_origin_url,title,name,first_user_message,originator,cli_version,model,tokens_used,archived,agent_nickname,agent_role FROM threads WHERE history_mode='paginated'";

fn threads(context: &AcquisitionContext<'_>, state: &Path, history: &Path) -> Result<Vec<Thread>> {
    let offsets = projection(context, history)?;
    sqlite_snapshot(context, state, |db| {
        let mut stmt = db.prepare(&format!("{THREAD_SQL} LIMIT 100001"))?;
        let mut out = Vec::new();
        let mut scanned = 0;
        for row in stmt.query_map([], thread_row)? {
            ensure!(
                scanned < MAX_THREADS,
                "Codex thread discovery exceeds safety limit"
            );
            scanned += 1;
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

pub(crate) fn enumerate(
    root: &Path,
    roots: &crate::ProviderRoots,
) -> Result<(Vec<Candidate>, HashSet<PathBuf>)> {
    let Some((state, history)) = databases(root)? else {
        return Ok((Vec::new(), HashSet::new()));
    };
    let context = AcquisitionContext::new("codex", roots);
    let threads = threads(&context, &state, &history)?;
    let incomplete_rollouts = threads
        .iter()
        .filter_map(|t| t.rollout_path.as_ref().map(PathBuf::from))
        .collect();
    let candidates = threads
        .into_iter()
        .filter(|t| !t.is_child())
        .map(|t| Candidate {
            source: "codex",
            locator: locator(&state, &t.id),
            session_id: Some(t.id.clone()),
            recency_hint_ms: Some(t.updated),
            stamp: t.stamp(),
        })
        .collect();
    Ok((candidates, incomplete_rollouts))
}

pub(crate) fn fingerprint(root: &Path) -> Result<Vec<Candidate>> {
    let Some((state, history)) = databases(root)? else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    for path in [state, history] {
        for suffix in ["", "-wal", "-journal"] {
            let sidecar = PathBuf::from(format!("{}{suffix}", path.display()));
            if sidecar.exists() {
                let (stamp, recency_hint_ms) = crate::file_stamp_and_modified(&sidecar)?;
                out.push(Candidate {
                    source: "codex",
                    locator: sidecar.to_string_lossy().into_owned(),
                    session_id: None,
                    recency_hint_ms,
                    stamp,
                });
            }
        }
    }
    Ok(out)
}

pub(crate) fn shallow(scan: &ScanEnv<'_>, c: &Candidate) -> Result<Option<ShallowSession>> {
    let Some((state, id)) = parse_locator(&c.locator) else {
        return Ok(None);
    };
    let context = AcquisitionContext::new("codex", scan.provider_roots);
    scan.note_open();
    scan.note_query();
    let thread = sqlite_snapshot(&context, &state, |db| {
        db.query_row(&format!("{THREAD_SQL} AND id=?1"), [id], thread_row)
            .optional()
            .map_err(Into::into)
    })?;
    let Some(mut t) = thread else {
        return Ok(None);
    };
    let Some((_, history)) = databases(&scan.provider_roots.codex)? else {
        return Ok(None);
    };
    scan.note_open();
    scan.note_query();
    if let Some((ordinal, offset)) = sqlite_snapshot(&context, &history, |db| {
        db.query_row(
            "SELECT next_rollout_ordinal,next_rollout_byte_offset FROM thread_history_projection_state WHERE thread_id=?1",
            [id],
            |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)),
        )
        .optional()
        .map_err(Into::into)
    })? {
        t.next_ordinal = ordinal;
        t.next_offset = offset;
    }
    scan.note_records(1);
    if t.rollout_complete() || t.is_child() {
        return Ok(None);
    }
    Ok(Some(ShallowSession {
        source: "codex".into(),
        session_id: t.id,
        cwd: t.cwd,
        git_branch: t.branch,
        first_activity_ms: Some(t.created),
        last_activity_ms: Some(t.updated),
        first_prompt: t.first_user_message.map(|s| s.chars().take(512).collect()),
        models: t.model.into_iter().collect(),
        originator: t.originator,
        agent_version: t.version,
        repo_url: t.repo,
        title: t.name.or(t.title),
        raw_path: Some(c.locator.clone()),
        source_stamp: Some(c.stamp.clone()),
        ..Default::default()
    }))
}

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

mod normalize;
pub(crate) use normalize::normalize;

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
        .get("codex_sqlite_v1")
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
        let acquired = capture(&roots, &state_db, &t.id)?;
        let captured = decode(&acquired)?;
        let tx = conn.unchecked_transaction()?;
        inserted += normalize(&tx, &captured, &state_db)?;
        tx.commit()?;
        read.push(t.id.clone());
        stamps.insert(t.id, json!(stamp));
    }
    state.insert("codex_sqlite_v1".into(), Value::Object(stamps));
    Ok((inserted, read))
}

#[cfg(test)]
mod tests;

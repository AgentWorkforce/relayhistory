//! Bounded, resumable selection of Codex SQLite-only threads.

use super::*;
use std::cell::RefCell;
use std::collections::HashMap;

mod cursor;
use cursor::{clear as clear_cursor, load as load_cursor, store as store_cursor, SweepCursor};

const STAMPS_KEY: &str = "codex_sqlite_v2";
const PAGE_THREADS: usize = 128;
pub(super) const DEFAULT_SWEEP_THREADS: usize = 50;
const DEFAULT_MAX_AGE_DAYS: u64 = 30;
const MAX_AGE_ENV: &str = "AI_HIST_CODEX_SQLITE_MAX_AGE_DAYS";

#[derive(Clone, Copy)]
pub(super) struct SweepPolicy {
    pub(super) max_threads: usize,
    pub(super) min_updated_ms: Option<i64>,
    pub(super) max_age_days: Option<u64>,
}

impl SweepPolicy {
    fn resolved(max_age_days: Option<u64>) -> Self {
        let days = max_age_days.unwrap_or_else(|| {
            std::env::var(MAX_AGE_ENV)
                .ok()
                .and_then(|raw| raw.trim().parse().ok())
                .unwrap_or(DEFAULT_MAX_AGE_DAYS)
        });
        let min_updated_ms = (days != 0).then(|| {
            let age = i64::try_from(days)
                .unwrap_or(i64::MAX)
                .saturating_mul(24 * 60 * 60 * 1_000);
            crate::now_ms().saturating_sub(age)
        });
        Self {
            max_threads: DEFAULT_SWEEP_THREADS,
            min_updated_ms,
            max_age_days: (days != 0).then_some(days),
        }
    }

    #[cfg(test)]
    pub(super) fn unbounded() -> Self {
        Self {
            max_threads: DEFAULT_SWEEP_THREADS,
            min_updated_ms: None,
            max_age_days: None,
        }
    }
}

thread_local! {
    static SWEEP_POLICY: RefCell<Option<SweepPolicy>> = const { RefCell::new(None) };
    static SWEEP_DISCOVERY_IDS: RefCell<Option<HashSet<String>>> = const { RefCell::new(None) };
    static SWEEP_SOURCE_FINGERPRINT: RefCell<Option<String>> = const { RefCell::new(None) };
}

pub(crate) fn with_sweep_policy<T, E>(
    max_age_days: Option<u64>,
    run: impl FnOnce() -> std::result::Result<T, E>,
) -> std::result::Result<T, E> {
    with_policy(SweepPolicy::resolved(max_age_days), run)
}

fn with_policy<T, E>(
    policy: SweepPolicy,
    run: impl FnOnce() -> std::result::Result<T, E>,
) -> std::result::Result<T, E> {
    struct Restore(Option<SweepPolicy>);
    impl Drop for Restore {
        fn drop(&mut self) {
            SWEEP_POLICY.with(|slot| *slot.borrow_mut() = self.0.take());
        }
    }
    let _restore = Restore(SWEEP_POLICY.with(|slot| slot.replace(Some(policy))));
    run()
}

/// Stable qualification for the full-source fingerprint. A changed cutoff
/// must run a sweep even when the provider databases did not move, and the
/// version makes the first bounded pass run after upgrading from 0.45.0.
pub(crate) fn fingerprint_component() -> String {
    // Unit-test sweeps default to an unbounded fixture window, but a direct
    // legacy sync and `SessionStore::sync(Default::default())` must still
    // spell the same production-default fingerprint.
    let policy = SWEEP_POLICY
        .with(|slot| *slot.borrow())
        .unwrap_or_else(|| SweepPolicy::resolved(None));
    format!(
        "codex-sqlite-v1:{}:{}",
        policy.max_threads,
        policy.max_age_days.unwrap_or(0)
    )
}

pub(crate) struct SyncPass {
    ids: Option<HashSet<String>>,
    source_fingerprint: Option<String>,
}

impl SyncPass {
    pub(crate) fn begin() -> Self {
        Self {
            ids: SWEEP_DISCOVERY_IDS.with(|slot| slot.replace(Some(HashSet::new()))),
            source_fingerprint: SWEEP_SOURCE_FINGERPRINT.with(|slot| slot.take()),
        }
    }
}

impl Drop for SyncPass {
    fn drop(&mut self) {
        SWEEP_DISCOVERY_IDS.with(|slot| *slot.borrow_mut() = self.ids.take());
        SWEEP_SOURCE_FINGERPRINT.with(|slot| {
            *slot.borrow_mut() = self.source_fingerprint.take();
        });
    }
}

pub(crate) fn remember_source_fingerprint(value: String) {
    SWEEP_SOURCE_FINGERPRINT.with(|slot| *slot.borrow_mut() = Some(value));
}

fn source_fingerprint() -> Option<String> {
    SWEEP_SOURCE_FINGERPRINT.with(|slot| slot.borrow().clone())
}

fn sweep_policy() -> SweepPolicy {
    if let Some(policy) = SWEEP_POLICY.with(|slot| *slot.borrow()) {
        return policy;
    }
    #[cfg(test)]
    return SweepPolicy::unbounded();
    #[cfg(not(test))]
    SweepPolicy::resolved(None)
}

fn remember_sweep_threads(threads: &[Thread]) {
    SWEEP_DISCOVERY_IDS.with(|slot| {
        if let Some(ids) = slot.borrow_mut().as_mut() {
            ids.extend(threads.iter().map(|thread| thread.id.clone()));
        }
    });
}

fn sweep_discovery_ids() -> Option<HashSet<String>> {
    SWEEP_DISCOVERY_IDS.with(|slot| slot.borrow().clone())
}

struct Selection {
    threads: Vec<Thread>,
    cursor: Option<SweepCursor>,
    tail: Option<SweepCursor>,
    clear_stamps: Vec<String>,
}

fn page_threads(
    db: &Connection,
    after: Option<&SweepCursor>,
    min_updated_ms: Option<i64>,
) -> Result<Vec<Thread>> {
    let cutoff = min_updated_ms.unwrap_or(i64::MIN);
    let sql = match after {
        Some(_) => format!(
            "{THREAD_SQL} AND updated_at_ms>=?1 AND \
             (updated_at_ms<?2 OR (updated_at_ms=?2 AND id<?3)) \
             ORDER BY updated_at_ms DESC,id DESC LIMIT ?4"
        ),
        None => format!(
            "{THREAD_SQL} AND updated_at_ms>=?1 \
             ORDER BY updated_at_ms DESC,id DESC LIMIT ?2"
        ),
    };
    let mut stmt = db.prepare(&sql)?;
    match after {
        Some(after) => Ok(stmt
            .query_map(
                params![cutoff, after.updated_at_ms, after.thread_id, PAGE_THREADS],
                thread_row,
            )?
            .collect::<rusqlite::Result<_>>()?),
        None => Ok(stmt
            .query_map(params![cutoff, PAGE_THREADS], thread_row)?
            .collect::<rusqlite::Result<_>>()?),
    }
}

fn projection_for(
    context: &AcquisitionContext<'_>,
    history: &Path,
    threads: &mut [Thread],
) -> Result<()> {
    if threads.is_empty() {
        return Ok(());
    }
    let ids = serde_json::to_string(
        &threads
            .iter()
            .map(|thread| thread.id.as_str())
            .collect::<Vec<_>>(),
    )?;
    let offsets = sqlite_snapshot(context, history, |db| {
        let mut stmt = db.prepare(
            "SELECT thread_id,next_rollout_ordinal,next_rollout_byte_offset \
             FROM thread_history_projection_state \
             WHERE thread_id IN (SELECT value FROM json_each(?1))",
        )?;
        let rows = stmt.query_map([ids], |row| {
            Ok((
                row.get::<_, String>(0)?,
                (row.get::<_, i64>(1)?, row.get::<_, i64>(2)?),
            ))
        })?;
        rows.collect::<rusqlite::Result<HashMap<_, _>>>()
            .map_err(Into::into)
    })?;
    for thread in threads {
        if let Some((ordinal, offset)) = offsets.get(&thread.id) {
            thread.next_ordinal = *ordinal;
            thread.next_offset = *offset;
        }
    }
    Ok(())
}

fn select_threads(
    context: &AcquisitionContext<'_>,
    state_db: &Path,
    history: &Path,
    start: Option<SweepCursor>,
    policy: SweepPolicy,
    source_fingerprint: Option<&str>,
) -> Result<Selection> {
    sqlite_snapshot(context, state_db, |db| {
        let retry_required = matches!(start.as_ref(), Some(cursor) if cursor.retry_required);
        let mut selected = Vec::new();
        let mut clear_stamps = Vec::new();
        let mut after = start.filter(|cursor| !cursor.restart);
        let mut scanned = 0usize;
        loop {
            crate::ingest::check_capture_cancelled()?;
            let mut page = page_threads(db, after.as_ref(), policy.min_updated_ms)?;
            if page.is_empty() {
                return Ok(Selection {
                    threads: selected,
                    cursor: None,
                    tail: after,
                    clear_stamps,
                });
            }
            projection_for(context, history, &mut page)?;
            let page_len = page.len();
            for thread in page {
                scanned += 1;
                ensure!(
                    scanned <= MAX_THREADS,
                    "Codex thread discovery exceeds safety limit"
                );
                after = Some(SweepCursor {
                    updated_at_ms: thread.updated,
                    thread_id: thread.id.clone(),
                    source_fingerprint: source_fingerprint.map(str::to_owned),
                    retry_required,
                    restart: false,
                });
                if thread.rollout_complete() {
                    clear_stamps.push(thread.id);
                    continue;
                }
                selected.push(thread);
                if selected.len() == policy.max_threads {
                    return Ok(Selection {
                        threads: selected,
                        cursor: after.clone(),
                        tail: after,
                        clear_stamps,
                    });
                }
            }
            if page_len < PAGE_THREADS {
                return Ok(Selection {
                    threads: selected,
                    cursor: None,
                    tail: after,
                    clear_stamps,
                });
            }
        }
    })
}

fn selected_threads(
    context: &AcquisitionContext<'_>,
    state_db: &Path,
    history: &Path,
    ids: &HashSet<String>,
    policy: SweepPolicy,
) -> Result<Vec<Thread>> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let ids_json = serde_json::to_string(ids)?;
    let cutoff = policy.min_updated_ms.unwrap_or(i64::MIN);
    let mut threads = sqlite_snapshot(context, state_db, |db| {
        let mut stmt = db.prepare(&format!(
            "{THREAD_SQL} AND updated_at_ms>=?1 \
             AND id IN (SELECT value FROM json_each(?2)) \
             ORDER BY updated_at_ms DESC,id DESC LIMIT ?3"
        ))?;
        let rows = stmt
            .query_map(params![cutoff, ids_json, MAX_THREADS + 1], thread_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    })?;
    ensure!(
        threads.len() <= MAX_THREADS,
        "Codex thread discovery exceeds safety limit"
    );
    projection_for(context, history, &mut threads)?;
    threads.retain(|thread| !thread.rollout_complete());
    Ok(threads)
}

pub(super) fn discovery_threads(
    context: &AcquisitionContext<'_>,
    state_db: &Path,
    history: &Path,
) -> Result<Vec<Thread>> {
    let policy = sweep_policy();
    if let Some(ids) = sweep_discovery_ids() {
        return selected_threads(context, state_db, history, &ids, policy);
    }
    Ok(select_threads(
        context,
        state_db,
        history,
        None,
        SweepPolicy {
            // Read one past the safety ceiling so an oversized store is a
            // diagnostic rather than a silently truncated discovery.
            max_threads: MAX_THREADS + 1,
            ..policy
        },
        None,
    )?
    .threads)
}

pub(super) fn incomplete_rollout_paths(
    context: &AcquisitionContext<'_>,
    state_db: &Path,
    history: &Path,
) -> Result<HashSet<PathBuf>> {
    let cutoff = sweep_policy().min_updated_ms.unwrap_or(i64::MIN);
    let mut threads = sqlite_snapshot(context, state_db, |db| {
        let mut stmt = db.prepare(&format!(
            "{THREAD_SQL} AND updated_at_ms>=?1 AND rollout_path IS NOT NULL \
             ORDER BY updated_at_ms DESC,id DESC LIMIT ?2"
        ))?;
        let rows = stmt
            .query_map(params![cutoff, MAX_THREADS + 1], thread_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    })?;
    ensure!(
        threads.len() <= MAX_THREADS,
        "Codex rollout discovery exceeds safety limit"
    );
    projection_for(context, history, &mut threads)?;
    Ok(threads
        .into_iter()
        .filter(|thread| !thread.rollout_complete())
        .filter_map(|thread| thread.rollout_path.map(PathBuf::from))
        .collect())
}

pub(crate) struct SyncOutcome {
    pub(crate) inserted: usize,
    pub(crate) read: Vec<String>,
    pub(crate) incomplete: bool,
}

pub(crate) fn backlog_pending(state: &serde_json::Map<String, Value>) -> bool {
    load_cursor(state).is_some()
}

pub(crate) fn reset_backlog(state: &mut serde_json::Map<String, Value>) {
    clear_cursor(state);
}

fn traversal_changed(
    cursor_pending: bool,
    traversal: &Option<String>,
    current: &Option<String>,
) -> bool {
    !cursor_pending && current.is_some() && traversal != current
}

fn finish_cursor(
    cursor: &mut Option<SweepCursor>,
    tail: &mut Option<SweepCursor>,
    retry_required: bool,
    failed: bool,
) {
    if failed {
        if let Some(cursor) = cursor.as_mut() {
            cursor.retry_required = true;
        }
    }
    if cursor.is_none() && (retry_required || failed) {
        if let Some(mut restart) = tail.take() {
            restart.restart = true;
            restart.retry_required = false;
            *cursor = Some(restart);
        }
    }
}

pub(crate) fn sync(
    conn: &Connection,
    state: &mut serde_json::Map<String, Value>,
    root: &Path,
    repairs: &crate::ingest::SweepRepairs,
) -> Result<SyncOutcome> {
    let Some((state_db, history)) = databases(root)? else {
        reset_backlog(state);
        return Ok(SyncOutcome {
            inserted: 0,
            read: Vec::new(),
            incomplete: false,
        });
    };
    let home = root.parent().unwrap_or(root).to_path_buf();
    let mut roots = crate::ProviderRoots::from_home(
        home.clone(),
        home.join(".local/share/opencode/opencode.db"),
    );
    roots.codex = root.to_path_buf();
    let context = AcquisitionContext::new("codex", &roots);
    let start = load_cursor(state);
    let source_fingerprint = source_fingerprint();
    let traversal_fingerprint = match start.as_ref() {
        Some(cursor) => cursor.source_fingerprint.clone(),
        None => source_fingerprint.clone(),
    };
    let retry_required = matches!(start.as_ref(), Some(cursor) if cursor.retry_required);
    let mut selection = select_threads(
        &context,
        &state_db,
        &history,
        start,
        sweep_policy(),
        traversal_fingerprint.as_deref(),
    )?;
    remember_sweep_threads(&selection.threads);
    let mut stamps = state
        .get(STAMPS_KEY)
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let _ = crate::ingest::forget_unobserved_paths(
        state,
        &mut stamps,
        STAMPS_KEY,
        selection.clear_stamps.clone(),
    );
    let sessions: Vec<_> = selection
        .threads
        .iter()
        .map(|thread| (thread.id.clone(), thread.rollout_path.clone()))
        .collect();
    let (inserted, read, failed) =
        crate::catalog_owner::with_raw_path_cache(conn, "codex", &sessions, || {
            let mut inserted = 0;
            let mut read = Vec::new();
            let mut failed = false;
            for thread in crate::ingest::capture_files("codex-sqlite", selection.threads) {
                crate::ingest::check_capture_cancelled()?;
                let stamp = thread.stamp();
                if stamps.get(&thread.id).and_then(Value::as_str) == Some(&stamp)
                    && !repairs.contains("codex", &thread.id)
                {
                    continue;
                }
                let result = (|| -> Result<usize> {
                    let acquired = capture(&roots, &state_db, &thread.id)?;
                    let captured = decode(&acquired)?;
                    let tx = conn.unchecked_transaction()?;
                    retire_if_changed(&tx, &captured, true)?;
                    let count = normalize(&tx, &captured, &state_db)?;
                    tx.commit()?;
                    Ok(count)
                })();
                match result {
                    Ok(count) => inserted += count,
                    Err(error) if error.is::<crate::ingest::CaptureCancelled>() => {
                        return Err(error)
                    }
                    Err(error) => {
                        failed = true;
                        crate::ingest::scoped_files::provider_diagnostic(format_args!(
                            "[codex] skipping SQLite thread {}: {error:#}",
                            thread.id
                        ));
                        continue;
                    }
                }
                read.push(thread.id.clone());
                stamps.insert(thread.id, json!(stamp));
            }
            Ok((inserted, read, failed))
        })?;
    state.insert(STAMPS_KEY.into(), Value::Object(stamps));
    finish_cursor(
        &mut selection.cursor,
        &mut selection.tail,
        retry_required,
        failed,
    );
    let cursor_pending = selection.cursor.is_some();
    let source_changed_during_traversal =
        traversal_changed(cursor_pending, &traversal_fingerprint, &source_fingerprint);
    store_cursor(state, selection.cursor)?;
    Ok(SyncOutcome {
        inserted,
        read,
        incomplete: cursor_pending || source_changed_during_traversal || retry_required || failed,
    })
}

#[cfg(test)]
pub(super) fn sync_with_policy(
    conn: &Connection,
    state: &mut serde_json::Map<String, Value>,
    root: &Path,
    repairs: &crate::ingest::SweepRepairs,
    policy: SweepPolicy,
) -> Result<SyncOutcome> {
    with_policy(policy, || sync(conn, state, root, repairs))
}

#[cfg(test)]
pub(super) fn with_test_policy<T>(
    policy: SweepPolicy,
    run: impl FnOnce() -> Result<T>,
) -> Result<T> {
    with_policy(policy, run)
}

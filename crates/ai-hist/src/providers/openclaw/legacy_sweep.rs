//! Sweep of the legacy JSONL store. Each changed transcript is re-read whole
//! (Pi semantics: an unterminated final record is deferred and counts as
//! unread); a transcript whose session the agent database holds is skipped.
use super::*;
use crate::ingest::check_capture_cancelled;
use crate::providers::file_history;
use anyhow::Context;
use std::collections::BTreeSet;

const STAMPS: &str = "openclaw_legacy_stamps";

/// Whether a scoped sweep reaches this transcript: the file itself, a
/// directory holding it, or its session index changed.
fn in_scope(path: &Path, scope: Option<&crate::ingest::path_scope::PathScope>) -> bool {
    let index = legacy::index_path(path);
    scope.is_none_or(|scope| {
        scope
            .paths()
            .iter()
            .any(|changed| path.starts_with(changed) || Some(changed) == index.as_ref())
    })
}

/// Returns (rows inserted, transcripts unread, whether any agent's index or
/// database was unreadable). Only the last withholds deletion proof: a
/// malformed or still-growing transcript says nothing about another file's
/// absence, but an unreadable database may hold an imported session.
pub(super) fn sweep(ctx: &mut SweepContext<'_>) -> Result<(usize, u64, bool)> {
    let context = AcquisitionContext::new(ID, ctx.roots);
    let mut cache = ctx
        .state
        .get(STAMPS)
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let mut visited = BTreeSet::new();
    let (mut inserted, mut unread, mut failed) = (0, 0, false);
    for dir in legacy::dirs(ctx.roots)? {
        check_capture_cancelled()?;
        let files = match legacy::transcripts(&context, &dir) {
            Ok(files) => files,
            Err(error) => {
                check_capture_cancelled()?;
                // This agent alone is unread: its stamps stay, and no
                // transcript absence is trusted this sweep.
                eprintln!("OpenClaw {}: {error:#}", dir.display());
                unread += 1;
                failed = true;
                let kept = cache.keys().filter(|p| Path::new(p).starts_with(&dir));
                visited.extend(kept.cloned());
                continue;
            }
        };
        visited.extend(files.iter().map(|p| p.to_string_lossy().into_owned()));
        let files: Vec<_> = files
            .into_iter()
            .filter(|p| in_scope(p, ctx.paths))
            .collect();
        // A scoped sweep that names a removed transcript still proves its
        // agent's database readable before that absence counts.
        let touched = ctx.paths.is_none_or(|scope| {
            scope
                .paths()
                .iter()
                .any(|c| c.starts_with(&dir) || dir.starts_with(c))
        });
        if files.is_empty() && !touched {
            continue;
        }
        match sweep_dir(ctx, &context, &dir, &files, &mut cache) {
            Ok((n, u)) => {
                inserted += n;
                unread += u;
            }
            Err(error) => {
                check_capture_cancelled()?;
                // An unreadable index or agent database: every transcript of
                // this agent keeps its prior evidence and cursor.
                eprintln!("OpenClaw {}: {error:#}", dir.display());
                unread += files.len() as u64;
                failed = true;
            }
        }
    }
    if ctx.paths.is_none() {
        cache.retain(|path, _| visited.contains(path));
    }
    ctx.state.insert(STAMPS.into(), Value::Object(cache));
    Ok((inserted, unread, failed))
}

fn sweep_dir(
    ctx: &SweepContext<'_>,
    context: &AcquisitionContext<'_>,
    dir: &Path,
    files: &[PathBuf],
    cache: &mut Map<String, Value>,
) -> Result<(usize, u64)> {
    let agent = dir
        .parent()
        .context("OpenClaw sessions directory has no agent")?;
    let held = stores::held(context, agent)?;
    let index = legacy::index(context, dir)?;
    let mut seen = BTreeSet::new();
    let (mut inserted, mut unread) = (0, 0);
    for path in files {
        check_capture_cancelled()?;
        let key = path.to_string_lossy().into_owned();
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default();
        let (stamp, _) = legacy::stamp(path, index.get(name))?;
        let stamp = format!("{}/{stamp}", ADAPTER.unwrap().generation);
        if let Some(native) = unchanged(ctx, cache.get(&key), &stamp, &held)? {
            seen.insert(native);
            continue;
        }
        let mut read = || -> Result<(usize, bool, String)> {
            let (s, partial) = legacy::session(context, path, index.get(name), stamp.clone())?;
            let native = s.native_id.clone();
            // The database's copy, or an earlier file of the same session,
            // owns it.
            if held.contains(&native) || !seen.insert(native.clone()) {
                return Ok((0, false, native));
            }
            let id = s.id.clone();
            let tx = ctx.conn.unchecked_transaction()?;
            let n = legacy::complete_or_append(&tx, s, path)?;
            // A rewritten file that now holds another session drops the old,
            // once the rewrite is complete.
            if !partial {
                file_history::retire_locator_sessions(&tx, ID, path, Some(&id))?;
            }
            tx.commit()?;
            Ok((n, partial, native))
        };
        match read() {
            Ok((n, partial, native)) => {
                inserted += n;
                unread += u64::from(partial);
                if partial {
                    cache.remove(&key);
                } else {
                    let id = storage::namespace(ctx.roots, path)? + ":" + &native;
                    cache.insert(
                        key,
                        serde_json::json!({"stamp": stamp, "native": native, "id": id}),
                    );
                }
            }
            Err(error) => {
                check_capture_cancelled()?;
                eprintln!("OpenClaw {}: {error:#}", path.display());
                unread += 1;
                cache.remove(&key);
            }
        }
    }
    Ok((inserted, unread))
}

/// The cached native ID when this transcript needs no read: same stamp, and
/// either the database holds its session or the ledger still has it.
fn unchanged(
    ctx: &SweepContext<'_>,
    cursor: Option<&Value>,
    stamp: &str,
    held: &BTreeSet<String>,
) -> Result<Option<String>> {
    let Some(cursor) = cursor.filter(|c| c["stamp"].as_str() == Some(stamp)) else {
        return Ok(None);
    };
    let (Some(native), Some(id)) = (cursor["native"].as_str(), cursor["id"].as_str()) else {
        return Ok(None);
    };
    let skip = held.contains(native) || (!ctx.repairs.contains(ID, id) && present(ctx.conn, id)?);
    Ok(skip.then(|| native.to_owned()))
}

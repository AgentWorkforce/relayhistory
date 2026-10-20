//! Where an agent's stores live: `agents/<id>/agent/openclaw-agent.sqlite`
//! and, before 2026.8, `agents/<id>/sessions/`. Enumeration, watches and the
//! stat-only fingerprint; nothing here opens a store's contents except
//! `held`, which reads one database's session IDs.
use super::storage::{agent_id, namespace, read, sidecar, DATABASE};
use super::*;
use crate::discover::{Candidate, WatchEntries};
use crate::ingest::check_capture_cancelled;
use anyhow::ensure;
const MAX_AGENTS: usize = 1024;

/// Every directory under each root's `agents/`, validated inside the root.
pub(super) fn agent_dirs(roots: &ProviderRoots) -> Result<Vec<PathBuf>> {
    let context = AcquisitionContext::new(ID, roots);
    let mut dirs = vec![];
    for root in roots.for_provider(ID) {
        check_capture_cancelled()?;
        match std::fs::metadata(root) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e.into()),
            Ok(_) => context.validate(root)?,
        }
        let agents = root.join("agents");
        if !agents.try_exists()? {
            continue;
        }
        context.validate(&agents)?;
        for (index, entry) in std::fs::read_dir(&agents)?.enumerate() {
            check_capture_cancelled()?;
            ensure!(
                index < MAX_AGENTS,
                "OpenClaw agent enumeration exceeds {MAX_AGENTS} entries"
            );
            let dir = entry?.path();
            context.validate(&dir)?;
            if dir.is_dir() {
                dirs.push(dir);
            }
        }
    }
    dirs.sort();
    dirs.dedup();
    Ok(dirs)
}
/// The agent database of an agent directory, when it exists.
pub(super) fn database_of(agent: &Path) -> Result<Option<PathBuf>> {
    let db = agent.join("agent").join(DATABASE);
    if !db.try_exists()? {
        return Ok(None);
    }
    agent_id(&db)?;
    Ok(Some(db))
}
pub(super) fn databases(roots: &ProviderRoots) -> Result<Vec<PathBuf>> {
    let mut paths = vec![];
    for agent in agent_dirs(roots)? {
        paths.extend(database_of(&agent)?);
    }
    Ok(paths)
}
pub(super) fn watch_roots(roots: &ProviderRoots) -> Vec<WatchRoot> {
    let mut watches: Vec<_> = databases(roots)
        .unwrap_or_default()
        .iter()
        .filter_map(|p| p.parent())
        .map(|dir| {
            // Narrow to the database and its sidecars: the agent directory
            // also holds credentials and Codex homes.
            WatchRoot::directory_of(
                dir,
                WatchEntries::OpencodeStores {
                    primary: DATABASE.into(),
                    channels: false,
                },
            )
        })
        .collect();
    // The legacy JSONL store: its index and transcripts change in place.
    watches.extend(
        legacy::dirs(roots)
            .unwrap_or_default()
            .into_iter()
            .map(WatchRoot::directory),
    );
    // New agents appear under agents/; the watcher backstop then installs
    // their narrow database watches.
    for root in roots.for_provider(ID) {
        watches.push(WatchRoot::directory(root.join("agents")));
    }
    watches
}

/// Native IDs of the sessions an agent database holds a transcript for, or
/// none when the agent has no database or it predates transcripts.
pub(super) fn held(
    context: &AcquisitionContext<'_>,
    agent: &Path,
) -> Result<std::collections::BTreeSet<String>> {
    let Some(path) = database_of(agent)? else {
        return Ok(Default::default());
    };
    read(context, &path, |conn, schema| {
        if !schema.transcripts {
            return Ok(Default::default());
        }
        let mut stmt = conn.prepare(&format!(
            "SELECT w.session_id FROM session_windows w \
             WHERE EXISTS(SELECT 1 FROM transcript_events e WHERE e.session_id=w.session_id) \
             OR EXISTS(SELECT 1 FROM {} c WHERE c.session_id=w.session_id)",
            schema.cold_archives()
        ))?;
        let ids = stmt
            .query_map([], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        Ok(ids)
    })
}

pub(super) fn fingerprint_inputs(roots: &ProviderRoots) -> Result<Vec<Candidate>> {
    let context = AcquisitionContext::new(ID, roots);
    let mut inputs = vec![];
    for db in databases(roots)? {
        let files = ["", "-wal", "-journal"]
            .iter()
            .map(|suffix| sidecar(&db, suffix))
            .chain(cold::files(&context, &db)?);
        inputs.push((namespace(roots, &db)?, files.collect::<Vec<_>>()));
    }
    for dir in legacy::dirs(roots)? {
        let mut files = legacy::transcripts(&context, &dir)?;
        files.push(dir.join(legacy::INDEX));
        inputs.push((namespace(roots, &dir.join(legacy::INDEX))?, files));
    }
    let mut result = vec![];
    for (namespace, files) in inputs {
        for path in files {
            if path.try_exists()? {
                context.validate(&path)?;
                let (stamp, modified) = crate::file_stamp_and_modified(&path)?;
                result.push(Candidate {
                    source: ID,
                    locator: path.to_string_lossy().into_owned(),
                    session_id: None,
                    recency_hint_ms: modified,
                    // The namespace is frozen identity metadata; keep the stat
                    // length last for the shared byte fold.
                    stamp: format!("{namespace}/{stamp}"),
                });
            }
        }
    }
    Ok(result)
}

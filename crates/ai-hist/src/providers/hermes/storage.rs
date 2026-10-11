use super::*;
use crate::discover::WatchEntries;
use crate::ingest::check_capture_cancelled;
use anyhow::{ensure, Context};
use sha2::{Digest, Sha256};
const MAX_SESSIONS: usize = 100_000;
fn hex(text: &[u8]) -> String {
    text.iter().map(|b| format!("{b:02x}")).collect()
}
pub(super) fn profile(roots: &ProviderRoots, path: &Path) -> String {
    let dir = path.parent().unwrap();
    if let Some(default) = roots.default_for_provider(ID) {
        if dir == default {
            return "default".into();
        }
        if let Ok(relative) = dir.strip_prefix(default.join("profiles")) {
            return format!("profile:{}", hex(relative.as_os_str().as_encoded_bytes()));
        }
    }
    format!(
        "root:{:x}",
        Sha256::digest(dir.as_os_str().as_encoded_bytes())
    )
}
fn identity(profile: &str, native: &str) -> String {
    format!("{profile}:{}", hex(native.as_bytes()))
}
pub(super) fn databases(roots: &ProviderRoots) -> Result<Vec<PathBuf>> {
    let context = AcquisitionContext::new(ID, roots);
    let mut paths = vec![];
    for root in roots.for_provider(ID) {
        check_capture_cancelled()?;
        match std::fs::metadata(root) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e.into()),
            Ok(_) => context.validate(root)?,
        }
        let main = root.join("state.db");
        if main.try_exists()? {
            paths.push(main);
        }
        let profiles = root.join("profiles");
        if !profiles.try_exists()? {
            continue;
        }
        context.validate(&profiles)?;
        for (index, entry) in std::fs::read_dir(profiles)?.enumerate() {
            check_capture_cancelled()?;
            ensure!(
                index < 1024,
                "Hermes profile enumeration exceeds 1024 entries"
            );
            let path = entry?.path();
            context.validate(&path)?;
            if !path.is_dir() {
                continue;
            }
            let db = path.join("state.db");
            if db.try_exists()? {
                paths.push(db);
            }
        }
    }
    paths.sort();
    paths.dedup();
    Ok(paths)
}
pub(super) fn watch_roots(roots: &ProviderRoots) -> Vec<WatchRoot> {
    let mut dirs: Vec<_> = roots.for_provider(ID).to_vec();
    if let Ok(paths) = databases(roots) {
        dirs.extend(
            paths
                .iter()
                .filter_map(|p| p.parent().map(Path::to_path_buf)),
        );
    }
    dirs.sort();
    dirs.dedup();
    let mut watches: Vec<_> = dirs
        .into_iter()
        .map(|p| {
            WatchRoot::directory_of(
                p,
                WatchEntries::OpencodeStores {
                    primary: "state.db".into(),
                    channels: false,
                },
            )
        })
        .collect();
    // Parent directory events notice new profiles; watcher backstop refreshes their
    // narrow database roots. Existing profile config/credential writes stay quiet.
    watches.extend(
        roots
            .for_provider(ID)
            .iter()
            .map(|p| WatchRoot::directory(p.join("profiles"))),
    );
    watches
}
fn read<T>(
    context: &AcquisitionContext<'_>,
    path: &Path,
    f: impl FnOnce(&Connection) -> Result<T>,
) -> Result<T> {
    check_capture_cancelled()?;
    context.validate(path)?;
    ensure!(
        path.file_name().is_some_and(|n| n == "state.db"),
        "Hermes locator must name state.db"
    );
    for suffix in ["-wal", "-shm", "-journal"] {
        let sidecar = sidecar(path, suffix);
        if sidecar.try_exists()? {
            context.validate(&sidecar)?;
        }
    }
    sqlite_snapshot(context, path, |conn| {
        let version: i64 = conn
            .query_row("SELECT version FROM schema_version", [], |r| r.get(0))
            .context("unsupported Hermes schema: missing schema_version")?;
        ensure!(
            version == 31,
            "unsupported Hermes schema {version}; supported: 31"
        );
        f(conn)
    })
}
fn timestamp(value: f64) -> Result<i64> {
    ensure!(
        value.is_finite() && value >= 0.0 && value < i64::MAX as f64 / 1000.0,
        "invalid Hermes timestamp"
    );
    Ok((value * 1000.0) as i64)
}
// Discovery scans the message table once, rather than once per session. A
// selected capture aggregates only that session's messages.
pub(super) fn session_query(selected: bool) -> String {
    let predicate = if selected { "WHERE session_id=?1" } else { "" };
    format!("SELECT s.id,s.title,s.model,s.cwd,s.git_branch,s.parent_session_id,s.end_reason,s.started_at,s.ended_at, max(s.started_at,coalesce(s.ended_at,s.started_at),coalesce(m.latest,s.started_at)) AS latest FROM sessions s LEFT JOIN (SELECT session_id,max(timestamp) AS latest FROM messages {predicate} GROUP BY session_id) m ON m.session_id=s.id WHERE (?1 IS NULL OR s.id=?1) ORDER BY latest DESC,s.id LIMIT ?2")
}
fn sessions(
    conn: &Connection,
    roots: &ProviderRoots,
    path: &Path,
    limit: usize,
    native: Option<&str>,
) -> Result<Vec<Session>> {
    let profile = profile(roots, path);
    let query = session_query(native.is_some());
    let mut stmt = conn.prepare(&query)?;
    let rows = stmt.query_map(rusqlite::params![native, limit as i64], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get(1)?,
            r.get(2)?,
            r.get(3)?,
            r.get(4)?,
            r.get(5)?,
            r.get(6)?,
            r.get::<_, f64>(7)?,
            r.get::<_, Option<f64>>(8)?,
            r.get::<_, f64>(9)?,
        ))
    })?;
    let mut result = vec![];
    for row in rows {
        check_capture_cancelled()?;
        let (native_id, title, model, cwd, git_branch, parent, end_reason, started, ended, latest) =
            row?;
        ensure!(
            !native_id.trim().is_empty(),
            "empty Hermes session identity"
        );
        result.push(Session {
            id: identity(&profile, &native_id),
            native_id,
            profile: profile.clone(),
            title,
            model,
            cwd,
            git_branch,
            parent,
            end_reason,
            started: timestamp(started)?,
            latest: timestamp(latest)?,
            ended: ended.map(timestamp).transpose()?,
            messages: vec![],
        });
    }
    Ok(result)
}
fn messages(conn: &Connection, native: &str) -> Result<Vec<Message>> {
    let mut stmt = conn.prepare("SELECT id,role,content,timestamp,tool_call_id,tool_calls,tool_name,reasoning,reasoning_content,finish_reason,_compressed_summary,active,compacted,observed,display_kind FROM messages WHERE session_id=? ORDER BY id LIMIT 1000001")?;
    let rows = stmt.query_map([native], message_row)?;
    let mut result = Vec::new();
    for row in rows {
        check_capture_cancelled()?;
        result.push(row?);
    }
    ensure!(
        result.len() <= 1_000_000,
        "Hermes message capture exceeds safety limit"
    );
    // Timestamps validated explicitly rather than coercing invalid SQLite values.
    let mut times = conn
        .prepare("SELECT timestamp FROM messages WHERE session_id=? ORDER BY id LIMIT 1000001")?;
    for (message, time) in result
        .iter_mut()
        .zip(times.query_map([native], |r| r.get::<_, f64>(0))?)
    {
        check_capture_cancelled()?;
        message.timestamp = timestamp(time?)?;
    }
    Ok(result)
}
fn message_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<Message> {
    Ok(Message {
        id: r.get(0)?,
        role: r.get(1)?,
        content: r.get(2)?,
        timestamp: 0,
        tool_call_id: r.get(4)?,
        tool_calls: r.get(5)?,
        tool_name: r.get(6)?,
        reasoning: r.get(7)?,
        reasoning_content: r.get(8)?,
        finish_reason: r.get(9)?,
        compressed: r.get(10)?,
        active: r.get(11)?,
        compacted: r.get(12)?,
        observed: r.get(13)?,
        display_kind: r.get(14)?,
    })
}
pub(super) fn enumerate(roots: &ProviderRoots, limit: Option<usize>) -> Result<Vec<Candidate>> {
    let context = AcquisitionContext::new(ID, roots);
    let mut candidates = vec![];
    for path in databases(roots)? {
        check_capture_cancelled()?;
        let mut stamps = vec![];
        for suffix in ["", "-wal", "-journal"] {
            let p = sidecar(&path, suffix);
            if p.try_exists()? {
                context.validate(&p)?;
                stamps.push(crate::file_stamp_and_modified(&p)?.0);
            }
        }
        let stamp = stamps.join("|");
        for s in read(&context, &path, |c| {
            sessions(
                c,
                roots,
                &path,
                limit.unwrap_or(MAX_SESSIONS).min(MAX_SESSIONS) + 1,
                None,
            )
        })? {
            candidates.push(Candidate {
                source: ID,
                locator: path.to_string_lossy().into_owned(),
                session_id: Some(s.id),
                recency_hint_ms: Some(s.latest),
                stamp: stamp.clone(),
            });
        }
    }
    ensure!(
        candidates.len() <= MAX_SESSIONS,
        "Hermes discovery exceeds safety limit"
    );
    candidates.sort_by(|a, b| {
        b.recency_hint_ms
            .cmp(&a.recency_hint_ms)
            .then_with(|| a.session_id.cmp(&b.session_id))
    });
    candidates.truncate(limit.unwrap_or(MAX_SESSIONS));
    Ok(candidates)
}
pub(super) fn shallow(scan: &ScanEnv<'_>, c: &Candidate) -> Result<Option<ShallowSession>> {
    let context = AcquisitionContext::new(ID, scan.provider_roots);
    scan.note_open();
    scan.note_query();
    read(&context, Path::new(&c.locator), |conn| {
        let found = sessions(
            conn,
            scan.provider_roots,
            Path::new(&c.locator),
            1,
            Some(&native_id(c.session_id.as_deref().unwrap_or(""))?),
        )?
        .into_iter()
        .find(|s| Some(&s.id) == c.session_id.as_ref());
        let Some(s) = found else {
            return Ok(None);
        };
        scan.note_records(1);
        scan.note_query();
        let prompt = first_prompt(conn, &s.native_id)?;
        Ok(Some(ShallowSession {
            source: ID.into(),
            session_id: s.id,
            title: s.title,
            cwd: s.cwd,
            git_branch: s.git_branch,
            models: s.model.into_iter().collect(),
            first_prompt: prompt,
            first_activity_ms: Some(s.started),
            last_activity_ms: Some(s.latest),
            raw_path: Some(c.locator.clone()),
            source_stamp: Some(c.stamp.clone()),
            discovery_state: "shallow".into(),
            ..Default::default()
        }))
    })
}
use rusqlite::OptionalExtension;
pub(super) fn all(context: &AcquisitionContext<'_>, path: &Path) -> Result<Vec<Session>> {
    read(context, path, |conn| {
        let mut rows = sessions(conn, context.roots, path, MAX_SESSIONS + 1, None)?;
        ensure!(
            rows.len() <= MAX_SESSIONS,
            "Hermes session capture exceeds safety limit"
        );
        for row in &mut rows {
            check_capture_cancelled()?;
            row.messages = messages(conn, &row.native_id)?;
        }
        Ok(rows)
    })
}
pub(super) fn capture(
    context: &AcquisitionContext<'_>,
    path: &Path,
    id: &str,
) -> Result<AcquiredSession> {
    let session = read(context, path, |conn| {
        let mut s = sessions(conn, context.roots, path, 1, Some(&native_id(id)?))?
            .into_iter()
            .find(|s| s.id == id)
            .ok_or_else(|| {
                anyhow::anyhow!("SESSION_SOURCE_MISMATCH: Hermes session not in this profile")
            })?;
        s.messages = messages(conn, &s.native_id)?;
        Ok(s)
    })?;
    let records = session.messages.len() as i64;
    let mut read_paths = vec![path.to_path_buf()];
    for suffix in ["-wal", "-shm", "-journal"] {
        let sidecar = sidecar(path, suffix);
        if sidecar.try_exists()? {
            context.validate(&sidecar)?;
            read_paths.push(sidecar);
        }
    }
    Ok(AcquiredSession::new(
        serde_json::to_value(session)?,
        0,
        records,
        read_paths,
    ))
}

fn native_id(id: &str) -> Result<String> {
    let encoded = id.rsplit(':').next().unwrap_or("");
    ensure!(
        encoded.len().is_multiple_of(2),
        "invalid Hermes identity encoding"
    );
    let bytes = encoded
        .as_bytes()
        .chunks(2)
        .map(|pair| {
            let text = std::str::from_utf8(pair)?;
            Ok(u8::from_str_radix(text, 16)?)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(String::from_utf8(bytes)?)
}

fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(suffix);
    PathBuf::from(name)
}

pub(super) fn fingerprint_inputs(roots: &ProviderRoots) -> Result<Vec<Candidate>> {
    let context = AcquisitionContext::new(ID, roots);
    let mut result = vec![];
    for db in databases(roots)? {
        for suffix in ["", "-wal", "-journal"] {
            let path = sidecar(&db, suffix);
            if path.try_exists()? {
                context.validate(&path)?;
                let (stamp, modified) = crate::file_stamp_and_modified(&path)?;
                result.push(Candidate {
                    source: ID,
                    locator: path.to_string_lossy().into_owned(),
                    session_id: None,
                    recency_hint_ms: modified,
                    // Namespace is frozen acquisition metadata: identical files
                    // can project different ledger IDs. Keep the stat length last.
                    stamp: format!("{}/{stamp}", profile(roots, &db)),
                });
            }
        }
    }
    Ok(result)
}
pub(super) fn in_scope(path: &Path, scope: Option<&crate::ingest::path_scope::PathScope>) -> bool {
    scope.is_none_or(|scope| {
        scope.paths().iter().any(|changed| {
            path.starts_with(changed)
                || ["", "-wal", "-shm", "-journal"]
                    .iter()
                    .any(|suffix| *changed == sidecar(path, suffix))
        })
    })
}

pub(super) fn first_prompt(conn: &Connection, native: &str) -> Result<Option<String>> {
    Ok(conn.query_row("SELECT substr(content,1,512) FROM messages WHERE session_id=? AND role='user' AND content IS NOT NULL AND _compressed_summary=0 AND observed=0 AND (active=1 OR compacted=1) AND (display_kind IS NULL OR display_kind NOT IN ('hidden','async_delegation_complete')) ORDER BY id LIMIT 1", [native], |r| r.get::<_,Option<String>>(0)).optional()?.flatten())
}

// Absence is authoritative only inside an accessible configured root. Validate
// existing ancestors; unavailable roots and escaped symlinks do not prove deletion.
pub(super) fn confirmed_missing(roots: &ProviderRoots, path: &Path) -> Result<bool> {
    let context = AcquisitionContext::new(ID, roots);
    for root in roots.for_provider(ID) {
        let Ok(relative) = path.strip_prefix(root) else {
            continue;
        };
        if !native_database_path(relative) || !root.try_exists()? {
            continue;
        }
        match std::fs::symlink_metadata(path) {
            Ok(_) => {
                context.validate(path)?;
                return Ok(false);
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        context.validate(root)?;
        // Reading the root distinguishes accessible absence from denied roots.
        for entry in std::fs::read_dir(root)? {
            entry?;
        }
        let mut parent = path.parent();
        while let Some(dir) = parent {
            if dir == root {
                break;
            }
            validate_existing_directory(&context, dir)?;
            parent = dir.parent();
        }
        return Ok(!path.try_exists()?);
    }
    Ok(false)
}

fn native_database_path(relative: &Path) -> bool {
    let parts: Vec<_> = relative.components().collect();
    relative == Path::new("state.db")
        || (parts.len() == 3
            && parts[0].as_os_str() == "profiles"
            && parts[2].as_os_str() == "state.db"
            && matches!(parts[1], std::path::Component::Normal(_)))
}
fn validate_existing_directory(context: &AcquisitionContext<'_>, dir: &Path) -> Result<()> {
    match std::fs::symlink_metadata(dir) {
        Ok(_) => {
            context.validate(dir)?;
            for entry in std::fs::read_dir(dir)? {
                entry?;
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    Ok(())
}

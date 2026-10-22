use super::*;
use crate::ingest::check_capture_cancelled;
use anyhow::{ensure, Context};
use std::collections::HashMap;
const MAX_SESSIONS: usize = 100_000;
const MAX_MESSAGES: usize = 1_000_000;
const MAX_PARTS: usize = 2_000_000;
const MAX_METADATA_BYTES: i64 = 16 * 1024;
pub(super) const MAX_ROW_BYTES: i64 = 1024 * 1024;
const MAX_SESSION_BYTES: i64 = 64 * 1024 * 1024;

use super::roots::{identity, namespace};
use super::shallow::first_prompt;
fn native_id(id: &str) -> Result<String> {
    let encoded = id.rsplit(':').next().unwrap_or("");
    ensure!(
        encoded.len().is_multiple_of(2),
        "invalid Kilo identity encoding"
    );
    let bytes = encoded
        .as_bytes()
        .chunks(2)
        .map(|pair| Ok(u8::from_str_radix(std::str::from_utf8(pair)?, 16)?))
        .collect::<Result<Vec<_>>>()?;
    Ok(String::from_utf8(bytes)?)
}
fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(suffix);
    PathBuf::from(name)
}
fn sidecar_safe(path: &Path, suffix: &str) -> Result<Option<PathBuf>> {
    let candidate = sidecar(path, suffix);
    if !candidate.try_exists()? {
        return Ok(None);
    }
    let canonical = std::fs::canonicalize(&candidate)?;
    let parent = std::fs::canonicalize(path.parent().context("Kilo database has no parent")?)?;
    ensure!(
        canonical.parent() == Some(parent.as_path()),
        "Kilo SQLite sidecar escapes configured database directory"
    );
    Ok(Some(candidate))
}
pub(super) use super::roots::databases;
pub(super) fn confirmed_missing(roots: &ProviderRoots, path: &Path) -> Result<bool> {
    if !roots.for_provider(ID).iter().any(|p| p == path) {
        return Ok(false);
    }
    let Some(parent) = path.parent() else {
        return Ok(false);
    };
    match std::fs::metadata(parent) {
        Ok(meta) if meta.is_dir() => {
            if path.try_exists()? {
                return Ok(false);
            }
            for suffix in ["-wal", "-shm", "-journal"] {
                if sidecar_safe(path, suffix)?.is_some() {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        Ok(_) => Ok(false),
        Err(_) => Ok(false),
    }
}
pub(super) fn schema(conn: &Connection) -> Result<()> {
    for (table, fields) in [
        (
            "session",
            &[
                "id",
                "title",
                "directory",
                "parent_id",
                "time_created",
                "time_updated",
            ][..],
        ),
        ("message", &["id", "session_id", "time_created", "data"][..]),
        (
            "part",
            &["id", "message_id", "session_id", "time_created", "data"][..],
        ),
    ] {
        let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
        let found = stmt
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<rusqlite::Result<std::collections::BTreeSet<_>>>()?;
        ensure!(
            fields.iter().all(|field| found.contains(*field)),
            "unsupported Kilo SQLite schema: {table} lacks required columns"
        );
    }
    Ok(())
}
fn read<T>(
    context: &AcquisitionContext<'_>,
    path: &Path,
    f: impl FnOnce(&Connection) -> Result<T>,
) -> Result<T> {
    check_capture_cancelled()?;
    ensure!(
        context
            .roots
            .for_provider(ID)
            .iter()
            .any(|root| root == path),
        "Kilo locator must be a configured database"
    );
    context.validate(path)?;
    for suffix in ["-wal", "-shm", "-journal"] {
        let _ = sidecar_safe(path, suffix)?;
    }
    sqlite_snapshot(context, path, |conn| {
        schema(conn)?;
        f(conn)
    })
}
fn sessions(
    conn: &Connection,
    roots: &ProviderRoots,
    path: &Path,
    selected: Option<&str>,
    limit: usize,
) -> Result<Vec<Session>> {
    let ns = namespace(roots, path);
    let mut stmt = conn.prepare("SELECT CASE WHEN length(CAST(id AS BLOB))<=?3 THEN id ELSE '' END,CASE WHEN length(CAST(title AS BLOB))<=?3 THEN title ELSE '' END,CASE WHEN length(CAST(directory AS BLOB))<=?3 THEN directory ELSE '' END,CASE WHEN length(CAST(parent_id AS BLOB))<=?3 THEN parent_id ELSE NULL END,time_created,time_updated,length(CAST(id AS BLOB)),length(CAST(title AS BLOB)),length(CAST(directory AS BLOB)),length(CAST(parent_id AS BLOB)) FROM session WHERE (?1 IS NULL OR id=?1) ORDER BY time_updated DESC,id LIMIT ?2")?;
    let rows = stmt.query_map(
        rusqlite::params![selected, limit as i64, MAX_METADATA_BYTES],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, i64>(5)?,
                row.get::<_, i64>(6)?,
                row.get::<_, i64>(7)?,
                row.get::<_, i64>(8)?,
                row.get::<_, Option<i64>>(9)?,
            ))
        },
    )?;
    let mut result = vec![];
    let mut metadata_bytes = 0_i64;
    for row in rows {
        check_capture_cancelled()?;
        let (
            native_id,
            title,
            cwd,
            parent,
            started,
            latest,
            id_len,
            title_len,
            cwd_len,
            parent_len,
        ) = row?;
        let invalid_metadata = title_len > MAX_METADATA_BYTES
            || cwd_len > MAX_METADATA_BYTES
            || parent_len.unwrap_or(0) > MAX_METADATA_BYTES;
        metadata_bytes += id_len
            + title_len.min(MAX_METADATA_BYTES)
            + cwd_len.min(MAX_METADATA_BYTES)
            + parent_len.unwrap_or(0).min(MAX_METADATA_BYTES);
        ensure!(
            metadata_bytes <= MAX_SESSION_BYTES,
            "Kilo session listing exceeds source size limit"
        );
        ensure!(
            !native_id.is_empty() && native_id.len() <= 512,
            "invalid Kilo native session ID"
        );
        ensure!(
            started >= 0 && latest >= 0,
            "invalid Kilo session timestamp"
        );
        result.push(Session {
            id: identity(&ns, &native_id),
            native_id,
            namespace: ns.clone(),
            title,
            cwd,
            parent,
            started,
            latest,
            invalid_metadata,
            messages: vec![],
        });
    }
    Ok(result)
}
fn messages(conn: &Connection, native: &str) -> Result<Vec<Message>> {
    let mut stmt = conn.prepare("SELECT CASE WHEN length(CAST(id AS BLOB))<=512 THEN id ELSE '' END,time_created,CASE WHEN length(CAST(data AS BLOB))<=?3 THEN data ELSE '' END,length(CAST(data AS BLOB)) FROM message WHERE session_id=?1 ORDER BY time_created,id LIMIT ?2")?;
    let rows = stmt.query_map(
        rusqlite::params![native, (MAX_MESSAGES + 1) as i64, MAX_ROW_BYTES],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
            ))
        },
    )?;
    let mut result = vec![];
    let mut total_bytes = 0_i64;
    for row in rows {
        check_capture_cancelled()?;
        let (id, timestamp, raw, bytes) = row?;
        ensure!(bytes <= MAX_ROW_BYTES, "oversized Kilo message data");
        total_bytes += bytes;
        ensure!(
            total_bytes <= MAX_SESSION_BYTES,
            "Kilo session exceeds source size limit"
        );
        ensure!(
            !id.is_empty() && id.len() <= 512 && timestamp >= 0,
            "invalid Kilo message identity/timestamp"
        );
        let data: Value = serde_json::from_str(&raw).context("malformed Kilo message data")?;
        let role = data["role"].as_str().context("Kilo message role missing")?;
        ensure!(
            ["user", "assistant"].contains(&role),
            "unsupported Kilo message role {role}"
        );
        result.push(Message {
            id,
            role: role.into(),
            timestamp,
            data,
            parts: vec![],
        });
    }
    ensure!(
        result.len() <= MAX_MESSAGES,
        "Kilo message capture exceeds safety limit"
    );
    read_parts(conn, native, &mut result, &mut total_bytes)?;
    Ok(result)
}
fn read_parts(
    conn: &Connection,
    native: &str,
    result: &mut [Message],
    total_bytes: &mut i64,
) -> Result<()> {
    let positions: HashMap<_, _> = result
        .iter()
        .enumerate()
        .map(|(index, message)| (message.id.clone(), index))
        .collect();
    let mut stmt = conn.prepare("SELECT CASE WHEN length(CAST(id AS BLOB))<=512 THEN id ELSE '' END,CASE WHEN length(CAST(message_id AS BLOB))<=512 THEN message_id ELSE '' END,time_created,CASE WHEN length(CAST(data AS BLOB))<=?3 THEN data ELSE '' END,length(CAST(data AS BLOB)) FROM part WHERE session_id=?1 ORDER BY message_id,id LIMIT ?2")?;
    let rows = stmt.query_map(
        rusqlite::params![native, (MAX_PARTS + 1) as i64, MAX_ROW_BYTES],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, i64>(4)?,
            ))
        },
    )?;
    let mut count = 0;
    for row in rows {
        check_capture_cancelled()?;
        let (id, message_id, timestamp, raw, bytes) = row?;
        count += 1;
        ensure!(count <= MAX_PARTS, "Kilo part capture exceeds safety limit");
        let Some(&position) = positions.get(message_id.as_str()) else {
            continue;
        };
        ensure!(bytes <= MAX_ROW_BYTES, "oversized Kilo part data");
        *total_bytes += bytes;
        ensure!(
            *total_bytes <= MAX_SESSION_BYTES,
            "Kilo session exceeds source size limit"
        );
        ensure!(
            !id.is_empty() && id.len() <= 512 && timestamp >= 0,
            "invalid Kilo part identity/timestamp"
        );
        let data = serde_json::from_str(&raw).context("malformed Kilo part data")?;
        result[position].parts.push(Part {
            id,
            timestamp,
            data,
        });
    }
    Ok(())
}
pub(super) fn enumerate(roots: &ProviderRoots, limit: Option<usize>) -> Result<Vec<Candidate>> {
    let context = AcquisitionContext::new(ID, roots);
    let mut candidates = vec![];
    let (paths, unread) = databases(roots)?;
    ensure!(
        !paths.is_empty() || unread == 0,
        "Kilo configured database roots are unreadable"
    );
    for path in paths {
        let per_path = (|| -> Result<Vec<Candidate>> {
            let mut stamps = vec![];
            for suffix in ["", "-wal", "-journal"] {
                let candidate = if suffix.is_empty() {
                    Some(path.clone())
                } else {
                    sidecar_safe(&path, suffix)?
                };
                if let Some(candidate) = candidate {
                    stamps.push(crate::file_stamp_and_modified(&candidate)?.0);
                }
            }
            let stamp = stamps.join("|");
            let rows = read(&context, &path, |conn| {
                sessions(conn, roots, &path, None, MAX_SESSIONS + 1)
            })?;
            ensure!(
                rows.len() <= MAX_SESSIONS,
                "Kilo discovery exceeds safety limit"
            );
            Ok(rows
                .into_iter()
                .map(|session| Candidate {
                    source: ID,
                    locator: path.to_string_lossy().into_owned(),
                    session_id: Some(session.id),
                    recency_hint_ms: Some(session.latest),
                    stamp: stamp.clone(),
                })
                .collect())
        })();
        match per_path {
            Ok(found) => candidates.extend(found),
            Err(error) => {
                check_capture_cancelled()?;
                eprintln!("Kilo {} discovery: {error:#}", path.display());
            }
        }
    }
    candidates.sort_by(|a, b| {
        b.recency_hint_ms
            .cmp(&a.recency_hint_ms)
            .then_with(|| a.session_id.cmp(&b.session_id))
    });
    candidates.truncate(limit.unwrap_or(MAX_SESSIONS));
    Ok(candidates)
}
pub(super) fn shallow(scan: &ScanEnv<'_>, candidate: &Candidate) -> Result<Option<ShallowSession>> {
    let path = Path::new(&candidate.locator);
    let context = AcquisitionContext::new(ID, scan.provider_roots);
    scan.note_open();
    scan.note_query();
    read(&context, path, |conn| {
        let native = native_id(candidate.session_id.as_deref().unwrap_or(""))?;
        let found = sessions(conn, scan.provider_roots, path, Some(&native), 1)?
            .into_iter()
            .find(|s| Some(&s.id) == candidate.session_id.as_ref());
        let Some(session) = found else {
            return Ok(None);
        };
        ensure!(!session.invalid_metadata, "oversized Kilo session metadata");
        scan.note_records(1);
        scan.note_query();
        let prompt = first_prompt(conn, &native)?;
        Ok(Some(ShallowSession {
            source: ID.into(),
            session_id: session.id,
            title: Some(session.title),
            cwd: Some(session.cwd),
            first_prompt: prompt,
            first_activity_ms: Some(session.started),
            last_activity_ms: Some(session.latest),
            raw_path: Some(candidate.locator.clone()),
            source_stamp: Some(candidate.stamp.clone()),
            discovery_state: "shallow".into(),
            ..Default::default()
        }))
    })
}
pub(super) fn scan(
    context: &AcquisitionContext<'_>,
    path: &Path,
    mut visit: impl FnMut(&str, Result<Session>) -> Result<()>,
) -> Result<Vec<String>> {
    read(context, path, |conn| {
        let rows = sessions(conn, context.roots, path, None, MAX_SESSIONS + 1)?;
        ensure!(
            rows.len() <= MAX_SESSIONS,
            "Kilo session scan exceeds safety limit"
        );
        let ids = rows.iter().map(|s| s.id.clone()).collect();
        for mut session in rows {
            check_capture_cancelled()?;
            let id = session.id.clone();
            if session.invalid_metadata {
                visit(&id, Err(anyhow::anyhow!("oversized Kilo session metadata")))?;
                continue;
            }
            visit(
                &id,
                messages(conn, &session.native_id).map(|messages| {
                    session.messages = messages;
                    session
                }),
            )?;
        }
        Ok(ids)
    })
}
pub(super) fn capture(
    context: &AcquisitionContext<'_>,
    path: &Path,
    id: &str,
) -> Result<AcquiredSession> {
    let session = read(context, path, |conn| {
        let native = native_id(id)?;
        let mut session = sessions(conn, context.roots, path, Some(&native), 1)?
            .into_iter()
            .find(|s| s.id == id)
            .ok_or_else(|| {
                anyhow::anyhow!("SESSION_SOURCE_MISMATCH: Kilo session not in this database")
            })?;
        ensure!(!session.invalid_metadata, "oversized Kilo session metadata");
        session.messages = messages(conn, &native)?;
        Ok(session)
    })?;
    let count = session.messages.len() as i64;
    // The configured root is the exact database file. SQLite sidecars are
    // validated and fingerprinted above, while the transaction already
    // includes their committed rows. Only the primary locator is handed to
    // the generic exact-root validation layer.
    Ok(AcquiredSession::new(
        serde_json::to_value(session)?,
        0,
        count,
        vec![path.to_path_buf()],
    ))
}
pub(super) fn fingerprint_inputs(roots: &ProviderRoots) -> Result<Vec<Candidate>> {
    let mut result = vec![];
    let (paths, unread) = databases(roots)?;
    ensure!(
        !paths.is_empty() || unread == 0,
        "Kilo configured database roots are unreadable"
    );
    for path in paths {
        let per_path = (|| -> Result<Vec<Candidate>> {
            let mut found = vec![];
            for suffix in ["", "-wal", "-journal"] {
                let candidate = if suffix.is_empty() {
                    Some(path.clone())
                } else {
                    sidecar_safe(&path, suffix)?
                };
                if let Some(candidate) = candidate {
                    let (stamp, modified) = crate::file_stamp_and_modified(&candidate)?;
                    found.push(Candidate {
                        source: ID,
                        locator: candidate.to_string_lossy().into_owned(),
                        session_id: None,
                        recency_hint_ms: modified,
                        stamp: format!("{}/{stamp}", namespace(roots, &path)),
                    });
                }
            }
            Ok(found)
        })();
        match per_path {
            Ok(found) => result.extend(found),
            Err(error) => {
                check_capture_cancelled()?;
                eprintln!("Kilo {} fingerprint: {error:#}", path.display());
            }
        }
    }
    Ok(result)
}
pub(super) fn in_scope(path: &Path, scope: Option<&crate::ingest::path_scope::PathScope>) -> bool {
    scope.is_none_or(|scope| {
        scope.paths().iter().any(|changed| {
            path.starts_with(changed)
                || ["-wal", "-shm", "-journal"]
                    .iter()
                    .any(|suffix| *changed == sidecar(path, suffix))
        })
    })
}

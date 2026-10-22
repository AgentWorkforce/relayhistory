use super::lifecycle::sidecar;
use super::*;
use crate::ingest::check_capture_cancelled;
use anyhow::{ensure, Context};
use rusqlite::OptionalExtension;
use sha2::{Digest, Sha256};
const MAX_THREADS: usize = 100_000;
const MAX_WRITES: usize = 100_000;
const MAX_THREAD_BYTES: usize = 64 * 1024 * 1024;
mod discovery;
pub(super) use discovery::{enumerate, fingerprint_inputs};

fn acknowledge(context: &AcquisitionContext<'_>, path: &Path) -> Result<bool> {
    if !path.try_exists()? {
        return Ok(false);
    }
    context.validate(path)?;
    Ok(true)
}
fn read<T>(
    context: &AcquisitionContext<'_>,
    path: &Path,
    f: impl FnOnce(&Connection) -> Result<T>,
) -> Result<T> {
    context.validate(path)?;
    ensure!(
        context.roots.for_provider(ID).contains(&path.to_path_buf()),
        "DeepAgents locator is not a configured database"
    );
    for suffix in ["-wal", "-shm", "-journal"] {
        acknowledge(context, &sidecar(path, suffix))?;
    }
    sqlite_snapshot(context, path, |conn| {
        for table in ["checkpoints", "writes"] {
            let exists: bool = conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name=?1)",
                [table],
                |row| row.get(0),
            )?;
            ensure!(exists, "unsupported DeepAgents database: missing {table}");
        }
        f(conn)
    })
}
fn thread_ids(conn: &Connection) -> Result<Vec<String>> {
    let mut stmt = conn.prepare("SELECT DISTINCT CASE WHEN length(thread_id)<=512 THEN thread_id ELSE NULL END FROM checkpoints WHERE checkpoint_ns='' ORDER BY thread_id LIMIT 100001")?;
    let mut out = vec![];
    for item in stmt.query_map([], |row| row.get::<_, Option<String>>(0))? {
        check_capture_cancelled()?;
        out.push(item?.ok_or_else(|| anyhow::anyhow!("DeepAgents thread ID exceeds limit"))?);
    }
    ensure!(
        out.len() <= MAX_THREADS,
        "DeepAgents thread count exceeds limit"
    );
    Ok(out)
}
fn timestamp(metadata: &serde_json::Value, field: &str) -> Result<i64> {
    let value = metadata
        .get(field)
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("DeepAgents metadata lacks {field}"))?;
    let ms = chrono::DateTime::parse_from_rfc3339(value)
        .with_context(|| format!("invalid DeepAgents {field}"))?
        .timestamp_millis();
    ensure!(ms > 0, "invalid DeepAgents {field}");
    Ok(ms)
}
fn metadata(raw: &[u8]) -> Result<serde_json::Value> {
    let value: serde_json::Value =
        serde_json::from_slice(raw).context("malformed DeepAgents checkpoint metadata")?;
    ensure!(value.is_object(), "DeepAgents metadata must be an object");
    Ok(value)
}
pub(super) fn identity(roots: &ProviderRoots, path: &Path, native: &str) -> String {
    let namespace = if roots
        .default_for_provider(ID)
        .is_some_and(|default| default == path)
    {
        "default".to_owned()
    } else {
        format!(
            "root:{:x}",
            Sha256::digest(path.as_os_str().as_encoded_bytes())
        )
    };
    format!(
        "{namespace}:{}",
        native
            .as_bytes()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    )
}
fn native(id: &str) -> Result<String> {
    let hex = id
        .rsplit(':')
        .next()
        .ok_or_else(|| anyhow::anyhow!("invalid DeepAgents identity"))?;
    ensure!(
        hex.len().is_multiple_of(2) && hex.len() <= 1024,
        "invalid DeepAgents identity encoding"
    );
    let bytes = hex
        .as_bytes()
        .chunks(2)
        .map(|pair| Ok(u8::from_str_radix(std::str::from_utf8(pair)?, 16)?))
        .collect::<Result<Vec<_>>>()?;
    Ok(String::from_utf8(bytes)?)
}
fn latest(conn: &Connection, thread: &str) -> Result<(String, String, Vec<u8>, serde_json::Value)> {
    latest_metadata(conn, thread)?;
    let row = conn.query_row("SELECT checkpoint_id,type,checkpoint,metadata FROM checkpoints WHERE thread_id=?1 AND checkpoint_ns='' ORDER BY checkpoint_id DESC LIMIT 1", [thread], |r| Ok((r.get::<_, String>(0)?,r.get::<_, String>(1)?,r.get::<_, Vec<u8>>(2)?,r.get::<_, Vec<u8>>(3)?))).optional()?;
    let (id, kind, blob, raw) =
        row.ok_or_else(|| anyhow::anyhow!("DeepAgents thread has no root checkpoint"))?;
    ensure!(
        kind == "msgpack",
        "unsupported DeepAgents checkpoint encoding {kind}"
    );
    ensure!(
        blob.len() <= decode::MAX_BLOB,
        "DeepAgents checkpoint blob exceeds limit"
    );
    Ok((id, kind, blob, metadata(&raw)?))
}
fn latest_metadata(conn: &Connection, thread: &str) -> Result<(String, serde_json::Value)> {
    let (id, kind, bytes, meta_bytes): (Option<String>, Option<String>, i64, i64) = conn.query_row(
        "SELECT CASE WHEN length(checkpoint_id)<=256 THEN checkpoint_id ELSE NULL END,CASE WHEN length(type)<=64 THEN type ELSE NULL END,length(checkpoint),length(metadata) FROM checkpoints WHERE thread_id=?1 AND checkpoint_ns='' ORDER BY checkpoint_id DESC LIMIT 1",
        [thread], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))?;
    let id = id.ok_or_else(|| anyhow::anyhow!("DeepAgents checkpoint ID exceeds limit"))?;
    let kind =
        kind.ok_or_else(|| anyhow::anyhow!("DeepAgents checkpoint encoding exceeds limit"))?;
    ensure!(
        kind == "msgpack",
        "unsupported DeepAgents checkpoint encoding {kind}"
    );
    ensure!(
        bytes >= 0 && bytes as usize <= decode::MAX_BLOB,
        "DeepAgents checkpoint blob exceeds limit"
    );
    ensure!(
        (0..=65_536).contains(&meta_bytes),
        "DeepAgents metadata exceeds limit"
    );
    let raw: Vec<u8> = conn.query_row("SELECT metadata FROM checkpoints WHERE thread_id=?1 AND checkpoint_ns='' ORDER BY checkpoint_id DESC LIMIT 1", [thread], |r| r.get(0))?;
    Ok((id, metadata(&raw)?))
}
fn latest_checkpoint_time(conn: &Connection, thread: &str) -> Result<i64> {
    let bytes: i64 = conn.query_row(
        "SELECT length(checkpoint) FROM checkpoints WHERE thread_id=?1 AND checkpoint_ns='' ORDER BY checkpoint_id DESC LIMIT 1",
        [thread],
        |r| r.get(0),
    )?;
    ensure!(
        bytes >= 0 && bytes as usize <= decode::MAX_BLOB,
        "DeepAgents recency checkpoint exceeds limit"
    );
    let blob: Vec<u8> = conn.query_row(
        "SELECT checkpoint FROM checkpoints WHERE thread_id=?1 AND checkpoint_ns='' ORDER BY checkpoint_id DESC LIMIT 1",
        [thread],
        |r| r.get(0),
    )?;
    decode::checkpoint_time(&blob)
}
fn first_time(conn: &Connection, thread: &str) -> Result<i64> {
    let (bytes, meta_bytes): (i64, i64) = conn.query_row("SELECT length(checkpoint),length(metadata) FROM checkpoints WHERE thread_id=?1 AND checkpoint_ns='' ORDER BY checkpoint_id ASC LIMIT 1", [thread], |r| Ok((r.get(0)?,r.get(1)?)))?;
    ensure!(
        (0..=65_536).contains(&meta_bytes),
        "DeepAgents metadata exceeds limit"
    );
    let raw: Vec<u8> = conn.query_row("SELECT metadata FROM checkpoints WHERE thread_id=?1 AND checkpoint_ns='' ORDER BY checkpoint_id ASC LIMIT 1", [thread], |r| r.get(0))?;
    let meta = metadata(&raw)?;
    timestamp(&meta, "updated_at").or_else(|_| {
        ensure!(
            bytes >= 0 && bytes as usize <= decode::MAX_BLOB,
            "DeepAgents first checkpoint blob exceeds limit"
        );
        let blob: Vec<u8> = conn.query_row("SELECT checkpoint FROM checkpoints WHERE thread_id=?1 AND checkpoint_ns='' ORDER BY checkpoint_id ASC LIMIT 1", [thread], |r| r.get(0))?;
        decode::checkpoint_time(&blob)
    })
}
fn native_creation_time(thread: &str) -> Option<i64> {
    // dcode generates UUIDv7 thread IDs. Its high 48 bits are the Unix
    // millisecond creation time, independent of checkpoint pruning.
    let bytes = thread.as_bytes();
    if bytes.len() != 36
        || bytes.get(14) != Some(&b'7')
        || bytes.get(8) != Some(&b'-')
        || bytes.get(13) != Some(&b'-')
        || bytes.get(18) != Some(&b'-')
        || bytes.get(23) != Some(&b'-')
    {
        return None;
    }
    let millis = i64::from_str_radix(&thread[..8], 16)
        .ok()?
        .checked_shl(16)?
        + i64::from_str_radix(&thread[9..13], 16).ok()?;
    (millis > 0).then_some(millis)
}
fn session(
    conn: &Connection,
    roots: &ProviderRoots,
    path: &Path,
    thread: &str,
    with_messages: bool,
) -> Result<Session> {
    ensure!(
        !thread.is_empty() && thread.len() <= 512,
        "invalid DeepAgents thread ID"
    );
    let (checkpoint_id, meta) = latest_metadata(conn, thread)?;
    let started = match native_creation_time(thread) {
        Some(ms) => ms,
        None => first_time(conn, thread)?,
    };
    let full_checkpoint = if with_messages {
        Some(latest(conn, thread)?.2)
    } else {
        None
    };
    let latest_ms = timestamp(&meta, "updated_at").or_else(|_| {
        if let Some(blob) = &full_checkpoint {
            decode::checkpoint_time(blob)
        } else {
            latest_checkpoint_time(conn, thread)
        }
    })?;
    let messages = if let Some(blob) = full_checkpoint.as_deref() {
        read_messages(conn, thread, &checkpoint_id, blob)?
    } else {
        Vec::new()
    };
    let has_names: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='dcode_thread_names')", [], |r| r.get(0))?;
    let title: Option<String> = if has_names {
        conn.query_row("SELECT CASE WHEN length(name)<=4096 THEN name ELSE NULL END FROM dcode_thread_names WHERE thread_id=?1", [thread], |r| r.get(0)).optional()?.flatten()
    } else {
        None
    };
    let title = title.or_else(|| {
        meta.get("thread_name")
            .and_then(serde_json::Value::as_str)
            .filter(|value| value.len() <= 4096)
            .map(str::to_owned)
    });
    Ok(Session {
        native_id: thread.into(),
        id: identity(roots, path, thread),
        started,
        latest: latest_ms,
        title,
        cwd: meta
            .get("cwd")
            .and_then(serde_json::Value::as_str)
            .filter(|value| value.len() <= 4096)
            .map(str::to_owned),
        git_branch: meta
            .get("git_branch")
            .and_then(serde_json::Value::as_str)
            .filter(|value| value.len() <= 1024)
            .map(str::to_owned),
        checkpoint_id,
        messages,
    })
}
fn read_messages(
    conn: &Connection,
    thread: &str,
    checkpoint_id: &str,
    blob: &[u8],
) -> Result<Vec<Message>> {
    let inline_messages = decode::inline(blob)?;
    let inline = inline_messages.is_some();
    let mut messages = decode::Fold::new(inline_messages.unwrap_or_default())?;
    {
        let sql = if inline {
            "SELECT type,value FROM writes WHERE thread_id=?1 AND checkpoint_ns='' AND checkpoint_id=?2 AND channel='messages' ORDER BY task_id,idx LIMIT 100001"
        } else {
            "SELECT type,value FROM writes WHERE thread_id=?1 AND checkpoint_ns='' AND channel='messages' AND checkpoint_id<=?2 ORDER BY checkpoint_id,task_id,idx LIMIT 100001"
        };
        let sql = sql.replace("SELECT type,value", "SELECT rowid,type,length(value)");
        let mut stmt = conn.prepare(&sql)?;
        let mut rows = stmt.query(rusqlite::params![thread, checkpoint_id])?;
        let mut count = 0;
        let mut total_bytes = blob.len();
        while let Some(row) = rows.next()? {
            check_capture_cancelled()?;
            count += 1;
            ensure!(count <= MAX_WRITES, "DeepAgents write count exceeds limit");
            let rowid: i64 = row.get(0)?;
            let kind: String = row.get(1)?;
            let bytes: i64 = row.get(2)?;
            ensure!(
                bytes >= 0 && bytes as usize <= decode::MAX_BLOB,
                "DeepAgents write blob exceeds limit"
            );
            total_bytes = total_bytes
                .checked_add(bytes as usize)
                .ok_or_else(|| anyhow::anyhow!("DeepAgents capture size overflow"))?;
            ensure!(
                total_bytes <= MAX_THREAD_BYTES,
                "DeepAgents thread capture exceeds 64 MiB"
            );
            let value: Vec<u8> =
                conn.query_row("SELECT value FROM writes WHERE rowid=?1", [rowid], |r| {
                    r.get(0)
                })?;
            ensure!(
                kind == "msgpack",
                "unsupported DeepAgents write encoding {kind}"
            );
            messages.apply(decode::write(&value)?)?;
        }
    }
    Ok(messages.finish())
}
pub(super) fn shallow(scan: &ScanEnv<'_>, candidate: &Candidate) -> Result<Option<ShallowSession>> {
    let context = AcquisitionContext::new(ID, scan.provider_roots);
    let thread = native(candidate.session_id.as_deref().unwrap_or(""))?;
    scan.note_open();
    scan.note_query();
    read(&context, Path::new(&candidate.locator), |conn| {
        let s = session(
            conn,
            scan.provider_roots,
            Path::new(&candidate.locator),
            &thread,
            false,
        )?;
        scan.note_records(1);
        Ok(Some(ShallowSession {
            source: ID.into(),
            session_id: s.id,
            title: s.title,
            cwd: s.cwd,
            git_branch: s.git_branch,
            first_prompt: None,
            first_activity_ms: Some(s.started),
            last_activity_ms: Some(s.latest),
            raw_path: Some(candidate.locator.clone()),
            source_stamp: Some(candidate.stamp.clone()),
            discovery_state: "shallow".into(),
            ..Default::default()
        }))
    })
}
#[cfg(test)]
pub(super) struct Capture {
    pub sessions: Vec<Session>,
    pub unread: u64,
}
#[cfg(test)]
pub(super) fn all(context: &AcquisitionContext<'_>, path: &Path) -> Result<Capture> {
    let mut sessions = Vec::new();
    let unread = visit(context, path, |session| {
        sessions.push(session);
        Ok(true)
    })?
    .unread;
    Ok(Capture { sessions, unread })
}
pub(super) struct Visit {
    pub ids: Vec<String>,
    pub unread: u64,
}
pub(super) fn visit(
    context: &AcquisitionContext<'_>,
    path: &Path,
    mut consume: impl FnMut(Session) -> Result<bool>,
) -> Result<Visit> {
    read(context, path, |conn| {
        let ids = thread_ids(conn)?;
        let mut unread = 0;
        for id in &ids {
            check_capture_cancelled()?;
            match session(conn, context.roots, path, id, true) {
                Ok(value) => {
                    if !consume(value)? {
                        unread += 1;
                    }
                }
                Err(_) => unread += 1,
            }
        }
        Ok(Visit { ids, unread })
    })
}
pub(super) fn capture(
    context: &AcquisitionContext<'_>,
    path: &Path,
    id: &str,
) -> Result<AcquiredSession> {
    let thread = native(id)?;
    let s = read(context, path, |conn| {
        session(conn, context.roots, path, &thread, true)
    })?;
    ensure!(
        s.id == id,
        "SESSION_SOURCE_MISMATCH: DeepAgents identity mismatch"
    );
    let records = s.messages.len() as i64;
    let mut paths = vec![path.to_path_buf()];
    for suffix in ["-wal", "-shm", "-journal"] {
        let file = sidecar(path, suffix);
        if acknowledge(context, &file)? {
            paths.push(file);
        }
    }
    Ok(AcquiredSession::new(
        serde_json::to_value(s)?,
        0,
        records,
        paths,
    ))
}

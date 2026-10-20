use super::schema::Schema;
use super::*;
use crate::ingest::check_capture_cancelled;
use anyhow::{ensure, Context};
use sha2::{Digest, Sha256};
pub(super) const DATABASE: &str = "openclaw-agent.sqlite";
const MAX_SESSIONS: usize = 100_000;

/// Ledger namespace for one agent database: the agent ID under the native
/// state directory, else that ID qualified by a hash of the custom root.
pub(super) fn namespace(roots: &ProviderRoots, path: &Path) -> Result<String> {
    let agent = agent_id(path)?;
    let state = path
        .ancestors()
        .nth(4)
        .context("OpenClaw database outside agents/<id>/agent")?;
    if roots.default_for_provider(ID) == Some(state) {
        return Ok(agent.into());
    }
    let digest = Sha256::digest(state.as_os_str().as_encoded_bytes());
    let hex: String = digest[..6].iter().map(|b| format!("{b:02x}")).collect();
    Ok(format!("root-{hex}:{agent}"))
}
/// The directory name under `agents/` is upstream's normalized agent ID.
pub(super) fn agent_id(path: &Path) -> Result<&str> {
    let agent = path
        .parent()
        .and_then(Path::parent)
        .and_then(Path::file_name)
        .and_then(|n| n.to_str())
        .context("OpenClaw agent directory name is not UTF-8")?;
    ensure!(
        !agent.is_empty()
            && agent
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
            && !agent.starts_with('.'),
        "OpenClaw agent directory {agent:?} is not a normalized agent ID"
    );
    Ok(agent)
}
fn identity(namespace: &str, native: &str) -> String {
    format!("{namespace}:{native}")
}
fn native_id<'a>(namespace: &str, id: &'a str) -> Result<&'a str> {
    id.strip_prefix(namespace)
        .and_then(|rest| rest.strip_prefix(':'))
        .filter(|native| !native.is_empty())
        .context("SESSION_SOURCE_MISMATCH: OpenClaw session not in this agent database")
}

pub(super) fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(suffix);
    PathBuf::from(name)
}
const SIDECARS: [&str; 3] = ["-wal", "-shm", "-journal"];

/// A WAL, shm or rollback sidecar that is present and inside the provider
/// root. A file another connection unlinks mid-check is absent.
fn acknowledge_sidecar(context: &AcquisitionContext<'_>, path: &Path) -> Result<bool> {
    if !path.try_exists()? {
        return Ok(false);
    }
    match context.validate(path) {
        Ok(()) => Ok(true),
        Err(error) if is_not_found(&error) => Ok(false),
        Err(error) => Err(error),
    }
}
fn is_not_found(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io| io.kind() == std::io::ErrorKind::NotFound)
    })
}

/// Open one coherent read-only snapshot of an agent database. The gateway
/// keeps it in WAL mode; a reader never checkpoints, migrates or changes it.
pub(super) fn read<T>(
    context: &AcquisitionContext<'_>,
    path: &Path,
    f: impl FnOnce(&Connection, &Schema) -> Result<T>,
) -> Result<T> {
    check_capture_cancelled()?;
    context.validate(path)?;
    ensure!(
        path.file_name().is_some_and(|n| n == DATABASE),
        "OpenClaw locator must name {DATABASE}"
    );
    for suffix in SIDECARS {
        acknowledge_sidecar(context, &sidecar(path, suffix))?;
    }
    let agent = agent_id(path)?;
    sqlite_snapshot(context, path, |conn| {
        let schema = schema::detect(conn, agent)?;
        f(conn, &schema)
    })
}

/// One window per transcript, hot (rows in `transcript_events`) or cold
/// (moved into a verified archive). Recency aggregates the event table once;
/// a selected capture aggregates only that session.
pub(super) fn session_query(selected: bool, schema: &Schema) -> String {
    let predicate = if selected { "WHERE session_id=?1" } else { "" };
    let cold = schema.cold_archives();
    format!(
        "SELECT w.session_id, w.session_key, w.previous_session_id, w.reason, w.session_scope, \
         w.created_at, w.started_at, w.ended_at, w.status, w.chat_type, w.channel, \
         w.model_provider, w.model, w.agent_harness_id, w.parent_session_key, w.spawned_by, \
         coalesce(n.label, n.display_name, w.display_name), n.created_via, \
         e.events, e.last_seq, \
         coalesce(e.latest, w.transcript_updated_at, w.updated_at), coalesce(e.first, w.created_at), \
         r.generation, w.updated_at, w.transcript_updated_at, w.account_id, \
         c.generation, c.archive_name, c.archive_sha256, c.event_count, c.raw_bytes, \
         c.archive_bytes, c.last_seq, c.archived_at, c.storage, n.updated_at \
         FROM session_windows w \
         LEFT JOIN (SELECT session_id, count(*) AS events, max(seq) AS last_seq, \
               max(created_at) AS latest, min(created_at) AS first \
               FROM transcript_events {predicate} GROUP BY session_id) e ON e.session_id=w.session_id \
         LEFT JOIN session_nodes n ON n.session_key=w.session_key \
         LEFT JOIN transcript_rewrite_watermarks r ON r.session_id=w.session_id \
         LEFT JOIN {cold} c ON c.session_id=w.session_id \
         WHERE (?1 IS NULL OR w.session_id=?1) AND (e.events IS NOT NULL OR c.session_id IS NOT NULL) \
         ORDER BY 21 DESC, w.session_id LIMIT ?2"
    )
}
/// Sessions of one agent database, inside the caller's snapshot.
pub(super) fn sessions(
    conn: &Connection,
    roots: &ProviderRoots,
    path: &Path,
    schema: &Schema,
    limit: usize,
    native: Option<&str>,
) -> Result<Vec<Session>> {
    if !schema.transcripts {
        return Ok(vec![]);
    }
    let namespace = namespace(roots, path)?;
    let agent = agent_id(path)?;
    let mut stmt = conn.prepare(&session_query(native.is_some(), schema))?;
    let mut rows = stmt.query(rusqlite::params![native, limit as i64])?;
    let mut result = vec![];
    while let Some(r) = rows.next()? {
        check_capture_cancelled()?;
        let mut session = session_row(r, &namespace, agent, schema.version)?;
        if let Some(archive) = session.cold.as_ref().filter(|c| c.storage == "file") {
            // A file-backed archive changes outside the database: its stat
            // (never its contents) is part of the session's stamp.
            let file = cold::archive_path(path, &archive.name)?;
            let stat = match crate::file_stamp_and_modified(&file) {
                Ok((stamp, _)) => stamp,
                Err(_) => "missing".into(),
            };
            session.stamp = format!("{}|{stat}", session.stamp);
        }
        result.push(session);
    }
    Ok(result)
}
/// Several nullable columns of one row, by index, in one read.
fn columns<T: rusqlite::types::FromSql, const N: usize>(
    r: &rusqlite::Row<'_>,
    index: [usize; N],
) -> rusqlite::Result<[Option<T>; N]> {
    let mut out = std::array::from_fn(|_| None);
    for (slot, i) in out.iter_mut().zip(index) {
        *slot = r.get(i)?;
    }
    Ok(out)
}
/// A session is hot or cold; hot rows are authoritative while upstream
/// restores an archive.
fn cold_archive(r: &rusqlite::Row<'_>, hot: Option<i64>) -> rusqlite::Result<Option<ColdArchive>> {
    let Some(generation) = r.get::<_, Option<String>>(26)?.filter(|_| hot.is_none()) else {
        return Ok(None);
    };
    let [name, sha256, storage] = columns::<String, 3>(r, [27, 28, 34])?;
    let [event_count, raw_bytes, archive_bytes, last_seq, archived_at] =
        columns::<i64, 5>(r, [29, 30, 31, 32, 33])?;
    Ok(Some(ColdArchive {
        generation,
        name: name.unwrap_or_default(),
        sha256: sha256.unwrap_or_default(),
        storage: storage.unwrap_or_default(),
        event_count: event_count.unwrap_or_default(),
        raw_bytes: raw_bytes.unwrap_or_default(),
        archive_bytes: archive_bytes.unwrap_or_default(),
        last_seq: last_seq.unwrap_or_default(),
        archived_at: archived_at.unwrap_or_default(),
    }))
}
fn session_row(
    r: &rusqlite::Row<'_>,
    namespace: &str,
    agent: &str,
    schema_version: i64,
) -> Result<Session> {
    let [native_id, session_key, scope] = columns::<String, 3>(r, [0, 1, 4])?;
    let native_id = native_id.unwrap_or_default();
    let session_key = session_key.unwrap_or_default();
    ensure!(
        !native_id.trim().is_empty() && !native_id.chars().any(char::is_control),
        "invalid OpenClaw session identity"
    );
    let [previous, reason, status, chat_type, window_channel, model_provider, model, harness, parent, spawned_by, title, created_via, rewrite, account_id] =
        columns::<String, 14>(r, [2, 3, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 22, 25])?;
    let [started, ended, hot, last_seq, latest, first, updated, transcript_updated, node_updated] =
        columns::<i64, 9>(r, [6, 7, 18, 19, 20, 21, 23, 24, 35])?;
    let (latest, first) = (latest.unwrap_or_default(), first.unwrap_or_default());
    let cold = cold_archive(r, hot)?;
    // Every input that changes when upstream appends, rewrites, truncates,
    // archives or restores this session; no event payload is scanned.
    let stamp = serde_json::json!([
        hot,
        last_seq,
        latest,
        rewrite,
        updated,
        transcript_updated,
        // Node-only edits (label, display name) change the projected title.
        node_updated,
        title,
        cold.as_ref()
            .map(|c| (&c.sha256, c.last_seq, c.archived_at)),
    ])
    .to_string();
    Ok(Session {
        id: identity(namespace, &native_id),
        channel: channel(&session_key),
        native_id,
        agent_id: agent.into(),
        session_key,
        previous_session_id: previous,
        reason,
        scope,
        started: timestamp(started.unwrap_or(first).min(first))?,
        ended: ended.map(timestamp).transpose()?,
        status,
        chat_type,
        window_channel,
        account_id,
        model_provider,
        model,
        harness,
        parent_session_key: parent,
        spawned_by,
        title,
        created_via,
        latest: timestamp(latest)?,
        event_count: hot,
        last_seq,
        rewrite,
        cold,
        schema_version: Some(schema_version),
        legacy: false,
        partial: false,
        stamp,
        events: vec![],
    })
}
pub(super) fn timestamp(ms: i64) -> Result<i64> {
    ensure!(ms >= 0, "invalid OpenClaw timestamp {ms}");
    Ok(ms)
}
/// `agent:<agentId>:<rest>`: the first segment of the rest names the surface
/// (`main`, `cron`, `subagent`, `relay`, `telegram`, ...).
pub(super) fn channel(key: &str) -> Option<String> {
    let mut parts = key.split(':');
    (parts.next()? == "agent").then_some(())?;
    parts.next()?;
    parts
        .next()
        .filter(|s| !s.is_empty())
        .map(str::to_ascii_lowercase)
}

/// Session list of one agent database, captured in one snapshot.
fn list(context: &AcquisitionContext<'_>, path: &Path, limit: usize) -> Result<Vec<Session>> {
    read(context, path, |conn, schema| {
        sessions(conn, context.roots, path, schema, limit, None)
    })
}

pub(super) fn enumerate(roots: &ProviderRoots, limit: Option<usize>) -> Result<Vec<Candidate>> {
    let context = AcquisitionContext::new(ID, roots);
    let cap = limit.unwrap_or(MAX_SESSIONS).min(MAX_SESSIONS) + 1;
    let mut candidates = vec![];
    for agent in stores::agent_dirs(roots)? {
        check_capture_cancelled()?;
        if let Some(path) = stores::database_of(&agent)? {
            for s in list(&context, &path, cap)? {
                candidates.push(Candidate {
                    source: ID,
                    locator: path.to_string_lossy().into_owned(),
                    session_id: Some(s.id),
                    recency_hint_ms: Some(s.latest),
                    // Per-session change stamp: other sessions' writes to the
                    // same database do not force a shallow re-read.
                    stamp: s.stamp,
                });
            }
        }
        // The database owns every session it holds, imported or not, however
        // few of them the bounded listing above returned.
        let dir = agent.join(legacy::DIR);
        if dir.is_dir() {
            let held = stores::held(&context, &agent)?;
            candidates.extend(legacy::candidates(&context, &dir, &held)?);
        }
    }
    ensure!(
        candidates.len() <= MAX_SESSIONS,
        "OpenClaw discovery exceeds safety limit"
    );
    candidates.sort_by(|a, b| {
        b.recency_hint_ms
            .cmp(&a.recency_hint_ms)
            .then_with(|| a.session_id.cmp(&b.session_id))
    });
    candidates.truncate(limit.unwrap_or(MAX_SESSIONS));
    Ok(candidates)
}

/// Bounded discovery read: catalog row plus the first prompt from at most
/// the first 64 events.
pub(super) fn shallow(scan: &ScanEnv<'_>, c: &Candidate) -> Result<Option<ShallowSession>> {
    if Path::new(&c.locator)
        .file_name()
        .is_some_and(|n| n != DATABASE)
    {
        return legacy::shallow(scan, c);
    }
    let context = AcquisitionContext::new(ID, scan.provider_roots);
    let path = Path::new(&c.locator);
    let namespace = namespace(scan.provider_roots, path)?;
    let Some(id) = c.session_id.as_deref() else {
        return Ok(None);
    };
    let native = native_id(&namespace, id)?;
    scan.note_open();
    scan.note_query();
    read(&context, path, |conn, schema| {
        let Some(s) = sessions(conn, scan.provider_roots, path, schema, 1, Some(native))?
            .into_iter()
            .next()
        else {
            return Ok(None);
        };
        scan.note_records(1);
        scan.note_query();
        let head = head(&context, conn, path, schema, &s)?;
        scan.note_bytes(head.iter().map(|e| e.to_string().len() as u64).sum());
        scan.note_records(head.len() as u64);
        let cwd = head
            .first()
            .and_then(|h| h["cwd"].as_str())
            .filter(|c| !c.trim().is_empty())
            .map(str::to_owned);
        Ok(Some(ShallowSession {
            source: ID.into(),
            session_id: s.id,
            title: s.title,
            cwd,
            models: s.model.into_iter().collect(),
            first_prompt: format::first_prompt(&head),
            first_activity_ms: Some(s.started),
            last_activity_ms: Some(s.latest),
            raw_path: Some(c.locator.clone()),
            source_stamp: Some(c.stamp.clone()),
            discovery_state: "shallow".into(),
            ..Default::default()
        }))
    })
}

/// The first 64 entries, for a bounded first prompt and cwd: hot rows, or
/// the verified cold archive once upstream has moved them there.
fn head(
    context: &AcquisitionContext<'_>,
    conn: &Connection,
    path: &Path,
    schema: &Schema,
    s: &Session,
) -> Result<Vec<Value>> {
    if let Some(archive) = &s.cold {
        let events = cold::events(context, conn, path, &s.native_id, archive)?;
        return Ok(events.into_iter().take(64).map(|e| e.entry).collect());
    }
    let mut stmt = conn.prepare(&format!(
        "SELECT e.event_json, {} FROM transcript_events e \
         WHERE e.session_id=?1 ORDER BY e.seq LIMIT 64",
        schema.payload_columns()
    ))?;
    let mut rows = stmt.query([&s.native_id])?;
    let mut head = vec![];
    while let Some(r) = rows.next()? {
        let text = match r.get::<_, Option<String>>(0)? {
            Some(json) => json,
            None => events::decompress(r.get(1)?, r.get(2)?)?,
        };
        head.push(serde_json::from_str::<Value>(&text)?);
    }
    Ok(head)
}

/// Read one selected session and all its events from a single snapshot.
pub(super) fn capture(
    context: &AcquisitionContext<'_>,
    path: &Path,
    id: &str,
) -> Result<AcquiredSession> {
    if path.file_name().is_some_and(|n| n != DATABASE) {
        return legacy::capture(context, path, id);
    }
    let namespace = namespace(context.roots, path)?;
    let native = native_id(&namespace, id)?;
    let session = read(context, path, |conn, schema| {
        let mut s = sessions(conn, context.roots, path, schema, 1, Some(native))?
            .into_iter()
            .next()
            .context("SESSION_SOURCE_MISMATCH: OpenClaw session not in this agent database")?;
        s.events = events::events(context, conn, path, schema, &s)?;
        Ok(s)
    })?;
    let records = session.events.len() as i64;
    let mut read_paths = vec![path.to_path_buf()];
    if let Some(archive) = session.cold.as_ref().filter(|c| c.storage == "file") {
        read_paths.push(cold::archive_path(path, &archive.name)?);
    }
    for suffix in SIDECARS {
        let sidecar = sidecar(path, suffix);
        if acknowledge_sidecar(context, &sidecar)? {
            read_paths.push(sidecar);
        }
    }
    // SQLite bytes read are unknown; never report the file size.
    Ok(AcquiredSession::new(
        serde_json::to_value(session)?,
        0,
        records,
        read_paths,
    ))
}

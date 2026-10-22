use super::*;
use crate::discover::WatchEntries;
use crate::ingest::check_capture_cancelled;
use anyhow::{ensure, Context};
use sha2::{Digest, Sha256};
const MAX_SESSIONS: usize = 100_000;
fn hex(text: &[u8]) -> String {
    text.iter().map(|b| format!("{b:02x}")).collect()
}
pub(super) fn namespace(roots: &ProviderRoots, path: &Path) -> Result<String> {
    let dir = path
        .parent()
        .and_then(Path::parent)
        .with_context(|| format!("GOOSE_INVALID_PATH: {} has no data root", path.display()))?;
    if let Some(default) = roots.default_for_provider(ID) {
        if dir == default {
            return Ok("default".into());
        }
    }
    Ok(format!(
        "root:{:x}",
        Sha256::digest(dir.as_os_str().as_encoded_bytes())
    ))
}
fn identity(namespace: &str, native: &str) -> String {
    format!("{namespace}:{}", hex(native.as_bytes()))
}
pub(super) fn databases(roots: &ProviderRoots) -> Result<Vec<PathBuf>> {
    roots.ensure_provider_roots(ID)?;
    let context = AcquisitionContext::new(ID, roots);
    let mut paths = vec![];
    for root in roots.for_provider(ID) {
        check_capture_cancelled()?;
        match std::fs::metadata(root) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e.into()),
            Ok(metadata) => {
                context.validate(root)?;
                ensure!(metadata.is_dir(), "Goose data root is not a directory");
            }
        }
        let main = root.join("sessions/sessions.db");
        if main.try_exists()? {
            context.validate(&main)?;
            paths.push(main);
        }
    }
    paths.sort();
    paths.dedup();
    Ok(paths)
}
pub(super) fn watch_roots(roots: &ProviderRoots) -> Vec<WatchRoot> {
    let mut dirs: Vec<_> = roots
        .for_provider(ID)
        .iter()
        .map(|r| r.join("sessions"))
        .collect();
    if let Ok(paths) = databases(roots) {
        dirs.extend(
            paths
                .iter()
                .filter_map(|p| p.parent().map(Path::to_path_buf)),
        );
    }
    dirs.sort();
    dirs.dedup();
    dirs.into_iter()
        .map(|p| {
            WatchRoot::directory_of(
                p,
                WatchEntries::OpencodeStores {
                    primary: "sessions.db".into(),
                    channels: false,
                },
            )
        })
        .collect()
}
fn read<T>(
    context: &AcquisitionContext<'_>,
    path: &Path,
    f: impl FnOnce(&Connection) -> Result<T>,
) -> Result<T> {
    check_capture_cancelled()?;
    context.validate(path)?;
    ensure!(
        path.file_name().is_some_and(|n| n == "sessions.db"),
        "Goose locator must name sessions.db"
    );
    ensure!(
        context
            .roots
            .for_provider(ID)
            .iter()
            .any(|root| path == root.join("sessions/sessions.db")),
        "Goose locator must be the configured session database"
    );
    for suffix in ["-wal", "-shm", "-journal"] {
        let _ = acknowledge_sidecar(context, &sidecar(path, suffix))?;
    }
    sqlite_snapshot(context, path, |conn| {
        let version: i64 = conn
            .query_row("SELECT max(version) FROM schema_version", [], |r| r.get(0))
            .context("unsupported Goose schema: missing schema_version")?;
        ensure!(
            version == 17,
            "unsupported Goose schema {version}; supported: 17"
        );
        f(conn)
    })
}
fn timestamp(value: i64) -> Result<i64> {
    value.checked_mul(1000).context("invalid Goose timestamp")
}
pub(super) fn session_query(selected: bool) -> String {
    let predicate = if selected { "WHERE session_id=?1" } else { "" };
    format!("SELECT s.id,s.name,s.provider_name,s.model_config_json,s.working_dir,s.parent_session_id,cast(strftime('%s',s.created_at) as integer),max(cast(strftime('%s',s.updated_at) as integer),coalesce(m.latest,0)) AS latest FROM sessions s LEFT JOIN (SELECT session_id,max(created_timestamp) AS latest FROM messages {predicate} GROUP BY session_id) m ON m.session_id=s.id WHERE (?1 IS NULL OR s.id=?1) ORDER BY latest DESC,s.id LIMIT ?2")
}
fn sessions(
    conn: &Connection,
    roots: &ProviderRoots,
    path: &Path,
    limit: usize,
    native: Option<&str>,
) -> Result<Vec<Session>> {
    let namespace = namespace(roots, path)?;
    let query = session_query(native.is_some());
    let mut stmt = conn.prepare(&query)?;
    let rows = stmt.query_map(rusqlite::params![native, limit as i64], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get(1)?,
            r.get::<_, Option<String>>(2)?,
            r.get::<_, Option<String>>(3)?,
            r.get::<_, String>(4)?,
            r.get::<_, Option<String>>(5)?,
            r.get::<_, Option<i64>>(6)?,
            r.get::<_, Option<i64>>(7)?,
        ))
    })?;
    let mut result = vec![];
    for row in rows {
        check_capture_cancelled()?;
        let (native_id, title, _provider, model_json, cwd, parent, started, latest) = row?;
        ensure!(!native_id.trim().is_empty(), "empty Goose session identity");
        result.push(Session {
            id: identity(&namespace, &native_id),
            native_id,
            namespace: namespace.clone(),
            title,
            model: model_json
                .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
                .and_then(|value| {
                    value
                        .get("model_name")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                }),
            cwd: Some(cwd),
            parent,
            started: timestamp(started.context("invalid Goose created_at")?)?,
            latest: timestamp(latest.context("invalid Goose updated_at")?)?,
            messages: vec![],
        });
    }
    Ok(result)
}
fn messages(conn: &Connection, native: &str) -> Result<Vec<Message>> {
    let mut stmt = conn.prepare("SELECT id,message_id,role,content_json,created_timestamp,metadata_json FROM messages WHERE session_id=? ORDER BY created_timestamp,id LIMIT 1000001")?;
    let rows = stmt.query_map([native], |r| {
        Ok((
            r.get::<_, i64>(0)?,
            r.get::<_, Option<String>>(1)?,
            r.get::<_, String>(2)?,
            r.get::<_, String>(3)?,
            r.get::<_, i64>(4)?,
            r.get::<_, Option<String>>(5)?,
        ))
    })?;
    let mut result = Vec::new();
    for row in rows {
        check_capture_cancelled()?;
        let (row_id, native_id, role, content, created, metadata) = row?;
        result.push(Message {
            row_id,
            native_id,
            key: String::new(),
            role,
            content: serde_json::from_str(&content).context("malformed Goose content_json")?,
            timestamp: timestamp(created)?,
            metadata: metadata
                .map(|value| serde_json::from_str(&value))
                .transpose()
                .context("malformed Goose metadata_json")?,
        });
    }
    ensure!(
        result.len() <= 1_000_000,
        "Goose message capture exceeds safety limit"
    );
    let mut occurrences = std::collections::BTreeMap::<String, usize>::new();
    for message in &mut result {
        if let Some(id) = message.native_id.as_ref().filter(|id| !id.is_empty()) {
            let occurrence = occurrences.entry(id.clone()).or_default();
            message.key = format!("native:{}:{occurrence}", hex(id.as_bytes()));
            *occurrence += 1;
        } else {
            message.key = format!("row:{}", message.row_id);
        }
    }
    Ok(result)
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
        "Goose discovery exceeds safety limit"
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
            title: Some(s.title),
            cwd: s.cwd,
            git_branch: None,
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
pub(super) fn all(context: &AcquisitionContext<'_>, path: &Path) -> Result<Vec<Session>> {
    read(context, path, |conn| {
        let mut rows = sessions(conn, context.roots, path, MAX_SESSIONS + 1, None)?;
        ensure!(
            rows.len() <= MAX_SESSIONS,
            "Goose session capture exceeds safety limit"
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
                anyhow::anyhow!("SESSION_SOURCE_MISMATCH: Goose session not in this database")
            })?;
        s.messages = messages(conn, &s.native_id)?;
        Ok(s)
    })?;
    let records = session.messages.len() as i64;
    let mut read_paths = vec![path.to_path_buf()];
    for suffix in ["-wal", "-shm", "-journal"] {
        let sidecar = sidecar(path, suffix);
        if acknowledge_sidecar(context, &sidecar)? {
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
        "invalid Goose identity encoding"
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

/// A WAL, shm, or rollback sidecar that is present and inside the provider
/// root. A file that disappears between the exists check and `canonicalize`
/// is absent: another connection can unlink it while this one reads.
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
    for cause in error.chain() {
        let Some(io) = cause.downcast_ref::<std::io::Error>() else {
            continue;
        };
        if io.kind() == std::io::ErrorKind::NotFound {
            return true;
        }
    }
    false
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
                    stamp: format!("{}/{stamp}", namespace(roots, &db)?),
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
    let mut stmt = conn.prepare("SELECT content_json,metadata_json FROM messages WHERE session_id=? AND role='user' ORDER BY created_timestamp,id LIMIT 32")?;
    for row in stmt.query_map([native], |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?))
    })? {
        let (raw, metadata) = row?;
        let metadata: Value = metadata
            .map(|s| serde_json::from_str(&s))
            .transpose()?
            .unwrap_or(Value::Null);
        if metadata["turnContext"] == true || metadata["userVisible"] == false {
            continue;
        }
        let content: Value = serde_json::from_str(&raw).context("malformed Goose content_json")?;
        let mut texts = vec![];
        for block in content
            .as_array()
            .context("Goose content_json must be an array")?
        {
            if block["type"] == "text" {
                if let Some(text) = block["text"].as_str() {
                    texts.push(text);
                }
            }
        }
        if !texts.is_empty() {
            return Ok(Some(texts.join("\n").chars().take(512).collect()));
        }
    }
    Ok(None)
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
    relative == Path::new("sessions/sessions.db")
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

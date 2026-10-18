//! The JSONL session store OpenClaw 2026.7.x and earlier wrote:
//! `agents/<id>/sessions/sessions.json` (session key -> entry) beside one Pi
//! v3 JSONL transcript per session (`src/config/sessions/paths.ts`,
//! `artifacts.ts`). From 2026.8.1 the doctor imports each transcript into the
//! agent database under the same session ID and moves the files to
//! `agents/<id>/session-sqlite-import-archive/`, so a session the database
//! holds is never read from JSONL.
use super::format::{entry_time, string};
use super::*;
use crate::discover::{Candidate, ScanEnv, ShallowSession};
use crate::ingest::check_capture_cancelled;
use anyhow::{ensure, Context};
use rusqlite::OptionalExtension;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;

pub(super) const DIR: &str = "sessions";
pub(super) const INDEX: &str = "sessions.json";
const MAX_FILES: usize = 100_000;
/// Bounded discovery read, as for Pi transcripts.
const HEAD_LIMIT: u64 = 64 * 1024;
/// Upstream bounds the store it loads; anything larger is not a session index.
const MAX_INDEX_BYTES: u64 = 256 * 1024 * 1024;
/// A transcript is read whole; past this it is unread rather than loaded.
#[cfg(not(test))]
pub(super) const MAX_TRANSCRIPT_BYTES: u64 = 512 * 1024 * 1024;
/// Small enough for a test to exceed.
#[cfg(test)]
pub(super) const MAX_TRANSCRIPT_BYTES: u64 = 64 * 1024;
/// The database layout's own per-session event bound.
const MAX_EVENTS: usize = 1_000_000;

/// `isPrimarySessionTranscriptFileName`: live transcripts only. Reset and
/// delete archives (`*.jsonl.reset.<ts>`, `*.jsonl.deleted.<ts>`), index
/// backups and temp files, trajectories and compaction checkpoints are not
/// sessions; a session whose primary file is gone retires.
pub(super) fn is_transcript_name(name: &str) -> bool {
    let Some(stem) = name.strip_suffix(".jsonl") else {
        return false;
    };
    !stem.is_empty() && !stem.ends_with(".trajectory") && !is_checkpoint(stem)
}
/// `<sessionId>.checkpoint.<uuid v1-5>`.
fn is_checkpoint(stem: &str) -> bool {
    let Some((session, uuid)) = stem.rsplit_once(".checkpoint.") else {
        return false;
    };
    let groups: Vec<&str> = uuid.split('-').collect();
    let shape = groups.iter().map(|g| g.len()).eq([8, 4, 4, 4, 12])
        && uuid.chars().all(|c| c == '-' || c.is_ascii_hexdigit());
    !session.is_empty()
        && shape
        && matches!(groups[2].as_bytes()[0], b'1'..=b'5')
        && matches!(
            groups[3].as_bytes()[0].to_ascii_lowercase(),
            b'8' | b'9' | b'a' | b'b'
        )
}
/// A transcript path of the native layout: `agents/<id>/sessions/<name>`.
pub(super) fn native_path(relative: &Path) -> bool {
    let parts: Vec<_> = relative.components().collect();
    parts.len() == 4
        && parts[0].as_os_str() == "agents"
        && matches!(parts[1], std::path::Component::Normal(_))
        && parts[2].as_os_str() == DIR
        && parts[3]
            .as_os_str()
            .to_str()
            .is_some_and(is_transcript_name)
}

/// Every agent's legacy sessions directory, beside its `agent/` directory.
pub(super) fn dirs(roots: &ProviderRoots) -> Result<Vec<PathBuf>> {
    let mut dirs = vec![];
    for agent in stores::agent_dirs(roots)? {
        let dir = agent.join(DIR);
        if dir.is_dir() {
            storage::agent_id(&dir.join(INDEX))?;
            dirs.push(dir);
        }
    }
    Ok(dirs)
}
/// Live transcript files in one sessions directory, in path order.
pub(super) fn transcripts(context: &AcquisitionContext<'_>, dir: &Path) -> Result<Vec<PathBuf>> {
    context.validate(dir)?;
    let mut files = vec![];
    for entry in std::fs::read_dir(dir)? {
        check_capture_cancelled()?;
        let entry = entry?;
        let named = entry.file_name().to_str().is_some_and(is_transcript_name);
        if named && entry.file_type()?.is_file() {
            ensure!(
                files.len() < MAX_FILES,
                "OpenClaw session directory exceeds {MAX_FILES} transcripts"
            );
            files.push(entry.path());
        }
    }
    files.sort();
    Ok(files)
}
/// The index file beside a transcript.
pub(super) fn index_path(transcript: &Path) -> Option<PathBuf> {
    transcript.parent().map(|dir| dir.join(INDEX))
}

/// One `sessions.json` entry: its session key and stored fields.
#[derive(Debug, Clone)]
pub(super) struct Entry {
    key: String,
    fields: Value,
}
/// `sessions.json` by transcript file name. An entry names its transcript by
/// `sessionFile` (upstream keeps it inside this directory) else
/// `<sessionId>.jsonl`; only the file name is used, so a store copied from
/// another home still resolves.
pub(super) fn index(
    context: &AcquisitionContext<'_>,
    dir: &Path,
) -> Result<BTreeMap<String, Entry>> {
    let path = dir.join(INDEX);
    if !path.try_exists()? {
        return Ok(BTreeMap::new());
    }
    #[cfg(test)]
    PARSES.with(|p| p.set(p.get() + 1));
    context.validate(&path)?;
    ensure!(
        std::fs::metadata(&path)?.len() <= MAX_INDEX_BYTES,
        "OpenClaw session index exceeds {MAX_INDEX_BYTES} bytes"
    );
    let store: Value = serde_json::from_slice(&context.read(&path)?)
        .context("OPENCLAW_INVALID_RECORD: sessions.json is not JSON")?;
    let store = store
        .as_object()
        .context("OPENCLAW_INVALID_RECORD: sessions.json is not an object")?;
    let mut by_file: BTreeMap<String, Entry> = BTreeMap::new();
    for (key, fields) in store {
        let Ok(id) = string(fields, "sessionId") else {
            continue;
        };
        let file = fields["sessionFile"]
            .as_str()
            .and_then(|f| Path::new(f.trim()).file_name())
            .and_then(|n| n.to_str())
            .map_or_else(|| format!("{id}.jsonl"), str::to_owned);
        let newer = by_file
            .get(&file)
            .is_none_or(|e| e.fields["updatedAt"].as_i64() < fields["updatedAt"].as_i64());
        if newer {
            let entry = Entry {
                key: key.clone(),
                fields: fields.clone(),
            };
            by_file.insert(file, entry);
        }
    }
    Ok(by_file)
}
fn file_name(path: &Path) -> &str {
    path.file_name()
        .and_then(|n| n.to_str())
        .unwrap_or_default()
}
/// File stat plus the index entry's content: a rename, label or model change
/// in `sessions.json` restamps the session without touching its transcript.
pub(super) fn stamp(path: &Path, entry: Option<&Entry>) -> Result<(String, Option<i64>)> {
    let (stat, modified) = crate::file_stamp_and_modified(path)?;
    let fields = entry.map(|e| serde_json::json!([e.key, e.fields]).to_string());
    let digest = Sha256::digest(fields.unwrap_or_default().as_bytes());
    let hex: String = digest[..8].iter().map(|b| format!("{b:02x}")).collect();
    Ok((format!("jsonl/{stat}/{hex}"), modified))
}

/// Complete JSONL records, at most `limit`, and whether an unterminated
/// final fragment was deferred.
fn parse(bytes: &[u8], limit: usize) -> Result<(Vec<Value>, bool)> {
    let end = bytes.iter().rposition(|b| *b == b'\n').map_or(0, |i| i + 1);
    let partial = bytes[end..].iter().any(|b| !b.is_ascii_whitespace());
    let mut entries = vec![];
    let mut ids = BTreeSet::new();
    for line in bytes[..end].split(|b| *b == b'\n') {
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        check_capture_cancelled()?;
        let entry: Value = serde_json::from_slice(line)
            .context("OPENCLAW_INVALID_RECORD: malformed complete JSONL record")?;
        format::validate(&entry, entries.is_empty())?;
        if !entries.is_empty() {
            let id = string(&entry, "id")?.to_owned();
            ensure!(
                ids.insert(id),
                "OPENCLAW_INVALID_RECORD: duplicate entry id"
            );
        }
        entries.push(entry);
        if entries.len() >= limit {
            break;
        }
    }
    ensure!(
        !entries.is_empty(),
        "OPENCLAW_INVALID_RECORD: no session header"
    );
    Ok((entries, partial))
}
fn head_bytes(context: &AcquisitionContext<'_>, path: &Path) -> Result<Vec<u8>> {
    context.validate(path)?;
    let mut bytes = vec![];
    std::fs::File::open(path)?
        .take(HEAD_LIMIT)
        .read_to_end(&mut bytes)?;
    Ok(bytes)
}
/// The header's session ID from the bounded head, or None while the header
/// line is unterminated or unreadable (the shallow read then reports it).
fn header_id(context: &AcquisitionContext<'_>, path: &Path) -> Option<String> {
    let bytes = head_bytes(context, path).ok()?;
    let line = bytes.split(|b| *b == b'\n').next()?;
    (line.len() < bytes.len()).then_some(())?;
    let header: Value = serde_json::from_slice(line).ok()?;
    (header["type"] == "session").then_some(())?;
    string(&header, "id").ok().map(str::to_owned)
}

/// A discovery candidate for one live transcript. The database's sessions
/// (`superseded`) and an earlier file of the same session are skipped.
pub(super) fn candidates(
    context: &AcquisitionContext<'_>,
    dir: &Path,
    superseded: &BTreeSet<String>,
) -> Result<Vec<Candidate>> {
    let index = index(context, dir)?;
    let namespace = storage::namespace(context.roots, &dir.join(INDEX))?;
    let mut seen = BTreeSet::new();
    let mut out = vec![];
    for path in transcripts(context, dir)? {
        let native = header_id(context, &path);
        if native
            .as_ref()
            .is_some_and(|n| superseded.contains(n) || !seen.insert(n.clone()))
        {
            continue;
        }
        let (stamp, modified) = stamp(&path, index.get(file_name(&path)))?;
        out.push(Candidate {
            source: ID,
            locator: path.to_string_lossy().into_owned(),
            session_id: native.map(|n| format!("{namespace}:{n}")),
            recency_hint_ms: modified,
            stamp,
        });
    }
    Ok(out)
}

fn text(fields: &Value, key: &str) -> Option<String> {
    fields[key]
        .as_str()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
}
/// The session one transcript holds, catalogued from its index entry the
/// way the doctor's import fills the database's window and node rows.
pub(super) fn session(
    context: &AcquisitionContext<'_>,
    path: &Path,
    entry: Option<&Entry>,
    stamp: String,
) -> Result<(Session, bool)> {
    // One handle: its size rejects an oversized file before any read, and
    // the bounded read rejects one still growing past the limit.
    context.validate(path)?;
    let file = std::fs::File::open(path)?;
    let too_large = || {
        anyhow::anyhow!(
            "OpenClaw transcript {} exceeds {MAX_TRANSCRIPT_BYTES} bytes",
            path.display()
        )
    };
    if file.metadata()?.len() > MAX_TRANSCRIPT_BYTES {
        return Err(too_large());
    }
    let mut bytes = vec![];
    file.take(MAX_TRANSCRIPT_BYTES + 1)
        .read_to_end(&mut bytes)?;
    #[cfg(test)]
    TRANSCRIPT_READS.with(|r| r.set(r.get() + 1));
    if bytes.len() as u64 > MAX_TRANSCRIPT_BYTES {
        return Err(too_large());
    }
    let (entries, partial) = parse(&bytes, MAX_EVENTS + 1)?;
    ensure!(
        entries.len() <= MAX_EVENTS,
        "OpenClaw transcript exceeds {MAX_EVENTS} records"
    );
    let header = &entries[0];
    let native = string(header, "id")?.to_owned();
    let namespace = storage::namespace(context.roots, path)?;
    let active = tree::active(&entries);
    let mut events = Vec::with_capacity(entries.len());
    for (seq, entry) in entries.into_iter().enumerate() {
        events.push(Event {
            seq: seq as i64,
            created_at: entry_time(&entry)?,
            active: active.contains(&seq),
            entry,
        });
    }
    let times = events.iter().map(|e| e.created_at);
    let (started, latest) = (times.clone().min().unwrap_or(0), times.max().unwrap_or(0));
    let null = Value::Null;
    let (key, fields) = entry.map_or(("", &null), |e| (e.key.as_str(), &e.fields));
    let delivery = &fields["deliveryContext"];
    let session = Session {
        id: format!("{namespace}:{native}"),
        native_id: native,
        agent_id: storage::agent_id(path)?.into(),
        session_key: key.into(),
        channel: storage::channel(key),
        // Pre-2026.2 stores named the channel `provider`.
        window_channel: text(fields, "channel").or_else(|| text(fields, "provider")),
        account_id: text(delivery, "accountId").or_else(|| text(fields, "lastAccountId")),
        chat_type: text(fields, "chatType"),
        scope: None,
        reason: None,
        previous_session_id: None,
        parent_session_key: text(fields, "parentSessionKey"),
        spawned_by: text(fields, "spawnedBy"),
        created_via: None,
        harness: text(fields, "agentHarnessId"),
        model_provider: text(fields, "modelProvider"),
        model: text(fields, "model"),
        status: text(fields, "status"),
        title: text(fields, "label").or_else(|| text(fields, "displayName")),
        started,
        latest,
        ended: fields["endedAt"].as_i64().filter(|t| *t >= 0),
        event_count: Some(events.len() as i64),
        last_seq: Some(events.len() as i64 - 1),
        rewrite: None,
        cold: None,
        schema_version: None,
        legacy: true,
        partial,
        stamp,
        events,
    };
    Ok((session, partial))
}
type Index = std::rc::Rc<BTreeMap<String, Entry>>;
thread_local! {
    /// The last index a shallow or selected read parsed, by its file stamp:
    /// discovery reads every transcript of a directory in turn, and each
    /// needs only its own entry.
    static INDEX_CACHE: std::cell::RefCell<Option<(PathBuf, String, Index)>> =
        const { std::cell::RefCell::new(None) };
}
#[cfg(test)]
thread_local! {
    static PARSES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}
#[cfg(test)]
thread_local! {
    static TRANSCRIPT_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}
/// Whole-transcript reads on this thread.
#[cfg(test)]
pub(super) fn transcript_reads() -> usize {
    TRANSCRIPT_READS.with(std::cell::Cell::get)
}
#[cfg(test)]
pub(super) fn index_parses() -> usize {
    PARSES.with(std::cell::Cell::get)
}
#[cfg(test)]
pub(super) fn reset_index_parses() {
    PARSES.with(|p| p.set(0));
    INDEX_CACHE.with(|c| c.borrow_mut().take());
}
/// The index file's identity: size and mtime, plus on Unix its inode and
/// change time, which no rewrite can preserve (an mtime can be restored).
fn index_identity(path: &Path) -> Result<String> {
    let metadata = match std::fs::metadata(path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok("absent".into()),
        result => result?,
    };
    let (stamp, _) = crate::file_stamp_and_modified(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let (ino, ctime, ns) = (metadata.ino(), metadata.ctime(), metadata.ctime_nsec());
        Ok(format!("{stamp}/{ino}/{ctime}.{ns}"))
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        Ok(stamp)
    }
}
/// Discovery's index: one parse per directory while the file is unchanged.
fn cached_index(context: &AcquisitionContext<'_>, dir: &Path) -> Result<Index> {
    let path = dir.join(INDEX);
    let stamp = index_identity(&path)?;
    let hit = INDEX_CACHE.with(|c| {
        c.borrow()
            .as_ref()
            .filter(|(p, s, _)| *p == path && *s == stamp)
            .map(|(_, _, index)| index.clone())
    });
    if let Some(index) = hit {
        return Ok(index);
    }
    let parsed = std::rc::Rc::new(index(context, dir)?);
    INDEX_CACHE.with(|c| *c.borrow_mut() = Some((path, stamp, parsed.clone())));
    Ok(parsed)
}
fn entry_for(context: &AcquisitionContext<'_>, path: &Path) -> Result<Option<Entry>> {
    let dir = path
        .parent()
        .context("OpenClaw transcript has no directory")?;
    Ok(cached_index(context, dir)?.get(file_name(path)).cloned())
}
/// An explicit selected read always parses the index it captures.
fn fresh_entry(context: &AcquisitionContext<'_>, path: &Path) -> Result<Option<Entry>> {
    let dir = path
        .parent()
        .context("OpenClaw transcript has no directory")?;
    Ok(index(context, dir)?.remove(file_name(path)))
}

/// Bounded discovery read: the index entry plus the first prompt from the
/// first 64 complete records.
pub(super) fn shallow(scan: &ScanEnv<'_>, c: &Candidate) -> Result<Option<ShallowSession>> {
    let context = AcquisitionContext::new(ID, scan.provider_roots);
    let path = Path::new(&c.locator);
    scan.note_open();
    let bytes = head_bytes(&context, path)?;
    scan.note_bytes(bytes.len() as u64);
    let (head, _) = parse(&bytes, 64)?;
    scan.note_records(head.len() as u64);
    let entry = entry_for(&context, path)?;
    let fields = entry.as_ref().map(|e| &e.fields).unwrap_or(&Value::Null);
    let namespace = storage::namespace(scan.provider_roots, path)?;
    let times: Vec<i64> = head.iter().map(entry_time).collect::<Result<_>>()?;
    Ok(Some(ShallowSession {
        source: ID.into(),
        session_id: format!("{namespace}:{}", string(&head[0], "id")?),
        title: text(fields, "label").or_else(|| text(fields, "displayName")),
        cwd: head[0]["cwd"]
            .as_str()
            .filter(|c| !c.trim().is_empty())
            .map(str::to_owned),
        models: text(fields, "model").into_iter().collect(),
        first_prompt: format::first_prompt(&head),
        first_activity_ms: times.iter().min().copied(),
        last_activity_ms: times.iter().max().copied(),
        raw_path: Some(c.locator.clone()),
        source_stamp: Some(c.stamp.clone()),
        discovery_state: "shallow".into(),
        ..Default::default()
    }))
}

/// Read one selected legacy session: its transcript and index entry.
pub(super) fn capture(
    context: &AcquisitionContext<'_>,
    path: &Path,
    id: &str,
) -> Result<AcquiredSession> {
    let entry = fresh_entry(context, path)?;
    let (stamp, _) = stamp(path, entry.as_ref())?;
    let (session, _) = session(context, path, entry.as_ref(), stamp)?;
    ensure!(
        session.id == id,
        "SESSION_SOURCE_MISMATCH: OpenClaw transcript holds another session"
    );
    let records = session.events.len() as i64;
    let bytes = std::fs::metadata(path)?.len() as i64;
    let mut read_paths = vec![path.to_path_buf()];
    read_paths.extend(index_path(path).filter(|p| p.is_file()));
    Ok(AcquiredSession::new(
        serde_json::to_value(session)?,
        bytes,
        records,
        read_paths,
    ))
}

/// The header marker of a JSONL session names its layout; a database
/// session's marker is unchanged.
pub(super) fn mark(payload: &mut Value, s: &Session) {
    if s.legacy {
        payload["layout"] = "jsonl".into();
    }
}

/// A complete transcript replaces the session's evidence; a partial one
/// (an unterminated final JSONL record) only adds to it, like an append.
pub(super) fn complete_or_append(conn: &Connection, s: &Session, path: &Path) -> Result<usize> {
    if s.partial {
        normalize::append(conn, s, path)
    } else {
        normalize::session(conn, s, path)
    }
}

/// Move a session's catalog row from its JSONL transcript to the agent
/// database that holds it, keeping every row of evidence.
pub(super) fn transfer(conn: &Connection, id: &str, database: &Path) -> Result<()> {
    conn.execute(
        "UPDATE sessions SET raw_path=?3 WHERE source=?1 AND session_id=?2",
        rusqlite::params![ID, id, database.to_string_lossy()],
    )?;
    Ok(())
}

/// A database session the catalog still records at its imported JSONL
/// transcript: the database now owns it, so the row follows the database
/// and a later move of the transcript into the import archive retires
/// nothing.
pub(super) fn claim(conn: &Connection, s: &Session, database: &Path) -> Result<()> {
    if s.legacy {
        return Ok(());
    }
    let Some(dir) = database
        .parent()
        .and_then(Path::parent)
        .map(|a| a.join(DIR))
    else {
        return Ok(());
    };
    let stored: Option<String> = conn
        .query_row(
            "SELECT raw_path FROM sessions WHERE source=?1 AND session_id=?2",
            [ID, &s.id],
            |r| r.get(0),
        )
        .optional()?
        .flatten();
    if stored.is_some_and(|p| Path::new(&p).parent() == Some(dir.as_path())) {
        transfer(conn, &s.id, database)?;
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn only_live_transcripts_are_sessions() {
        for live in ["abc.jsonl", "abc-topic-42.jsonl", "a.b.jsonl"] {
            assert!(is_transcript_name(live), "{live}");
        }
        for not in [
            "sessions.json",
            "sessions.json.0f6c1d2e-1111-4222-8333-444455556666.tmp",
            "sessions.json.bak.1790000000000",
            "abc.jsonl.deleted.2026-09-21T14-13-20.000Z",
            "abc.jsonl.reset.2026-09-21T14-13-20Z",
            "abc.jsonl.bak.2026-09-21T14-13-20Z",
            "abc.trajectory.jsonl",
            "abc.trajectory-path.json",
            "abc.checkpoint.0f6c1d2e-1111-4222-8333-444455556666.jsonl",
            ".jsonl",
        ] {
            assert!(!is_transcript_name(not), "{not}");
        }
        // Not a UUID v1-5 with an RFC variant: an ordinary session ID.
        assert!(is_transcript_name(
            "abc.checkpoint.0f6c1d2e-1111-6222-8333-444455556666.jsonl"
        ));
        assert!(native_path(Path::new("agents/main/sessions/abc.jsonl")));
        assert!(!native_path(Path::new(
            "agents/main/sessions/cold/abc.jsonl"
        )));
        assert!(!native_path(Path::new(
            "agents/main/session-sqlite-import-archive/abc.jsonl"
        )));
    }
    #[test]
    fn unterminated_tail_is_deferred_and_blank_lines_skipped() {
        let header = r#"{"type":"session","version":3,"id":"s","timestamp":"2026-09-21T14:13:20.000Z","cwd":"/f"}"#;
        let message = r#"{"type":"message","id":"m","parentId":null,"timestamp":"2026-09-21T14:13:21.000Z","message":{"role":"user","content":"hi"}}"#;
        let (entries, partial) = parse(
            format!("{header}\n\n{message}\n{{\"type\":").as_bytes(),
            usize::MAX,
        )
        .unwrap();
        assert_eq!((entries.len(), partial), (2, true));
        let (_, partial) = parse(format!("{header}\n{message}\n").as_bytes(), usize::MAX).unwrap();
        assert!(!partial);
        assert!(parse(
            format!("{header}\n{message}\n{message}\n").as_bytes(),
            usize::MAX
        )
        .unwrap_err()
        .to_string()
        .contains("duplicate entry id"));
        assert!(parse(b"{\"type\":\"session\"", usize::MAX).is_err());
        let v2 =
            r#"{"type":"session","version":2,"id":"s","timestamp":"2026-09-21T14:13:20.000Z"}"#;
        assert!(
            format!("{:#}", parse(format!("{v2}\n").as_bytes(), 1).unwrap_err())
                .contains("OPENCLAW_UNSUPPORTED_SCHEMA")
        );
    }
}

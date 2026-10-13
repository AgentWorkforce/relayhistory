//! One open of a transcript, shared by the passes of a cold sweep.
//!
//! A cold ingest used to open the same file for a readability probe, each
//! record walk, a commit-time rehash, and discovery's head read. The probe
//! parks its handle. The first pass that needs a file small enough to hold
//! reads it once and keeps those bytes. Later passes in the same sweep, and
//! discovery, use that copy while the file's stat still matches. A resume
//! from a cursor, or a file too large to hold, keeps the streaming reader.

use std::collections::HashMap;
use std::fs;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use crate::discover::{HEAD_SCAN_MAX_BYTES, TAIL_SCAN_MAX_BYTES};

use super::metadata_mtime_ns;
use super::transcript_cursor::{change_time_ns, file_identity, filesystem_keeps_change_time};

/// Largest transcript retained so a second pass can reuse the first read.
///
/// Above this the streaming reader stays: an incremental resume must not pull
/// a whole rollout into memory to share a tail, and a single record over the
/// walk's ceiling must still be drained without being held.
pub(crate) const CAPTURE_MAX_BYTES: u64 = 8 * 1024 * 1024;

/// Head and tail snapshots kept until discovery finishes. A cold archive must
/// not pin every transcript for the rest of the sweep; past this, discovery
/// reads the file itself.
const RETAIN_BUDGET_BYTES: usize = 256 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
struct Stat {
    len: u64,
    mtime_ns: u64,
    ctime_ns: Option<i64>,
    device: Option<u64>,
    inode: Option<u64>,
}

struct Capture {
    path: PathBuf,
    bytes: Arc<[u8]>,
    stat: Stat,
}

struct Bounded {
    stat: Stat,
    head: Vec<u8>,
    tail: Vec<u8>,
}

struct Parked {
    path: PathBuf,
    file: fs::File,
    stat: Stat,
}

struct State {
    #[cfg(test)]
    opens: HashMap<PathBuf, u64>,
    parked: Option<Parked>,
    /// Change time of the first open of a path. A walk publishes only when
    /// that time still matches, so a rewrite cannot be stored under the new one.
    opened: HashMap<PathBuf, Stat>,
    /// The file most recently read whole.
    full: Option<Capture>,
    /// Earlier whole files from this sweep, so a later pass of the same file
    /// does not open it again. Past [`RETAIN_BUDGET_BYTES`] only the head and
    /// tail stay, and discovery reads anything that fell out.
    kept: HashMap<PathBuf, Capture>,
    bounded: HashMap<PathBuf, Bounded>,
    retained: usize,
    sweep: bool,
}

impl State {
    fn empty() -> Self {
        Self {
            #[cfg(test)]
            opens: HashMap::new(),
            parked: None,
            opened: HashMap::new(),
            full: None,
            kept: HashMap::new(),
            bounded: HashMap::new(),
            retained: 0,
            sweep: false,
        }
    }
}

fn lock() -> std::sync::MutexGuard<'static, State> {
    state()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn state() -> &'static Mutex<State> {
    static STATE: OnceLock<Mutex<State>> = OnceLock::new();
    STATE.get_or_init(|| Mutex::new(State::empty()))
}

/// Held across one sweep so discovery can see bytes the ingest already read.
/// Dropping it forgets those bytes. Open counts stay, for the test that reads
/// them after the sweep returns.
pub(crate) struct SweepGuard {
    /// Tests sweep many stores in one process, and this state is the
    /// process's: one sweep at a time keeps each sweep's open counts its own.
    #[cfg(test)]
    _serial: std::sync::MutexGuard<'static, ()>,
}

impl Drop for SweepGuard {
    fn drop(&mut self) {
        let mut state = lock();
        state.sweep = false;
        state.full = None;
        state.kept.clear();
        state.bounded.clear();
        state.retained = 0;
        state.parked = None;
        state.opened.clear();
    }
}

pub(crate) fn begin_sweep() -> SweepGuard {
    #[cfg(test)]
    let serial = {
        static SWEEPS: Mutex<()> = Mutex::new(());
        SWEEPS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    };
    let mut state = lock();
    state.sweep = true;
    state.full = None;
    state.kept.clear();
    state.bounded.clear();
    state.retained = 0;
    state.parked = None;
    state.opened.clear();
    SweepGuard {
        #[cfg(test)]
        _serial: serial,
    }
}

pub(crate) fn in_sweep() -> bool {
    lock().sweep
}

/// How many times `path` was opened through this module since the process started.
#[cfg(test)]
pub(crate) fn opens(path: &Path) -> u64 {
    lock().opens.get(path).copied().unwrap_or(0)
}

fn note_open(path: &Path) {
    #[cfg(test)]
    {
        let mut state = lock();
        *state.opens.entry(path.to_path_buf()).or_insert(0) += 1;
    }
    #[cfg(not(test))]
    let _ = path;
}

/// Keep `raw` while the file opened under the capture cap and this line still
/// fits. A log that grows past the cap during the walk is parsed and not held.
pub(crate) fn capture_walked(walked: &mut Vec<u8>, raw: &[u8], opened_len: u64) {
    if opened_len > CAPTURE_MAX_BYTES {
        return;
    }
    if walked.len().saturating_add(raw.len()) > CAPTURE_MAX_BYTES as usize {
        return;
    }
    walked.extend_from_slice(raw);
}

fn stat_of(path: &Path) -> io::Result<Stat> {
    let metadata = fs::metadata(path)?;
    Ok(Stat {
        len: metadata.len(),
        mtime_ns: metadata_mtime_ns(&metadata),
        ctime_ns: change_time_ns(&metadata),
        device: file_identity(&metadata).0,
        inode: file_identity(&metadata).1,
    })
}

/// `Some(len)` when `path` is a regular file small enough to retain.
pub(crate) fn small_file_len(path: &Path) -> io::Result<Option<u64>> {
    let metadata = fs::metadata(path)?;
    if !metadata.is_file() {
        return Ok(None);
    }
    let len = metadata.len();
    Ok((len <= CAPTURE_MAX_BYTES).then_some(len))
}

/// Bytes already read for `path`, when the file's stat is still the one they
/// were read under.
pub(crate) fn fresh(path: &Path) -> Option<Arc<[u8]>> {
    let stat = retained_stat(path)?;
    let state = lock();
    if let Some(bytes) = matching_bytes(state.full.as_ref(), path, &stat) {
        return Some(bytes);
    }
    matching_bytes(state.kept.get(path), path, &stat)
}

fn matching_bytes(capture: Option<&Capture>, path: &Path, stat: &Stat) -> Option<Arc<[u8]>> {
    let capture = capture?;
    (capture.path == path && capture.stat == *stat).then(|| Arc::clone(&capture.bytes))
}

/// A stat that can prove retained bytes. Without a change time, restoring
/// mtime hides a rewrite, so the bytes are not reused.
fn retained_stat(path: &Path) -> Option<Stat> {
    let stat = stat_of(path).ok()?;
    stat.ctime_ns.is_some().then_some(stat)
}

/// The head and tail discovery would have read, from bytes already in hand.
pub(crate) fn live_regions(path: &Path) -> Option<(Vec<u8>, Vec<u8>)> {
    let stat = retained_stat(path)?;
    let state = lock();
    if let Some(bytes) = matching_bytes(state.full.as_ref(), path, &stat) {
        return Some(split_live(&bytes));
    }
    if let Some(bytes) = matching_bytes(state.kept.get(path), path, &stat) {
        return Some(split_live(&bytes));
    }
    let bounded = state.bounded.get(path)?;
    (bounded.stat == stat).then(|| (bounded.head.clone(), bounded.tail.clone()))
}

fn publish_locked(state: &mut State, path: PathBuf, bytes: Arc<[u8]>, stat: Stat) {
    forget_retained(state, &path);
    if let Some(previous) = state.full.take() {
        if previous.path != path {
            retain_full(state, previous);
        }
    }
    state.full = Some(Capture { path, bytes, stat });
}

fn retain_full(state: &mut State, capture: Capture) {
    if !state.sweep {
        return;
    }
    let cost = capture.bytes.len();
    if state.retained.saturating_add(cost) > RETAIN_BUDGET_BYTES {
        retain_bounded(state, capture);
        return;
    }
    state.retained += cost;
    state.kept.insert(capture.path.clone(), capture);
}

fn forget_retained(state: &mut State, path: &Path) {
    if let Some(old) = state.kept.remove(path) {
        state.retained = state.retained.saturating_sub(old.bytes.len());
    }
    if let Some(old) = state.bounded.remove(path) {
        state.retained = state
            .retained
            .saturating_sub(old.head.len() + old.tail.len());
    }
}

fn retain_bounded(state: &mut State, capture: Capture) {
    if !state.sweep {
        return;
    }
    let (head, tail) = split_live(&capture.bytes);
    let cost = head.len() + tail.len();
    if state.retained.saturating_add(cost) > RETAIN_BUDGET_BYTES {
        return;
    }
    state.retained += cost;
    state.bounded.insert(
        capture.path,
        Bounded {
            stat: capture.stat,
            head,
            tail,
        },
    );
}

pub(crate) fn publish(path: &Path, bytes: Arc<[u8]>, stat_at_read: &fs::Metadata) {
    let stat = Stat {
        len: stat_at_read.len(),
        mtime_ns: metadata_mtime_ns(stat_at_read),
        ctime_ns: change_time_ns(stat_at_read),
        device: file_identity(stat_at_read).0,
        inode: file_identity(stat_at_read).1,
    };
    if stat.len != bytes.len() as u64 {
        return;
    }
    let mut state = lock();
    publish_locked(&mut state, path.to_path_buf(), bytes, stat);
}

/// Read `path` whole when it fits in [`CAPTURE_MAX_BYTES`]. A parked handle
/// from the readability probe is the open; the bytes are kept for later passes.
pub(crate) fn read_all(path: &Path) -> io::Result<Arc<[u8]>> {
    if let Some(bytes) = fresh(path) {
        return Ok(bytes);
    }
    let before = fs::metadata(path)?;
    if !before.is_file() || before.len() > CAPTURE_MAX_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "transcript is not a retainable file",
        ));
    }
    let mut shared = open_shared(path)?;
    let mut buf = Vec::with_capacity(before.len() as usize);
    shared.file().read_to_end(&mut buf)?;
    drop(shared.into_file());
    let after = fs::metadata(path)?;
    let bytes: Arc<[u8]> = buf.into();
    if metadata_mtime_ns(&before) == metadata_mtime_ns(&after)
        && before.len() == after.len()
        && change_time_ns(&before) == change_time_ns(&after)
        && before.len() == bytes.len() as u64
    {
        publish(path, Arc::clone(&bytes), &after);
    }
    Ok(bytes)
}

/// `read_to_string`, served from bytes already read when the stat still matches.
pub(crate) fn read_string(path: &Path) -> io::Result<String> {
    let bytes = if let Some(bytes) = fresh(path) {
        bytes
    } else if small_file_len(path)?.is_some() {
        read_all(path)?
    } else {
        let mut file = open_file(path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        return utf8_string(buf);
    };
    utf8_string(bytes.to_vec())
}

fn utf8_string(bytes: Vec<u8>) -> io::Result<String> {
    String::from_utf8(bytes).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

/// The first record of `path`, from bytes already read or from one shared open.
pub(crate) fn opening_record(path: &Path) -> io::Result<Option<(Vec<u8>, u64)>> {
    if let Some(bytes) = fresh(path) {
        return Ok(first_record(&bytes));
    }
    // Outside a sweep the caller only needs the first record. Slurping here
    // would read every sibling rollout a hydration merely classifies.
    if in_sweep() && small_file_len(path)?.is_some() {
        let bytes = read_all(path)?;
        return Ok(first_record(&bytes));
    }
    read_one_record(path)
}

fn first_record(bytes: &[u8]) -> Option<(Vec<u8>, u64)> {
    if bytes.is_empty() {
        return None;
    }
    let end = bytes
        .iter()
        .position(|byte| *byte == b'\n')
        .map(|index| index + 1)
        .unwrap_or(bytes.len());
    Some((bytes[..end].to_vec(), end as u64))
}

fn read_one_record(path: &Path) -> io::Result<Option<(Vec<u8>, u64)>> {
    use std::io::BufRead;
    let mut shared = open_shared(path)?;
    let mut reader = std::io::BufReader::new(shared.file());
    let mut raw = Vec::new();
    let ceiling = crate::ingest::transcript_cursor::MAX_RECORD_BYTES;
    let read = reader.by_ref().take(ceiling).read_until(b'\n', &mut raw)? as u64;
    if read == 0 {
        return Ok(None);
    }
    if raw.last() == Some(&b'\n') || read < ceiling {
        return Ok(Some((raw, read)));
    }
    match reader.fill_buf()?.first().copied() {
        Some(b'\n') => {
            reader.consume(1);
            raw.push(b'\n');
            Ok(Some((raw, read + 1)))
        }
        None => Ok(Some((raw, read))),
        Some(_) => {
            let skipped = skip_to_newline(&mut reader)?;
            Ok(Some((Vec::new(), read + skipped)))
        }
    }
}

fn skip_to_newline(reader: &mut impl std::io::BufRead) -> io::Result<u64> {
    let mut skipped = 0u64;
    loop {
        let buffered = reader.fill_buf()?;
        if buffered.is_empty() {
            return Ok(skipped);
        }
        match buffered.iter().position(|byte| *byte == b'\n') {
            Some(index) => {
                reader.consume(index + 1);
                return Ok(skipped + index as u64 + 1);
            }
            None => {
                let len = buffered.len();
                reader.consume(len);
                skipped += len as u64;
            }
        }
    }
}

/// Publish `walked` when it is still the generation the walk opened.
///
/// Size and mtime are not enough: a same-length rewrite can restore mtime.
/// The change time recorded at the first open has to match too, and only on
/// a filesystem that keeps one. Otherwise the next pass reads the path.
pub(crate) fn publish_walk(path: &Path, walked: &[u8], opened_len: u64, opened_mtime_ns: u64) {
    if walked.is_empty() || walked.len() as u64 != opened_len {
        return;
    }
    let opened_ctime = take_opened_ctime(path);
    let Ok(metadata) = fs::metadata(path) else {
        return;
    };
    if metadata.len() != opened_len || metadata_mtime_ns(&metadata) != opened_mtime_ns {
        return;
    }
    let same_generation = match (opened_ctime, change_time_ns(&metadata)) {
        (Some(opened), Some(now)) => opened == now && filesystem_keeps_change_time(path, &metadata),
        _ => false,
    };
    if same_generation {
        publish(path, Arc::<[u8]>::from(walked), &metadata);
    }
}

fn take_opened_ctime(path: &Path) -> Option<i64> {
    lock().opened.remove(path).and_then(|stat| stat.ctime_ns)
}

fn remember_open(path: &Path, metadata: &fs::Metadata) {
    let stat = Stat {
        len: metadata.len(),
        mtime_ns: metadata_mtime_ns(metadata),
        ctime_ns: change_time_ns(metadata),
        device: file_identity(metadata).0,
        inode: file_identity(metadata).1,
    };
    lock().opened.entry(path.to_path_buf()).or_insert(stat);
}

/// SHA-256 of the first `offset` bytes already in hand, and the hasher that
/// produced it so a cursor can keep updating the same prefix.
pub(crate) fn cached_sha_prefix(path: &Path, offset: u64) -> Option<(String, sha2::Sha256)> {
    use sha2::{Digest, Sha256};
    let bytes = fresh(path)?;
    if offset > bytes.len() as u64 {
        return None;
    }
    let mut hasher = Sha256::new();
    hasher.update(&bytes[..offset as usize]);
    let hash = format!("{:x}", hasher.clone().finalize());
    Some((hash, hasher))
}

/// Whether this process can read `path`.
///
/// One byte is enough: `EACCES`, `EISDIR` and `ENXIO` surface on the open or
/// the first read. The handle stays parked so the parse that follows is not a
/// second open. An unreadable file is not an observation — the walk must not
/// stamp it as empty.
pub(crate) fn probe(path: &Path) -> bool {
    let mut shared = match open_shared(path) {
        Ok(shared) => shared,
        Err(_) => return false,
    };
    let ok = shared.file().read(&mut [0u8; 1]).is_ok();
    if !ok {
        let _ = shared.into_file();
    }
    ok
}

/// An open file. Dropped handles go back to the park so the next reader of the
/// same unchanged path does not open it again. [`SharedFile::into_file`] takes
/// the handle for a reader that keeps it.
pub(crate) struct SharedFile {
    path: PathBuf,
    file: Option<fs::File>,
    repark: bool,
}

impl SharedFile {
    pub(crate) fn file(&mut self) -> &mut fs::File {
        self.file.as_mut().expect("shared file is live")
    }

    pub(crate) fn into_file(mut self) -> fs::File {
        self.repark = false;
        self.file.take().expect("shared file is live")
    }
}

impl Drop for SharedFile {
    fn drop(&mut self) {
        let Some(mut file) = self.file.take() else {
            return;
        };
        if self.repark && file.seek(SeekFrom::Start(0)).is_ok() {
            park(&self.path, file);
        }
    }
}

pub(crate) fn open_file(path: &Path) -> io::Result<fs::File> {
    open_shared(path).map(SharedFile::into_file)
}

pub(crate) fn open_shared(path: &Path) -> io::Result<SharedFile> {
    if let Some(file) = take_parked(path) {
        return Ok(SharedFile {
            path: path.to_path_buf(),
            file: Some(file),
            repark: true,
        });
    }
    let file = fs::File::open(path)?;
    if let Ok(metadata) = file.metadata() {
        remember_open(path, &metadata);
    }
    note_open(path);
    Ok(SharedFile {
        path: path.to_path_buf(),
        file: Some(file),
        repark: true,
    })
}

/// The handle parked for `path`, when it still names the same file. A take
/// for another path leaves the parked handle where it is: it belongs to the
/// read that parked it.
fn take_parked(path: &Path) -> Option<fs::File> {
    let current = stat_of(path).ok()?;
    let mut state = lock();
    match state.parked.take()? {
        parked if parked.path == path && parked.stat == current => Some(parked.file),
        parked if parked.path != path => {
            state.parked = Some(parked);
            None
        }
        _ => None,
    }
}

fn park(path: &Path, file: fs::File) {
    let Ok(stat) = stat_of(path) else {
        return;
    };
    let mut state = lock();
    state.parked = Some(Parked {
        path: path.to_path_buf(),
        file,
        stat,
    });
}

fn split_live(bytes: &[u8]) -> (Vec<u8>, Vec<u8>) {
    let head_budget = HEAD_SCAN_MAX_BYTES as usize;
    let tail_budget = TAIL_SCAN_MAX_BYTES as usize;
    if bytes.len() as u64 <= HEAD_SCAN_MAX_BYTES {
        let mut head = bytes.to_vec();
        keep_complete_lines(&mut head);
        return (head, Vec::new());
    }
    let mut head = bytes[..head_budget].to_vec();
    keep_complete_lines(&mut head);
    let tail_start = bytes.len().saturating_sub(tail_budget);
    let mut tail = bytes[tail_start..].to_vec();
    if let Some(first_newline) = tail.iter().position(|byte| *byte == b'\n') {
        tail.drain(..=first_newline);
    } else {
        tail.clear();
    }
    keep_complete_lines(&mut tail);
    (head, tail)
}

fn keep_complete_lines(buffer: &mut Vec<u8>) {
    match buffer.iter().rposition(|byte| *byte == b'\n') {
        Some(last_newline) => buffer.truncate(last_newline + 1),
        None => buffer.clear(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn claude_line(session_id: &str) -> String {
        format!(
            "{{\"type\":\"user\",\"uuid\":\"u1\",\"sessionId\":\"{session_id}\",\
             \"cwd\":\"/work/app\",\"timestamp\":\"2026-08-31T10:00:00Z\",\
             \"message\":{{\"role\":\"user\",\"content\":\"hello\"}}}}\n"
        )
    }

    fn codex_line(session_id: &str) -> String {
        format!(
            "{{\"timestamp\":\"2026-08-31T10:00:00Z\",\"type\":\"session_meta\",\
             \"payload\":{{\"id\":\"{session_id}\",\"cwd\":\"/work/app\"}}}}\n\
             {{\"timestamp\":\"2026-08-31T10:00:01Z\",\"type\":\"event_msg\",\
             \"payload\":{{\"type\":\"user_message\",\"message\":\"hello\"}}}}\n\
             {{\"timestamp\":\"2026-08-31T10:00:02Z\",\"type\":\"event_msg\",\
             \"payload\":{{\"type\":\"agent_message\",\"message\":\"done\"}}}}\n\
             {{\"timestamp\":\"2026-08-31T10:00:03Z\",\"type\":\"event_msg\",\
             \"payload\":{{\"type\":\"task_complete\",\"turn_id\":\"t1\"}}}}\n"
        )
    }

    fn cursor_line() -> &'static str {
        "{\"role\":\"user\",\"message\":{\"content\":[{\"type\":\"text\",\"text\":\"<user_query>\\nfix it\\n</user_query>\"}]}}\n"
    }

    /// A cold sweep opens a stable transcript once. The probe, the walks, the
    /// commit digest and discovery's head all share that open.
    #[test]
    fn a_cold_sweep_opens_a_stable_transcript_once() {
        let home = tempfile::tempdir().unwrap();
        let claude = home.path().join(".claude/projects/app/session-cold.jsonl");
        fs::create_dir_all(claude.parent().unwrap()).unwrap();
        fs::write(&claude, claude_line("session-cold")).unwrap();
        let codex = home
            .path()
            .join(".codex/sessions/2026/08/31/rollout-cold.jsonl");
        fs::create_dir_all(codex.parent().unwrap()).unwrap();
        fs::write(&codex, codex_line("codex-cold")).unwrap();
        let cursor = home
            .path()
            .join(".cursor/projects/app/agent-transcripts/cursor-cold/cursor-cold.jsonl");
        fs::create_dir_all(cursor.parent().unwrap()).unwrap();
        fs::write(&cursor, cursor_line()).unwrap();

        let db = home.path().join("history.db");
        // `from_home` ignores `CODEX_HOME` and `CLAUDE_CONFIG_DIR`. The
        // fixture lives under this temp home, not under a developer override.
        let roots = crate::ProviderRoots::from_home(
            home.path().to_path_buf(),
            home.path().join(".local/share/opencode/opencode.db"),
        );
        super::super::SYNC_QUIET.store(true, std::sync::atomic::Ordering::Relaxed);
        super::super::sync_exclusive_with_roots(&db, &roots, false).unwrap();

        let conn = crate::open_db(&db).unwrap();
        let claude_rows: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sessions WHERE source = 'claude' AND session_id = 'session-cold'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let codex_rows: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sessions WHERE source = 'codex' AND session_id = 'codex-cold'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let cursor_rows: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sessions WHERE source = 'cursor' AND session_id = 'cursor-cold'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(claude_rows, 1, "the claude fixture was ingested");
        assert_eq!(codex_rows, 1, "the codex fixture was ingested");
        assert_eq!(cursor_rows, 1, "the cursor fixture was ingested");
        assert_eq!(opens(&claude), 1, "claude cold open count");
        assert_eq!(opens(&codex), 1, "codex cold open count");
        assert_eq!(opens(&cursor), 1, "cursor cold open count");
    }

    /// A same-length rewrite that restores mtime still moves change time.
    /// The bytes the walk already held are not published under the new one.
    #[test]
    fn a_walk_does_not_publish_a_rewrite_that_restores_mtime() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cursor.jsonl");
        let original = b"{\"role\":\"user\"}\n";
        fs::write(&path, original).unwrap();
        let before = fs::metadata(&path).unwrap();
        let mtime = metadata_mtime_ns(&before);
        let _opened = open_file(&path).unwrap();
        drop(_opened);
        let rewritten = b"{\"role\":\"bot!\"}\n";
        assert_eq!(original.len(), rewritten.len());
        fs::write(&path, rewritten).unwrap();
        fs::File::open(&path)
            .unwrap()
            .set_modified(before.modified().unwrap())
            .unwrap();
        publish_walk(&path, original, before.len(), mtime);
        assert!(
            fresh(&path).is_none(),
            "walked bytes are not the generation now at the path"
        );
    }
}

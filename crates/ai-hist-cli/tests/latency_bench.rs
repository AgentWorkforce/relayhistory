//! Session-visible latency harness: how long from a harness writing a new
//! transcript to that session being on a list a desktop can render.
//!
//! This is the measurement half of the latency benchmark described in
//! `docs/benchmarks.md` ("Session-visible latency"). `scripts/benchmark-latency.mjs`
//! runs one phase of this harness per process and applies the budgets and
//! baselines in `scripts/latency-thresholds.json` to what it reports.
//!
//! Run one phase by hand with:
//!
//! ```text
//! AI_HIST_LATENCY_PHASE=stages AI_HIST_LATENCY_DAYS=30 \
//!   cargo test --release -p ai-hist-cli --test latency_bench -- --ignored --exact --nocapture \
//!   latency_phase
//! ```
//!
//! # The phases
//!
//! | Phase | What it times |
//! |---|---|
//! | `stages` | each budgeted stage on its own, against a store of `days` history: the raw filesystem event (`notify`: FSEvents on macOS, inotify on Linux), the hook-path parse + SQLite transaction (`hydrate` by path), the scoped forced `sync` a watch tick (and the desktop collector) runs, `open_db` on a populated store, a change-feed pull of one new session, and the catalog list query at the two page sizes that matter |
//! | `e2e` | the whole path through the public API: write a new transcript, `SessionStore::watch` (production defaults) reports it in `TickReport::changed`, the list query returns it first. Each sample is spaced past the debounce window so it is a leading-edge tick, the case a newly started session hits |
//! | `list_scale` | the list query alone at `days` of history, first page and a deep page |
//! | `ingest_upload` | ingest throughput and per-session latency, first alone, then while an uploader drains the change feed and commits its cursor on its own `SessionStore` (its own connections) |
//! | `prune` | list-query latency on a reader thread, first alone, then while retention maintenance (`SessionStore::prune_expired`, its default batch and cooldown, uploads acknowledged) evicts sessions older than `retain_days` for `prune_window_ms`; plus how long a new session's hook ingest waits meanwhile |
//! | `codex_idle` | a full `SessionStore::sync` over a settled Codex store with nothing new, forced and unforced: `codex_rollouts` rollouts, every `codex_compressed_every`-th one zstd-compressed, every `codex_subagent_every`-th one a delegated subagent of the rollout before it, all named by a Codex thread index (`state_5.sqlite`) |
//!
//! # What is measured, and how
//!
//! Every latency is an `Instant` pair around exactly one operation, collected
//! per sample and reported as nearest-rank p50/p90/p99/max in milliseconds.
//! Store generation and the populating sync happen outside every timed
//! region. Each sample of a write stage is a *new* session, never a re-read
//! of one already stored, because a new session is what the budget is about.
//!
//! The store is a fake HOME with Claude transcripts only, read through
//! `ProviderRoots::from_home`, so nothing in the process environment can point
//! a phase at a real provider directory. History is `sessions_per_day`
//! sessions on each of `days` days, each `turns` turns long.
//!
//! It is `#[ignore]`d for the same reason `sync_bench.rs` is: its headline
//! numbers are wall clocks.

use ai_hist::retention::{RetentionOptions, RetentionPolicy};
use ai_hist::{
    CatalogQuery, ChangeQuery, HydrateOptions, HydrateStatus, ProviderRoots, SessionRef,
    SessionScope, SessionStore, Source, StoreOptions, SyncOptions, WatchDriver, WatchOptions,
    Watermark,
};
use notify::{RecursiveMode, Watcher};
use serde_json::{json, Value};
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

const DAY_MS: i64 = 24 * 60 * 60 * 1000;
/// The desktop's own list page (`SESSION_LIST_LIMIT` in relay-desktop).
const DESKTOP_PAGE: i64 = 500;
/// A first paint: the rows above the fold.
const FIRST_PAINT_PAGE: i64 = 50;
/// Projects the generated sessions are spread over.
const PROJECTS: u64 = 20;

// ---------------------------------------------------------------------------
// configuration
// ---------------------------------------------------------------------------

fn env_u64(name: &str, fallback: u64) -> u64 {
    std::env::var(name)
        .ok()
        .map(|value| {
            value
                .parse()
                .unwrap_or_else(|_| panic!("{name} must be an integer"))
        })
        .unwrap_or(fallback)
}

struct Config {
    phase: String,
    days: u64,
    sessions_per_day: u64,
    turns: u64,
    samples: u64,
    sweep_samples: u64,
    fs_samples: u64,
    retain_days: u64,
    prune_window_ms: u64,
    codex_rollouts: u64,
    codex_compressed_every: u64,
    codex_subagent_every: u64,
}

impl Config {
    fn from_env() -> Self {
        Self {
            phase: std::env::var("AI_HIST_LATENCY_PHASE")
                .expect("AI_HIST_LATENCY_PHASE must be set; see the module docs"),
            days: env_u64("AI_HIST_LATENCY_DAYS", 30),
            sessions_per_day: env_u64("AI_HIST_LATENCY_SESSIONS_PER_DAY", 40),
            turns: env_u64("AI_HIST_LATENCY_TURNS", 2),
            samples: env_u64("AI_HIST_LATENCY_SAMPLES", 200),
            sweep_samples: env_u64("AI_HIST_LATENCY_SWEEP_SAMPLES", 20),
            fs_samples: env_u64("AI_HIST_LATENCY_FS_SAMPLES", 1000),
            retain_days: env_u64("AI_HIST_LATENCY_RETAIN_DAYS", 30),
            prune_window_ms: env_u64("AI_HIST_LATENCY_PRUNE_WINDOW_MS", 10_000),
            codex_rollouts: env_u64("AI_HIST_LATENCY_CODEX_ROLLOUTS", 3_000),
            codex_compressed_every: env_u64("AI_HIST_LATENCY_CODEX_COMPRESSED_EVERY", 2),
            codex_subagent_every: env_u64("AI_HIST_LATENCY_CODEX_SUBAGENT_EVERY", 5),
        }
    }
}

// ---------------------------------------------------------------------------
// statistics
// ---------------------------------------------------------------------------

/// Nearest-rank percentiles of a set of durations, in milliseconds.
fn summary(samples: &[Duration]) -> Value {
    if samples.is_empty() {
        return json!({ "n": 0 });
    }
    let mut ms: Vec<f64> = samples.iter().map(|d| d.as_secs_f64() * 1000.0).collect();
    ms.sort_by(|a, b| a.partial_cmp(b).expect("durations are finite"));
    let rank = |p: f64| ms[((p * ms.len() as f64).ceil() as usize).clamp(1, ms.len()) - 1];
    let round = |v: f64| (v * 1000.0).round() / 1000.0;
    json!({
        "n": ms.len(),
        "p50": round(rank(0.50)),
        "p90": round(rank(0.90)),
        "p99": round(rank(0.99)),
        "max": round(*ms.last().unwrap()),
        "mean": round(ms.iter().sum::<f64>() / ms.len() as f64),
    })
}

fn time<T>(op: impl FnOnce() -> T) -> (T, Duration) {
    let started = Instant::now();
    let out = op();
    (out, started.elapsed())
}

// ---------------------------------------------------------------------------
// the synthetic store
// ---------------------------------------------------------------------------

fn session_id(n: u64) -> String {
    format!("{:08x}-0000-4000-8000-{:012x}", n >> 16, n)
}

fn iso(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .expect("timestamp in range")
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// A Claude transcript shaped like `claudeTranscript` in
/// `scripts/benchmark-sync-lib.mjs`: per turn a prompt, an assistant record
/// with thinking, text and one tool use, and the matching tool result.
fn transcript(id: &str, project: &str, turns: u64, base_ms: i64) -> String {
    let cwd = format!("/work/{project}");
    let mut out = String::new();
    let mut previous = Value::Null;
    for turn in 0..turns {
        let at = |step: u64| iso(base_ms + ((turn * 3 + step) * 1000) as i64);
        let prompt = format!("{id}-{turn}-p");
        let assistant = format!("{id}-{turn}-a");
        let result = format!("{id}-{turn}-r");
        let tool = format!("toolu_{id}_{turn}");
        let records = [
            json!({"sessionId": id, "uuid": prompt, "parentUuid": previous, "cwd": cwd,
                "gitBranch": "main", "version": "1.2.3", "type": "user",
                "message": {"role": "user", "content": [{"type": "text",
                    "text": format!("turn {turn}: make the build faster and keep it green")}]},
                "timestamp": at(0)}),
            json!({"sessionId": id, "uuid": assistant, "parentUuid": prompt, "cwd": cwd,
                "gitBranch": "main", "type": "assistant",
                "message": {"role": "assistant", "model": "claude-sonnet-5",
                    "usage": {"input_tokens": 1200 + turn, "output_tokens": 300 + turn},
                    "content": [
                        {"type": "thinking", "thinking": "look at the slow step first"},
                        {"type": "text", "text": "Reading the build script."},
                        {"type": "tool_use", "id": tool, "name": "Bash",
                            "input": {"command": "rg --files src"}}]},
                "timestamp": at(1)}),
            json!({"sessionId": id, "uuid": result, "parentUuid": assistant, "cwd": cwd,
                "gitBranch": "main", "type": "user",
                "message": {"role": "user", "content": [{"type": "tool_result",
                    "tool_use_id": tool, "content": [{"type": "text", "text": "src/main.rs"}]}]},
                "timestamp": at(2)}),
        ];
        for record in records {
            out.push_str(&record.to_string());
            out.push('\n');
        }
        previous = Value::String(result);
    }
    out
}

struct Bench {
    /// Keeps a temporary store alive; `None` for the fixed work directory.
    _dir: Option<tempfile::TempDir>,
    root: PathBuf,
    home: PathBuf,
    db: PathBuf,
    /// Where a populated store is kept between phases, when there is one.
    cache: Option<PathBuf>,
    next_id: AtomicU64,
    turns: u64,
}

impl Bench {
    /// With `AI_HIST_LATENCY_DIR`, the store lives at `<dir>/work`, on the
    /// disk being measured, and populated stores are cached under
    /// `<dir>/cache` so the five phases build a year of history once rather
    /// than five times. The work path is fixed because the catalog records
    /// absolute transcript paths: a store restored anywhere else would be a
    /// store whose every session had moved. Phases run one process at a
    /// time, so one work directory is enough. Without it -- the system temp
    /// dir, which is tmpfs on some Linux hosts and makes an fsync free -- the
    /// store is a fresh temporary directory and nothing is cached.
    fn new(turns: u64) -> Self {
        let (dir, root, cache) = match std::env::var_os("AI_HIST_LATENCY_DIR") {
            Some(parent) => {
                let parent = PathBuf::from(parent);
                let root = parent.join("work");
                let _ = fs::remove_dir_all(&root);
                fs::create_dir_all(&root).unwrap();
                (None, root, Some(parent.join("cache")))
            }
            None => {
                let dir = tempfile::tempdir().expect("temp home");
                let root = dir.path().to_path_buf();
                (Some(dir), root, None)
            }
        };
        let home = root.join("home");
        fs::create_dir_all(home.join(".claude/projects")).unwrap();
        Self {
            _dir: dir,
            db: root.join("ai-history.db"),
            root,
            home,
            cache,
            next_id: AtomicU64::new(1),
            turns,
        }
    }

    fn roots(&self) -> ProviderRoots {
        ProviderRoots::from_home(
            self.home.clone(),
            self.home.join(".local/share/opencode/opencode.db"),
        )
    }

    fn open(&self) -> SessionStore {
        SessionStore::open(
            StoreOptions::default()
                .db_path(&self.db)
                .roots(self.roots()),
        )
        .expect("open store")
    }

    fn project_dir(&self, n: u64) -> PathBuf {
        self.home
            .join(".claude/projects")
            .join(format!("-work-p{}", n % PROJECTS))
    }

    /// The bytes and path of a session that does not exist yet.
    fn next_session(&self, base_ms: i64) -> (String, PathBuf, String) {
        let n = self.next_id.fetch_add(1, Ordering::Relaxed);
        let id = session_id(n);
        let dir = self.project_dir(n);
        fs::create_dir_all(&dir).unwrap();
        let body = transcript(&id, &format!("p{}", n % PROJECTS), self.turns, base_ms);
        (id.clone(), dir.join(format!("{id}.jsonl")), body)
    }

    /// `days` of history at `per_day`, ingested by one untimed forced sync,
    /// or restored from the cache when an earlier phase built the same one.
    fn populate(&self, days: u64, per_day: u64) -> (SessionStore, Value) {
        let sessions = days * per_day;
        let key = format!("v1-d{days}-s{per_day}-t{}", self.turns);
        let cached = self.cache.as_ref().map(|cache| cache.join(&key));
        let (restored, elapsed) = match cached.as_ref().filter(|dir| dir.join("done").exists()) {
            Some(dir) => {
                let (_, elapsed) = time(|| {
                    fs::remove_dir_all(&self.root).unwrap();
                    copy_dir(&dir.join("work"), &self.root);
                });
                (true, elapsed)
            }
            None => {
                let store = self.open();
                let now = now_ms();
                for day in 0..days {
                    for slot in 0..per_day {
                        let base = now - (days - day) as i64 * DAY_MS
                            + (slot as i64 * DAY_MS / per_day as i64);
                        let (_, path, body) = self.next_session(base);
                        fs::write(path, body).unwrap();
                    }
                }
                let (_, elapsed) = time(|| {
                    store
                        .sync(SyncOptions::default().force(true))
                        .expect("populating sync")
                });
                if let Some(dir) = &cached {
                    checkpoint(&self.db);
                    let _ = fs::remove_dir_all(dir);
                    copy_dir(&self.root, &dir.join("work"));
                    fs::write(dir.join("done"), b"").unwrap();
                }
                drop(store);
                (false, elapsed)
            }
        };
        self.next_id.store(sessions + 1, Ordering::Relaxed);
        let store = self.open();

        let db_bytes = fs::metadata(&self.db).map(|m| m.len()).unwrap_or(0);
        let info = json!({
            "days": days,
            "sessionsPerDay": per_day,
            "sessions": sessions,
            "restoredFromCache": restored,
            "populateMs": elapsed.as_millis() as u64,
            "headRevision": store.head_revision().expect("head").revision,
            "dbBytes": db_bytes,
        });
        (store, info)
    }
}

/// Fold the WAL into the main file, so copying the database file alone
/// copies all of it.
fn checkpoint(db: &Path) {
    let conn = rusqlite::Connection::open(db).expect("open for checkpoint");
    conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))
        .expect("checkpoint");
}

/// Copy a store: the database files are copied, every transcript is hard
/// linked. A link keeps the inode and mtime, which is what the store's
/// per-transcript stamp checks, so a restored store looks unchanged to the
/// next sweep instead of costing it a re-read of every session. No phase
/// writes into a transcript the populate step created -- new sessions are
/// new files, and a prune deletes rows, not files -- so sharing them with the
/// cache is safe. Cache and work dir sit under one parent, on one filesystem.
fn copy_dir(from: &Path, to: &Path) {
    fs::create_dir_all(to).unwrap();
    for entry in fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else if entry
            .file_name()
            .to_string_lossy()
            .starts_with("ai-history.db")
        {
            fs::copy(entry.path(), target).unwrap();
        } else {
            fs::hard_link(entry.path(), target).unwrap();
        }
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// `(device, inode)` of a file: what a cached schema check compares to
/// notice the database was replaced underneath an open connection.
#[cfg(unix)]
fn file_identity(path: &Path) -> (u64, u64) {
    use std::os::unix::fs::MetadataExt;
    let meta = fs::metadata(path).expect("stat database");
    (meta.dev(), meta.ino())
}

#[cfg(not(unix))]
fn file_identity(path: &Path) -> (u64, u64) {
    (fs::metadata(path).expect("stat database").len(), 0)
}

/// The macOS alternative to FSEvents for a new transcript: a kqueue
/// `EVFILT_VNODE` / `NOTE_WRITE` watch on the project *directory*, which fires
/// when an entry is created in it. Raw libc rather than notify's kqueue
/// backend: enabling notify's `macos_kqueue` feature, even for this test,
/// unifies into ai-hist's own notify and switches its `RecommendedWatcher` to
/// kqueue, which would change what every other phase measures.
///
/// A vnode event names no file, so a watcher still has to find which entry
/// is new. `resolved` adds that step -- `read_dir` of the project directory
/// diffed against the names already known -- to the event time.
#[cfg(target_os = "macos")]
mod kqueue {
    use super::{session_id, write_file};
    use std::collections::HashSet;
    use std::ffi::{CString, OsString};
    use std::fs;
    use std::os::unix::ffi::OsStrExt;
    use std::path::Path;
    use std::time::{Duration, Instant};

    /// Sessions already in the directory, so the rescan reads a project
    /// directory of realistic size rather than an empty one.
    const EXISTING: u64 = 200;

    fn names(dir: &Path) -> HashSet<OsString> {
        fs::read_dir(dir)
            .expect("read project dir")
            .map(|entry| entry.expect("entry").file_name())
            .collect()
    }

    pub(super) fn dir_latency(root: &Path, samples: u64) -> (Vec<Duration>, Vec<Duration>) {
        let dir = root.join("kqueue-events");
        fs::create_dir_all(&dir).unwrap();
        for n in 0..EXISTING {
            write_file(
                &dir.join(format!("{}.jsonl", session_id(2 << 40 | n))),
                "{}\n",
            );
        }
        let path = CString::new(dir.as_os_str().as_bytes()).unwrap();
        // SAFETY: plain syscalls on a NUL-terminated path and fds this
        // function owns and closes; the kevent structs are fully initialized.
        unsafe {
            let fd = libc::open(path.as_ptr(), libc::O_EVTONLY);
            assert!(fd >= 0, "open project dir for events");
            let kq = libc::kqueue();
            assert!(kq >= 0, "kqueue");
            let mut change: libc::kevent = std::mem::zeroed();
            change.ident = fd as libc::uintptr_t;
            change.filter = libc::EVFILT_VNODE;
            change.flags = libc::EV_ADD | libc::EV_CLEAR;
            change.fflags = libc::NOTE_WRITE;
            let added = libc::kevent(kq, &change, 1, std::ptr::null_mut(), 0, std::ptr::null());
            assert_eq!(added, 0, "register EVFILT_VNODE");
            let mut known = names(&dir);
            let timeout = libc::timespec {
                tv_sec: 2,
                tv_nsec: 0,
            };
            let mut event_times = Vec::new();
            let mut resolved_times = Vec::new();
            for n in 0..samples {
                let name = format!("{}.jsonl", session_id(3 << 40 | n));
                let started = Instant::now();
                write_file(&dir.join(&name), "{}\n");
                let mut event: libc::kevent = std::mem::zeroed();
                let got = libc::kevent(kq, std::ptr::null(), 0, &mut event, 1, &timeout);
                assert_eq!(got, 1, "no kqueue event for {name} within 2 s");
                event_times.push(started.elapsed());
                let now = names(&dir);
                let new: Vec<_> = now.difference(&known).cloned().collect();
                assert!(
                    new.iter().any(|entry| entry.as_bytes() == name.as_bytes()),
                    "rescan did not find {name}"
                );
                resolved_times.push(started.elapsed());
                known = now;
                // Drain anything else this write raised before the next clock.
                let zero = libc::timespec {
                    tv_sec: 0,
                    tv_nsec: 0,
                };
                while libc::kevent(kq, std::ptr::null(), 0, &mut event, 1, &zero) > 0 {}
            }
            libc::close(kq);
            libc::close(fd);
            (event_times, resolved_times)
        }
    }
}

fn write_file(path: &Path, body: &str) {
    let mut file = fs::File::create(path).unwrap();
    file.write_all(body.as_bytes()).unwrap();
}

fn list_query(store: &SessionStore, page: i64) -> Vec<(Source, String)> {
    let mut query = CatalogQuery::default();
    query.scope = SessionScope::All;
    query.exclude_delegated = true;
    query.page_size = page;
    store
        .sessions(query)
        .take(page as usize)
        .map(|row| {
            let row = row.expect("catalog row");
            (row.source, row.session_id)
        })
        .collect()
}

fn names(report: &[SessionRef], id: &str) -> bool {
    report
        .iter()
        .any(|r| matches!(r, SessionRef::Id { session_id, .. } if session_id == id))
}

// ---------------------------------------------------------------------------
// phases
// ---------------------------------------------------------------------------

/// Write → first watcher callback for that path. The floor of stage 1; what
/// the production watch loop adds on top shows up in `e2e`.
fn fs_event_latency(bench: &Bench, samples: u64) -> Vec<Duration> {
    // Outside every provider root: these files are not transcripts, and the
    // timed sweeps that follow must not walk them.
    let dir = bench.root.join("fs-events");
    fs::create_dir_all(&dir).unwrap();
    let (tx, rx) = mpsc::channel::<(Instant, Vec<PathBuf>)>();
    let mut watcher = notify::recommended_watcher(move |event: notify::Result<notify::Event>| {
        if let Ok(event) = event {
            let _ = tx.send((Instant::now(), event.paths));
        }
    })
    .expect("watcher");
    // Canonical, because FSEvents reports `/private/var/...` for `/var/...`.
    let dir = dir.canonicalize().unwrap();
    watcher.watch(&dir, RecursiveMode::Recursive).unwrap();
    // FSEvents replays registration-time history; let it drain.
    std::thread::sleep(Duration::from_millis(200));
    while rx.try_recv().is_ok() {}
    let mut out = Vec::new();
    for n in 0..samples {
        let path = dir.join(format!("{}.jsonl", session_id(1 << 40 | n)));
        let started = Instant::now();
        write_file(&path, "{}\n");
        let deadline = started + Duration::from_secs(2);
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            match rx.recv_timeout(left) {
                Ok((at, paths)) if paths.iter().any(|p| p == &path) => {
                    out.push(at.duration_since(started));
                    break;
                }
                Ok(_) => continue,
                Err(_) => panic!("no filesystem event for {} within 2 s", path.display()),
            }
        }
        // Let trailing events (modify, close) for this file arrive before the
        // next sample starts its clock.
        std::thread::sleep(Duration::from_millis(5));
        while rx.try_recv().is_ok() {}
    }
    out
}

fn stages(config: &Config) -> Value {
    let bench = Bench::new(config.turns);
    let (store, store_info) = bench.populate(config.days, config.sessions_per_day);
    let samples = config.samples;

    // Cheap (one small write and a 5 ms settle each), and a raw-event tail
    // needs more than a hundred samples before its p99 stops being its max.
    let fs_event = fs_event_latency(&bench, config.fs_samples);
    #[cfg(target_os = "macos")]
    let (kq_event, kq_resolved) = kqueue::dir_latency(&bench.root, config.fs_samples);

    // Hook path: one new transcript, parsed and committed in its own
    // transaction. This is the parse + SQLite txn stage.
    let mut ingest = Vec::new();
    for _ in 0..samples {
        let (_, path, body) = bench.next_session(now_ms());
        write_file(&path, &body);
        let (report, elapsed) = time(|| {
            store
                .hydrate(
                    &SessionRef::path(Source::Claude, &path),
                    HydrateOptions::default(),
                )
                .expect("hook ingest")
        });
        assert_eq!(
            report.status,
            HydrateStatus::Hydrated,
            "a new session is hydrated"
        );
        ingest.push(elapsed);
    }

    // What a watch tick and the desktop collector actually run for an event
    // under the Claude root: a forced sync scoped to that source. It walks
    // the whole root, so it costs ~100 ms a sample on a 30-day store; fewer
    // samples, so a gate round stays in seconds.
    let mut sweep = Vec::new();
    for _ in 0..config.sweep_samples {
        let (_, path, body) = bench.next_session(now_ms());
        write_file(&path, &body);
        let (_, elapsed) = time(|| {
            store
                .sync(SyncOptions::default().force(true).sources([Source::Claude]))
                .expect("scoped sync")
        });
        sweep.push(elapsed);
    }

    // The exact-path capture an embedder's watcher is meant to use
    // (`SyncOptions::paths`): first through a plain `sync`, then through one
    // `Reconciliation` round, which retains its validated writer between
    // calls the way a long-lived watcher would.
    let mut capture = Vec::new();
    for _ in 0..samples {
        let (_, path, body) = bench.next_session(now_ms());
        write_file(&path, &body);
        let (_, elapsed) = time(|| {
            store
                .sync(
                    SyncOptions::default()
                        .sources([Source::Claude])
                        .paths([path]),
                )
                .expect("exact path capture")
        });
        capture.push(elapsed);
    }
    let mut capture_retained = Vec::new();
    {
        let mut round = store.reconcile().expect("reconciliation round");
        for _ in 0..samples {
            let (_, path, body) = bench.next_session(now_ms());
            write_file(&path, &body);
            let (_, elapsed) = time(|| {
                round
                    .capture_paths(
                        SyncOptions::default()
                            .sources([Source::Claude])
                            .paths([path]),
                    )
                    .expect("retained exact path capture")
            });
            capture_retained.push(elapsed);
        }
    }

    // Every sync, forget, discover and change-feed commit opens the store
    // writable, which runs the schema check; this is that fixed cost.
    let mut open = Vec::new();
    for _ in 0..samples.min(100) {
        let (conn, elapsed) = time(|| ai_hist::open_db(&bench.db).expect("open_db"));
        drop(conn);
        open.push(elapsed);
    }

    // The same open without the schema check: SQLite's own share of it.
    let mut open_raw = Vec::new();
    for _ in 0..samples.min(100) {
        let (_, elapsed) = time(|| {
            let conn = rusqlite::Connection::open(&bench.db).expect("open");
            conn.query_row("PRAGMA schema_version", [], |row| row.get::<_, i64>(0))
                .expect("schema_version")
        });
        open_raw.push(elapsed);
    }

    // What a cached "schema is current" check would cost per write instead
    // of `open_db`'s full check: on a connection already open, stat the
    // database file for the (dev, ino) it was opened as -- a replaced file
    // must invalidate the cache -- and read `PRAGMA schema_version`, which
    // moves on any DDL. Measures the proposal; no cache exists yet.
    let mut schema_cached = Vec::new();
    {
        let conn = rusqlite::Connection::open(&bench.db).expect("open");
        let opened_as = file_identity(&bench.db);
        let version: i64 = conn
            .query_row("PRAGMA schema_version", [], |row| row.get(0))
            .expect("schema_version");
        for _ in 0..samples {
            let (current, elapsed) = time(|| {
                file_identity(&bench.db) == opened_as
                    && conn
                        .query_row("PRAGMA schema_version", [], |row| row.get::<_, i64>(0))
                        .expect("schema_version")
                        == version
            });
            assert!(current, "nothing replaced the database mid-measurement");
            schema_cached.push(elapsed);
        }
    }

    // A pull consumer learning about one new session: the read a polling
    // desktop would do on each tick, after the commit.
    let mut feed = Vec::new();
    for _ in 0..samples {
        let head = store.head_revision().expect("head");
        let (_, path, body) = bench.next_session(now_ms());
        write_file(&path, &body);
        store
            .hydrate(
                &SessionRef::path(Source::Claude, &path),
                HydrateOptions::default(),
            )
            .expect("hook ingest");
        let (count, elapsed) = time(|| {
            store
                .changes_since(head, ChangeQuery::default())
                .expect("changes_since")
                .inspect(|change| assert!(change.is_ok(), "change"))
                .count()
        });
        assert!(count > 0);
        feed.push(elapsed);
    }

    let mut first_paint = Vec::new();
    let mut desktop_page = Vec::new();
    for _ in 0..samples {
        first_paint.push(time(|| list_query(&store, FIRST_PAINT_PAGE)).1);
        desktop_page.push(time(|| list_query(&store, DESKTOP_PAGE)).1);
    }

    // Only macOS adds to it below.
    #[cfg_attr(not(target_os = "macos"), allow(unused_mut))]
    let mut report = json!({
        "store": store_info,
        "metrics": {
            "fs_event": summary(&fs_event),
            "ingest_hook": summary(&ingest),
            "sweep_scoped_sync": summary(&sweep),
            "capture_path": summary(&capture),
            "capture_path_retained": summary(&capture_retained),
            "open_db": summary(&open),
            "open_raw": summary(&open_raw),
            "schema_check_cached": summary(&schema_cached),
            "feed_pull": summary(&feed),
            "list_first_paint": summary(&first_paint),
            "list_desktop_page": summary(&desktop_page),
        }
    });
    #[cfg(target_os = "macos")]
    {
        report["metrics"]["fs_event_kqueue"] = summary(&kq_event);
        report["metrics"]["fs_event_kqueue_resolved"] = summary(&kq_resolved);
    }
    report
}

fn e2e(config: &Config) -> Value {
    let bench = Bench::new(config.turns);
    let (store, store_info) = bench.populate(config.days, config.sessions_per_day);
    let options = WatchOptions::default();
    let quiet = Duration::from_millis(options.debounce_ms + 50);
    let mut handle = store.watch(options).expect("watch");
    // Drain the startup tick (and, on macOS, the replayed registration
    // history) before the first sample starts its clock.
    while handle.next_timeout(Duration::from_millis(1500)).is_some() {}
    let driver = handle.driver();

    let mut total = Vec::new();
    let mut to_tick = Vec::new();
    let mut query = Vec::new();
    let mut misses = 0u64;
    // Why a slow sample was slow: the ticks that reported before the one
    // that named the new session. A session the first tick after the write
    // misses waits out the debounce window for the trailing tick.
    let mut late = 0u64;
    let mut late_examples = Vec::new();
    for _ in 0..config.samples {
        let (id, path, body) = bench.next_session(now_ms());
        let started = Instant::now();
        write_file(&path, &body);
        let deadline = started + Duration::from_secs(10);
        let mut seen = None;
        let mut before = Vec::new();
        while let Some(report) =
            handle.next_timeout(deadline.saturating_duration_since(Instant::now()))
        {
            let report = report.expect("tick");
            if names(&report.changed, &id) {
                seen = Some(Instant::now());
                break;
            }
            before.push(json!({
                "atMs": started.elapsed().as_secs_f64() * 1000.0,
                "trigger": format!("{:?}", report.trigger),
                "swept": report.swept,
                "contended": report.contended,
                "elapsedMs": report.elapsed_ms,
                "firstEventAgeMs": report.first_event_age_ms,
                "changed": report.changed.len(),
                "sources": report.sources.as_ref().map(|s| s.len()),
            }));
        }
        if !before.is_empty() {
            late += 1;
            if late_examples.len() < 8 {
                late_examples.push(json!({
                    "visibleMs": seen.map(|at| at.duration_since(started).as_secs_f64() * 1000.0),
                    "ticksBefore": before,
                }));
            }
        }
        let Some(ticked) = seen else {
            misses += 1;
            continue;
        };
        let rows = list_query(&store, FIRST_PAINT_PAGE);
        let listed = Instant::now();
        assert!(
            rows.iter().any(|(_, row)| row == &id),
            "the reported session is not on the first page"
        );
        to_tick.push(ticked.duration_since(started));
        query.push(listed.duration_since(ticked));
        total.push(listed.duration_since(started));
        // Quiet past the debounce window, so the next write is a leading
        // edge: a new session, not a busy one.
        std::thread::sleep(quiet);
        while let Some(Ok(_)) = handle.next_timeout(Duration::ZERO) {}
    }
    handle.stop();
    // A session that never became visible is the worst latency there is, not
    // a sample to leave out: the phase fails, and with it the gate.
    assert_eq!(
        misses, 0,
        "{misses} of {} new sessions were not reported by watch within 10 s",
        config.samples
    );
    json!({
        "store": store_info,
        "driver": match driver {
            Some(WatchDriver::FsEvents) => "fs-events",
            Some(WatchDriver::Polling) => "polling",
            None => "unknown",
        },
        "watchDefaults": {
            "debounceMs": ai_hist_watch_default_debounce(),
            "leadingEdgeSettleMs": ai_hist::watch::LEADING_EDGE_SETTLE_MS,
        },
        "misses": misses,
        "lateSamples": late,
        "lateExamples": late_examples,
        "metrics": {
            "e2e_visible": summary(&total),
            "e2e_write_to_tick": summary(&to_tick),
            "e2e_tick_to_listed": summary(&query),
        }
    })
}

fn ai_hist_watch_default_debounce() -> u64 {
    WatchOptions::default().debounce_ms
}

fn list_scale(config: &Config) -> Value {
    let bench = Bench::new(config.turns);
    let (store, store_info) = bench.populate(config.days, config.sessions_per_day);
    let mut first_paint = Vec::new();
    let mut desktop_page = Vec::new();
    let mut deep_page = Vec::new();
    // A page starting halfway back through history: "load more", or a
    // date-scoped list.
    let middle = now_ms() - (config.days as i64 * DAY_MS) / 2;
    for _ in 0..config.samples {
        first_paint.push(time(|| list_query(&store, FIRST_PAINT_PAGE)).1);
        desktop_page.push(time(|| list_query(&store, DESKTOP_PAGE)).1);
        deep_page.push(
            time(|| {
                let mut query = CatalogQuery::default();
                query.scope = SessionScope::All;
                query.exclude_delegated = true;
                query.page_size = FIRST_PAINT_PAGE;
                query.before_ms = Some(middle);
                store
                    .sessions(query)
                    .take(FIRST_PAINT_PAGE as usize)
                    .fold(0usize, |rows, row| {
                        row.expect("catalog row");
                        rows + 1
                    })
            })
            .1,
        );
    }
    json!({
        "store": store_info,
        "metrics": {
            "list_first_paint": summary(&first_paint),
            "list_desktop_page": summary(&desktop_page),
            "list_deep_page": summary(&deep_page),
        }
    })
}

/// Ingest `count` new sessions through the hook path, one at a time.
fn ingest_batch(bench: &Bench, store: &SessionStore, count: u64) -> (Vec<Duration>, Duration) {
    let mut each = Vec::new();
    let started = Instant::now();
    for _ in 0..count {
        let (_, path, body) = bench.next_session(now_ms());
        write_file(&path, &body);
        let (_, elapsed) = time(|| {
            store
                .hydrate(
                    &SessionRef::path(Source::Claude, &path),
                    HydrateOptions::default(),
                )
                .expect("hook ingest")
        });
        each.push(elapsed);
    }
    (each, started.elapsed())
}

fn ingest_upload(config: &Config) -> Value {
    let bench = Bench::new(config.turns);
    let (store, store_info) = bench.populate(config.days, config.sessions_per_day);
    let count = config.samples;

    let (alone, alone_wall) = ingest_batch(&bench, &store, count);

    // The uploader: its own SessionStore (its own read pool, and its own
    // writable connection per commit), draining every kind from its named
    // cursor and committing after each drain -- what a delivery coordinator
    // does before a destination ever sees a byte.
    let uploader = bench.open();
    let stop = Arc::new(AtomicBool::new(false));
    let upload_stop = stop.clone();
    let (ready, caught_up) = mpsc::channel::<()>();
    let worker = std::thread::spawn(move || {
        let mut catchup = None;
        let mut drains = Vec::new();
        let mut commits = Vec::new();
        let mut rows = 0u64;
        let mut bytes = 0u64;
        let mut busy = 0u64;
        while !upload_stop.load(Ordering::Relaxed) {
            let (changes, drain) = time(|| {
                let mut changes = uploader
                    .changes_since(
                        Watermark::CONSUMER,
                        ChangeQuery::default()
                            .consumer("latency-bench-uploader")
                            .batch(1000),
                    )
                    .expect("changes_since");
                let mut n = 0u64;
                for change in changes.by_ref() {
                    // Serialize each row, as a destination batch would.
                    let change = change.expect("change");
                    bytes += serde_json::to_vec(&change).expect("serialize").len() as u64;
                    n += 1;
                }
                (changes, n)
            });
            let (changes, n) = changes;
            rows += n;
            // The first drain is the backlog of every row the populated
            // store holds; steady state is everything after it.
            if catchup.is_none() {
                catchup = Some(drain);
            } else {
                drains.push(drain);
            }
            let (result, commit) = time(|| changes.commit());
            match result {
                Ok(_) => commits.push(commit),
                Err(_) => busy += 1,
            }
            let _ = ready.send(());
            if n == 0 {
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        (catchup, drains, commits, rows, bytes, busy)
    });
    // Measure steady state: the uploader has drained the backlog first.
    caught_up.recv().expect("uploader caught up");
    let (with_upload, with_wall) = ingest_batch(&bench, &store, count);
    stop.store(true, Ordering::Relaxed);
    let (catchup, drains, commits, rows, bytes, busy) = worker.join().expect("uploader");

    let rate = |n: u64, wall: Duration| n as f64 / wall.as_secs_f64();
    let alone_rate = rate(count, alone_wall);
    let with_rate = rate(count, with_wall);
    json!({
        "store": store_info,
        "sessionsPerPhase": count,
        "sessionsPerSecondAlone": (alone_rate * 10.0).round() / 10.0,
        "sessionsPerSecondWithUploader": (with_rate * 10.0).round() / 10.0,
        "throughputRatio": ((with_rate / alone_rate) * 1000.0).round() / 1000.0,
        "uploaderCatchupMs": catchup.map(|d| d.as_millis() as u64),
        "uploaderRowsDrained": rows,
        "uploaderBytesSerialized": bytes,
        "uploaderCommitFailures": busy,
        "metrics": {
            "ingest_alone": summary(&alone),
            "ingest_with_uploader": summary(&with_upload),
            "uploader_drain": summary(&drains),
            "uploader_commit": summary(&commits),
        }
    })
}

fn prune(config: &Config) -> Value {
    let bench = Bench::new(config.turns);
    let (store, store_info) = bench.populate(config.days, config.sessions_per_day);
    // The retention a desktop would configure, and an uploader that has
    // durably delivered everything so far: without an acknowledgement
    // retention evicts nothing, by design.
    store
        .set_retention_policy(RetentionPolicy::from_days(config.retain_days as u32))
        .expect("retention policy");
    store
        .register_retention_consumer("latency-bench-uploader")
        .expect("register consumer");
    store
        .acknowledge_retention(
            "latency-bench-uploader",
            store.head_revision().expect("head"),
        )
        .expect("acknowledge");

    // One reader, its own store handle, sampling the first-paint list query
    // back to back until told to stop.
    let read = |stop: Arc<AtomicBool>| {
        let reader = bench.open();
        std::thread::spawn(move || {
            let mut out = Vec::new();
            while !stop.load(Ordering::Relaxed) {
                out.push(time(|| list_query(&reader, FIRST_PAINT_PAGE)).1);
            }
            out
        })
    };

    let quiet_window = Duration::from_millis(1500);
    let stop = Arc::new(AtomicBool::new(false));
    let thread = read(stop.clone());
    std::thread::sleep(quiet_window);
    stop.store(true, Ordering::Relaxed);
    let quiet = thread.join().unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let thread = read(stop.clone());
    // A new session arriving mid-prune, through the hook path on its own
    // handle: does visibility stall behind retention?
    let writer = bench.open();
    let ingest_stop = Arc::new(AtomicBool::new(false));
    let ingest_flag = ingest_stop.clone();
    let (writer_bench_dir, turns) = (bench.project_dir(0), bench.turns);
    fs::create_dir_all(&writer_bench_dir).unwrap();
    let ingester = std::thread::spawn(move || {
        let mut out = Vec::new();
        let mut n = 1u64 << 44;
        while !ingest_flag.load(Ordering::Relaxed) {
            let id = session_id(n);
            n += 1;
            let path = writer_bench_dir.join(format!("{id}.jsonl"));
            write_file(&path, &transcript(&id, "p0", turns, now_ms()));
            out.push(
                time(|| {
                    writer
                        .hydrate(
                            &SessionRef::path(Source::Claude, &path),
                            HydrateOptions::default(),
                        )
                        .expect("hook ingest during prune")
                })
                .1,
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        out
    });
    // Maintenance as a host runs it: one bounded batch per call, yielding
    // between calls, honouring the cooldown and retrying when the store is
    // busy -- for a fixed window, or until the cycle completes. A year's
    // expiry at the default batch size and cooldown takes minutes; the
    // window measures what capture and reads see while it runs.
    let window = Duration::from_millis(config.prune_window_ms);
    let mut maintenance = json!({ "batches": 0, "rows": 0, "sessions": 0, "busy": 0,
        "deferred": 0, "awaitingUploadAck": 0, "cycleComplete": false });
    let (_, prune_elapsed) = time(|| {
        let started = Instant::now();
        let options = RetentionOptions::default();
        while started.elapsed() < window {
            let batch = store.prune_expired(options.clone()).expect("prune_expired");
            let count = |key: &str, by: u64, m: &mut Value| {
                m[key] = json!(m[key].as_u64().unwrap_or(0) + by);
            };
            count("batches", 1, &mut maintenance);
            count("rows", batch.rows, &mut maintenance);
            count("sessions", batch.sessions, &mut maintenance);
            count("busy", u64::from(batch.busy), &mut maintenance);
            count("deferred", u64::from(batch.deferred), &mut maintenance);
            count(
                "awaitingUploadAck",
                u64::from(batch.awaiting_upload_ack),
                &mut maintenance,
            );
            if batch.cycle_complete {
                maintenance["cycleComplete"] = json!(true);
                break;
            }
            // The cooldown is the store's own pacing; a host schedules the
            // next call rather than spinning.
            let pause = if batch.busy {
                10
            } else {
                u64::from(options.min_interval_ms)
            };
            std::thread::sleep(Duration::from_millis(pause));
        }
    });
    stop.store(true, Ordering::Relaxed);
    ingest_stop.store(true, Ordering::Relaxed);
    let during = thread.join().unwrap();
    let ingest_during = ingester.join().unwrap();
    // A window in which maintenance evicted nothing -- busy throughout, or
    // waiting on an acknowledgement -- measured capture beside an idle store,
    // which would read as retention costing nothing. The store holds expired
    // sessions whenever its history is longer than the retention, so that is
    // a failed run, not a fast one.
    if config.days > config.retain_days {
        assert!(
            maintenance["rows"].as_u64().unwrap_or(0) > 0,
            "retention maintenance evicted nothing in its window: {maintenance}"
        );
    }

    json!({
        "store": store_info,
        "retainDays": config.retain_days,
        "maintenance": maintenance,
        "pruneMs": prune_elapsed.as_millis() as u64,
        "metrics": {
            "read_quiet": summary(&quiet),
            "read_during_prune": summary(&during),
            "ingest_during_prune": summary(&ingest_during),
        }
    })
}

/// One Codex rollout: its `session_meta`, then `turns` prompt/reply pairs.
fn codex_rollout(id: &str, parent: Option<&str>, turns: u64, base_ms: i64) -> String {
    let meta = match parent {
        Some(parent) => json!({"id": id, "parent_thread_id": parent, "cwd": "/work/codex",
            "thread_source": "subagent", "source": {"subagent": {"other": "reviewer"}}}),
        None => json!({"id": id, "cwd": "/work/codex"}),
    };
    let mut out = format!(
        "{}\n",
        json!({"timestamp": iso(base_ms), "type": "session_meta", "payload": meta})
    );
    for turn in 0..turns {
        let at = base_ms + 1_000 * (turn as i64 + 1);
        for (offset, payload) in [
            (
                0,
                json!({"type": "user_message", "message": format!("task {turn} in {id}")}),
            ),
            (
                500,
                json!({"type": "agent_message", "message": format!("done {turn}")}),
            ),
        ] {
            out.push_str(&format!(
                "{}\n",
                json!({"timestamp": iso(at + offset), "type": "event_msg", "payload": payload})
            ));
        }
    }
    out
}

/// Codex's own thread index over `threads` (id, rollout locator): what the
/// SQLite-only thread selection pages through and stats on every sweep.
fn codex_thread_index(codex: &Path, threads: &[(String, PathBuf)], updated_ms: i64) {
    let state = rusqlite::Connection::open(codex.join("state_5.sqlite")).expect("state db");
    state
        .execute_batch(
            "CREATE TABLE threads(id TEXT PRIMARY KEY, rollout_path TEXT, created_at_ms INTEGER, \
             updated_at_ms INTEGER, source TEXT, cwd TEXT, git_branch TEXT, git_origin_url TEXT, \
             title TEXT, name TEXT, first_user_message TEXT, originator TEXT, cli_version TEXT, \
             model TEXT, tokens_used INTEGER, archived INTEGER, agent_nickname TEXT, \
             agent_role TEXT, history_mode TEXT, thread_source TEXT); \
             CREATE TABLE thread_spawn_edges(parent_thread_id TEXT,child_thread_id TEXT,status TEXT);",
        )
        .expect("state schema");
    let history =
        rusqlite::Connection::open(codex.join("thread_history_1.sqlite")).expect("history db");
    history
        .execute_batch(
            "CREATE TABLE thread_history_projection_state(thread_id TEXT PRIMARY KEY, \
             next_rollout_ordinal INTEGER,next_rollout_byte_offset INTEGER); \
             CREATE TABLE thread_items(thread_id TEXT,turn_id TEXT,item_id TEXT, \
             rollout_ordinal INTEGER,created_at_ms INTEGER,item_type TEXT,item_json TEXT); \
             CREATE TABLE thread_turns(thread_id TEXT,turn_id TEXT,rollout_ordinal INTEGER, \
             started_at INTEGER,error_json TEXT);",
        )
        .expect("history schema");
    for (id, locator) in threads {
        state
            .execute(
                "INSERT INTO threads (id,rollout_path,created_at_ms,updated_at_ms,source,archived,history_mode) \
                 VALUES (?1,?2,?3,?3,'cli',0,'paginated')",
                rusqlite::params![id, locator.to_string_lossy(), updated_ms],
            )
            .expect("thread row");
        history
            .execute(
                "INSERT INTO thread_history_projection_state VALUES (?1,1,1)",
                [id],
            )
            .expect("projection row");
    }
}

/// A full sync over a Codex store with nothing new: the sweep a watch loop or
/// a desktop collector runs back to back when a file event forces one.
fn codex_idle(config: &Config) -> Value {
    let bench = Bench::new(config.turns);
    let codex = bench.home.join(".codex");
    let day = codex.join("sessions/2026/10/01");
    fs::create_dir_all(&day).unwrap();
    let now = now_ms();
    let (mut compressed, mut subagents) = (0u64, 0u64);
    let mut threads = Vec::new();
    let mut previous: Option<String> = None;
    for n in 0..config.codex_rollouts {
        let id = format!("codex-{n:07}");
        let parent = previous
            .as_deref()
            .filter(|_| config.codex_subagent_every > 0 && n % config.codex_subagent_every == 1);
        subagents += u64::from(parent.is_some());
        let body = codex_rollout(&id, parent, config.turns, now - DAY_MS + n as i64 * 1_000);
        let locator = day.join(format!("rollout-2026-10-01T00-00-00-{id}.jsonl"));
        if config.codex_compressed_every > 0 && n % config.codex_compressed_every == 0 {
            let mut stored = locator.clone().into_os_string();
            stored.push(".zst");
            fs::write(stored, zstd::bulk::compress(body.as_bytes(), 3).unwrap()).unwrap();
            compressed += 1;
        } else {
            fs::write(&locator, body).unwrap();
        }
        threads.push((id.clone(), locator));
        if parent.is_none() {
            previous = Some(id);
        }
    }
    codex_thread_index(&codex, &threads, now);

    let store = bench.open();
    // Two untimed sweeps: the first ingests, the second settles whatever the
    // first left owed (delegation edges, continuity), so the timed sweeps
    // start from a store that holds everything.
    let (_, populate) = time(|| {
        store
            .sync(SyncOptions::default().force(true))
            .expect("populating sync")
    });
    store
        .sync(SyncOptions::default().force(true))
        .expect("settling sync");
    let head = store.head_revision().expect("head").revision;
    let sweep = |force: bool| {
        (0..config.sweep_samples)
            .map(|_| {
                time(|| {
                    store
                        .sync(SyncOptions::default().force(force))
                        .expect("idle sync")
                })
                .1
            })
            .collect::<Vec<_>>()
    };
    let forced = sweep(true);
    let unforced = sweep(false);
    let head_after = store.head_revision().expect("head").revision;
    json!({
        "store": {
            "rollouts": config.codex_rollouts,
            "compressed": compressed,
            "subagents": subagents,
            "populateMs": populate.as_millis() as u64,
            "dbBytes": fs::metadata(&bench.db).map(|m| m.len()).unwrap_or(0),
            "idleSweepsWrote": head_after != head,
        },
        "metrics": {
            "idle_sync_forced": summary(&forced),
            "idle_sync_unforced": summary(&unforced),
        }
    })
}

#[test]
#[ignore = "latency benchmark: run through scripts/benchmark-latency.mjs"]
fn latency_phase() {
    let config = Config::from_env();
    let started = Instant::now();
    let mut report = match config.phase.as_str() {
        "stages" => stages(&config),
        "e2e" => e2e(&config),
        "list_scale" => list_scale(&config),
        "ingest_upload" => ingest_upload(&config),
        "prune" => prune(&config),
        "codex_idle" => codex_idle(&config),
        other => panic!("unknown AI_HIST_LATENCY_PHASE {other}"),
    };
    report["phase"] = json!(config.phase);
    report["samples"] = json!(config.samples);
    report["turns"] = json!(config.turns);
    report["phaseWallMs"] = json!(started.elapsed().as_millis() as u64);
    report["release"] = json!(!cfg!(debug_assertions));
    // How much un-checkpointed WAL the phase left behind: every reader pays
    // to search it, so a list query after a burst of writes is slower than
    // the same query on a checkpointed store.
    if let Some(dir) = std::env::var_os("AI_HIST_LATENCY_DIR") {
        let wal = PathBuf::from(dir).join("work/ai-history.db-wal");
        report["walBytesAtEnd"] = json!(fs::metadata(wal).map(|m| m.len()).unwrap_or(0));
    }
    let line = report.to_string();
    if let Some(path) = std::env::var_os("AI_HIST_LATENCY_REPORT") {
        fs::write(path, &line).expect("write report");
    }
    println!("LATENCY_JSON {line}");
}

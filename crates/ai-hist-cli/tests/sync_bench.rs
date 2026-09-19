//! Full-sync and hydration throughput harness.
//!
//! This is the measurement half of the benchmark described in
//! `docs/benchmarks.md`. `scripts/benchmark-sync.mjs` generates a synthetic
//! store, invokes one phase of this harness per process, and applies the
//! thresholds in `scripts/benchmark-thresholds.json` to what it reports.
//!
//! Run one phase by hand with:
//!
//! ```text
//! AI_HIST_BENCH_PHASE=cold_sync \
//! AI_HIST_BENCH_HOME=/tmp/bench-home \
//! AI_HIST_BENCH_DB=/tmp/bench-home/ai-history.db \
//!   cargo test -p ai-hist-cli --test sync_bench -- --ignored --exact --nocapture \
//!   benchmark_phase
//! ```
//!
//! # Why one phase per process
//!
//! Peak RSS is read from `getrusage(RUSAGE_SELF)`, which is a high-water mark
//! for the whole process and never falls. Running several phases in one process
//! would report the largest phase's footprint for every phase after it, so the
//! driver spawns a process per phase and each one measures only its own work.
//! The same isolation is why this harness may set `HOME` for itself: exactly
//! one test function runs per process, so there is no second thread to race the
//! environment with.
//!
//! # What is measured, and how
//!
//! * **Wall time** — `Instant` around the operation only. Store generation,
//!   catalog opening and row counting happen outside the timed region.
//! * **Records** — the exact row delta across `history`, `session_events`,
//!   `tool_calls`, `file_edits` and `sessions`, read from the database before
//!   and after. Not a parser-reported estimate.
//! * **Bytes read** — the `rchar` delta from `/proc/self/io` on Linux, which
//!   counts every byte the process received from a `read`, page cache included.
//!   macOS has no equivalent that does not require entitlements, so the field
//!   is `null` there and the driver does not gate on it.
//! * **Peak RSS** — `ru_maxrss` from `getrusage`, normalized to bytes
//!   (Linux reports kibibytes, macOS bytes). `null` on Windows.
//!
//!   Linux does **not** reset that high-water mark across `execve`, so a
//!   harness launched through `cargo test` inherits cargo's own footprint and
//!   reports it as the phase's. The driver therefore locates this test
//!   executable once with `cargo test --no-run` and then executes it directly.
//!   A phase run by hand under `cargo test` still produces a valid report, but
//!   its `peakRssBytes` is cargo's, not the phase's, and must not be compared
//!   with a driver-produced baseline.
//! * **DB and WAL growth** — file sizes after the phase.
//!
//! It is `#[ignore]`d for the same reason `discovery_bench.rs` is: it writes
//! and parses tens of megabytes, and its headline numbers are wall clocks.

use ai_hist::{
    hydrate_session_at, open_db_readonly, HydrateSessionOptions, SessionScope, SessionStore,
    StoreOptions, SyncOptions,
};
use serde_json::{json, Value};
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::Instant;

// ---------------------------------------------------------------------------
// process measurement
// ---------------------------------------------------------------------------

/// Peak resident set size of this process, in bytes.
///
/// `ru_maxrss` is kibibytes on Linux and bytes on macOS — the one portability
/// trap in this file, and the reason the conversion is spelled out rather than
/// inherited from a helper crate.
#[cfg(unix)]
fn peak_rss_bytes() -> Option<u64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: `getrusage` writes a fully initialized `rusage` on success and
    // touches nothing else; the pointer is to a live local.
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }
    // SAFETY: `getrusage` returned 0, so the struct is initialized.
    let usage = unsafe { usage.assume_init() };
    let max = u64::try_from(usage.ru_maxrss).ok()?;
    if cfg!(target_os = "macos") {
        Some(max)
    } else {
        Some(max * 1024)
    }
}

#[cfg(not(unix))]
fn peak_rss_bytes() -> Option<u64> {
    None
}

/// `(rchar, syscr)` from `/proc/self/io`: bytes returned by reads, and read
/// syscalls. Linux only; `None` everywhere else.
fn proc_io() -> Option<(u64, u64)> {
    let text = fs::read_to_string("/proc/self/io").ok()?;
    let field = |name: &str| -> Option<u64> {
        text.lines()
            .find_map(|line| line.strip_prefix(name)?.trim().parse::<u64>().ok())
    };
    Some((field("rchar:")?, field("syscr:")?))
}

// ---------------------------------------------------------------------------
// store and catalog helpers
// ---------------------------------------------------------------------------

fn env_path(name: &str) -> PathBuf {
    PathBuf::from(
        std::env::var_os(name).unwrap_or_else(|| panic!("{name} must be set; see the module docs")),
    )
}

/// Total bytes and file count under `root`.
fn store_size(root: &Path) -> (u64, u64) {
    let mut bytes = 0;
    let mut files = 0;
    let Ok(entries) = fs::read_dir(root) else {
        return (0, 0);
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            let (child_bytes, child_files) = store_size(&path);
            bytes += child_bytes;
            files += child_files;
        } else if let Ok(meta) = path.metadata() {
            bytes += meta.len();
            files += 1;
        }
    }
    (bytes, files)
}

const COUNTED_TABLES: [&str; 5] = [
    "history",
    "session_events",
    "tool_calls",
    "file_edits",
    "sessions",
];

/// Rows across every table a local sync writes. Zero for a database that does
/// not exist yet, which is what makes a cold run's delta the whole corpus.
fn row_count(db_path: &Path) -> u64 {
    if !db_path.exists() {
        return 0;
    }
    let Ok(conn) = open_db_readonly(db_path) else {
        return 0;
    };
    let mut total = 0;
    for table in COUNTED_TABLES {
        let count: i64 = conn
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap_or(0);
        total += u64::try_from(count).unwrap_or(0);
    }
    total
}

fn file_len(path: &Path) -> u64 {
    fs::metadata(path).map(|meta| meta.len()).unwrap_or(0)
}

fn wal_path(db_path: &Path) -> PathBuf {
    let mut name = db_path.as_os_str().to_os_string();
    name.push("-wal");
    PathBuf::from(name)
}

/// Point every provider lookup at the synthetic store. Safe here because the
/// driver runs exactly one test function per process (see the module docs).
fn isolate_home(home: &Path) {
    std::env::set_var("HOME", home);
    std::env::set_var("USERPROFILE", home);
    std::env::set_var("XDG_DATA_HOME", home.join("xdg"));
    std::env::set_var(
        "OPENCODE_DB",
        home.join(".local/share/opencode/opencode.db"),
    );
    std::env::remove_var("AI_HIST_DB");
    std::env::remove_var("TRAJECTORY_ROOT");
    std::env::remove_var("RELAYCAST_API_KEY");
    std::env::remove_var("RELAYCAST_WORKSPACE_ID");
}

// `StoreOptions` is `#[non_exhaustive]`, so a struct expression — with or
// without `..Default::default()` — will not compile outside `ai-hist`. Field
// assignment after `default()` is the only way to build one here.
#[allow(clippy::field_reassign_with_default)]
fn sync_once(db_path: &Path, home: &Path) {
    let mut options = StoreOptions::default();
    options.db_path = Some(db_path.to_path_buf());
    options.home = Some(home.to_path_buf());
    let store = SessionStore::open(options).expect("open bench store");
    store.sync(SyncOptions::default()).expect("local sync");
}

fn hydrate_once(db_path: &Path, source: &str, session_id: &str) -> (u64, Option<i64>, Option<i64>) {
    let result = hydrate_session_at(
        db_path,
        &HydrateSessionOptions {
            source: source.to_string(),
            session_id: session_id.to_string(),
            scope: SessionScope::Local,
            include_related: false,
        },
    )
    .expect("hydrate bench session");
    let evidence = result.evidence;
    let diagnostic = result.diagnostics.first();
    (
        evidence.prompts + evidence.events + evidence.tool_calls + evidence.file_edits,
        diagnostic.and_then(|d| d.source_bytes),
        diagnostic.and_then(|d| d.records_parsed),
    )
}

// ---------------------------------------------------------------------------
// phases
// ---------------------------------------------------------------------------

struct Measurement {
    elapsed_ms: f64,
    records: u64,
    source_bytes: u64,
    detail: Value,
}

/// Append one 1 KiB assistant record to `path`, the change an incremental sync
/// is measured against. Written outside the timed region.
fn append_one_record(path: &Path) -> u64 {
    let session_id = path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or("bench")
        .to_string();
    let line = json!({
        "sessionId": session_id,
        "uuid": format!("{session_id}-bench-append"),
        "cwd": "/work/relayhistory-bench",
        "gitBranch": "main",
        "type": "assistant",
        "message": {
            "role": "assistant",
            "model": "claude-opus-4",
            "content": [{ "type": "text", "text": "x" }],
        },
        "timestamp": "2026-09-19T12:00:00.000Z",
    })
    .to_string();
    let padding = 1024usize.saturating_sub(line.len() + 1);
    let record = line.replace("\"x\"", &format!("\"{}\"", "x".repeat(padding.max(1))));
    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(path)
        .expect("open transcript for append");
    let bytes = record.len() as u64 + 1;
    writeln!(file, "{record}").expect("append bench record");
    bytes
}

/// A fixed reference workload, used to divide out how busy the machine is.
///
/// A throughput floor stored on one machine is meaningless on another, and a
/// shared CI runner under load can be several times slower than the same
/// runner idle — exactly the shape of a regression the gate is looking for.
/// So the driver measures this alongside every run and scales the
/// measurements by `stored_calibration / measured_calibration` before
/// comparing them with the baselines.
///
/// It has to be made of the same materials as the work it is calibrating —
/// `serde_json` parsing plus SQLite inserts through the same bundled
/// amalgamation — or it would normalize CPU contention while the real cost
/// was I/O. It deliberately touches neither the synthetic store nor the
/// benchmark database, so a code change to the ingestion path cannot move it:
/// that is what keeps a real 3x slowdown red instead of normalizing itself
/// away.
fn calibration() -> Measurement {
    let document = serde_json::to_string(&json!({
        "sessionId": "calibration",
        "uuid": "calibration-0000",
        "type": "assistant",
        "message": {
            "role": "assistant",
            "model": "claude-opus-4",
            "usage": { "input_tokens": 1200, "output_tokens": 300 },
            "content": [
                { "type": "thinking", "thinking": "x".repeat(220) },
                { "type": "text", "text": "y".repeat(260) },
                { "type": "tool_use", "id": "toolu_0", "name": "Read",
                  "input": { "file_path": "/work/bench/src/lib.rs" } },
            ],
        },
        "timestamp": "2026-09-19T12:00:00.000Z",
    }))
    .expect("calibration document");
    const ROWS: usize = 3_500;
    const COMMIT_EVERY: usize = 400;
    const TREE_FILES: usize = 120;
    const WALKS: usize = 12;
    let dir = tempfile::tempdir().expect("calibration tempdir");

    // A small provider-shaped tree, built once outside the timed region. The
    // first version of this calibration measured only parsing and inserts, and
    // under a neighbour hammering the disk it moved 1.15x while a full sync —
    // which enumerates and stats every provider file — moved 2.9x. Directory
    // metadata contention is real and has to be part of the reference or the
    // normalization silently under-corrects.
    let tree = dir.path().join("tree");
    for index in 0..TREE_FILES {
        let path = tree
            .join(format!("project-{}", index % 8))
            .join(format!("session-{index:04}.jsonl"));
        fs::create_dir_all(path.parent().expect("tree parent")).expect("calibration tree dir");
        fs::write(&path, &document).expect("calibration tree file");
    }

    let mut best = f64::INFINITY;
    // Best of three: contention only ever makes a round slower, so the fastest
    // round is the one that describes the machine rather than its neighbours.
    for round in 0..3 {
        let path = dir.path().join(format!("calibration-{round}.db"));
        let started = Instant::now();

        // Enumerate, stat and read the tree, the way an ingest walk does.
        let mut walked = 0u64;
        for _ in 0..WALKS {
            let mut stack = vec![tree.clone()];
            while let Some(directory) = stack.pop() {
                for entry in fs::read_dir(&directory)
                    .expect("calibration walk")
                    .flatten()
                {
                    let entry_path = entry.path();
                    if entry_path.is_dir() {
                        stack.push(entry_path);
                    } else {
                        let _ = entry_path.metadata().expect("calibration stat").len();
                        let _ = fs::read_to_string(&entry_path).expect("calibration read");
                        walked += 1;
                    }
                }
            }
        }

        // WAL, a full-text index and a truncating checkpoint, because that is
        // what the ledger being calibrated against is made of. Without the FTS
        // writes the reference under-counts exactly the page churn a cold sync
        // is dominated by.
        let conn = rusqlite::Connection::open(&path).expect("calibration db");
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE rows (id INTEGER PRIMARY KEY, kind TEXT, body TEXT);
             CREATE VIRTUAL TABLE rows_fts USING fts5(body);
             BEGIN",
        )
        .expect("calibration schema");
        for index in 0..ROWS {
            let value: Value = serde_json::from_str(&document).expect("calibration parse");
            let kind = value["message"]["content"][index % 3]["type"]
                .as_str()
                .unwrap_or("")
                .to_string();
            let body = value["message"]["content"][index % 3].to_string();
            conn.execute(
                "INSERT INTO rows (kind, body) VALUES (?, ?)",
                rusqlite::params![kind, body],
            )
            .expect("calibration insert");
            conn.execute("INSERT INTO rows_fts (body) VALUES (?)", [&body])
                .expect("calibration fts insert");
            // Commit periodically so the reference pays for durable writes too,
            // not just one fsync at the end.
            if index % COMMIT_EVERY == COMMIT_EVERY - 1 {
                conn.execute_batch("COMMIT; BEGIN")
                    .expect("calibration commit");
            }
        }
        conn.execute_batch("COMMIT").expect("calibration commit");
        conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
            row.get::<_, i64>(0)
        })
        .expect("calibration checkpoint");
        drop(conn);
        best = best.min(started.elapsed().as_secs_f64() * 1000.0);
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(format!("{}-wal", path.display()));
        let _ = fs::remove_file(format!("{}-shm", path.display()));
        assert_eq!(walked, (TREE_FILES * WALKS) as u64);
    }
    Measurement {
        elapsed_ms: best,
        records: ROWS as u64,
        source_bytes: (document.len() * ROWS) as u64,
        detail: json!({
            "rounds": 3,
            "rowsPerRound": ROWS,
            "commitEvery": COMMIT_EVERY,
            "treeFiles": TREE_FILES,
            "walksPerRound": WALKS,
        }),
    }
}

fn run_phase(phase: &str, home: &Path, db_path: &Path) -> Measurement {
    match phase {
        "calibration" => calibration(),
        "cold_sync" => {
            assert!(
                !db_path.exists(),
                "cold_sync needs a database that does not exist yet: {}",
                db_path.display()
            );
            let (store_bytes, store_files) = store_size(home);
            let before = row_count(db_path);
            let started = Instant::now();
            sync_once(db_path, home);
            let elapsed = started.elapsed();
            Measurement {
                elapsed_ms: elapsed.as_secs_f64() * 1000.0,
                records: row_count(db_path).saturating_sub(before),
                source_bytes: store_bytes,
                detail: json!({ "storeFiles": store_files }),
            }
        }
        "incremental_sync" => {
            let target = env_path("AI_HIST_BENCH_APPEND");
            assert!(
                db_path.exists(),
                "incremental_sync must run after cold_sync against the same database"
            );
            let appended = append_one_record(&target);
            let before = row_count(db_path);
            let started = Instant::now();
            sync_once(db_path, home);
            let elapsed = started.elapsed();
            Measurement {
                elapsed_ms: elapsed.as_secs_f64() * 1000.0,
                records: row_count(db_path).saturating_sub(before),
                source_bytes: appended,
                detail: json!({ "appendedBytes": appended, "appendedTo": target.display().to_string() }),
            }
        }
        "unchanged_sync" => {
            assert!(
                db_path.exists(),
                "unchanged_sync must run after cold_sync against the same database"
            );
            let before = row_count(db_path);
            let started = Instant::now();
            sync_once(db_path, home);
            let elapsed = started.elapsed();
            let after = row_count(db_path);
            Measurement {
                elapsed_ms: elapsed.as_secs_f64() * 1000.0,
                records: after.saturating_sub(before),
                source_bytes: 0,
                detail: json!({ "rowsBefore": before, "rowsAfter": after }),
            }
        }
        "hydrate_cold" | "hydrate_unchanged" => {
            let target = std::env::var("AI_HIST_BENCH_SESSION")
                .expect("AI_HIST_BENCH_SESSION must be set to source:session_id");
            let (source, session_id) = target
                .split_once(':')
                .expect("AI_HIST_BENCH_SESSION must look like claude:<session-id>");
            if phase == "hydrate_unchanged" {
                // Warm the checkpoint outside the timed region so the measured
                // call is the unchanged path, not a first hydration.
                hydrate_once(db_path, source, session_id);
            }
            let before = row_count(db_path);
            let started = Instant::now();
            let (evidence, source_bytes, records_parsed) =
                hydrate_once(db_path, source, session_id);
            let elapsed = started.elapsed();
            let written = row_count(db_path).saturating_sub(before);
            // A cold hydration is measured by what it parsed; an unchanged one
            // parses nothing, so its record count is the evidence it served.
            let records = match records_parsed {
                Some(parsed) if parsed > 0 => u64::try_from(parsed).unwrap_or(0),
                _ => evidence,
            };
            Measurement {
                elapsed_ms: elapsed.as_secs_f64() * 1000.0,
                records,
                source_bytes: source_bytes
                    .and_then(|b| u64::try_from(b).ok())
                    .unwrap_or(0),
                detail: json!({
                    "evidenceRows": evidence,
                    "rowsWritten": written,
                    "recordsParsed": records_parsed,
                    "target": target,
                }),
            }
        }
        other => panic!("unknown AI_HIST_BENCH_PHASE {other}"),
    }
}

#[test]
#[ignore = "benchmark: parses a synthetic multi-megabyte store; driven by scripts/benchmark-sync.mjs"]
fn benchmark_phase() {
    let phase = std::env::var("AI_HIST_BENCH_PHASE")
        .expect("AI_HIST_BENCH_PHASE must be set; see the module docs");
    let home = env_path("AI_HIST_BENCH_HOME");
    let db_path = env_path("AI_HIST_BENCH_DB");
    isolate_home(&home);

    let io_before = proc_io();
    let measurement = run_phase(&phase, &home, &db_path);
    let io_after = proc_io();

    let (bytes_read, read_syscalls) = match (io_before, io_after) {
        (Some((rchar_before, syscr_before)), Some((rchar_after, syscr_after))) => (
            Some(rchar_after.saturating_sub(rchar_before)),
            Some(syscr_after.saturating_sub(syscr_before)),
        ),
        _ => (None, None),
    };
    let seconds = measurement.elapsed_ms / 1000.0;
    let report = json!({
        "phase": phase,
        "elapsedMs": measurement.elapsed_ms,
        "records": measurement.records,
        "recordsPerSecond": if seconds > 0.0 { measurement.records as f64 / seconds } else { 0.0 },
        "storeBytes": measurement.source_bytes,
        "megabytesPerSecond": if seconds > 0.0 {
            measurement.source_bytes as f64 / 1_000_000.0 / seconds
        } else { 0.0 },
        "bytesRead": bytes_read,
        "readSyscalls": read_syscalls,
        "peakRssBytes": peak_rss_bytes(),
        "dbBytes": file_len(&db_path),
        "walBytes": file_len(&wal_path(&db_path)),
        "detail": measurement.detail,
        "os": std::env::consts::OS,
        "arch": std::env::consts::ARCH,
        "profile": if cfg!(debug_assertions) { "debug" } else { "release" },
    });
    let line = serde_json::to_string(&report).expect("serialize bench report");
    if let Some(path) = std::env::var_os("AI_HIST_BENCH_REPORT") {
        fs::write(&path, format!("{line}\n")).expect("write bench report");
    }
    // The driver scrapes this prefix out of cargo's output when no report path
    // was given; `--nocapture` is required for it to appear.
    println!("BENCH_JSON {line}");
}

//! Performance validation for the shallow session catalog.
//!
//! This is a measurement harness, not a pass/fail timing gate. It builds a
//! synthetic multi-provider archive in a temp directory (the isolated-`HOME`
//! pattern `session_discovery.rs` established), exercises the two catalog
//! operations against it, and prints a table of what each one actually cost.
//!
//! Run it with:
//!
//! ```text
//! cargo test -p ai-hist-cli --test discovery_bench -- --ignored --nocapture
//! ```
//!
//! It is `#[ignore]`d because it writes tens of megabytes of fixtures and runs
//! a full `ai-hist sync` for comparison — too slow for the default suite, and
//! its headline numbers are wall clocks that vary by machine.
//!
//! # What is asserted, and what is only printed
//!
//! Every assertion here is an **operation count** — bytes read, files opened,
//! shallow reads, rows returned — because those are properties of the
//! algorithm and hold on any machine. Wall clocks are printed for the human
//! reading the table and are never asserted against a threshold; a loaded CI
//! box would fail such a check without anything having regressed.
//!
//! The four claims this harness validates:
//!
//! 1. **Cached listing scales with the rows you ask for, not with the archive.**
//!    Timings across catalog sizes and event volumes, at three limits. The
//!    structural half of this claim — the query is served by
//!    `idx_sessions_recency` / `idx_sessions_source_recency` and never sorts the
//!    table — is asserted with `EXPLAIN QUERY PLAN` by the unit test
//!    `discover::tests::the_catalog_listing_is_served_by_an_index_not_a_table_scan`.
//! 2. **A cached listing reads zero provider files.** Structurally true
//!    (`list_session_catalog` takes only a `Connection`), and demonstrated here
//!    by deleting the entire archive and listing it anyway. The unit tests
//!    `the_cache_only_listing_survives_the_provider_files_disappearing` and
//!    `the_catalog_query_reads_only_the_sessions_table` pin the same property.
//! 3. **Shallow discovery does far less work than full indexing.** Same
//!    archive, `sessions discover` versus `sync`, compared on wall time, bytes
//!    read, and rows written.
//! 4. **A bounded request does not parse the archive.** `--limit 5` reads the
//!    same bytes from a 5x larger archive.

use ai_hist::discover::{EXCERPT_TRIM_WHITESPACE, HEAD_SCAN_MAX_BYTES, TAIL_SCAN_MAX_BYTES};
use ai_hist::open_db;
use ai_hist::{
    discover_sessions_with_env, list_session_catalog, CatalogListOptions, DiscoverOptions,
    DiscoveryEnv, DiscoverySummary,
};
use rusqlite::Connection;
use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// archive fixtures
// ---------------------------------------------------------------------------

/// Roughly how large the "ordinary" synthetic transcripts are.
const SMALL_TRANSCRIPT_BYTES: usize = 24 * 1024;
/// Roughly how large the "long-running session" transcripts are — comfortably
/// past the head budget, so bounded reads have something to be bounded against.
const LONG_TRANSCRIPT_BYTES: usize = 900 * 1024;

fn write(path: &Path, contents: &str, mtime_ms: u64) {
    fs::create_dir_all(path.parent().expect("fixture parent")).expect("mkdir");
    fs::write(path, contents).expect("write fixture");
    // Recency ordering is mtime-driven, so pin it instead of letting the order
    // the fixtures happened to be written decide which sessions win.
    let file = fs::OpenOptions::new()
        .write(true)
        .open(path)
        .expect("open for mtime");
    let when = std::time::UNIX_EPOCH + Duration::from_millis(mtime_ms);
    file.set_times(fs::FileTimes::new().set_modified(when))
        .expect("set mtime");
}

fn claude_transcript(id: &str, target_bytes: usize) -> String {
    let mut body = String::with_capacity(target_bytes + 512);
    body.push_str(&format!(
        r#"{{"sessionId":"{id}","cwd":"/work/{id}","gitBranch":"main","version":"1.2.3","type":"user","message":{{"role":"user","content":"first human prompt for {id}"}},"timestamp":"2026-06-20T10:00:00.000Z"}}"#
    ));
    body.push('\n');
    let mut turn = 0usize;
    while body.len() < target_bytes {
        body.push_str(&format!(
            r#"{{"sessionId":"{id}","type":"assistant","message":{{"role":"assistant","model":"claude-opus-4","content":[{{"type":"text","text":"turn {turn} PADDINGPADDINGPADDINGPADDINGPADDINGPADDINGPADDINGPADDINGPADDINGPADDING"}}]}},"timestamp":"2026-06-20T11:00:00.000Z"}}"#
        ));
        body.push('\n');
        turn += 1;
    }
    body.push_str(&format!(
        r#"{{"sessionId":"{id}","type":"assistant","message":{{"role":"assistant","content":[{{"type":"text","text":"final"}}]}},"timestamp":"2026-06-20T23:00:00.000Z"}}"#
    ));
    body.push('\n');
    body
}

fn codex_rollout(id: &str, target_bytes: usize) -> String {
    let mut body = String::with_capacity(target_bytes + 512);
    body.push_str(&format!(
        r#"{{"timestamp":"2026-06-21T11:00:00.000Z","type":"session_meta","payload":{{"id":"{id}","cwd":"/work/{id}","originator":"codex_cli_rs","cli_version":"0.148.0","workspace_roots":["/work/{id}"],"git":{{"branch":"dev","repository_url":"git@github.com:acme/{id}.git","commit_hash":"abc1234"}}}}}}"#
    ));
    body.push('\n');
    body.push_str(
        r#"{"timestamp":"2026-06-21T11:00:01.000Z","type":"turn_context","payload":{"model":"gpt-5-codex"}}"#,
    );
    body.push('\n');
    body.push_str(&format!(
        r#"{{"timestamp":"2026-06-21T11:00:03.000Z","type":"event_msg","payload":{{"type":"user_message","message":"first human prompt for {id}"}}}}"#
    ));
    body.push('\n');
    let mut turn = 0usize;
    while body.len() < target_bytes {
        body.push_str(&format!(
            r#"{{"timestamp":"2026-06-21T12:00:00.000Z","type":"event_msg","payload":{{"type":"agent_message","message":"turn {turn} PADDINGPADDINGPADDINGPADDINGPADDINGPADDINGPADDINGPADDINGPADDING"}}}}"#
        ));
        body.push('\n');
        turn += 1;
    }
    body
}

fn cursor_transcript(id: &str, target_bytes: usize) -> String {
    let mut body = format!(
        "{{\"role\":\"user\",\"message\":{{\"content\":[{{\"type\":\"text\",\"text\":\"<user_query>\\nfirst human prompt for {id}\\n</user_query>\"}}]}}}}\n"
    );
    while body.len() < target_bytes {
        body.push_str(
            r#"{"role":"assistant","message":{"content":[{"type":"text","text":"PADDINGPADDINGPADDINGPADDINGPADDINGPADDINGPADDING"}]}}"#,
        );
        body.push('\n');
    }
    body
}

fn grok_chat(id: &str, target_bytes: usize) -> String {
    let mut body = format!(
        r#"{{"type":"user","content":[{{"type":"text","text":"first human prompt for {id}"}}]}}"#
    );
    body.push('\n');
    while body.len() < target_bytes {
        body.push_str(
            r#"{"type":"assistant","content":[{"type":"text","text":"PADDINGPADDINGPADDINGPADDINGPADDINGPADDING"}]}"#,
        );
        body.push('\n');
    }
    body
}

/// One session's worth of files, written under the fake home.
struct ArchiveBuilder {
    home: PathBuf,
}

impl ArchiveBuilder {
    fn claude(&self, index: usize, bytes: usize, mtime_ms: u64) {
        let id = format!("claude-{index:04}");
        write(
            &self.home.join(format!(".claude/projects/app/{id}.jsonl")),
            &claude_transcript(&id, bytes),
            mtime_ms,
        );
    }

    fn codex(&self, index: usize, bytes: usize, mtime_ms: u64) {
        let id = format!("codex-{index:04}");
        write(
            &self
                .home
                .join(format!(".codex/sessions/2026/06/21/rollout-{id}.jsonl")),
            &codex_rollout(&id, bytes),
            mtime_ms,
        );
    }

    fn cursor(&self, index: usize, bytes: usize, mtime_ms: u64) {
        let id = format!("cursor-{index:04}");
        write(
            &self.home.join(format!(
                ".cursor/projects/work-app/agent-transcripts/{id}/{id}.jsonl"
            )),
            &cursor_transcript(&id, bytes),
            mtime_ms,
        );
    }

    fn grok(&self, index: usize, bytes: usize, mtime_ms: u64) {
        let id = format!("grok-{index:04}");
        let dir = self
            .home
            .join(format!(".grok/sessions/%2Fwork%2Fgrok/{id}"));
        write(
            &dir.join("summary.json"),
            &format!(
                r#"{{"info":{{"id":"{id}","cwd":"/work/grok"}},"created_at":"2026-06-20T09:00:00.000Z","updated_at":"2026-06-20T09:30:00.000Z","head_branch":"trunk"}}"#
            ),
            mtime_ms,
        );
        write(
            &dir.join("chat_history.jsonl"),
            &grok_chat(&id, bytes),
            mtime_ms,
        );
    }

    fn opencode(&self, sessions: usize) {
        let db = Connection::open(self.home.join("opencode.db")).expect("opencode db");
        db.execute_batch(
            "CREATE TABLE session (id TEXT PRIMARY KEY, directory TEXT, time_created INTEGER, time_updated INTEGER);
             CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT, time_created INTEGER, data TEXT);
             CREATE TABLE part (id TEXT PRIMARY KEY, message_id TEXT, session_id TEXT, time_created INTEGER, data TEXT);
             CREATE INDEX session_time_updated_id_idx ON session(time_updated DESC, id);
             CREATE INDEX message_session_time_created_id_idx ON message(session_id, time_created, id);
             CREATE INDEX part_session_idx ON part(session_id);
             CREATE INDEX part_message_id_id_idx ON part(message_id, id);",
        )
        .expect("opencode schema");
        for index in 0..sessions {
            let id = format!("oc-{index:04}");
            let created = 1_600_000_000_000i64 + index as i64;
            db.execute(
                "INSERT INTO session VALUES (?, ?, ?, ?)",
                rusqlite::params![id, "/work/oc", created, created + 1_000],
            )
            .expect("opencode session");
            db.execute(
                "INSERT INTO message VALUES (?, ?, ?, ?)",
                rusqlite::params![
                    format!("m-{index}"),
                    id,
                    created,
                    r#"{"role":"user","modelID":"claude-sonnet"}"#
                ],
            )
            .expect("opencode message");
            db.execute(
                "INSERT INTO part VALUES (?, ?, ?, ?, ?)",
                rusqlite::params![
                    format!("p-{index}"),
                    format!("m-{index}"),
                    id,
                    created,
                    format!(r#"{{"type":"text","text":"first human prompt for {id}"}}"#)
                ],
            )
            .expect("opencode part");
        }
    }
}

/// Total bytes of provider data under a fake home.
fn archive_bytes(root: &Path) -> u64 {
    let mut total = 0;
    let Ok(entries) = fs::read_dir(root) else {
        return 0;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            total += archive_bytes(&path);
        } else if let Ok(meta) = path.metadata() {
            total += meta.len();
        }
    }
    total
}

// ---------------------------------------------------------------------------
// measurement helpers
// ---------------------------------------------------------------------------

fn median(mut samples: Vec<Duration>) -> Duration {
    samples.sort();
    samples[samples.len() / 2]
}

fn time_repeated<T>(runs: usize, mut body: impl FnMut() -> T) -> (Duration, T) {
    let mut last = body(); // warm the page cache / prepared-statement path
    let mut samples = Vec::with_capacity(runs);
    for _ in 0..runs {
        let start = Instant::now();
        last = body();
        samples.push(start.elapsed());
    }
    (median(samples), last)
}

fn micros(duration: Duration) -> String {
    format!("{:.3} ms", duration.as_secs_f64() * 1_000.0)
}

fn bytes(value: u64) -> String {
    const UNITS: [&str; 4] = ["B", "KB", "MB", "GB"];
    let mut scaled = value as f64;
    let mut unit = 0;
    while scaled >= 1024.0 && unit + 1 < UNITS.len() {
        scaled /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{value} B")
    } else {
        format!("{scaled:.1} {}", UNITS[unit])
    }
}

fn table(title: &str, headers: &[&str], rows: &[Vec<String>]) {
    let mut widths: Vec<usize> = headers.iter().map(|header| header.len()).collect();
    for row in rows {
        for (index, cell) in row.iter().enumerate() {
            widths[index] = widths[index].max(cell.len());
        }
    }
    let line = |cells: &[String]| {
        let rendered: Vec<String> = cells
            .iter()
            .enumerate()
            .map(|(index, cell)| format!("{cell:<width$}", width = widths[index]))
            .collect();
        println!("  {}", rendered.join("  "));
    };
    println!("\n{title}");
    line(&headers.iter().map(|h| h.to_string()).collect::<Vec<_>>());
    line(
        &widths
            .iter()
            .map(|width| "-".repeat(*width))
            .collect::<Vec<_>>(),
    );
    for row in rows {
        line(row);
    }
}

fn cli(home: &Path, db_path: &Path, args: &[&str]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ai-hist"));
    command
        .arg("--db")
        .arg(db_path)
        .args(args)
        .env("HOME", home)
        .env("USERPROFILE", home)
        .env("XDG_DATA_HOME", home.join("xdg"))
        .env("OPENCODE_DB", home.join("opencode.db"))
        .env_remove("AI_HIST_DB")
        .env_remove("TRAJECTORY_ROOT")
        .env_remove("RELAYCAST_API_KEY")
        .env_remove("RELAYCAST_WORKSPACE_ID");
    command
}

/// Run one CLI invocation, returning its wall time and stdout.
fn run_cli(home: &Path, db_path: &Path, args: &[&str]) -> (Duration, String) {
    let start = Instant::now();
    let output = cli(home, db_path, args).output().expect("spawn ai-hist");
    let elapsed = start.elapsed();
    assert!(
        output.status.success(),
        "`ai-hist {}` failed: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stderr)
    );
    (
        elapsed,
        String::from_utf8_lossy(&output.stdout).into_owned(),
    )
}

/// The trailing `{"type":"summary",…}` line of a `sessions discover --json` run.
fn discover_summary(stdout: &str) -> Value {
    let last = stdout
        .lines()
        .rfind(|line| !line.trim().is_empty())
        .expect("a summary line");
    let value: Value = serde_json::from_str(last).expect("summary is JSON");
    assert_eq!(value["type"], "summary");
    value
}

fn counter(summary: &Value, name: &str) -> u64 {
    summary["counters"][name].as_u64().unwrap_or_else(|| {
        panic!("counter {name} missing from {summary}");
    })
}

// ---------------------------------------------------------------------------
// the benchmark
// ---------------------------------------------------------------------------

#[test]
#[ignore = "benchmark: writes a multi-megabyte archive and runs a full sync"]
fn session_catalog_performance_report() {
    println!("\n=== RelayHistory session catalog — performance validation ===");
    measure_cached_listing_scaling();
    let home = tempfile::tempdir().expect("fake home");
    let counts = build_archive(home.path());
    measure_bounded_request(home.path(), &counts);
    measure_shallow_versus_full(home.path());
    measure_cache_only_listing(home.path());
    println!(
        "\nAll assertions above are operation counts. Timings are reported, never asserted.\n"
    );
}

fn create_opencode_scaling_fixture(path: &Path, unrelated_sessions: usize) {
    let db = Connection::open(path).expect("scaling opencode db");
    db.pragma_update(None, "journal_mode", "WAL").unwrap();
    db.execute_batch(
        "CREATE TABLE session (id TEXT PRIMARY KEY, directory TEXT, time_created INTEGER, time_updated INTEGER);
         CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT, time_created INTEGER, data TEXT);
         CREATE TABLE part (id TEXT PRIMARY KEY, message_id TEXT, session_id TEXT, time_created INTEGER, data TEXT);
         CREATE INDEX session_time_updated_id_idx ON session(time_updated DESC, id);
         CREATE INDEX message_session_time_created_id_idx ON message(session_id, time_created, id);
         CREATE INDEX part_session_idx ON part(session_id);
         CREATE INDEX part_message_id_id_idx ON part(message_id, id);
         BEGIN",
    )
    .unwrap();
    let padding = "x".repeat(384);
    for index in 0..unrelated_sessions {
        let id = format!("ses_old_{index:06}");
        let message = format!("msg_old_{index:06}");
        db.execute(
            "INSERT INTO session VALUES (?, '/work/old', ?, ?)",
            rusqlite::params![id, index as i64, index as i64],
        )
        .unwrap();
        db.execute(
            "INSERT INTO message VALUES (?, ?, ?, ?)",
            rusqlite::params![
                message,
                id,
                index as i64,
                r#"{"role":"user","modelID":"old-model"}"#
            ],
        )
        .unwrap();
        for part in 0..8 {
            db.execute(
                "INSERT INTO part VALUES (?, ?, ?, ?, ?)",
                rusqlite::params![
                    format!("prt_old_{index:06}_{part}"),
                    message,
                    id,
                    index as i64,
                    format!(r#"{{"type":"tool","padding":"{padding}"}}"#)
                ],
            )
            .unwrap();
        }
    }
    for index in 0..20 {
        let id = format!("ses_newest_{index:02}");
        let message = format!("msg_newest_{index:02}");
        let timestamp = 2_000_000_000_000_i64 + index as i64;
        db.execute(
            "INSERT INTO session VALUES (?, '/work/newest', ?, ?)",
            rusqlite::params![id, timestamp, timestamp],
        )
        .unwrap();
        db.execute(
            "INSERT INTO message VALUES (?, ?, ?, '{\"role\":\"user\",\"modelID\":\"new-model\"}')",
            rusqlite::params![message, id, timestamp],
        )
        .unwrap();
        db.execute(
            "INSERT INTO part VALUES (?, ?, ?, ?, '{\"type\":\"text\",\"text\":\"same newest prompt\"}')",
            rusqlite::params![format!("prt_newest_{index:02}"), message, id, timestamp],
        )
        .unwrap();
    }
    db.execute_batch("COMMIT").unwrap();
}

fn opencode_plan<P: rusqlite::Params>(conn: &Connection, sql: &str, params: P) -> String {
    conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
        .unwrap()
        .query_map(params, |row| row.get::<_, String>(3))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap()
        .join(" | ")
}

/// Fixed-limit OpenCode discovery against 1,000 and 10,000-session stores.
/// Both stores contain the same newest 20 sessions; only unrelated message and
/// part history grows. WAL commits run concurrently with each measurement.
#[test]
#[ignore = "benchmark: creates a 10,000-session OpenCode store with large unrelated histories"]
fn opencode_fixed_limit_scaling_report() {
    let root = tempfile::tempdir().unwrap();
    let mut rows = Vec::new();
    for unrelated in [1_000_usize, 10_000] {
        let provider_path = root.path().join(format!("opencode-{unrelated}.db"));
        create_opencode_scaling_fixture(&provider_path, unrelated);
        let provider_bytes = fs::metadata(&provider_path).unwrap().len();

        let planning = Connection::open(&provider_path).unwrap();
        let candidate_plan = opencode_plan(
            &planning,
            "SELECT id, directory, time_created, time_updated FROM session
             WHERE id IS NOT NULL AND id <> ''
             ORDER BY time_updated DESC, id ASC LIMIT ?",
            rusqlite::params![20_i64],
        );
        let prompt_plan = opencode_plan(
            &planning,
            "SELECT substr(json_extract(p.data, '$.text'), 1, ?)
             FROM part p JOIN message m ON m.id = p.message_id
             WHERE p.session_id = ? AND json_valid(m.data) AND json_valid(p.data)
             AND json_extract(m.data, '$.role') = 'user'
             AND json_extract(p.data, '$.type') = 'text'
             AND json_type(p.data, '$.text') = 'text'
             AND trim(substr(json_extract(p.data, '$.text'), 1, ?), ?) <> ''
             ORDER BY COALESCE(p.time_created, m.time_created) ASC LIMIT 1",
            rusqlite::params![4096_i64, "ses_newest_19", 4096_i64, EXCERPT_TRIM_WHITESPACE],
        );
        let model_plan = opencode_plan(
            &planning,
            "SELECT COALESCE(json_extract(data, '$.modelID'),
                             json_extract(data, '$.model.modelID'))
             FROM message WHERE session_id = ? AND json_valid(data)
             AND COALESCE(json_extract(data, '$.modelID'),
                          json_extract(data, '$.model.modelID')) IS NOT NULL LIMIT 1",
            rusqlite::params!["ses_newest_19"],
        );
        assert!(
            candidate_plan.contains("session_time_updated_id_idx"),
            "{candidate_plan}"
        );
        assert!(
            prompt_plan.contains("SEARCH p USING INDEX part_session_idx"),
            "{prompt_plan}"
        );
        assert!(!prompt_plan.contains("SCAN p"), "{prompt_plan}");
        assert!(!prompt_plan.contains("SCAN m"), "{prompt_plan}");
        assert!(
            model_plan.contains("SEARCH message USING INDEX message_session_time_created_id_idx"),
            "{model_plan}"
        );
        assert!(!model_plan.contains("SCAN message"), "{model_plan}");
        drop(planning);

        let running = Arc::new(AtomicBool::new(true));
        let writer_running = Arc::clone(&running);
        let writer_path = provider_path.clone();
        let writer = std::thread::spawn(move || {
            let writer = Connection::open(writer_path).unwrap();
            writer.busy_timeout(Duration::from_secs(1)).unwrap();
            while writer_running.load(Ordering::Relaxed) {
                writer
                    .execute(
                        "UPDATE session SET directory = CASE directory WHEN '/work/old' THEN '/work/old.' ELSE '/work/old' END WHERE id = 'ses_old_000000'",
                        [],
                    )
                    .unwrap();
            }
        });

        let mut samples = Vec::new();
        let mut last_summary = None;
        for _ in 0..5 {
            let ledger = tempfile::tempdir().unwrap();
            let conn = open_db(&ledger.path().join("catalog.db")).unwrap();
            let env =
                DiscoveryEnv::with_roots(&conn, root.path().to_path_buf(), provider_path.clone());
            let options = DiscoverOptions {
                sources: vec!["opencode".into()],
                limit: Some(20),
                ..Default::default()
            };
            let started = Instant::now();
            let summary = discover_sessions_with_env(&env, &options, |_| {}).unwrap();
            samples.push(started.elapsed());
            last_summary = Some(summary);
        }
        running.store(false, Ordering::Relaxed);
        writer.join().unwrap();

        let summary = last_summary.unwrap();
        assert_eq!(summary.discovered, 20);
        assert_eq!(summary.counters.candidates_enumerated, 20);
        assert_eq!(summary.counters.shallow_reads, 20);
        assert_eq!(summary.counters.provider_queries, 41);
        assert_eq!(summary.counters.records_inspected, 60);
        assert_eq!(summary.counters.bytes_read, 0);
        rows.push(vec![
            unrelated.to_string(),
            bytes(provider_bytes),
            summary.counters.candidates_enumerated.to_string(),
            summary.counters.provider_queries.to_string(),
            summary.counters.records_inspected.to_string(),
            micros(median(samples)),
        ]);
        println!("  {unrelated} session candidate plan: {candidate_plan}");
        println!("  {unrelated} session prompt plan: {prompt_plan}");
    }
    table(
        "OpenCode fixed-limit cold discovery with concurrent WAL writes",
        &[
            "unrelated sessions",
            "database",
            "candidates",
            "queries",
            "records",
            "median",
        ],
        &rows,
    );
}

/// The archive as first built, before `grow_archive` adds older sessions.
struct ArchiveCounts {
    sessions: usize,
    bytes: u64,
}

fn build_archive(home: &Path) -> ArchiveCounts {
    let builder = ArchiveBuilder {
        home: home.to_path_buf(),
    };
    // The five newest sessions are long-running transcripts, so a bounded read
    // has something to be bounded against. They keep the newest mtimes in both
    // the small and the grown archive, so a `--limit 5` request selects exactly
    // these five either way.
    for index in 0..3 {
        builder.claude(
            9_000 + index,
            LONG_TRANSCRIPT_BYTES,
            1_800_000_000_000 + index as u64,
        );
    }
    for index in 0..2 {
        builder.codex(
            9_000 + index,
            LONG_TRANSCRIPT_BYTES,
            1_800_000_010_000 + index as u64,
        );
    }
    // An ordinary spread across every file-backed provider.
    for index in 0..40 {
        builder.claude(
            index,
            SMALL_TRANSCRIPT_BYTES,
            1_700_000_000_000 + index as u64,
        );
    }
    for index in 0..20 {
        builder.codex(
            index,
            SMALL_TRANSCRIPT_BYTES,
            1_700_000_100_000 + index as u64,
        );
    }
    for index in 0..10 {
        builder.cursor(
            index,
            SMALL_TRANSCRIPT_BYTES,
            1_700_000_200_000 + index as u64,
        );
    }
    for index in 0..5 {
        builder.grok(
            index,
            SMALL_TRANSCRIPT_BYTES,
            1_700_000_300_000 + index as u64,
        );
    }
    builder.opencode(10);
    ArchiveCounts {
        sessions: 3 + 2 + 40 + 20 + 10 + 5 + 10,
        bytes: archive_bytes(home),
    }
}

/// Add older sessions to an existing archive. They are older than everything
/// already there, so the newest-N winners do not change.
fn grow_archive(home: &Path, added: usize) -> u64 {
    let builder = ArchiveBuilder {
        home: home.to_path_buf(),
    };
    for index in 0..added {
        builder.claude(
            1_000 + index,
            SMALL_TRANSCRIPT_BYTES,
            1_600_000_000_000 + index as u64,
        );
    }
    archive_bytes(home)
}

// --- 1. cached listing -----------------------------------------------------

/// The cache-only listing's cost tracks the rows requested, not the size of
/// the catalog and not the volume of transcript/event data beside it.
fn measure_cached_listing_scaling() {
    let temp = tempfile::tempdir().expect("catalog dir");
    let db_path = temp.path().join("catalog.db");
    let conn = open_db(&db_path).expect("catalog db");

    let mut rows = Vec::new();
    let mut sessions_so_far = 0usize;
    let mut events_so_far = 0usize;
    for (target_sessions, target_events) in [(1_000usize, 5_000usize), (20_000, 100_000)] {
        seed_catalog(&conn, sessions_so_far, target_sessions);
        seed_event_volume(&conn, events_so_far, target_events);
        sessions_so_far = target_sessions;
        events_so_far = target_events;

        for limit in [20i64, 200, 2_000] {
            let options = CatalogListOptions {
                limit: Some(limit),
                ..Default::default()
            };
            let (elapsed, listed) = time_repeated(15, || {
                list_session_catalog(&conn, &options).expect("catalog listing")
            });
            let expected = (limit as usize).min(target_sessions);
            assert_eq!(
                listed.len(),
                expected,
                "the listing must return exactly the rows requested"
            );
            assert!(
                listed.iter().all(|row| row.from_cache),
                "every cache-only row is served from the catalog"
            );
            rows.push(vec![
                target_sessions.to_string(),
                target_events.to_string(),
                limit.to_string(),
                listed.len().to_string(),
                micros(elapsed),
            ]);
        }
    }

    table(
        "1. Cached listing — cost tracks the rows requested, not the archive",
        &[
            "catalog rows",
            "history+event rows",
            "limit",
            "returned",
            "median",
        ],
        &rows,
    );
    println!(
        "  index use is asserted structurally by \
         `discover::tests::the_catalog_listing_is_served_by_an_index_not_a_table_scan`"
    );
}

fn seed_catalog(conn: &Connection, from: usize, to: usize) {
    conn.execute_batch("BEGIN").expect("begin");
    {
        let mut stmt = conn
            .prepare(
                "INSERT INTO sessions \
                 (session_id, source, cwd, first_activity_ms, last_activity_ms, first_prompt, \
                  discovery_state) \
                 VALUES (?, 'claude', '/work/app', ?, ?, ?, 'shallow')",
            )
            .expect("prepare catalog insert");
        for index in from..to {
            let ts = 1_700_000_000_000i64 + index as i64;
            stmt.execute(rusqlite::params![
                format!("bench-{index:06}"),
                ts,
                ts,
                format!("synthetic prompt {index}")
            ])
            .expect("catalog insert");
        }
    }
    conn.execute_batch("COMMIT").expect("commit");
}

/// Transcript-shaped volume beside the catalog: `history` rows (which carry an
/// FTS index) and `session_events` rows. The cache-only listing must not care
/// that these exist.
fn seed_event_volume(conn: &Connection, from: usize, to: usize) {
    conn.execute_batch("BEGIN").expect("begin");
    {
        let mut history = conn
            .prepare(
                "INSERT OR IGNORE INTO history (source, session_id, project, prompt, timestamp_ms) \
                 VALUES ('claude', ?, '/work/app', ?, ?)",
            )
            .expect("prepare history insert");
        let mut events = conn
            .prepare(
                "INSERT INTO session_events \
                 (source, session_id, ts_ms, role, kind, text, event_uid) \
                 VALUES ('claude', ?, ?, 'assistant', 'text', ?, ?)",
            )
            .expect("prepare event insert");
        for index in from..to {
            let ts = 1_700_000_000_000i64 + index as i64;
            let session = format!("bench-{:06}", index % 1_000);
            history
                .execute(rusqlite::params![
                    session,
                    format!("synthetic history row {index} with some searchable words"),
                    ts
                ])
                .expect("history insert");
            events
                .execute(rusqlite::params![
                    session,
                    ts,
                    format!("synthetic assistant turn {index}"),
                    format!("uid-{index}")
                ])
                .expect("event insert");
        }
    }
    conn.execute_batch("COMMIT").expect("commit");
}

// --- 2. bounded request ----------------------------------------------------

/// A `--limit 5` request reads five sessions' worth of bounded head/tail, and
/// reads exactly the same bytes when the archive around it grows 5x.
fn measure_bounded_request(home: &Path, counts: &ArchiveCounts) {
    let temp = tempfile::tempdir().expect("db dir");
    let limit = 5usize;

    let db_small = temp.path().join("small.db");
    let (elapsed_small, stdout_small) = run_cli(
        home,
        &db_small,
        &["sessions", "discover", "--json", "--limit", "5"],
    );
    let small = discover_summary(&stdout_small);

    let bytes_grown = grow_archive(home, counts.sessions * 4);
    let db_grown = temp.path().join("grown.db");
    let (elapsed_grown, stdout_grown) = run_cli(
        home,
        &db_grown,
        &["sessions", "discover", "--json", "--limit", "5"],
    );
    let grown = discover_summary(&stdout_grown);

    let budget = (HEAD_SCAN_MAX_BYTES + TAIL_SCAN_MAX_BYTES) * limit as u64;
    for (label, summary, archive) in [
        ("small", &small, counts.bytes),
        ("grown", &grown, bytes_grown),
    ] {
        assert_eq!(
            counter(summary, "shallow_reads"),
            limit as u64,
            "{label}: a limit of {limit} must read exactly {limit} sources"
        );
        assert!(
            counter(summary, "bytes_read") < archive,
            "{label}: a bounded request must not read the whole archive"
        );
    }
    assert_eq!(
        counter(&small, "bytes_read"),
        counter(&grown, "bytes_read"),
        "growing the archive must not change what a limit-5 request reads"
    );
    assert!(
        counter(&small, "bytes_read") <= budget + 64 * 1024,
        "five bounded reads must stay inside the documented file-read budget of {}",
        bytes(budget)
    );
    assert!(
        counter(&grown, "candidates_enumerated") > counter(&small, "candidates_enumerated"),
        "the grown archive must actually be larger"
    );

    let row = |label: &str, summary: &Value, archive: u64, elapsed: Duration| {
        let read = counter(summary, "bytes_read");
        vec![
            label.to_string(),
            counter(summary, "candidates_enumerated").to_string(),
            bytes(archive),
            counter(summary, "shallow_reads").to_string(),
            counter(summary, "files_opened").to_string(),
            bytes(read),
            format!("{:.1}%", read as f64 * 100.0 / archive as f64),
            micros(elapsed),
        ]
    };
    table(
        "2. Bounded request (--limit 5) — work is set by the limit, not the archive",
        &[
            "archive",
            "candidates",
            "archive bytes",
            "shallow reads",
            "files opened",
            "bytes read",
            "share of archive",
            "wall",
        ],
        &[
            row("small", &small, counts.bytes, elapsed_small),
            row("grown (5x sessions)", &grown, bytes_grown, elapsed_grown),
        ],
    );
}

// --- 3. shallow discovery versus full indexing -----------------------------

/// The same archive, discovered shallowly and then indexed fully.
fn measure_shallow_versus_full(home: &Path) {
    let temp = tempfile::tempdir().expect("db dir");
    let archive = archive_bytes(home);

    let discover_db = temp.path().join("discover.db");
    let (cold, cold_stdout) = run_cli(home, &discover_db, &["sessions", "discover", "--json"]);
    let cold_summary = discover_summary(&cold_stdout);
    let discovered = cold_summary["discovered"].as_u64().expect("discovered");

    // A rescan over unchanged bytes: every candidate is served from the
    // catalog on its stamp, so no source is opened at all.
    let (warm, warm_stdout) = run_cli(home, &discover_db, &["sessions", "discover", "--json"]);
    let warm_summary = discover_summary(&warm_stdout);
    assert_eq!(
        counter(&warm_summary, "shallow_reads"),
        0,
        "an unchanged rescan must reparse nothing"
    );
    assert_eq!(
        warm_summary["skipped_unchanged"].as_u64(),
        Some(discovered),
        "every session discovered cold must be served from the catalog warm"
    );

    let sync_db = temp.path().join("sync.db");
    let (full, _) = run_cli(home, &sync_db, &["sync"]);
    let sync_conn = open_db(&sync_db).expect("sync db");
    let full_rows: i64 = sync_conn
        .query_row(
            "SELECT (SELECT COUNT(*) FROM history) + (SELECT COUNT(*) FROM session_events)",
            [],
            |row| row.get(0),
        )
        .expect("full ingest rows");
    let shallow_conn = open_db(&discover_db).expect("discover db");
    let shallow_rows: i64 = shallow_conn
        .query_row(
            "SELECT (SELECT COUNT(*) FROM history) + (SELECT COUNT(*) FROM session_events)",
            [],
            |row| row.get(0),
        )
        .expect("shallow rows");

    assert!(
        counter(&cold_summary, "bytes_read") < archive,
        "shallow discovery must read less than the archive it catalogs"
    );
    assert_eq!(
        shallow_rows, 0,
        "shallow discovery writes catalog rows only — no history, no events"
    );
    assert!(
        full_rows > 0,
        "the full sync must actually have ingested the archive"
    );

    table(
        "3. Shallow discovery vs full indexing — same archive",
        &[
            "operation",
            "bytes read",
            "files opened",
            "sessions",
            "history+event rows",
            "wall",
        ],
        &[
            vec![
                "sessions discover (cold)".into(),
                bytes(counter(&cold_summary, "bytes_read")),
                counter(&cold_summary, "files_opened").to_string(),
                discovered.to_string(),
                shallow_rows.to_string(),
                micros(cold),
            ],
            vec![
                "sessions discover (rescan)".into(),
                bytes(counter(&warm_summary, "bytes_read")),
                counter(&warm_summary, "files_opened").to_string(),
                warm_summary["skipped_unchanged"].to_string(),
                "0".into(),
                micros(warm),
            ],
            vec![
                "sync (full ingest)".into(),
                format!(">= {}", bytes(archive)),
                "all".into(),
                "-".into(),
                full_rows.to_string(),
                micros(full),
            ],
        ],
    );
    println!(
        "  archive: {}; full ingest reads every transcript end to end, so its byte",
        bytes(archive)
    );
    println!("  figure is a floor rather than a measurement.");
}

// --- 4. cache-only listing reads nothing -----------------------------------

/// The catalog listing opens no provider file — demonstrated by deleting every
/// provider directory and listing the catalog anyway.
fn measure_cache_only_listing(home: &Path) {
    let temp = tempfile::tempdir().expect("db dir");
    let db_path = temp.path().join("catalog.db");
    let conn = open_db(&db_path).expect("catalog db");
    let env = DiscoveryEnv::with_roots(&conn, home.to_path_buf(), home.join("opencode.db"));
    let mut discovered = 0usize;
    let summary: DiscoverySummary =
        discover_sessions_with_env(&env, &DiscoverOptions::default(), |_| discovered += 1)
            .expect("discovery");
    assert!(discovered > 0, "the archive must yield sessions");

    let options = CatalogListOptions {
        limit: Some(50),
        ..Default::default()
    };
    let (before, listed_before) = time_repeated(15, || {
        list_session_catalog(&conn, &options).expect("listing")
    });

    for provider in [".claude", ".codex", ".cursor", ".grok"] {
        fs::remove_dir_all(home.join(provider)).expect("remove provider dir");
    }
    fs::remove_file(home.join("opencode.db")).expect("remove opencode db");

    let (after, listed_after) = time_repeated(15, || {
        list_session_catalog(&conn, &options).expect("listing after deletion")
    });
    assert_eq!(
        listed_before.len(),
        listed_after.len(),
        "a cache-only listing cannot depend on provider files"
    );
    assert_eq!(
        listed_before
            .iter()
            .map(|row| (
                row.source.clone(),
                row.session_id.clone(),
                row.first_prompt.clone()
            ))
            .collect::<Vec<_>>(),
        listed_after
            .iter()
            .map(|row| (
                row.source.clone(),
                row.session_id.clone(),
                row.first_prompt.clone()
            ))
            .collect::<Vec<_>>(),
        "the same rows, with the same derived prompts, after the archive is gone"
    );

    table(
        "4. Cache-only listing — zero provider reads",
        &["archive on disk", "rows listed", "median"],
        &[
            vec![
                "present".into(),
                listed_before.len().to_string(),
                micros(before),
            ],
            vec![
                "deleted".into(),
                listed_after.len().to_string(),
                micros(after),
            ],
        ],
    );
    println!(
        "  discovery that populated this catalog: {} row(s), {} file(s) opened, {} read",
        summary.discovered,
        summary.counters.files_opened,
        bytes(summary.counters.bytes_read)
    );
}

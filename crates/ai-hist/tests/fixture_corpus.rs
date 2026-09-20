//! Checked-in harness fixture corpus and parser characterization snapshots.
//!
//! Every fixture in `tests/fixtures/` is staged into an isolated provider
//! `HOME`, taken through the acquisition path a host actually uses (local
//! sync, shallow discovery, targeted hydration), and the resulting evidence
//! tables are dumped to a canonical JSON snapshot under `tests/snapshots/`.
//!
//! The snapshots record **current behaviour, including the gaps**. They are
//! not aspirational. Each later parity issue in the #160 epic updates the
//! snapshots it changes, so a reviewer sees exactly what a parser change did
//! to every provider log shape we know about.
//!
//! Regenerate with:
//!
//! ```text
//! UPDATE_SNAPSHOTS=1 cargo test -p ai-hist --all-features --test fixture_corpus
//! ```
//!
//! Facts relayhistory does not capture yet are still written as tests, marked
//! `#[ignore = "closed by #<issue>"]`. The issue that closes the gap removes
//! the attribute.

use ai_hist::{
    discover_sessions_with_env, hydrate_session_at, open_db, sync_scoped_at, DiscoverOptions,
    DiscoveryEnv, HydrateSessionOptions, SessionScope,
};
use rusqlite::Connection;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;
use std::time::{Duration, SystemTime};

// ---------------------------------------------------------------------------
// manifest
// ---------------------------------------------------------------------------

/// How a corpus entry is placed under the fixture's isolated `HOME`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Layout {
    /// Flat transcript(s) into `~/.claude/projects/corpus/`.
    ClaudeTranscript,
    /// Flat rollout(s) into `~/.codex/sessions/2026/04/20/`, renamed to the
    /// `rollout-` prefix the codex adapter enumerates.
    CodexRollout,
    /// A directory whose contents are copied verbatim into `HOME`. Used for
    /// every fixture whose provider layout is itself part of the fixture.
    HomeTree,
    /// A `.sql` file executed into `~/.local/share/opencode/opencode.db`.
    OpencodeSqlite,
    /// burn's older OpenCode JSON layout, copied under
    /// `~/.local/share/opencode/`. relayhistory reads the SQLite store only,
    /// so these snapshot as empty until #168.
    OpencodeLegacyJson,
    /// Kept for provenance, never staged and never snapshotted.
    Reference,
}

/// Who authored the fixture.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Origin {
    /// Copied verbatim from `AgentWorkforce/burn` (Apache-2.0).
    Burn,
    /// Authored here, for a log shape burn's corpus does not cover.
    RelayHistory,
}

struct Fixture {
    /// `SOURCE_CHOICES` entry the fixture exercises.
    source: &'static str,
    /// Snapshot name: `tests/snapshots/<source>/<name>.json`.
    name: &'static str,
    layout: Layout,
    origin: Origin,
    /// Corpus paths, relative to `tests/fixtures`. More than one means the
    /// fixture deliberately stages several files into one `HOME`, because the
    /// quirk it encodes only exists across files.
    files: &'static [&'static str],
    /// The harness quirk this fixture pins down. Mirrored in the corpus
    /// README, which a test checks.
    quirk: &'static str,
}

/// The corpus. Every file under `tests/fixtures` must appear in exactly one
/// entry's `files` (a directory entry covers everything beneath it), and every
/// non-[`Layout::Reference`] entry has a committed snapshot.
const CORPUS: &[Fixture] = &[
    // -- claude, from burn -------------------------------------------------
    Fixture {
        source: "claude",
        name: "simple-turn",
        layout: Layout::ClaudeTranscript,
        origin: Origin::Burn,
        files: &["claude/simple-turn.jsonl"],
        quirk: "one user turn and one complete assistant turn with full usage, preceded by a `permission-mode` control record",
    },
    Fixture {
        source: "claude",
        name: "multi-block-turn",
        layout: Layout::ClaudeTranscript,
        origin: Origin::Burn,
        files: &["claude/multi-block-turn.jsonl"],
        quirk: "four assistant records share one `message.id` and one `requestId`; only the first carries the usage payload",
    },
    Fixture {
        source: "claude",
        name: "interleaved-turns",
        layout: Layout::ClaudeTranscript,
        origin: Origin::Burn,
        files: &["claude/interleaved-turns.jsonl"],
        quirk: "two assistant messages interleave their blocks instead of arriving contiguously",
    },
    Fixture {
        source: "claude",
        name: "incomplete-then-complete",
        layout: Layout::ClaudeTranscript,
        origin: Origin::Burn,
        files: &["claude/incomplete-then-complete.jsonl"],
        quirk: "a complete assistant message is followed by an in-progress one (`stop_reason: null`)",
    },
    Fixture {
        source: "claude",
        name: "files-touched",
        layout: Layout::ClaudeTranscript,
        origin: Origin::Burn,
        files: &["claude/files-touched.jsonl"],
        quirk: "two Read tool uses and a Grep in one assistant message — three calls, no file mutated",
    },
    Fixture {
        source: "claude",
        name: "retry-loop",
        layout: Layout::ClaudeTranscript,
        origin: Origin::Burn,
        files: &["claude/retry-loop.jsonl"],
        quirk: "the same Bash command is retried four times, every attempt returning `is_error: true`",
    },
    Fixture {
        source: "claude",
        name: "consecutive-failures",
        layout: Layout::ClaudeTranscript,
        origin: Origin::Burn,
        files: &["claude/consecutive-failures.jsonl"],
        quirk: "three different tools fail back to back, each with its own errored tool_result",
    },
    Fixture {
        source: "claude",
        name: "edit-revert",
        layout: Layout::ClaudeTranscript,
        origin: Origin::Burn,
        files: &["claude/edit-revert.jsonl"],
        quirk: "an Edit is applied and then reverted; tool_results carry pre/post file hashes",
    },
    Fixture {
        source: "claude",
        name: "missing-output-tokens",
        layout: Layout::ClaudeTranscript,
        origin: Origin::Burn,
        files: &["claude/missing-output-tokens.jsonl"],
        quirk: "usage carries `input_tokens` only — `output_tokens` is absent, which is not the same as zero",
    },
    Fixture {
        source: "claude",
        name: "user-turn-blocks",
        layout: Layout::ClaudeTranscript,
        origin: Origin::Burn,
        files: &["claude/user-turn-blocks.jsonl"],
        quirk: "user records carrying tool_result blocks of very different sizes, one of them errored",
    },
    Fixture {
        source: "claude",
        name: "compact-boundary",
        layout: Layout::ClaudeTranscript,
        origin: Origin::Burn,
        files: &["claude/compact-boundary.jsonl"],
        quirk: "a `system` record with `subtype: compact_boundary` splits the transcript",
    },
    Fixture {
        source: "claude",
        name: "sidechain-turn",
        layout: Layout::ClaudeTranscript,
        origin: Origin::Burn,
        files: &["claude/sidechain-turn.jsonl"],
        quirk: "every record is `isSidechain: true` — a subagent sidecar, not a session of its own",
    },
    Fixture {
        source: "claude",
        name: "sidechain-leading-then-main",
        layout: Layout::ClaudeTranscript,
        origin: Origin::Burn,
        files: &["claude/sidechain-leading-then-main.jsonl"],
        quirk: "sidechain records precede the first main-chain record in the same file",
    },
    Fixture {
        source: "claude",
        name: "nested-subagent",
        layout: Layout::ClaudeTranscript,
        origin: Origin::Burn,
        files: &["claude/nested-subagent.jsonl"],
        quirk: "a subagent spawns a subagent, in one file, joined by `agentId`",
    },
    Fixture {
        source: "claude",
        name: "system-subagent-notification",
        layout: Layout::ClaudeTranscript,
        origin: Origin::Burn,
        files: &["claude/system-subagent-notification.jsonl"],
        quirk: "a `system`/`subagent_completed` record reports a child session id the transcript never contains",
    },
    Fixture {
        source: "claude",
        name: "task-notification",
        layout: Layout::ClaudeTranscript,
        origin: Origin::Burn,
        files: &["claude/task-notification.jsonl"],
        quirk: "Task tool notifications arrive as their own records",
    },
    Fixture {
        source: "claude",
        name: "slash-command-triad",
        layout: Layout::ClaudeTranscript,
        origin: Origin::Burn,
        files: &["claude/slash-command-triad.jsonl"],
        quirk: "a slash command expands into a `<command-name>`/`<command-message>`/`<command-args>` triad of user records",
    },
    Fixture {
        source: "claude",
        name: "replacement-meta",
        layout: Layout::ClaudeTranscript,
        origin: Origin::Burn,
        files: &["claude/replacement-meta.jsonl"],
        quirk: "edit metadata only reaches the log on the tool_result, not on the tool_use",
    },
    Fixture {
        source: "claude",
        name: "oversized-bash-output",
        layout: Layout::ClaudeTranscript,
        origin: Origin::Burn,
        files: &["claude/oversized-bash-output.jsonl"],
        quirk: "an 80 KB Bash tool_result — the byte size is the fact, and no record is pretty-printed",
    },
    Fixture {
        source: "claude",
        name: "resume-marker",
        layout: Layout::ClaudeTranscript,
        origin: Origin::Burn,
        files: &["claude/resume-marker.jsonl"],
        quirk: "the first user record is a `/resume <sessionId>` marker naming the prior session",
    },
    Fixture {
        source: "claude",
        name: "parent-chain-out-of-order",
        layout: Layout::ClaudeTranscript,
        origin: Origin::Burn,
        files: &["claude/parent-chain-out-of-order.jsonl"],
        quirk: "records arrive out of `parentUuid` order, so turn grouping cannot rely on file order",
    },
    Fixture {
        source: "claude",
        name: "parent-chain-interrupt-resume",
        layout: Layout::ClaudeTranscript,
        origin: Origin::Burn,
        files: &["claude/parent-chain-interrupt-resume.jsonl"],
        quirk: "an interrupted turn is resumed, and both halves hang off the same parent record",
    },
    Fixture {
        source: "claude",
        name: "original-session",
        layout: Layout::ClaudeTranscript,
        origin: Origin::Burn,
        files: &["claude/original-session.jsonl"],
        quirk: "the root transcript that the fork and continuation fixtures point back at",
    },
    Fixture {
        source: "claude",
        name: "cross-file-parent-reconciliation",
        layout: Layout::ClaudeTranscript,
        origin: Origin::Burn,
        files: &[
            "claude/original-session.jsonl",
            "claude/cross-file-parent.jsonl",
        ],
        quirk: "a continuation whose first record's `parentUuid` only exists in the other file — the link is cross-file, not in-file",
    },
    Fixture {
        source: "claude",
        name: "explicit-continuation-reconciliation",
        layout: Layout::ClaudeTranscript,
        origin: Origin::Burn,
        files: &[
            "claude/original-session.jsonl",
            "claude/explicit-line-relationships.jsonl",
        ],
        quirk: "a continuation that states `continuedFromSessionId` on the record itself",
    },
    Fixture {
        source: "claude",
        name: "fork-reconciliation",
        layout: Layout::ClaudeTranscript,
        origin: Origin::Burn,
        files: &[
            "claude/original-session.jsonl",
            "claude/fork-branch-a.jsonl",
            "claude/fork-branch-b.jsonl",
        ],
        quirk: "two transcripts share one source session id: a fork, not a continuation",
    },
    Fixture {
        source: "claude",
        name: "settings-reference",
        layout: Layout::Reference,
        origin: Origin::Burn,
        files: &["claude/settings/oversized-bash-output-length.json"],
        quirk: "burn's Claude settings input that sets the Bash output cap; relayhistory reads no settings file, so it is kept for provenance only",
    },
    // -- claude, authored here --------------------------------------------
    Fixture {
        source: "claude",
        name: "summary-record",
        layout: Layout::ClaudeTranscript,
        origin: Origin::RelayHistory,
        files: &["claude/summary-record.jsonl"],
        quirk: "a `type: \"summary\"` record with a `leafUuid` between two ordinary turns",
    },
    Fixture {
        source: "claude",
        name: "sidecar-subagent",
        layout: Layout::HomeTree,
        origin: Origin::RelayHistory,
        files: &["claude/sidecar-subagent"],
        quirk: "a subagent transcript in `<sessionId>/subagents/agent-<id>.jsonl` with its `agent-<id>.meta.json` sidecar, carrying the PARENT's sessionId",
    },
    // -- codex, from burn --------------------------------------------------
    Fixture {
        source: "codex",
        name: "simple-turn",
        layout: Layout::CodexRollout,
        origin: Origin::Burn,
        files: &["codex/simple-turn.jsonl"],
        quirk: "one turn: `session_meta`, `turn_context`, `task_started`, a null `token_count`, a populated one, `task_complete`",
    },
    Fixture {
        source: "codex",
        name: "multi-turn",
        layout: Layout::CodexRollout,
        origin: Origin::Burn,
        files: &["codex/multi-turn.jsonl"],
        quirk: "two turns in one rollout, each with its own cumulative `total_token_usage`",
    },
    Fixture {
        source: "codex",
        name: "with-tool-call",
        layout: Layout::CodexRollout,
        origin: Origin::Burn,
        files: &["codex/with-tool-call.jsonl"],
        quirk: "function calls and their outputs as `response_item` records",
    },
    Fixture {
        source: "codex",
        name: "with-spawn-agent",
        layout: Layout::CodexRollout,
        origin: Origin::Burn,
        files: &["codex/with-spawn-agent.jsonl"],
        quirk: "a `spawn_agent` function call: delegation stated in the tool call, not in session metadata",
    },
    Fixture {
        source: "codex",
        name: "compaction",
        layout: Layout::CodexRollout,
        origin: Origin::Burn,
        files: &["codex/compaction.jsonl"],
        quirk: "a `compacted` record with `replacement_history`, followed by `context_compacted` and a fresh turn",
    },
    Fixture {
        source: "codex",
        name: "session-meta-relationships",
        layout: Layout::CodexRollout,
        origin: Origin::Burn,
        files: &["codex/session-meta-relationships.jsonl"],
        quirk: "`sourceSessionId` / `forkSessionId` / `continuedFromSessionId` on a repeated `session_meta`",
    },
    Fixture {
        source: "codex",
        name: "user-turn-blocks",
        layout: Layout::CodexRollout,
        origin: Origin::Burn,
        files: &["codex/user-turn-blocks.jsonl"],
        quirk: "user input arriving as `response_item` message blocks rather than `event_msg`",
    },
    Fixture {
        source: "codex",
        name: "oversized-shell-output",
        layout: Layout::CodexRollout,
        origin: Origin::Burn,
        files: &["codex/oversized-shell-output.jsonl"],
        quirk: "an 80 KB shell function-call output",
    },
    // -- codex, authored here ---------------------------------------------
    Fixture {
        source: "codex",
        name: "parent-thread-id",
        layout: Layout::HomeTree,
        origin: Origin::RelayHistory,
        files: &["codex/parent-thread-id"],
        quirk: "a subagent rollout naming its root through `parent_thread_id` plus `thread_source: subagent`",
    },
    Fixture {
        source: "codex",
        name: "archived-session",
        layout: Layout::HomeTree,
        origin: Origin::RelayHistory,
        files: &["codex/archived-session"],
        quirk: "a rollout under `~/.codex/archived_sessions/`, the second codex discovery root",
    },
    // -- cursor, authored here ---------------------------------------------
    Fixture {
        source: "cursor",
        name: "prompt-transcript",
        layout: Layout::HomeTree,
        origin: Origin::RelayHistory,
        files: &["cursor/prompt-transcript"],
        quirk: "`agent-transcripts/<id>/<id>.jsonl` with string, block-array and `<user_query>`-wrapped prompts, assistant text, a tool_use and a tool_result; the provider records no timestamps",
    },
    // -- grok, authored here -----------------------------------------------
    Fixture {
        source: "grok",
        name: "full-session",
        layout: Layout::HomeTree,
        origin: Origin::RelayHistory,
        files: &["grok/full-session"],
        quirk: "a complete grok session directory: `summary.json`, `chat_history.jsonl` (including a `synthetic_reason` turn), plus the `updates.jsonl`, `prompt_context.json`, `signals.json` and `subagents/` files no relayhistory parser reads yet",
    },
    // -- opencode ----------------------------------------------------------
    Fixture {
        source: "opencode",
        name: "sqlite-store",
        layout: Layout::OpencodeSqlite,
        origin: Origin::RelayHistory,
        files: &["opencode/sqlite-store.sql"],
        quirk: "the current SQLite store: two sessions (one a child through `parent_id`), text/tool/step-finish parts, provider+model and token payloads on the assistant messages",
    },
    Fixture {
        source: "opencode",
        name: "legacy-json-simple",
        layout: Layout::OpencodeLegacyJson,
        origin: Origin::Burn,
        files: &["opencode/legacy-json-simple"],
        quirk: "legacy `storage/{session,message,part}` JSON layout: one session, three parts",
    },
    Fixture {
        source: "opencode",
        name: "legacy-json-multi-turn",
        layout: Layout::OpencodeLegacyJson,
        origin: Origin::Burn,
        files: &["opencode/legacy-json-multi-turn"],
        quirk: "legacy layout with a `ses_child` session carrying `parentID`",
    },
    Fixture {
        source: "opencode",
        name: "legacy-json-with-tool",
        layout: Layout::OpencodeLegacyJson,
        origin: Origin::Burn,
        files: &["opencode/legacy-json-with-tool"],
        quirk: "legacy layout with a `tool` part and its completed state",
    },
    Fixture {
        source: "opencode",
        name: "legacy-json-with-compaction",
        layout: Layout::OpencodeLegacyJson,
        origin: Origin::Burn,
        files: &["opencode/legacy-json-with-compaction"],
        quirk: "legacy layout with a summarized/compacted message",
    },
    Fixture {
        source: "opencode",
        name: "legacy-json-user-turn-blocks",
        layout: Layout::OpencodeLegacyJson,
        origin: Origin::Burn,
        files: &["opencode/legacy-json-user-turn-blocks"],
        quirk: "legacy layout with several tool parts of different sizes, one errored",
    },
];

// ---------------------------------------------------------------------------
// staging
// ---------------------------------------------------------------------------

/// Pinned base mtime for every staged file: 2026-04-20T00:00:00Z.
///
/// Discovery stamps, cursor prompt timestamps and grok's fallback timestamps
/// are all filesystem-derived, so without pinning the snapshots would change
/// on every checkout. Files inside one fixture get `base + index` seconds in
/// sorted-path order, which also makes discovery's recency ordering (and
/// therefore which of two transcripts sharing a session id wins the catalog
/// row) deterministic.
const FIXTURE_MTIME_MS: i64 = 1_776_643_200_000;

fn fixtures_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
}

fn snapshots_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("snapshots")
}

fn copy_tree(from: &Path, to: &Path) {
    if from.is_dir() {
        fs::create_dir_all(to).expect("create staged directory");
        let mut entries = fs::read_dir(from)
            .expect("read fixture directory")
            .map(|entry| entry.expect("fixture directory entry").path())
            .collect::<Vec<_>>();
        entries.sort();
        for entry in entries {
            copy_tree(&entry, &to.join(entry.file_name().expect("entry name")));
        }
    } else {
        fs::create_dir_all(to.parent().expect("staged parent")).expect("create staged parent");
        fs::copy(from, to).expect("copy fixture file");
    }
}

fn walk_files(root: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };
    let mut paths = entries
        .map(|entry| entry.expect("home entry").path())
        .collect::<Vec<_>>();
    paths.sort();
    for path in paths {
        if path.is_dir() {
            walk_files(&path, out);
        } else {
            out.push(path);
        }
    }
}

/// Give every staged file a deterministic mtime.
fn pin_mtimes(home: &Path) {
    let mut files = Vec::new();
    walk_files(home, &mut files);
    for (index, path) in files.iter().enumerate() {
        let file = fs::OpenOptions::new()
            .write(true)
            .open(path)
            .expect("open staged file for mtime");
        let when = SystemTime::UNIX_EPOCH
            + Duration::from_millis((FIXTURE_MTIME_MS + index as i64 * 1_000) as u64);
        file.set_times(fs::FileTimes::new().set_modified(when))
            .expect("pin staged mtime");
    }
}

fn stage(fixture: &Fixture, home: &Path) {
    let root = fixtures_root();
    match fixture.layout {
        Layout::Reference => {}
        Layout::ClaudeTranscript => {
            let project = home.join(".claude/projects/corpus");
            fs::create_dir_all(&project).expect("claude project dir");
            for file in fixture.files {
                let from = root.join(file);
                let name = from.file_name().expect("fixture file name");
                copy_tree(&from, &project.join(name));
            }
        }
        Layout::CodexRollout => {
            let day = home.join(".codex/sessions/2026/04/20");
            fs::create_dir_all(&day).expect("codex day dir");
            for file in fixture.files {
                let from = root.join(file);
                let stem = from.file_stem().and_then(|s| s.to_str()).expect("stem");
                copy_tree(
                    &from,
                    &day.join(format!("rollout-2026-04-20T00-00-00-{stem}.jsonl")),
                );
            }
        }
        Layout::HomeTree => {
            for file in fixture.files {
                copy_tree(&root.join(file), home);
            }
        }
        Layout::OpencodeSqlite => {
            let db_path = home.join(".local/share/opencode/opencode.db");
            fs::create_dir_all(db_path.parent().expect("opencode dir")).expect("opencode dir");
            let db = Connection::open(&db_path).expect("open opencode fixture store");
            for file in fixture.files {
                let sql = fs::read_to_string(root.join(file)).expect("read opencode fixture sql");
                db.execute_batch(&sql).expect("apply opencode fixture sql");
            }
        }
        Layout::OpencodeLegacyJson => {
            let target = home.join(".local/share/opencode");
            for file in fixture.files {
                copy_tree(&root.join(file), &target);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// snapshot construction
// ---------------------------------------------------------------------------

/// Every fixture's snapshot, built once for the whole test binary.
///
/// The builder sets `HOME` per fixture, which is only sound because it runs
/// inside this `LazyLock`: every test in this binary reaches its data through
/// here, so nothing else in the process is reading the environment while a
/// fixture is staged.
static CORPUS_SNAPSHOTS: LazyLock<BTreeMap<String, Value>> = LazyLock::new(build_corpus);

fn snapshot_key(fixture: &Fixture) -> String {
    format!("{}/{}", fixture.source, fixture.name)
}

fn build_corpus() -> BTreeMap<String, Value> {
    // One temp root, created before any environment mutation, so the only
    // `getenv` inside the staging window is the library's own. It is dropped
    // when the build finishes: every snapshot is already in memory by then,
    // and nothing else in this binary reads `HOME`.
    let root = tempfile::tempdir().expect("corpus temp root");
    let mut snapshots = BTreeMap::new();
    for fixture in CORPUS {
        if fixture.layout == Layout::Reference {
            continue;
        }
        let home = root.path().join(snapshot_key(fixture).replace('/', "__"));
        fs::create_dir_all(&home).expect("fixture home");
        stage(fixture, &home);
        pin_mtimes(&home);
        snapshots.insert(snapshot_key(fixture), capture(fixture, &home));
    }
    snapshots
}

fn capture(fixture: &Fixture, home: &Path) -> Value {
    let opencode_db = home.join(".local/share/opencode/opencode.db");
    std::env::set_var("HOME", home);
    std::env::set_var("USERPROFILE", home);
    std::env::set_var("OPENCODE_DB", &opencode_db);
    std::env::remove_var("AI_HIST_DB");

    let db = home.join("ai-history.db");
    let mut notes: Vec<String> = Vec::new();
    if let Err(error) = sync_scoped_at(&db, SessionScope::Local) {
        notes.push(format!("sync: {error:#}"));
    }

    let mut discovered = Vec::new();
    let mut diagnostics = Vec::new();
    {
        let conn = open_db(&db).expect("open history database");
        let env = DiscoveryEnv::with_roots(&conn, home.to_path_buf(), opencode_db.clone());
        let options = DiscoverOptions {
            scope: SessionScope::Local,
            sources: Vec::new(),
            limit: None,
        };
        match discover_sessions_with_env(&env, &options, |row| {
            discovered.push((row.source.clone(), row.session_id.clone()))
        }) {
            Ok(summary) => {
                for diagnostic in summary.diagnostics {
                    diagnostics.push(json!({
                        "source": diagnostic.source,
                        "error": diagnostic.error,
                    }));
                }
            }
            Err(error) => notes.push(format!("discover: {error:#}")),
        }
    }
    discovered.sort();
    discovered.dedup();

    let mut hydrations = Vec::new();
    for (source, session_id) in &discovered {
        let result = hydrate_session_at(
            &db,
            &HydrateSessionOptions {
                source: source.clone(),
                session_id: session_id.clone(),
                scope: SessionScope::Local,
                include_related: true,
            },
        );
        // Only what hydration *decided* is snapshotted. `capability`,
        // `discovery_state` and the `evidence` counters are derived bookkeeping
        // about one call, they duplicate what the evidence tables below already
        // say, and they are not reproducible: under a loaded
        // `cargo test --workspace` run `capability` flips between "full" and
        // "partial" and `evidence.events` over-reports against the row count.
        // Pinning them would buy a flaky gate and no extra information. See
        // #169, which owns `capability`.
        hydrations.push(match result {
            Ok(result) => json!({
                "source": source,
                "session_id": session_id,
                "status": result.status,
                "related_session_ids": result.related_session_ids,
            }),
            Err(error) => json!({
                "source": source,
                "session_id": session_id,
                "error": format!("{error:#}"),
            }),
        });
    }

    let conn = open_db(&db).expect("reopen history database");
    let snapshot = json!({
        "fixture": snapshot_key(fixture),
        "source": fixture.source,
        "origin": format!("{:?}", fixture.origin).to_lowercase(),
        "layout": format!("{:?}", fixture.layout),
        "corpus_files": fixture.files,
        "quirk": fixture.quirk,
        "notes": notes,
        "discovery_diagnostics": diagnostics,
        "hydration": hydrations,
        "sessions": dump(&conn, SESSIONS_SQL),
        "session_events": dump(&conn, SESSION_EVENTS_SQL),
        "tool_calls": dump(&conn, TOOL_CALLS_SQL),
        "file_edits": dump(&conn, FILE_EDITS_SQL),
        "session_relationships": dump(&conn, SESSION_RELATIONSHIPS_SQL),
        "history": dump(&conn, HISTORY_SQL),
    });
    redact(snapshot, home)
}

/// `sessions.parser_version` is deliberately not selected. It is the ingest
/// engine's own generation counter, not anything extracted from a provider
/// log: bumping `HYDRATION_PARSER_VERSION` (which several of the parity issues
/// will) would rewrite every snapshot in the corpus while saying nothing about
/// what any parser now reads.
const SESSIONS_SQL: &str = "SELECT source, session_id, cwd, git_branch, first_activity_ms, \
     last_activity_ms, last_assistant_text, raw_path, first_prompt, models_json, \
     originator, agent_version, repo_url, initial_commit, workspace_roots_json, source_stamp, \
     discovery_state FROM sessions ORDER BY source, session_id";
const SESSION_EVENTS_SQL: &str =
    "SELECT source, session_id, project, cwd, git_branch, message_id, \
     parent_id, ts_ms, role, kind, text, model, token_json, event_uid FROM session_events \
     ORDER BY source, session_id, ts_ms, event_uid";
const TOOL_CALLS_SQL: &str = "SELECT source, session_id, message_id, tool_use_id, name, target, \
     args_json, is_error, ts_ms FROM tool_calls ORDER BY source, session_id, ts_ms, tool_use_id";
const FILE_EDITS_SQL: &str = "SELECT source, session_id, message_id, tool_use_id, file_path, \
     tool_name, lines_added, lines_removed, structured_patch_json, user_modified, ts_ms, \
     git_branch, cwd FROM file_edits ORDER BY source, session_id, ts_ms, tool_use_id";
const SESSION_RELATIONSHIPS_SQL: &str = "SELECT source, parent_session_id, relationship_uid, \
     child_session_id, relationship, identity_status, child_agent_type, child_agent_name, \
     child_model, spawn_depth, evidence_kind, evidence_locator, evidence_ref, child_has_events, \
     spawned_at_ms FROM session_relationships ORDER BY source, parent_session_id, relationship_uid";
const HISTORY_SQL: &str = "SELECT source, session_id, project, prompt, timestamp_ms FROM history \
     ORDER BY source, session_id, timestamp_ms, prompt";

/// Run one canonical query and return its rows as JSON objects. Autoincrement
/// ids are never selected, so a snapshot says nothing about insertion order.
fn dump(conn: &Connection, sql: &str) -> Vec<Value> {
    let mut stmt = conn.prepare(sql).expect("prepare snapshot query");
    let columns = stmt
        .column_names()
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();
    let rows = stmt
        .query_map([], |row| {
            let mut object = Map::new();
            for (index, column) in columns.iter().enumerate() {
                object.insert(column.clone(), sql_value(row.get_ref(index)?));
            }
            Ok(Value::Object(object))
        })
        .expect("run snapshot query")
        .collect::<rusqlite::Result<Vec<_>>>()
        .expect("collect snapshot rows");
    rows
}

fn sql_value(value: rusqlite::types::ValueRef<'_>) -> Value {
    use rusqlite::types::ValueRef;
    match value {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(number) => json!(number),
        ValueRef::Real(number) => json!(number),
        ValueRef::Text(bytes) => json!(String::from_utf8_lossy(bytes)),
        ValueRef::Blob(bytes) => json!(format!("<blob {} bytes>", bytes.len())),
    }
}

/// Replace the values a snapshot cannot own: the temp `HOME` prefix
/// everywhere, and the filesystem-derived numbers inside `source_stamp`.
fn redact(value: Value, home: &Path) -> Value {
    let mut homes = vec![home.to_string_lossy().to_string()];
    if let Ok(canonical) = fs::canonicalize(home) {
        homes.push(canonical.to_string_lossy().to_string());
    }
    // Longest first: on macOS the temp root is `/var/...` while its canonical
    // form is `/private/var/...`, so replacing the short one first would leave
    // `/private<home>/…` behind and the long one would then never match.
    homes.sort_by_key(|home| std::cmp::Reverse(home.len()));
    redact_value(value, &homes, false)
}

fn redact_value(value: Value, homes: &[String], stamp: bool) -> Value {
    match value {
        Value::String(text) => Value::String(redact_string(text, homes, stamp)),
        Value::Array(items) => Value::Array(
            items
                .into_iter()
                .map(|item| redact_value(item, homes, stamp))
                .collect(),
        ),
        Value::Object(map) => Value::Object(
            map.into_iter()
                .map(|(key, item)| {
                    let stamp = stamp || key == "source_stamp";
                    (key, redact_value(item, homes, stamp))
                })
                .collect(),
        ),
        other => other,
    }
}

/// Redact one string: strip the fixture's `HOME` prefix, and — only when that
/// prefix was actually found — normalize the separators of what is left.
///
/// The separator pass is what keeps the corpus cross-platform. `raw_path`,
/// `evidence_locator` and the `evidence:…:<path>` form of `relationship_uid`
/// are built from real `PathBuf`s, so on Windows they arrive as
/// `<home>\.claude\projects\…` while the committed snapshots (generated on
/// Linux) hold `<home>/.claude/projects/…`. Without this, every snapshot reads
/// as stale on a Windows checkout even though no parser changed.
///
/// It is deliberately gated on a replacement having happened: a `\` only means
/// "path separator" in a string that carries one of these locators. Applying it
/// to every string would corrupt the JSON payloads in `token_json` and
/// `args_json`, whose escapes are backslashes. Nothing this normalizes reaches
/// the store — the ledger keeps the platform's own separators.
fn redact_string(text: String, homes: &[String], stamp: bool) -> String {
    let mut text = text;
    let mut replaced = false;
    for home in homes {
        if home.is_empty() || !text.contains(home.as_str()) {
            continue;
        }
        text = text.replace(home.as_str(), "<home>");
        replaced = true;
    }
    if replaced {
        text = text.replace('\\', "/");
    }
    if stamp {
        text = mask_long_digit_runs(&text);
    }
    text
}

/// Collapse every run of ten or more digits in a change stamp to `<n>`.
///
/// Claude/codex/cursor/grok stamps are `"{mtime_nanos}:{len}"` and OpenCode's
/// is `"{generation}:{schema}:{created}:{updated}"`; the epoch components are
/// 13 to 19 digits and depend on the filesystem's timestamp resolution, while
/// a fixture's byte length is five or six digits. The threshold keeps the fact
/// and drops the noise. Applied to `source_stamp` only, so session ids that
/// happen to be long digit runs are untouched.
fn mask_long_digit_runs(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut digits = String::new();
    for character in text.chars() {
        if character.is_ascii_digit() {
            digits.push(character);
            continue;
        }
        flush_digits(&mut digits, &mut out);
        out.push(character);
    }
    flush_digits(&mut digits, &mut out);
    out
}

fn flush_digits(digits: &mut String, out: &mut String) {
    if digits.len() >= 10 {
        out.push_str("<n>");
    } else {
        out.push_str(digits);
    }
    digits.clear();
}

/// A Windows locator redacts to the same string a Linux one does.
///
/// This is the whole reason the corpus can be reviewed from any checkout: the
/// snapshots are generated on Linux, and a Windows contributor running the
/// harness locally must see "no parser changed", not 46 stale snapshots.
#[test]
fn redaction_normalizes_windows_locators_to_the_committed_form() {
    let windows = vec![r"C:\Users\dev\AppData\Local\Temp\.tmpAbCd\claude__simple-turn".to_string()];
    assert_eq!(
        redact_string(
            r"C:\Users\dev\AppData\Local\Temp\.tmpAbCd\claude__simple-turn\.claude\projects\corpus\simple-turn.jsonl".to_string(),
            &windows,
            false,
        ),
        "<home>/.claude/projects/corpus/simple-turn.jsonl"
    );
    // The `evidence:<kind>:<path>` relationship key carries a locator too.
    assert_eq!(
        redact_string(
            r"evidence:claude_sidechain_records:C:\Users\dev\AppData\Local\Temp\.tmpAbCd\claude__simple-turn\.claude\projects\corpus\sidechain-turn.jsonl".to_string(),
            &windows,
            false,
        ),
        "evidence:claude_sidechain_records:<home>/.claude/projects/corpus/sidechain-turn.jsonl"
    );

    let unix = vec!["/tmp/.tmpAbCd/claude__simple-turn".to_string()];
    assert_eq!(
        redact_string(
            "/tmp/.tmpAbCd/claude__simple-turn/.claude/projects/corpus/simple-turn.jsonl"
                .to_string(),
            &unix,
            false,
        ),
        "<home>/.claude/projects/corpus/simple-turn.jsonl",
        "both platforms redact to one committed form"
    );

    // A string with no locator in it is left exactly as it was: the escapes in
    // a stored JSON payload are not path separators.
    let payload = r#"{"text":"a\\b","input_tokens":3}"#.to_string();
    assert_eq!(redact_string(payload.clone(), &unix, false), payload);

    // macOS hands back both `/var/...` and its canonical `/private/var/...`.
    // `redact` sorts longest-first so the short one cannot shadow the long one.
    let mut macos = vec![
        "/var/folders/xy/T/corpus".to_string(),
        "/private/var/folders/xy/T/corpus".to_string(),
    ];
    macos.sort_by_key(|home| std::cmp::Reverse(home.len()));
    assert_eq!(
        redact_string(
            "/private/var/folders/xy/T/corpus/.claude/projects/corpus/simple-turn.jsonl"
                .to_string(),
            &macos,
            false,
        ),
        "<home>/.claude/projects/corpus/simple-turn.jsonl"
    );
}

// ---------------------------------------------------------------------------
// snapshot assertions
// ---------------------------------------------------------------------------

fn snapshot(key: &str) -> &'static Value {
    CORPUS_SNAPSHOTS
        .get(key)
        .unwrap_or_else(|| panic!("no snapshot for fixture {key}; is it in CORPUS?"))
}

fn rows<'a>(key: &'a str, table: &str) -> &'a [Value] {
    snapshot(key)
        .get(table)
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("{key} snapshot has no {table} array"))
        .as_slice()
}

fn field<'a>(row: &'a Value, name: &str) -> &'a Value {
    row.get(name)
        .unwrap_or_else(|| panic!("row has no column {name}: {row}"))
}

fn text<'a>(row: &'a Value, name: &str) -> &'a str {
    field(row, name).as_str().unwrap_or_default()
}

/// Every fixture's evidence matches its committed snapshot.
///
/// This is the characterization gate: any parser change that alters what
/// relayhistory extracts from a known harness log shape fails here until the
/// snapshot is regenerated, and the regenerated diff is the review artifact.
#[test]
fn corpus_snapshots_match_committed_evidence() {
    let update = std::env::var_os("UPDATE_SNAPSHOTS").is_some_and(|value| value != "0");
    let mut mismatched = Vec::new();
    for (key, value) in CORPUS_SNAPSHOTS.iter() {
        let path = snapshots_root().join(format!("{key}.json"));
        let mut rendered = serde_json::to_string_pretty(value).expect("render snapshot");
        rendered.push('\n');
        if update {
            fs::create_dir_all(path.parent().expect("snapshot parent"))
                .expect("create snapshot directory");
            fs::write(&path, &rendered).expect("write snapshot");
            continue;
        }
        match fs::read_to_string(&path) {
            Ok(committed) if committed == rendered => {}
            Ok(committed) => mismatched.push(format!(
                "{key}: {}",
                first_difference(&committed, &rendered)
            )),
            Err(error) => mismatched.push(format!("{key}: {error} ({})", path.display())),
        }
    }
    assert!(
        mismatched.is_empty(),
        "fixture snapshots are stale. Review the change, then regenerate with \
         `UPDATE_SNAPSHOTS=1 cargo test -p ai-hist --all-features --test fixture_corpus`:\n{}",
        mismatched.join("\n")
    );
}

/// The first line that moved, so a failure names the behaviour that changed
/// instead of only the fixture it changed in.
fn first_difference(committed: &str, rendered: &str) -> String {
    for (index, (was, now)) in committed.lines().zip(rendered.lines()).enumerate() {
        if was != now {
            return format!("line {}: committed `{was}`, now `{now}`", index + 1);
        }
    }
    format!(
        "committed has {} lines, current behaviour has {}",
        committed.lines().count(),
        rendered.lines().count()
    )
}

/// No snapshot is left behind by a fixture that was renamed or removed.
#[test]
fn no_orphaned_snapshots() {
    let root = snapshots_root();
    let mut committed = Vec::new();
    walk_files(&root, &mut committed);
    let expected = CORPUS_SNAPSHOTS.keys().cloned().collect::<BTreeSet<_>>();
    for path in committed {
        let key = path
            .strip_prefix(&root)
            .expect("snapshot under root")
            .with_extension("")
            .to_string_lossy()
            .replace('\\', "/");
        assert!(
            expected.contains(&key),
            "{} has no CORPUS entry",
            path.display()
        );
    }
}

// ---------------------------------------------------------------------------
// corpus bookkeeping
// ---------------------------------------------------------------------------

/// Every checked-in fixture file belongs to exactly one corpus entry, and
/// every corpus entry names files that exist.
#[test]
fn corpus_manifest_covers_every_fixture_file() {
    let root = fixtures_root();
    let mut claimed = BTreeSet::new();
    for fixture in CORPUS {
        for file in fixture.files {
            let path = root.join(file);
            assert!(path.exists(), "{} is in CORPUS but not on disk", file);
            let mut files = Vec::new();
            if path.is_dir() {
                walk_files(&path, &mut files);
            } else {
                files.push(path);
            }
            claimed.extend(files);
        }
    }
    let mut on_disk = Vec::new();
    walk_files(&root, &mut on_disk);
    for path in on_disk {
        if path.file_name().and_then(|name| name.to_str()) == Some("README.md") {
            continue;
        }
        assert!(
            claimed.contains(&path),
            "{} is not referenced by any CORPUS entry",
            path.display()
        );
    }
}

/// The corpus README names every fixture and the quirk it encodes.
#[test]
fn corpus_readme_lists_every_fixture_and_quirk() {
    let readme = fs::read_to_string(fixtures_root().join("README.md")).expect("corpus README");
    for fixture in CORPUS {
        let key = snapshot_key(fixture);
        assert!(readme.contains(&key), "README does not list {key}");
        assert!(
            readme.contains(fixture.quirk),
            "README does not carry the quirk text for {key}"
        );
    }
}

/// Every `SOURCE_CHOICES` entry has at least one fixture, or a documented
/// exemption. Adding a provider without a fixture fails here.
///
/// The exemption list mirrors `DISCOVERY_EXEMPTIONS`: a source that is not a
/// provider session at all has nothing to put in a harness corpus.
#[test]
fn every_source_choice_has_a_fixture_or_an_exemption() {
    const FIXTURE_EXEMPTIONS: &[(&str, &str)] = &[
        (
            "trajectory",
            "derived trajectory records, not provider sessions",
        ),
        (
            "relay",
            "projected from already-synced local rows; no provider log on disk to capture",
        ),
    ];
    let covered = CORPUS
        .iter()
        .filter(|fixture| fixture.layout != Layout::Reference)
        .map(|fixture| fixture.source)
        .collect::<BTreeSet<_>>();
    for source in ai_hist::SOURCE_CHOICES {
        let exempt = FIXTURE_EXEMPTIONS
            .iter()
            .find(|(name, _)| name == source)
            .map(|(_, reason)| *reason);
        match exempt {
            Some(reason) => assert!(
                !reason.is_empty() && !covered.contains(source),
                "{source} is both exempt and covered; pick one"
            ),
            None => assert!(
                covered.contains(source),
                "{source} has no fixture under tests/fixtures/ and no exemption; see \
                 docs/session-catalog.md 'Adding a provider'"
            ),
        }
    }
}

// ---------------------------------------------------------------------------
// ported raw-fact assertions
//
// Ported from burn's reader suites (`crates/relayburn-sdk/src/reader/
// {claude,codex,opencode}/tests.rs`). Only assertions about *raw facts* in the
// fixture cross over; burn-derived values (cost, fidelity class, inference
// collapsing) stay in burn.
// ---------------------------------------------------------------------------

#[test]
fn claude_simple_turn_carries_observed_session_metadata() {
    let sessions = rows("claude/simple-turn", "sessions");
    assert_eq!(sessions.len(), 1, "one catalog row");
    let session = &sessions[0];
    assert_eq!(
        text(session, "session_id"),
        "11111111-1111-1111-1111-111111111111"
    );
    assert_eq!(text(session, "cwd"), "/tmp/project");
    assert_eq!(text(session, "agent_version"), "2.1.96");
    assert_eq!(text(session, "first_prompt"), "hello");
    assert_eq!(text(session, "models_json"), "[\"claude-sonnet-4-6\"]");
    assert_eq!(text(session, "discovery_state"), "full");
}

/// burn: `multi_block_turn_emits_one_inference_with_merged_usage` — the four
/// assistant records share one `message.id` and exactly one of them carries
/// the usage payload. The carrier's values are the raw fact, and they must
/// reach storage unchanged rather than being summed, scaled or rounded.
#[test]
fn claude_multi_block_turn_preserves_the_carrier_usage_values() {
    let events = rows("claude/multi-block-turn", "session_events");
    let payloads = events
        .iter()
        .filter(|event| !field(event, "token_json").is_null())
        .map(|event| {
            serde_json::from_str::<Value>(text(event, "token_json")).expect("token payload is JSON")
        })
        .collect::<Vec<_>>();
    assert!(!payloads.is_empty(), "the turn's usage reached storage");
    for usage in &payloads {
        assert_eq!(usage.get("input_tokens").and_then(Value::as_i64), Some(3));
        assert_eq!(usage.get("output_tokens").and_then(Value::as_i64), Some(43));
        assert_eq!(
            usage.get("cache_read_input_tokens").and_then(Value::as_i64),
            Some(11_496)
        );
        assert_eq!(
            usage
                .get("cache_creation_input_tokens")
                .and_then(Value::as_i64),
            Some(4_773)
        );
    }
}

/// burn: `multi_block_turn_collapses_to_single_turn` — the turn's two tool
/// uses are `Bash` then `Agent`, in that order.
#[test]
fn claude_multi_block_turn_records_both_tool_uses_in_order() {
    let calls = rows("claude/multi-block-turn", "tool_calls");
    let names = calls
        .iter()
        .map(|call| text(call, "name"))
        .collect::<Vec<_>>();
    assert_eq!(names, vec!["Bash", "Agent"]);
    assert_eq!(text(&calls[0], "target"), "ls -la /tmp/project");
}

/// burn: `fidelity_marks_missing_output_tokens_as_partial`. The raw fact is
/// that `output_tokens` is *absent* from the wire payload. burn forces it to
/// 0 and records the absence separately; relayhistory must not turn the
/// absence into a zero either.
#[test]
fn claude_missing_output_tokens_stores_absence_not_zero() {
    let events = rows("claude/missing-output-tokens", "session_events");
    let usage = events
        .iter()
        .find(|event| !field(event, "token_json").is_null())
        .map(|event| {
            serde_json::from_str::<Value>(text(event, "token_json")).expect("token payload is JSON")
        })
        .expect("the assistant record carries a usage payload");
    assert_eq!(usage.get("input_tokens").and_then(Value::as_i64), Some(10));
    assert_eq!(
        usage.get("output_tokens"),
        None,
        "an absent output_tokens must stay absent, never 0: {usage}"
    );
}

/// burn: `files_touched_excludes_grep_and_bash` — the raw facts are the three
/// tool uses in the turn and the file each one names. Reading a file and
/// grepping for a pattern are not edits, so neither produces a `file_edits`
/// row; burn's read-set ("files touched") is a burn-side projection over the
/// same tool calls.
#[test]
fn claude_files_touched_records_every_tool_use_and_no_edit() {
    let calls = rows("claude/files-touched", "tool_calls");
    let seen = calls
        .iter()
        .map(|call| (text(call, "name"), text(call, "target")))
        .collect::<BTreeSet<_>>();
    assert_eq!(
        seen,
        BTreeSet::from([
            ("Grep", "foo.*bar"),
            ("Read", "/src/a.ts"),
            ("Read", "/src/b.ts"),
        ])
    );
    assert!(
        rows("claude/files-touched", "file_edits").is_empty(),
        "Read and Grep never mutate a file"
    );
}

/// burn: `marks_tool_call_is_error_when_tool_result_has_is_error_true` — every
/// one of the four `npm run build` attempts came back `is_error: true`, and
/// each retry is its own `tool_use_id`.
#[test]
fn claude_retry_loop_marks_every_failed_call() {
    let calls = rows("claude/retry-loop", "tool_calls");
    assert_eq!(calls.len(), 4, "{calls:?}");
    assert!(
        calls
            .iter()
            .all(|call| field(call, "is_error").as_i64() == Some(1)),
        "each errored tool_result marks its own call: {calls:?}"
    );
    let ids = calls
        .iter()
        .map(|call| text(call, "tool_use_id"))
        .collect::<BTreeSet<_>>();
    assert_eq!(ids.len(), 4, "a retry is a new tool_use_id: {ids:?}");
}

/// Three *different* tools failing back to back are three failed calls, not
/// one retried call.
#[test]
fn claude_consecutive_failures_marks_each_distinct_tool() {
    let calls = rows("claude/consecutive-failures", "tool_calls");
    let failed = calls
        .iter()
        .filter(|call| field(call, "is_error").as_i64() == Some(1))
        .map(|call| text(call, "name"))
        .collect::<BTreeSet<_>>();
    assert_eq!(failed, BTreeSet::from(["Bash", "Grep", "Read"]));
}

/// burn: `oversized-bash-output` — the transcript is one 80 KB tool result.
/// The tool call itself is recorded whatever the result size.
#[test]
fn claude_oversized_bash_output_records_the_call() {
    let calls = rows("claude/oversized-bash-output", "tool_calls");
    assert_eq!(calls.len(), 1, "one Bash call: {calls:?}");
    assert_eq!(text(&calls[0], "name"), "Bash");
    assert_eq!(text(&calls[0], "tool_use_id"), "tu_bash_big");
}

/// A sidecar transcript whose every record is `isSidechain: true` is evidence
/// about somebody else's session, never a session of its own. It still records
/// an *unlinked* delegation edge: the work happened, but the file names no
/// child identity.
#[test]
fn claude_sidechain_only_transcript_is_evidence_not_a_session() {
    assert!(
        rows("claude/sidechain-turn", "sessions").is_empty(),
        "a sidechain-only file must not enter the catalog"
    );
    let relationships = rows("claude/sidechain-turn", "session_relationships");
    assert_eq!(relationships.len(), 1, "{relationships:?}");
    let edge = &relationships[0];
    assert_eq!(
        text(edge, "parent_session_id"),
        "44444444-4444-4444-4444-444444444444"
    );
    assert_eq!(text(edge, "identity_status"), "unlinked");
    assert_eq!(text(edge, "evidence_kind"), "claude_sidechain_records");
}

/// The same file becomes a session as soon as one main-chain record appears.
#[test]
fn claude_sidechain_leading_then_main_is_a_session() {
    let sessions = rows("claude/sidechain-leading-then-main", "sessions");
    assert_eq!(sessions.len(), 1, "{sessions:?}");
    assert_eq!(
        text(&sessions[0], "session_id"),
        "cccccccc-cccc-cccc-cccc-cccccccccccc"
    );
}

/// The `agent-<id>.meta.json` sidecar names the delegated agent, and the
/// subagent transcript's records carry the PARENT's session id.
#[test]
fn claude_sidecar_subagent_is_recorded_as_a_delegation() {
    let relationships = rows("claude/sidecar-subagent", "session_relationships");
    assert_eq!(relationships.len(), 1, "{relationships:?}");
    let edge = &relationships[0];
    assert_eq!(text(edge, "parent_session_id"), "claude-sidecar-parent");
    assert_eq!(text(edge, "child_session_id"), "plan01");
    assert_eq!(text(edge, "identity_status"), "observed");
    assert_eq!(text(edge, "child_agent_name"), "plan the work");
    assert_eq!(text(edge, "child_agent_type"), "Plan");
    assert_eq!(field(edge, "spawn_depth").as_i64(), Some(1));
    assert!(
        rows("claude/sidecar-subagent", "sessions")
            .iter()
            .all(|session| text(session, "session_id") != "plan01"),
        "a delegated thread is evidence, not a catalog row"
    );
}

/// A subagent transcript's own usage belongs to the delegated thread, not to
/// the parent whose session id its records carry.
#[test]
fn claude_sidecar_subagent_usage_lands_under_the_child_thread() {
    let events = rows("claude/sidecar-subagent", "session_events");
    let child = events
        .iter()
        .filter(|event| text(event, "session_id") == "plan01")
        .collect::<Vec<_>>();
    assert_eq!(child.len(), 1, "{events:?}");
    let usage: Value =
        serde_json::from_str(text(child[0], "token_json")).expect("token payload is JSON");
    assert_eq!(usage.get("input_tokens").and_then(Value::as_i64), Some(30));
    assert_eq!(usage.get("output_tokens").and_then(Value::as_i64), Some(18));
    assert!(
        events
            .iter()
            .filter(|event| text(event, "session_id") == "claude-sidecar-parent")
            .all(|event| text(event, "message_id") != "u-agent-asst-1"),
        "the child's output is not re-attributed to the parent: {events:?}"
    );
}

/// burn: `parent_chain_groups_out_of_order_rows_for_classification`. The raw
/// fact that survives into relayhistory is that no record is dropped for
/// arriving out of `parentUuid` order.
#[test]
fn claude_parent_chain_out_of_order_keeps_every_record() {
    let events = rows("claude/parent-chain-out-of-order", "session_events");
    assert!(
        events.len() >= 4,
        "all four records are indexed: {events:?}"
    );
    let uids = events
        .iter()
        .map(|event| text(event, "event_uid"))
        .collect::<BTreeSet<_>>();
    assert_eq!(uids.len(), events.len(), "event ids stay unique");
}

/// The codex rollout's observed `session_meta` reaches the catalog.
#[test]
fn codex_simple_turn_carries_session_meta() {
    let sessions = rows("codex/simple-turn", "sessions");
    assert_eq!(sessions.len(), 1, "{sessions:?}");
    let session = &sessions[0];
    assert_eq!(text(session, "session_id"), "sess_simple_1");
    assert_eq!(text(session, "cwd"), "/tmp/project");
    assert_eq!(text(session, "agent_version"), "0.121.0");
}

/// burn: `emits_compaction_event_anchored_to_preceding_turn`. The raw facts
/// here are the two turns either side of the `compacted` record.
#[test]
fn codex_compaction_keeps_both_turns() {
    let sessions = rows("codex/compaction", "sessions");
    assert_eq!(sessions.len(), 1, "{sessions:?}");
    assert_eq!(text(&sessions[0], "session_id"), "sess_codex_compact");
}

/// A rollout under `~/.codex/archived_sessions/` is discovered exactly like
/// one under `~/.codex/sessions/`: two roots, one adapter.
#[test]
fn codex_archived_rollouts_are_discovered() {
    let sessions = rows("codex/archived-session", "sessions");
    assert_eq!(sessions.len(), 1, "{sessions:?}");
    assert_eq!(text(&sessions[0], "session_id"), "sess_archived_1");
}

/// `parent_thread_id` in a subagent rollout's `session_meta` is a delegation
/// edge, and the delegated thread is evidence rather than a catalog row.
#[test]
fn codex_parent_thread_id_becomes_a_delegation_edge() {
    let relationships = rows("codex/parent-thread-id", "session_relationships");
    assert_eq!(relationships.len(), 1, "{relationships:?}");
    let edge = &relationships[0];
    assert_eq!(text(edge, "parent_session_id"), "sess_parent_thread_root");
    assert_eq!(text(edge, "child_session_id"), "sess_parent_thread_child");
    let catalog = rows("codex/parent-thread-id", "sessions")
        .iter()
        .map(|session| text(session, "session_id").to_string())
        .collect::<Vec<_>>();
    assert_eq!(catalog, vec!["sess_parent_thread_root".to_string()]);
}

/// Cursor records no timestamps at all, so every prompt in one transcript is
/// stamped from the file's mtime. The prompts themselves are the raw fact:
/// string content, block-array content and a `<user_query>` wrapper all
/// unwrap, assistant and tool records do not.
#[test]
fn cursor_transcript_yields_only_unwrapped_user_prompts() {
    let prompts = rows("cursor/prompt-transcript", "history")
        .iter()
        .map(|entry| text(entry, "prompt").to_string())
        .collect::<Vec<_>>();
    assert_eq!(
        prompts,
        vec![
            "add a retry to the client".to_string(),
            "now write the test".to_string(),
            "wrapped query".to_string(),
        ],
        "assistant text, tool uses, tool results and blank prompts are not prompts"
    );
    let entries = rows("cursor/prompt-transcript", "history");
    assert!(
        entries
            .iter()
            .all(|entry| text(entry, "project") == "/tmp/project"),
        "the project is decoded from the cursor project directory name"
    );
}

/// Grok's `summary.json` supplies the observed identity and timestamps; the
/// `synthetic_reason` turn in `chat_history.jsonl` is not a human prompt.
#[test]
fn grok_session_reads_summary_and_skips_synthetic_turns() {
    let sessions = rows("grok/full-session", "sessions");
    assert_eq!(sessions.len(), 1, "{sessions:?}");
    let session = &sessions[0];
    assert_eq!(text(session, "session_id"), "grok-00000001");
    assert_eq!(text(session, "cwd"), "/tmp/project");
    assert_eq!(text(session, "git_branch"), "main");
    let prompts = rows("grok/full-session", "history")
        .iter()
        .map(|entry| text(entry, "prompt").to_string())
        .collect::<Vec<_>>();
    assert_eq!(
        prompts,
        vec![
            "add a retry to the client".to_string(),
            "looks good, now the test".to_string(),
        ],
        "a turn carrying synthetic_reason is not a prompt"
    );
}

/// The SQLite store is the layout relayhistory reads. Both sessions are
/// catalogued and their text parts become prompts.
#[test]
fn opencode_sqlite_store_is_read() {
    let sessions = rows("opencode/sqlite-store", "sessions")
        .iter()
        .map(|session| text(session, "session_id").to_string())
        .collect::<Vec<_>>();
    assert_eq!(
        sessions,
        vec![
            "ses_sqlite_child".to_string(),
            "ses_sqlite_root".to_string()
        ]
    );
    let prompts = rows("opencode/sqlite-store", "history")
        .iter()
        .map(|entry| text(entry, "prompt").to_string())
        .collect::<BTreeSet<_>>();
    assert!(prompts.contains("add a retry to the client"), "{prompts:?}");
    assert!(prompts.contains("now write the test"), "{prompts:?}");
    assert!(prompts.contains("review the retry change"), "{prompts:?}");
}

// ---------------------------------------------------------------------------
// gaps: facts the corpus contains that relayhistory does not capture yet
//
// Each of these is un-ignored by the issue named in its attribute, which also
// regenerates the snapshots the change moves.
// ---------------------------------------------------------------------------

/// burn: `simple_turn_parses` — `requestId` and `stop_reason` are raw fields
/// on every complete Claude assistant record.
#[test]
#[ignore = "closed by #164"]
fn claude_request_id_and_stop_reason_are_captured() {
    let events = rows("claude/simple-turn", "session_events");
    let assistant = events
        .iter()
        .find(|event| text(event, "role") == "assistant")
        .expect("assistant record");
    assert_eq!(field(assistant, "request_id").as_str(), Some("req_1"));
    assert_eq!(field(assistant, "stop_reason").as_str(), Some("end_turn"));
}

/// burn: `incremental_defers_in_progress_trailing_message` — a trailing
/// assistant record with `stop_reason: null` is still being written and must
/// not be published as a completed message.
#[test]
#[ignore = "closed by #164"]
fn claude_in_progress_assistant_message_is_not_emitted() {
    let events = rows("claude/incomplete-then-complete", "session_events");
    assert!(
        events
            .iter()
            .all(|event| text(event, "message_id") != "u-asst-2"),
        "the in-progress message must not be emitted: {events:?}"
    );
}

/// burn: `compact_boundary_emits_compaction_event` — the `system` record with
/// `subtype: compact_boundary` is a marker, not a droppable row.
#[test]
#[ignore = "closed by #165"]
fn claude_compact_boundary_becomes_a_marker() {
    let markers = rows("claude/compact-boundary", "session_markers");
    assert_eq!(markers.len(), 1, "{markers:?}");
    assert_eq!(text(&markers[0], "kind"), "compaction");
    assert_eq!(text(&markers[0], "preceding_message_id"), "msg_c_1");
}

/// A `type: "summary"` record carries a durable session summary and the
/// `leafUuid` it summarizes. Today it is dropped by the `_ => {}` arm.
#[test]
#[ignore = "closed by #165"]
fn claude_summary_record_becomes_a_marker() {
    let markers = rows("claude/summary-record", "session_markers");
    assert_eq!(markers.len(), 1, "{markers:?}");
    assert_eq!(text(&markers[0], "kind"), "summary");
}

/// burn: `system_subagent_notification_emits_tool_result_event` — the
/// notification names a child session id that appears nowhere else.
#[test]
#[ignore = "closed by #165"]
fn claude_system_subagent_notification_is_recorded() {
    let markers = rows("claude/system-subagent-notification", "session_markers");
    assert!(
        markers
            .iter()
            .any(|marker| text(marker, "kind") == "subagent_completed"),
        "{markers:?}"
    );
}

/// burn: `reconcile_emits_fork_rows_when_two_files_share_source_session_id`.
#[test]
#[ignore = "closed by #170"]
fn claude_two_transcripts_sharing_a_session_id_are_a_fork() {
    let relationships = rows("claude/fork-reconciliation", "session_relationships");
    let forks = relationships
        .iter()
        .filter(|edge| text(edge, "relationship") == "fork")
        .count();
    assert_eq!(
        forks, 2,
        "both branches fork from the original: {relationships:?}"
    );
}

/// burn: `reconcile_emits_continuation_when_parent_uuid_lives_in_other_file`.
#[test]
#[ignore = "closed by #170"]
fn claude_cross_file_parent_uuid_becomes_a_continuation() {
    let relationships = rows(
        "claude/cross-file-parent-reconciliation",
        "session_relationships",
    );
    assert!(
        relationships
            .iter()
            .any(|edge| text(edge, "relationship") == "continuation"),
        "{relationships:?}"
    );
}

/// burn: `explicit_line_continuedfrom_and_fork_session_id` — the record states
/// the continuation outright.
#[test]
#[ignore = "closed by #170"]
fn claude_explicit_continued_from_session_id_becomes_a_continuation() {
    let relationships = rows(
        "claude/explicit-continuation-reconciliation",
        "session_relationships",
    );
    assert!(
        relationships
            .iter()
            .any(|edge| text(edge, "relationship") == "continuation"),
        "{relationships:?}"
    );
}

/// burn: `resume_marker_root_carries_provenance_when_in_log_id_differs` — the
/// `/resume <id>` marker names the session being resumed.
#[test]
#[ignore = "closed by #170"]
fn claude_resume_marker_links_to_the_resumed_session() {
    let relationships = rows("claude/resume-marker", "session_relationships");
    assert!(
        relationships.iter().any(|edge| {
            text(edge, "child_session_id") == "11111111-1111-1111-1111-111111111111"
                || text(edge, "parent_session_id") == "11111111-1111-1111-1111-111111111111"
        }),
        "{relationships:?}"
    );
}

/// burn's codex `session-meta-relationships` fixture states all three links on
/// the rollout's own `session_meta`.
#[test]
#[ignore = "closed by #170"]
fn codex_session_meta_relationship_ids_are_recorded() {
    let relationships = rows("codex/session-meta-relationships", "session_relationships");
    let parents = relationships
        .iter()
        .map(|edge| text(edge, "parent_session_id").to_string())
        .collect::<BTreeSet<_>>();
    assert!(parents.contains("sess_original"), "{relationships:?}");
    assert!(parents.contains("sess_previous"), "{relationships:?}");
    assert!(parents.contains("sess_fork_base"), "{relationships:?}");
}

/// burn: `measure_tool_result_populates_byte_length_and_truncation_flag` — the
/// size of a tool result is the fact that decides whether it was truncated.
#[test]
#[ignore = "closed by #171"]
fn claude_oversized_tool_result_records_its_byte_length() {
    let results = rows("claude/oversized-bash-output", "tool_results");
    assert_eq!(results.len(), 1, "{results:?}");
    assert!(
        field(&results[0], "bytes").as_i64().unwrap_or_default() > 70_000,
        "{results:?}"
    );
}

/// burn: the codex shell output fixture is the same fact on the other
/// provider.
#[test]
#[ignore = "closed by #171"]
fn codex_oversized_shell_output_records_its_byte_length() {
    let results = rows("codex/oversized-shell-output", "tool_results");
    assert_eq!(results.len(), 1, "{results:?}");
    assert!(
        field(&results[0], "bytes").as_i64().unwrap_or_default() > 70_000,
        "{results:?}"
    );
}

/// burn: `user_turn_blocks_text_and_tool_results` — three user records, the
/// middle two carrying tool_result blocks of very different sizes.
#[test]
#[ignore = "closed by #171"]
fn claude_user_turn_tool_result_blocks_are_indexed_individually() {
    let results = rows("claude/user-turn-blocks", "tool_results");
    assert_eq!(results.len(), 3, "{results:?}");
    assert!(
        results
            .iter()
            .any(|result| field(result, "is_error").as_i64() == Some(1)),
        "{results:?}"
    );
}

/// burn: `multi_block_turn_emits_one_inference_with_merged_usage` — the four
/// assistant records are one API request, so the request's usage must be
/// countable once. Today every block of the message is stamped with the same
/// `token_json`, which is the row-summing pathology burn's test names: a
/// consumer that adds `token_json` across `session_events` triples the turn.
#[test]
#[ignore = "closed by #172"]
fn claude_multi_block_turn_reports_usage_once_per_request() {
    let events = rows("claude/multi-block-turn", "session_events");
    let with_usage = events
        .iter()
        .filter(|event| !field(event, "token_json").is_null())
        .count();
    assert_eq!(
        with_usage, 1,
        "four records sharing requestId req_1 carry one usage payload: {events:?}"
    );
}

/// Codex `token_count` records carry cumulative counters that reset after a
/// compaction; burn reads the per-turn delta out of that sequence. relayhistory
/// keeps no per-request usage for codex at all today.
#[test]
#[ignore = "closed by #172"]
fn codex_cumulative_token_counters_are_recorded_per_turn() {
    let events = rows("codex/compaction", "session_events");
    let usages = events
        .iter()
        .filter(|event| !field(event, "token_json").is_null())
        .count();
    assert_eq!(usages, 2, "one usage payload per turn: {events:?}");
}

/// burn: `opencode` legacy `storage/` JSON layout. relayhistory reads the
/// SQLite store only, so the whole legacy corpus snapshots as empty.
#[test]
#[ignore = "closed by #168"]
fn opencode_legacy_json_layout_is_read() {
    let sessions = rows("opencode/legacy-json-multi-turn", "sessions")
        .iter()
        .map(|session| text(session, "session_id").to_string())
        .collect::<BTreeSet<_>>();
    assert!(sessions.contains("ses_multi"), "{sessions:?}");
    assert!(sessions.contains("ses_child"), "{sessions:?}");
}

/// burn: OpenCode `multi-turn`'s `ses_child` states its parent through
/// `parentID`; the SQLite store says the same thing in `session.parent_id`.
#[test]
#[ignore = "closed by #168"]
fn opencode_child_session_parent_link_is_recorded() {
    let relationships = rows("opencode/sqlite-store", "session_relationships");
    assert_eq!(relationships.len(), 1, "{relationships:?}");
    assert_eq!(
        text(&relationships[0], "parent_session_id"),
        "ses_sqlite_root"
    );
    assert_eq!(
        text(&relationships[0], "child_session_id"),
        "ses_sqlite_child"
    );
}

/// OpenCode records provider, model and a full token payload per assistant
/// message. relayhistory captures prompts only.
#[test]
#[ignore = "closed by #168"]
fn opencode_assistant_messages_carry_model_and_tokens() {
    let events = rows("opencode/sqlite-store", "session_events");
    let models = events
        .iter()
        .filter_map(|event| field(event, "model").as_str())
        .collect::<BTreeSet<_>>();
    assert!(models.contains("claude-sonnet-4-5"), "{models:?}");
    assert!(models.contains("claude-opus-4-5"), "{models:?}");
}

/// Cursor records assistant text, tool uses and tool results that never reach
/// `session_events` today.
#[test]
#[ignore = "closed by #166"]
fn cursor_assistant_and_tool_records_reach_session_events() {
    let events = rows("cursor/prompt-transcript", "session_events");
    assert!(
        events.iter().any(|event| text(event, "kind") == "tool_use"),
        "{events:?}"
    );
    assert!(
        events
            .iter()
            .any(|event| text(event, "role") == "assistant"),
        "{events:?}"
    );
}

/// Grok's chat history carries assistant text, tool uses, tool results and
/// real per-record timestamps.
#[test]
fn grok_events_and_real_timestamps_reach_session_events() {
    let events = rows("grok/full-session", "session_events");
    assert!(
        events.iter().any(|event| text(event, "kind") == "tool_use"),
        "{events:?}"
    );
    let prompts = rows("grok/full-session", "history");
    assert!(
        prompts
            .iter()
            .all(|entry| field(entry, "timestamp_ms").as_i64() != Some(FIXTURE_MTIME_MS + 1)),
        "prompt timestamps come from the record, not from `created_at + index`: {prompts:?}"
    );
}

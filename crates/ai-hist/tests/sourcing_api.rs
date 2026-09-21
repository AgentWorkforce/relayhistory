//! The sourcing SDK, exercised the way an out-of-workspace crate uses it.
//!
//! This file has no `[[test]]` entry in `Cargo.toml`, so it builds on the
//! crate's *default* features: no `unstable-internal`, no raw connection. It
//! reaches the store through `SessionStore` and the structs re-exported
//! beside it, and nothing else — which is exactly what #178 promises a
//! consumer.
//!
//! Each corpus fixture is staged into an isolated provider home, taken
//! through `open` → `sync` → `sessions` → `session`, and the resulting
//! `SessionEvidence` is compared field for field with the parser
//! characterization snapshot `tests/fixture_corpus.rs` committed for it. The
//! snapshot is produced by the internal APIs; agreeing with it is what makes
//! the facade a projection of the store rather than a second reading of it.

use ai_hist::{
    CatalogQuery, DiscoveryState, EvidenceKind, HydrateOptions, HydrateStatus, ProviderRoots,
    RelationshipSide, Role, SessionEvidence, SessionQuery, SessionRef, SessionStore, Source,
    StoreOptions, SyncOptions, TickTrigger, WatchOptions,
};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

// ---------------------------------------------------------------------------
// staging
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
enum Layout {
    ClaudeTranscript,
    CodexRollout,
    OpencodeSqlite,
}

struct Fixture {
    source: Source,
    name: &'static str,
    layout: Layout,
    file: &'static str,
}

/// Single-transcript fixtures whose timestamps come from the records, so the
/// snapshot agrees with a store synced from any staging of the same bytes.
const CORPUS: &[Fixture] = &[
    Fixture {
        source: Source::Claude,
        name: "simple-turn",
        layout: Layout::ClaudeTranscript,
        file: "claude/simple-turn.jsonl",
    },
    Fixture {
        source: Source::Claude,
        name: "multi-block-turn",
        layout: Layout::ClaudeTranscript,
        file: "claude/multi-block-turn.jsonl",
    },
    Fixture {
        source: Source::Claude,
        name: "user-turn-blocks",
        layout: Layout::ClaudeTranscript,
        file: "claude/user-turn-blocks.jsonl",
    },
    Fixture {
        source: Source::Claude,
        name: "compact-boundary",
        layout: Layout::ClaudeTranscript,
        file: "claude/compact-boundary.jsonl",
    },
    Fixture {
        source: Source::Claude,
        name: "files-touched",
        layout: Layout::ClaudeTranscript,
        file: "claude/files-touched.jsonl",
    },
    Fixture {
        source: Source::Claude,
        name: "system-subagent-notification",
        layout: Layout::ClaudeTranscript,
        file: "claude/system-subagent-notification.jsonl",
    },
    Fixture {
        source: Source::Claude,
        name: "missing-output-tokens",
        layout: Layout::ClaudeTranscript,
        file: "claude/missing-output-tokens.jsonl",
    },
    Fixture {
        source: Source::Codex,
        name: "simple-turn",
        layout: Layout::CodexRollout,
        file: "codex/simple-turn.jsonl",
    },
    Fixture {
        source: Source::Codex,
        name: "with-tool-call",
        layout: Layout::CodexRollout,
        file: "codex/with-tool-call.jsonl",
    },
    Fixture {
        source: Source::Codex,
        name: "compaction",
        layout: Layout::CodexRollout,
        file: "codex/compaction.jsonl",
    },
    Fixture {
        source: Source::Codex,
        name: "two-requests-one-turn",
        layout: Layout::CodexRollout,
        file: "codex/two-requests-one-turn.jsonl",
    },
    Fixture {
        source: Source::OpenCode,
        name: "sqlite-store",
        layout: Layout::OpencodeSqlite,
        file: "opencode/sqlite-store.sql",
    },
];

fn fixtures_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
}

fn snapshot(fixture: &Fixture) -> Value {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("snapshots")
        .join(fixture.source.as_str())
        .join(format!("{}.json", fixture.name));
    serde_json::from_str(&fs::read_to_string(&path).expect("committed snapshot"))
        .expect("snapshot is JSON")
}

/// Stage one fixture into `home` and return the transcript path, when the
/// fixture is a single file the store can be asked to hydrate by path.
fn stage(fixture: &Fixture, home: &Path) -> Option<PathBuf> {
    let from = fixtures_root().join(fixture.file);
    match fixture.layout {
        Layout::ClaudeTranscript => {
            let project = home.join(".claude/projects/corpus");
            fs::create_dir_all(&project).unwrap();
            let to = project.join(from.file_name().unwrap());
            fs::copy(&from, &to).unwrap();
            Some(to)
        }
        Layout::CodexRollout => {
            let day = home.join(".codex/sessions/2026/04/20");
            fs::create_dir_all(&day).unwrap();
            let stem = from.file_stem().unwrap().to_str().unwrap();
            let to = day.join(format!("rollout-2026-04-20T00-00-00-{stem}.jsonl"));
            fs::copy(&from, &to).unwrap();
            Some(to)
        }
        Layout::OpencodeSqlite => {
            let db_path = home.join(".local/share/opencode/opencode.db");
            fs::create_dir_all(db_path.parent().unwrap()).unwrap();
            let db = rusqlite::Connection::open(&db_path).unwrap();
            db.execute_batch(&fs::read_to_string(&from).unwrap())
                .unwrap();
            None
        }
    }
}

/// The provider roots under a fixture home, resolved without consulting the
/// environment: a `CODEX_HOME` or `CLAUDE_CONFIG_DIR` set on the machine
/// running this suite must not pull real sessions into a fixture's store.
fn roots_under(home: &Path) -> ProviderRoots {
    ProviderRoots::from_home(
        home.to_path_buf(),
        home.join(".local/share/opencode/opencode.db"),
    )
}

// `StoreOptions` is `#[non_exhaustive]`, so an outside crate builds it field
// by field; this test is written as that crate.
#[allow(clippy::field_reassign_with_default)]
fn open_with_roots(home: &Path, roots: ProviderRoots) -> SessionStore {
    let mut options = StoreOptions::default();
    options.db_path = Some(home.join("ai-history.db"));
    options.roots = Some(roots);
    SessionStore::open(options).expect("open")
}

fn open(home: &Path) -> SessionStore {
    open_with_roots(home, roots_under(home))
}

fn synced(fixture: &Fixture) -> (tempfile::TempDir, SessionStore, Option<PathBuf>) {
    let dir = tempfile::tempdir().unwrap();
    let transcript = stage(fixture, dir.path());
    let store = open(dir.path());
    let report = store.sync(SyncOptions::default()).expect("sync");
    assert!(report.swept, "a cold store always sweeps");
    (dir, store, transcript)
}

fn rows<'a>(snapshot: &'a Value, table: &str) -> &'a [Value] {
    snapshot[table].as_array().expect("snapshot table")
}

fn text<'a>(row: &'a Value, field: &str) -> Option<&'a str> {
    row[field].as_str()
}

fn int(row: &Value, field: &str) -> Option<i64> {
    row[field].as_i64()
}

fn opt_string(value: &Option<String>) -> Option<&str> {
    value.as_deref()
}

// ---------------------------------------------------------------------------
// the corpus, field for field
// ---------------------------------------------------------------------------

/// Compare one session's evidence with the snapshot rows for that session.
fn assert_matches_snapshot(fixture: &Fixture, evidence: &SessionEvidence, snapshot: &Value) {
    let sid = evidence.session.session_id.as_str();
    let source = fixture.source.as_str();
    let mine =
        |row: &Value| text(row, "session_id") == Some(sid) && text(row, "source") == Some(source);
    let context = format!("{}/{} session {sid}", source, fixture.name);

    // -- catalog row --------------------------------------------------------
    let row = rows(snapshot, "sessions")
        .iter()
        .find(|row| mine(row))
        .unwrap_or_else(|| panic!("{context}: snapshot has no catalog row"));
    let session = &evidence.session;
    assert_eq!(session.source, fixture.source, "{context}");
    assert_eq!(opt_string(&session.cwd), text(row, "cwd"), "{context} cwd");
    assert_eq!(
        opt_string(&session.git_branch),
        text(row, "git_branch"),
        "{context} branch"
    );
    assert_eq!(
        session.first_activity_ms,
        int(row, "first_activity_ms"),
        "{context} first"
    );
    assert_eq!(
        session.last_activity_ms,
        int(row, "last_activity_ms"),
        "{context} last"
    );
    assert_eq!(
        opt_string(&session.first_prompt),
        text(row, "first_prompt"),
        "{context} prompt"
    );
    assert_eq!(
        opt_string(&session.last_assistant_text),
        text(row, "last_assistant_text"),
        "{context} last text"
    );
    let models: Vec<String> = text(row, "models_json")
        .map(|raw| serde_json::from_str(raw).unwrap())
        .unwrap_or_default();
    assert_eq!(session.models, models, "{context} models");
    assert_eq!(
        opt_string(&session.originator),
        text(row, "originator"),
        "{context}"
    );
    assert_eq!(
        opt_string(&session.agent_version),
        text(row, "agent_version"),
        "{context}"
    );
    assert_eq!(
        opt_string(&session.repo_url),
        text(row, "repo_url"),
        "{context}"
    );
    assert_eq!(
        opt_string(&session.initial_commit),
        text(row, "initial_commit"),
        "{context}"
    );
    let state = match session.discovery_state {
        DiscoveryState::Full => "full",
        DiscoveryState::Shallow => "shallow",
        _ => unreachable!(),
    };
    assert_eq!(Some(state), text(row, "discovery_state"), "{context} state");
    assert!(
        session.project_key.is_some(),
        "{context}: every row with a cwd has a project key"
    );

    // -- prompts ------------------------------------------------------------
    let history: Vec<&Value> = rows(snapshot, "history")
        .iter()
        .filter(|row| mine(row))
        .collect();
    assert_eq!(
        evidence.prompts.len(),
        history.len(),
        "{context} prompt count"
    );
    for (prompt, row) in evidence.prompts.iter().zip(history) {
        assert_eq!(opt_string(&prompt.prompt), text(row, "prompt"), "{context}");
        assert_eq!(
            prompt.timestamp_ms,
            int(row, "timestamp_ms").unwrap(),
            "{context}"
        );
        assert_eq!(
            opt_string(&prompt.project),
            text(row, "project"),
            "{context}"
        );
    }

    // -- events, through messages and blocks --------------------------------
    let events: BTreeMap<&str, &Value> = rows(snapshot, "session_events")
        .iter()
        .filter(|row| mine(row))
        .map(|row| (text(row, "event_uid").unwrap(), row))
        .collect();
    let mut seen = 0;
    for message in &evidence.messages {
        for block in &message.blocks {
            seen += 1;
            let row = events
                .get(block.event_uid.as_str())
                .unwrap_or_else(|| panic!("{context}: block {} not in snapshot", block.event_uid));
            assert_eq!(
                opt_string(&block.text),
                text(row, "text"),
                "{context} {}",
                block.event_uid
            );
            assert_eq!(
                block.text_bytes,
                text(row, "text").map(|text| text.len() as i64),
                "{context} {}",
                block.event_uid
            );
            let kind = match block.kind {
                ai_hist::BlockKind::Text => "text",
                ai_hist::BlockKind::Thinking => "thinking",
                ai_hist::BlockKind::ToolUse => "tool_use",
                ai_hist::BlockKind::ToolResult => "tool_result",
                _ => unreachable!(),
            };
            assert_eq!(
                Some(kind),
                text(row, "kind"),
                "{context} {}",
                block.event_uid
            );
            let role = match block.role {
                Role::User => "user",
                Role::Assistant => "assistant",
                Role::ToolResult => "tool_result",
                _ => unreachable!(),
            };
            assert_eq!(
                Some(role),
                text(row, "role"),
                "{context} {}",
                block.event_uid
            );
            assert_eq!(
                block.ts_ms,
                int(row, "ts_ms").unwrap(),
                "{context} {}",
                block.event_uid
            );
            assert_eq!(
                opt_string(&message.message_id),
                text(row, "message_id"),
                "{context}"
            );
            assert_eq!(opt_string(&message.cwd), text(row, "cwd"), "{context}");
            assert_eq!(
                opt_string(&message.project),
                text(row, "project"),
                "{context}"
            );
            if let Some(model) = text(row, "model") {
                assert_eq!(
                    message.model.as_deref(),
                    Some(model),
                    "{context} {}",
                    block.event_uid
                );
            }
            if let Some(raw) = text(row, "token_json") {
                // Claude copies one message's usage onto every block; the
                // message carries it once, and only when the copies agree.
                if message.usage_error.is_none() {
                    assert_eq!(
                        message.raw_usage(),
                        Some(raw),
                        "{context} {}",
                        block.event_uid
                    );
                }
            }
        }
    }
    assert_eq!(
        seen,
        events.len(),
        "{context}: every event is a block of exactly one message"
    );

    // -- tool results carry the event's own row ----------------------------
    for result in &evidence.tool_results {
        let row = events[result.event_uid.as_str()];
        assert_eq!(
            Some("tool_result"),
            text(row, "kind"),
            "{context} {}",
            result.event_uid
        );
        assert_eq!(opt_string(&result.text), text(row, "text"), "{context}");
    }
    let stored_results = events
        .values()
        .filter(|row| text(row, "kind") == Some("tool_result"))
        .count();
    assert_eq!(
        evidence.tool_results.len(),
        stored_results,
        "{context} tool results"
    );

    // -- tool calls ----------------------------------------------------------
    let calls: BTreeMap<&str, &Value> = rows(snapshot, "tool_calls")
        .iter()
        .filter(|row| mine(row))
        .map(|row| (text(row, "tool_use_id").unwrap(), row))
        .collect();
    assert_eq!(
        evidence.tool_calls.len(),
        calls.len(),
        "{context} tool calls"
    );
    for call in &evidence.tool_calls {
        let row = calls[call.tool_use_id.as_str()];
        assert_eq!(call.name, text(row, "name").unwrap(), "{context}");
        assert_eq!(opt_string(&call.target), text(row, "target"), "{context}");
        assert_eq!(call.raw_args(), text(row, "args_json"), "{context}");
        assert_eq!(
            call.args,
            text(row, "args_json").map(|raw| serde_json::from_str::<Value>(raw).unwrap()),
            "{context}: args arrive parsed"
        );
        assert_eq!(
            call.is_error,
            int(row, "is_error").map(|flag| flag != 0),
            "{context}"
        );
        assert_eq!(call.ts_ms, int(row, "ts_ms"), "{context}");
        assert_eq!(
            opt_string(&call.message_id),
            text(row, "message_id"),
            "{context}"
        );
    }

    // -- file edits ----------------------------------------------------------
    let edits: BTreeMap<&str, &Value> = rows(snapshot, "file_edits")
        .iter()
        .filter(|row| mine(row))
        .map(|row| (text(row, "tool_use_id").unwrap(), row))
        .collect();
    assert_eq!(
        evidence.file_edits.len(),
        edits.len(),
        "{context} file edits"
    );
    for edit in &evidence.file_edits {
        let row = edits[edit.tool_use_id.as_str()];
        assert_eq!(edit.file_path, text(row, "file_path").unwrap(), "{context}");
        assert_eq!(
            opt_string(&edit.tool_name),
            text(row, "tool_name"),
            "{context}"
        );
        assert_eq!(edit.lines_added, int(row, "lines_added"), "{context}");
        assert_eq!(edit.lines_removed, int(row, "lines_removed"), "{context}");
        assert_eq!(
            edit.raw_structured_patch(),
            text(row, "structured_patch_json"),
            "{context}"
        );
        assert_eq!(
            edit.structured_patch,
            text(row, "structured_patch_json")
                .map(|raw| serde_json::from_str::<Value>(raw).unwrap()),
            "{context}: the patch arrives parsed"
        );
        assert_eq!(edit.ts_ms, int(row, "ts_ms"), "{context}");
        assert_eq!(
            opt_string(&edit.message_id),
            text(row, "message_id"),
            "{context}"
        );
    }

    // -- relationships, from the parent side --------------------------------
    let as_parent: Vec<&Value> = rows(snapshot, "session_relationships")
        .iter()
        .filter(|row| {
            text(row, "parent_session_id") == Some(sid) && text(row, "source") == Some(source)
        })
        .filter(|row| {
            matches!(
                text(row, "relationship"),
                Some("delegated" | "materialized_local")
            )
        })
        .collect();
    let parent_edges: Vec<_> = evidence
        .relationships
        .iter()
        .filter(|edge| edge.side == RelationshipSide::Parent)
        .collect();
    assert_eq!(parent_edges.len(), as_parent.len(), "{context} delegations");
    for row in as_parent {
        let uid = text(row, "relationship_uid").unwrap();
        let edge = parent_edges
            .iter()
            .find(|edge| edge.relationship_uid == uid)
            .unwrap_or_else(|| panic!("{context}: relationship {uid} missing"));
        assert_eq!(
            opt_string(&edge.child_session_id),
            text(row, "child_session_id"),
            "{context}"
        );
        assert_eq!(
            edge.relationship,
            text(row, "relationship").unwrap(),
            "{context}"
        );
        assert_eq!(
            edge.identity_status,
            text(row, "identity_status").unwrap(),
            "{context}"
        );
        assert_eq!(
            opt_string(&edge.child_agent_type),
            text(row, "child_agent_type"),
            "{context}"
        );
        assert_eq!(edge.spawned_at_ms, int(row, "spawned_at_ms"), "{context}");
    }

    // -- coverage is the source's declared ceiling ---------------------------
    assert_eq!(
        evidence.coverage,
        fixture.source.capabilities().evidence_kinds,
        "{context}"
    );
    assert!(
        evidence.loaded.contains(&EvidenceKind::SessionEvent),
        "{context}"
    );
}

#[test]
fn every_corpus_fixture_reads_back_through_the_facade_as_the_snapshot_says() {
    for fixture in CORPUS {
        let snapshot = snapshot(fixture);
        let (_dir, store, _) = synced(fixture);

        let expected: Vec<&str> = rows(&snapshot, "sessions")
            .iter()
            .map(|row| text(row, "session_id").unwrap())
            .collect();
        let mut listed: Vec<String> = store
            .sessions(CatalogQuery::default())
            .map(|row| row.expect("catalog row").session_id)
            .collect();
        listed.sort();
        assert_eq!(
            listed, expected,
            "{}/{} catalog",
            fixture.source, fixture.name
        );

        for session_id in expected {
            let evidence = store
                .session(
                    &SessionRef::id(fixture.source, session_id),
                    SessionQuery::default(),
                )
                .expect("read")
                .unwrap_or_else(|| {
                    panic!(
                        "{}/{}: {session_id} is catalogued",
                        fixture.source, fixture.name
                    )
                });
            assert_matches_snapshot(fixture, &evidence, &snapshot);

            // Round trip: the struct set is what a consumer persists and
            // sends across process boundaries.
            let json = serde_json::to_string(&evidence).expect("serialize");
            let back: SessionEvidence = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(
                back, evidence,
                "{}/{} {session_id} serde",
                fixture.source, fixture.name
            );

            // The hash-only read keeps every measurement and no text.
            let mut hashed = SessionQuery::default();
            hashed.include_text = false;
            let hashed = store
                .session(&SessionRef::id(fixture.source, session_id), hashed)
                .expect("read")
                .expect("catalogued");
            assert_eq!(hashed.messages.len(), evidence.messages.len());
            for (with, without) in evidence.messages.iter().zip(&hashed.messages) {
                for (a, b) in with.blocks.iter().zip(&without.blocks) {
                    assert!(b.text.is_none());
                    assert_eq!(a.text_bytes, b.text_bytes);
                }
                assert_eq!(with.usage, without.usage);
            }
            assert_eq!(hashed.tool_results.len(), evidence.tool_results.len());
            for (a, b) in evidence.tool_results.iter().zip(&hashed.tool_results) {
                assert!(b.text.is_none());
                assert_eq!(a.payload_bytes, b.payload_bytes);
                assert_eq!(a.payload_hash, b.payload_hash);
            }
            for prompt in &hashed.prompts {
                assert!(prompt.prompt.is_none());
                assert!(
                    prompt.prompt_hash.is_some(),
                    "the stored hash is read, not the text"
                );
            }
            assert!(hashed.session.first_prompt.is_none());
        }
    }
}

// ---------------------------------------------------------------------------
// the reads the acceptance list names
// ---------------------------------------------------------------------------

fn only_session(store: &SessionStore, source: Source) -> SessionEvidence {
    let mut rows: Vec<_> = store
        .sessions(CatalogQuery::default())
        .map(|row| row.unwrap())
        .filter(|row| row.source == source)
        .collect();
    assert_eq!(rows.len(), 1, "one session in the fixture");
    let row = rows.remove(0);
    store
        .session(&row.session_ref(), SessionQuery::default())
        .unwrap()
        .unwrap()
}

#[test]
fn usage_request_id_and_stop_reason_arrive_typed_on_the_message() {
    let (_dir, store, _) = synced(&CORPUS[1]); // claude/multi-block-turn
    let evidence = only_session(&store, Source::Claude);
    let assistant: Vec<_> = evidence
        .messages
        .iter()
        .filter(|message| message.role == Role::Assistant)
        .collect();
    // Claude writes one API response as several records, each with its own
    // `uuid` (the ledger's `message_id`), all sharing the provider's message
    // id and request id.
    assert!(assistant.len() > 1, "four records, four ledger messages");
    let request_id = assistant[0]
        .request_id
        .clone()
        .expect("Claude's requestId is read");
    let provider_message_id = assistant[0]
        .provider_message_id
        .clone()
        .expect("message.id");
    for message in &assistant {
        assert_eq!(message.request_id.as_deref(), Some(request_id.as_str()));
        assert_eq!(
            message.provider_message_id.as_deref(),
            Some(provider_message_id.as_str())
        );
    }
    // Only the first record carries the usage payload; the request groups
    // them and counts it once.
    assert!(assistant.iter().any(|message| message.usage.is_some()));
    assert_eq!(
        evidence.requests.len(),
        1,
        "one request, not one per record"
    );
    let mut ids: Vec<String> = assistant
        .iter()
        .map(|m| m.message_id.clone().unwrap())
        .collect();
    ids.sort();
    let mut grouped = evidence.requests[0].message_ids.clone();
    grouped.sort();
    assert_eq!(grouped, ids);
    let summary = evidence
        .usage
        .as_ref()
        .expect("a session with usage has a rollup");
    assert_eq!(summary.request_count, 1);

    let (_dir, store, _) = synced(&CORPUS[0]); // claude/simple-turn
    let evidence = only_session(&store, Source::Claude);
    let assistant = &evidence.messages[1];
    assert_eq!(assistant.stop_reason.as_deref(), Some("end_turn"));
    assert_eq!(assistant.usage.as_ref().unwrap().input_tokens, 10);
    assert_eq!(assistant.usage.as_ref().unwrap().cache_read_tokens, 500);
}

#[test]
fn markers_carry_the_compaction_boundary() {
    let (_dir, store, _) = synced(&CORPUS[3]); // claude/compact-boundary
    let evidence = only_session(&store, Source::Claude);
    let kinds: Vec<&str> = evidence
        .markers
        .iter()
        .map(|marker| marker.kind.as_str())
        .collect();
    assert!(
        kinds.iter().any(|kind| kind.contains("compact")),
        "a compaction marker is stored and read back: {kinds:?}"
    );
    for marker in &evidence.markers {
        assert_eq!(
            marker.payload.is_some(),
            marker.raw_payload().is_some(),
            "a stored payload is JSON and arrives parsed"
        );
    }

    let (_dir, store, _) = synced(&CORPUS[9]); // codex/compaction
    let evidence = only_session(&store, Source::Codex);
    assert!(
        !evidence.markers.is_empty(),
        "Codex lifecycle rows are markers too"
    );
}

#[test]
fn tool_results_carry_measured_payload_bytes() {
    let (_dir, store, _) = synced(&CORPUS[2]); // claude/user-turn-blocks
    let evidence = only_session(&store, Source::Claude);
    assert!(!evidence.tool_results.is_empty());
    for result in &evidence.tool_results {
        assert!(
            result.tool_use_id.is_some(),
            "every Claude result names its call"
        );
        assert!(
            result.payload_bytes.is_some(),
            "bytes are measured, not guessed"
        );
        assert!(result.payload_hash.is_some());
        assert!(result.event_index.is_some());
        assert!(result.result_status.is_some());
    }
    assert!(
        evidence
            .tool_results
            .iter()
            .any(|result| result.result_status.as_deref() == Some("errored")),
        "one of the results errored"
    );
    assert!(!evidence.user_turns.is_empty());
    let blocks: usize = evidence
        .user_turns
        .iter()
        .map(|turn| turn.blocks.len())
        .sum();
    assert!(blocks >= evidence.tool_results.len());
}

#[test]
fn continuity_and_delegation_are_read_as_relationships() {
    let (_dir, store, _) = synced(&CORPUS[5]); // claude/system-subagent-notification
    let evidence = only_session(&store, Source::Claude);
    let notification = evidence
        .tool_results
        .iter()
        .find(|result| result.event_source.as_deref() == Some("subagent_notification"))
        .expect("the system notification is a tool result");
    assert!(notification.subagent_session_id.is_some() || notification.agent_id.is_some());

    let (_dir, store, _) = synced(&CORPUS[11]); // opencode/sqlite-store
    let mut parents = 0;
    let mut children = 0;
    for row in store.sessions(CatalogQuery::default()) {
        let row = row.unwrap();
        let evidence = store
            .session(&row.session_ref(), SessionQuery::default())
            .unwrap()
            .unwrap();
        parents += evidence
            .relationships
            .iter()
            .filter(|edge| edge.side == RelationshipSide::Parent)
            .count();
        children += evidence
            .relationships
            .iter()
            .filter(|edge| edge.side == RelationshipSide::Child)
            .count();
    }
    assert_eq!(parents, 1, "the child session names its parent");
    assert_eq!(children, 1, "and the child sees it from its side");
}

#[test]
fn kinds_skip_the_tables_a_consumer_does_not_want() {
    let (_dir, store, _) = synced(&CORPUS[8]); // codex/with-tool-call
    let row = store
        .sessions(CatalogQuery::default())
        .next()
        .unwrap()
        .unwrap();
    let mut query = SessionQuery::default();
    query.kinds = Some(vec![EvidenceKind::ToolCall, EvidenceKind::FileEdit]);
    let evidence = store.session(&row.session_ref(), query).unwrap().unwrap();
    assert!(!evidence.tool_calls.is_empty());
    assert!(!evidence.file_edits.is_empty());
    assert!(evidence.messages.is_empty());
    assert!(evidence.requests.is_empty());
    assert!(evidence.usage.is_none());
    assert_eq!(
        evidence.loaded,
        vec![EvidenceKind::ToolCall, EvidenceKind::FileEdit]
    );
    assert!(evidence.coverage.contains(&EvidenceKind::SessionEvent));
}

#[test]
fn a_session_can_be_hydrated_by_id_and_by_transcript_path() {
    let (dir, store, transcript) = synced(&CORPUS[0]); // claude/simple-turn
    let row = store
        .sessions(CatalogQuery::default())
        .next()
        .unwrap()
        .unwrap();

    let by_id = store
        .hydrate(&row.session_ref(), HydrateOptions::default())
        .expect("hydrate by id");
    assert_eq!(by_id.session, row.session_ref());
    assert!(matches!(
        by_id.status,
        HydrateStatus::Hydrated | HydrateStatus::Unchanged
    ));
    assert!(by_id.capability.is_some());
    assert!(by_id.coverage.contains(&EvidenceKind::SessionEvent));

    let path = transcript.expect("a single-file fixture");
    let by_path = store
        .hydrate(
            &SessionRef::path(Source::Claude, &path),
            HydrateOptions::default(),
        )
        .expect("hydrate by path");
    assert_eq!(
        by_path.session,
        row.session_ref(),
        "the path resolves to the same session"
    );
    assert!(matches!(
        by_path.status,
        HydrateStatus::Hydrated | HydrateStatus::Unchanged
    ));

    // A transcript that is not there is an answer, not an error.
    let missing = store
        .hydrate(
            &SessionRef::path(Source::Claude, dir.path().join("gone.jsonl")),
            HydrateOptions::default(),
        )
        .expect("missing is reported");
    assert_eq!(missing.status, HydrateStatus::Missing);

    // And the path form reads the session back too.
    let evidence = store
        .session(
            &SessionRef::path(Source::Claude, &path),
            SessionQuery::default(),
        )
        .unwrap()
        .expect("catalogued by path");
    assert_eq!(evidence.session.session_id, row.session_id);
}

/// A changed transcript re-hydrates as `Updated` — by id and by path — with
/// the new evidence readable, and one that has not changed as `Unchanged`.
#[test]
fn re_hydrating_a_changed_transcript_reports_updated() {
    let (_dir, store, transcript) = synced(&CORPUS[0]); // claude/simple-turn
    let path = transcript.expect("a single-file fixture");
    let by_id = store
        .sessions(CatalogQuery::default())
        .next()
        .unwrap()
        .unwrap()
        .session_ref();
    let prompts = |store: &SessionStore| {
        store
            .session(&by_id, SessionQuery::default())
            .unwrap()
            .unwrap()
            .prompts
            .len()
    };
    assert_eq!(prompts(&store), 1);

    // The sweep catalogued and indexed it; the first *targeted* hydration
    // writes the session's own checkpoint and reports a first ingestion, and
    // the one after that finds nothing moved.
    let first = store.hydrate(&by_id, HydrateOptions::default()).unwrap();
    assert_eq!(first.status, HydrateStatus::Hydrated, "{first:?}");
    let same = store.hydrate(&by_id, HydrateOptions::default()).unwrap();
    assert_eq!(same.status, HydrateStatus::Unchanged, "{same:?}");

    let append = |uuid: &str, parent: &str, text: &str, ts: &str| {
        let record = format!(
            "{{\"parentUuid\":\"{parent}\",\"isSidechain\":false,\"promptId\":\"p-{uuid}\",\
             \"type\":\"user\",\"message\":{{\"role\":\"user\",\"content\":\"{text}\"}},\
             \"uuid\":\"{uuid}\",\"timestamp\":\"{ts}\",\"cwd\":\"/tmp/project\",\
             \"sessionId\":\"11111111-1111-1111-1111-111111111111\",\"version\":\"2.1.96\"}}\n"
        );
        let mut file = fs::OpenOptions::new().append(true).open(&path).unwrap();
        std::io::Write::write_all(&mut file, record.as_bytes()).unwrap();
    };

    append("u-user-2", "u-asst-1", "again", "2026-04-20T00:00:02.000Z");
    let updated = store.hydrate(&by_id, HydrateOptions::default()).unwrap();
    assert_eq!(updated.status, HydrateStatus::Updated, "{updated:?}");
    assert_eq!(prompts(&store), 2, "the appended prompt is readable");

    append(
        "u-user-3",
        "u-user-2",
        "once more",
        "2026-04-20T00:00:03.000Z",
    );
    let by_path = store
        .hydrate(
            &SessionRef::path(Source::Claude, &path),
            HydrateOptions::default(),
        )
        .unwrap();
    assert_eq!(by_path.status, HydrateStatus::Updated, "{by_path:?}");
    assert_eq!(by_path.session, by_id);
    assert_eq!(prompts(&store), 3);

    let settled = store.hydrate(&by_id, HydrateOptions::default()).unwrap();
    assert_eq!(settled.status, HydrateStatus::Unchanged);
}

/// A hydration's coverage is the same declaration `session()` reports for
/// the source, markers included, not the adapter list the engine narrows
/// its own capability classification from.
#[test]
fn hydration_coverage_agrees_with_the_session_read() {
    let (_dir, store, _) = synced(&CORPUS[3]); // claude/compact-boundary
    let row = store
        .sessions(CatalogQuery::default())
        .next()
        .unwrap()
        .unwrap();
    let report = store
        .hydrate(&row.session_ref(), HydrateOptions::default())
        .unwrap();
    assert!(
        report.coverage.contains(&EvidenceKind::SessionMarker),
        "the parse that stored the compaction marker covers markers: {:?}",
        report.coverage
    );
    let evidence = store
        .session(&row.session_ref(), SessionQuery::default())
        .unwrap()
        .unwrap();
    assert_eq!(evidence.coverage, report.coverage);
    assert!(!evidence.markers.is_empty());

    // The request's narrowing still applies: without related transcripts the
    // relationship kind is not claimed.
    let mut narrow = HydrateOptions::default();
    narrow.include_related = false;
    let narrowed = store.hydrate(&row.session_ref(), narrow).unwrap();
    assert!(!narrowed.coverage.contains(&EvidenceKind::Relationship));
    assert!(narrowed.coverage.contains(&EvidenceKind::SessionMarker));
}

#[test]
fn a_second_sync_over_unchanged_sources_reports_nothing_changed() {
    let (_dir, store, _) = synced(&CORPUS[7]); // codex/simple-turn
    let again = store.sync(SyncOptions::default()).expect("second sync");
    assert!(!again.swept, "the stat-only fingerprint matched");
    assert!(again.changed.is_empty());

    let mut forced = SyncOptions::default();
    forced.force = true;
    let forced = store.sync(forced).expect("forced sync");
    assert!(forced.swept);
    assert!(
        forced.changed.is_empty(),
        "a forced walk over the same bytes changes no row"
    );
}

#[test]
fn the_first_sync_lists_every_new_session_as_changed() {
    let dir = tempfile::tempdir().unwrap();
    stage(&CORPUS[0], dir.path());
    stage(&CORPUS[7], dir.path());
    let store = open(dir.path());
    let report = store.sync(SyncOptions::default()).unwrap();
    let mut sources: Vec<Source> = report.changed.iter().map(|r| r.source()).collect();
    sources.sort();
    assert_eq!(sources, vec![Source::Claude, Source::Codex]);
}

#[test]
fn source_capabilities_are_static_and_honest() {
    let claude = Source::Claude.capabilities();
    assert!(claude.evidence_kinds.contains(&EvidenceKind::SessionMarker));
    assert!(claude.hydrates_by_path);
    assert_eq!(claude.relationships.stable_child_identity, "sometimes");
    assert!(claude.usage_accounting.is_some());
    let home = tempfile::tempdir().unwrap();
    let roots = roots_under(home.path());
    assert!(!claude.watch_roots(&roots).is_empty());

    let relay = Source::Relay.capabilities();
    assert!(!relay.hydrates_by_path);
    assert!(relay.usage_accounting.is_none());
    assert!(relay.watch_roots(&roots).is_empty());
    assert!(relay.evidence_kinds.contains(&EvidenceKind::History));
    assert_eq!(
        Source::Codex
            .capabilities()
            .relationships
            .stable_child_identity,
        "always"
    );
}

/// Explicit roots drive every operation the same way: a session the sweep
/// catalogued from a configured Codex root is hydrated from that root, and
/// the advertised watch roots name it too. Before, `hydrate` re-derived the
/// roots from `home` alone, so a `CODEX_HOME` the sweep honoured made the
/// hydration fail with `SESSION_SOURCE_MISMATCH`.
#[test]
fn sync_hydrate_and_watch_roots_share_one_root_resolution() {
    let dir = tempfile::tempdir().unwrap();
    let mut roots = roots_under(dir.path());
    roots.codex = dir.path().join("configured-codex");
    // The rollout lives only under the configured root; `~/.codex` is empty.
    let day = roots.codex.join("sessions/2026/04/20");
    fs::create_dir_all(&day).unwrap();
    fs::copy(
        fixtures_root().join(CORPUS[7].file),
        day.join("rollout-2026-04-20T00-00-00-simple-turn.jsonl"),
    )
    .unwrap();

    let store = open_with_roots(dir.path(), roots.clone());
    assert_eq!(store.roots(), &roots);
    let report = store.sync(SyncOptions::default()).expect("sync");
    assert_eq!(report.changed.len(), 1);
    let row = store
        .sessions(CatalogQuery::default())
        .next()
        .expect("the configured root was scanned")
        .unwrap();
    assert_eq!(row.source, Source::Codex);

    let hydrated = store
        .hydrate(&row.session_ref(), HydrateOptions::default())
        .expect("hydration resolves the same configured root the sweep did");
    assert!(matches!(
        hydrated.status,
        HydrateStatus::Hydrated | HydrateStatus::Unchanged
    ));

    let watched = Source::Codex.capabilities().watch_roots(&roots);
    assert!(
        watched
            .iter()
            .any(|root| root.path.starts_with(&roots.codex)),
        "the advertised watch roots follow the configured root: {watched:?}"
    );
    assert!(!watched
        .iter()
        .any(|root| root.path.starts_with(dir.path().join(".codex"))));
}

// ---------------------------------------------------------------------------
// locks
// ---------------------------------------------------------------------------

/// An in-flight sync elsewhere makes `sync` here return `SyncLocked` within
/// the documented wait, never a silent no-op.
#[cfg(unix)]
#[test]
fn a_held_sync_lock_is_reported_not_skipped() {
    use std::os::unix::io::AsRawFd;

    let (dir, store, _) = synced(&CORPUS[0]);
    // The lock the CLI's `SyncRunLock` takes: an exclusive advisory lock on
    // `<db>.sync.lock` beside the database.
    let lock_path = dir.path().join("ai-history.db.sync.lock");
    let holder = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .unwrap();
    // SAFETY: `holder` owns the descriptor for the whole test.
    assert_eq!(unsafe { libc::flock(holder.as_raw_fd(), libc::LOCK_EX) }, 0);

    let started = std::time::Instant::now();
    let error = store
        .sync(SyncOptions::default())
        .expect_err("the lock is held");
    assert_eq!(error.code(), "SYNC_LOCKED");
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "a zero timeout tries once"
    );

    let mut patient = SyncOptions::default();
    patient.lock_timeout_ms = 300;
    let started = std::time::Instant::now();
    let error = store.sync(patient).expect_err("still held");
    let waited = started.elapsed();
    match error {
        ai_hist::Error::SyncLocked { waited_ms, .. } => {
            assert!(
                waited_ms >= 300,
                "waited the documented budget: {waited_ms}"
            );
        }
        other => panic!("expected SyncLocked, got {other}"),
    }
    assert!(waited >= Duration::from_millis(300));
    assert!(waited < Duration::from_secs(5));

    // SAFETY: as above.
    assert_eq!(unsafe { libc::flock(holder.as_raw_fd(), libc::LOCK_UN) }, 0);
    store.sync(SyncOptions::default()).expect("released");
}

// ---------------------------------------------------------------------------
// watch
// ---------------------------------------------------------------------------

#[test]
fn watch_reports_the_startup_sweep_and_a_session_that_appears() {
    let dir = tempfile::tempdir().unwrap();
    stage(&CORPUS[7], dir.path()); // codex/simple-turn
    let store = open(dir.path());

    let mut options = WatchOptions::default();
    options.use_fs_events = false;
    options.poll_interval_ms = 100;
    options.immediate = true;
    let mut watch = store.watch(options).expect("watch");

    let first = watch
        .next_timeout(Duration::from_secs(30))
        .expect("a startup tick")
        .expect("the sweep succeeds");
    assert_eq!(first.trigger, TickTrigger::Startup);
    assert!(first.swept);
    assert_eq!(first.changed.len(), 1);
    assert_eq!(first.changed[0].source(), Source::Codex);

    // A transcript appearing between polls is a change on the next sweep.
    // The Claude fixture is staged with a later mtime than the rollout so the
    // stat-only fingerprint moves.
    std::thread::sleep(Duration::from_millis(50));
    stage(&CORPUS[0], dir.path());
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut saw_claude = false;
    while std::time::Instant::now() < deadline {
        let Some(tick) = watch.next_timeout(Duration::from_secs(5)) else {
            break;
        };
        let tick = tick.expect("ticks succeed");
        assert_ne!(tick.trigger, TickTrigger::Startup);
        if tick.changed.iter().any(|r| r.source() == Source::Claude) {
            saw_claude = true;
            break;
        }
    }
    assert!(saw_claude, "the new session was reported by a poll tick");

    let stop = watch.stopper();
    stop.stop();
    // The iterator drains and ends once the loop has stopped.
    let mut remaining = 0;
    while watch.next_timeout(Duration::from_secs(5)).is_some() {
        remaining += 1;
        assert!(remaining < 1_000);
    }
    drop(watch);
}

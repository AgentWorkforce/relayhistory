//! Characterization of admitted formats, not a claim about current upstream releases.
//! Reuses checked-in provider fixtures and the public conformance contract.
mod provider_support;
use ai_hist::{
    CatalogQuery, DiscoveryOptions, ProviderRoots, Role, SessionEvidence, SessionStore, Source,
    StoreOptions,
};
use provider_support::{assert_history_conformance, HistoryCase};
use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};

fn copy_tree(from: &Path, to: &Path) {
    fs::create_dir_all(to).unwrap();
    for item in fs::read_dir(from).unwrap() {
        let item = item.unwrap();
        let dest = to.join(item.file_name());
        if item.file_type().unwrap().is_dir() {
            copy_tree(&item.path(), &dest);
        } else {
            fs::copy(item.path(), dest).unwrap();
        }
    }
}
fn stage(home: &Path, source: &str, name: &str) {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let descriptor: Value = serde_json::from_slice(
        &fs::read(root.join(format!("../../providers/history-sources/{source}.json"))).unwrap(),
    )
    .unwrap();
    let fixture = descriptor["fixtures"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["name"] == name)
        .unwrap();
    for file in fixture["files"].as_array().unwrap() {
        let relative = file.as_str().unwrap();
        let src = root.join("tests/fixtures").join(relative);
        let dest = match fixture["layout"].as_str().unwrap() {
            "HomeTree" => {
                copy_tree(&src, home);
                continue;
            }
            "ClaudeTranscript" => home
                .join(".claude/projects/baseline")
                .join(src.file_name().unwrap()),
            "OpencodeSqlite" => home.join(".local/share/opencode/opencode.db"),
            "DevinSqlite" if relative.ends_with(".sql") => {
                home.join(".local/share/devin/cli/sessions.db")
            }
            "DevinSqlite" => home
                .join(".local/share/devin/cli")
                .join(relative.strip_prefix("devin/").unwrap()),
            other => panic!("uncharacterized staging family: {other}"),
        };
        fs::create_dir_all(dest.parent().unwrap()).unwrap();
        if relative.ends_with(".sql") {
            rusqlite::Connection::open(dest)
                .unwrap()
                .execute_batch(&fs::read_to_string(src).unwrap())
                .unwrap();
        } else {
            fs::copy(src, dest).unwrap();
        }
    }
}
fn characterize(source: Source, name: &str, sid: &str, user: &str, assistant: &str) {
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), source.as_str(), name);
    let evidence = assert_history_conformance(
        home.path(),
        HistoryCase {
            source,
            session_id: sid,
            messages: &[(Role::User, user), (Role::Assistant, assistant)],
        },
    );
    assert_eq!(evidence.session.session_id, sid);
}
#[test]
fn claude_existing_format_repeat_acquisition() {
    characterize(
        Source::Claude,
        "simple-turn",
        "11111111-1111-1111-1111-111111111111",
        "hello",
        "Hello!",
    );
}
#[test]
fn codex_archived_format_repeat_acquisition() {
    characterize(
        Source::Codex,
        "archived-session",
        "sess_archived_1",
        "archived but still readable",
        "Yes, archived rollouts are still discovered.",
    );
}
#[test]
fn opencode_sqlite_format_repeat_acquisition() {
    characterize(
        Source::OpenCode,
        "sqlite-store",
        "ses_sqlite_child",
        "review the retry change",
        "The retry looks correct.",
    );
}
#[test]
fn cursor_admitted_transcript_repeat_acquisition() {
    characterize(
        Source::Cursor,
        "prompt-transcript",
        "cur-00000001",
        "add a retry to the client",
        "on it",
    );
}
#[test]
fn muse_main_child_format_repeat_acquisition() {
    characterize(
        Source::Muse,
        "tools-session",
        "muse-a001",
        "Add a retry helper to src/http.rs and run the tests.",
        "Added `retry` and a test; `cargo test` fails with exit code 101.",
    );
}
#[test]
fn grok_documented_events_format_repeat_acquisition() {
    characterize(
        Source::Grok,
        "events-session",
        "grok-evt-0001",
        "add a retry to the http client",
        "Reading the client and patching it.",
    );
}
#[test]
fn devin_preserved_sqlite_format_repeat_acquisition() {
    characterize(
        Source::Devin,
        "sqlite-store",
        "fixture-devin-session",
        "Rename the helper in the fixture repo.",
        "I will look at the fixture sources first.",
    );
}

#[test]
fn grok_legacy_format_repeat_acquisition() {
    characterize(
        Source::Grok,
        "full-session",
        "grok-00000001",
        "add a retry to the client",
        "Reading the client.",
    );
}

#[test]
fn grok_unified_usage_format_repeat_acquisition() {
    characterize(
        Source::Grok,
        "unified-usage",
        "grok-uni-0001",
        "summarize the retry policy",
        "The client retries three times with backoff.",
    );
}

/// The conformance oracle must reject content loss even when discovery,
/// hydration and restart still succeed. Mutate only this test's fixture home.
#[test]
#[should_panic(expected = "missing Assistant text")]
fn claude_assistant_content_loss_is_detected() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), "claude", "simple-turn");
    let directory = home.path().join(".claude/projects/baseline");
    for entry in fs::read_dir(directory).unwrap() {
        let path = entry.unwrap().path();
        let retained: Vec<_> = fs::read_to_string(&path)
            .unwrap()
            .lines()
            .filter(|line| !line.contains("Hello!"))
            .map(str::to_owned)
            .collect();
        fs::write(path, retained.join("\n") + "\n").unwrap();
    }
    assert_history_conformance(
        home.path(),
        HistoryCase {
            source: Source::Claude,
            session_id: "11111111-1111-1111-1111-111111111111",
            messages: &[(Role::User, "hello"), (Role::Assistant, "Hello!")],
        },
    );
}

/// A nonempty other-source catalog makes source-filter exclusion observable.
#[test]
fn source_filter_excludes_another_admitted_provider() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), "claude", "simple-turn");
    stage(home.path(), "grok", "events-session");
    let roots = ProviderRoots::from_home(
        home.path().to_path_buf(),
        home.path().join(".local/share/opencode/opencode.db"),
    );
    let store = SessionStore::open(
        StoreOptions::default()
            .db_path(home.path().join("filter.db"))
            .roots(roots),
    )
    .unwrap();
    store
        .discover(DiscoveryOptions::default().sources([Source::Claude, Source::Grok]))
        .unwrap();
    for source in [Source::Claude, Source::Grok] {
        let mut query = CatalogQuery::default();
        query.sources = Some(vec![source]);
        query.page_size = 1;
        let rows: Vec<_> = store.sessions(query).map(Result::unwrap).collect();
        assert!(!rows.is_empty());
        assert!(rows.iter().all(|row| row.source == source));
    }
    let mut none = CatalogQuery::default();
    none.sources = Some(vec![]);
    assert_eq!(store.sessions(none).count(), 0);
}

/// The native importer writes SQLite; the separate CLI exporter is the oracle.
fn opencode_native_capture(erase_result: bool) -> (SessionEvidence, Value) {
    let home = tempfile::tempdir().unwrap();
    let directory = home.path().join(".local/share/opencode");
    fs::create_dir_all(&directory).unwrap();
    let captures = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../plans/history-providers/fixtures/baseline/opencode-1.18.31");
    rusqlite::Connection::open(directory.join("opencode.db"))
        .unwrap()
        .execute_batch(&fs::read_to_string(captures.join("native-store.sql")).unwrap())
        .unwrap();
    if erase_result {
        rusqlite::Connection::open(directory.join("opencode.db")).unwrap().execute(
            "UPDATE part SET data=json_set(data, '$.state.output', '') WHERE id='prt_baseline11831tool'", [],
        ).unwrap();
    }
    let oracle: Value =
        serde_json::from_slice(&fs::read(captures.join("official-export.json")).unwrap()).unwrap();
    let evidence = assert_history_conformance(
        home.path(),
        HistoryCase {
            source: Source::OpenCode,
            session_id: "ses_baseline11831",
            messages: &[
                (Role::User, "Inspect the synthetic retry fixture."),
                (Role::Assistant, "The synthetic retry is bounded."),
            ],
        },
    );
    (evidence, oracle)
}
fn assert_opencode_export(evidence: &SessionEvidence, oracle: &Value) {
    for message in oracle["messages"].as_array().unwrap() {
        let role = match message["info"]["role"].as_str().unwrap() {
            "user" => Role::User,
            "assistant" => Role::Assistant,
            other => panic!("unexpected oracle role: {other}"),
        };
        for part in message["parts"].as_array().unwrap() {
            if part["type"] == "text" {
                assert!(evidence.messages.iter().any(|m| m.role == role
                    && m.blocks
                        .iter()
                        .any(|b| b.text.as_deref() == part["text"].as_str())));
            } else if part["type"] == "tool" {
                let call = evidence
                    .tool_calls
                    .iter()
                    .find(|call| Some(call.tool_use_id.as_str()) == part["callID"].as_str())
                    .unwrap();
                assert_eq!(Some(call.name.as_str()), part["tool"].as_str());
                assert_eq!(call.args.as_ref(), Some(&part["state"]["input"]));
                let result = evidence
                    .tool_results
                    .iter()
                    .find(|result| result.tool_use_id.as_deref() == part["callID"].as_str())
                    .unwrap();
                assert_eq!(
                    result.text.as_deref(),
                    part["state"]["output"].as_str(),
                    "tool result text"
                );
                assert_eq!(
                    result.result_status.as_deref(),
                    part["state"]["status"].as_str()
                );
            }
        }
    }
}

#[test]
fn opencode_1_18_31_native_writer_matches_official_export() {
    let (evidence, oracle) = opencode_native_capture(false);
    assert_opencode_export(&evidence, &oracle);
}
#[test]
#[should_panic(expected = "tool result text")]
fn opencode_native_tool_result_loss_is_detected() {
    let (evidence, oracle) = opencode_native_capture(true);
    assert_opencode_export(&evidence, &oracle);
}
#[test]
fn muse_cli_capture_matches_1_4_7_official_exporter() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), "muse", "cli-capture");
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../plans/history-providers/fixtures/baseline/muse-1.4.7/official-export-projection.json");
    let oracle: Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    let messages: Vec<_> = oracle["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|message| {
            let role = match message["role"].as_str().unwrap() {
                "user" => Role::User,
                "assistant" => Role::Assistant,
                other => panic!("unexpected oracle role: {other}"),
            };
            (role, message["text"].as_str().unwrap())
        })
        .collect();
    let evidence = assert_history_conformance(
        home.path(),
        HistoryCase {
            source: Source::Muse,
            session_id: "74747474-7474-4747-8747-747474747474",
            messages: &messages,
        },
    );
    for call in oracle["tool_calls"].as_array().unwrap() {
        let actual = evidence
            .tool_calls
            .iter()
            .find(|actual| Some(actual.tool_use_id.as_str()) == call["call_id"].as_str())
            .unwrap();
        assert_eq!(Some(actual.name.as_str()), call["name"].as_str());
        let args: Value = serde_json::from_str(call["args"].as_str().unwrap()).unwrap();
        assert_eq!(actual.args.as_ref(), Some(&args));
    }
    for result in oracle["tool_results"].as_array().unwrap() {
        assert!(evidence
            .tool_results
            .iter()
            .any(
                |actual| actual.tool_use_id.as_deref() == result["tool_call_id"].as_str()
                    && actual.text.as_deref() == result["text"].as_str()
            ));
    }
}

/// Auth-free native writer probe; expected text is injection input, not a successful official reader.
#[test]
fn codex_0_161_native_writer_repeat_acquisition() {
    let home = tempfile::tempdir().unwrap();
    let captures = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../plans/history-providers/fixtures/baseline/codex-0.161.0");
    let directory = home.path().join(".codex/sessions/2026/01/01");
    fs::create_dir_all(&directory).unwrap();
    fs::copy(
        captures.join("session.jsonl"),
        directory.join("rollout-native.jsonl"),
    )
    .unwrap();
    let expected: Value =
        serde_json::from_slice(&fs::read(captures.join("expected-messages.json")).unwrap())
            .unwrap();
    let messages: Vec<_> = expected
        .as_array()
        .unwrap()
        .iter()
        .map(|message| {
            let role = match message["role"].as_str().unwrap() {
                "user" => Role::User,
                "assistant" => Role::Assistant,
                other => panic!("unexpected input role: {other}"),
            };
            (role, message["text"].as_str().unwrap())
        })
        .collect();
    let evidence = assert_history_conformance(
        home.path(),
        HistoryCase {
            source: Source::Codex,
            session_id: "019b76da-a800-7000-8000-000000000161",
            messages: &messages,
        },
    );
    assert!(evidence
        .tool_calls
        .iter()
        .any(|call| call.tool_use_id == "offline-call-1"));
    assert!(evidence
        .tool_results
        .iter()
        .any(|result| result.tool_use_id.as_deref() == Some("offline-call-1")));
}

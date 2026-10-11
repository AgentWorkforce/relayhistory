//! Characterization of admitted formats, not a claim about current upstream releases.
//! Reuses checked-in provider fixtures and the public conformance contract.
mod provider_support;
use ai_hist::{Role, Source};
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

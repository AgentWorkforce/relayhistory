//! Synthetic source evidence for the two-machine Projects fixture.
use ai_hist::{ProviderRoots, SessionStore, StoreOptions, SyncOptions};
use rusqlite::Connection;
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

const AT: &str = "2026-09-19T10:00:00Z";
const REMOTE: &str = "github.com/Synthetic/T28";

fn transcript(path: &Path, rows: Vec<Value>) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let lines: Vec<_> = rows.iter().map(Value::to_string).collect();
    fs::write(path, lines.join("\n") + "\n").unwrap();
}

fn codex(home: &Path, id: &str, cwd: &Path, parent: Option<&str>, messages: &[&str]) {
    let mut meta = json!({"id":id,"cwd":cwd});
    if let Some(parent) = parent {
        meta["session_id"] = json!(parent);
        meta["parent_thread_id"] = json!(parent);
        meta["thread_source"] = json!("subagent");
    }
    let mut rows = vec![json!({"timestamp":AT,"type":"session_meta","payload":meta})];
    for text in messages {
        rows.push(json!({"timestamp":AT,"type":"event_msg","payload":{"type":"user_message","message":text}}));
    }
    transcript(
        &home.join(format!(".codex/sessions/2026/09/19/rollout-{id}.jsonl")),
        rows,
    );
}

#[test]
fn two_store_project_evidence() {
    let mut evidence = BTreeMap::<(String, String, String), Vec<(String, String)>>::new();
    for machine in ["A", "B"] {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path();
        let repo = home.join("checkout");
        let path = home.join("scratch");
        fs::create_dir_all(repo.join(".git")).unwrap();
        fs::create_dir(&path).unwrap();
        let origin = if machine == "A" {
            "git@github.com:Synthetic/T28.git"
        } else {
            "https://github.com/Synthetic/T28"
        };
        fs::write(
            repo.join(".git/config"),
            format!("[remote \"origin\"]\nurl = {origin}\n"),
        )
        .unwrap();
        codex(
            home,
            "shared",
            &repo,
            None,
            &[
                "same prompt",
                if machine == "A" {
                    "variant A"
                } else {
                    "variant B"
                },
            ],
        );
        codex(
            home,
            &format!("work-{machine}"),
            &repo,
            None,
            &["independent action"],
        );
        if machine == "A" {
            codex(home, "child", &path, Some("shared"), &["delegated action"]);
            codex(home, "scratch", &path, None, &["local action"]);
            transcript(
                &home.join(".claude/projects/synthetic/shared.jsonl"),
                vec![
                    json!({"type":"user","sessionId":"shared","uuid":"claude-event","timestamp":AT,"cwd":repo,"message":{"role":"user","content":"claude action"}}),
                ],
            );
        }
        let db = home.join("history.db");
        let roots = ProviderRoots::from_home(home.into(), home.join("unused.db"));
        let store = SessionStore::open(StoreOptions::default().roots(roots).db_path(&db)).unwrap();
        store
            .sync(SyncOptions::default().sources([ai_hist::Source::Codex, ai_hist::Source::Claude]))
            .unwrap();
        let conn =
            Connection::open_with_flags(&db, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        let mut stmt = conn.prepare("SELECT source,session_id,event_uid,text,project_key,project_key_method FROM session_events WHERE role='user' ORDER BY source,session_id,event_uid").unwrap();
        let mut rows = stmt.query([]).unwrap();
        let mut count = 0;
        while let Some(row) = rows.next().unwrap() {
            let key: (String, String, String) = (
                row.get(0).unwrap(),
                row.get(1).unwrap(),
                row.get(2).unwrap(),
            );
            let text: String = row.get(3).unwrap();
            let project: String = row.get(4).unwrap();
            let method: String = row.get(5).unwrap();
            let expected = match key.1.as_str() {
                "scratch" => (path.to_str().unwrap(), "path"),
                "child" => (REMOTE, "inherited"),
                _ => (REMOTE, "remote"),
            };
            assert_eq!((project.as_str(), method.as_str()), expected);
            println!(
                "{}",
                json!({"store":machine,"ref":["session_event",key.0,key.1,key.2],"payload":text,"project":if method=="path" {"<A>/scratch"}else{&project},"method":method})
            );
            evidence
                .entry(key)
                .or_default()
                .push((machine.into(), text));
            count += 1;
        }
        assert_eq!(count, if machine == "A" { 6 } else { 3 });
    }
    assert_eq!(evidence.len(), 7);
    let repeated: Vec<_> = evidence.iter().filter(|(_, v)| v.len() == 2).collect();
    assert_eq!(repeated.len(), 2);
    let variants: BTreeSet<_> = repeated.iter().map(|(_, v)| (&v[0].1, &v[1].1)).collect();
    assert!(variants
        .iter()
        .any(|(a, b)| a.as_str() == "same prompt" && a == b));
    assert!(variants
        .iter()
        .any(|(a, b)| a.as_str() == "variant A" && b.as_str() == "variant B"));
}

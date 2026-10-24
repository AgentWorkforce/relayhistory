use super::*;
use crate::{ProviderRoots, SessionStore, Source, StoreOptions, SyncOptions};

#[cfg(unix)]
fn persisted_session_replays_middle_rewrites(force: bool) {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            super::super::super::transcript_cursor::set_filesystem_keeps_change_time_for_test(None);
            super::super::super::transcript_cursor::set_settle_window_for_test(None);
        }
    }
    super::super::super::transcript_cursor::set_filesystem_keeps_change_time_for_test(Some(true));
    super::super::super::transcript_cursor::set_settle_window_for_test(Some(0));
    let _reset = Reset;
    for source in [Source::Claude, Source::Codex] {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("home");
        let path = home.join(if source == Source::Claude {
            ".claude/projects/p/session.jsonl"
        } else {
            ".codex/sessions/2026/10/24/rollout-proof.jsonl"
        });
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut text = String::new();
        if source == Source::Codex {
            text.push_str(
                "{\"type\":\"session_meta\",\"payload\":{\"id\":\"s\",\"cwd\":\"/tmp/p\"}}\n",
            );
        }
        for (i, marker) in ["head", "middle-before", "tail"].iter().enumerate() {
            let content = format!("{marker}{}", "x".repeat(100 * 1024));
            let record = if source == Source::Claude {
                json!({"sessionId":"s","uuid":format!("m{i}"),"type":"user","timestamp":"2026-10-20T12:00:00Z","message":{"role":"user","content":content}})
            } else {
                json!({"type":"response_item","timestamp":"2026-10-20T12:00:00Z","payload":{"id":format!("m{i}"),"type":"message","role":"user","content":[{"type":"input_text","text":content}]}})
            };
            text.push_str(&format!("{record}\n"));
        }
        if source == Source::Codex {
            text.push_str("{\"type\":\"event_msg\",\"payload\":{\"type\":\"task_complete\"}}\n");
        }
        fs::write(&path, &text).unwrap();
        let store = SessionStore::open(
            StoreOptions::default()
                .db_path(temp.path().join("history.db"))
                .roots(ProviderRoots::from_home(
                    home.clone(),
                    home.join("opencode.db"),
                )),
        )
        .unwrap();
        let mut options = SyncOptions::default().sources([source]);
        options.paths = Some(vec![path.clone()]);
        options.force = true;
        store.sync(options.clone()).unwrap();
        store.sync(options.clone()).unwrap();
        let conn = open_db(store.db_path()).unwrap();
        let key = cursor_key(source.as_str(), "s");
        let previous = load_cursor(&conn, &key).unwrap();
        assert_eq!(previous.file.as_ref().unwrap().offset, text.len() as u64);
        let settled = previous.settled.unwrap();
        assert!(conn.query_row("SELECT EXISTS(SELECT 1 FROM session_events WHERE source=?1 AND session_id='s' AND text LIKE 'middle-before%')",[source.as_str()],|r|r.get::<_,bool>(0)).unwrap());
        let before = fs::metadata(&path).unwrap();
        use std::os::unix::fs::MetadataExt;
        let edit = text.find("middle-before").unwrap();
        assert!(edit > 64 * 1024 && edit + 13 < text.len() - 64 * 1024);
        let rewrite = text.replacen("middle-before", "middle-after_", 1);
        assert_eq!(rewrite.len(), text.len());
        fs::write(&path, rewrite).unwrap();
        fs::File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_modified(before.modified().unwrap())
            .unwrap();
        let after = fs::metadata(&path).unwrap();
        assert_eq!(
            (
                before.dev(),
                before.ino(),
                before.len(),
                before.modified().unwrap()
            ),
            (
                after.dev(),
                after.ino(),
                after.len(),
                after.modified().unwrap()
            )
        );
        assert_ne!(change_time_ns(&after), Some(settled.ctime_ns));
        options.force = force;
        store.sync(options).unwrap();
        let captured: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM session_events WHERE source=?1 AND session_id='s' AND text LIKE 'middle-after_%')",[source.as_str()],|row|row.get(0)).unwrap();
        assert!(captured,"source={source:?} force={force}: acquisition must repair actual changed evidence, not merely reject the stat skip");
        let stale:bool=conn.query_row("SELECT EXISTS(SELECT 1 FROM session_events WHERE source=?1 AND session_id='s' AND text LIKE 'middle-before%')",[source.as_str()],|r|r.get(0)).unwrap();
        assert!(
            !stale,
            "source={source:?} force={force}: rewritten provider record must replace its old text"
        );
    }
}

#[cfg(unix)]
#[test]
fn persisted_session_middle_rewrite_is_acquired_without_force() {
    persisted_session_replays_middle_rewrites(false);
}

#[cfg(unix)]
#[test]
fn persisted_session_middle_rewrite_is_acquired_with_force() {
    persisted_session_replays_middle_rewrites(true);
}

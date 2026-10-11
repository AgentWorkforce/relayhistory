use super::*;
#[test]
fn strict_versions_and_tail() {
    assert!(
        format::parse(b"{\"type\":\"session\",\"version\":4}\n", false)
            .unwrap_err()
            .to_string()
            .contains("UNSUPPORTED_SCHEMA")
    );
    assert!(format::parse(b"{", true)
        .unwrap_err()
        .to_string()
        .contains("INCOMPLETE_HEADER"));
}
#[test]
fn usage_is_measured_and_partial() {
    let u = usage::normalize(
        &serde_json::json!({"input":10,"output":2,"cacheRead":3,"cacheWrite":4,"totalTokens":19}),
    )
    .unwrap()
    .unwrap();
    assert_eq!(u.input_tokens, 10);
    assert_eq!(u.cache_read_tokens, 3);
    assert!(u.coverage.is_complete());
    assert!(usage::normalize(&serde_json::json!({"input":-1})).is_err());
    assert!(usage::normalize(&serde_json::json!({"tokensBefore":1000}))
        .unwrap()
        .is_none());
}

const FIXTURE: &str = include_str!(
    "../../../tests/fixtures/pi/writer-v3/.pi/agent/sessions/--fixture--/session.jsonl"
);
fn fixture() -> (tempfile::TempDir, crate::SessionStore, PathBuf) {
    let temp = tempfile::tempdir().unwrap();
    let roots = ProviderRoots::from_home(temp.path().into(), temp.path().join("opencode.db"));
    let path = roots.for_provider(ID)[0].join("session.jsonl");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, FIXTURE).unwrap();
    let store = crate::SessionStore::open(
        crate::StoreOptions::default()
            .db_path(temp.path().join("store.db"))
            .roots(roots),
    )
    .unwrap();
    (temp, store, path)
}
#[test]
fn pi_selected_snapshot_is_immutable_and_prefix_bounded() {
    let (_temp, store, path) = fixture();
    let ctx = AcquisitionContext::new(ID, store.roots());
    let captured = snapshot(&path, &ctx).unwrap();
    std::fs::write(&path, FIXTURE.replace("Hello fixture", "Changed on disk")).unwrap();
    let conn = crate::open_db(store.db_path()).unwrap();
    normalize::write(&conn, &captured.payload, &path).unwrap();
    let e = store
        .session(
            &crate::SessionRef::id(crate::Source::Pi, "custom-history-session"),
            crate::SessionQuery::default(),
        )
        .unwrap()
        .unwrap();
    assert!(e
        .messages
        .iter()
        .flat_map(|m| &m.blocks)
        .any(|b| b.text.as_deref() == Some("The greeting says Hello fixture.")));
    std::fs::write(&path, vec![b'a'; 128 * 1024]).unwrap();
    assert_eq!(prefix(&ctx, &path).unwrap().len(), HEAD_LIMIT as usize);
    assert!(format::parse(&prefix(&ctx, &path).unwrap(), true)
        .unwrap_err()
        .to_string()
        .contains("INCOMPLETE_HEADER"));
}
#[test]
fn pi_scoped_tick_skips_sibling_and_full_backstop_preserves_it() {
    let (_temp, store, path) = fixture();
    let sibling = path.with_file_name("sibling.jsonl");
    std::fs::write(
        &sibling,
        FIXTURE.replace("custom-history-session", "other-session"),
    )
    .unwrap();
    let conn = crate::open_db(store.db_path()).unwrap();
    let scope = crate::ingest::SweepScope::only([ID]).with_paths(
        crate::ingest::path_scope::PathScope::from_paths([path.clone()]),
    );
    crate::ingest::sync_basic(&conn, store.db_path(), store.roots(), true, &scope).unwrap();
    let read = |id| {
        store
            .session(
                &crate::SessionRef::id(crate::Source::Pi, id),
                crate::SessionQuery::default(),
            )
            .unwrap()
    };
    assert!(read("custom-history-session").is_some());
    assert!(read("other-session").is_none());
    crate::ingest::sync_basic(
        &conn,
        store.db_path(),
        store.roots(),
        true,
        &crate::ingest::SweepScope::only([ID]),
    )
    .unwrap();
    assert!(read("other-session").is_some());
    std::fs::write(sibling, "not readable Pi").unwrap();
    crate::ingest::sync_basic(&conn, store.db_path(), store.roots(), true, &scope).unwrap();
    assert!(read("other-session").is_some());
}
#[test]
fn pi_explicit_roots_and_legacy_serialized_defaults() {
    let (temp, store, _) = fixture();
    let custom = temp.path().join("explicit");
    std::fs::create_dir(&custom).unwrap();
    std::fs::write(custom.join("custom.jsonl"), FIXTURE).unwrap();
    let mut json = serde_json::to_value(store.roots()).unwrap();
    json["provider_roots"]["pi"] = serde_json::json!([custom]);
    let roots: ProviderRoots = serde_json::from_value(json.clone()).unwrap();
    assert_eq!(roots.for_provider(ID), &[custom]);
    let conn = crate::open_db(store.db_path()).unwrap();
    let env = DiscoveryEnv::with_provider_roots(&conn, roots);
    assert_eq!(Provider.enumerate(&env, None).unwrap().len(), 1);
    json.as_object_mut().unwrap().remove("provider_roots");
    let legacy: ProviderRoots = serde_json::from_value(json).unwrap();
    assert_eq!(
        legacy.for_provider(ID),
        &[temp.path().join(".pi/agent/sessions")]
    );
    assert_eq!(expand(temp.path(), "~/custom"), temp.path().join("custom"));
}
#[test]
#[cfg(unix)]
fn pi_denied_root_is_error_and_parent_or_spill_paths_are_not_followed() {
    use std::os::unix::fs::PermissionsExt;
    let (_temp, store, path) = fixture();
    let root = path.parent().unwrap();
    let conn = crate::open_db(store.db_path()).unwrap();
    let env = DiscoveryEnv::with_provider_roots(&conn, store.roots().clone());
    std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o000)).unwrap();
    let result = Provider.enumerate(&env, None);
    std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o700)).unwrap();
    assert!(result.is_err());
    let mut lines: Vec<Value> = FIXTURE
        .lines()
        .map(|s| serde_json::from_str(s).unwrap())
        .collect();
    lines[0]["parentSession"] = "/private/not-authorized.jsonl".into();
    lines[2]["message"]["content"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::json!({"type":"image","path":"/private/spill.png"}));
    std::fs::write(
        &path,
        lines.iter().map(|v| format!("{v}\n")).collect::<String>(),
    )
    .unwrap();
    let acquired = snapshot(&path, &AcquisitionContext::new(ID, store.roots())).unwrap();
    assert_eq!(acquired.read_paths, vec![path.clone()]);
    normalize::write(&conn, &acquired.payload, &path).unwrap();
}

#[test]
fn pi_root_precedence_and_partial_tail_prevent_pruning() {
    let home = Path::new("/isolated");
    assert_eq!(
        roots_from_values(home, Some("~/sessions"), Some("~/agent")),
        vec![home.join("sessions")]
    );
    assert_eq!(
        roots_from_values(home, None, Some("~/agent")),
        vec![home.join("agent/sessions")]
    );
    let (_temp, store, path) = fixture();
    let conn = crate::open_db(store.db_path()).unwrap();
    let ctx = AcquisitionContext::new(ID, store.roots());
    let original = snapshot(&path, &ctx).unwrap();
    normalize::write(&conn, &original.payload, &path).unwrap();
    let before: (i64, Option<String>) = conn
        .query_row(
            "SELECT last_activity_ms,last_assistant_text FROM sessions WHERE source='pi'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    let partial = FIXTURE
        .lines()
        .take(3)
        .map(|l| format!("{l}\n"))
        .collect::<String>()
        + "{partial";
    std::fs::write(&path, partial).unwrap();
    let capture = snapshot(&path, &ctx).unwrap();
    normalize::write(&conn, &capture.payload, &path).unwrap();
    let after: (i64, Option<String>) = conn
        .query_row(
            "SELECT last_activity_ms,last_assistant_text FROM sessions WHERE source='pi'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(
        before, after,
        "incomplete rewrite preserves catalog end and preview"
    );
    let count: i64 = conn
        .query_row(
            "SELECT count(*) FROM session_events WHERE source='pi' AND message_id='entry-7'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        count, 1,
        "partial rewrite must not retire prior complete rows"
    );
}
#[test]
fn pi_rewrite_retires_stale_tools_and_clears_removed_result_status() {
    let (_temp, store, path) = fixture();
    let conn = crate::open_db(store.db_path()).unwrap();
    let ctx = AcquisitionContext::new(ID, store.roots());
    let capture = snapshot(&path, &ctx).unwrap();
    normalize::write(&conn, &capture.payload, &path).unwrap();
    let mut payload = capture.payload;
    payload["entries"]
        .as_array_mut()
        .unwrap()
        .retain(|e| e["id"] != "entry-3");
    normalize::write(&conn, &payload, &path).unwrap();
    let error: Option<bool> = conn
        .query_row(
            "SELECT is_error FROM tool_calls WHERE source='pi'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(error, None);
    let blocks = payload["entries"][2]["message"]["content"]
        .as_array_mut()
        .unwrap();
    blocks.retain(|b| b["type"] != "toolCall");
    normalize::write(&conn, &payload, &path).unwrap();
    let calls: i64 = conn
        .query_row(
            "SELECT count(*) FROM tool_calls WHERE source='pi'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(calls, 0);
}

#[test]
fn pi_fork_shared_multiblock_prompt_survives_original_rewrite() {
    let (_temp, store, path) = fixture();
    let conn = crate::open_db(store.db_path()).unwrap();
    let mut payload = snapshot(&path, &AcquisitionContext::new(ID, store.roots()))
        .unwrap()
        .payload;
    payload["entries"][1]["message"]["content"] =
        serde_json::json!([{"type":"text","text":"part one"},{"type":"text","text":"part two"}]);
    normalize::write(&conn, &payload, &path).unwrap();
    let mut fork = payload.clone();
    fork["entries"][0]["id"] = "another-session".into();
    normalize::write(&conn, &fork, &path.with_file_name("fork.jsonl")).unwrap();
    payload["entries"]
        .as_array_mut()
        .unwrap()
        .retain(|e| e["id"] != "entry-1");
    normalize::write(&conn, &payload, &path).unwrap();
    let owner:String=conn.query_row("SELECT session_id FROM history WHERE source='pi' AND prompt = 'part one'||char(10)||'part two'",[],|r|r.get(0)).unwrap();
    assert_eq!(owner, "another-session");
}

#[test]
fn pi_rewrite_preserves_prompt_backed_by_same_session_remote_events() {
    let (_temp, store, path) = fixture();
    let conn = crate::open_db(store.db_path()).unwrap();
    let mut payload = snapshot(&path, &AcquisitionContext::new(ID, store.roots()))
        .unwrap()
        .payload;
    payload["entries"][1]["message"]["content"] = serde_json::json!([{"type":"text","text":"remote first"},{"type":"text","text":"remote second"}]);
    normalize::write(&conn, &payload, &path).unwrap();
    conn.execute("UPDATE session_events SET location = 'both' WHERE source = 'pi' AND message_id = 'entry-1'", []).unwrap();
    payload["entries"]
        .as_array_mut()
        .unwrap()
        .retain(|e| e["message"]["role"] != "user");
    normalize::write(&conn, &payload, &path).unwrap();
    let remote: i64 = conn.query_row("SELECT count(*) FROM session_events WHERE source = 'pi' AND message_id = 'entry-1' AND location = 'remote'", [], |r| r.get(0)).unwrap();
    assert_eq!(
        remote, 2,
        "local retirement must preserve both remote text blocks"
    );
    let prompts: Vec<(String, String)> = conn
        .prepare("SELECT session_id, prompt FROM history WHERE source = 'pi'")
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(
        prompts,
        vec![(
            "custom-history-session".into(),
            "remote first\nremote second".into()
        )],
        "same-session remote prompt survives; local-only prompt is retired"
    );
    normalize::write(&conn, &payload, &path).unwrap();
    let remaining: i64 = conn
        .query_row(
            "SELECT count(*) FROM history WHERE source = 'pi'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        remaining, 1,
        "unchanged replay preserves remote prompt ownership"
    );
    conn.execute(
        "DELETE FROM session_events WHERE source = 'pi' AND message_id = 'entry-1'",
        [],
    )
    .unwrap();
    normalize::write(&conn, &payload, &path).unwrap();
    let remaining: i64 = conn
        .query_row(
            "SELECT count(*) FROM history WHERE source = 'pi'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        remaining, 0,
        "prompt retires once no finalized user projection supports it"
    );
}

#![cfg(all(feature = "delivery", feature = "unstable-internal"))]
//! Isolated provider roots; synthetic evidence only, no receiver/network.
use ai_hist::{
    discover_sessions_scoped_at, hydrate_session_at, open_db, DiscoverOptions,
    HydrateSessionOptions, SessionScope,
};

#[test]
fn selected_hydration_does_not_reconcile_unrelated_history() {
    let home = tempfile::tempdir().unwrap();
    std::env::set_var("HOME", home.path());
    std::env::set_var("USERPROFILE", home.path());
    std::env::remove_var("CLAUDE_CONFIG_DIR");
    std::env::remove_var("AI_HIST_DB");
    let transcript = home
        .path()
        .join(".claude/projects/synthetic/selected.jsonl");
    std::fs::create_dir_all(transcript.parent().unwrap()).unwrap();
    std::fs::write(&transcript,"{\"sessionId\":\"selected\",\"uuid\":\"u1\",\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":\"synthetic selected record\"},\"timestamp\":\"2026-09-01T01:00:00Z\"}\n").unwrap();
    for count in [100, 10_000, 50_000] {
        let path = home.path().join(format!("fixture-{count}.db"));
        discover_sessions_scoped_at(
            &path,
            &DiscoverOptions {
                scope: SessionScope::Local,
                sources: vec!["claude".into()],
                limit: None,
            },
        )
        .unwrap();
        let conn = open_db(&path).unwrap();
        conn.execute_batch("BEGIN").unwrap();
        conn.execute("WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<?1) INSERT INTO sessions(source,session_id,project_key,project_key_method) SELECT 'codex','unrelated-'||x,'github.com/synthetic/unrelated','remote' FROM n",[count]).unwrap();
        conn.execute("INSERT INTO session_events(source,session_id,event_uid,ts_ms,role,kind,text) SELECT source,session_id,session_id,1,'user','text','unrelated synthetic evidence' FROM sessions WHERE source='codex'",[]).unwrap();
        conn.execute_batch("COMMIT").unwrap();
        let started = std::time::Instant::now();
        let result = hydrate_session_at(
            &path,
            &HydrateSessionOptions {
                source: "claude".into(),
                session_id: "selected".into(),
                scope: SessionScope::Local,
                include_related: false,
            },
        )
        .unwrap();
        eprintln!(
            "selected-hydration unrelated={count} elapsed_ms={:.3} parsed_events={}",
            started.elapsed().as_secs_f64() * 1000.,
            result.evidence.events
        );
        assert_eq!(result.evidence.events, 1);
        assert_eq!(conn.query_row("SELECT count(*) FROM session_events WHERE source='codex' AND project_key IS NOT NULL",[],|r|r.get::<_,i64>(0)).unwrap(),0,"unrelated project maintenance must not run during hydration");
    }
}

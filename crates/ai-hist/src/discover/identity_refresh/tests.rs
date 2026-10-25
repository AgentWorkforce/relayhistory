//! The capture projection must preserve cache admission and identity semantics.
use super::*;
use rusqlite::hooks::{AuthAction, AuthContext, Authorization};

fn catalog() -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    crate::init_db(&conn).unwrap();
    conn.execute(
        "INSERT INTO sessions (source,session_id,cwd,repo_url,raw_path,discovery_state,project_key,project_key_method,first_prompt,last_assistant_text,title,models_json,workspace_roots_json) VALUES ('codex','projected','/work/repo','git@github.com:Org/Repo.git','/fixture/transcript.jsonl','shallow','/work/repo','path','first prompt','last reply','session title','[\"model-a\",\"model-b\"]','[\"/work/repo\",\"/work/library\"]')",
        [],
    ).unwrap();
    conn
}

fn capture_fields(row: &ShallowSession) -> ShallowSession {
    ShallowSession {
        source: row.source.clone(),
        session_id: row.session_id.clone(),
        cwd: row.cwd.clone(),
        repo_url: row.repo_url.clone(),
        raw_path: row.raw_path.clone(),
        discovery_state: row.discovery_state.clone(),
        project_key: row.project_key.clone(),
        project_key_method: row.project_key_method.clone(),
        from_cache: row.from_cache,
        ..ShallowSession::default()
    }
}

#[test]
fn capture_projection_keeps_identity_and_locator_without_unused_materialization() {
    let conn = catalog();
    let full = fetch_catalog_row(&conn, "codex", "projected")
        .unwrap()
        .unwrap();
    let projected = IdentityRefresh::ExactPaths
        .cached_row(&conn, "codex", "projected")
        .unwrap()
        .unwrap();
    assert_eq!(capture_fields(&projected), capture_fields(&full));
    assert_eq!(full.models, ["model-a", "model-b"]);
    assert_eq!(full.workspace_roots, ["/work/repo", "/work/library"]);
    assert_eq!(full.title.as_deref(), Some("session title"));
    assert!(projected.models.is_empty());
    assert!(projected.workspace_roots.is_empty());
    assert!(projected.locations.is_empty());
    assert!(projected.first_prompt.is_none());
    assert!(projected.last_assistant_text.is_none());
    assert!(projected.title.is_none());
    assert_eq!(
        IdentityRefresh::Streamed
            .cached_row(&conn, "codex", "projected")
            .unwrap()
            .unwrap(),
        full
    );
    assert!(IdentityRefresh::ExactPaths
        .cached_row(&conn, "codex", "missing")
        .unwrap()
        .is_none());
    conn.execute("UPDATE sessions SET discovery_state=NULL", [])
        .unwrap();
    assert_eq!(
        IdentityRefresh::ExactPaths
            .cached_row(&conn, "codex", "projected")
            .unwrap()
            .unwrap()
            .discovery_state,
        "full"
    );
}

#[test]
fn capture_projection_preserves_scalar_corruption_cache_misses() {
    for column in [
        "source",
        "session_id",
        "cwd",
        "git_branch",
        "first_activity_ms",
        "last_activity_ms",
        "first_prompt",
        "last_assistant_text",
        "models_json",
        "originator",
        "agent_version",
        "repo_url",
        "initial_commit",
        "workspace_roots_json",
        "raw_path",
        "source_stamp",
        "discovery_state",
        "project_key",
        "project_key_method",
        "title",
        "session_kind",
    ] {
        for corrupt in ["X'80'", "CAST(X'80' AS TEXT)"] {
            let conn = catalog();
            conn.execute_batch(&format!("UPDATE sessions SET {column}={corrupt}"))
                .unwrap();
            let full = fetch_catalog_row(&conn, "codex", "projected").unwrap();
            let projected = IdentityRefresh::ExactPaths
                .cached_row(&conn, "codex", "projected")
                .unwrap();
            assert!(
                full.is_none(),
                "fixture must exercise a cache miss: {column}={corrupt}"
            );
            assert!(
                projected.is_none(),
                "projection admitted corrupt scalar: {column}={corrupt}"
            );
            // A malformed identity key also misses the lookup predicate. Read
            // the row without a predicate to prove all cases reach each decoder.
            let full_error = conn
                .query_row(
                    &format!("SELECT {SESSION_COLUMNS} FROM sessions"),
                    [],
                    row_to_session,
                )
                .unwrap_err();
            let capture_error = conn
                .query_row(
                    &format!("SELECT {SESSION_SCALAR_COLUMNS} FROM sessions"),
                    [],
                    catalog_row::row_to_capture_session,
                )
                .unwrap_err();
            for error in [full_error, capture_error] {
                assert!(
                    matches!(
                        error,
                        rusqlite::Error::InvalidColumnType(..)
                            | rusqlite::Error::FromSqlConversionFailure(..)
                    ),
                    "expected a decoder error for {column}={corrupt}, got {error}"
                );
            }
        }
    }
    // Invalid JSON is deliberately tolerated by the full decoder.
    let conn = catalog();
    conn.execute(
        "UPDATE sessions SET models_json='not json', workspace_roots_json='not json'",
        [],
    )
    .unwrap();
    assert!(fetch_catalog_row(&conn, "codex", "projected")
        .unwrap()
        .is_some());
    assert!(IdentityRefresh::ExactPaths
        .cached_row(&conn, "codex", "projected")
        .unwrap()
        .is_some());
    conn.execute_batch("DROP TABLE sessions").unwrap();
    assert!(fetch_catalog_row(&conn, "codex", "projected")
        .unwrap()
        .is_none());
    assert!(IdentityRefresh::ExactPaths
        .cached_row(&conn, "codex", "projected")
        .unwrap()
        .is_none());
}

#[test]
fn capture_projection_preserves_identity_upgrade_and_concurrent_stronger_write() {
    let conn = catalog();
    let mut row = IdentityRefresh::ExactPaths
        .cached_row(&conn, "codex", "projected")
        .unwrap()
        .unwrap();
    crate::project_identity::begin_acquisition_pass();
    upgrade_cached_project_identity(&conn, &mut row).unwrap();
    assert_eq!(row.project_key.as_deref(), Some("github.com/Org/Repo"));
    assert_eq!(row.project_key_method.as_deref(), Some("remote"));

    conn.execute(
        "UPDATE sessions SET repo_url=NULL, project_key=NULL, project_key_method=NULL",
        [],
    )
    .unwrap();
    let mut stale = IdentityRefresh::ExactPaths
        .cached_row(&conn, "codex", "projected")
        .unwrap()
        .unwrap();
    conn.execute(
        "UPDATE sessions SET project_key='github.com/Other/Settled', project_key_method='remote'",
        [],
    )
    .unwrap();
    upgrade_cached_project_identity(&conn, &mut stale).unwrap();
    assert_eq!(
        stale.project_key.as_deref(),
        Some("github.com/Other/Settled")
    );
    assert_eq!(stale.project_key_method.as_deref(), Some("remote"));
}

#[test]
fn capture_projection_actual_exact_discovery_does_not_query_unused_presences() {
    let temp = tempfile::tempdir().unwrap();
    let roots = crate::ProviderRoots::from_home(temp.path().into(), temp.path().join("missing.db"));
    let path = roots
        .codex
        .join("sessions/2026/10/23/rollout-projected.jsonl");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, "{\"timestamp\":\"2026-10-23T12:00:00Z\",\"type\":\"session_meta\",\"payload\":{\"id\":\"projected\",\"cwd\":\"/fixture\"}}\n").unwrap();
    let conn = catalog();
    let env = DiscoveryEnv::with_provider_roots(&conn, roots.clone())
        .with_path_scope(crate::ingest::path_scope::PathScope::from_paths([path]));
    let options = DiscoverOptions {
        sources: vec!["codex".into()],
        ..Default::default()
    };
    discover_sessions_for_capture(&env, &options, |_| {}).unwrap();
    let env =
        DiscoveryEnv::with_provider_roots(&conn, roots).with_path_scope(env.path_scope().cloned());
    conn.flush_prepared_statement_cache();
    conn.authorizer(Some(|context: AuthContext<'_>| match context.action {
        AuthAction::Read {
            table_name: "session_presences",
            ..
        } => Authorization::Deny,
        _ => Authorization::Allow,
    }));
    let mut captured = Vec::new();
    let summary =
        discover_sessions_for_capture(&env, &options, |row| captured.push(row.clone())).unwrap();
    // The public full-row path retains its presence dependency. Only the
    // private callback can ignore an unavailable unused location projection.
    assert!(IdentityRefresh::Streamed
        .cached_row(&conn, "codex", "projected")
        .unwrap()
        .is_none());
    conn.authorizer(None::<fn(AuthContext<'_>) -> Authorization>);
    assert_eq!(summary.skipped_unchanged, 1);
    assert_eq!(summary.counters.shallow_reads, 0);
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].session_id, "projected");
    assert!(captured[0].raw_path.is_some());
}

#[test]
#[ignore = "bounded catalog-query VM/materialization diagnostic"]
fn capture_projection_query_work_diagnostic() {
    for excerpt_bytes in [32, 2000] {
        let conn = catalog();
        crate::mark_session_presence(&conn, "codex", "projected", SessionLocation::Local).unwrap();
        conn.execute(
            "UPDATE sessions SET first_prompt=?1,last_assistant_text=?1",
            ["x".repeat(excerpt_bytes)],
        )
        .unwrap();
        for (name, sql, decode) in [
            (
                "full",
                CATALOG_ROW_SQL.as_str(),
                row_to_session as fn(&rusqlite::Row<'_>) -> rusqlite::Result<ShallowSession>,
            ),
            (
                "exact",
                CAPTURE_ROW_SQL.as_str(),
                catalog_row::row_to_capture_session,
            ),
        ] {
            let mut statement = conn.prepare_cached(sql).unwrap();
            let mut unused_bytes = 0;
            for _ in 0..1000 {
                let row = statement
                    .query_row(params!["codex", "projected"], decode)
                    .unwrap();
                unused_bytes += materialized_unused_bytes(&row);
            }
            println!("CAPTURE_QUERY_PROFILE mode={name} calls=1000 excerpt_bytes_each={excerpt_bytes} sqlite_vm_steps={} unused_materialized_utf8_bytes={unused_bytes}", statement.get_status(rusqlite::StatementStatus::VmStep));
        }
    }
}

fn materialized_unused_bytes(row: &ShallowSession) -> usize {
    let strings = [
        &row.git_branch,
        &row.first_prompt,
        &row.last_assistant_text,
        &row.originator,
        &row.agent_version,
        &row.initial_commit,
        &row.source_stamp,
        &row.title,
        &row.session_kind,
    ];
    strings
        .into_iter()
        .flatten()
        .map(String::len)
        .sum::<usize>()
        + row
            .models
            .iter()
            .chain(&row.workspace_roots)
            .chain(&row.locations)
            .map(String::len)
            .sum::<usize>()
}

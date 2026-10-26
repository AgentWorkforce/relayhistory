//! Causal work counts and filesystem freshness of the private capture policy.
use super::*;
use crate::store::project_identity_resolver::{take_work, Work};
use rusqlite::params;

struct Fixture {
    _temp: tempfile::TempDir,
    conn: Connection,
    db: PathBuf,
    roots: crate::ProviderRoots,
    path: PathBuf,
    work: PathBuf,
}
impl Fixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path();
        let db = home.join("history.db");
        let conn = crate::open_db(&db).unwrap();
        let roots = crate::ProviderRoots::from_home(home.into(), home.join("missing.db"));
        let work = home.join("work/repo/subdir");
        fs::create_dir_all(&work).unwrap();
        let path = roots
            .codex
            .join("sessions/2026/10/23/rollout-admitted.jsonl");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, format!(
            "{{\"timestamp\":\"2026-10-23T12:00:00Z\",\"type\":\"session_meta\",\"payload\":{{\"id\":\"admitted\",\"cwd\":{}}}}}\n{{\"timestamp\":\"2026-10-23T12:00:01Z\",\"type\":\"event_msg\",\"payload\":{{\"type\":\"user_message\",\"message\":\"hello\"}}}}\n",
            serde_json::to_string(&work).unwrap()
        )).unwrap();
        let fixture = Self {
            _temp: temp,
            conn,
            db,
            roots,
            path,
            work,
        };
        fixture.capture();
        fixture.capture(); // initialize maintenance through the first acquisition
        fixture
    }
    fn capture(&self) -> Work {
        take_work();
        let scope = SweepScope::only(["codex"])
            .with_paths(path_scope::PathScope::from_paths([self.path.clone()]));
        assert_eq!(
            capture_paths(&self.conn, &self.db, &self.roots, &scope, false, false).unwrap(),
            Some(true)
        );
        take_work()
    }
    fn insert_session(&self, id: &str, cwd: &Path) {
        self.conn.execute(
            "INSERT INTO sessions (source, session_id, cwd, project_key, project_key_method, last_activity_ms, discovery_state) VALUES ('codex', ?1, ?2, ?2, 'path', 1, 'full')",
            params![id, cwd.to_string_lossy()],
        ).unwrap();
    }
    fn key(&self, id: &str) -> String {
        self.conn
            .query_row(
                "SELECT project_key FROM sessions WHERE source='codex' AND session_id=?1",
                [id],
                |row| row.get(0),
            )
            .unwrap()
    }
    fn descendants(&self) {
        self.insert_session("dependent", &self.roots.home.join("child"));
        self.conn.execute("INSERT INTO session_relationships (source,parent_session_id,relationship_uid,child_session_id,relationship,identity_status,evidence_kind,created_ms,updated_ms) VALUES ('codex','admitted','edge','dependent','delegates','observed','test',1,1)", []).unwrap();
        for session in ["admitted", "dependent"] {
            self.conn.execute("INSERT INTO session_events (source,session_id,event_uid,ts_ms,role,kind,text) VALUES ('codex',?1,?1,1,'user','text','identity')", [session]).unwrap();
        }
        self.capture();
    }
    fn assert_promoted(&self) {
        assert_eq!(self.key("admitted"), "github.com/Org/Upgraded");
        assert_eq!(self.key("dependent"), "github.com/Org/Upgraded");
        for session in ["admitted", "dependent"] {
            let keys: Vec<String> = self
                .conn
                .prepare(
                    "SELECT project_key FROM session_events WHERE source='codex' AND session_id=?1",
                )
                .unwrap()
                .query_map([session], |row| row.get(0))
                .unwrap()
                .collect::<rusqlite::Result<_>>()
                .unwrap();
            assert!(!keys.is_empty());
            assert!(keys.iter().all(|key| key == "github.com/Org/Upgraded"));
        }
    }
}
fn write_origin(git: &Path) {
    fs::create_dir_all(git).unwrap();
    fs::write(
        git.join("config"),
        "[remote \"origin\"]\nurl = git@github.com:Org/Upgraded.git\n",
    )
    .unwrap();
}
fn assert_local_work(work: Work) {
    assert_eq!(
        work.queries, 0,
        "exact capture ran the catalog-wide resolver query"
    );
    assert_eq!(
        work.rows, 0,
        "exact capture materialized unrelated eligible rows"
    );
    assert_eq!(
        work.observations, 1,
        "the admitted cached row must still observe identity"
    );
}

#[test]
fn exact_identity_work_is_independent_of_unrelated_catalog_population() {
    let f = Fixture::new();
    let small = f.capture();
    for index in 0..400 {
        f.insert_session(
            &format!("unrelated-{index}"),
            &f.roots.home.join(format!("outside/{index}")),
        );
    }
    f.capture(); // consume revisions from unrelated inserts
    for _ in 0..3 {
        let large = f.capture();
        assert_eq!(
            small, large,
            "unrelated catalog growth changed exact identity work"
        );
        assert_local_work(large);
    }
}

#[test]
fn exact_identity_sees_new_ancestor_git_without_transcript_or_revision_changes() {
    let f = Fixture::new();
    f.descendants();
    let before = crate::change_feed::read_head(&f.conn).unwrap();
    let transcript = fs::read(&f.path).unwrap();
    write_origin(&f.work.parent().unwrap().join(".git"));
    assert_eq!(crate::change_feed::read_head(&f.conn).unwrap(), before);
    assert_local_work(f.capture());
    assert_eq!(fs::read(&f.path).unwrap(), transcript);
    f.assert_promoted();
}

#[test]
fn exact_identity_sees_retargeted_pointer_and_common_dir_for_inherited_rows() {
    for retarget_pointer in [true, false] {
        let f = Fixture::new();
        let first = f.roots.home.join("git-first");
        let second = f.roots.home.join("git-second");
        let common = f.roots.home.join("git-common");
        fs::create_dir_all(&first).unwrap();
        fs::write(
            f.work.join(".git"),
            format!("gitdir: {}\n", first.display()),
        )
        .unwrap();
        f.conn.execute("UPDATE sessions SET project_key='github.com/Org/Parent', project_key_method='inherited' WHERE session_id='admitted'", []).unwrap();
        f.descendants();
        let before = crate::change_feed::read_head(&f.conn).unwrap();
        let transcript = fs::read(&f.path).unwrap();
        write_origin(&common);
        if retarget_pointer {
            fs::create_dir_all(&second).unwrap();
            fs::write(
                second.join("commondir"),
                common.to_string_lossy().as_bytes(),
            )
            .unwrap();
            fs::write(
                f.work.join(".git"),
                format!("gitdir: {}\n", second.display()),
            )
            .unwrap();
        } else {
            fs::write(first.join("commondir"), common.to_string_lossy().as_bytes()).unwrap();
        }
        assert_eq!(crate::change_feed::read_head(&f.conn).unwrap(), before);
        assert_local_work(f.capture());
        assert_eq!(fs::read(&f.path).unwrap(), transcript);
        f.assert_promoted();
    }
}

#[test]
fn general_discovery_still_promotes_unadmitted_catalog_rows() {
    let f = Fixture::new();
    let orphan = f.roots.home.join("orphan");
    f.insert_session("orphan", &orphan);
    write_origin(&orphan.join(".git"));
    assert_local_work(f.capture());
    assert_eq!(f.key("orphan"), orphan.to_string_lossy());
    let env = DiscoveryEnv::with_provider_roots(&f.conn, f.roots.clone());
    take_work();
    discover_sessions_with_env(
        &env,
        &DiscoverOptions {
            sources: vec!["codex".into()],
            ..Default::default()
        },
        |_| {},
    )
    .unwrap();
    assert_eq!(take_work().queries, 1);
    assert_eq!(f.key("orphan"), "github.com/Org/Upgraded");
}

#[test]
fn exact_identity_rechecks_a_missing_cwd_when_it_becomes_a_checkout() {
    let f = Fixture::new();
    f.descendants();
    fs::remove_dir(&f.work).unwrap();
    assert_local_work(f.capture()); // cache a negative filesystem observation
    let before = crate::change_feed::read_head(&f.conn).unwrap();
    fs::create_dir_all(&f.work).unwrap();
    write_origin(&f.work.join(".git"));
    assert_eq!(crate::change_feed::read_head(&f.conn).unwrap(), before);
    assert_local_work(f.capture());
    f.assert_promoted();
}

#[test]
fn exact_identity_observes_an_unadmitted_delegation_ancestor() {
    let f = Fixture::new();
    let parent = f.roots.home.join("parent");
    fs::create_dir_all(&parent).unwrap();
    f.insert_session("parent", &parent);
    f.conn.execute("INSERT INTO session_relationships (source,parent_session_id,relationship_uid,child_session_id,relationship,identity_status,evidence_kind,created_ms,updated_ms) VALUES ('codex','parent','parent-edge','admitted','delegates','observed','test',1,1)", []).unwrap();
    f.descendants();
    let before = crate::change_feed::read_head(&f.conn).unwrap();
    write_origin(&parent.join(".git"));
    assert_eq!(crate::change_feed::read_head(&f.conn).unwrap(), before);
    assert_local_work(f.capture());
    f.assert_promoted();
}

/// A bounded profile of the actual unchanged exact-capture path. No collector,
/// process-global roots, or wall-clock performance threshold is involved.
#[test]
#[ignore = "bounded exact-capture catalog work diagnostic"]
fn exact_capture_catalog_work_diagnostic() {
    for excerpt_bytes in [32, 2000] {
        profile_exact_capture_catalog(excerpt_bytes);
    }
}

fn profile_exact_capture_catalog(excerpt_bytes: usize) {
    use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
    use std::sync::{Arc, Mutex};
    let f = Fixture::new();
    let excerpt = "x".repeat(excerpt_bytes);
    f.conn.execute(
        "UPDATE sessions SET first_prompt=?1, last_assistant_text=?1, title='Profile session', models_json='[\"gpt-5\",\"gpt-5-codex\"]', workspace_roots_json='[\"/work/repo\",\"/work/library\"]', git_branch='feature', originator='codex_cli_rs', agent_version='0.148.0', initial_commit='abcdef', session_kind='main' WHERE session_id='admitted'",
        [&excerpt],
    ).unwrap();
    f.capture(); // settle revisions before measuring unchanged work
    f.conn.flush_prepared_statement_cache();
    let reads = Arc::new(Mutex::new(
        std::collections::BTreeMap::<String, usize>::new(),
    ));
    let observed = Arc::clone(&reads);
    f.conn.authorizer(Some(move |context: AuthContext<'_>| {
        if let AuthAction::Read {
            table_name,
            column_name,
        } = context.action
        {
            *observed
                .lock()
                .unwrap()
                .entry(format!("{table_name}.{column_name}"))
                .or_default() += 1;
        }
        Authorization::Allow
    }));
    let steps = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = Arc::clone(&steps);
    f.conn.progress_handler(
        1,
        Some(move || {
            counted.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            false
        }),
    );
    let started = std::time::Instant::now();
    for _ in 0..1000 {
        assert_local_work(f.capture());
    }
    let elapsed = started.elapsed();
    f.conn.progress_handler(0, None::<fn() -> bool>);
    f.conn
        .authorizer(None::<fn(AuthContext<'_>) -> Authorization>);
    println!("EXACT_CAPTURE_CATALOG_PROFILE calls=1000 distinct_paths=1 excerpt_bytes_each={excerpt_bytes} source_bytes={} sqlite_vm_steps={} instrumented_wall_us={} prepare_reads={:?}", fs::metadata(&f.path).unwrap().len(), steps.load(std::sync::atomic::Ordering::Relaxed), elapsed.as_micros(), reads.lock().unwrap());
    assert_eq!(
        f.conn
            .query_row(
                "SELECT first_prompt FROM sessions WHERE session_id='admitted'",
                [],
                |row| row.get::<_, String>(0)
            )
            .unwrap(),
        excerpt
    );
}

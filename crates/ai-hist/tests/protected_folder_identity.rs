//! Project identity under Desktop, Documents, and Downloads.
//!
//! Its own test binary because it sets `HOME` for the process.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use ai_hist::{CatalogQuery, DiscoveryOptions, ProviderRoots, SessionStore, Source, StoreOptions};

static ENV_LOCK: Mutex<()> = Mutex::new(());

struct Fixture {
    home: PathBuf,
    dotdot: String,
    downloads: String,
    documents: String,
    escape: String,
    lower: String,
}

fn write_origin(dir: &Path, url: &str) {
    let git = dir.join(".git");
    fs::create_dir_all(&git).unwrap();
    fs::write(
        git.join("config"),
        format!("[remote \"origin\"]\n\turl = {url}\n"),
    )
    .unwrap();
}

fn rollout(home: &Path, id: &str, cwd: &str, repo_url: Option<&str>) {
    let day = home.join(".codex/sessions/2026/10/12");
    fs::create_dir_all(&day).unwrap();
    let mut payload = serde_json::json!({ "id": id, "cwd": cwd });
    if let Some(url) = repo_url {
        payload["git"] = serde_json::json!({ "repository_url": url });
    }
    let meta = serde_json::json!({
        "timestamp": "2026-10-12T12:00:00Z",
        "type": "session_meta",
        "payload": payload,
    });
    let user = serde_json::json!({
        "timestamp": "2026-10-12T12:00:01Z",
        "type": "event_msg",
        "payload": { "type": "user_message", "message": "hello" },
    });
    fs::write(
        day.join(format!("rollout-{id}.jsonl")),
        format!("{meta}\n{user}\n"),
    )
    .unwrap();
}

fn fixture() -> (tempfile::TempDir, Fixture) {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().to_path_buf();
    // A walk that leaves the protected folder finds this remote. The protected
    // directories themselves are never created.
    write_origin(&home, "git@github.com:Org/Home.git");
    let backup = home.join("Desktop-backup/app");
    fs::create_dir_all(&backup).unwrap();
    write_origin(&backup, "git@github.com:Org/Backup.git");
    let projects = home.join("Projects/app");
    fs::create_dir_all(&projects).unwrap();
    write_origin(&projects, "git@github.com:Org/App.git");

    let dotdot = format!("{}/Desktop/example/../example", home.display());
    let downloads = format!("{}/Downloads/no-such", home.display());
    let documents = format!("{}/Documents/no-such", home.display());
    let recorded = format!("{}/Desktop/missing-repo", home.display());
    let escape = format!("{}/Desktop/../Projects/app", home.display());
    let lower = format!("{}/desktop/no-such", home.display());
    rollout(&home, "dotdot", &dotdot, None);
    rollout(&home, "downloads", &downloads, None);
    rollout(&home, "documents", &documents, None);
    rollout(
        &home,
        "recorded",
        &recorded,
        Some("git@github.com:Org/Repo.git"),
    );
    rollout(&home, "escape", &escape, None);
    rollout(&home, "lower", &lower, None);
    rollout(&home, "backup", &backup.to_string_lossy(), None);
    rollout(&home, "projects", &projects.to_string_lossy(), None);
    (
        temp,
        Fixture {
            home,
            dotdot,
            downloads,
            documents,
            escape,
            lower,
        },
    )
}

fn isolate(home: &Path) {
    std::env::set_var("HOME", home);
    std::env::set_var("USERPROFILE", home);
    std::env::set_var("GIT_CONFIG_NOSYSTEM", "1");
    let global = home.join("empty.gitconfig");
    fs::write(&global, "").unwrap();
    std::env::set_var("GIT_CONFIG_GLOBAL", &global);
    std::env::set_var("XDG_CONFIG_HOME", home.join("xdg-config"));
    std::env::remove_var("AI_HIST_DB");
}

fn open(home: &Path, name: &str) -> SessionStore {
    SessionStore::open(StoreOptions::default().db_path(home.join(name)).roots(
        ProviderRoots::from_home(home.to_path_buf(), home.join("missing-opencode.db")),
    ))
    .unwrap()
}

fn discover(store: &SessionStore, skip: bool) -> ai_hist::DiscoveryReport {
    let report = store
        .discover(
            DiscoveryOptions::default()
                .sources([Source::Codex])
                .skip_protected_folder_identity(skip),
        )
        .unwrap();
    assert!(
        report.diagnostics.is_empty(),
        "discovery failed: {:?}",
        report.diagnostics
    );
    report
}

fn project_key(store: &SessionStore, id: &str) -> (String, String) {
    let mut found = None;
    for row in store.sessions(CatalogQuery::default()) {
        let row = row.unwrap();
        if row.session_id == id {
            found = Some((
                row.project_key.expect("project key"),
                row.project_key_method.expect("project key method"),
            ));
        }
    }
    found.unwrap_or_else(|| panic!("no session {id}"))
}

#[test]
fn policy_on_keeps_the_raw_protected_cwd_and_a_second_pass_does_not_resolve_it() {
    let _lock = ENV_LOCK.lock().unwrap();
    let (temp, fix) = fixture();
    isolate(&fix.home);
    let store = open(&fix.home, "policy-on.db");
    let first = discover(&store, true);
    assert!(first.discovered >= 8, "{first:?}");
    let path = |cwd: &str| (cwd.to_string(), "path".to_string());
    assert_eq!(project_key(&store, "dotdot"), path(&fix.dotdot));
    assert_eq!(project_key(&store, "downloads"), path(&fix.downloads));
    assert_eq!(project_key(&store, "documents"), path(&fix.documents));
    assert_eq!(project_key(&store, "escape"), path(&fix.escape));
    assert_eq!(
        project_key(&store, "recorded"),
        ("github.com/Org/Repo".to_string(), "remote".to_string())
    );
    assert_eq!(
        project_key(&store, "backup"),
        ("github.com/Org/Backup".to_string(), "remote".to_string())
    );
    assert_eq!(
        project_key(&store, "projects"),
        ("github.com/Org/App".to_string(), "remote".to_string())
    );
    let lower = project_key(&store, "lower");
    if cfg!(target_os = "macos") {
        assert_eq!(lower, path(&fix.lower));
    } else {
        assert_eq!(
            lower,
            ("github.com/Org/Home".to_string(), "remote".to_string())
        );
    }

    let second = discover(&store, true);
    assert!(
        second.skipped_unchanged >= 8,
        "the second pass must serve cached rows, not re-read them: {second:?}"
    );
    assert_eq!(
        project_key(&store, "dotdot"),
        path(&fix.dotdot),
        "a later pass resolved a protected directory"
    );
    assert_eq!(project_key(&store, "escape"), path(&fix.escape));
    assert_eq!(project_key(&store, "downloads"), path(&fix.downloads));
    drop(store);
    drop(temp);
}

#[test]
fn policy_off_still_resolves_git_remotes() {
    let _lock = ENV_LOCK.lock().unwrap();
    let (temp, fix) = fixture();
    isolate(&fix.home);
    let store = open(&fix.home, "policy-off.db");
    discover(&store, false);
    let remote = |key: &str| (key.to_string(), "remote".to_string());
    // The protected path does not exist. Resolving it walks up to the home
    // repository; that is today's behavior when the option is off.
    assert_eq!(project_key(&store, "dotdot"), remote("github.com/Org/Home"));
    assert_eq!(
        project_key(&store, "projects"),
        remote("github.com/Org/App")
    );
    assert_eq!(
        project_key(&store, "backup"),
        remote("github.com/Org/Backup")
    );
    assert_eq!(
        project_key(&store, "recorded"),
        remote("github.com/Org/Repo")
    );
    drop(store);
    drop(temp);
}

/// Plugin intake is a second discovery pass. The flag has to travel with the
/// observation request; the built-in discover option does not apply there.
#[cfg(feature = "unstable-internal")]
#[test]
fn a_plugin_observation_keeps_a_protected_cwd_raw() {
    use ai_hist::source_intake::{apply_source_observations, ApplyObservationsRequest};
    use ai_hist::{SessionLocation, ShallowSession};

    let _lock = ENV_LOCK.lock().unwrap();
    let (temp, fix) = fixture();
    isolate(&fix.home);
    let cwd = format!("{}/Desktop/plugin-app", fix.home.display());
    let apply = |name: &str, skip: bool| {
        let db = fix.home.join(name);
        apply_source_observations(ApplyObservationsRequest {
            db_path: Some(db.clone()),
            connector_id: "plugin".into(),
            connector_instance: "one".into(),
            location: SessionLocation::Local,
            skip_protected_folder_identity: skip,
            observations: vec![ShallowSession {
                source: "codex".into(),
                session_id: "plugin-desktop".into(),
                cwd: Some(cwd.clone()),
                raw_path: Some("plugin:one".into()),
                source_stamp: Some("listing1".into()),
                ..Default::default()
            }
            .into()],
        })
        .unwrap();
        let store = SessionStore::open(StoreOptions::default().db_path(db).roots(
            ProviderRoots::from_home(fix.home.clone(), fix.home.join("missing-opencode.db")),
        ))
        .unwrap();
        project_key(&store, "plugin-desktop")
    };
    assert_eq!(
        apply("plugin-on.db", true),
        (cwd.clone(), "path".to_string())
    );
    assert_eq!(
        apply("plugin-off.db", false),
        ("github.com/Org/Home".to_string(), "remote".to_string())
    );
    drop(temp);
}

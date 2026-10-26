use super::*;

#[test]
fn global_cli_fixture_matches_pinned_shape() {
    let bytes = include_bytes!("../../../tests/fixtures/qoder/global-cli/.qoder/projects/-synthetic-qoder-project/synthetic-tool.jsonl");
    let entries = format::parse(bytes, false).unwrap();
    assert_eq!(
        format::string(&entries[0], "sessionId").unwrap(),
        "synthetic-tool"
    );
    assert_eq!(
        format::first_prompt(&entries).as_deref(),
        Some("Read note.txt.")
    );
    assert_eq!(entries.iter().filter(|e| e["type"] == "user").count(), 2);
    assert!(entries.iter().any(|e| e["message"]["content"]
        .as_array()
        .is_some_and(|blocks| blocks.iter().any(|block| block["tool_use_id"] == "call-1"))));
}

#[test]
fn relative_config_root_is_frozen_against_the_opening_cwd() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    let opening_cwd = root.path().join("first-workspace");
    let config = expand(&home, "relative-qoder-config", || Ok(opening_cwd.clone())).unwrap();
    assert_eq!(
        config.join("projects"),
        opening_cwd.join("relative-qoder-config/projects")
    );
    assert_eq!(
        expand(&home, "~/.qoder", || Ok(opening_cwd.clone())).unwrap(),
        home.join(".qoder")
    );
    assert_eq!(
        expand(&home, home.to_str().unwrap(), || Ok(opening_cwd)).unwrap(),
        home
    );
}

#[test]
fn absolute_config_and_default_do_not_require_a_live_cwd() {
    let home = tempfile::tempdir().unwrap();
    let no_cwd = || Err(std::io::Error::from(std::io::ErrorKind::NotFound));
    let absolute = home.path().join("absolute-qoder");
    assert_eq!(
        expand(home.path(), "~/.qoder", no_cwd).unwrap(),
        home.path().join(".qoder")
    );
    assert_eq!(
        expand(home.path(), absolute.to_str().unwrap(), no_cwd).unwrap(),
        absolute
    );
    assert_eq!(
        expand(home.path(), "relative-qoder", no_cwd)
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::NotFound
    );
}

#[cfg(unix)]
#[test]
fn selected_transcript_symlink_cannot_escape_declared_root() {
    let home = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let root = home.path().join(".qoder/projects");
    let path = root.join("project/synthetic-tool.jsonl");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let secret = outside.path().join("synthetic-tool.jsonl");
    std::fs::write(&secret, b"synthetic external bytes").unwrap();
    std::os::unix::fs::symlink(&secret, &path).unwrap();
    let roots = ProviderRoots::from_home(home.path().into(), home.path().join("opencode.db"));
    assert!(AcquisitionContext::new(ID, &roots).validate(&path).is_err());
}

#[cfg(unix)]
#[test]
fn scoped_enumeration_does_not_open_unrelated_project() {
    use std::os::unix::fs::PermissionsExt;
    let home = tempfile::tempdir().unwrap();
    let root = home.path().join(".qoder/projects");
    let selected = root.join("selected/session.jsonl");
    std::fs::create_dir_all(selected.parent().unwrap()).unwrap();
    std::fs::write(&selected, b"{}\n").unwrap();
    let unrelated = root.join("unrelated");
    std::fs::create_dir_all(&unrelated).unwrap();
    struct RestorePermissions(PathBuf, std::fs::Permissions);
    impl Drop for RestorePermissions {
        fn drop(&mut self) {
            let _ = std::fs::set_permissions(&self.0, self.1.clone());
        }
    }
    let _restore = RestorePermissions(
        unrelated.clone(),
        std::fs::metadata(&unrelated).unwrap().permissions(),
    );
    std::fs::set_permissions(&unrelated, std::fs::Permissions::from_mode(0o000)).unwrap();
    if std::fs::read_dir(&unrelated).is_ok() {
        // Privileged test runners can bypass directory permissions.
        return;
    }
    let conn = Connection::open_in_memory().unwrap();
    let roots = ProviderRoots::from_home(home.path().into(), home.path().join("opencode.db"));
    let scope = crate::ingest::path_scope::PathScope::from_paths([selected.clone()]);
    let env = DiscoveryEnv::with_provider_roots(&conn, roots).with_path_scope(scope);
    let candidates = Provider.enumerate(&env, None).unwrap();
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].locator, selected.to_string_lossy());
}

#[test]
fn rejected_oversized_opening_is_counted_as_discovery_work() {
    let home = tempfile::tempdir().unwrap();
    let root = home.path().join(".qoder/projects/project");
    std::fs::create_dir_all(&root).unwrap();
    let fixture = include_str!("../../../tests/fixtures/qoder/global-cli/.qoder/projects/-synthetic-qoder-project/synthetic-tool.jsonl");
    std::fs::write(root.join("good.jsonl"), fixture).unwrap();
    let oversized = format!(
        "{{\"type\":\"user\",\"padding\":\"{}\"}}\n",
        "X".repeat(8 * 1024 * 1024)
    );
    std::fs::write(root.join("oversized.jsonl"), oversized).unwrap();
    let conn = crate::open_db(&home.path().join("catalog.db")).unwrap();
    let roots = ProviderRoots::from_home(home.path().into(), home.path().join("opencode.db"));
    let env = DiscoveryEnv::with_provider_roots(&conn, roots);
    let summary = crate::discover::discover_sessions_with_env(
        &env,
        &crate::discover::DiscoverOptions::new(crate::SessionScope::Local, vec![ID.into()], None),
        |_| {},
    )
    .unwrap();
    assert_eq!(summary.discovered, 1);
    assert!(summary
        .diagnostics
        .iter()
        .any(|d| d.error.contains("QODER_OVERSIZED_OPENING_RECORD")));
    assert_eq!(summary.counters.files_opened, 2);
    assert!(summary.counters.bytes_read >= 8 * 1024 * 1024);
}

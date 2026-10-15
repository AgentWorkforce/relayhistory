use super::*;
const FIXTURE: &[u8] = include_bytes!(
    "../../../tests/fixtures/omp/title-v3/.omp/agent/sessions/--fixture--/session.jsonl"
);
#[test]
fn omp_title_overlay_legacy_rejection_and_partial_usage() {
    let entries = format::parse(FIXTURE, false).unwrap();
    assert_eq!(entries[0]["title"], "Synthetic initial title");
    let direct = &FIXTURE[256..];
    assert!(format::parse(direct, false)
        .unwrap_err()
        .to_string()
        .contains("AMBIGUOUS_LEGACY"));
    let mut bad = FIXTURE.to_vec();
    bad.remove(254);
    assert!(format::parse(&bad, false).is_err());
    let usage = usage::normalize(&entries[2]["message"]["usage"])
        .unwrap()
        .unwrap();
    assert_eq!(usage.input_tokens, 12);
    assert!(!usage.coverage.has_output_tokens);
    assert!(usage::normalize(&serde_json::json!({})).unwrap().is_none());
}
#[test]
fn omp_frozen_root_precedence_profiles_and_xdg() {
    use roots::RootValues;
    let home = tempfile::tempdir().unwrap();
    let h = home.path();
    assert_eq!(
        roots::from_values(h, &RootValues::default()),
        [h.join(".omp/agent/sessions")]
    );
    let values = RootValues {
        session: Some("~/custom".into()),
        agent: Some("~/ignored".into()),
        profile: Some("work".into()),
        ..Default::default()
    };
    assert_eq!(roots::from_values(h, &values), [h.join("custom")]);
    let values = RootValues {
        agent: Some("~/ignored".into()),
        profile: Some("work".into()),
        ..Default::default()
    };
    assert_eq!(
        roots::from_values(h, &values),
        [h.join(".omp/profiles/work/agent/sessions")]
    );
    let values = RootValues {
        agent: Some("~/chosen".into()),
        profile: Some("".into()),
        ..Default::default()
    };
    assert_eq!(roots::from_values(h, &values), [h.join("chosen/sessions")]);
    let data = h.join("data");
    std::fs::create_dir_all(data.join("omp")).unwrap();
    let mut values = RootValues {
        data: Some(data.to_string_lossy().into_owned()),
        ..Default::default()
    };
    if cfg!(any(target_os = "linux", target_os = "macos")) {
        assert_eq!(roots::from_values(h, &values), [data.join("omp/sessions")]);
        values.profile = Some("work".into());
        assert_eq!(
            roots::from_values(h, &values),
            [h.join(".omp/profiles/work/agent/sessions")]
        );
        std::fs::create_dir_all(data.join("omp/profiles/work")).unwrap();
        assert_eq!(
            roots::from_values(h, &values),
            [data.join("omp/profiles/work/sessions")]
        );
    }
    values.profile = Some("../escape".into());
    assert!(roots::from_values(h, &values).is_empty());
}
#[test]
fn omp_snapshot_immutable_and_shallow_bounded() {
    let home = tempfile::tempdir().unwrap();
    let roots = ProviderRoots::from_home(home.path().into(), home.path().join("opencode.db"));
    let path = roots.for_provider(ID)[0].join("session.jsonl");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, FIXTURE).unwrap();
    let context = AcquisitionContext::new(ID, &roots);
    let captured = snapshot(&path, &context).unwrap();
    std::fs::write(&path, vec![b'x'; 128 * 1024]).unwrap();
    assert_eq!(
        captured.payload["entries"][0]["title"],
        "Synthetic initial title"
    );
    assert_eq!(prefix(&context, &path).unwrap().len(), HEAD_LIMIT as usize);
}

#[test]
fn omp_environment_profile_precedence_and_frozen_errors() {
    const CHILD: &str = "RH_OMP_ROOT_TEST_CHILD";
    if let Ok(mode) = std::env::var(CHILD) {
        let home = PathBuf::from(std::env::var("RH_OMP_ROOT_TEST_HOME").unwrap());
        let resolved = roots::resolve(&home, true);
        if mode == "invalid" {
            assert!(resolved.paths.is_empty());
            assert!(resolved.error.unwrap().contains("OMP_INVALID_PROFILE"));
            let frozen = ProviderRoots::from_env(home.clone());
            let json = serde_json::to_string(&frozen).unwrap();
            std::env::set_var("OMP_PROFILE", "valid-now");
            let frozen: ProviderRoots = serde_json::from_str(&json).unwrap();
            assert!(frozen.for_provider(ID).is_empty());
            assert!(frozen
                .ensure_provider_roots(ID)
                .unwrap_err()
                .to_string()
                .contains("OMP_INVALID_PROFILE"));
            let store = crate::SessionStore::open(
                crate::StoreOptions::default()
                    .db_path(home.join("frozen.db"))
                    .roots(frozen),
            )
            .unwrap();
            assert!(store
                .discover(crate::DiscoveryOptions::default().sources([crate::Source::Omp]))
                .is_err());
            store
                .sync(crate::SyncOptions::default().force(true))
                .unwrap();
            let conn = crate::open_db(store.db_path()).unwrap();
            let mut state = Map::new();
            assert!(sweep(SweepContext {
                conn: &conn,
                state: &mut state,
                roots: store.roots(),
                paths: None,
                repairs: &Default::default()
            })
            .err()
            .unwrap()
            .to_string()
            .contains("OMP_INVALID_PROFILE"));
        } else {
            assert_eq!(resolved.paths, [home.join("chosen/sessions")]);
            assert!(resolved.error.is_none());
        }
        assert_eq!(
            roots::resolve(&home, false).paths,
            [home.join(".omp/agent/sessions")]
        );
        return;
    }
    let home = tempfile::tempdir().unwrap();
    for (mode, profile) in [("invalid", "../escape"), ("empty-default", "")] {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .env_clear()
            .args(["--exact", "ingest::providers::omp::tests::omp_environment_profile_precedence_and_frozen_errors", "--nocapture"])
            .env(CHILD, mode)
            .env("RH_OMP_ROOT_TEST_HOME", home.path())
            .env("OMP_PROFILE", profile)
            .env("PI_PROFILE", "legacy")
            .env("PI_CODING_AGENT_DIR", home.path().join("chosen"))
            .env_remove("PI_CODING_AGENT_SESSION_DIR")
            .env_remove("PI_CONFIG_DIR")
            .env_remove("XDG_DATA_HOME")
            .output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            String::from_utf8_lossy(&output.stdout).contains("1 passed"),
            "subprocess must execute the named test: {}",
            String::from_utf8_lossy(&output.stdout)
        );
    }
}

#[test]
fn omp_empty_title_slot_clears_stale_header_and_header_only_is_diagnostic() {
    let mut slot: Value = serde_json::from_slice(&FIXTURE[..255]).unwrap();
    let old = slot["title"].as_str().unwrap().len();
    slot["title"] = "".into();
    slot["pad"] = format!("{}{}", slot["pad"].as_str().unwrap(), " ".repeat(old)).into();
    let prefix = format!("{}\n", serde_json::to_string(&slot).unwrap());
    assert_eq!(prefix.len(), 256);
    assert!(format::parse(prefix.as_bytes(), false)
        .unwrap_err()
        .to_string()
        .contains("INCOMPLETE_HEADER"));
    let bytes = [prefix.as_bytes(), &FIXTURE[256..]].concat();
    let entries = format::parse(&bytes, false).unwrap();
    assert!(entries[0].get("title").is_none());
}

#[test]
fn omp_execution_messages_are_control_markers_and_do_not_hide_conversation() {
    let entries = format::parse(FIXTURE, false).unwrap();
    let mut control = entries[1].clone();
    control["id"] = "execution-entry".into();
    control["message"] = serde_json::json!({"role":"bashExecution","command":"echo synthetic","output":"synthetic","timestamp":1767225600000_i64});
    let bytes = [FIXTURE, format!("{}\n", control).as_bytes()].concat();
    assert_eq!(format::parse(&bytes, false).unwrap().len(), 5);
}

use super::*;

fn path(root: &Path, slot: &str, suffix: &str) -> PathBuf {
    root.join(format!(
        "{}.blob",
        format::encode_key(&format!("sand.client.slice.account.{slot}.{suffix}"))
    ))
}

#[test]
fn rosters_are_counted_once_per_account_and_refreshed_each_pass() {
    let home = tempfile::tempdir().unwrap();
    let roots = ProviderRoots::from_home(home.path().into(), home.path().join("opencode.db"));
    let root = &roots.for_provider(ID)[0];
    std::fs::create_dir_all(root).unwrap();
    let roster = path(root, "first", "roster.last-roster");
    let second_roster = path(root, "second", "roster.last-roster");
    let initial = br#"{"value":{"rows":[{"id":"bot","name":"First"}]}}"#;
    let second = br#"{"value":{"rows":[{"id":"bot","name":"Second"}]}}"#;
    std::fs::write(&roster, initial).unwrap();
    std::fs::write(&second_roster, second).unwrap();
    let first = path(root, "first", "transcript.replicas.bot");
    let missing = path(root, "first", "transcript.replicas.retired");
    let second_bot = path(root, "second", "transcript.replicas.bot");
    let conn = Connection::open_in_memory().unwrap();
    let env = DiscoveryEnv::with_provider_roots(&conn, roots);
    let scan = env.scan();
    let cache = RosterCache::default();
    let pass = cache.begin();
    assert_eq!(cache.title(&missing, &scan), None);
    assert_eq!(env.counters().files_opened, 1);
    assert_eq!(env.counters().bytes_read, initial.len() as u64);
    assert_eq!(cache.title(&first, &scan).as_deref(), Some("First"));
    assert_eq!(cache.title(&second_bot, &scan).as_deref(), Some("Second"));
    assert_eq!(env.counters().files_opened, 2);
    assert_eq!(
        env.counters().bytes_read,
        (initial.len() + second.len()) as u64
    );
    let updated = br#"{"value":{"rows":[{"id":"bot","name":"Renamed"}]}}"#;
    std::fs::write(&roster, updated).unwrap();
    assert_eq!(cache.title(&first, &scan).as_deref(), Some("First"));
    assert_eq!(env.counters().files_opened, 2);
    drop(pass);
    let _pass = cache.begin();
    assert_eq!(cache.title(&first, &scan).as_deref(), Some("Renamed"));
    assert_eq!(env.counters().files_opened, 3);
    assert_eq!(
        env.counters().bytes_read,
        (initial.len() + second.len() + updated.len()) as u64
    );
}

#[test]
fn malformed_roster_reads_are_counted_and_cached() {
    let home = tempfile::tempdir().unwrap();
    let roots = ProviderRoots::from_home(home.path().into(), home.path().join("opencode.db"));
    let root = &roots.for_provider(ID)[0];
    std::fs::create_dir_all(root).unwrap();
    std::fs::write(path(root, "slot", "roster.last-roster"), b"not JSON").unwrap();
    let transcript = path(root, "slot", "transcript.replicas.bot");
    let conn = Connection::open_in_memory().unwrap();
    let env = DiscoveryEnv::with_provider_roots(&conn, roots);
    let cache = RosterCache::default();
    let _pass = cache.begin();
    for _ in 0..2 {
        assert_eq!(cache.title(&transcript, &env.scan()), None);
    }
    assert_eq!(env.counters().files_opened, 1);
    assert_eq!(env.counters().bytes_read, 8);
}

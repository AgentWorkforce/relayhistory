//! Persistent failures must not monopolize healthy coverage or lose retry debt.
use super::*;

#[derive(Clone, Copy, Debug)]
enum Failure {
    Open,
    Read,
    Entry,
}
struct ResetHooks;
impl Drop for ResetHooks {
    fn drop(&mut self) {
        FAIL_ENTRY_PATH.with(|p| *p.borrow_mut() = None);
        FAIL_READ_DIRECTORY.with(|p| *p.borrow_mut() = None);
    }
}
fn codex(path: &std::path::Path, id: &str) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, format!("{{\"timestamp\":\"2026-10-23T12:00:00Z\",\"type\":\"session_meta\",\"payload\":{{\"id\":\"{id}\",\"cwd\":\"/synthetic\"}}}}\n{{\"timestamp\":\"2026-10-23T12:00:01Z\",\"type\":\"event_msg\",\"payload\":{{\"type\":\"user_message\",\"message\":\"fixture\"}}}}\n")).unwrap();
}
fn ids(store: &SessionStore) -> HashSet<String> {
    store
        .sessions(Default::default())
        .map(|r| r.unwrap().session_id)
        .collect()
}
fn step(round: &mut Reconciliation, shallow: bool) -> Result<ReconciliationStep, Error> {
    if shallow {
        round.discover_step(2, 1, Default::default())
    } else {
        round.step(2, 1, SyncOptions::default())
    }
}
fn drain_healthy(round: &mut Reconciliation, store: &SessionStore, shallow: bool) -> (usize, bool) {
    let mut errors = 0;
    let mut retry_while_ready = false;
    for _ in 0..256 {
        let ready_before = !round.healthy_pass_exhausted();
        let healthy_tail_missing = ids(store)
            .iter()
            .filter(|id| id.starts_with("main-"))
            .count()
            < 16;
        match step(round, shallow) {
            Ok(s) => {
                assert!(s.entries <= 2 && s.paths <= 1);
                assert!(!s.complete);
            }
            Err(Error::Discovery(_)) => {
                errors += 1;
                retry_while_ready |= errors > 1 && ready_before && healthy_tail_missing;
            }
            Err(error) => panic!("unexpected {error:?}"),
        }
        for source in &round.sources {
            assert!(usize::from(source.current.is_some()) + source.paused.len() <= 2);
        }
        if round.healthy_pass_exhausted() && errors >= 3 {
            return (errors, retry_while_ready);
        }
    }
    panic!("healthy work did not drain");
}
fn persistent_case(shallow: bool, failure: Failure) {
    let _reset = ResetHooks;
    let (_temp, store, project) = fixture();
    write(&project.join("other.jsonl"), "other", "u1");
    let archive = store.roots().codex.join("archived_sessions");
    let bad_entry = archive.join("2026/10/23/rollout-repaired.jsonl");
    fs::create_dir_all(&store.roots().codex).unwrap();
    match failure {
        Failure::Open => fs::write(&archive, b"not a directory").unwrap(),
        Failure::Read => {
            codex(&bad_entry, "repaired");
            FAIL_READ_DIRECTORY.with(|p| *p.borrow_mut() = Some(archive.clone()));
        }
        Failure::Entry => {
            codex(&bad_entry, "repaired");
            codex(
                &archive.join("2026/10/23/rollout-archive-good.jsonl"),
                "archive-good",
            );
            FAIL_ENTRY_PATH.with(|p| *p.borrow_mut() = Some(bad_entry.clone()));
        }
    }
    for n in 0..16 {
        codex(
            &store
                .roots()
                .codex
                .join(format!("sessions/2026/10/23/rollout-main-{n}.jsonl")),
            &format!("main-{n}"),
        );
    }
    let mut round = store
        .reconcile()
        .unwrap()
        .sources([Source::Claude, Source::Codex]);
    let (errors, fair_retry) = drain_healthy(&mut round, &store, shallow);
    assert!(
        fair_retry,
        "failed obligations must retry before the healthy pass/backstop ends"
    );
    let before = ids(&store);
    assert!(before.contains("other"));
    for n in 0..16 {
        assert!(
            before.contains(&format!("main-{n}")),
            "same-source healthy root starved: {failure:?}"
        );
    }
    assert!(!before.contains("repaired"));
    // The level query remains useful after the error, and coverage is not complete.
    let mut error_at_boundary = false;
    for _ in 0..8 {
        match step(&mut round, shallow) {
            Err(Error::Discovery(_)) => {
                assert!(round.healthy_pass_exhausted());
                error_at_boundary = true;
                break;
            }
            Ok(s) => assert!(!s.complete),
            Err(error) => panic!("unexpected {error:?}"),
        }
    }
    assert!(error_at_boundary);
    let debt: usize = round.sources.iter().map(|s| s.deferred.len()).sum();
    assert!(debt > 0 && errors >= 3);
    write(&project.join("other-late.jsonl"), "other-late", "u2");
    codex(
        &store
            .roots()
            .codex
            .join("sessions/2026/10/23/rollout-late.jsonl"),
        "late",
    );
    assert!(round.refresh_healthy_pass());
    assert_eq!(
        round
            .sources
            .iter()
            .map(|s| s.deferred.len())
            .sum::<usize>(),
        debt
    );
    assert!(
        !round.refresh_healthy_pass(),
        "active traversal must not be reset"
    );
    drain_healthy(&mut round, &store, shallow);
    let refreshed = ids(&store);
    assert!(
        refreshed.contains("late") && refreshed.contains("other-late"),
        "refresh must include completed siblings"
    );
    assert_eq!(
        round
            .sources
            .iter()
            .map(|s| s.deferred.len())
            .sum::<usize>(),
        debt,
        "refresh must deduplicate failed obligations"
    );
    match failure {
        Failure::Open => {
            fs::remove_file(&archive).unwrap();
            codex(&bad_entry, "repaired");
        }
        Failure::Read => FAIL_READ_DIRECTORY.with(|p| *p.borrow_mut() = None),
        Failure::Entry => FAIL_ENTRY_PATH.with(|p| *p.borrow_mut() = None),
    }
    let mut complete = false;
    for _ in 0..256 {
        let s = step(&mut round, shallow).unwrap();
        assert!(s.entries <= 2 && s.paths <= 1);
        if s.complete {
            complete = true;
            break;
        }
    }
    assert!(
        complete && ids(&store).contains("repaired"),
        "same-round repair must retain the obligation"
    );
}
#[test]
fn recovery_v44_capture_persistent_open_failure() {
    persistent_case(false, Failure::Open);
}
#[test]
fn recovery_v44_capture_persistent_read_failure() {
    persistent_case(false, Failure::Read);
}
#[test]
fn recovery_v44_capture_persistent_entry_failure() {
    persistent_case(false, Failure::Entry);
}
#[test]
fn recovery_v44_discovery_persistent_open_failure() {
    persistent_case(true, Failure::Open);
}
#[test]
fn recovery_v44_discovery_persistent_read_failure() {
    persistent_case(true, Failure::Read);
}
#[test]
fn recovery_v44_discovery_persistent_entry_failure() {
    persistent_case(true, Failure::Entry);
}
#[test]
fn recovery_v44_refresh_refuses_pending_and_preserves_completed_source_filter() {
    let (_temp, store, project) = fixture();
    let pending = project.join("pending.jsonl");
    write(&pending, "pending", "u1");
    let mut round = store.reconcile().unwrap().sources([Source::Claude]);
    round.sources[0].roots.clear();
    round.sources[0].pending.push(pending);
    assert!(!round.healthy_pass_exhausted());
    assert!(!round.refresh_healthy_pass());
    assert_eq!(round.sources[0].pending.len(), 1);
    assert!(round.step(1, 1, SyncOptions::default()).unwrap().complete);
    fs::create_dir_all(&store.roots().codex).unwrap();
    fs::write(
        store.roots().codex.join("archived_sessions"),
        "not a directory",
    )
    .unwrap();
    write(&project.join("later.jsonl"), "later", "u2");
    assert!(round.healthy_pass_exhausted() && round.refresh_healthy_pass());
    for _ in 0..64 {
        if round.step(2, 1, SyncOptions::default()).unwrap().complete {
            break;
        }
    }
    assert_eq!(
        ids(&store),
        HashSet::from(["pending".into(), "later".into()])
    );
    assert!(round.sources.is_empty());
}
#[test]
fn recovery_v44_many_repaired_directories_bound_handles_and_keep_all_tails() {
    let (_temp, store, project) = fixture();
    let mut round = store.reconcile().unwrap().sources([Source::Claude]);
    let owner = Source::Claude
        .capabilities()
        .watch_roots(store.roots())
        .into_iter()
        .find(|r| r.scope == WatchScope::Tree)
        .unwrap();
    round.sources[0].roots.clear();
    for n in 0..3 {
        let path = project.join(format!("replaced-{n}"));
        fs::write(&path, "directory replaced before open").unwrap();
        round.sources[0].queued.push_back((path, owner.clone()));
    }
    for _ in 0..16 {
        assert!(matches!(
            round.step(1, 1, SyncOptions::default()),
            Err(Error::Discovery(_))
        ));
        if round.healthy_pass_exhausted() {
            break;
        }
    }
    assert_eq!(round.sources[0].deferred.len(), 3);
    for n in 0..3 {
        let path = project.join(format!("replaced-{n}"));
        fs::remove_file(&path).unwrap();
        fs::create_dir_all(&path).unwrap();
        for file in 0..3 {
            write(
                &path.join(format!("{file}.jsonl")),
                &format!("s-{n}-{file}"),
                "u",
            );
        }
    }
    let mut done = false;
    for _ in 0..128 {
        let s = round.step(1, 1, SyncOptions::default()).unwrap();
        assert!(s.entries <= 1 && s.paths <= 1);
        for source in &round.sources {
            assert!(usize::from(source.current.is_some()) + source.paused.len() <= 2);
        }
        if s.complete {
            done = true;
            break;
        }
    }
    assert!(done);
    assert_eq!(ids(&store).len(), 9);
}

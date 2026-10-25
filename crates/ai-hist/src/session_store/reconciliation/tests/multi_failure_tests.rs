//! Multiple failed iterator reopenings must not consume every healthy turn.
use super::recovery_tests::{codex, ids};
use super::*;

struct Reset;
impl Drop for Reset {
    fn drop(&mut self) {
        FAIL_READ_DIRECTORY.with(|paths| paths.borrow_mut().clear());
        FAILED_DIRECTORY_READS.with(|reads| reads.borrow_mut().clear());
        FAIL_ENTRY_PATH.with(|path| *path.borrow_mut() = None);
    }
}

fn advance(
    round: &mut Reconciliation,
    shallow: bool,
    budget: usize,
) -> Result<ReconciliationStep, Error> {
    if shallow {
        round.discover_step(budget, 1, Default::default())
    } else {
        round.step(budget, 1, Default::default())
    }
}

fn check_slice(round: &Reconciliation, result: Result<ReconciliationStep, Error>, budget: usize) {
    match result {
        Ok(step) => {
            assert!(step.entries <= budget && step.paths <= 1);
            assert!(!step.complete, "unresolved debt is not complete coverage");
        }
        Err(Error::Discovery(_)) => {}
        Err(error) => panic!("unexpected {error:?}"),
    }
    for source in &round.sources {
        assert!(usize::from(source.current.is_some()) + source.paused.len() <= 2);
    }
}

fn healthy_count(store: &SessionStore) -> usize {
    ids(store)
        .iter()
        .filter(|id| id.starts_with("healthy-"))
        .count()
}

fn repeated_reads(paths: &[PathBuf]) -> bool {
    FAILED_DIRECTORY_READS.with(|reads| {
        paths
            .iter()
            .all(|path| reads.borrow().iter().filter(|seen| *seen == path).count() >= 2)
    })
}

fn multiple_readers(shallow: bool, count: usize, mixed: bool) {
    let _reset = Reset;
    let (_temp, store, _) = fixture();
    let archive = store.roots().codex.join("archived_sessions");
    let bad: Vec<_> = (0..count)
        .map(|n| archive.join(format!("bad-{n}")))
        .collect();
    for (n, path) in bad.iter().enumerate() {
        codex(
            &path.join(format!("rollout-repaired-{n}.jsonl")),
            &format!("repaired-{n}"),
        );
    }
    for n in 0..16 {
        codex(
            &store
                .roots()
                .codex
                .join(format!("sessions/rollout-healthy-{n}.jsonl")),
            &format!("healthy-{n}"),
        );
    }
    codex(
        &store
            .roots()
            .codex
            .join("sessions/queued-tail/rollout-nested.jsonl"),
        "nested",
    );
    let open_bad = archive.join("open-bad");
    let type_bad = archive.join("rollout-type-bad.jsonl");
    if mixed {
        fs::create_dir_all(&open_bad).unwrap();
        codex(&type_bad, "type-repaired");
        FAIL_ENTRY_PATH.with(|path| *path.borrow_mut() = Some(type_bad.clone()));
    }
    FAIL_READ_DIRECTORY.with(|paths| *paths.borrow_mut() = bad.clone());
    let mut round = store.reconcile().unwrap().sources([Source::Codex]);
    // This is the retained acquisition state after collect finds a file then
    // encounters an error. It must drain before more failing enumeration.
    if mixed {
        let pending = store.roots().codex.join("sessions/rollout-pending.jsonl");
        codex(&pending, "pending");
        round.sources[0].pending.push(pending);
        assert!(!round.refresh_healthy_pass());
        let first = advance(&mut round, shallow, 1).unwrap();
        assert_eq!(first.paths, 1);
        assert!(ids(&store).contains("pending"));
    }
    let budget = if mixed { 1 } else { 2 };
    let mut replaced = false;
    let mut fair_retries = false;
    let mut paused_healthy = false;
    for _ in 0..1024 {
        // Replace a directory only after ordinary traversal queues it, so its
        // open fails independently of the read and entry-type fault hooks.
        if mixed
            && !replaced
            && round
                .sources
                .iter()
                .any(|s| s.queued.iter().any(|(p, _)| p == &open_bad))
        {
            fs::remove_dir(&open_bad).unwrap();
            fs::write(&open_bad, "directory changed before open").unwrap();
            replaced = true;
        }
        let result = advance(&mut round, shallow, budget);
        check_slice(&round, result, budget);
        let before_tail = healthy_count(&store) < 16;
        fair_retries |= before_tail
            && FAILED_DIRECTORY_READS.with(|reads| {
                bad.iter()
                    .any(|path| reads.borrow().iter().filter(|seen| *seen == path).count() >= 2)
            });
        paused_healthy |= before_tail
            && round.sources.iter().any(|s| {
                s.paused
                    .iter()
                    .any(|r| r.path == store.roots().codex.join("sessions"))
            });

        if round.healthy_pass_exhausted() && repeated_reads(&bad) {
            break;
        }
    }
    assert_eq!(
        healthy_count(&store),
        16,
        "failed readers starved untouched healthy root"
    );
    assert!(round.healthy_pass_exhausted());
    assert!(
        repeated_reads(&bad),
        "each retained failed reader must retry"
    );
    assert!(
        paused_healthy,
        "healthy iterator must resume after being paused beside retry readers"
    );
    assert!(
        ids(&store).contains("nested"),
        "fresh queued healthy tail must also advance"
    );
    assert!(
        fair_retries,
        "failed readers must retry while healthy coverage remains"
    );
    assert_eq!(
        round
            .sources
            .iter()
            .map(|s| s.deferred.len())
            .sum::<usize>(),
        count + if mixed { 2 } else { 0 }
    );
    assert!(!mixed || replaced);

    codex(
        &store.roots().codex.join("sessions/rollout-late.jsonl"),
        "late",
    );
    assert!(round.refresh_healthy_pass());
    for _ in 0..1024 {
        let result = advance(&mut round, shallow, budget);
        check_slice(&round, result, budget);
        if round.healthy_pass_exhausted() {
            break;
        }
    }
    assert!(
        ids(&store).contains("late"),
        "refresh must revisit healthy roots while debt persists"
    );
    assert!(round.healthy_pass_exhausted());

    FAIL_READ_DIRECTORY.with(|paths| paths.borrow_mut().clear());
    FAIL_ENTRY_PATH.with(|path| *path.borrow_mut() = None);
    if mixed {
        fs::remove_file(&open_bad).unwrap();
        codex(
            &open_bad.join("rollout-open-repaired.jsonl"),
            "open-repaired",
        );
    }
    let mut complete = false;
    for _ in 0..1024 {
        let step = advance(&mut round, shallow, budget).unwrap();
        assert!(step.entries <= budget && step.paths <= 1);
        for source in &round.sources {
            assert!(usize::from(source.current.is_some()) + source.paused.len() <= 2);
        }
        if step.complete {
            complete = true;
            break;
        }
    }
    assert!(
        complete,
        "same round must finish after all failed locations repair"
    );
    let captured = ids(&store);
    for n in 0..count {
        assert!(captured.contains(&format!("repaired-{n}")));
    }
    if mixed {
        assert!(
            captured.contains("type-repaired")
                && captured.contains("open-repaired")
                && captured.contains("pending")
        );
    }
}

#[test]
fn recovery_v45_capture_two_readers() {
    multiple_readers(false, 2, false);
}
#[test]
fn recovery_v45_discovery_two_readers() {
    multiple_readers(true, 2, false);
}
#[test]
fn recovery_v45_capture_four_readers() {
    multiple_readers(false, 4, false);
}
#[test]
fn recovery_v45_discovery_four_readers() {
    multiple_readers(true, 4, false);
}
#[test]
fn recovery_v45_capture_mixed_debt_and_pending() {
    multiple_readers(false, 3, true);
}
#[test]
fn recovery_v45_discovery_mixed_debt_and_pending() {
    multiple_readers(true, 3, true);
}

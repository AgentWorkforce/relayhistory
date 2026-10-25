//! Reuse captured bytes where they are owned; parallelize uncached file reads.
use super::*;

fn filesystem_jobs(entries: &mut [WindowEntry<'_>], scan: &ScanEnv<'_>) -> Result<Vec<usize>> {
    let mut jobs = Vec::new();
    for (index, entry) in entries.iter_mut().enumerate() {
        let WindowEntry::Read {
            candidate,
            provider,
            result,
            ..
        } = entry
        else {
            continue;
        };
        if provider.read_access() != ShallowReadAccess::Filesystem {
            continue;
        }
        if crate::ingest::cold_read::has_live_regions(Path::new(&candidate.locator)) {
            crate::ingest::check_capture_cancelled()?;
            // read_bounded_jsonl checks current stat/ctime again. A rewrite
            // after this scheduling decision cannot publish the cached bytes.
            *result = Some(provider.read_shallow(scan, None, candidate));
        } else {
            jobs.push(index);
        }
    }
    Ok(jobs)
}

pub(super) fn read_files(
    entries: &mut [WindowEntry<'_>],
    scan: &ScanEnv<'_>,
    worker_limit: usize,
) -> Result<()> {
    let fs_reads = filesystem_jobs(entries, scan)?;
    let workers = worker_limit.min(fs_reads.len()).min(MAX_READ_WORKERS);
    if workers > 1 {
        let stop = crate::ingest::shared_capture_stop();
        let next = AtomicUsize::new(0);
        let results = Mutex::new(Vec::with_capacity(fs_reads.len()));
        std::thread::scope(|scope| {
            for _ in 0..workers {
                scope.spawn(|| loop {
                    if stop.as_ref().is_some_and(crate::StopToken::is_stopped) {
                        break;
                    }
                    let slot = next.fetch_add(1, Ordering::Relaxed);
                    let Some(&entry_index) = fs_reads.get(slot) else {
                        break;
                    };
                    let WindowEntry::Read {
                        candidate,
                        provider,
                        ..
                    } = &entries[entry_index]
                    else {
                        continue;
                    };
                    let outcome = provider.read_shallow(scan, None, candidate);
                    results
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push((entry_index, outcome));
                });
            }
        });
        let results = results
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (entry_index, outcome) in results {
            if let WindowEntry::Read { result, .. } = &mut entries[entry_index] {
                *result = Some(outcome);
            }
        }
    }
    Ok(())
}

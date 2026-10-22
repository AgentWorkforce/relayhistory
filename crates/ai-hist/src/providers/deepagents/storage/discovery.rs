use super::*;
fn databases(roots: &ProviderRoots) -> Result<(Vec<PathBuf>, usize)> {
    let (paths, unread) = lifecycle::sweep_databases(roots, None)?;
    ensure!(
        !paths.is_empty() || unread == 0,
        "all configured DeepAgents databases are unreadable"
    );
    if unread > 0 {
        eprintln!("DeepAgents discovery skipped {unread} unreadable database root(s)");
    }
    Ok((paths, unread as usize))
}
pub(in super::super) fn enumerate(
    roots: &ProviderRoots,
    limit: Option<usize>,
) -> Result<Vec<Candidate>> {
    let context = AcquisitionContext::new(ID, roots);
    let mut out = vec![];
    let (paths, mut unread) = databases(roots)?;
    for path in paths {
        check_capture_cancelled()?;
        let result = (|| -> Result<(Vec<Candidate>, usize)> {
            let stamp = file_stamp(&context, &path)?;
            read(&context, &path, |conn| {
                let mut candidates = Vec::new();
                let mut unread_threads = 0;
                for thread in thread_ids(conn)? {
                    check_capture_cancelled()?;
                    let recency = match latest_metadata(conn, &thread).and_then(|(_, meta)| {
                        timestamp(&meta, "updated_at")
                            .or_else(|_| latest_checkpoint_time(conn, &thread))
                    }) {
                        Ok(value) => value,
                        Err(error) => {
                            unread_threads += 1;
                            eprintln!("DeepAgents discovery skipped unreadable thread {thread}: {error:#}");
                            continue;
                        }
                    };
                    candidates.push(Candidate {
                        source: ID,
                        locator: path.to_string_lossy().into_owned(),
                        session_id: Some(identity(roots, &path, &thread)),
                        recency_hint_ms: Some(recency),
                        stamp: stamp.clone(),
                    });
                }
                Ok((candidates, unread_threads))
            })
        })();
        match result {
            Ok((candidates, unread_threads)) => {
                unread += unread_threads;
                out.extend(candidates);
            }
            Err(error) => {
                check_capture_cancelled()?;
                unread += 1;
                eprintln!("DeepAgents discovery skipped {}: {error:#}", path.display());
            }
        }
    }
    ensure!(
        !out.is_empty() || unread == 0,
        "all configured DeepAgents conversations are unreadable"
    );
    out.sort_by(|a, b| {
        b.recency_hint_ms
            .cmp(&a.recency_hint_ms)
            .then_with(|| a.session_id.cmp(&b.session_id))
    });
    out.truncate(limit.unwrap_or(MAX_THREADS));
    Ok(out)
}
fn file_stamp(context: &AcquisitionContext<'_>, path: &Path) -> Result<String> {
    let mut stamps = vec![];
    for suffix in ["", "-wal", "-journal"] {
        let file = sidecar(path, suffix);
        if acknowledge(context, &file)? {
            stamps.push(crate::file_stamp_and_modified(&file)?.0);
        }
    }
    Ok(stamps.join("|"))
}
pub(in super::super) fn fingerprint_inputs(roots: &ProviderRoots) -> Result<Vec<Candidate>> {
    let context = AcquisitionContext::new(ID, roots);
    let mut out = vec![];
    let (paths, mut unread) = databases(roots)?;
    for path in paths {
        check_capture_cancelled()?;
        let result = (|| -> Result<Vec<Candidate>> {
            read(&context, &path, |_| Ok(()))?;
            let mut candidates = Vec::new();
            for suffix in ["", "-wal", "-journal"] {
                let file = sidecar(&path, suffix);
                if acknowledge(&context, &file)? {
                    let (stamp, recency_hint_ms) = crate::file_stamp_and_modified(&file)?;
                    candidates.push(Candidate {
                        source: ID,
                        locator: file.to_string_lossy().into_owned(),
                        session_id: None,
                        recency_hint_ms,
                        stamp,
                    });
                }
            }
            Ok(candidates)
        })();
        match result {
            Ok(candidates) => out.extend(candidates),
            Err(error) => {
                check_capture_cancelled()?;
                unread += 1;
                eprintln!(
                    "DeepAgents fingerprint skipped {}: {error:#}",
                    path.display()
                );
            }
        }
    }
    ensure!(
        !out.is_empty() || unread == 0,
        "all configured DeepAgents fingerprints are unreadable"
    );
    Ok(out)
}

//! Bounded and path-scoped acquisition through the shared sync engine.
use super::*;

#[derive(Debug)]
pub(super) struct SyncBaseline {
    pub(super) catalog: CatalogFingerprint,
    pending: std::collections::BTreeSet<SessionRef>,
}

impl SyncBaseline {
    fn new(catalog: CatalogFingerprint) -> Self {
        Self {
            catalog,
            pending: Default::default(),
        }
    }

    fn finish(&mut self, conn: &Connection, succeeded: bool) -> anyhow::Result<Vec<SessionRef>> {
        self.pending
            .extend(catalog_digest::advance(conn, &mut self.catalog)?);
        Ok(if succeeded {
            std::mem::take(&mut self.pending).into_iter().collect()
        } else {
            Vec::new()
        })
    }
}

impl SessionStore {
    /// One full local sweep into this store, under the `SyncRunLock`.
    ///
    /// The sweep is the one `ai-hist sync` runs: every local provider, the
    /// stat-only source fingerprint fast path unless `force`, shallow
    /// discovery at the end. When another process holds the lock this waits
    /// up to [`SyncOptions::lock_timeout_ms`] and then returns
    /// [`Error::SyncLocked`] — never a silent no-op.
    pub fn sync(&self, opts: SyncOptions) -> Result<SyncReport, Error> {
        if self.read_only {
            return Err(Error::read_only("sync"));
        }
        if opts.paths.as_ref().is_some_and(Vec::is_empty)
            || opts.sources.as_ref().is_some_and(Vec::is_empty)
        {
            return Ok(SyncReport {
                head_revision: self.head_revision()?.revision,
                ..Default::default()
            });
        }
        let scope = match &opts.sources {
            None => SweepScope::everything(),
            Some(sources) => SweepScope::only(sources.iter().map(|source| source.as_str())),
        }
        .with_paths(path_scope::scope(
            &self.roots,
            opts.paths.as_deref(),
            opts.sources.as_deref(),
        ));
        let (tick, changed) = crate::providers::codex::sqlite::with_sweep_policy(
            opts.codex_sqlite_max_age_days,
            || {
                controlled(opts.stop.as_ref(), opts.progress.as_ref(), || {
                    self.sync_tick(opts.force, opts.lock_timeout_ms, &scope)
                })
            },
        )?;
        Ok(SyncReport {
            swept: tick.swept,
            changed,
            head_revision: self.head_revision()?.revision,
        })
    }

    /// One cold-round recovery pass over unresolved continuity. Exact path
    /// acquisition has already reconciled the current/affected identities.
    pub(super) fn reconcile_pending(
        &self,
        source: Source,
        options: &SyncOptions,
    ) -> Result<SyncReport, Error> {
        controlled(options.stop.as_ref(), options.progress.as_ref(), || {
            // Only these providers bank ContinuityEvidence during acquisition.
            // Empty/other providers must not open the writable schema merely
            // to finish a directory iterator with no continuity work.
            if !matches!(source, Source::Claude | Source::Codex) {
                return Ok(SyncReport {
                    head_revision: self.head_revision()?.revision,
                    ..Default::default()
                });
            }
            let started = Instant::now();
            let deadline =
                started + Duration::from_millis(options.lock_timeout_ms.min(MAX_LOCK_WAIT_MS));
            loop {
                if crate::ingest::reconcile_pending(&self.db_path, source.as_str())
                    .map_err(Error::sync)?
                {
                    return Ok(SyncReport {
                        head_revision: self.head_revision()?.revision,
                        ..Default::default()
                    });
                }
                crate::ingest::check_capture_cancelled().map_err(Error::sync)?;
                let now = Instant::now();
                if now >= deadline {
                    return Err(Error::SyncLocked {
                        path: self.db_path.clone(),
                        waited_ms: started.elapsed().as_millis() as u64,
                    });
                }
                std::thread::sleep((deadline - now).min(SYNC_LOCK_RETRY));
            }
        })
    }

    /// Run the sweep, retrying a held lock until `lock_timeout_ms` is spent.
    ///
    /// The catalog digest is taken and compared inside the locked section —
    /// see [`sync_facade_tick`] — so `changed` is what the catalog gained
    /// between this sweep taking the lock and releasing it, not since some
    /// earlier read that another process's sync could have moved past.
    pub(super) fn sync_tick(
        &self,
        force: bool,
        lock_timeout_ms: u64,
        scope: &SweepScope,
    ) -> Result<(SyncTick, Vec<SessionRef>), Error> {
        let started = Instant::now();
        // Bounded where it enters, like every watch interval: `Instant +
        // Duration` panics when the sum is not representable, and a caller
        // passing `u64::MAX` to mean "wait as long as it takes" would die
        // here before the first attempt. Seven days is far longer than any
        // lock this crate holds.
        let deadline = started + Duration::from_millis(lock_timeout_ms.min(MAX_LOCK_WAIT_MS));
        loop {
            let outcome = sync_facade_tick(
                &self.db_path,
                &self.roots,
                force,
                scope,
                |conn| {
                    let mut baseline = self
                        .sync_baseline
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner);
                    let identity = conn.path().and_then(|path| {
                        crate::read_pool::file_identity(std::path::Path::new(path))
                    });
                    let mut previous_identity = self
                        .sync_identity
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner);
                    if identity.is_none() || identity != *previous_identity {
                        *baseline = None;
                    }
                    *previous_identity = identity;
                    if baseline.is_none() {
                        *baseline = Some(SyncBaseline::new(catalog_fingerprint(conn)?));
                    } else if let Some(before) = baseline.as_mut() {
                        // Exclude other writers' commits preceding this call.
                        catalog_digest::advance(conn, &mut before.catalog)?;
                    }
                    Ok(())
                },
                |conn, (), _tick, succeeded| {
                    // The before callback holds this baseline exclusively.
                    let mut baseline = self
                        .sync_baseline
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner);
                    match baseline.as_mut() {
                        Some(before) => before.finish(conn, succeeded),
                        None => anyhow::bail!("sync catalog baseline unavailable"),
                    }
                },
            )
            .map_err(Error::sync)?;
            if let Some((tick, changed)) = outcome {
                return Ok((tick, changed));
            }
            crate::ingest::check_capture_cancelled().map_err(Error::sync)?;
            let now = Instant::now();
            if now >= deadline {
                return Err(Error::SyncLocked {
                    path: self.db_path.clone(),
                    waited_ms: started.elapsed().as_millis() as u64,
                });
            }
            std::thread::sleep((deadline - now).min(SYNC_LOCK_RETRY));
        }
    }
}

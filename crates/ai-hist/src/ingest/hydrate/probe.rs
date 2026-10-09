//! What a local hydration of a catalogued session would read, answered
//! without hydrating it.

use super::*;

/// Answers, per catalogued session, what a local hydration would read --
/// decided by the snapshot hydration itself takes: the session's own source
/// and, with related evidence, every child transcript it enumerates (Claude
/// sidecars and their metadata, Codex child rollouts, Muse subagent logs).
/// `None` when hydration would refuse the session: its source gone, the
/// session no longer in it or hidden, superseded, or a source with no local
/// parser.
///
/// Read-only: the snapshot's own cursor bookkeeping is rolled back. An
/// OpenCode store is classified once per locator and kept open, so asking
/// about every session of a large store is one indexed lookup each.
#[derive(Default)]
pub(crate) struct HydrationProbe {
    /// Each OpenCode locator's classification; `Err` when hydration refuses
    /// it outright.
    opencode_sources: HashMap<String, std::result::Result<OpencodeSource, ()>>,
    opencode_stores: HashMap<PathBuf, Option<Connection>>,
    /// Each Claude project directory's sidecars by the session id their
    /// records carry, walked once per probe instead of once per session.
    claude_sidecars: HashMap<PathBuf, HashMap<String, Vec<PathBuf>>>,
}

impl HydrationProbe {
    pub(crate) fn reads(
        &mut self,
        conn: &Connection,
        source: &str,
        session_id: &str,
        roots: &crate::ProviderRoots,
    ) -> Result<Option<Vec<PathBuf>>> {
        let options = HydrateSessionOptions {
            source: source.to_string(),
            session_id: session_id.to_string(),
            scope: SessionScope::Local,
            include_related: true,
        };
        let Some(target) = observed_target(conn, &options)? else {
            return Ok(None);
        };
        if source == "opencode" {
            let store_reads = target
                .locator
                .as_deref()
                .and_then(|locator| self.opencode_store_reads(locator, session_id, roots));
            if let Some(reads) = store_reads {
                return Ok(reads);
            }
        }
        // A Codex child that comes back is one a delegation recorded: hydration
        // reads the recorded rollouts beside its sibling scan, and the scan
        // reads every rollout of two whole date directories, which is not a
        // cost to pay per session here. So the snapshot runs without it and
        // the recorded descendants are resolved the way the scan resolves
        // them.
        let options = HydrateSessionOptions {
            include_related: !matches!(source, "codex" | "claude"),
            ..options
        };
        let probe = conn.unchecked_transaction()?;
        let snapshot = source_snapshot(&probe, &options, &target, roots, None);
        probe.rollback()?;
        let snapshot = match snapshot {
            Ok(snapshot) => snapshot,
            Err(error)
                if error
                    .chain()
                    .any(|cause| cause.is::<crate::ingest::CaptureCancelled>()) =>
            {
                return Err(error)
            }
            Err(_) => return Ok(None),
        };
        let mut reads: Vec<PathBuf> = snapshot
            .stamped_cursors
            .into_iter()
            .map(|(_, path)| path)
            .collect();
        if let Some(path) = snapshot.path {
            self.extend_child_reads(conn, source, session_id, &path, &mut reads)?;
            reads.push(path);
        }
        Ok(Some(reads))
    }

    /// The child transcripts a related hydration reads beside `path` that
    /// the snapshot leaves out.
    fn extend_child_reads(
        &mut self,
        conn: &Connection,
        source: &str,
        session_id: &str,
        path: &Path,
        reads: &mut Vec<PathBuf>,
    ) -> Result<()> {
        match source {
            // A Muse session's subagent logs are read with it but carry no
            // cursor of their own.
            "muse" => reads.extend(muse_session_files(path)?),
            "codex" => {
                let recorded = recorded_codex_child_rollouts(conn, session_id)?;
                reads.extend(codex_descendants(recorded, path, session_id)?.0);
            }
            "claude" => {
                for sidecar in self.claude_sidecars_of(conn, path, session_id)? {
                    let metadata = claude_subagent_meta_path(&sidecar);
                    if metadata.is_file() {
                        reads.push(metadata);
                    }
                    reads.push(sidecar);
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// The sidecars a related Claude hydration of `session_id` reads: the
    /// same walk and the same identity test as [`claude_subagents`], over a
    /// directory index built once.
    fn claude_sidecars_of(
        &mut self,
        conn: &Connection,
        transcript: &Path,
        session_id: &str,
    ) -> Result<Vec<PathBuf>> {
        let Some(directory) = transcript.parent() else {
            return Ok(Vec::new());
        };
        if !self.claude_sidecars.contains_key(directory) {
            let probe = conn.unchecked_transaction()?;
            let walked = claude_sidecars(&probe, directory);
            probe.rollback()?;
            let mut index: HashMap<String, Vec<PathBuf>> = HashMap::new();
            for (path, meta) in walked?.0 {
                index.entry(meta.session_id).or_default().push(path);
            }
            self.claude_sidecars.insert(directory.to_path_buf(), index);
        }
        Ok(self.claude_sidecars[directory]
            .get(session_id)
            .into_iter()
            .flatten()
            .filter(|path| path.as_path() != transcript)
            .cloned()
            .collect())
    }

    /// The OpenCode SQLite branch of [`source_snapshot`], with the store's
    /// classification and connections cached: `Some(None)` when hydration
    /// would refuse the session, `None` when the locator is not a SQLite
    /// store and the full snapshot must answer.
    fn opencode_store_reads(
        &mut self,
        locator: &str,
        session_id: &str,
        roots: &crate::ProviderRoots,
    ) -> Option<Option<Vec<PathBuf>>> {
        let path = PathBuf::from(locator);
        let source = self
            .opencode_sources
            .entry(locator.to_string())
            .or_insert_with(|| opencode_source(&path, roots).map_err(|_| ()));
        let earlier = match source {
            Err(()) => return Some(None),
            Ok(OpencodeSource::TreeSession) => return None,
            Ok(OpencodeSource::Store { earlier }) => earlier.clone(),
        };
        for store in &earlier {
            // An earlier store that cannot be read claims nothing.
            if self.store_holds(store, session_id) == Some(true) {
                return Some(None);
            }
        }
        match self.store_holds(&path, session_id) {
            Some(true) => Some(Some(vec![path])),
            _ => Some(None),
        }
    }

    fn store_holds(&mut self, store: &Path, session_id: &str) -> Option<bool> {
        let conn = self
            .opencode_stores
            .entry(store.to_path_buf())
            .or_insert_with(|| {
                Connection::open_with_flags(
                    store,
                    rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                        | rusqlite::OpenFlags::SQLITE_OPEN_URI,
                )
                .ok()
            })
            .as_ref()?;
        conn.prepare_cached("SELECT 1 FROM session WHERE id = ?")
            .and_then(|mut statement| statement.exists([session_id]))
            .ok()
    }
}

/// The catalog target hydration would take, located where the session was
/// last observed; `None` when the catalog refuses the session.
fn observed_target(
    conn: &Connection,
    options: &HydrateSessionOptions,
) -> Result<Option<CatalogTarget>> {
    let Ok(mut target) = catalog_target(conn, options) else {
        return Ok(None);
    };
    let source = options.source.as_str();
    let local_key = ObservationKey {
        source: source.to_string(),
        session_id: options.session_id.to_string(),
        location: SessionLocation::Local,
        connector_id: source.to_string(),
        connector_instance: "default".into(),
    };
    if let Some(observation) = observations::get(conn, &local_key)? {
        if !matches!(source, "opencode" | "devin") {
            target.locator = observation.raw_locator.clone();
        }
    }
    Ok(Some(target))
}

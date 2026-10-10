//! Which local sources one sweep reads, and the per-source phases it runs.
//!
//! A full sweep walks every provider. A live watch used to run one for every
//! filesystem event — a Claude session appending every couple of seconds
//! re-walked Codex, Cursor, Grok, Muse, OpenCode and Devin each time. A [`SweepScope`] names the sources an event actually fell
//! under, and the sweep runs only their phases.
//!
//! What a scoped sweep must never do is claim more than it read. The stored
//! source fingerprint and destination marker are statements about *every*
//! source, so a scoped sweep neither consults nor writes them: the next
//! unforced full sweep still compares against the last full one and sees
//! whatever moved in the sources the scoped sweep left alone. Everything a
//! phase stores — cursors, stamps, per-source generations — belongs to its own
//! source, so running only some phases leaves the others' state exactly as the
//! last sweep that ran them left it.

use super::*;

/// The local sources a sweep reads. `None` is every one of them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct SweepScope {
    only: Option<Vec<&'static str>>,
}

impl SweepScope {
    /// Every local source: a full sweep.
    pub(crate) fn everything() -> Self {
        Self::default()
    }

    /// Only these sources, by their ledger names. A name this build does not
    /// know reads nothing; an empty list is a sweep that reads nothing.
    pub(crate) fn only<'a>(sources: impl IntoIterator<Item = &'a str>) -> Self {
        let mut only: Vec<&'static str> = sources
            .into_iter()
            .filter_map(|source| {
                crate::store::SOURCE_CHOICES
                    .iter()
                    .copied()
                    .find(|known| *known == source)
            })
            .collect();
        only.sort_unstable();
        only.dedup();
        Self { only: Some(only) }
    }

    /// Whether this is a full sweep.
    pub(crate) fn is_everything(&self) -> bool {
        self.only.is_none()
    }

    /// Whether the sweep reads `source`.
    pub(crate) fn includes(&self, source: &str) -> bool {
        self.only.as_ref().is_none_or(|only| only.contains(&source))
    }

    /// Whether a scoped sweep has no phase to run at all: an empty list, or
    /// only names this build does not know.
    pub(crate) fn reads_nothing(&self) -> bool {
        !SWEEP_PHASE_SOURCES
            .iter()
            .any(|source| self.includes(source))
    }
}

/// The sweep's shallow discovery pass over `providers`, the in-scope ones.
///
/// Discovery fails outright when every provider it was handed fails, which
/// for a full sweep means no provider on the machine could enumerate. A
/// scoped sweep hands it only its own providers — often one — so the same
/// rule would turn one adapter's hiccup into a failed tick, where a full
/// sweep records it as a diagnostic beside the adapters that did enumerate.
/// A scoped sweep keeps it the diagnostic it is in a full sweep; the sweep
/// still notes it as an unread source.
pub(super) fn discover_for_sweep(
    env: &DiscoveryEnv<'_>,
    providers: &[Box<dyn ShallowSessionProvider>],
    scope: &SweepScope,
) -> Result<DiscoverySummary> {
    match discover::discover_sessions_for_sweep(env, &DiscoverOptions::default(), providers) {
        Err(error) if !scope.is_everything() => error
            .downcast::<AllProvidersFailed>()
            .map(|failed| failed.summary),
        result => result,
    }
}

/// The sources with a local sweep phase, in the order the sweep runs them.
const SWEEP_PHASE_SOURCES: &[&str] = &[
    "claude", "codex", "cursor", "grok", "muse", "opencode", "devin",
];

/// One sweep's per-source phases and the state they share. Each phase
/// reports its own outcome through `report`, checkpoints `state` when it
/// advanced it, and adds what it inserted to `inserted`.
pub(super) struct SweepPhases<'s, 'c> {
    pub(super) conn: &'s Connection,
    pub(super) state: &'s mut Map<String, Value>,
    pub(super) roots: &'s crate::ProviderRoots,
    pub(super) repairs: &'s SweepRepairs,
    pub(super) coverage: &'s mut SweepCoverage,
    pub(super) report: &'s mut SyncSourceReport,
    pub(super) checkpoints: &'s mut SweepCheckpoints<'c>,
    pub(super) inserted: usize,
}

impl SweepPhases<'_, '_> {
    /// Run every phase `scope` includes, in the order a full sweep runs them.
    pub(super) fn run(&mut self, scope: &SweepScope) -> Result<()> {
        if scope.includes("claude") {
            self.claude()?;
        }
        if scope.includes("codex") {
            self.codex()?;
        }
        if scope.includes("cursor") {
            self.cursor()?;
        }
        if scope.includes("grok") {
            self.grok()?;
        }
        if scope.includes("muse") {
            self.muse()?;
        }
        if scope.includes("opencode") {
            self.opencode()?;
        }
        if scope.includes("devin") {
            self.devin()?;
        }
        check_capture_cancelled()
    }

    /// Count a phase that returns rows inserted, checkpointing its state.
    fn counted(&mut self, source: &str, result: Result<usize>) {
        if let Some(inserted) = self.report.capture(source, result) {
            self.inserted += inserted;
            self.checkpoints.save(self.state);
        }
    }

    fn claude(&mut self) -> Result<()> {
        capture_progress("claude-history", 0, None);
        check_capture_cancelled()?;
        let checkpoints = &mut *self.checkpoints;
        let history = sync_jsonl_incremental(
            self.conn,
            self.state,
            "claude",
            &self.roots.claude.join("history.jsonl"),
            parse_claude_line,
            &mut |in_progress| checkpoints.save(in_progress),
        );
        self.counted("claude", history);
        capture_progress("claude", 0, None);
        check_capture_cancelled()?;
        let metadata = sync_claude_session_metadata_with_repairs_and_coverage(
            self.conn,
            self.state,
            &self.roots.claude.join("projects"),
            self.repairs,
            self.coverage,
        );
        if self.report.capture("claude-metadata", metadata).is_some() {
            self.checkpoints.save(self.state);
        }
        Ok(())
    }

    fn codex(&mut self) -> Result<()> {
        capture_progress("codex", 0, None);
        check_capture_cancelled()?;
        let result = sync_codex_with_repairs_and_coverage(
            self.conn,
            self.state,
            &self.roots.codex,
            self.repairs,
            self.coverage,
        );
        self.counted("codex", result);
        Ok(())
    }

    fn cursor(&mut self) -> Result<()> {
        capture_progress("cursor", 0, None);
        check_capture_cancelled()?;
        let projects = self.roots.home.join(".cursor/projects");
        let result = sync_cursor(self.conn, self.state, &projects, self.coverage);
        self.counted("cursor", result);
        Ok(())
    }

    fn grok(&mut self) -> Result<()> {
        capture_progress("grok", 0, None);
        check_capture_cancelled()?;
        let result = sync_grok_home(self.conn, self.state, &self.roots.grok, self.coverage);
        self.counted("grok", result);
        Ok(())
    }

    fn muse(&mut self) -> Result<()> {
        capture_progress("muse", 0, None);
        check_capture_cancelled()?;
        let result = sync_muse_with_coverage(
            self.conn,
            self.state,
            &self.roots.muse,
            self.repairs,
            self.coverage,
        );
        self.counted("muse", result);
        Ok(())
    }

    fn opencode(&mut self) -> Result<()> {
        capture_progress("opencode", 0, None);
        check_capture_cancelled()?;
        let result =
            opencode_sweep::sync_opencode_sources(self.conn, self.state, self.roots, self.repairs);
        self.counted("opencode", result);
        Ok(())
    }

    fn devin(&mut self) -> Result<()> {
        capture_progress("devin", 0, None);
        check_capture_cancelled()?;
        let result = devin::sync_devin_db(
            self.conn,
            self.state,
            &self.roots.devin,
            self.repairs,
            self.coverage,
        );
        if let Some(inserted) = self.report.capture("devin", result) {
            self.inserted += inserted;
            self.checkpoints.save(self.state);
            if inserted > 0 {
                sync_note!("  [devin] +{inserted} rows");
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_scope_names_only_sources_this_build_knows() {
        let scope = SweepScope::only(["codex", "claude", "codex", "nonsense"]);
        assert_eq!(scope, SweepScope::only(["claude", "codex"]));
        assert!(scope.includes("claude") && !scope.includes("devin"));
        assert!(!scope.is_everything() && !scope.reads_nothing());
        assert!(SweepScope::everything().includes("devin"));
        assert!(SweepScope::only([]).reads_nothing());
        assert!(SweepScope::only(["trajectory"]).reads_nothing());
    }

    #[test]
    fn every_local_source_has_a_phase() {
        for source in crate::store::SOURCE_CHOICES {
            assert!(
                SWEEP_PHASE_SOURCES.contains(source),
                "{source} has no sweep phase, so a scoped sweep could never read it"
            );
        }
    }

    fn append(path: &Path, line: &str) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .unwrap();
        std::io::Write::write_all(&mut file, format!("{line}\n").as_bytes()).unwrap();
    }

    fn history_rows(conn: &Connection, source: &str) -> i64 {
        conn.query_row(
            "SELECT COUNT(*) FROM history WHERE source = ?1",
            [source],
            |row| row.get(0),
        )
        .unwrap()
    }

    fn stored_state(db: &Path) -> Map<String, Value> {
        let state = fs::read_to_string(db.parent().unwrap().join(".sync-state.json")).unwrap();
        serde_json::from_str(&state).unwrap()
    }

    struct Fixture {
        _home: tempfile::TempDir,
        db: PathBuf,
        roots: crate::ProviderRoots,
        claude_log: PathBuf,
        codex_log: PathBuf,
        conn: Connection,
    }

    fn fixture() -> Fixture {
        let home = tempfile::tempdir().unwrap();
        let db = home.path().join("store/ai-history.db");
        fs::create_dir_all(db.parent().unwrap()).unwrap();
        let roots = crate::ProviderRoots::from_home(
            home.path().to_path_buf(),
            home.path().join("opencode.db"),
        );
        let claude_log = roots.claude.join("history.jsonl");
        let codex_log = roots.codex.join("history.jsonl");
        append(
            &claude_log,
            r#"{"display":"first claude prompt","sessionId":"c-1","timestamp":1}"#,
        );
        append(
            &codex_log,
            r#"{"session_id":"x-1","ts":1.0,"text":"first codex prompt"}"#,
        );
        let conn = crate::open_db(&db).unwrap();
        Fixture {
            _home: home,
            db,
            roots,
            claude_log,
            codex_log,
            conn,
        }
    }

    /// A scoped sweep reads its sources and nothing else, and leaves every
    /// statement about the other sources — their cursors, the fingerprint
    /// and the destination marker the last full sweep stored — exactly where
    /// that sweep left them, so the next unforced full sweep still sees what
    /// moved in them.
    #[test]
    fn a_scoped_sweep_reads_only_its_sources_and_leaves_the_full_stamp() {
        let f = fixture();
        assert!(sync_basic(&f.conn, &f.db, &f.roots, false, &SweepScope::everything()).unwrap());
        let stamped = stored_state(&f.db);
        assert!(stamped.contains_key(SOURCE_FINGERPRINT_KEY));
        append(
            &f.claude_log,
            r#"{"display":"second claude prompt","sessionId":"c-1","timestamp":2}"#,
        );
        append(
            &f.codex_log,
            r#"{"session_id":"x-1","ts":2.0,"text":"second codex prompt"}"#,
        );

        let claude_only = SweepScope::only(["claude"]);
        assert!(sync_basic(&f.conn, &f.db, &f.roots, true, &claude_only).unwrap());
        assert_eq!(history_rows(&f.conn, "claude"), 2);
        assert_eq!(history_rows(&f.conn, "codex"), 1, "codex was not read");
        let after = stored_state(&f.db);
        for key in [
            SOURCE_FINGERPRINT_KEY,
            DESTINATION_GENERATION_KEY,
            DESTINATION_HEAD_KEY,
            "codex",
        ] {
            assert_eq!(after.get(key), stamped.get(key), "{key} moved");
        }
        assert_ne!(after.get("claude"), stamped.get("claude"));

        // The full sweep's stamp still describes the sources before both
        // appends, so an unforced full sweep cannot skip the codex one.
        assert!(sync_basic(&f.conn, &f.db, &f.roots, false, &SweepScope::everything()).unwrap());
        assert_eq!(history_rows(&f.conn, "codex"), 2);
        assert!(!sync_basic(&f.conn, &f.db, &f.roots, false, &SweepScope::everything()).unwrap());
    }

    #[test]
    fn a_scope_with_no_local_source_reads_nothing() {
        let f = fixture();
        for scope in [SweepScope::only([]), SweepScope::only(["trajectory"])] {
            assert!(!sync_basic(&f.conn, &f.db, &f.roots, true, &scope).unwrap());
        }
        assert_eq!(history_rows(&f.conn, "claude"), 0);
        assert!(!f.db.parent().unwrap().join(".sync-state.json").exists());
    }

    /// One in-scope adapter failing is a diagnostic, as it is in a full
    /// sweep; every adapter of a full sweep failing is still an error.
    #[test]
    fn a_scoped_sweeps_lone_failing_adapter_is_a_diagnostic_not_an_error() {
        let home = tempfile::tempdir().unwrap();
        fs::write(home.path().join("opencode.db"), "definitely not sqlite").unwrap();
        let conn = crate::open_db(&home.path().join("store.db")).unwrap();
        let roots = crate::ProviderRoots::from_home(
            home.path().to_path_buf(),
            home.path().join("opencode.db"),
        );
        let env = DiscoveryEnv::with_provider_roots(&conn, roots);
        let mut failing = shallow_providers();
        failing.retain(|provider| provider.source() == "opencode");

        let scoped = discover_for_sweep(&env, &failing, &SweepScope::only(["opencode"])).unwrap();
        assert_eq!(scoped.diagnostics.len(), 1);
        assert_eq!(scoped.diagnostics[0].source, "opencode");

        let error = discover_for_sweep(&env, &failing, &SweepScope::everything()).unwrap_err();
        assert!(error.is::<AllProvidersFailed>(), "{error:#}");
    }

    /// The tick the finding was about: a Claude event tick whose Claude
    /// discovery fails still completes, with the failure recorded as an
    /// unread source rather than a failed sweep.
    #[cfg(unix)]
    #[test]
    fn a_claude_tick_survives_a_claude_discovery_failure() {
        let f = fixture();
        // A projects directory this process may not list: the transcript
        // walk and Claude discovery both fail to enumerate it, while the
        // prompt log beside it still reads.
        let projects = f.roots.claude.join("projects");
        fs::create_dir_all(projects.join("app")).unwrap();
        let unlistable = std::os::unix::fs::PermissionsExt::from_mode(0o000);
        fs::set_permissions(&projects, unlistable).unwrap();
        struct Relist(PathBuf);
        impl Drop for Relist {
            fn drop(&mut self) {
                let listable = std::os::unix::fs::PermissionsExt::from_mode(0o755);
                let _ = fs::set_permissions(&self.0, listable);
            }
        }
        let _relist = Relist(projects.clone());
        // A process that may read past the mode (root, or a capability such
        // as CAP_DAC_READ_SEARCH) has no failure to survive here: the test
        // would fail for the wrong reason, so it stands aside.
        if fs::read_dir(&projects).is_ok() {
            eprintln!("skipped: this process can list a mode 000 directory");
            return;
        }
        append(
            &f.claude_log,
            r#"{"display":"second claude prompt","sessionId":"c-1","timestamp":2}"#,
        );
        let claude_only = SweepScope::only(["claude"]);
        let env = DiscoveryEnv::with_provider_roots(&f.conn, f.roots.clone());
        let mut claude = shallow_providers();
        claude.retain(|provider| provider.source() == "claude");
        let discovered = discover_for_sweep(&env, &claude, &claude_only).unwrap();
        assert_eq!(
            discovered.diagnostics.len(),
            1,
            "{:?}",
            discovered.diagnostics
        );

        assert!(sync_basic(&f.conn, &f.db, &f.roots, true, &claude_only).unwrap());
        assert_eq!(history_rows(&f.conn, "claude"), 2);
    }
}

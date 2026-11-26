//! The ways into a hydration: by catalog identity on a connection of its own,
//! or, for exact path capture and the hook fast path, on the connection the
//! caller already holds (see [`hydrate_local_session_on`]).

use super::*;

pub(crate) fn hydrate_session_at_with_roots_and_connectors(
    db_path: &Path,
    options: &HydrateSessionOptions,
    roots: &crate::ProviderRoots,
    connectors: &crate::remote::SourceConnectorSelection,
) -> Result<HydrateSessionResult> {
    hydrate_session_at_with_roots_connectors_and_claude_snapshot(
        db_path, options, roots, connectors, None,
    )
}

pub(crate) fn hydrate_session_at_with_roots_connectors_and_claude_snapshot(
    db_path: &Path,
    options: &HydrateSessionOptions,
    roots: &crate::ProviderRoots,
    connectors: &crate::remote::SourceConnectorSelection,
    claude_snapshot: Option<ClaudeTranscriptSnapshot>,
) -> Result<HydrateSessionResult> {
    let home = &roots.home;
    let _reads = begin_hydration(options, home, connectors)?;
    let started = Instant::now();
    // Acquisition and replacement are one per-session critical section. The
    // provider call has to be inside it: otherwise an older response can wait
    // behind a newer writer and then replace that newer evidence. A file lock
    // covers both threads and independent RelayHistory processes.
    let _remote_lock = (options.scope == SessionScope::Remote)
        .then(|| acquire_remote_hydration_lock(db_path, options))
        .transpose()?;
    let mut conn = open_db(db_path)?;
    let target = catalog_target(&conn, options)?;
    if options.scope == SessionScope::Remote {
        return hydrate_remote_session(&mut conn, options, home, connectors, started);
    }
    hydrate_local(
        &conn,
        db_path,
        options,
        roots,
        target,
        claude_snapshot,
        started,
    )
}

/// A local hydration on a connection the caller already holds.
///
/// Exact path capture and the hook fast path have just written the session's
/// catalog row through a validated writable connection; hydrating on that
/// same connection, rather than opening and validating a second one, saves
/// the schema check (most of `open_db`'s cost) and keeps every write of the
/// capture on one connection, so the retained writer's `data_version` does
/// not move and the next capture need not re-validate it either. The
/// connection must be in autocommit: the pass opens its own `IMMEDIATE`
/// transaction, exactly as it does on a connection of its own.
pub(crate) fn hydrate_local_session_on(
    conn: &Connection,
    db_path: &Path,
    options: &HydrateSessionOptions,
    roots: &crate::ProviderRoots,
    claude_snapshot: Option<ClaudeTranscriptSnapshot>,
) -> Result<HydrateSessionResult> {
    anyhow::ensure!(
        options.scope == SessionScope::Local,
        "hydrate_local_session_on hydrates local sessions only"
    );
    anyhow::ensure!(
        conn.is_autocommit(),
        "hydrate_local_session_on needs a connection outside any transaction"
    );
    let _reads = begin_hydration(
        options,
        &roots.home,
        &crate::remote::SourceConnectorSelection::default(),
    )?;
    let started = Instant::now();
    let target = catalog_target(conn, options)?;
    hydrate_local(
        conn,
        db_path,
        options,
        roots,
        target,
        claude_snapshot,
        started,
    )
}

/// Refuse a cancelled or invalid request, and a remote one whose connector is
/// not configured, before any acquisition starts.
fn begin_hydration(
    options: &HydrateSessionOptions,
    home: &Path,
    connectors: &crate::remote::SourceConnectorSelection,
) -> Result<cold_read::ReadScope> {
    super::check_capture_cancelled()?;
    validate_options(options)?;
    // One hydration is one acquisition pass; see `begin_acquisition_pass`.
    crate::project_identity::begin_acquisition_pass();
    if options.scope == SessionScope::Remote {
        crate::remote::ensure_selected_remote_connectors_configured_for_at(
            "hydration",
            home,
            std::slice::from_ref(&options.source),
            connectors,
        )
        .map_err(|error| hydration_error("CONNECTOR_NOT_CONFIGURED", error))?;
    }
    Ok(cold_read::begin_read_scope())
}

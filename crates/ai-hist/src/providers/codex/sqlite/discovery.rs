//! Shallow Codex SQLite discovery and change fingerprints.
use super::*;
use crate::ingest::provider_diagnostics::{provider_diagnostic, ProviderSkip};

/// A cataloged rollout yields to SQLite only when that same thread's
/// projection extends beyond the rollout. A sibling with the same id retains
/// catalog ownership. A malformed locator cannot take it over.
pub(crate) fn incomplete_projection_owns_rollout(incoming: &str, observed: &str) -> Result<bool> {
    let Some((state, id)) = parse_locator(incoming) else {
        return Ok(false);
    };
    let Some(root) = state.parent() else {
        return Ok(false);
    };
    let observed_path = Path::new(observed);
    if !observed_path.starts_with(root.join("sessions"))
        && !observed_path.starts_with(root.join("archived_sessions"))
    {
        return Ok(false);
    }
    let Some(home) = root.parent() else {
        return Ok(false);
    };
    let mut roots = crate::ProviderRoots::from_home(
        home.to_path_buf(),
        home.join(".local/share/opencode/opencode.db"),
    );
    roots.codex = root.to_path_buf();
    match current_locator(&roots, id, observed) {
        Ok(current) => Ok(current.as_deref() == Some(incoming)),
        Err(error)
            if error
                .chain()
                .any(|cause| cause.is::<crate::ingest::CaptureCancelled>()) =>
        {
            Err(error)
        }
        Err(error) => {
            provider_diagnostic(ProviderSkip {
                source: crate::Source::Codex,
                code: "SESSION_SOURCE_UNAVAILABLE",
                subject: Some(id),
                what: format_args!("SQLite catalog ownership unavailable"),
                detail: format_args!("{error:#}"),
            });
            Ok(false)
        }
    }
}

pub(crate) fn enumerate(
    root: &Path,
    roots: &crate::ProviderRoots,
) -> Result<(Vec<Candidate>, HashSet<PathBuf>)> {
    let selection = database_selection(root)?;
    if let Some(diagnostic) = selection.diagnostic {
        provider_diagnostic(ProviderSkip {
            source: crate::Source::Codex,
            code: "SESSION_SOURCE_UNAVAILABLE",
            subject: None,
            what: format_args!("skipping SQLite history"),
            detail: format_args!("{diagnostic}"),
        });
    }
    let Some((state, history)) = selection.pair else {
        return Ok((Vec::new(), HashSet::new()));
    };
    let context = AcquisitionContext::new("codex", roots);
    let threads = super::sweep::discovery_threads(&context, &state, &history)?;
    let incomplete_rollouts = super::sweep::incomplete_rollout_paths(&context, &state, &history)?;
    let candidates = threads
        .into_iter()
        .filter(|t| !t.is_child())
        .map(|t| Candidate {
            source: "codex",
            locator: locator(&state, &t.id),
            session_id: Some(t.id.clone()),
            recency_hint_ms: Some(t.updated),
            stamp: t.stamp(),
        })
        .collect();
    Ok((candidates, incomplete_rollouts))
}

pub(crate) fn fingerprint(root: &Path) -> Result<Vec<Candidate>> {
    let Some((state, history)) = databases(root)? else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    for path in [state, history] {
        for suffix in ["", "-wal", "-journal"] {
            let sidecar = PathBuf::from(format!("{}{suffix}", path.display()));
            if sidecar.exists() {
                let (stamp, recency_hint_ms) = crate::file_stamp_and_modified(&sidecar)?;
                out.push(Candidate {
                    source: "codex",
                    locator: sidecar.to_string_lossy().into_owned(),
                    session_id: None,
                    recency_hint_ms,
                    stamp,
                });
            }
        }
    }
    Ok(out)
}

pub(crate) fn shallow(scan: &ScanEnv<'_>, c: &Candidate) -> Result<Option<ShallowSession>> {
    let Some((state, id)) = parse_locator(&c.locator) else {
        return Ok(None);
    };
    let context = AcquisitionContext::new("codex", scan.provider_roots);
    scan.note_open();
    scan.note_query();
    let thread = sqlite_snapshot(&context, &state, |db| {
        db.query_row(&format!("{THREAD_SQL} AND id=?1"), [id], thread_row)
            .optional()
            .map_err(Into::into)
    })?;
    let Some(mut t) = thread else {
        return Ok(None);
    };
    let Some((_, history)) = databases(&scan.provider_roots.codex)? else {
        return Ok(None);
    };
    scan.note_open();
    scan.note_query();
    if let Some((ordinal, offset)) = sqlite_snapshot(&context, &history, |db| {
        db.query_row(
            "SELECT next_rollout_ordinal,next_rollout_byte_offset FROM thread_history_projection_state WHERE thread_id=?1",
            [id],
            |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)),
        )
        .optional()
        .map_err(Into::into)
    })? {
        t.next_ordinal = ordinal;
        t.next_offset = offset;
    }
    scan.note_records(1);
    if t.rollout_complete() || t.is_child() {
        return Ok(None);
    }
    Ok(Some(ShallowSession {
        source: "codex".into(),
        session_id: t.id,
        cwd: t.cwd,
        git_branch: t.branch,
        first_activity_ms: Some(t.created),
        last_activity_ms: Some(t.updated),
        first_prompt: t
            .first_user_message
            .as_deref()
            .filter(|text| !text.trim().is_empty())
            .filter(|text| crate::ingest::control::codex_text_control_kind(text).is_none())
            .map(crate::discover::excerpt),
        models: t.model.into_iter().collect(),
        originator: t.originator,
        agent_version: t.version,
        repo_url: t.repo,
        title: t.name.or(t.title),
        raw_path: Some(c.locator.clone()),
        source_stamp: Some(c.stamp.clone()),
        ..Default::default()
    }))
}

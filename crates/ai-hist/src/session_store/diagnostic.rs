//! What a facade call reports besides its result: the skips and gaps that
//! are not errors.
use super::*;

pub(super) fn hydrate_report(
    source: Source,
    session_id: String,
    status: HydrateStatus,
    result: Option<HydrateSessionResult>,
) -> Result<HydrateReport, Error> {
    let session = SessionRef::id(source, session_id);
    let Some(result) = result else {
        return Ok(HydrateReport {
            session,
            status,
            capability: None,
            coverage: Vec::new(),
            related: Vec::new(),
            bytes_read: 0,
            diagnostics: Vec::new(),
        });
    };
    // The engine's coverage comes from the adapter declaration, which leaves
    // parser-derived markers out on purpose (a connector cannot supply them);
    // the facade's contract is the capability declaration `session()` also
    // reports. Markers are written by the same parse that writes events, so
    // they are covered exactly when events are; everything else follows the
    // engine's narrowing (`include_related`, incomplete Codex child search).
    let engine = &result.coverage;
    let coverage = source
        .capabilities()
        .evidence_kinds
        .into_iter()
        .filter(|kind| {
            engine.contains(kind)
                || (*kind == EvidenceKind::SessionMarker
                    && engine.contains(&EvidenceKind::SessionEvent))
        })
        .collect();
    Ok(HydrateReport {
        session,
        status,
        capability: Some(Capability::parse(&result.capability)?),
        coverage,
        related: result
            .related_session_ids
            .into_iter()
            .map(|id| SessionRef::id(source, id))
            .collect(),
        bytes_read: result.bytes_read,
        diagnostics: result
            .diagnostics
            .into_iter()
            .map(|diagnostic| Diagnostic {
                code: diagnostic.code,
                message: diagnostic.message,
                subject: None,
            })
            .collect(),
    })
}

/// Something a read or a hydration noticed that is not an error.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Diagnostic {
    /// Stable code, e.g. `RELATIONSHIP_UNLINKED_CHILD`, `EVIDENCE_PARTIAL`.
    pub code: String,
    pub message: String,
    /// What the diagnostic is about, when it names one thing: a
    /// `relationship_uid`, a locator.
    pub subject: Option<String>,
}

/// Something a sweep skipped and carried on without: a provider store it
/// could not read, or one session it could not capture.
///
/// Reported on [`SyncReport::diagnostics`] and [`TickReport::diagnostics`],
/// and folded into [`HydrateReport::diagnostics`] for a hydration. The same
/// skip is printed to stderr as `ai-hist: [source] message` unless the pass
/// is quiet, which every facade call is; the CLI's own sync is not.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SyncDiagnostic {
    pub source: Source,
    /// Stable code. `SESSION_SOURCE_UNAVAILABLE`: a store of the provider's
    /// own could not be read, so the history it holds is absent from this
    /// pass. `EVIDENCE_PARTIAL`: one session or related thread was skipped
    /// while the provider's other sessions were read. A skip whose cause
    /// names a code carries that code instead.
    pub code: String,
    pub message: String,
    /// The session or thread the skip names, when it names one.
    pub subject: Option<String>,
}

impl From<SyncDiagnostic> for Diagnostic {
    fn from(diagnostic: SyncDiagnostic) -> Self {
        Self {
            code: diagnostic.code,
            message: diagnostic.message,
            subject: diagnostic.subject,
        }
    }
}

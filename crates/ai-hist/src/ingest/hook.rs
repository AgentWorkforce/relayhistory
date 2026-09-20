//! Lifecycle-hook fast path: ingest exactly one provider transcript, by path.
//!
//! An agent harness knows which transcript it just wrote long before a sweep
//! could find it. Claude Code's hooks hand us that path directly
//! (`transcript_path` in the `SessionStart` / `PostToolUse` / `Stop` /
//! `PreCompact` payloads), and `PreCompact` in particular fires *before* the
//! compaction rewrites the file — the one moment the pre-compaction evidence
//! still exists.
//!
//! The ordinary hydration entry points name a session by `(source,
//! session_id)` and read its locator out of the catalog, which a hook payload
//! does not have. [`ingest_transcript_at`] goes the other way: it takes the
//! path, checks it belongs to the provider's root, reads the session's identity
//! out of the file through the provider's own shallow adapter, upserts the
//! catalog row that identifies it, and only then runs the normal hydration. So
//! the catalog lookup is bypassed while `validate_provider_path` still decides
//! what the path is allowed to be — a hook payload is input from another
//! process, and an arbitrary path must never become an ingest target.
//!
//! Hook policy: a missing or rotated transcript is not an error. It reports
//! [`TranscriptStatus::Missing`] and the caller exits zero, because failing a
//! hook fails the tool call the agent was in the middle of.

use super::hydrate::{
    hydrate_session_at_with_roots_and_connectors, validate_provider_path, HydrateSessionOptions,
    HydrateSessionResult,
};
use super::*;
use crate::discover::{
    discover_sessions_with_provider_refs, Candidate, DiscoverOptions, DiscoveryEnv, ScanEnv,
    ShallowReadAccess, ShallowSession, ShallowSessionProvider,
};

/// Harnesses whose lifecycle hooks this crate can ingest from.
///
/// Claude Code is the only one today. Codex and OpenCode expose no transcript
/// lifecycle hook to attach to, so they are covered by watch mode instead; see
/// `docs/agent-integration.md`.
pub const HOOK_HARNESSES: &[&str] = &["claude"];

/// What a single-transcript ingest did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum TranscriptStatus {
    /// Evidence was read from the transcript into the catalog.
    Ingested,
    /// The transcript had not changed since it was last hydrated.
    Unchanged,
    /// The transcript is gone, or was never written. Not an error.
    Missing,
    /// The file exists but carries no session identity — a Claude subagent
    /// sidecar, or a transcript whose first records are still being written.
    Unidentified,
    /// The transcript belongs to a different session than the payload named.
    ///
    /// A hook payload is a claim from another process about two things — which
    /// session fired, and which file holds it — and a delayed, replayed or
    /// malformed one can pair them wrongly. Ingesting the file anyway would
    /// attribute the lifecycle event to a session it did not come from, so
    /// nothing is ingested and the caller is told.
    Mismatched,
}

impl TranscriptStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            TranscriptStatus::Ingested => "ingested",
            TranscriptStatus::Unchanged => "unchanged",
            TranscriptStatus::Missing => "missing",
            TranscriptStatus::Unidentified => "unidentified",
            TranscriptStatus::Mismatched => "mismatched",
        }
    }
}

/// The result of ingesting one named transcript.
#[derive(Debug, Clone, Serialize)]
pub struct TranscriptIngest {
    pub source: String,
    pub transcript: String,
    pub session_id: Option<String>,
    pub status: TranscriptStatus,
    /// The hydration that ran, when one did.
    pub hydration: Option<HydrateSessionResult>,
}

impl TranscriptIngest {
    fn short(source: &str, transcript: &Path, status: TranscriptStatus) -> Self {
        Self {
            source: source.to_string(),
            transcript: transcript.to_string_lossy().into_owned(),
            session_id: None,
            status,
            hydration: None,
        }
    }
}

/// Ingest one provider transcript named by path, against the default home.
pub fn ingest_transcript_at(
    db_path: &Path,
    source: &str,
    transcript: &Path,
    expected_session: Option<&str>,
    include_related: bool,
) -> Result<TranscriptIngest> {
    let roots = crate::ProviderRoots::from_env(home_dir());
    ingest_transcript_at_with_roots(
        db_path,
        &roots,
        source,
        transcript,
        expected_session,
        include_related,
    )
}

/// Ingest one provider transcript named by path, against an explicit provider
/// home.
///
/// `include_related` also hydrates the transcript's bounded sidecars — Claude
/// subagent transcripts living beside it in the same project directory. It does
/// not walk the rest of the provider root.
pub fn ingest_transcript_at_with_home(
    db_path: &Path,
    home: &Path,
    source: &str,
    transcript: &Path,
    expected_session: Option<&str>,
    include_related: bool,
) -> Result<TranscriptIngest> {
    let opencode_db = home.join(".local/share/opencode/opencode.db");
    let roots = crate::ProviderRoots::from_home(home.to_path_buf(), opencode_db);
    ingest_transcript_at_with_roots(
        db_path,
        &roots,
        source,
        transcript,
        expected_session,
        include_related,
    )
}

fn ingest_transcript_at_with_roots(
    db_path: &Path,
    roots: &crate::ProviderRoots,
    source: &str,
    transcript: &Path,
    expected_session: Option<&str>,
    include_related: bool,
) -> Result<TranscriptIngest> {
    match fs::metadata(transcript) {
        Ok(metadata) if metadata.is_file() => {}
        // A path that resolves to something other than a regular file is the
        // same non-event as a missing one: there is nothing to read, and a
        // hook must not fail the tool call that produced it.
        Ok(_) => {
            return Ok(TranscriptIngest::short(
                source,
                transcript,
                TranscriptStatus::Missing,
            ))
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(TranscriptIngest::short(
                source,
                transcript,
                TranscriptStatus::Missing,
            ))
        }
        // Anything else — a permission error, an I/O failure — is a real
        // condition the caller should be able to see, even if it chooses to
        // swallow it.
        Err(error) => {
            return Err(error).with_context(|| format!("reading {}", transcript.display()))
        }
    }
    // The payload comes from another process. Before the path is read as
    // evidence it has to be inside the root that provider actually owns.
    validate_provider_path(source, transcript, roots)?;

    let providers = shallow_providers();
    let provider = providers
        .iter()
        .map(|provider| provider.as_ref())
        .find(|provider| provider.source() == source)
        .with_context(|| format!("INVALID_ARGUMENT: no local adapter for source '{source}'"))?;

    let candidate = candidate_for(provider.source(), transcript)?;
    let single = SingleCandidate {
        inner: provider,
        candidate,
    };

    // Resolved the way the sweep resolves it, against the home this call was
    // given rather than the process one. No hook harness is database-backed
    // today, so this only keeps the environment honest.
    let mut session_id = None;
    {
        let conn = open_db(db_path)?;
        let env = DiscoveryEnv::with_provider_roots(&conn, roots.clone());
        // Read before anything is written. The payload says which session
        // fired *and* which file holds it, and those are two claims: a
        // delayed or replayed hook can pair a live session id with a
        // transcript that belongs to another one. For Claude, require the
        // provider-native `sessionId`: its ordinary shallow adapter may fall
        // back to the filename for old transcripts, but a hook must not turn
        // that guess into a catalog row. Other providers retain their adapter
        // identity path; none currently advertises lifecycle-hook support.
        let observed_session = if source == "claude" {
            crate::discover::claude_transcript_session_id(&env.scan(), transcript)?
        } else {
            single
                .read_shallow(&env.scan(), Some(&conn), &single.candidate)?
                .map(|observed| observed.session_id)
        };
        let Some(observed_session) = observed_session else {
            return Ok(TranscriptIngest::short(
                source,
                transcript,
                TranscriptStatus::Unidentified,
            ));
        };
        if expected_session.is_some_and(|expected| expected != observed_session) {
            return Ok(TranscriptIngest {
                source: source.to_string(),
                transcript: transcript.to_string_lossy().into_owned(),
                // The identity that was *found*, so the caller can see what
                // the file really was rather than only that it disagreed.
                session_id: Some(observed_session),
                status: TranscriptStatus::Mismatched,
                hydration: None,
            });
        }
        discover_sessions_with_provider_refs(
            &env,
            &DiscoverOptions::default(),
            &[&single],
            |row: &ShallowSession| session_id = Some(row.session_id.clone()),
        )?;
    }
    let Some(session_id) = session_id else {
        return Ok(TranscriptIngest::short(
            source,
            transcript,
            TranscriptStatus::Unidentified,
        ));
    };

    let hydration = hydrate_session_at_with_roots_and_connectors(
        db_path,
        &HydrateSessionOptions {
            source: source.to_string(),
            session_id: session_id.clone(),
            scope: SessionScope::Local,
            include_related,
        },
        roots,
        &crate::remote::SourceConnectorSelection::default(),
    )?;
    let status = if hydration.status == "unchanged" {
        TranscriptStatus::Unchanged
    } else {
        TranscriptStatus::Ingested
    };
    Ok(TranscriptIngest {
        source: source.to_string(),
        transcript: transcript.to_string_lossy().into_owned(),
        session_id: Some(session_id),
        status,
        hydration: Some(hydration),
    })
}

/// The one candidate a hook payload names, stamped the way that provider's own
/// enumeration would stamp it so a later sweep sees the same value and skips
/// the file instead of re-reading it.
fn candidate_for(source: &'static str, transcript: &Path) -> Result<Candidate> {
    let (stamp, recency_hint_ms) = match source {
        "grok" => crate::grok_session_stamp_and_modified(transcript)?,
        _ => crate::file_stamp_and_modified(transcript)?,
    };
    Ok(Candidate {
        source,
        locator: transcript.to_string_lossy().into_owned(),
        session_id: None,
        recency_hint_ms,
        stamp,
    })
}

/// One provider adapter narrowed to a single candidate.
///
/// Everything except enumeration delegates, so the catalog row, the observation
/// key and the connector identity are byte-for-byte what a full sweep would
/// have written — the hook path adds no second way to describe a session.
struct SingleCandidate<'a> {
    inner: &'a dyn ShallowSessionProvider,
    candidate: Candidate,
}

impl ShallowSessionProvider for SingleCandidate<'_> {
    fn connector_id(&self) -> &str {
        self.inner.connector_id()
    }

    fn connector_instance(&self) -> &str {
        self.inner.connector_instance()
    }

    fn check_available(&self, home: &Path) -> Result<()> {
        self.inner.check_available(home)
    }

    fn acquire(
        &self,
        home: &Path,
        observation: &crate::observations::SessionObservation,
    ) -> Result<crate::sources::AcquiredEvidence> {
        self.inner.acquire(home, observation)
    }

    fn source(&self) -> &'static str {
        self.inner.source()
    }

    fn location(&self) -> SessionLocation {
        self.inner.location()
    }

    fn enumerate(
        &self,
        _env: &DiscoveryEnv<'_>,
        _requested_limit: Option<usize>,
    ) -> Result<Vec<Candidate>> {
        Ok(vec![self.candidate.clone()])
    }

    fn read_access(&self) -> ShallowReadAccess {
        self.inner.read_access()
    }

    fn read_shallow(
        &self,
        scan: &ScanEnv<'_>,
        catalog: Option<&Connection>,
        candidate: &Candidate,
    ) -> Result<Option<ShallowSession>> {
        self.inner.read_shallow(scan, catalog, candidate)
    }
}

//! Host-composed source connector intake: the stable surface a driver process
//! (a source plugin helper, an embedded adapter) uses to land enumerated
//! observations and acquired evidence in this store. Revision fences apply
//! exactly as they do for the SDK's native intake calls — a snapshot acquired
//! against an older observation revision is refused, not applied.
use super::{Error, SessionStore};
use crate::discover::DiscoverySummary;
use crate::observations::ObservationKey;
use crate::source_intake::{self, NormalizedSourceEvidence, ObservationState, SourceObservation};
use crate::SessionLocation;
use serde::{Deserialize, Serialize};

/// One connector's enumerated catalog rows for
/// [`SessionStore::apply_source_observations`]. One call carries one
/// connector instance's batch; a driver calls it once per selected connector.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SourceObservationBatch {
    /// The connector's acquisition identity (`devin-cloud`, `claude-web`, …).
    pub connector_id: String,
    /// Non-secret instance key; `"default"` for a single account.
    pub connector_instance: String,
    /// Where the connector's sessions live. `Remote` rows never merge with or
    /// overwrite the same session's `Local` provenance.
    pub location: SessionLocation,
    /// Shallow rows the connector enumerated, at most 10 000.
    pub observations: Vec<SourceObservation>,
    /// See [`crate::DiscoveryOptions::skip_protected_folder_identity`].
    #[serde(default)]
    pub skip_protected_folder_identity: bool,
}

impl SourceObservationBatch {
    /// One batch of rows `connector_id`/`connector_instance` enumerated for
    /// `location`.
    pub fn new(
        connector_id: impl Into<String>,
        connector_instance: impl Into<String>,
        location: SessionLocation,
        observations: Vec<SourceObservation>,
    ) -> Self {
        Self {
            connector_id: connector_id.into(),
            connector_instance: connector_instance.into(),
            location,
            observations,
            skip_protected_folder_identity: false,
        }
    }

    /// Keep a working directory under Desktop, Documents, or Downloads as its
    /// raw path. Defaults off, matching [`crate::DiscoveryOptions`].
    pub fn skip_protected_folder_identity(mut self, skip: bool) -> Self {
        self.skip_protected_folder_identity = skip;
        self
    }
}

/// One acquired evidence snapshot for [`SessionStore::apply_source_evidence`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SourceEvidenceSubmission {
    /// The observation this evidence was acquired for.
    pub key: ObservationKey,
    /// The revision [`SessionStore::source_observation`] returned before the
    /// connector's I/O ran. A concurrent writer moving the observation forward
    /// makes the snapshot stale, and intake refuses it rather than replacing
    /// newer evidence.
    pub expected_revision: String,
    /// `None` keeps the historical default of reporting related sessions.
    pub include_related: Option<bool>,
    /// The connector's normalized snapshot: the kinds it covers and the
    /// records it acquired.
    pub evidence: NormalizedSourceEvidence,
}

impl SourceEvidenceSubmission {
    /// `evidence` acquired for `key` while the observation held
    /// `expected_revision`.
    pub fn new(
        key: ObservationKey,
        expected_revision: impl Into<String>,
        evidence: NormalizedSourceEvidence,
    ) -> Self {
        Self {
            key,
            expected_revision: expected_revision.into(),
            include_related: None,
            evidence,
        }
    }

    /// `false` narrows the applied evidence to the selected thread.
    pub fn include_related(mut self, include_related: bool) -> Self {
        self.include_related = Some(include_related);
        self
    }
}

impl SessionStore {
    /// Read the observation, checkpoint and revision fence a connector key
    /// currently holds. A driver reads this before acquiring evidence so the
    /// revision it returns can fence the apply that follows.
    ///
    /// A read-only store serves this on a read-only connection; it never opens
    /// a writable one or migrates the database here.
    pub fn source_observation(&self, key: &ObservationKey) -> Result<ObservationState, Error> {
        key.validate()
            .map_err(|error| Error::InvalidArgument(format!("{error:#}")))?;
        if !self.db_path.exists() {
            return Ok(ObservationState::default());
        }
        let conn = if self.read_only {
            let conn = crate::store::open_db_readonly(&self.db_path)
                .map_err(|error| Error::classify(error, Error::DatabaseOpen))?;
            // A read-only open cannot migrate; a database that predates the
            // observation schema gets the same remedy open gives for the
            // other read indexes rather than a bare `no such table`.
            if !crate::observations::schema_is_current(&conn).map_err(Error::query)? {
                return Err(Error::stale_schema(&self.db_path, "connector-observation"));
            }
            conn
        } else {
            crate::store::open_db(&self.db_path)
                .map_err(|error| Error::classify(error, Error::DatabaseOpen))?
        };
        source_intake::observation_state(&conn, key)
            .map_err(|error| Error::classify(error, Error::Discovery))
    }

    /// Upsert one connector's enumerated shallow rows. Invalid or duplicate
    /// identities refuse the whole batch before the database opens. The rows
    /// enter the same catalog pipeline local discovery uses, keyed to the
    /// connector identity and location the batch declares.
    pub fn apply_source_observations(
        &self,
        batch: SourceObservationBatch,
    ) -> Result<DiscoverySummary, Error> {
        if self.read_only {
            return Err(Error::read_only("apply_source_observations"));
        }
        source_intake::apply_source_observations(source_intake::ApplyObservationsRequest {
            db_path: Some(self.db_path.clone()),
            connector_id: batch.connector_id,
            connector_instance: batch.connector_instance,
            location: batch.location,
            observations: batch.observations,
            skip_protected_folder_identity: batch.skip_protected_folder_identity,
        })
        .map_err(|error| Error::classify(error, Error::Discovery))
    }

    /// Atomically apply one acquired evidence snapshot, fenced by
    /// `expected_revision`. Intake validates identities and fields and merges
    /// the snapshot's coverage: a kind the snapshot omits stays as the last
    /// pass left it, while a covered kind's records replace.
    pub fn apply_source_evidence(
        &self,
        submission: SourceEvidenceSubmission,
    ) -> Result<crate::HydrateSessionResult, Error> {
        if self.read_only {
            return Err(Error::read_only("apply_source_evidence"));
        }
        source_intake::apply_source_evidence(source_intake::ApplyEvidenceRequest {
            db_path: Some(self.db_path.clone()),
            key: submission.key,
            expected_revision: submission.expected_revision,
            include_related: submission.include_related,
            evidence: submission.evidence,
        })
        .map_err(Error::hydration)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        CatalogQuery, EvidenceKind, EvidenceRecord, SessionQuery, SessionRef, SessionScope,
        ShallowSession, Source, StoreOptions,
    };
    use serde_json::json;

    fn store_at(path: &std::path::Path, read_only: bool) -> SessionStore {
        SessionStore::open(StoreOptions::default().db_path(path).read_only(read_only)).unwrap()
    }

    fn key(session_id: &str) -> ObservationKey {
        ObservationKey {
            source: "devin".into(),
            session_id: session_id.into(),
            location: SessionLocation::Remote,
            connector_id: "devin-cloud".into(),
            connector_instance: "default".into(),
        }
    }

    fn observation(session_id: &str) -> SourceObservation {
        SourceObservation {
            session: ShallowSession {
                source: "devin".into(),
                session_id: session_id.into(),
                first_prompt: Some("catch bugs".into()),
                source_stamp: Some("cloud:100:active:aa".into()),
                ..Default::default()
            },
            raw_locator: Some(format!("v3:sessions/{session_id}")),
        }
    }

    fn evidence() -> NormalizedSourceEvidence {
        let record = |kind: EvidenceKind, payload: serde_json::Value| EvidenceRecord {
            kind,
            payload: payload.as_object().unwrap().clone(),
            record_id: None,
            revision_id: None,
        };
        NormalizedSourceEvidence {
            source_stamp: "cloud:100:active:aa".into(),
            source_bytes: 128,
            covered_kinds: vec![EvidenceKind::History, EvidenceKind::SessionEvent],
            records: vec![
                record(
                    EvidenceKind::History,
                    json!({
                        "source": "devin",
                        "session_id": "sess-1",
                        "prompt": "catch bugs",
                        "timestamp_ms": 1000
                    }),
                ),
                record(
                    EvidenceKind::SessionEvent,
                    json!({
                        "source": "devin",
                        "session_id": "sess-1",
                        "ts_ms": 1000,
                        "role": "user",
                        "kind": "text",
                        "event_uid": "ev-1",
                        "text": "catch bugs"
                    }),
                ),
                record(
                    EvidenceKind::SessionEvent,
                    json!({
                        "source": "devin",
                        "session_id": "sess-1",
                        "ts_ms": 2000,
                        "role": "assistant",
                        "kind": "text",
                        "event_uid": "ev-2",
                        "text": "done"
                    }),
                ),
            ],
        }
    }

    /// The full host flow a driver runs: enumerate → land observations → read
    /// the revision fence → acquire → apply the snapshot → catalog and events
    /// read back through the same store.
    #[test]
    fn store_intake_round_trips_observations_and_evidence() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("history.db");
        let store = store_at(&db, false);

        let before = store.source_observation(&key("sess-1")).unwrap();
        assert!(before.observation.is_none() && before.revision.is_none());

        let summary = store
            .apply_source_observations(SourceObservationBatch::new(
                "devin-cloud",
                "default",
                SessionLocation::Remote,
                vec![observation("sess-1")],
            ))
            .unwrap();
        assert_eq!(summary.discovered, 1);

        let fence = store.source_observation(&key("sess-1")).unwrap();
        let observation = fence.observation.expect("observed row");
        assert_eq!(observation.key.connector_id, "devin-cloud");
        let revision = fence.revision.expect("revision fence");

        let result = store
            .apply_source_evidence(
                SourceEvidenceSubmission::new(key("sess-1"), revision, evidence())
                    .include_related(false),
            )
            .unwrap();
        assert_eq!(result.status, "hydrated");

        let query = CatalogQuery {
            scope: SessionScope::All,
            ..Default::default()
        };
        let rows: Vec<_> = store.sessions(query).collect::<Result<_, _>>().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].source, Source::parse("devin").unwrap());

        let query = SessionQuery {
            include_text: true,
            ..Default::default()
        };
        let session = store
            .session(
                &SessionRef::id(Source::parse("devin").unwrap(), "sess-1"),
                query,
            )
            .unwrap()
            .expect("catalogued session");
        assert_eq!(session.messages.len(), 2);
        assert!(session.coverage.contains(&EvidenceKind::SessionEvent));
    }

    /// A snapshot acquired against a superseded revision is refused rather
    /// than replacing newer evidence.
    #[test]
    fn stale_revision_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("history.db");
        let store = store_at(&db, false);
        store
            .apply_source_observations(SourceObservationBatch::new(
                "devin-cloud",
                "default",
                SessionLocation::Remote,
                vec![observation("sess-1")],
            ))
            .unwrap();
        let error = store
            .apply_source_evidence(
                SourceEvidenceSubmission::new(
                    key("sess-1"),
                    "not-the-current-revision",
                    evidence(),
                )
                .include_related(false),
            )
            .unwrap_err();
        assert_eq!(error.code(), "SOURCE_REVISION_CONFLICT");
    }

    /// Read-only stores answer the observation fence on a read-only
    /// connection and refuse both writes.
    #[test]
    fn read_only_store_reads_but_never_writes() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("history.db");
        let store = store_at(&db, false);
        store
            .apply_source_observations(SourceObservationBatch::new(
                "devin-cloud",
                "default",
                SessionLocation::Remote,
                vec![observation("sess-1")],
            ))
            .unwrap();
        drop(store);

        let store = store_at(&db, true);
        assert!(store
            .source_observation(&key("sess-1"))
            .unwrap()
            .observation
            .is_some());
        for result in [
            store
                .apply_source_observations(SourceObservationBatch {
                    connector_id: "devin-cloud".into(),
                    connector_instance: "default".into(),
                    location: SessionLocation::Remote,
                    observations: vec![observation("sess-1")],
                    skip_protected_folder_identity: false,
                })
                .map(|_| ()),
            store
                .apply_source_evidence(SourceEvidenceSubmission::new(
                    key("sess-1"),
                    "any",
                    evidence(),
                ))
                .map(|_| ()),
        ] {
            assert!(matches!(result, Err(Error::UnsupportedOperation(_))));
        }
    }

    /// A read-only store on a database that predates the observation schema
    /// is told to reopen writable, not handed a bare `no such table`.
    #[test]
    fn a_read_only_store_reports_a_stale_observation_schema() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("history.db");
        store_at(&db, false);
        crate::store::open_db(&db)
            .unwrap()
            .execute("DROP TABLE session_observations", [])
            .unwrap();

        let error = store_at(&db, true)
            .source_observation(&key("sess-1"))
            .unwrap_err();
        assert!(error.is_stale_schema());
    }
}

//! Transport-free source plugin intake. Each complete snapshot is fenced by the
//! observation revision acquired before external I/O, then reconciled atomically.
use crate::{discover::*, *};
use crate::{
    observations::{self, ObservationCheckpoint, ObservationKey, SessionObservation},
    source_evidence::{self, EvidenceKind, EvidenceRecord, FULL_SESSION_KINDS},
};
use anyhow::{ensure, Context, Result};
use rusqlite::{Connection, TransactionBehavior};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::time::Instant;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizedSourceEvidence {
    pub source_stamp: String,
    pub source_bytes: i64,
    pub covered_kinds: Vec<EvidenceKind>,
    pub records: Vec<EvidenceRecord>,
}
#[derive(Debug, Deserialize)]
pub struct ObservationRequest {
    pub db_path: Option<PathBuf>,
    #[serde(flatten)]
    pub key: ObservationKey,
}
#[derive(Debug, Serialize)]
pub struct ObservationState {
    pub observation: Option<SessionObservation>,
    pub checkpoint: Option<ObservationCheckpoint>,
    pub revision: Option<String>,
}
pub fn get_source_observation(request: ObservationRequest) -> Result<ObservationState> {
    request.key.validate()?;
    let path = request.db_path.unwrap_or_else(default_db_path);
    let mut state = ObservationState {
        observation: None,
        checkpoint: None,
        revision: None,
    };
    if !path.exists() {
        return Ok(state);
    }
    // Opening an existing database upgrades legacy observation state without
    // reading any source adapter or credential store.
    let conn = open_db(&path)?;
    state.observation = observations::get(&conn, &request.key)?;
    state.checkpoint = observations::checkpoint(&conn, &request.key)?;
    state.revision = observations::revision(&conn, &request.key)?;
    Ok(state)
}
#[derive(Debug, Deserialize)]
pub struct ApplyEvidenceRequest {
    pub db_path: Option<PathBuf>,
    #[serde(flatten)]
    pub key: ObservationKey,
    pub expected_revision: String,
    /// Absent means the caller did not say, which keeps the historical
    /// behaviour of reporting related sessions.
    #[serde(default)]
    pub include_related: Option<bool>,
    #[serde(flatten)]
    pub evidence: NormalizedSourceEvidence,
}
pub fn apply_source_evidence(mut request: ApplyEvidenceRequest) -> Result<HydrateSessionResult> {
    validate(&request.key, &mut request.evidence)?;
    ensure!(
        !request.expected_revision.is_empty(),
        "INVALID_ARGUMENT: expected_revision is required"
    );
    let path = request.db_path.unwrap_or_else(default_db_path);
    ensure!(
        path.exists(),
        "SESSION_NOT_FOUND: source observation is missing"
    );
    let mut conn = open_db(&path)?;
    apply_normalized(
        &mut conn,
        &request.key,
        &request.expected_revision,
        request.evidence,
        request.include_related.unwrap_or(true),
        Instant::now(),
    )
}
fn validate(key: &ObservationKey, evidence: &mut NormalizedSourceEvidence) -> Result<()> {
    ensure!(
        evidence.source_bytes >= 0,
        "INVALID_ARGUMENT: negative source_bytes"
    );
    ensure!(
        !evidence.source_stamp.is_empty(),
        "INVALID_ARGUMENT: source_stamp is required"
    );
    source_evidence::validate_records(key, &evidence.covered_kinds, &mut evidence.records)
}
#[derive(Clone, PartialEq, Serialize, Deserialize)]
struct ManagedRecord {
    record: EvidenceRecord,
    managed: bool,
}
#[derive(Default, Serialize, Deserialize)]
struct RecordSnapshot {
    format: String,
    covered_kinds: Vec<EvidenceKind>,
    records: Vec<ManagedRecord>,
}
fn read_snapshot(conn: &Connection, key: &ObservationKey) -> Result<RecordSnapshot> {
    let Some(payload) = observations::evidence(conn, key)? else {
        return Ok(RecordSnapshot::default());
    };
    match payload.get("format").and_then(Value::as_str) {
        Some("records") => Ok(serde_json::from_value(payload)?),
        Some("events") => {
            let managed = payload["managed"]
                .as_array()
                .context("legacy managed event ids")?;
            let mut records = vec![];
            for event in payload["events"].as_array().context("legacy events")? {
                let record = EvidenceRecord {
                    kind: EvidenceKind::SessionEvent,
                    payload: event.as_object().context("legacy event object")?.clone(),
                    record_id: None,
                    revision_id: None,
                };
                records.push(ManagedRecord {
                    managed: managed.contains(&event["event_uid"]),
                    record,
                });
            }
            Ok(RecordSnapshot {
                format: "records".into(),
                covered_kinds: vec![EvidenceKind::SessionEvent],
                records,
            })
        }
        // Legacy raw snapshots did not record canonical ownership. Retain their
        // canonical rows conservatively; reacquisition retains a new independent
        // snapshot but cannot retroactively prove who owned old canonical rows.
        _ => Ok(RecordSnapshot::default()),
    }
}
fn owner(key: &ObservationKey) -> (u8, String, String) {
    (
        if key.location == SessionLocation::Local {
            0
        } else {
            1
        },
        key.connector_id.clone(),
        key.connector_instance.clone(),
    )
}
pub(crate) fn apply_normalized(
    conn: &mut Connection,
    key: &ObservationKey,
    expected: &str,
    mut evidence: NormalizedSourceEvidence,
    include_related: bool,
    started: Instant,
) -> Result<HydrateSessionResult> {
    validate(key, &mut evidence)?;
    // Enforced here rather than asked of each connector.
    //
    // `ShallowSessionProvider::acquire` takes no `include_related`, and for
    // Claude's full export the engine derives the edges from a transcript the
    // connector merely handed over -- so a connector cannot honour the option
    // even when it wants to, and a third-party one has never been told about
    // it. The option is a property of the request, so the boundary that owns
    // the request enforces it: every path into intake, in-process provider and
    // napi plugin alike, passes through here. Validation runs first, so a
    // malformed relationship record is still rejected rather than quietly
    // dropped. The plugin-side handling stays as an optimization -- do not
    // fetch or ship what was not asked for -- not as what correctness rests on.
    if !include_related {
        evidence
            .covered_kinds
            .retain(|kind| *kind != EvidenceKind::Relationship);
        evidence
            .records
            .retain(|record| record.kind != EvidenceKind::Relationship);
    }
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    ensure!(
        observations::revision(&tx, key)?.as_deref() == Some(expected),
        "SOURCE_REVISION_CONFLICT: source observation changed during acquisition"
    );
    let observation =
        observations::get(&tx, key)?.context("SESSION_NOT_FOUND: source observation is missing")?;
    ensure!(
        observation.access_state != "withdrawn",
        "CONNECTOR_NOT_CONFIGURED: selected observation is withdrawn"
    );
    let previous = observations::checkpoint(&tx, key)?;
    let mut snapshots = BTreeMap::new();
    let mut ambiguous_local = false;
    for observed in observations::list(&tx, &key.source, &key.session_id)? {
        let snapshot = read_snapshot(&tx, &observed.key)?;
        // Direct local parsers and legacy rows do not yet prove per-record
        // ownership. Preserve canonical rows, even if byte-identical to remote
        // evidence, while retaining each remote snapshot separately.
        ambiguous_local |=
            observed.key.location == SessionLocation::Local && snapshot.covered_kinds.is_empty();
        snapshots.insert(owner(&observed.key), snapshot);
    }
    let mut previous_winners = BTreeMap::new();
    for snapshot in snapshots.values() {
        for item in &snapshot.records {
            previous_winners
                .entry(item.record.identity())
                .or_insert(&item.record);
        }
    }
    let mut revoked = observations::protected_canonical_evidence(&tx, key)?;
    for (identity, record) in previous_winners {
        if (ambiguous_local && record.exists(&tx)?) || !record.matches_canonical(&tx)? {
            observations::protect_canonical_evidence(&tx, key, &identity)?;
            revoked.insert(identity);
        }
    }
    // Apply canonical protection only to this reconciliation view. Other
    // connectors retain their exact acquired snapshot and revision fence.
    for snapshot in snapshots.values_mut() {
        for item in &mut snapshot.records {
            if revoked.contains(&item.record.identity()) {
                item.managed = false;
            }
        }
    }
    let mut previously_managed = BTreeMap::new();
    for snapshot in snapshots.values() {
        for item in &snapshot.records {
            if item.managed {
                previously_managed.insert(item.record.identity(), item.record.clone());
            }
        }
    }
    let mut own = snapshots.remove(&owner(key)).unwrap_or_default();
    let prior_records = own.records.clone();
    let prior_kinds = own.covered_kinds.clone();
    own.format = "records".into();
    own.records
        .retain(|item| !evidence.covered_kinds.contains(&item.record.kind));
    let mut kinds: BTreeSet<_> = own.covered_kinds.into_iter().collect();
    kinds.extend(evidence.covered_kinds.iter().copied());
    own.covered_kinds = kinds.into_iter().collect();
    for record in &evidence.records {
        let managed = !revoked.contains(&record.identity())
            && (previously_managed.contains_key(&record.identity()) || !record.exists(&tx)?);
        own.records.push(ManagedRecord {
            record: record.clone(),
            managed,
        });
    }
    own.records
        .sort_by_cached_key(|item| item.record.identity());
    // Coverage is part of the result, not just bookkeeping: an acquisition that
    // covers a kind the last one did not changes the capability even when it
    // adds no row -- the session simply has none of that kind. Reporting that
    // as `unchanged` invites a consumer to skip the upgrade it just asked for.
    let unchanged = own.records == prior_records
        && own.covered_kinds == prior_kinds
        && previous.as_ref().is_some_and(|checkpoint| {
            checkpoint.source_stamp.as_deref() == Some(&evidence.source_stamp)
        });
    observations::save_evidence(&tx, key, &serde_json::to_value(&own)?)?;
    // The accumulated set models what this connector's snapshot as a whole
    // still asserts, which is what the stored `discovery_state` is about: an
    // acquisition that did not cover a kind does not withdraw the records an
    // earlier one contributed.
    let full = FULL_SESSION_KINDS
        .iter()
        .all(|kind| own.covered_kinds.contains(kind));
    // The result's `coverage` is a different question: what *this* acquisition
    // examined. Reporting the accumulated set would let a later
    // `include_related: false` snapshot inherit `relationship` from an earlier
    // one and read as `full` despite the opt-out -- the same "nobody looked"
    // overstatement this contract removes, arriving through retained state
    // instead of a literal. Canonical order, so the wire shape does not depend
    // on how a connector ordered its declaration.
    let coverage = evidence
        .covered_kinds
        .iter()
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    snapshots.insert(owner(key), own);
    let mut union = BTreeMap::new();
    let mut managed = BTreeSet::new();
    for snapshot in snapshots.values() {
        for item in &snapshot.records {
            let identity = item.record.identity();
            if item.managed {
                managed.insert(identity.clone());
            }
            union.entry(identity).or_insert(&item.record);
        }
    }
    for (identity, record) in &previously_managed {
        if !union.contains_key(identity) {
            record.remove(&tx)?;
        }
    }
    for (identity, record) in union {
        if managed.contains(&identity) {
            record.write(&tx)?;
        }
    }
    crate::hydrate::save_observation_progress(
        &tx,
        &observation,
        &evidence.source_stamp,
        evidence.source_bytes,
        evidence.records.len() as i64,
        // What this acquisition actually did, so the stored checkpoint cannot
        // contradict a hydration that did index delegation.
        include_related,
        full,
    )?;
    tx.commit()?;
    let options = HydrateSessionOptions {
        source: key.source.clone(),
        session_id: key.session_id.clone(),
        scope: if key.location == SessionLocation::Local {
            SessionScope::Local
        } else {
            SessionScope::Remote
        },
        // The request's own answer, not an assumption: with it hardcoded, a
        // hydration that declined related evidence still came back listing
        // related sessions and counting them.
        include_related,
    };
    crate::hydrate::build_remote_result(
        conn,
        &options,
        if unchanged {
            "unchanged"
        } else if previous.is_some() {
            "updated"
        } else {
            "hydrated"
        },
        // Capability follows this acquisition's coverage, not the accumulated
        // set, or it would contradict the `coverage` beside it -- which the SDK
        // re-derives and rejects on mismatch. `discovery_state` keeps following
        // the stored row.
        crate::hydrate::capability_for(&coverage),
        if full { "full" } else { "shallow" },
        coverage,
        evidence.source_stamp,
        evidence.source_bytes,
        evidence.records.len() as i64,
        "SOURCE_EVIDENCE_INDEXED",
        "selected source snapshot committed",
        started,
    )
}

/// One acquired catalog row, with a separate opaque handle for subsequent reads.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceObservation {
    #[serde(flatten)]
    pub session: ShallowSession,
    pub raw_locator: Option<String>,
}
impl From<ShallowSession> for SourceObservation {
    fn from(session: ShallowSession) -> Self {
        Self {
            session,
            raw_locator: None,
        }
    }
}
impl std::ops::Deref for SourceObservation {
    type Target = ShallowSession;
    fn deref(&self) -> &Self::Target {
        &self.session
    }
}
impl std::ops::DerefMut for SourceObservation {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.session
    }
}
#[derive(Debug, Deserialize)]
pub struct ApplyObservationsRequest {
    pub db_path: Option<PathBuf>,
    pub connector_id: String,
    pub connector_instance: String,
    pub location: SessionLocation,
    pub observations: Vec<SourceObservation>,
}
struct BatchProvider {
    source: &'static str,
    id: String,
    instance: String,
    location: SessionLocation,
    rows: Vec<SourceObservation>,
}
impl ShallowSessionProvider for BatchProvider {
    fn source(&self) -> &'static str {
        self.source
    }
    fn connector_id(&self) -> &str {
        &self.id
    }
    fn connector_instance(&self) -> &str {
        &self.instance
    }
    fn location(&self) -> SessionLocation {
        self.location
    }
    fn enumerate(&self, _: &DiscoveryEnv<'_>, _: Option<usize>) -> Result<Vec<Candidate>> {
        self.rows
            .iter()
            .map(|row| {
                Ok(Candidate {
                    source: self.source,
                    locator: row
                        .raw_locator
                        .clone()
                        .or_else(|| row.raw_path.clone())
                        .unwrap_or_else(|| row.session_id.clone()),
                    session_id: Some(row.session_id.clone()),
                    recency_hint_ms: row.last_activity_ms,
                    stamp: row.source_stamp.clone().unwrap_or_else(|| {
                        format!(
                            "{:x}",
                            Sha256::digest(
                                serde_json::to_vec(row).expect("serializable observation")
                            )
                        )
                    }),
                })
            })
            .collect()
    }
    fn read_shallow(
        &self,
        _: &ScanEnv<'_>,
        _: Option<&Connection>,
        candidate: &Candidate,
    ) -> Result<Option<ShallowSession>> {
        Ok(self
            .rows
            .iter()
            .find(|row| Some(&row.session_id) == candidate.session_id.as_ref())
            .map(|row| row.session.clone()))
    }
}
pub fn apply_source_observations(request: ApplyObservationsRequest) -> Result<DiscoverySummary> {
    ensure!(
        request.observations.len() <= 10_000,
        "INVALID_ARGUMENT: observation batch exceeds 10000 rows"
    );
    let mut grouped: BTreeMap<&'static str, Vec<SourceObservation>> = BTreeMap::new();
    let mut identities = BTreeSet::new();
    // Validate the entire batch before opening a DB. Empty batches still validate
    // the connector identity and deliberately create no database.
    ObservationKey {
        source: "claude".into(),
        session_id: "validation".into(),
        location: request.location,
        connector_id: request.connector_id.clone(),
        connector_instance: request.connector_instance.clone(),
    }
    .validate()?;
    for mut row in request.observations {
        let key = ObservationKey {
            source: row.source.clone(),
            session_id: row.session_id.clone(),
            location: request.location,
            connector_id: request.connector_id.clone(),
            connector_instance: request.connector_instance.clone(),
        };
        key.validate()?;
        ensure!(
            identities.insert((row.source.clone(), row.session_id.clone())),
            "INVALID_ARGUMENT: duplicate source observation"
        );
        ensure!(
            row.raw_locator
                .as_ref()
                .or(row.raw_path.as_ref())
                .is_none_or(|v| !v.is_empty() && !v.contains('\0')),
            "INVALID_ARGUMENT: invalid observation locator"
        );
        // External discovery has not supplied any evidence yet.
        row.discovery_state = "shallow".into();
        row.locations = vec![request.location.as_str().into()];
        row.from_cache = false;
        let source = *crate::SOURCE_CHOICES
            .iter()
            .find(|source| **source == row.source)
            .context("INVALID_ARGUMENT: unknown source")?;
        grouped.entry(source).or_default().push(row);
    }
    let scope = if request.location == SessionLocation::Local {
        SessionScope::Local
    } else {
        SessionScope::Remote
    };
    if grouped.is_empty() {
        return Ok(DiscoverySummary {
            contract_version: SESSION_CATALOG_CONTRACT_VERSION,
            scope,
            ..Default::default()
        });
    }
    let providers = grouped
        .into_iter()
        .map(|(source, rows)| BatchProvider {
            source,
            rows,
            id: request.connector_id.clone(),
            instance: request.connector_instance.clone(),
            location: request.location,
        })
        .collect::<Vec<_>>();
    let conn = open_db(&request.db_path.unwrap_or_else(default_db_path))?;
    let env = DiscoveryEnv::new(&conn);
    discover_sessions_with_provider_refs(
        &env,
        &DiscoverOptions {
            scope,
            ..Default::default()
        },
        &providers
            .iter()
            .map(|provider| provider as &dyn ShallowSessionProvider)
            .collect::<Vec<_>>(),
        |_| {},
    )
}

//! Host-driven local source intake using only the default public API.
use ai_hist::{
    CatalogQuery, EvidenceKind, EvidenceRecord, NormalizedSourceEvidence, ObservationKey,
    ProviderRoots, Role, SessionLocation, SessionQuery, SessionScope, SessionStore, ShallowSession,
    Source, SourceEvidenceSubmission, SourceObservation, SourceObservationBatch, StoreOptions,
};
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::path::Path;

const CONNECTOR: &str = "example-host";
const TEXT: &str = "A response retained by the host application.";

// A synthetic host-owned record of Claude evidence, not a newly supported
// harness format. Production adapters must establish native writer provenance.
#[derive(Deserialize, Serialize)]
struct HostRecord {
    session_id: String,
    message_id: String,
    timestamp_ms: i64,
    text: String,
}

fn key(session_id: &str) -> ObservationKey {
    ObservationKey {
        source: "claude".into(),
        session_id: session_id.into(),
        location: SessionLocation::Local,
        connector_id: CONNECTOR.into(),
        connector_instance: "default".into(),
    }
}

// The host owns enumeration and acquisition. Nothing registers a parser in the
// store's automatic sync/watch loop. Only this explicitly selected file is read.
fn acquire(store: &SessionStore, path: &Path) -> Result<()> {
    let bytes = std::fs::read(path)?;
    let record: HostRecord = serde_json::from_slice(&bytes)?;
    let stamp = serde_json::to_string(&record)?;
    let key = key(&record.session_id);
    store.apply_source_observations(SourceObservationBatch::new(
        CONNECTOR,
        "default",
        SessionLocation::Local,
        vec![SourceObservation {
            session: ShallowSession {
                source: "claude".into(),
                session_id: record.session_id.clone(),
                raw_path: Some(path.to_string_lossy().into_owned()),
                source_stamp: Some(stamp.clone()),
                ..Default::default()
            },
            raw_locator: Some(path.to_string_lossy().into_owned()),
        }],
    ))?;
    let fence = store.source_observation(&key)?;
    ensure!(
        fence.observation.as_ref().is_some_and(|row| row.key == key),
        "local observation identity was not retained"
    );
    let revision = fence
        .revision
        .context("discovery returned no revision fence")?;
    // Re-read after the fence, as a driver would before potentially slow I/O.
    let acquired = std::fs::read(path)?;
    ensure!(
        bytes == acquired,
        "source changed during acquisition; retry discovery"
    );
    let payload = json!({
        "source": "claude", "session_id": record.session_id,
        "message_id": record.message_id, "event_uid": "host-event-1",
        "ts_ms": record.timestamp_ms, "role": "assistant", "kind": "text",
        "text": record.text,
    });
    let result = store.apply_source_evidence(
        SourceEvidenceSubmission::new(
            key,
            revision,
            NormalizedSourceEvidence {
                source_stamp: stamp,
                source_bytes: acquired.len() as i64,
                covered_kinds: vec![EvidenceKind::SessionEvent],
                records: vec![EvidenceRecord {
                    kind: EvidenceKind::SessionEvent,
                    payload: payload
                        .as_object()
                        .context("event payload is not an object")?
                        .clone(),
                    record_id: None,
                    revision_id: None,
                }],
            },
        )
        .include_related(false),
    )?;
    ensure!(
        matches!(result.status.as_str(), "hydrated" | "unchanged"),
        "evidence was not hydrated"
    );
    Ok(())
}

fn main() -> Result<()> {
    let home = tempfile::tempdir()?;
    let file = home.path().join("host-response.json");
    std::fs::write(
        &file,
        serde_json::to_vec(&HostRecord {
            session_id: "host-session".into(),
            message_id: "native-message-1".into(),
            timestamp_ms: 1_700_000_000_000,
            text: TEXT.into(),
        })?,
    )?;
    let options = || {
        let mut options = StoreOptions::default();
        options.home = Some(home.path().to_path_buf());
        options.roots = Some(ProviderRoots::from_home(
            home.path().to_path_buf(),
            home.path().join("empty.db"),
        ));
        options
    };
    let store = SessionStore::open(options())?;
    // Merely having a host file does not admit it into the catalog.
    ensure!(
        store.sessions(CatalogQuery::default()).next().is_none(),
        "unselected file was admitted"
    );
    acquire(&store, &file)?;
    acquire(&store, &file)?; // Replay must not duplicate the native message.
    drop(store);
    let store = SessionStore::open(options())?;
    let query = CatalogQuery::default();
    ensure!(query.scope == SessionScope::Local, "default scope changed");
    let rows = store.sessions(query).collect::<Result<Vec<_>, _>>()?;
    ensure!(rows.len() == 1, "unexpected local catalog contents");
    ensure!(
        rows[0].source == Source::Claude && rows[0].session_id == "host-session",
        "session identity changed"
    );
    ensure!(rows[0].locations == vec!["local"], "unexpected presence");
    let mut remote = CatalogQuery::default();
    remote.scope = SessionScope::Remote;
    ensure!(
        store.sessions(remote).next().is_none(),
        "local evidence leaked into remote scope"
    );
    let mut query = SessionQuery::default();
    query.include_text = true;
    let evidence = store
        .session(&rows[0].session_ref(), query)?
        .context("persisted session is missing")?;
    ensure!(evidence.messages.len() == 1, "message lost or duplicated");
    let message = &evidence.messages[0];
    ensure!(
        message.message_id.as_deref() == Some("native-message-1")
            && message.role == Role::Assistant,
        "native message identity changed"
    );
    ensure!(
        message.blocks.len() == 1
            && message.blocks[0].event_uid == "host-event-1"
            && message.blocks[0].text.as_deref() == Some(TEXT),
        "message content changed"
    );
    println!("local observation, hydration, replay, scope and reopened evidence verified");
    Ok(())
}

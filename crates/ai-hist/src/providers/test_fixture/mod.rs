//! Test-only provider: admitted solely by its shard, module and fixture.
use super::*;
use crate::discover::{Candidate, DiscoveryEnv, ScanEnv, ShallowSession, ShallowSessionProvider};
use crate::hydrate::{IngestOutcome, SelectedIngest, SelectedIngestResult};
use crate::ingest::{insert_session_event, upsert_session, EventRow, SessionCatalogRow};
use crate::sources::catalog::{Discovery, Fixtures, Hydration, LocalSource, Relationships};
use crate::{EvidenceKind, HistoryEntry};

const ID: &str = "test-fixture";
pub(crate) const LOCAL_SOURCE: LocalSource = LocalSource {
    id: ID,
    discovery: Discovery::Provider(|| Box::new(Provider)),
    hydration: Hydration::Parser(ingest),
    transcript_roots: |roots| vec![directory(roots)],
    relationships: Relationships::NONE,
    resume: None,
    fixtures: Fixtures::Dir(ID),
};
pub(crate) const METADATA: ProviderMetadata = ProviderMetadata {
    markers: true,
    message_ids: MessageIdOrigin::Provider,
    usage: None,
};
pub(crate) const ADAPTER: Option<ProviderAdapter> = Some(ProviderAdapter {
    generation: "test_fixture_v1",
    acquire,
    sweep,
    resolve_roots: |home, use_env| {
        vec![resolve_root(
            home,
            use_env,
            "RELAYHISTORY_TEST_FIXTURE_HOME",
            ".history-fixture",
        )]
        .into()
    },
    watch_roots: |roots| vec![WatchRoot::tree(directory(roots))],
});
fn invalid(field: &str) -> anyhow::Error {
    anyhow::anyhow!("TEST_FIXTURE_INVALID_RECORD: missing {field}")
}
fn string<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value[field].as_str().ok_or_else(|| invalid(field))
}
fn messages(value: &Value) -> Result<&Vec<Value>> {
    value["messages"]
        .as_array()
        .ok_or_else(|| invalid("messages"))
}
fn directory(roots: &ProviderRoots) -> PathBuf {
    roots.for_provider(ID)[0].clone()
}
struct Provider;
impl ShallowSessionProvider for Provider {
    fn source(&self) -> &'static str {
        ID
    }
    fn evidence_kinds(&self) -> &'static [EvidenceKind] {
        &[
            EvidenceKind::History,
            EvidenceKind::SessionEvent,
            EvidenceKind::SessionMarker,
        ]
    }
    fn enumerate(&self, env: &DiscoveryEnv<'_>, _: Option<usize>) -> Result<Vec<Candidate>> {
        let dir = directory(&env.provider_roots);
        if !dir.exists() {
            return Ok(Vec::new());
        }
        crate::ingest::collect_matching_files_scoped(&dir, "", "json", env.path_scope())?
            .into_iter()
            .map(|path| {
                let (stamp, recency_hint_ms) = crate::file_stamp_and_modified(&path)?;
                Ok(Candidate {
                    source: ID,
                    locator: path.to_string_lossy().into_owned(),
                    session_id: None,
                    recency_hint_ms,
                    stamp,
                })
            })
            .collect()
    }
    fn read_shallow(
        &self,
        scan: &ScanEnv<'_>,
        _: Option<&Connection>,
        c: &Candidate,
    ) -> Result<Option<ShallowSession>> {
        let context = AcquisitionContext::new(ID, scan.provider_roots);
        let bytes = context.read(Path::new(&c.locator))?;
        scan.note_open();
        scan.note_bytes(bytes.len() as u64);
        let v: Value = serde_json::from_slice(&bytes)?;
        Ok(Some(ShallowSession {
            source: ID.into(),
            session_id: string(&v, "id")?.into(),
            first_prompt: v["messages"][0]["text"].as_str().map(str::to_owned),
            raw_path: Some(c.locator.clone()),
            source_stamp: Some(c.stamp.clone()),
            first_activity_ms: Some(1000),
            last_activity_ms: Some(2000),
            discovery_state: "shallow".into(),
            ..Default::default()
        }))
    }
}
fn acquire(
    options: &HydrateSessionOptions,
    path: &Path,
    context: &AcquisitionContext<'_>,
) -> Result<AcquiredSession> {
    let bytes = context.read(path)?;
    let value: Value = serde_json::from_slice(&bytes)?;
    anyhow::ensure!(
        value["id"].as_str() == Some(&options.session_id),
        "SESSION_SOURCE_MISMATCH: wrong native identity"
    );
    if let Some(sidecar) = value["sidecar"].as_str() {
        file_snapshot(context, vec![PathBuf::from(sidecar)])?;
    }
    let records = messages(&value)?.len() as i64;
    Ok(AcquiredSession::new(
        value,
        bytes.len() as i64,
        records,
        vec![path.into()],
    ))
}
fn normalize(conn: &Connection, payload: &Value, path: &Path) -> Result<usize> {
    let id = string(payload, "id")?;
    let mut inserted = 0;
    for message in messages(payload)? {
        let text = string(message, "text")?;
        let mid = string(message, "id")?;
        let role = string(message, "role")?;
        let ts = message["ts"].as_i64().ok_or_else(|| invalid("ts"))?;
        if role == "user" {
            inserted += crate::insert_history(
                conn,
                &HistoryEntry {
                    id: 0,
                    source: ID.into(),
                    session_id: Some(id.into()),
                    project: None,
                    prompt: text.into(),
                    prompt_hash: None,
                    timestamp_ms: ts,
                },
            )?;
        }
        insert_session_event(
            conn,
            &EventRow {
                source: ID,
                session_id: id,
                message_id: mid,
                event_uid: &format!("{ID}:{id}:{mid}"),
                ts_ms: ts,
                role,
                kind: "text",
                text: Some(text),
                ..Default::default()
            },
        )?;
    }
    crate::insert_session_marker(
        conn,
        ID,
        id,
        &crate::NewSessionMarker {
            marker_uid: "fixture-start",
            kind: "session_start",
            ts_ms: Some(1000),
            ..Default::default()
        },
    )?;
    upsert_session(
        conn,
        &SessionCatalogRow {
            source: ID,
            session_id: id,
            cwd: None,
            git_branch: None,
            first_ts: 1000,
            last_ts: 2000,
            last_assistant_text: Some("fixture assistant answer"),
            raw_path: Some(&path.to_string_lossy()),
        },
    )?;
    Ok(inserted)
}
fn ingest(ctx: SelectedIngest<'_>) -> Result<SelectedIngestResult> {
    anyhow::ensure!(
        !ctx.cursor.extra.contains_key("must_reset"),
        "old provider cursor reused"
    );
    ctx.cursor
        .extra
        .insert("upgraded".into(), Value::Bool(ctx.parser_upgrade));
    let snapshot = ctx
        .provider_session
        .ok_or_else(|| anyhow::anyhow!("missing test fixture snapshot"))?;
    let path = ctx
        .path
        .ok_or_else(|| anyhow::anyhow!("missing test fixture session path"))?;
    normalize(ctx.conn, &snapshot.payload, path)?;
    Ok((
        IngestOutcome {
            records: snapshot.records,
            ..Default::default()
        },
        Vec::new(),
        None,
    ))
}
fn sweep(ctx: SweepContext<'_>) -> Result<SweepResult> {
    let env = DiscoveryEnv::with_provider_roots(ctx.conn, ctx.roots.clone())
        .with_path_scope(ctx.paths.cloned());
    let mut inserted = 0;
    for c in Provider.enumerate(&env, None)? {
        let path = Path::new(&c.locator);
        let context = AcquisitionContext::new(ID, ctx.roots);
        let payload: Value = serde_json::from_slice(&context.read(path)?)?;
        let tx = ctx.conn.unchecked_transaction()?;
        inserted += normalize(&tx, &payload, path)?;
        tx.commit()?;
    }
    Ok(SweepResult {
        inserted,
        unread: 0,
    })
}

#[cfg(test)]
mod tests;

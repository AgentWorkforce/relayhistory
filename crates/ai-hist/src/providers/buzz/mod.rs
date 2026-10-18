//! Agent Relay's append-only capture of native Buzz ACP sessions.
//! Buzz itself keeps these conversations in memory; this source reads only
//! the recorder owned by Agent Relay's detached ACP host.
use super::*;
use crate::discover::{Candidate, DiscoveryEnv, ScanEnv, ShallowSession, ShallowSessionProvider};
use crate::hydrate::{IngestOutcome, SelectedIngest, SelectedIngestResult};
use crate::providers::file_history;
use crate::sources::catalog::{Discovery, Fixtures, Hydration, LocalSource, Relationships};
use crate::EvidenceKind;
use anyhow::Context;
use std::io::Read;

mod normalize;
const ID: &str = "buzz";
const HEAD_LIMIT: u64 = 64 * 1024;
pub(crate) const LOCAL_SOURCE: LocalSource = LocalSource {
    id: ID,
    discovery: Discovery::Provider(|| Box::new(Provider)),
    hydration: Hydration::Parser(ingest),
    transcript_roots: |r| r.for_provider(ID).to_vec(),
    relationships: Relationships::NONE,
    resume: None,
    fixtures: Fixtures::Dir(ID),
};
pub(crate) const METADATA: ProviderMetadata = ProviderMetadata {
    markers: true,
    message_ids: MessageIdOrigin::Mixed,
    usage: None,
};
pub(crate) const ADAPTER: Option<ProviderAdapter> = Some(ProviderAdapter {
    generation: "buzz_acp_v2",
    acquire,
    sweep,
    resolve_roots: roots,
    watch_roots: |r| {
        r.for_provider(ID)
            .iter()
            .cloned()
            .map(WatchRoot::tree)
            .collect()
    },
});

fn roots(home: &Path, use_env: bool) -> ResolvedProviderRoots {
    if let Some(value) = use_env
        .then(|| std::env::var_os("RELAYHISTORY_BUZZ_HOME"))
        .flatten()
        .filter(|v| !v.is_empty())
    {
        return vec![PathBuf::from(value)].into();
    }
    let mut paths = vec![
        home.join("Library/Application Support/com.agentrelay.desktop/spawned-agents/buzz"),
        home.join("Library/Application Support/com.agentrelay.desktop.dev/spawned-agents/buzz"),
        home.join(".local/share/com.agentrelay.desktop/spawned-agents/buzz"),
        home.join(".local/share/com.agentrelay.desktop.dev/spawned-agents/buzz"),
    ];
    if use_env {
        if let Some(xdg) = std::env::var_os("XDG_DATA_HOME") {
            let xdg = PathBuf::from(xdg);
            if xdg.is_absolute() {
                paths.push(xdg.join("com.agentrelay.desktop/spawned-agents/buzz"));
                paths.push(xdg.join("com.agentrelay.desktop.dev/spawned-agents/buzz"));
            }
        }
    }
    paths.sort();
    paths.dedup();
    paths.into()
}
fn valid_id(id: &str) -> bool {
    id.strip_prefix("ses_")
        .is_some_and(|v| v.len() == 16 && v.bytes().all(|b| b.is_ascii_hexdigit()))
}
fn header(entries: &[Value]) -> Result<(&str, i64, Option<&str>)> {
    let first = entries.first().context("BUZZ_INCOMPLETE_HEADER")?;
    anyhow::ensure!(
        first["version"] == 1 && first["kind"] == "session",
        "BUZZ_UNSUPPORTED_SCHEMA"
    );
    let id = first["session_id"]
        .as_str()
        .filter(|s| valid_id(s))
        .context("BUZZ_INVALID_SESSION_ID")?;
    let ts = first["timestamp_ms"]
        .as_i64()
        .context("BUZZ_INVALID_TIMESTAMP")?;
    Ok((id, ts, first["frame"]["cwd"].as_str()))
}
fn parse(bytes: &[u8], shallow: bool) -> Result<(Vec<Value>, Vec<u8>)> {
    let end = bytes
        .iter()
        .rposition(|b| *b == b'\n')
        .context("BUZZ_INCOMPLETE_HEADER")?;
    let mut entries = Vec::new();
    for line in bytes[..end]
        .split(|b| *b == b'\n')
        .filter(|s| !s.is_empty())
    {
        let value: Value = serde_json::from_slice(line).context("BUZZ_INVALID_RECORD")?;
        entries.push(value);
        if shallow && entries.len() >= 128 {
            break;
        }
    }
    let (id, _, _) = header(&entries)?;
    for (index, entry) in entries.iter().enumerate() {
        anyhow::ensure!(
            entry["version"] == 1 && entry["session_id"] == id,
            "BUZZ_SESSION_SOURCE_MISMATCH"
        );
        anyhow::ensure!(
            entry["timestamp_ms"].as_i64().is_some(),
            "BUZZ_INVALID_TIMESTAMP"
        );
        match entry["kind"].as_str() {
            Some("session") => anyhow::ensure!(index == 0, "BUZZ_DUPLICATE_HEADER"),
            Some("prompt") => anyhow::ensure!(
                entry["frame"]["text"]
                    .as_str()
                    .is_some_and(|s| !s.is_empty())
                    && entry["frame"]["delivery_id"].as_str().is_some(),
                "BUZZ_INVALID_PROMPT"
            ),
            Some("acp") => {
                if let Some(frame_id) = entry["frame"]["params"]["sessionId"].as_str() {
                    anyhow::ensure!(frame_id == id, "BUZZ_SESSION_SOURCE_MISMATCH");
                }
            }
            _ => anyhow::bail!("BUZZ_INVALID_RECORD_KIND"),
        }
    }
    Ok((entries, bytes[end + 1..].to_vec()))
}
fn prompt(entry: &Value) -> Option<String> {
    if entry["kind"] != "prompt" {
        return None;
    }
    entry["frame"]["text"]
        .as_str()
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
}
fn first_prompt(entries: &[Value]) -> Option<String> {
    entries
        .iter()
        .filter_map(prompt)
        .next()
        .map(|s| s.chars().take(4096).collect())
}
fn prefix(context: &AcquisitionContext<'_>, path: &Path) -> Result<Vec<u8>> {
    context.validate(path)?;
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(HEAD_LIMIT)
        .read_to_end(&mut bytes)?;
    Ok(bytes)
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
            EvidenceKind::ToolCall,
            EvidenceKind::SessionMarker,
        ]
    }
    fn enumerate(&self, env: &DiscoveryEnv<'_>, _: Option<usize>) -> Result<Vec<Candidate>> {
        let mut out = Vec::new();
        for root in env.provider_roots.for_provider(ID) {
            if env.path_scope().is_some_and(|s| !s.mentions(root)) {
                continue;
            }
            match std::fs::metadata(root) {
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                value => {
                    value.with_context(|| format!("Buzz root {}", root.display()))?;
                }
            }
            for path in crate::ingest::collect_matching_files_scoped(
                root,
                "ses_",
                "jsonl",
                env.path_scope(),
            )? {
                if !path
                    .file_name()
                    .is_some_and(|n| n.to_string_lossy().ends_with(".events.jsonl"))
                {
                    continue;
                }
                let (stamp, recency_hint_ms) = crate::file_stamp_and_modified(&path)?;
                out.push(Candidate {
                    source: ID,
                    locator: path.to_string_lossy().into_owned(),
                    session_id: None,
                    recency_hint_ms,
                    stamp,
                });
            }
        }
        out.sort_by(|a, b| a.locator.cmp(&b.locator));
        out.dedup_by(|a, b| a.locator == b.locator);
        Ok(out)
    }
    fn read_access(&self) -> crate::discover::ShallowReadAccess {
        crate::discover::ShallowReadAccess::Catalog
    }
    fn read_shallow(
        &self,
        scan: &ScanEnv<'_>,
        conn: Option<&Connection>,
        c: &Candidate,
    ) -> Result<Option<ShallowSession>> {
        let bytes = prefix(
            &AcquisitionContext::new(ID, scan.provider_roots),
            Path::new(&c.locator),
        )?;
        scan.note_open();
        scan.note_bytes(bytes.len() as u64);
        let (entries, _) = parse(&bytes, true)?;
        scan.note_records(entries.len() as u64);
        let (sid, first, cwd) = header(&entries)?;
        if let Some(conn) = conn {
            file_history::check_identity(conn, ID, "BUZZ", sid, Path::new(&c.locator))?;
        }
        let last = entries
            .iter()
            .filter_map(|e| e["timestamp_ms"].as_i64())
            .max()
            .unwrap_or(first);
        Ok(Some(ShallowSession {
            source: ID.into(),
            session_id: sid.into(),
            cwd: cwd.map(str::to_owned),
            first_prompt: first_prompt(&entries),
            first_activity_ms: Some(first),
            last_activity_ms: Some(last),
            raw_path: Some(c.locator.clone()),
            source_stamp: Some(c.stamp.clone()),
            discovery_state: "shallow".into(),
            ..Default::default()
        }))
    }
}
fn snapshot(path: &Path, context: &AcquisitionContext<'_>) -> Result<AcquiredSession> {
    let bytes = context.read(path)?;
    let (entries, tail) = parse(&bytes, false)?;
    let records = entries.len() as i64;
    Ok(AcquiredSession::new(
        serde_json::json!({"entries":entries,"tail":tail}),
        bytes.len() as i64,
        records,
        vec![path.into()],
    ))
}
fn acquire(
    options: &HydrateSessionOptions,
    path: &Path,
    context: &AcquisitionContext<'_>,
) -> Result<AcquiredSession> {
    let result = snapshot(path, context)?;
    anyhow::ensure!(
        result.payload["entries"][0]["session_id"].as_str() == Some(&options.session_id),
        "SESSION_SOURCE_MISMATCH: Buzz identity changed"
    );
    Ok(result)
}
fn ingest(ctx: SelectedIngest<'_>) -> Result<SelectedIngestResult> {
    let snapshot = ctx.provider_session.context("Buzz snapshot missing")?;
    normalize::write(
        ctx.conn,
        &snapshot.payload,
        ctx.path.context("Buzz path missing")?,
    )?;
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
    let mut result = SweepResult::default();
    for candidate in Provider.enumerate(&env, None)? {
        crate::ingest::check_capture_cancelled()?;
        let path = Path::new(&candidate.locator);
        let run = || -> Result<(usize, bool)> {
            let snapshot = snapshot(path, &AcquisitionContext::new(ID, ctx.roots))?;
            let tx = ctx.conn.unchecked_transaction()?;
            let count = normalize::write(&tx, &snapshot.payload, path)?;
            tx.commit()?;
            Ok((
                count,
                snapshot.payload["tail"]
                    .as_array()
                    .is_some_and(|v| !v.is_empty()),
            ))
        };
        match run() {
            Ok((count, partial)) => {
                result.inserted += count;
                result.unread += u64::from(partial);
            }
            Err(error) => {
                crate::ingest::check_capture_cancelled()?;
                eprintln!("Buzz {}: {error:#}", path.display());
                result.unread += 1;
            }
        }
    }
    if ctx.paths.is_none() && result.unread == 0 {
        file_history::retire_missing(ctx.conn, ID, ctx.roots)?;
    }
    Ok(result)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_wrong_identity_and_incomplete_header() {
        assert!(parse(b"{", true)
            .unwrap_err()
            .to_string()
            .contains("INCOMPLETE_HEADER"));
        let bad = b"{\"version\":1,\"session_id\":\"../escape\",\"timestamp_ms\":1,\"kind\":\"session\",\"frame\":{}}\n";
        assert!(parse(bad, false)
            .unwrap_err()
            .to_string()
            .contains("INVALID_SESSION_ID"));
    }

    #[test]
    fn resolves_production_and_dev_app_data() {
        let resolved = roots(Path::new("/home/fixture"), false);
        let paths = resolved.paths;
        assert_eq!(paths.len(), 4);
        assert!(paths
            .iter()
            .any(|p| p.ends_with("com.agentrelay.desktop/spawned-agents/buzz")));
        assert!(paths
            .iter()
            .any(|p| p.ends_with("com.agentrelay.desktop.dev/spawned-agents/buzz")));
    }
}

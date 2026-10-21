use super::*;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::io::Read;

const SHALLOW_MAX: u64 = 64 * 1024;
const INDEX_MAX: usize = 8 * 1024 * 1024;

pub(super) fn selected(context: &AcquisitionContext<'_>, path: &Path) -> Result<PathBuf> {
    context.validate(path)?;
    roots::identity(context.roots, path)?;
    Ok(path.to_owned())
}

pub(super) fn enumerate(env: &DiscoveryEnv<'_>, limit: Option<usize>) -> Result<Vec<Candidate>> {
    env.provider_roots.ensure_provider_roots(ID)?;
    let context = AcquisitionContext::new(ID, &env.provider_roots);
    let mut candidates = Vec::new();
    for root in env.provider_roots.for_provider(ID) {
        if env.path_scope().is_some_and(|scope| !scope.mentions(root)) {
            continue;
        }
        candidates.extend(enumerate_root(env, &context, root)?);
    }
    candidates.sort_by(|a, b| a.locator.cmp(&b.locator));
    candidates.dedup_by(|a, b| a.locator == b.locator);
    candidates.sort_by(|a, b| {
        b.recency_hint_ms
            .cmp(&a.recency_hint_ms)
            .then_with(|| a.locator.cmp(&b.locator))
    });
    if let Some(limit) = limit {
        candidates.truncate(limit);
    }
    Ok(candidates)
}

fn enumerate_root(
    env: &DiscoveryEnv<'_>,
    context: &AcquisitionContext<'_>,
    root: &Path,
) -> Result<Vec<Candidate>> {
    if !root.try_exists()? {
        return Ok(Vec::new());
    }
    context.validate(root)?;
    let index = root.join("index.jsonl");
    let index_stamp = if index.try_exists()? {
        context.validate(&index)?;
        crate::file_stamp_and_modified(&index)?.0
    } else {
        "missing-index".into()
    };
    let mut candidates = Vec::new();
    for entry in std::fs::read_dir(root)? {
        crate::ingest::check_capture_cancelled()?;
        let directory = entry?.path();
        context.validate(&directory)?;
        if !directory.is_dir() {
            continue;
        }
        let path = directory.join("events.jsonl");
        if env
            .path_scope()
            .is_some_and(|scope| !scope.mentions(&path) && !scope.mentions(&index))
        {
            continue;
        }
        if !path.try_exists()? {
            continue;
        }
        selected(context, &path)?;
        let (stamp, recency_hint_ms) = crate::file_stamp_and_modified(&path)?;
        candidates.push(Candidate {
            source: ID,
            locator: path.to_string_lossy().into_owned(),
            session_id: None,
            recency_hint_ms,
            stamp: format!("{stamp}:{index_stamp}"),
        });
    }
    Ok(candidates)
}

fn summary(
    context: &AcquisitionContext<'_>,
    path: &Path,
) -> Result<(Value, Vec<u8>, Option<PathBuf>)> {
    let parent = path
        .parent()
        .and_then(Path::parent)
        .context("JUNIE_INVALID_PATH: events parent")?;
    let index_path = parent.join("index.jsonl");
    if !index_path.try_exists()? {
        return Ok((Value::Null, Vec::new(), None));
    }
    context.validate(&index_path)?;
    anyhow::ensure!(
        std::fs::metadata(&index_path)?.len() <= INDEX_MAX as u64,
        "JUNIE_INDEX_TOO_LARGE: bounded index read exceeds 8 MiB"
    );
    let bytes = context.read(&index_path)?;
    anyhow::ensure!(
        bytes.len() <= INDEX_MAX,
        "JUNIE_INDEX_TOO_LARGE: bounded index read exceeds 8 MiB"
    );
    let native = path
        .parent()
        .and_then(Path::file_name)
        .and_then(|name| name.to_str())
        .context("JUNIE_INVALID_ID: session directory")?;
    let mut found = None;
    for (index, line) in bytes.split(|b| *b == b'\n').enumerate() {
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let row: Value = match serde_json::from_slice(line) {
            Ok(row) => row,
            Err(error)
                if error.is_eof()
                    && !bytes.ends_with(b"\n")
                    && index == bytes.split(|b| *b == b'\n').count() - 1 =>
            {
                break
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("JUNIE_INVALID_INDEX: line {}", index + 1))
            }
        };
        let sid = format::string(&row, "sessionId")?;
        anyhow::ensure!(
            row["createdAt"].as_i64().is_some() && row["updatedAt"].as_i64().is_some(),
            "JUNIE_INVALID_INDEX: timestamps"
        );
        if sid == native {
            anyhow::ensure!(
                found.is_none(),
                "JUNIE_INVALID_INDEX: duplicate native session ID"
            );
            found = Some(row);
        }
    }
    Ok((found.unwrap_or(Value::Null), bytes, Some(index_path)))
}

pub(super) fn capture(path: &Path, context: &AcquisitionContext<'_>) -> Result<AcquiredSession> {
    let path = selected(context, path)?;
    let sid = roots::identity(context.roots, &path)?;
    let events = context.read(&path)?;
    let (rows, partial) = format::parse(&events)?;
    let records = rows.len() as i64;
    anyhow::ensure!(
        !rows.is_empty() || partial,
        "JUNIE_EMPTY_JOURNAL: no readable persisted events"
    );
    let (summary, index_bytes, index_path) = summary(context, &path)?;
    let mut read_paths = vec![path.clone()];
    if let Some(index_path) = index_path {
        read_paths.push(index_path);
    }
    let stamp = format!(
        "sha256:{:x}",
        Sha256::digest([events.as_slice(), index_bytes.as_slice()].concat())
    );
    Ok(AcquiredSession {
        stamp,
        payload: serde_json::json!({"sid":sid,"native_id":path.parent().and_then(Path::file_name).and_then(|s|s.to_str()),"path":path,"rows":rows,"summary":summary,"partial":partial}),
        bytes: (events.len() + index_bytes.len()) as i64,
        records,
        read_paths,
    })
}

pub(super) fn shallow(
    scan: &ScanEnv<'_>,
    context: &AcquisitionContext<'_>,
    path: &Path,
) -> Result<AcquiredSession> {
    let path = selected(context, path)?;
    let sid = roots::identity(context.roots, &path)?;
    let (summary, index_bytes, _) = summary(context, &path)?;
    if !index_bytes.is_empty() {
        scan.note_open();
        scan.note_bytes(index_bytes.len() as u64);
    }
    context.validate(&path)?;
    let file = std::fs::File::open(&path)?;
    scan.note_open();
    let length = file.metadata()?.len();
    let mut head = Vec::new();
    file.take(SHALLOW_MAX + 1).read_to_end(&mut head)?;
    scan.note_bytes(head.len() as u64);
    let bounded = length > SHALLOW_MAX;
    if bounded {
        if let Some(end) = head
            .iter()
            .take(SHALLOW_MAX as usize)
            .rposition(|byte| *byte == b'\n')
        {
            head.truncate(end + 1);
        } else {
            anyhow::bail!("JUNIE_OVERSIZED_RECORD: first event exceeds bounded discovery window");
        }
    }
    let (rows, partial) = format::parse(&head)?;
    let records = rows.len() as i64;
    anyhow::ensure!(
        !rows.is_empty() || partial,
        "JUNIE_EMPTY_JOURNAL: no readable persisted events"
    );
    scan.note_records(records as u64);
    Ok(AcquiredSession {
        stamp: String::new(),
        payload: serde_json::json!({"sid":sid,"path":path,"rows":rows,"summary":summary,"partial":partial,"bounded":bounded}),
        bytes: 0,
        records,
        read_paths: vec![path],
    })
}

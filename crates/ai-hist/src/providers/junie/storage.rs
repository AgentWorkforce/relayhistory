use super::*;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::io::Read;
use std::sync::{Arc, Mutex};

const SHALLOW_MAX: u64 = 64 * 1024;
const INDEX_MAX: usize = 8 * 1024 * 1024;

#[derive(Default)]
pub(super) struct IndexCache {
    roots: Mutex<HashMap<PathBuf, Arc<IndexData>>>,
}

impl IndexCache {
    pub(super) fn clear(&self) {
        self.roots.lock().unwrap().clear();
    }

    fn get(&self, context: &AcquisitionContext<'_>, root: &Path) -> Result<(Arc<IndexData>, bool)> {
        let mut roots = self.roots.lock().unwrap();
        if let Some(data) = roots.get(root) {
            return Ok((Arc::clone(data), false));
        }
        let data = Arc::new(IndexData::load(context, root)?);
        roots.insert(root.to_owned(), Arc::clone(&data));
        Ok((data, true))
    }
}

#[derive(Default)]
struct IndexData {
    summaries: HashMap<String, Value>,
    duplicates: HashSet<String>,
    bytes: Vec<u8>,
    path: Option<PathBuf>,
}

impl IndexData {
    fn load(context: &AcquisitionContext<'_>, root: &Path) -> Result<Self> {
        let index_path = root.join("index.jsonl");
        if !index_path.try_exists()? {
            return Ok(Self::default());
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
        let mut summaries = HashMap::new();
        let mut duplicates = HashSet::new();
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
            let sid = format::string(&row, "sessionId")?.to_owned();
            anyhow::ensure!(
                row["createdAt"].as_i64().is_some() && row["updatedAt"].as_i64().is_some(),
                "JUNIE_INVALID_INDEX: timestamps"
            );
            if summaries.insert(sid.clone(), row).is_some() {
                duplicates.insert(sid);
            }
        }
        Ok(Self {
            summaries,
            duplicates,
            bytes,
            path: Some(index_path),
        })
    }
}

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
    cache: Option<&IndexCache>,
) -> Result<(Value, Arc<IndexData>, bool)> {
    let root = path
        .parent()
        .and_then(Path::parent)
        .context("JUNIE_INVALID_PATH: events parent")?;
    let (index, newly_read) = match cache {
        Some(cache) => cache.get(context, root)?,
        None => (Arc::new(IndexData::load(context, root)?), true),
    };
    let native = path
        .parent()
        .and_then(Path::file_name)
        .and_then(|name| name.to_str())
        .context("JUNIE_INVALID_ID: session directory")?;
    anyhow::ensure!(
        !index.duplicates.contains(native),
        "JUNIE_INVALID_INDEX: duplicate native session ID"
    );
    Ok((
        index.summaries.get(native).cloned().unwrap_or(Value::Null),
        index,
        newly_read,
    ))
}

pub(super) fn capture(path: &Path, context: &AcquisitionContext<'_>) -> Result<AcquiredSession> {
    capture_inner(path, context, None)
}

pub(super) fn capture_with_index(
    path: &Path,
    context: &AcquisitionContext<'_>,
    cache: &IndexCache,
) -> Result<AcquiredSession> {
    capture_inner(path, context, Some(cache))
}

fn capture_inner(
    path: &Path,
    context: &AcquisitionContext<'_>,
    cache: Option<&IndexCache>,
) -> Result<AcquiredSession> {
    let path = selected(context, path)?;
    let sid = roots::identity(context.roots, &path)?;
    let events = context.read(&path)?;
    let (rows, partial) = format::parse(&events)?;
    let records = rows.len() as i64;
    anyhow::ensure!(
        !rows.is_empty() || partial,
        "JUNIE_EMPTY_JOURNAL: no readable persisted events"
    );
    let (summary, index, _) = summary(context, &path, cache)?;
    let mut read_paths = vec![path.clone()];
    if let Some(index_path) = &index.path {
        read_paths.push(index_path.clone());
    }
    let mut hash = Sha256::new();
    hash.update(&events);
    hash.update(&index.bytes);
    let stamp = format!("sha256:{:x}", hash.finalize());
    Ok(AcquiredSession {
        stamp,
        payload: serde_json::json!({"sid":sid,"native_id":path.parent().and_then(Path::file_name).and_then(|s|s.to_str()),"path":path,"rows":rows,"summary":summary,"partial":partial}),
        bytes: (events.len() + index.bytes.len()) as i64,
        records,
        read_paths,
    })
}

pub(super) fn shallow(
    scan: &ScanEnv<'_>,
    context: &AcquisitionContext<'_>,
    path: &Path,
    cache: &IndexCache,
) -> Result<AcquiredSession> {
    let path = selected(context, path)?;
    let sid = roots::identity(context.roots, &path)?;
    let (summary, index, newly_read) = summary(context, &path, Some(cache))?;
    if newly_read && index.path.is_some() {
        scan.note_open();
        scan.note_bytes(index.bytes.len() as u64);
    }
    context.validate(&path)?;
    let file = std::fs::File::open(&path)?;
    scan.note_open();
    let length = file.metadata()?.len();
    let mut head = Vec::new();
    file.take(SHALLOW_MAX + 1).read_to_end(&mut head)?;
    scan.note_bytes(head.len() as u64);
    let bounded = length > SHALLOW_MAX;
    let mut oversized_first = false;
    if bounded {
        if let Some(end) = head
            .iter()
            .take(SHALLOW_MAX as usize)
            .rposition(|byte| *byte == b'\n')
        {
            head.truncate(end + 1);
        } else {
            // The index and directory still identify a selectable session.
            // A selected hydrate can read the complete native prompt later.
            head.clear();
            oversized_first = true;
        }
    }
    let (rows, partial) = if oversized_first {
        (Vec::new(), false)
    } else {
        format::parse(&head)?
    };
    let records = rows.len() as i64;
    anyhow::ensure!(
        !rows.is_empty() || partial || oversized_first,
        "JUNIE_EMPTY_JOURNAL: no readable persisted events"
    );
    scan.note_records(records as u64);
    Ok(AcquiredSession {
        stamp: String::new(),
        payload: serde_json::json!({"sid":sid,"path":path,"rows":rows,"summary":summary,"partial":partial,"bounded":bounded,"oversized_first":oversized_first}),
        bytes: 0,
        records,
        read_paths: vec![path],
    })
}

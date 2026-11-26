//! A single immutable roster read per account and discovery pass. A registry
//! can retain a provider across calls, so clear cached rows at both pass edges.
use super::*;
use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard, PoisonError};

type Rows = HashMap<String, Value>;
type Cache = Mutex<HashMap<PathBuf, Option<Rows>>>;

#[derive(Default)]
pub(super) struct RosterCache {
    pass: Mutex<()>,
    rows: Cache,
}

struct Pass<'a> {
    _lock: MutexGuard<'a, ()>,
    rows: &'a Cache,
}

impl Drop for Pass<'_> {
    fn drop(&mut self) {
        self.rows
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
    }
}

impl RosterCache {
    pub(super) fn begin(&self) -> impl crate::discover::DiscoveryPassGuard + '_ {
        let lock = self.pass.lock().unwrap_or_else(PoisonError::into_inner);
        self.rows
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
        Pass {
            _lock: lock,
            rows: &self.rows,
        }
    }

    pub(super) fn title(&self, transcript: &Path, scan: &ScanEnv<'_>) -> Option<String> {
        let (slot, id) = format::replica_identity(transcript)?;
        let path = format::roster_path(transcript, &slot);
        let mut cache = self.rows.lock().unwrap_or_else(PoisonError::into_inner);
        let rows = cache.entry(path.clone()).or_insert_with(|| {
            let context = AcquisitionContext::new(ID, scan.provider_roots);
            let (roster, _) = read_roster_observed(&path, &context, |bytes| {
                scan.note_open();
                scan.note_bytes(bytes as u64);
            })?;
            let mut indexed = Rows::new();
            for row in roster["value"]["rows"].as_array()? {
                if let Some(id) = row["id"].as_str() {
                    indexed.entry(id.to_owned()).or_insert_with(|| row.clone());
                }
            }
            Some(indexed)
        });
        profile::name(rows.as_ref()?.get(&id)?)
    }
}

#[cfg(test)]
mod tests;

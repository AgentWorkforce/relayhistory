//! Whether a local hydration reads a session's evidence back.

use super::{Family, Key};
use crate::ingest::check_capture_cancelled;
use crate::ProviderRoots;
use rusqlite::{params, Connection};
use std::collections::{BTreeSet, HashMap, HashSet};

/// Claude transcripts the sweep's cursors know, by the session id their
/// records carry.
fn claude_transcripts(conn: &Connection) -> anyhow::Result<HashMap<String, Vec<String>>> {
    let mut statement = conn.prepare(
        "SELECT json_extract(parser_state_json, '$.claude.scan.fold.session_id'), locator \
         FROM transcript_cursors WHERE source = 'claude' AND json_valid(parser_state_json)",
    )?;
    let mut rows = statement.query([])?;
    let mut transcripts: HashMap<String, Vec<String>> = HashMap::new();
    while let Some(row) = rows.next()? {
        if let Some(session_id) = row.get::<_, Option<String>>(0)? {
            transcripts.entry(session_id).or_default().push(row.get(1)?);
        }
    }
    Ok(transcripts)
}

/// Whether the session has evidence rows, as against only a catalog row that
/// claims a hydrated state.
fn holds_evidence(conn: &Connection, (source, session_id): &Key) -> anyhow::Result<bool> {
    Ok(conn
        .prepare_cached(
            "SELECT EXISTS(SELECT 1 FROM session_events WHERE source = ?1 AND session_id = ?2) \
             OR EXISTS(SELECT 1 FROM tool_calls WHERE source = ?1 AND session_id = ?2) \
             OR EXISTS(SELECT 1 FROM file_edits WHERE source = ?1 AND session_id = ?2) \
             OR EXISTS(SELECT 1 FROM session_markers WHERE source = ?1 AND session_id = ?2) \
             OR EXISTS(SELECT 1 FROM observation_evidence WHERE source = ?1 AND session_id = ?2)",
        )?
        .query_row(params![source, session_id], |row| row.get(0))?)
}

/// Whether a local hydration reads a session's evidence back, answered by
/// the snapshot hydration itself takes.
pub(super) struct Recovery<'a> {
    conn: &'a Connection,
    roots: &'a ProviderRoots,
    family: &'a Family,
    catalog: &'a HashSet<Key>,
    /// Every Claude transcript a sweep has read, by the session id its
    /// records carry.
    transcripts: HashMap<String, Vec<String>>,
    probe: crate::ingest::hydrate::HydrationProbe,
    /// Each catalogued session's hydration reads, once probed; `None` when
    /// hydration would refuse it.
    reads: HashMap<Key, Option<HashSet<String>>>,
}

impl<'a> Recovery<'a> {
    pub(super) fn new(
        conn: &'a Connection,
        roots: &'a ProviderRoots,
        family: &'a Family,
        catalog: &'a HashSet<Key>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            conn,
            roots,
            family,
            catalog,
            transcripts: claude_transcripts(conn)?,
            probe: crate::ingest::hydrate::HydrationProbe::default(),
            reads: HashMap::new(),
        })
    }

    /// The sessions of `scope` to forget, and how many holding evidence were
    /// skipped as unrecoverable.
    pub(super) fn select(
        &mut self,
        scope: &BTreeSet<Key>,
        include_unrecoverable: bool,
    ) -> anyhow::Result<(BTreeSet<Key>, u64)> {
        let mut forget = BTreeSet::new();
        let mut skipped = 0;
        for key in scope {
            check_capture_cancelled()?;
            if include_unrecoverable || self.recoverable(key)? {
                forget.insert(key.clone());
            } else if holds_evidence(self.conn, key)? {
                skipped += 1;
            }
        }
        Ok((forget, skipped))
    }

    /// Every surviving catalogued ancestor whose hydration re-reads one of
    /// `locators`, the child's transcripts, through any catalog-less
    /// intermediaries: its checkpoint vouches for evidence that is about to
    /// go.
    pub(super) fn kept_parents(
        &mut self,
        key: &Key,
        locators: &[String],
        forget: &BTreeSet<Key>,
        scope: &BTreeSet<Key>,
    ) -> anyhow::Result<Vec<Key>> {
        let mut kept = Vec::new();
        for ancestor in self.family.catalogued_ancestors(key, self.catalog) {
            if forget.contains(&ancestor) || scope.contains(&ancestor) {
                continue;
            }
            if self.catalogued_recoverable(&ancestor)?
                && self
                    .reads(&ancestor)?
                    .is_some_and(|reads| locators.iter().any(|locator| reads.contains(locator)))
            {
                kept.push(ancestor);
            }
        }
        Ok(kept)
    }

    fn recoverable(&mut self, key: &Key) -> anyhow::Result<bool> {
        if self.catalog.contains(key) {
            return self.catalogued_recoverable(key);
        }
        if self.foreign(key)? {
            return Ok(false);
        }
        self.enumerated_by_ancestor(key)
    }

    /// A catalogued session hydration reads back in full: hydration accepts
    /// it, nothing of it came from another connector, and every transcript a
    /// sweep indexed it from is one hydration reads. A Claude conversation
    /// forked into branch files keeps its id in each of them; a sweep indexes
    /// all of them under that id, hydration only the catalogued one.
    fn catalogued_recoverable(&mut self, key: &Key) -> anyhow::Result<bool> {
        if self.foreign(key)? {
            return Ok(false);
        }
        let swept = match key.0.as_str() {
            "claude" => self.transcripts.get(&key.1).cloned().unwrap_or_default(),
            _ => Vec::new(),
        };
        Ok(self
            .reads(key)?
            .is_some_and(|reads| swept.iter().all(|locator| reads.contains(locator))))
    }

    /// A catalog-less child comes back only through the hydration of a
    /// catalogued ancestor that itself passes, and that hydration has to
    /// read every transcript the child's evidence came from: the child's own
    /// locators, checked against each ancestor, never an intermediary's.
    fn enumerated_by_ancestor(&mut self, key: &Key) -> anyhow::Result<bool> {
        let locators = self.family.locators.get(key).cloned().unwrap_or_default();
        if locators.is_empty() {
            return Ok(false);
        }
        for ancestor in self.family.catalogued_ancestors(key, self.catalog) {
            if self.rereads(&ancestor, &locators)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Whether `ancestor` hydrates back and its hydration reads every one of
    /// `locators`.
    fn rereads(&mut self, ancestor: &Key, locators: &[String]) -> anyhow::Result<bool> {
        if !self.catalogued_recoverable(ancestor)? {
            return Ok(false);
        }
        Ok(self
            .reads(ancestor)?
            .is_some_and(|reads| locators.iter().all(|locator| reads.contains(locator))))
    }

    fn reads(&mut self, key: &Key) -> anyhow::Result<Option<&HashSet<String>>> {
        if !self.reads.contains_key(key) {
            let reads = self
                .probe
                .reads(self.conn, &key.0, &key.1, self.roots)?
                .map(|paths| {
                    paths
                        .into_iter()
                        .map(|path| path.to_string_lossy().into_owned())
                        .collect()
                });
            self.reads.insert(key.clone(), reads);
        }
        Ok(self.reads.get(key).and_then(Option::as_ref))
    }

    /// Evidence a remote connector or plugin intake supplied: local
    /// hydration reads only the builtin local adapter's source.
    fn foreign(&self, (source, session_id): &Key) -> anyhow::Result<bool> {
        Ok(self
            .conn
            .prepare_cached(
                "SELECT EXISTS(SELECT 1 FROM session_presences \
                   WHERE source = ?1 AND session_id = ?2 AND location <> 'local') \
                 OR EXISTS(SELECT 1 FROM session_observations \
                   WHERE source = ?1 AND session_id = ?2 AND (location <> 'local' \
                   OR (connector_id NOT IN (?1, 'legacy-unknown') \
                   OR connector_instance <> 'default')))",
            )?
            .query_row(params![source, session_id], |row| row.get(0))?)
    }
}

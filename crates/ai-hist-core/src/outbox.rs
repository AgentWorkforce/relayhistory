//! WS-9 cloud-sync increment 2a: the outbox builder (pure sync logic, no network).
//!
//! Reads new local rows past a resume cursor, maps them to WS-1 convergence envelopes
//! (via [`crate::convergence`]), applies the incognito exclusion, and returns the batch
//! plus the advanced cursor. Network I/O (POST `/v1/ingest`, `rth_` auth) lives in the
//! binding layer (the `ai-hist` binary) per the no-async-in-core rule — this module only
//! does sync rusqlite reads, so it is fully unit-testable without a server.
//!
//! Cursor model (mirrors burn's `archive_state` watermark): monotonic `history.id`,
//! a trajectories keyset `(updated_ms, rowid)` so rows revised after first sync re-push
//! without skipping equal-timestamp boundaries (the server upsert is already safe),
//! `session_commit_links.id` for `session_outcome` envelopes, and `file_edits.id` so a
//! session whose edits grow after its last prompt envelope republishes `filesTouched`.

use crate::convergence::{
    map_history_entry_with, map_session_outcome, map_trajectory, normalize_home_path,
    resolve_project_id, ConvergenceEnvelope, SessionCommitLink, TokenUsage, TrajectoryRow,
    UNKNOWN_PROJECT,
};
use crate::{
    session_events, session_file_edits, session_file_edits_page, HistoryEntry, SessionEvent,
    SessionEvidenceCursor, SessionFileEdit,
};
use anyhow::Result;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::Path;

/// Resume watermarks for incremental cloud sync (the local cursor store). Persisted by the
/// binding layer (single cursor store) and advanced to the server-confirmed values after a
/// successful `/v1/ingest`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SyncCursor {
    /// Highest `history.id` included in a synced batch.
    #[serde(default)]
    pub history_id: i64,
    /// `trajectories.rowid` of the last row emitted in keyset order.
    #[serde(default)]
    pub trajectory_rowid: i64,
    /// `trajectories.updated_ms` of the last row emitted in keyset order. Together
    /// with `trajectory_rowid` this is a total continuation key, not an independent max.
    #[serde(default)]
    pub trajectory_updated_ms: i64,
    /// Highest `session_commit_links.id` included in a synced batch.
    #[serde(default)]
    pub commit_link_id: i64,
    /// Highest `file_edits.id` whose session's `filesTouched` has been published.
    #[serde(default)]
    pub file_edit_id: i64,
    /// Highest `session_events.id` whose session's conversation turns have been
    /// published. Independent of `capture_version`: a cursor file written before turns
    /// existed defaults to 0 and publishes the whole transcript backlog, without
    /// forcing another convergence replay.
    #[serde(default)]
    pub session_event_id: i64,
    /// Mapper generation. Missing from pre-neighborhood-memory cursor files (serde
    /// default 0). Those rewind history/trajectory watermarks once so existing
    /// rows are re-upserted with `projectId` / `filesTouched`.
    #[serde(default)]
    pub capture_version: i64,
}

impl Default for SyncCursor {
    fn default() -> Self {
        Self {
            history_id: 0,
            trajectory_rowid: 0,
            trajectory_updated_ms: 0,
            commit_link_id: 0,
            file_edit_id: 0,
            session_event_id: 0,
            capture_version: Self::CAPTURE_VERSION,
        }
    }
}

impl SyncCursor {
    /// Neighborhood-memory capture mapper. Bump when an upgraded client must
    /// re-upsert already-synced rows with newly populated fields.
    // Re-upsert existing prompts once: their original envelopes omitted token usage.
    pub const CAPTURE_VERSION: i64 = 2;

    /// Advance watermarks so a stale writer cannot rewind one. History and
    /// commit-link ids are independent maxima. Trajectories use a single keyset
    /// position `(updated_ms, rowid)` — taking those two fields independently
    /// skips equal-timestamp rows when `LIMIT` splits a batch. A higher
    /// `capture_version` may rewind history/trajectory positions to backfill.
    pub fn merge_max(&self, other: &Self) -> Self {
        if other.capture_version > self.capture_version {
            return Self {
                capture_version: other.capture_version,
                history_id: other.history_id,
                trajectory_rowid: other.trajectory_rowid,
                trajectory_updated_ms: other.trajectory_updated_ms,
                commit_link_id: self.commit_link_id.max(other.commit_link_id),
                file_edit_id: other.file_edit_id,
                // Deliberately NOT rewound. `capture_version` is the convergence
                // mapper's generation; the transcript is a separate stream with its
                // own watermark. Rewinding it here would republish the whole turns
                // backlog every time the convergence mapper changes.
                session_event_id: self.session_event_id.max(other.session_event_id),
            };
        }
        if self.capture_version > other.capture_version {
            return self.clone();
        }
        let (trajectory_updated_ms, trajectory_rowid) = if other.trajectory_key_after(self) {
            (other.trajectory_updated_ms, other.trajectory_rowid)
        } else {
            (self.trajectory_updated_ms, self.trajectory_rowid)
        };
        Self {
            history_id: self.history_id.max(other.history_id),
            trajectory_rowid,
            trajectory_updated_ms,
            commit_link_id: self.commit_link_id.max(other.commit_link_id),
            file_edit_id: self.file_edit_id.max(other.file_edit_id),
            session_event_id: self.session_event_id.max(other.session_event_id),
            capture_version: self.capture_version,
        }
    }

    fn migrated(&self) -> Self {
        if self.capture_version >= Self::CAPTURE_VERSION {
            return self.clone();
        }
        Self {
            capture_version: Self::CAPTURE_VERSION,
            history_id: 0,
            trajectory_rowid: 0,
            trajectory_updated_ms: 0,
            commit_link_id: self.commit_link_id,
            file_edit_id: 0,
            // Carried, not reset: the convergence backfill has nothing to do with the
            // transcript stream, and restarting it would re-send every turn.
            session_event_id: self.session_event_id,
        }
    }

    fn trajectory_key_after(&self, other: &Self) -> bool {
        self.trajectory_updated_ms > other.trajectory_updated_ms
            || (self.trajectory_updated_ms == other.trajectory_updated_ms
                && self.trajectory_rowid > other.trajectory_rowid)
    }
}

/// The next outbox batch: the envelopes to POST and the cursor they advance to.
#[derive(Debug, Clone, PartialEq)]
pub struct OutboxBatch {
    pub records: Vec<ConvergenceEnvelope>,
    pub cursor: SyncCursor,
}

/// Build the next batch of convergence envelopes from local rows past `cursor`.
///
/// - `limit` caps rows scanned **per source** (history, trajectories).
/// - `incognito` holds session ids (history `session_id` / trajectory `id`) to exclude —
///   incognito rows are skipped but still advance the cursor so they are never re-scanned.
/// - The returned cursor advances to the max id/rowid *scanned* (not just emitted), so
///   skipped/empty rows don't cause re-scanning on the next call.
pub fn build_outbox_batch(
    conn: &Connection,
    cursor: &SyncCursor,
    limit: usize,
    incognito: &HashSet<String>,
) -> Result<OutboxBatch> {
    let limit = limit.max(1) as i64;
    // The server caps an ingest batch at 1000 records. A single trajectory row — especially
    // a compacted roll-up — expands to many convergence events (decisions + findings +
    // reflections), so the batch must be bounded on EMITTED records, not rows scanned, or a
    // handful of roll-ups blows past the cap and the push 400s.
    const MAX_RECORDS: usize = 900;
    let cursor = cursor.migrated();
    let mut records = Vec::new();
    let mut next = cursor.clone();
    let mut remotes: HashMap<String, Option<String>> = HashMap::new();
    let mut branches: HashMap<String, Option<String>> = HashMap::new();
    let mut file_cache: HashMap<(String, String), Vec<String>> = HashMap::new();
    let mut usage_cache = HashMap::new();
    let mut published_sessions: HashSet<(String, String)> = HashSet::new();

    // --- history (prompts) — append-only, watermark on id ---
    {
        let mut stmt = conn.prepare(
            "SELECT id, source, session_id, project, prompt, prompt_hash, timestamp_ms \
             FROM history WHERE id > ?1 ORDER BY id ASC LIMIT ?2",
        )?;
        let rows = stmt.query_map([cursor.history_id, limit], |r| {
            Ok(HistoryEntry {
                id: r.get(0)?,
                source: r.get(1)?,
                session_id: r.get(2)?,
                project: r.get(3)?,
                prompt: r.get(4)?,
                prompt_hash: r.get(5)?,
                timestamp_ms: r.get(6)?,
            })
        })?;
        for row in rows {
            let entry = row?;
            // Stop before consuming this row if the batch is full, so the cursor does not
            // advance past an un-emitted row (the next batch resumes from here).
            if records.len() >= MAX_RECORDS {
                break;
            }
            next.history_id = next.history_id.max(entry.id);
            // incognito: skip rows whose session is suppressed (still advances cursor)
            if let Some(sid) = &entry.session_id {
                if incognito.contains(sid) {
                    continue;
                }
            }
            let git_remote = git_remote_for_entry(conn, &entry, &mut remotes);
            let files = match &entry.session_id {
                Some(sid) => session_files(conn, &mut file_cache, Some(&entry.source), sid)?,
                None => Vec::new(),
            };
            if let Some(sid) = &entry.session_id {
                published_sessions.insert((entry.source.clone(), sid.clone()));
            }
            let branch = session_branch_for_entry(conn, &entry, &mut branches);
            let usage = session_usage_for_entry(conn, &entry, &mut usage_cache)?;
            records.push(map_history_entry_with(
                &entry,
                git_remote.as_deref(),
                files,
                branch.as_deref(),
                usage,
            ));
        }
    }

    // --- trajectories (decisions/retro) — keyset on (updated_ms, rowid) ---
    {
        let mut stmt = conn.prepare(
            "SELECT rowid, id, persona_id, project_id, task_title, task_description, status, \
             decisions_json, retrospective_json, timestamp_ms, updated_ms, path \
             FROM trajectories \
             WHERE updated_ms > ?1 OR (updated_ms = ?1 AND rowid > ?2) \
             ORDER BY updated_ms ASC, rowid ASC LIMIT ?3",
        )?;
        let raw = stmt.query_map(
            rusqlite::params![cursor.trajectory_updated_ms, cursor.trajectory_rowid, limit],
            |r| {
                Ok(TrajRowOwned {
                    rowid: r.get(0)?,
                    id: r.get(1)?,
                    persona_id: r.get(2)?,
                    project_id: r.get(3)?,
                    task_title: r.get(4)?,
                    task_description: r.get(5)?,
                    status: r.get(6)?,
                    decisions_json: r.get(7)?,
                    retrospective_json: r.get(8)?,
                    timestamp_ms: r.get(9)?,
                    updated_ms: r.get(10)?,
                    path: r.get(11)?,
                })
            },
        )?;
        for row in raw {
            let t = row?;
            if incognito.contains(&t.id) {
                next.trajectory_rowid = t.rowid;
                next.trajectory_updated_ms = t.updated_ms;
                continue;
            }
            let mut mapped = map_trajectory(&TrajectoryRow {
                id: &t.id,
                persona_id: t.persona_id.as_deref(),
                project_id: t.project_id.as_deref(),
                task_title: t.task_title.as_deref(),
                task_description: t.task_description.as_deref(),
                status: t.status.as_deref(),
                task_ref: None, // not in local store yet (see convergence::TrajectoryRow)
                decisions_json: &t.decisions_json,
                retrospective_json: &t.retrospective_json,
                timestamp_ms: t.timestamp_ms,
            });
            // A compacted roll-up expands to many events. If adding this row would exceed the
            // batch cap and we already have records, defer it to the next batch — leave the
            // cursor before it so nothing is skipped. (A lone row under the cap always fits.)
            if !records.is_empty() && records.len() + mapped.len() > MAX_RECORDS {
                break;
            }
            let files = trajectory_files(conn, &mut file_cache, &t.id, t.path.as_deref())?;
            for env in &mut mapped {
                env.files_touched = files.clone();
            }
            next.trajectory_rowid = t.rowid;
            next.trajectory_updated_ms = t.updated_ms;
            records.extend(mapped);
        }
    }

    // --- session_commit_links → kind=session_outcome (one envelope per link row) ---
    {
        let mut stmt = conn.prepare(
            "SELECT id, source, session_id, repo, branch, commit_sha, match_method, confidence, \
             files_json, numstat_json, evidence_json, created_at_ms \
             FROM session_commit_links WHERE id > ?1 ORDER BY id ASC LIMIT ?2",
        )?;
        let rows = stmt.query_map([cursor.commit_link_id, limit], |r| {
            Ok(CommitLinkOwned {
                id: r.get(0)?,
                source: r.get(1)?,
                session_id: r.get(2)?,
                repo: r.get(3)?,
                branch: r.get(4)?,
                commit_sha: r.get(5)?,
                match_method: r.get(6)?,
                confidence: r.get(7)?,
                files_json: r.get(8)?,
                numstat_json: r.get(9)?,
                evidence_json: r.get(10)?,
                created_at_ms: r.get(11)?,
            })
        })?;
        for row in rows {
            let link = row?;
            if records.len() + 2 > MAX_RECORDS {
                break;
            }
            next.commit_link_id = next.commit_link_id.max(link.id);
            if incognito.contains(&link.session_id) {
                continue;
            }
            let files = session_files(conn, &mut file_cache, Some(&link.source), &link.session_id)?;
            let project_id = session_project_id(
                conn,
                &link.source,
                &link.session_id,
                link.repo.as_deref(),
                &mut remotes,
            );
            let outcome = map_session_outcome(
                &SessionCommitLink {
                    source: &link.source,
                    session_id: &link.session_id,
                    repo: link.repo.as_deref(),
                    branch: link.branch.as_deref(),
                    commit_sha: &link.commit_sha,
                    match_method: &link.match_method,
                    confidence: link.confidence,
                    files_json: link.files_json.as_deref(),
                    numstat_json: link.numstat_json.as_deref(),
                    evidence_json: link.evidence_json.as_deref(),
                    created_at_ms: link.created_at_ms,
                },
                &project_id,
                files,
            );
            if let Some(task_ref) = link
                .evidence_json
                .as_deref()
                .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
                .and_then(|value| value.get("github_pr").cloned())
            {
                let mut event = outcome.clone();
                event.kind = "event".into();
                event.lens = Some("github".into());
                event.event_type = "github_pr".into();
                event.event_id = format!("github_pr:{}", outcome.event_id);
                event.task_ref = Some(task_ref);
                records.push(event);
            }
            records.push(outcome);
        }
    }

    // --- file_edits that grew after the last prompt envelope for a session ---
    {
        struct FileEditWatermark {
            source: String,
            session_id: String,
            max_id: i64,
        }
        let mut stmt = conn.prepare(
            "SELECT source, session_id, MAX(id) AS max_id \
             FROM file_edits WHERE id > ?1 \
             GROUP BY source, session_id \
             ORDER BY max_id ASC LIMIT ?2",
        )?;
        let rows = stmt.query_map([cursor.file_edit_id, limit], |r| {
            Ok(FileEditWatermark {
                source: r.get(0)?,
                session_id: r.get(1)?,
                max_id: r.get(2)?,
            })
        })?;
        for row in rows {
            let hit = row?;
            if records.len() >= MAX_RECORDS {
                break;
            }
            next.file_edit_id = next.file_edit_id.max(hit.max_id);
            if incognito.contains(&hit.session_id) {
                continue;
            }
            if published_sessions.contains(&(hit.source.clone(), hit.session_id.clone())) {
                continue;
            }
            let Some(entry) = latest_history_for_session(conn, &hit.source, &hit.session_id)?
            else {
                continue;
            };
            let git_remote = git_remote_for_entry(conn, &entry, &mut remotes);
            let files = session_files(conn, &mut file_cache, Some(&entry.source), &hit.session_id)?;
            let branch = session_branch_for_entry(conn, &entry, &mut branches);
            let usage = session_usage_for_entry(conn, &entry, &mut usage_cache)?;
            records.push(map_history_entry_with(
                &entry,
                git_remote.as_deref(),
                files,
                branch.as_deref(),
                usage,
            ));
            published_sessions.insert((hit.source, hit.session_id));
        }
    }

    Ok(OutboxBatch {
        records,
        cursor: next,
    })
}

/// Owned trajectory row (rusqlite can't borrow across the row closure).
struct TrajRowOwned {
    rowid: i64,
    id: String,
    persona_id: Option<String>,
    project_id: Option<String>,
    task_title: Option<String>,
    task_description: Option<String>,
    status: Option<String>,
    decisions_json: String,
    retrospective_json: String,
    timestamp_ms: i64,
    updated_ms: i64,
    path: Option<String>,
}

struct CommitLinkOwned {
    id: i64,
    source: String,
    session_id: String,
    repo: Option<String>,
    branch: Option<String>,
    commit_sha: String,
    match_method: String,
    confidence: f64,
    files_json: Option<String>,
    numstat_json: Option<String>,
    evidence_json: Option<String>,
    created_at_ms: i64,
}

fn latest_history_for_session(
    conn: &Connection,
    source: &str,
    session_id: &str,
) -> Result<Option<HistoryEntry>> {
    let mut stmt = conn.prepare(
        "SELECT id, source, session_id, project, prompt, prompt_hash, timestamp_ms \
         FROM history WHERE source = ?1 AND session_id = ?2 ORDER BY id DESC LIMIT 1",
    )?;
    let mut rows = stmt.query_map(rusqlite::params![source, session_id], |r| {
        Ok(HistoryEntry {
            id: r.get(0)?,
            source: r.get(1)?,
            session_id: r.get(2)?,
            project: r.get(3)?,
            prompt: r.get(4)?,
            prompt_hash: r.get(5)?,
            timestamp_ms: r.get(6)?,
        })
    })?;
    Ok(rows.next().transpose()?)
}

type PromptKey = (i64, String);
type UsageCache = HashMap<(String, String), HashMap<PromptKey, TokenUsage>>;

fn session_usage_for_entry(
    conn: &Connection,
    entry: &HistoryEntry,
    cache: &mut UsageCache,
) -> Result<Option<TokenUsage>> {
    // A present-but-blank session id is not a session. Treating it as one makes every
    // malformed row across the store share the cache key `(source, "")`, so
    // `session_events` returns an unrelated pile of rows and their usage gets attributed
    // to prompts that did not produce it. That is the same failure this function is built
    // to avoid — a confident wrong number rather than an honest gap — so blank reads as
    // missing and the row stays unattributed.
    // Trim TESTS for blankness; it must not change the identity. `session_events` matches
    // `session_id` exactly, so looking up a trimmed copy of a genuine id like " abc "
    // would find nothing — or another session — and silently lose or misattribute usage.
    let Some(sid) = entry
        .session_id
        .as_deref()
        .filter(|sid| !sid.trim().is_empty())
    else {
        return Ok(None);
    };
    let key = (entry.source.clone(), sid.to_string());
    if !cache.contains_key(&key) {
        let events = session_events(conn, sid, Some(&entry.source))?;
        cache.insert(key.clone(), attribute_session_usage(&events, &entry.source));
    }
    Ok(cache[&key]
        .get(&(entry.timestamp_ms, entry.prompt.clone()))
        .cloned())
}

struct UsageMessage {
    id: String,
    parent: Option<String>,
    ts: i64,
    role: String,
    text: String,
    usage: Option<TokenUsage>,
}

fn attribute_session_usage(
    events: &[SessionEvent],
    source: &str,
) -> HashMap<PromptKey, TokenUsage> {
    let mut groups: HashMap<&str, Vec<&SessionEvent>> = HashMap::new();
    for event in events {
        if let Some(id) = event.message_id.as_deref().filter(|id| !id.is_empty()) {
            groups.entry(id).or_default().push(event);
        }
    }
    let messages: HashMap<String, UsageMessage> = groups
        .into_iter()
        .filter_map(|(id, rows)| {
            let first = rows[0];
            if rows.iter().any(|r| {
                r.ts_ms != first.ts_ms || r.role != first.role || r.parent_id != first.parent_id
            }) {
                return None;
            }
            // Claude copies message.usage onto every content block. Counting rows
            // would multiply one model request by its thinking/text/tool block count.
            let tokens: Option<Vec<serde_json::Value>> = rows
                .iter()
                .filter_map(|r| r.token_json.as_deref())
                .map(|raw| serde_json::from_str(raw).ok())
                .collect();
            let usage = tokens.and_then(|tokens| {
                let first = tokens.first()?;
                if tokens.iter().any(|value| value != first) {
                    return None;
                }
                parse_token_usage(first, source)
            });
            let text = rows
                .iter()
                .filter(|r| r.kind == "text")
                .filter_map(|r| r.text.as_deref())
                .map(str::trim)
                .filter(|text| !text.is_empty())
                .collect::<Vec<_>>()
                .join("\n");
            Some((
                id.to_string(),
                UsageMessage {
                    id: id.to_string(),
                    parent: first.parent_id.clone(),
                    ts: first.ts_ms,
                    role: first.role.clone(),
                    text,
                    usage,
                },
            ))
        })
        .collect();
    // Keep even unidentifiable user events as boundaries; dropping one would
    // incorrectly charge its answer to the preceding identifiable prompt.
    let mut boundaries: Vec<_> = events.iter().filter(|e| e.role == "user").collect();
    boundaries.sort_by_key(|e| e.ts_ms);
    // Parsers use zero when time is missing. Such a turn could fall anywhere,
    // so timestamp-only ownership is unsafe for the session.
    let timestamps_known = boundaries.iter().all(|e| e.ts_ms > 0);
    let mut prompt_counts = HashMap::new();
    for user in messages.values().filter(|m| m.role == "user") {
        *prompt_counts
            .entry((user.ts, user.text.clone()))
            .or_insert(0) += 1;
    }
    let mut attributed: HashMap<PromptKey, Option<TokenUsage>> = HashMap::new();
    for message in messages.values().filter(|m| m.role == "assistant") {
        let Some(usage) = &message.usage else {
            continue;
        };
        let owner = if message.parent.is_some() {
            parent_prompt(message, &messages)
        } else if source == "codex" && message.ts > 0 && timestamps_known {
            // Codex persists no parent IDs. Only its ordered human-turn stream
            // establishes ownership: a tie at either boundary is ambiguous, and
            // an explicit but broken parent link must never fall back to time.
            let boundary = boundaries.partition_point(|u| u.ts_ms < message.ts);
            if boundaries
                .get(boundary)
                .is_some_and(|u| u.ts_ms == message.ts)
            {
                None
            } else {
                boundary.checked_sub(1).and_then(|i| {
                    let user = boundaries[i];
                    if user.ts_ms <= 0 || (i > 0 && boundaries[i - 1].ts_ms == user.ts_ms) {
                        return None;
                    }
                    messages.get(user.message_id.as_deref()?)
                })
            }
        } else {
            None
        };
        let Some(owner) = owner else { continue };
        let key = (owner.ts, owner.text.clone());
        // Identical text and timestamps cannot distinguish two user messages.
        // Assigning both to a single history row would conceal a bad join.
        if owner.text.is_empty() || prompt_counts.get(&key) != Some(&1) {
            continue;
        }
        let total = attributed
            .entry(key)
            .or_insert_with(|| Some(TokenUsage::default()));
        *total = total.as_ref().and_then(|total| add_usage(total, usage));
    }
    attributed
        .into_iter()
        .filter_map(|(key, usage)| usage.map(|u| (key, u)))
        .collect()
}

fn parent_prompt<'a>(
    message: &'a UsageMessage,
    messages: &'a HashMap<String, UsageMessage>,
) -> Option<&'a UsageMessage> {
    let mut current = message;
    let mut visited = HashSet::new();
    while visited.insert(current.id.as_str()) {
        if current.role == "user" {
            return Some(current);
        }
        current = messages.get(current.parent.as_deref()?)?;
    }
    // Broken/cyclic ancestry is missing evidence, not permission to charge the
    // nearest prompt (which could belong to another conversation branch).
    None
}

fn parse_token_usage(value: &serde_json::Value, source: &str) -> Option<TokenUsage> {
    let obj = value.as_object()?;
    let get = |key: &str| match obj.get(key) {
        None => Some(0),
        Some(value) => value.as_u64(),
    };
    let (input, cache_read, cache_create) = match source {
        "codex" => {
            let cached = get("cached_input_tokens")?;
            // CodexTokenTotals::to_token_json preserves inclusive input even
            // after snapshot differencing. Emit exclusive input so cache reads
            // cannot be counted again when convergence sums token categories.
            (
                get("input_tokens")?.checked_sub(cached)?,
                cached,
                get("cache_write_input_tokens")?,
            )
        }
        // Claude stores native message.usage: input already excludes cache reads
        // and writes. Subtracting them here would under-report ordinary input.
        "claude" => (
            get("input_tokens")?,
            get("cache_read_input_tokens")?,
            get("cache_creation_input_tokens")?,
        ),
        _ => return None,
    };
    let usage = TokenUsage {
        input,
        cache_read,
        cache_create,
        output: get("output_tokens")?,
        reasoning: get("reasoning_output_tokens")?,
    };
    (usage != TokenUsage::default()).then_some(usage)
}

fn add_usage(a: &TokenUsage, b: &TokenUsage) -> Option<TokenUsage> {
    Some(TokenUsage {
        input: a.input.checked_add(b.input)?,
        output: a.output.checked_add(b.output)?,
        reasoning: a.reasoning.checked_add(b.reasoning)?,
        cache_read: a.cache_read.checked_add(b.cache_read)?,
        cache_create: a.cache_create.checked_add(b.cache_create)?,
    })
}

/// The git branch a session was working on, cached per `(source, session_id)`.
///
/// Read from the local `sessions` row rather than from the working tree: the branch that
/// matters is the one checked out when the prompt was typed, and the tree has almost
/// certainly moved on by the time a push runs.
fn session_branch_for_entry(
    conn: &Connection,
    entry: &HistoryEntry,
    branches: &mut HashMap<String, Option<String>>,
) -> Option<String> {
    let sid = entry.session_id.as_deref()?;
    let key = format!("{}\u{1}{sid}", entry.source);
    branches
        .entry(key)
        .or_insert_with(|| {
            conn.query_row(
                "SELECT git_branch FROM sessions WHERE source = ?1 AND session_id = ?2",
                rusqlite::params![&entry.source, sid],
                |r| r.get::<_, Option<String>>(0),
            )
            .ok()
            .flatten()
        })
        .clone()
}

fn git_remote_for_entry(
    conn: &Connection,
    entry: &HistoryEntry,
    remotes: &mut HashMap<String, Option<String>>,
) -> Option<String> {
    // Only an EXPLICIT, non-path project makes the remote unnecessary. When the entry
    // carries its cwd — the common case, and the one the fragmentation comes from —
    // the remote is exactly what we need, so returning None here made
    // `resolve_project_id`'s remote preference unreachable on the publishing path and
    // the cwd basename went out unchanged.
    if entry
        .project
        .as_deref()
        .map(str::trim)
        .is_some_and(|s| !s.is_empty() && !crate::convergence::is_filesystem_path(s))
    {
        return None;
    }
    let sid = entry.session_id.as_deref()?;
    let cwd = session_cwd(conn, &entry.source, sid)?;
    remotes
        .entry(cwd.clone())
        .or_insert_with(|| git_origin_url(&cwd))
        .clone()
}

fn session_cwd(conn: &Connection, source: &str, session_id: &str) -> Option<String> {
    conn.query_row(
        "SELECT cwd FROM sessions WHERE source = ?1 AND session_id = ?2",
        rusqlite::params![source, session_id],
        |r| r.get::<_, Option<String>>(0),
    )
    .ok()
    .flatten()
    .map(|s| s.trim().to_string())
    .filter(|s| !s.is_empty())
}

fn git_origin_url(cwd: &str) -> Option<String> {
    if !Path::new(cwd).is_dir() {
        return None;
    }
    let out = std::process::Command::new("git")
        .args(["-C", cwd, "remote", "get-url", "origin"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let url = String::from_utf8_lossy(&out.stdout).trim().to_string();
    (!url.is_empty()).then_some(url)
}

fn session_project_id(
    conn: &Connection,
    source: &str,
    session_id: &str,
    repo: Option<&str>,
    remotes: &mut HashMap<String, Option<String>>,
) -> String {
    let project: Option<String> = conn
        .query_row(
            "SELECT project FROM history WHERE source = ?1 AND session_id = ?2 \
             AND project IS NOT NULL AND trim(project) != '' LIMIT 1",
            rusqlite::params![source, session_id],
            |r| r.get(0),
        )
        .ok();
    let remote = session_cwd(conn, source, session_id).and_then(|cwd| {
        remotes
            .entry(cwd.clone())
            .or_insert_with(|| git_origin_url(&cwd))
            .clone()
    });
    let resolved = resolve_project_id(project.as_deref(), remote.as_deref());
    if resolved != UNKNOWN_PROJECT {
        resolved
    } else {
        resolve_project_id(repo, None)
    }
}

fn session_files(
    conn: &Connection,
    cache: &mut HashMap<(String, String), Vec<String>>,
    source: Option<&str>,
    session_id: &str,
) -> Result<Vec<String>> {
    let key = (source.unwrap_or("*").to_string(), session_id.to_string());
    if let Some(hit) = cache.get(&key) {
        return Ok(hit.clone());
    }
    let edits = match source {
        Some(source) => collect_session_file_edits(conn, source, session_id)?,
        None => session_file_edits(conn, session_id, None)?,
    };
    let mut deduped = Vec::new();
    for edit in edits {
        let trimmed = edit.file_path.trim();
        if !trimmed.is_empty() && !deduped.iter().any(|existing| existing == trimmed) {
            deduped.push(trimmed.to_string());
        }
    }
    cache.insert(key, deduped.clone());
    Ok(deduped)
}

/// Page through [`session_file_edits_page`] until the session is exhausted.
fn collect_session_file_edits(
    conn: &Connection,
    source: &str,
    session_id: &str,
) -> Result<Vec<SessionFileEdit>> {
    let mut all = Vec::new();
    let mut after: Option<SessionEvidenceCursor> = None;
    loop {
        let page = session_file_edits_page(conn, source, session_id, 1_000, after.as_ref())?;
        let next = page.next_cursor;
        all.extend(page.file_edits);
        match next {
            Some(cursor) => after = Some(cursor),
            None => break,
        }
    }
    Ok(all)
}

fn trajectory_files(
    conn: &Connection,
    cache: &mut HashMap<(String, String), Vec<String>>,
    trajectory_id: &str,
    path: Option<&str>,
) -> Result<Vec<String>> {
    let mut files = session_files(conn, cache, None, trajectory_id)?;
    if let Some(origin) = path.and_then(learn_origin_session) {
        for file in session_files(conn, cache, None, origin)? {
            if !files.iter().any(|existing| existing == &file) {
                files.push(file);
            }
        }
    }
    Ok(files
        .into_iter()
        .map(|f| normalize_home_path(f.trim()))
        .filter(|f| !f.is_empty())
        .collect())
}

fn learn_origin_session(path: &str) -> Option<&str> {
    let rest = path.strip_prefix("learn://")?;
    rest.split_once('/')
        .map(|(_, session)| session)
        .filter(|s| !s.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{init_db, insert_history};

    fn mem() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        conn
    }

    fn add_history(conn: &Connection, session: &str, prompt: &str, ts: i64) {
        add_history_project(conn, session, None, prompt, ts);
    }

    fn add_history_project(
        conn: &Connection,
        session: &str,
        project: Option<&str>,
        prompt: &str,
        ts: i64,
    ) {
        insert_history(
            conn,
            &HistoryEntry {
                id: 0,
                source: "claude".into(),
                session_id: Some(session.into()),
                project: project.map(str::to_string),
                prompt: prompt.into(),
                prompt_hash: Some(crate::prompt_hash(prompt)),
                timestamp_ms: ts,
            },
        )
        .unwrap();
    }

    #[allow(clippy::too_many_arguments)]
    fn usage_event(
        conn: &Connection,
        source: &str,
        id: &str,
        parent: Option<&str>,
        ts: i64,
        role: &str,
        text: &str,
        tokens: Option<serde_json::Value>,
    ) {
        let row: i64 = conn
            .query_row("SELECT COUNT(*) FROM session_events", [], |r| r.get(0))
            .unwrap();
        conn.execute(
            "INSERT INTO session_events (source, session_id, message_id, parent_id, ts_ms, role, kind, text, token_json, event_uid) \
             VALUES (?1, 'usage-session', ?2, ?3, ?4, ?5, 'text', ?6, ?7, ?8)",
            rusqlite::params![source, id, parent, ts, role, text, tokens.map(|t| t.to_string()), format!("event-{row}")],
        ).unwrap();
    }

    fn usage_prompt(conn: &Connection, source: &str, id: &str, ts: i64, prompt: &str) {
        insert_history(
            conn,
            &HistoryEntry {
                id: 0,
                source: source.into(),
                session_id: Some("usage-session".into()),
                project: None,
                prompt: prompt.into(),
                prompt_hash: Some(crate::prompt_hash(prompt)),
                timestamp_ms: ts,
            },
        )
        .unwrap();
        usage_event(conn, source, id, None, ts, "user", prompt, None);
    }

    #[test]
    fn prompt_usage_sums_session_deltas_once_even_across_batches() {
        let conn = mem();
        for (id, ts, prompt) in [
            ("u1", 100, "first"),
            ("u2", 300, "second"),
            ("u3", 500, "third"),
        ] {
            usage_prompt(&conn, "codex", id, ts, prompt);
        }
        for (id, ts, input, cached, output) in [
            ("a1", 150, 1000, 400, 120),
            ("a2", 200, 600, 500, 60),
            ("a3", 350, 200, 50, 20),
            ("a4", 550, 400, 100, 40),
        ] {
            usage_event(
                &conn,
                "codex",
                id,
                None,
                ts,
                "assistant",
                "answer",
                Some(serde_json::json!({
                    "input_tokens": input, "cached_input_tokens": cached, "output_tokens": output,
                    "reasoning_output_tokens": 10, "cache_write_input_tokens": 5, "total_tokens": input + output,
                })),
            );
        }
        let batch =
            build_outbox_batch(&conn, &SyncCursor::default(), 100, &HashSet::new()).unwrap();
        let usages: Vec<_> = batch
            .records
            .iter()
            .map(|r| r.usage.as_ref().unwrap())
            .collect();
        assert_eq!(
            usages.iter().map(|u| u.input).collect::<Vec<_>>(),
            vec![700, 150, 300]
        );
        assert_eq!(
            usages.iter().map(|u| u.output).collect::<Vec<_>>(),
            vec![180, 20, 40]
        );
        assert_eq!(
            usages.iter().map(|u| u.input + u.cache_read).sum::<u64>(),
            2200
        );
        assert_eq!(usages.iter().map(|u| u.output).sum::<u64>(), 240);
        assert_eq!(usages.iter().map(|u| u.reasoning).sum::<u64>(), 40);
        assert_eq!(usages.iter().map(|u| u.cache_create).sum::<u64>(), 20);
        let wire = serde_json::to_value(&batch.records[0]).unwrap();
        assert_eq!(
            wire["usage"],
            serde_json::json!({"input":700,"output":180,"reasoning":20,"cacheRead":900,"cacheCreate":10})
        );
        assert!(!wire.to_string().contains("costUsdMicros"));

        let mut cursor = SyncCursor::default();
        let mut paged = Vec::new();
        for _ in 0..3 {
            let page = build_outbox_batch(&conn, &cursor, 1, &HashSet::new()).unwrap();
            cursor = page.cursor;
            paged.extend(page.records);
        }
        assert_eq!(paged, batch.records);
        let old_cursor = SyncCursor {
            capture_version: 1,
            ..batch.cursor
        };
        let backfill = build_outbox_batch(&conn, &old_cursor, 100, &HashSet::new()).unwrap();
        assert_eq!(backfill.records, batch.records);
    }

    #[test]
    fn claude_usage_follows_parents_and_counts_content_blocks_once() {
        let conn = mem();
        usage_prompt(&conn, "claude", "u1", 100, "first\nsecond block");
        conn.execute(
            "UPDATE session_events SET text = 'first' WHERE message_id = 'u1'",
            [],
        )
        .unwrap();
        usage_event(
            &conn,
            "claude",
            "u1",
            None,
            100,
            "user",
            "second block",
            None,
        );
        usage_prompt(&conn, "claude", "u2", 300, "next prompt");
        let tokens = serde_json::json!({"input_tokens":10,"cache_read_input_tokens":20,"cache_creation_input_tokens":30,"output_tokens":40});
        for text in ["thinking", "answer", "tool call"] {
            usage_event(
                &conn,
                "claude",
                "a1",
                Some("u1"),
                150,
                "assistant",
                text,
                Some(tokens.clone()),
            );
        }
        usage_event(
            &conn,
            "claude",
            "tool1",
            Some("a1"),
            200,
            "tool_result",
            "result",
            None,
        );
        usage_event(
            &conn,
            "claude",
            "a2",
            Some("tool1"),
            250,
            "assistant",
            "continuation",
            Some(tokens.clone()),
        );
        // The branch still answers u1 even though u2 is now the nearest timestamp.
        usage_event(
            &conn,
            "claude",
            "a3",
            Some("a2"),
            350,
            "assistant",
            "branch answer",
            Some(tokens.clone()),
        );
        usage_event(
            &conn,
            "claude",
            "a4",
            Some("u2"),
            400,
            "assistant",
            "next answer",
            Some(tokens),
        );
        let batch =
            build_outbox_batch(&conn, &SyncCursor::default(), 100, &HashSet::new()).unwrap();
        assert_eq!(
            batch.records[0].usage,
            Some(TokenUsage {
                input: 30,
                output: 120,
                reasoning: 0,
                cache_read: 60,
                cache_create: 90
            })
        );
        assert_eq!(batch.records[1].usage.as_ref().unwrap().input, 10);
        add_file_edit(&conn, "usage-session", "src/main.rs", "Write");
        let refreshed = build_outbox_batch(&conn, &batch.cursor, 100, &HashSet::new()).unwrap();
        assert_eq!(refreshed.records.len(), 1);
        assert_eq!(refreshed.records[0].usage, batch.records[1].usage);
    }

    #[test]
    fn ambiguous_or_unlinked_usage_is_omitted() {
        let conn = mem();
        usage_prompt(&conn, "codex", "u1", 100, "first");
        usage_prompt(&conn, "codex", "u2", 300, "second");
        usage_prompt(&conn, "codex", "u3", 300, "tied");
        let tokens = Some(serde_json::json!({"input_tokens":100,"output_tokens":10}));
        for (id, parent, ts) in [
            ("before", None, 50),
            ("tie", None, 100),
            ("broken", Some("missing"), 150),
            ("after-tie", None, 350),
        ] {
            usage_event(
                &conn,
                "codex",
                id,
                parent,
                ts,
                "assistant",
                "answer",
                tokens.clone(),
            );
        }
        usage_event(
            &conn,
            "codex",
            "cycle1",
            Some("cycle2"),
            200,
            "assistant",
            "answer",
            tokens.clone(),
        );
        usage_event(
            &conn,
            "codex",
            "cycle2",
            Some("cycle1"),
            250,
            "assistant",
            "answer",
            tokens.clone(),
        );
        // Same session ID from another harness cannot donate tokens to Codex.
        usage_event(
            &conn,
            "claude",
            "other",
            Some("u1"),
            200,
            "assistant",
            "answer",
            tokens,
        );
        let batch =
            build_outbox_batch(&conn, &SyncCursor::default(), 100, &HashSet::new()).unwrap();
        assert_eq!(batch.records.len(), 3);
        for record in batch.records {
            assert!(record.usage.is_none());
            assert!(serde_json::to_value(record).unwrap().get("usage").is_none());
        }
    }

    #[test]
    fn unidentified_user_turn_blocks_timestamp_attribution() {
        let conn = mem();
        usage_prompt(&conn, "codex", "u1", 100, "first");
        usage_event(
            &conn,
            "codex",
            "unknown",
            None,
            200,
            "user",
            "unidentified",
            None,
        );
        conn.execute(
            "UPDATE session_events SET message_id = NULL WHERE message_id = 'unknown'",
            [],
        )
        .unwrap();
        usage_event(
            &conn,
            "codex",
            "a1",
            None,
            250,
            "assistant",
            "answer",
            Some(serde_json::json!({"input_tokens":100})),
        );
        let batch =
            build_outbox_batch(&conn, &SyncCursor::default(), 100, &HashSet::new()).unwrap();
        assert!(batch.records[0].usage.is_none());
        conn.execute(
            "UPDATE session_events SET ts_ms = 0 WHERE message_id IS NULL",
            [],
        )
        .unwrap();
        let undated =
            build_outbox_batch(&conn, &SyncCursor::default(), 100, &HashSet::new()).unwrap();
        assert!(undated.records[0].usage.is_none());
    }

    #[test]
    fn blank_session_ids_stay_unattributed_and_do_not_share_a_cache_entry() {
        // A blank-but-present session id used to reach the cache as `(source, "")`, so
        // every malformed row in the store collapsed onto one key and could be handed
        // usage produced by an unrelated prompt.
        let conn = mem();
        // Usage really does exist under the blank id, so a regression finds something to
        // attribute rather than returning None merely because the table was empty.
        usage_event(
            &conn,
            "codex",
            "blank-user",
            None,
            100,
            "user",
            "empty",
            None,
        );
        usage_event(
            &conn,
            "codex",
            "blank-answer",
            Some("blank-user"),
            150,
            "assistant",
            "answer",
            Some(serde_json::json!({"input_tokens": 999, "output_tokens": 999})),
        );

        let mut cache = HashMap::new();
        for (sid, prompt, ts) in [("", "empty", 100i64), ("   ", "spaces", 200)] {
            let entry = HistoryEntry {
                id: 0,
                source: "codex".into(),
                session_id: Some(sid.into()),
                project: None,
                prompt: prompt.into(),
                prompt_hash: Some(crate::prompt_hash(prompt)),
                timestamp_ms: ts,
            };
            assert_eq!(
                session_usage_for_entry(&conn, &entry, &mut cache).unwrap(),
                None,
                "a blank session id must stay unattributed ({prompt})"
            );
        }
        assert!(
            cache.is_empty(),
            "blank identities must never create a cache entry to share"
        );
    }

    #[test]
    fn a_padded_but_real_session_id_keeps_its_identity() {
        // The blank guard trims to TEST emptiness. If it also trimmed the value used for
        // lookup, a genuine id stored with surrounding whitespace would query a different
        // session id than the one its events are filed under, and quietly lose its usage.
        let conn = mem();
        let sid = " padded-session ";
        usage_event(&conn, "codex", "u1", None, 100, "user", "ask", None);
        usage_event(
            &conn,
            "codex",
            "a1",
            Some("u1"),
            150,
            "assistant",
            "answer",
            Some(serde_json::json!({"input_tokens": 42, "output_tokens": 7})),
        );
        conn.execute(
            "UPDATE session_events SET session_id = ?1",
            rusqlite::params![sid],
        )
        .unwrap();

        let entry = HistoryEntry {
            id: 0,
            source: "codex".into(),
            session_id: Some(sid.into()),
            project: None,
            prompt: "ask".into(),
            prompt_hash: Some(crate::prompt_hash("ask")),
            timestamp_ms: 100,
        };
        let usage = session_usage_for_entry(&conn, &entry, &mut HashMap::new()).unwrap();
        assert!(
            usage.is_some(),
            "a padded but non-blank session id must still resolve its own events"
        );
    }

    #[test]
    fn usage_cache_is_scoped_to_source_and_session() {
        let conn = mem();
        usage_prompt(&conn, "codex", "u1", 100, "first");
        usage_prompt(&conn, "claude", "u1", 100, "first");
        for (source, input) in [("codex", 100), ("claude", 200)] {
            usage_event(
                &conn,
                source,
                "a1",
                Some("u1"),
                200,
                "assistant",
                "answer",
                Some(serde_json::json!({"input_tokens":input})),
            );
        }
        let mut cache = HashMap::new();
        let entry = latest_history_for_session(&conn, "codex", "usage-session")
            .unwrap()
            .unwrap();
        assert_eq!(
            session_usage_for_entry(&conn, &entry, &mut cache)
                .unwrap()
                .unwrap()
                .input,
            100
        );
        conn.execute("DELETE FROM session_events WHERE source = 'codex'", [])
            .unwrap();
        assert_eq!(
            session_usage_for_entry(&conn, &entry, &mut cache)
                .unwrap()
                .unwrap()
                .input,
            100
        );
        let claude = latest_history_for_session(&conn, "claude", "usage-session")
            .unwrap()
            .unwrap();
        assert_eq!(
            session_usage_for_entry(&conn, &claude, &mut cache)
                .unwrap()
                .unwrap()
                .input,
            200
        );
        for unmatched in [
            HistoryEntry {
                prompt: " first ".into(),
                ..entry.clone()
            },
            HistoryEntry {
                timestamp_ms: 101,
                ..entry.clone()
            },
            HistoryEntry {
                session_id: None,
                ..entry.clone()
            },
        ] {
            assert!(session_usage_for_entry(&conn, &unmatched, &mut cache)
                .unwrap()
                .is_none());
        }
        let absent = HistoryEntry {
            session_id: Some("other-session".into()),
            ..entry
        };
        assert!(session_usage_for_entry(&conn, &absent, &mut cache)
            .unwrap()
            .is_none());
    }

    #[test]
    fn invalid_or_empty_tokens_are_not_reported() {
        for raw in [
            "null",
            "[]",
            "{}",
            "{\"total_tokens\":100}",
            "{\"input_tokens\":0}",
            "{\"input_tokens\":-1}",
            "{\"output_tokens\":1.5}",
            "{\"input_tokens\":\"10\"}",
            "{\"input_tokens\":10,\"cached_input_tokens\":20}",
        ] {
            assert!(
                parse_token_usage(&serde_json::from_str(raw).unwrap(), "codex").is_none(),
                "{raw}"
            );
        }
        let conn = mem();
        usage_prompt(&conn, "claude", "u1", 100, "first");
        usage_event(
            &conn,
            "claude",
            "a1",
            Some("u1"),
            200,
            "assistant",
            "answer",
            Some(serde_json::json!({"input_tokens":10})),
        );
        usage_event(
            &conn,
            "claude",
            "a1",
            Some("u1"),
            200,
            "assistant",
            "conflicting copy",
            Some(serde_json::json!({"input_tokens":20})),
        );
        usage_event(
            &conn,
            "claude",
            "bad",
            Some("u1"),
            300,
            "assistant",
            "bad JSON",
            None,
        );
        conn.execute(
            "UPDATE session_events SET token_json = '{' WHERE message_id = 'bad'",
            [],
        )
        .unwrap();
        let batch =
            build_outbox_batch(&conn, &SyncCursor::default(), 100, &HashSet::new()).unwrap();
        assert!(batch.records[0].usage.is_none());
        assert!(add_usage(
            &TokenUsage {
                input: u64::MAX,
                ..TokenUsage::default()
            },
            &TokenUsage {
                input: 1,
                ..TokenUsage::default()
            }
        )
        .is_none());
    }

    /// A path-valued `project` must NOT suppress the remote lookup.
    ///
    /// This is the test whose absence let the first version of the fix ship inert.
    /// `resolve_project_id` learned to prefer the remote, but `git_remote_for_entry`
    /// returned `None` for ANY non-empty `project` — and `project` is the cwd in the
    /// common case, so the publishing path never had a remote to prefer and kept
    /// emitting the cwd basename. Unit tests on `resolve_project_id` all passed.
    #[test]
    fn a_path_valued_project_still_fetches_the_git_remote() {
        let repo = tempfile::tempdir().unwrap();
        let cwd = repo.path().to_str().unwrap().to_string();
        for args in [
            vec!["init", "--quiet"],
            vec![
                "remote",
                "add",
                "origin",
                "git@github.com:AgentWorkforce/relayfile.git",
            ],
        ] {
            let ok = std::process::Command::new("git")
                .arg("-C")
                .arg(&cwd)
                .args(&args)
                .status()
                .map(|s| s.success())
                .unwrap_or(false);
            if !ok {
                return; // no usable git in this environment; nothing to assert
            }
        }

        let conn = mem();
        conn.execute(
            "INSERT INTO sessions (session_id, source, cwd, parser_version) VALUES ('s-path','claude',?1,1)",
            rusqlite::params![cwd],
        )
        .unwrap();

        let mut remotes = HashMap::new();
        let entry = HistoryEntry {
            id: 1,
            source: "claude".into(),
            session_id: Some("s-path".into()),
            // The cwd, exactly as the harness records it.
            project: Some(cwd.clone()),
            prompt: "hi".into(),
            prompt_hash: Some("h".into()),
            timestamp_ms: 0,
        };

        let remote = git_remote_for_entry(&conn, &entry, &mut remotes);
        assert!(
            remote.is_some(),
            "a path-valued project must still resolve the remote, or the cwd basename goes out unchanged"
        );
        assert_eq!(
            crate::convergence::resolve_project_id(entry.project.as_deref(), remote.as_deref()),
            "AgentWorkforce/relayfile"
        );
    }

    /// The other half: an explicit label is a deliberate choice, so the remote lookup
    /// stays skipped and the label survives.
    #[test]
    fn an_explicit_label_skips_the_remote_lookup() {
        let conn = mem();
        conn.execute(
            "INSERT INTO sessions (session_id, source, cwd, parser_version) VALUES ('s-label','claude','/tmp/whatever',1)",
            [],
        )
        .unwrap();

        let mut remotes = HashMap::new();
        let entry = HistoryEntry {
            id: 1,
            source: "claude".into(),
            session_id: Some("s-label".into()),
            project: Some("relayfile-demo-readiness-0901".into()),
            prompt: "hi".into(),
            prompt_hash: Some("h".into()),
            timestamp_ms: 0,
        };

        assert!(git_remote_for_entry(&conn, &entry, &mut remotes).is_none());
    }

    fn add_trajectory(conn: &Connection, id: &str, decisions: &str, retro: &str) {
        add_trajectory_at(conn, id, decisions, retro, 1, None);
    }

    fn add_trajectory_at(
        conn: &Connection,
        id: &str,
        decisions: &str,
        retro: &str,
        updated_ms: i64,
        path: Option<&str>,
    ) {
        add_trajectory_rowid(conn, None, id, decisions, retro, updated_ms, path);
    }

    fn add_trajectory_rowid(
        conn: &Connection,
        rowid: Option<i64>,
        id: &str,
        decisions: &str,
        retro: &str,
        updated_ms: i64,
        path: Option<&str>,
    ) {
        conn.execute(
            "INSERT INTO trajectories (rowid, id, version, persona_id, project_id, task_title, \
             task_description, status, decisions_json, retrospective_json, search_text, path, \
             updated_ms, timestamp_ms) VALUES (?, ?,1,?,?,?,?,?,?,?,?,?,?,?)",
            rusqlite::params![
                rowid,
                id,
                "planner",
                "proj",
                "Build forms",
                "desc",
                "completed",
                decisions,
                retro,
                "search",
                path,
                updated_ms,
                1_782_036_000_000i64
            ],
        )
        .unwrap();
    }

    fn add_file_edit(conn: &Connection, session: &str, path: &str, tool: &str) {
        conn.execute(
            "INSERT INTO file_edits (source, session_id, tool_use_id, file_path, tool_name) \
             VALUES ('claude', ?, ?, ?, ?)",
            rusqlite::params![session, format!("tool_{path}"), path, tool],
        )
        .unwrap();
    }

    fn add_commit_link(conn: &Connection, session: &str, sha: &str, method: &str) {
        add_commit_link_evidence(conn, session, sha, method, None);
    }

    fn add_commit_link_evidence(
        conn: &Connection,
        session: &str,
        sha: &str,
        method: &str,
        evidence_json: Option<&str>,
    ) {
        conn.execute(
            "INSERT INTO session_commit_links \
             (source, session_id, repo, branch, commit_sha, match_method, confidence, \
              files_json, numstat_json, evidence_json, created_at_ms) \
             VALUES ('claude', ?, '/Users/khaliqgant/Projects/relayhistory', 'main', ?, ?, 0.9, \
                     ?, ?, ?, 1_782_036_000_000)",
            rusqlite::params![
                session,
                sha,
                method,
                r#"["src/lib.rs"]"#,
                r#"[{"path":"src/lib.rs","additions":2,"deletions":0}]"#,
                evidence_json,
            ],
        )
        .unwrap();
    }

    fn init_git_repo(origin: &str) -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        let status = std::process::Command::new("git")
            .args(["init"])
            .current_dir(dir.path())
            .output()
            .unwrap();
        assert!(status.status.success(), "git init failed");
        let add = std::process::Command::new("git")
            .args(["remote", "add", "origin", origin])
            .current_dir(dir.path())
            .output()
            .unwrap();
        assert!(add.status.success(), "git remote add failed");
        dir
    }

    #[test]
    fn builds_batch_and_advances_cursor() {
        let conn = mem();
        add_history(&conn, "s1", "first prompt", 1);
        add_history(&conn, "s1", "second prompt", 2);
        let none = HashSet::new();

        let batch = build_outbox_batch(&conn, &SyncCursor::default(), 100, &none).unwrap();
        assert_eq!(batch.records.len(), 2);
        assert_eq!(batch.cursor.history_id, 2);
        assert!(batch.records.iter().all(|r| r.kind == "prompt"));

        // a second call from the advanced cursor yields nothing new
        let empty = build_outbox_batch(&conn, &batch.cursor, 100, &none).unwrap();
        assert!(empty.records.is_empty());
        assert_eq!(empty.cursor, batch.cursor);
    }

    #[test]
    fn incognito_sessions_are_excluded_but_advance_cursor() {
        let conn = mem();
        add_history(&conn, "public", "keep me", 1);
        add_history(&conn, "secret", "drop me", 2);
        let incognito: HashSet<String> = ["secret".to_string()].into_iter().collect();

        let batch = build_outbox_batch(&conn, &SyncCursor::default(), 100, &incognito).unwrap();
        assert_eq!(batch.records.len(), 1);
        assert_eq!(batch.records[0].content, "keep me");
        // cursor still advances past the skipped incognito row (id 2) — never re-scanned
        assert_eq!(batch.cursor.history_id, 2);
    }

    #[test]
    fn trajectories_fan_out_into_batch() {
        let conn = mem();
        add_trajectory(
            &conn,
            "traj-1",
            r#"[{"chosen":"Formik"}]"#,
            r#"{"summary":"shipped","learnings":["L0"],"confidence":0.8}"#,
        );
        let none = HashSet::new();
        let batch = build_outbox_batch(&conn, &SyncCursor::default(), 100, &none).unwrap();
        // decision + summary + learning = 3 events, all trajectory lens
        assert_eq!(batch.records.len(), 3);
        assert!(batch
            .records
            .iter()
            .all(|r| r.lens.as_deref() == Some("trajectories")));
        assert!(batch
            .records
            .iter()
            .any(|r| r.event_id == "decision:traj-1:0"));
        assert!(batch
            .records
            .iter()
            .any(|r| r.event_id == "finding:traj-1:learning:0"));
        assert_eq!(batch.cursor.trajectory_rowid, 1);
    }

    #[test]
    fn incognito_excludes_trajectory_by_id() {
        let conn = mem();
        add_trajectory(&conn, "secret-traj", "[]", r#"{"summary":"hidden"}"#);
        let incognito: HashSet<String> = ["secret-traj".to_string()].into_iter().collect();
        let batch = build_outbox_batch(&conn, &SyncCursor::default(), 100, &incognito).unwrap();
        assert!(batch.records.is_empty());
        assert_eq!(batch.cursor.trajectory_rowid, 1); // still advanced
    }

    #[test]
    fn limit_caps_rows_per_source() {
        let conn = mem();
        for i in 1..=5 {
            add_history(&conn, "s", &format!("p{i}"), i);
        }
        let none = HashSet::new();
        let batch = build_outbox_batch(&conn, &SyncCursor::default(), 2, &none).unwrap();
        assert_eq!(batch.records.len(), 2);
        assert_eq!(batch.cursor.history_id, 2);
    }

    #[test]
    fn cursor_round_trips_through_json() {
        let c = SyncCursor {
            history_id: 7,
            trajectory_rowid: 3,
            trajectory_updated_ms: 99,
            commit_link_id: 4,
            ..Default::default()
        };
        let s = serde_json::to_string(&c).unwrap();
        assert_eq!(serde_json::from_str::<SyncCursor>(&s).unwrap(), c);
        let old: SyncCursor =
            serde_json::from_str(r#"{"history_id":7,"trajectory_rowid":3}"#).unwrap();
        assert_eq!(old.trajectory_updated_ms, 0);
        assert_eq!(old.commit_link_id, 0);
        assert_eq!(old.file_edit_id, 0);
        assert_eq!(old.capture_version, 0);
    }

    #[test]
    fn outbox_batch_project_id_is_never_null() {
        let conn = mem();
        add_history_project(
            &conn,
            "s-path",
            Some("/Users/khaliqgant/Projects/relayhistory"),
            "path project prompt",
            1,
        );
        add_history(&conn, "s-unknown", "no project", 2);
        add_trajectory(
            &conn,
            "traj-1",
            r#"[{"chosen":"Formik"}]"#,
            r#"{"summary":"shipped"}"#,
        );
        add_commit_link(&conn, "s-path", "abc111", "cwd");
        let none = HashSet::new();
        let batch = build_outbox_batch(&conn, &SyncCursor::default(), 100, &none).unwrap();
        assert!(!batch.records.is_empty());
        assert!(batch.records.iter().all(|r| r.project_id.is_some()));
        assert!(batch
            .records
            .iter()
            .all(|r| r.project_id.as_deref() != Some("")));
        let path_prompt = batch
            .records
            .iter()
            .find(|r| r.kind == "prompt" && r.session_id == "s-path")
            .unwrap();
        assert_eq!(path_prompt.project_id.as_deref(), Some("relayhistory"));
        let unknown_prompt = batch
            .records
            .iter()
            .find(|r| r.kind == "prompt" && r.session_id == "s-unknown")
            .unwrap();
        assert_eq!(unknown_prompt.project_id.as_deref(), Some(UNKNOWN_PROJECT));
        assert!(batch.records.iter().any(|r| r.kind == "session_outcome"));
    }

    #[test]
    fn git_remote_of_session_cwd_becomes_repo_slug() {
        let repo = init_git_repo("git@github.com:AgentWorkforce/relayhistory.git");
        let conn = mem();
        conn.execute(
            "INSERT INTO sessions (session_id, source, cwd, parser_version) VALUES (?, 'claude', ?, 1)",
            rusqlite::params!["s-git", repo.path().display().to_string()],
        )
        .unwrap();
        add_history(&conn, "s-git", "work in the cloned repo", 1);
        let none = HashSet::new();
        let batch = build_outbox_batch(&conn, &SyncCursor::default(), 100, &none).unwrap();
        assert_eq!(batch.records.len(), 1);
        assert_eq!(batch.records[0].kind, "prompt");
        assert_eq!(
            batch.records[0].project_id.as_deref(),
            Some("AgentWorkforce/relayhistory")
        );
        let v = serde_json::to_value(&batch.records[0]).unwrap();
        assert_eq!(v["projectId"], "AgentWorkforce/relayhistory");
        assert!(!v["projectId"].is_null());
    }

    #[test]
    fn files_touched_on_session_and_trajectory_envelopes() {
        let conn = mem();
        add_history_project(
            &conn,
            "s1",
            Some("/Users/me/Projects/relayhistory"),
            "edit the mapper",
            1,
        );
        add_file_edit(
            &conn,
            "s1",
            "/Users/me/Projects/relayhistory/src/outbox.rs",
            "Edit",
        );
        add_file_edit(
            &conn,
            "s1",
            "crates/ai-hist-core/src/convergence.rs",
            "Write",
        );
        add_trajectory_at(
            &conn,
            "learn_abc",
            r#"[{"chosen":"slug the project"}]"#,
            r#"{"summary":"shipped"}"#,
            5,
            Some("learn://claude/s1"),
        );
        let none = HashSet::new();
        let batch = build_outbox_batch(&conn, &SyncCursor::default(), 100, &none).unwrap();
        let prompts: Vec<_> = batch
            .records
            .iter()
            .filter(|r| r.kind == "prompt")
            .collect();
        assert_eq!(prompts.len(), 1);
        assert!(prompts[0]
            .files_touched
            .iter()
            .any(|f| f.ends_with("src/outbox.rs")));
        assert!(prompts[0]
            .files_touched
            .iter()
            .any(|f| f.ends_with("crates/ai-hist-core/src/convergence.rs")));
        let traj: Vec<_> = batch
            .records
            .iter()
            .filter(|r| r.lens.as_deref() == Some("trajectories"))
            .collect();
        assert!(!traj.is_empty());
        assert!(traj
            .iter()
            .all(|r| r.files_touched == prompts[0].files_touched));
        let v = serde_json::to_value(prompts[0]).unwrap();
        assert!(v["filesTouched"].as_array().unwrap().len() >= 2);
    }

    #[test]
    fn revised_trajectory_is_re_pushed_via_updated_ms_watermark() {
        let conn = mem();
        add_trajectory(
            &conn,
            "traj-1",
            r#"[{"chosen":"Formik"}]"#,
            r#"{"summary":"first draft"}"#,
        );
        let none = HashSet::new();
        let first = build_outbox_batch(&conn, &SyncCursor::default(), 100, &none).unwrap();
        assert!(first
            .records
            .iter()
            .any(|r| r.content.contains("first draft")));
        assert!(first.cursor.trajectory_rowid >= 1);
        assert!(first.cursor.trajectory_updated_ms >= 1);

        let empty = build_outbox_batch(&conn, &first.cursor, 100, &none).unwrap();
        assert!(empty
            .records
            .iter()
            .all(|r| r.lens.as_deref() != Some("trajectories")));

        conn.execute(
            "UPDATE trajectories SET retrospective_json = ?, updated_ms = ? WHERE id = ?",
            rusqlite::params![r#"{"summary":"amended after the run"}"#, 50i64, "traj-1"],
        )
        .unwrap();
        let second = build_outbox_batch(&conn, &first.cursor, 100, &none).unwrap();
        assert!(
            second
                .records
                .iter()
                .any(|r| r.kind == "reflection" && r.content.contains("amended after the run")),
            "revised trajectory must appear in the next batch: {:?}",
            second
                .records
                .iter()
                .map(|r| (r.kind.as_str(), r.content.as_str()))
                .collect::<Vec<_>>()
        );
        assert!(second.cursor.trajectory_updated_ms >= 50);
    }

    #[test]
    fn n_linked_commits_yield_n_session_outcome_envelopes() {
        let conn = mem();
        add_history_project(
            &conn,
            "s-n",
            Some("/Users/me/Projects/relayhistory"),
            "land the fix",
            1,
        );
        for i in 1..=3 {
            add_commit_link(&conn, "s-n", &format!("sha{i:03}"), "cwd+branch");
        }
        let none = HashSet::new();
        let batch = build_outbox_batch(&conn, &SyncCursor::default(), 100, &none).unwrap();
        let outcomes: Vec<_> = batch
            .records
            .iter()
            .filter(|r| r.kind == "session_outcome")
            .collect();
        assert_eq!(outcomes.len(), 3);
        let shas: Vec<_> = outcomes
            .iter()
            .map(|r| r.commit_sha.clone().unwrap())
            .collect();
        assert_eq!(shas, vec!["sha001", "sha002", "sha003"]);
        assert!(outcomes
            .iter()
            .all(|r| r.match_method.as_deref() == Some("cwd+branch")));
        assert!(outcomes
            .iter()
            .all(|r| r.project_id.as_deref() == Some("relayhistory")));
        assert!(outcomes.iter().all(|r| r.confidence == Some(0.9)));
        let v = serde_json::to_value(outcomes[0]).unwrap();
        assert_eq!(v["kind"], "session_outcome");
        assert_eq!(v["commitSha"], "sha001");
        assert_eq!(v["matchMethod"], "cwd+branch");
        assert_eq!(v["numstat"]["files"][0]["path"], "src/lib.rs");
        assert_eq!(v["files"][0], "src/lib.rs");
        assert_eq!(batch.cursor.commit_link_id, 3);

        let empty = build_outbox_batch(&conn, &batch.cursor, 100, &none).unwrap();
        assert!(!empty.records.iter().any(|r| r.kind == "session_outcome"));
    }

    #[test]
    fn upgraded_cursor_replays_previously_synced_prompts() {
        let conn = mem();
        add_history_project(
            &conn,
            "s-old",
            Some("/Users/me/Projects/relayhistory"),
            "already pushed without projectId",
            1,
        );
        let old: SyncCursor =
            serde_json::from_str(r#"{"history_id":1,"trajectory_rowid":0}"#).unwrap();
        assert_eq!(old.capture_version, 0);
        assert_eq!(old.history_id, 1);
        let none = HashSet::new();
        let batch = build_outbox_batch(&conn, &old, 100, &none).unwrap();
        let prompt = batch
            .records
            .iter()
            .find(|r| r.kind == "prompt")
            .expect("historical prompt must be re-upserted once");
        assert_eq!(prompt.content, "already pushed without projectId");
        assert_eq!(prompt.project_id.as_deref(), Some("relayhistory"));
        assert_eq!(batch.cursor.capture_version, SyncCursor::CAPTURE_VERSION);
        assert_eq!(batch.cursor.history_id, 1);

        let empty = build_outbox_batch(&conn, &batch.cursor, 100, &none).unwrap();
        assert!(!empty.records.iter().any(|r| r.kind == "prompt"));
    }

    #[test]
    fn capture_version_upgrade_rewinds_history_in_merge_max() {
        let old = SyncCursor {
            history_id: 99,
            capture_version: 0,
            ..Default::default()
        };
        let upgraded = old.migrated();
        assert_eq!(upgraded.history_id, 0);
        assert_eq!(upgraded.capture_version, SyncCursor::CAPTURE_VERSION);
        let merged = old.merge_max(&upgraded);
        assert_eq!(merged.history_id, 0);
        assert_eq!(merged.capture_version, SyncCursor::CAPTURE_VERSION);
        let stale = upgraded.merge_max(&old);
        assert_eq!(stale, upgraded);
    }

    #[test]
    fn equal_timestamp_keyset_does_not_skip_rows() {
        // Reviewer example: (rowid, updated_ms) = (3,1), (1,2), (2,2) with LIMIT 2.
        // Independent max(rowid)/max(updated_ms) advances to (3, 2) and permanently
        // drops (2, 2). Keyset continuation from the last emitted pair does not.
        let conn = mem();
        add_trajectory_rowid(
            &conn,
            Some(3),
            "t3",
            "[]",
            r#"{"summary":"rowid-3"}"#,
            1,
            None,
        );
        add_trajectory_rowid(
            &conn,
            Some(1),
            "t1",
            "[]",
            r#"{"summary":"rowid-1"}"#,
            2,
            None,
        );
        add_trajectory_rowid(
            &conn,
            Some(2),
            "t2",
            "[]",
            r#"{"summary":"rowid-2"}"#,
            2,
            None,
        );
        let none = HashSet::new();
        let first = build_outbox_batch(&conn, &SyncCursor::default(), 2, &none).unwrap();
        let first_ids: Vec<_> = first
            .records
            .iter()
            .filter(|r| r.kind == "reflection")
            .map(|r| r.session_id.as_str())
            .collect();
        assert_eq!(first_ids, vec!["t3", "t1"]);
        assert_eq!(first.cursor.trajectory_updated_ms, 2);
        assert_eq!(first.cursor.trajectory_rowid, 1);

        let second = build_outbox_batch(&conn, &first.cursor, 2, &none).unwrap();
        let second_ids: Vec<_> = second
            .records
            .iter()
            .filter(|r| r.kind == "reflection")
            .map(|r| r.session_id.as_str())
            .collect();
        assert_eq!(
            second_ids,
            vec!["t2"],
            "equal-timestamp row after LIMIT split must appear in the next batch"
        );
        assert_eq!(second.cursor.trajectory_updated_ms, 2);
        assert_eq!(second.cursor.trajectory_rowid, 2);
    }

    #[test]
    fn legacy_zero_updated_ms_watermark_replays_amended_trajectory() {
        // Old client pushed rowid 1 with no updated_ms field (deserializes as 0).
        // The row was then amended. Seeding from MAX(updated_ms) would set the
        // watermark to 50 and permanently exclude it; keep the stored 0.
        let conn = mem();
        add_trajectory_at(
            &conn,
            "traj-1",
            "[]",
            r#"{"summary":"amended after old push"}"#,
            50,
            None,
        );
        let old = SyncCursor {
            trajectory_rowid: 1,
            trajectory_updated_ms: 0,
            ..Default::default()
        };
        let none = HashSet::new();
        let batch = build_outbox_batch(&conn, &old, 100, &none).unwrap();
        assert!(
            batch
                .records
                .iter()
                .any(|r| r.content.contains("amended after old push")),
            "amended trajectory must replay from a zero updated_ms watermark: {:?}",
            batch
                .records
                .iter()
                .map(|r| r.content.as_str())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn file_edits_after_last_prompt_republish_files_touched() {
        let conn = mem();
        add_history_project(
            &conn,
            "s-late",
            Some("/Users/me/Projects/relayhistory"),
            "start the session",
            1,
        );
        let none = HashSet::new();
        let first = build_outbox_batch(&conn, &SyncCursor::default(), 100, &none).unwrap();
        let prompt = first.records.iter().find(|r| r.kind == "prompt").unwrap();
        assert!(prompt.files_touched.is_empty());

        add_file_edit(
            &conn,
            "s-late",
            "/Users/me/Projects/relayhistory/src/outbox.rs",
            "Edit",
        );
        let second = build_outbox_batch(&conn, &first.cursor, 100, &none).unwrap();
        let replayed: Vec<_> = second
            .records
            .iter()
            .filter(|r| r.kind == "prompt")
            .collect();
        assert_eq!(replayed.len(), 1);
        assert_eq!(replayed[0].content, "start the session");
        assert!(replayed[0]
            .files_touched
            .iter()
            .any(|f| f.ends_with("src/outbox.rs")));
        assert!(second.cursor.file_edit_id > first.cursor.file_edit_id);

        let third = build_outbox_batch(&conn, &second.cursor, 100, &none).unwrap();
        assert!(!third.records.iter().any(|r| r.kind == "prompt"));
    }

    #[test]
    fn new_prompt_with_new_file_edits_does_not_double_emit() {
        let conn = mem();
        add_history(&conn, "s1", "first", 1);
        let none = HashSet::new();
        let first = build_outbox_batch(&conn, &SyncCursor::default(), 100, &none).unwrap();
        add_file_edit(&conn, "s1", "src/a.rs", "Edit");
        add_history(&conn, "s1", "second", 2);
        let second = build_outbox_batch(&conn, &first.cursor, 100, &none).unwrap();
        let prompts: Vec<_> = second
            .records
            .iter()
            .filter(|r| r.kind == "prompt")
            .collect();
        assert_eq!(prompts.len(), 1);
        assert_eq!(prompts[0].content, "second");
        assert!(prompts[0]
            .files_touched
            .iter()
            .any(|f| f.ends_with("src/a.rs")));
    }

    #[test]
    fn session_outcome_shipped_at_comes_from_evidence_commit_time() {
        let conn = mem();
        add_history_project(
            &conn,
            "s-ship",
            Some("/Users/me/Projects/relayhistory"),
            "land it",
            1,
        );
        add_commit_link_evidence(
            &conn,
            "s-ship",
            "deadbeef",
            "cwd",
            Some(r#"{"commit_time_ms":1000}"#),
        );
        let none = HashSet::new();
        let batch = build_outbox_batch(&conn, &SyncCursor::default(), 100, &none).unwrap();
        let outcome = batch
            .records
            .iter()
            .find(|r| r.kind == "session_outcome")
            .unwrap();
        assert_eq!(
            outcome.shipped_at.as_deref(),
            Some("1970-01-01T00:00:01.000Z")
        );
        assert_eq!(outcome.ts, "2026-06-21T10:00:00.000Z");
    }

    #[test]
    fn session_outcome_event_ids_differ_when_match_method_differs() {
        let conn = mem();
        add_history_project(
            &conn,
            "s-dup",
            Some("/Users/me/Projects/relayhistory"),
            "same commit two ways",
            1,
        );
        add_commit_link(&conn, "s-dup", "abc123def", "cwd");
        add_commit_link(&conn, "s-dup", "abc123def", "cwd+branch");
        let none = HashSet::new();
        let batch = build_outbox_batch(&conn, &SyncCursor::default(), 100, &none).unwrap();
        let ids: Vec<_> = batch
            .records
            .iter()
            .filter(|r| r.kind == "session_outcome")
            .map(|r| r.event_id.as_str())
            .collect();
        assert_eq!(
            ids,
            vec![
                "session_outcome:claude:s-dup:abc123def:cwd",
                "session_outcome:claude:s-dup:abc123def:cwd+branch",
            ]
        );
    }
}

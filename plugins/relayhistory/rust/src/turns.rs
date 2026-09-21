//! Conversation turns: the readable transcript half of cloud sync.
//!
//! The convergence outbox ([`crate::outbox`]) publishes *prompts* — what the human typed
//! — plus distilled trajectory records. It has never carried the other side of the
//! conversation. Measured on one machine on 2026-09-03, the cloud held 337,139 prompts
//! while these were local-only:
//!
//! ```text
//! assistant text      190,165   the answers
//! assistant tool_use  350,551   what the agent actually did
//! tool_result         338,614
//! assistant thinking    4,823   the reasoning
//! ```
//!
//! So "replay this session" returned only what the user typed. This module builds the
//! batches that `POST /v1/sessions/:sessionId/turns` needs to close that, from the local
//! `session_events` table the `ai-hist events` replay already reads.
//!
//! Network I/O lives in the binding layer, per the no-async-in-core rule; this module is
//! sync rusqlite reads and is unit-testable without a server.

use anyhow::Result;
use rusqlite::Connection;
use serde::Serialize;
use std::collections::HashSet;

/// The server caps a turns request at 1,000. Chunk below it rather than at it, so a
/// future server-side reduction does not silently start rejecting whole sessions.
pub const MAX_TURNS_PER_REQUEST: usize = 500;

/// Sessions published per push run. A push already carries the convergence batch; this
/// bounds the extra work so a machine with a large backlog drains steadily instead of
/// stalling one run for hours.
pub const DEFAULT_SESSION_BUDGET: usize = 12;

/// One turn in the wire shape `POST /v1/sessions/:sessionId/turns` accepts.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ConversationTurn {
    #[serde(rename = "sessionOwner")]
    pub session_owner: String,
    #[serde(rename = "turnIndex")]
    pub turn_index: i64,
    /// `user` | `assistant` | `system` — the only roles the server accepts.
    pub role: String,
    pub content: String,
    #[serde(rename = "actorName")]
    pub actor_name: String,
    /// `owner` | `steerer`.
    #[serde(rename = "actorRole")]
    pub actor_role: String,
    pub metadata: serde_json::Value,
    /// RFC3339.
    pub ts: String,
}

/// A whole session's turns, chunked for the wire.
#[derive(Debug, Clone, PartialEq)]
pub struct SessionTurns {
    pub session_id: String,
    pub source: String,
    /// Chunks of at most [`MAX_TURNS_PER_REQUEST`], in turn order.
    pub chunks: Vec<Vec<ConversationTurn>>,
}

/// The batch a single push run should publish, plus the watermark it advances to.
#[derive(Debug, Clone, PartialEq)]
pub struct TurnsBatch {
    pub sessions: Vec<SessionTurns>,
    /// End of the safely scanned event prefix, including suppressed events.
    /// Whole transcripts may contain later events without advancing past an
    /// unselected session's first pending event.
    pub session_event_id: i64,
}

/// Map a local `session_events` role/kind pair onto the three roles the server accepts.
///
/// `tool_result` has no server-side role of its own and is not the assistant speaking, so
/// it lands as `system` with the original kind preserved in `metadata`. Dropping these
/// would leave a transcript where the agent calls a tool and nothing ever comes back.
fn wire_role(role: &str) -> &'static str {
    match role {
        "user" => "user",
        "assistant" => "assistant",
        _ => "system",
    }
}

/// Who to attribute a turn to. Never empty — the server rejects an empty `actorName`, and
/// an unattributed turn in a shared transcript is worse than a coarse one.
fn actor_name(role: &str, model: Option<&str>, source: &str) -> String {
    match role {
        "user" => "user".to_string(),
        "assistant" => model
            .filter(|m| !m.trim().is_empty())
            .unwrap_or(source)
            .to_string(),
        _ => format!("{source}:tool"),
    }
}

fn epoch_ms_to_iso(ms: i64) -> String {
    crate::convergence::epoch_ms_to_iso(ms)
}

/// Build the next batch of conversation turns past `session_event_id`.
///
/// Sessions are published **whole**, not incrementally, because `turnIndex` is the
/// server's idempotency key and it is a position within the session. Publishing only the
/// new tail would number those turns from zero and overwrite the beginning of the
/// transcript with its own end — a corruption that still returns 200 and still looks like
/// a full session on read. Whole-session publishing costs a re-send and cannot do that.
///
/// `incognito` sessions are skipped but still advance the watermark, matching the
/// convergence outbox so a suppressed session is never rescanned forever.
pub fn build_turns_batch(
    conn: &Connection,
    session_event_id: i64,
    session_budget: usize,
    incognito: &HashSet<String>,
) -> Result<TurnsBatch> {
    // Selection and transcript reads must see the same snapshot: a concurrent
    // append must not move this batch's watermark beyond unselected history.
    // Respect an existing caller transaction instead of trying to nest BEGIN.
    let snapshot = if conn.is_autocommit() {
        Some(conn.unchecked_transaction()?)
    } else {
        None
    };
    let batch = build_turns_batch_in_snapshot(conn, session_event_id, session_budget, incognito)?;
    if let Some(snapshot) = snapshot {
        snapshot.commit()?;
    }
    Ok(batch)
}

fn build_turns_batch_in_snapshot(
    conn: &Connection,
    session_event_id: i64,
    session_budget: usize,
    incognito: &HashSet<String>,
) -> Result<TurnsBatch> {
    let budget = session_budget.clamp(1, ai_hist::storage::MAX_SCAN_LIMIT - 1);
    let lookahead = budget + 1;

    // Which sessions have anything new, oldest change first so the backlog drains in a
    // predictable order rather than jumping around. One extra session identifies
    // the first event this batch cannot acknowledge.
    let mut pending =
        ai_hist::storage::pending_event_sessions(conn, session_event_id, lookahead)?;

    if pending.is_empty() {
        return Ok(TurnsBatch {
            sessions: Vec::new(),
            session_event_id,
        });
    }

    let watermark_ceiling = if pending.len() > budget {
        pending.pop().map(|(_, _, first_new_id)| first_new_id - 1)
    } else {
        None
    };

    // A selected session may have later events beyond an unselected session.
    // Publish it whole, but acknowledge only the contiguous processed prefix.
    // Revisiting its tail is safe and finite: each nonempty batch advances past
    // at least its first pending event, even if all its turns are suppressed.
    let mut watermark = session_event_id;
    let mut sessions = Vec::new();

    for (session_id, source, _) in pending {
        let rows = ai_hist::session_events(conn, &session_id, Some(&source))?;

        let mut turns: Vec<ConversationTurn> = Vec::new();
        for event in rows {
            watermark = watermark.max(event.id);

            // An event with no text carries nothing a reader can use. Skip it, but only
            // after the watermark has moved past it.
            let content = match event.text.as_deref().map(str::trim) {
                Some(text) if !text.is_empty() => text.to_string(),
                _ => continue,
            };

            let index = turns.len() as i64;
            turns.push(ConversationTurn {
                session_owner: event.source.clone(),
                turn_index: index,
                role: wire_role(&event.role).to_string(),
                content,
                actor_name: actor_name(&event.role, event.model.as_deref(), &event.source),
                actor_role: "owner".to_string(),
                metadata: serde_json::json!({
                    // The server validates `nativeCli` against claude/codex and ignores
                    // other keys, so the local kind survives for readers that want to
                    // distinguish thinking from a tool call.
                    "kind": event.kind,
                    "sourceRole": event.role,
                }),
                ts: epoch_ms_to_iso(event.ts_ms),
            });
            let _ = event.session_id;
        }

        if incognito.contains(&session_id) || turns.is_empty() {
            continue;
        }

        let chunks: Vec<Vec<ConversationTurn>> = turns
            .chunks(MAX_TURNS_PER_REQUEST)
            .map(|chunk| chunk.to_vec())
            .collect();

        sessions.push(SessionTurns {
            session_id,
            source,
            chunks,
        });
    }

    Ok(TurnsBatch {
        sessions,
        session_event_id: watermark_ceiling.map_or(watermark, |ceiling| watermark.min(ceiling)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE session_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                session_id TEXT NOT NULL,
                project TEXT, project_key TEXT, cwd TEXT, git_branch TEXT,
                message_id TEXT, parent_id TEXT,
                ts_ms INTEGER NOT NULL,
                role TEXT NOT NULL,
                kind TEXT NOT NULL,
                text TEXT, model TEXT, token_json TEXT,
                provider TEXT,
                event_uid TEXT NOT NULL,
                raw_kind TEXT,
                -- Per-tool-result fidelity columns. `ai_hist::session_events`
                -- selects every column the crate defines, so a hand-built
                -- fixture table that stops at `event_uid` fails the read with
                -- `no such column` the moment the crate grows one.
                tool_use_id TEXT,
                payload_bytes INTEGER,
                payload_truncated INTEGER,
                payload_hash TEXT,
                call_index INTEGER,
                event_index INTEGER,
                result_status TEXT,
                event_source TEXT,
                error_signal TEXT,
                subagent_session_id TEXT,
                agent_id TEXT,
                request_id TEXT, provider_message_id TEXT,
                stop_reason TEXT, agent_version TEXT,
                is_sidechain INTEGER, is_meta INTEGER, turn_id TEXT, request_span TEXT
            );",
        )
        .unwrap();
        conn
    }

    fn insert(conn: &Connection, session: &str, ts: i64, role: &str, kind: &str, text: &str) {
        conn.execute(
            "INSERT INTO session_events (source, session_id, ts_ms, role, kind, text, model, event_uid)
             VALUES ('claude', ?1, ?2, ?3, ?4, ?5, 'claude-opus-5', ?6)",
            rusqlite::params![session, ts, role, kind, text, format!("{session}-{ts}-{kind}")],
        )
        .unwrap();
    }

    #[test]
    fn publishes_both_sides_of_the_conversation() {
        let conn = db();
        insert(&conn, "s1", 1000, "user", "text", "why is it failing?");
        insert(
            &conn,
            "s1",
            2000,
            "assistant",
            "thinking",
            "consider the regex",
        );
        insert(&conn, "s1", 3000, "assistant", "tool_use", "grep -n scrub");
        insert(
            &conn,
            "s1",
            4000,
            "tool_result",
            "tool_result",
            "scrub.ts:41",
        );
        insert(
            &conn,
            "s1",
            5000,
            "assistant",
            "text",
            "the scheme is unbounded",
        );

        let batch = build_turns_batch(&conn, 0, 10, &HashSet::new()).unwrap();
        let turns = &batch.sessions[0].chunks[0];

        assert_eq!(turns.len(), 5);
        assert_eq!(
            turns.iter().map(|t| t.role.as_str()).collect::<Vec<_>>(),
            ["user", "assistant", "assistant", "system", "assistant"],
        );
        // Dense, ordered indices — the server's idempotency key.
        assert_eq!(
            turns.iter().map(|t| t.turn_index).collect::<Vec<_>>(),
            [0, 1, 2, 3, 4],
        );
    }

    #[test]
    fn orders_by_timestamp_not_insertion() {
        let conn = db();
        insert(&conn, "s1", 3000, "assistant", "text", "third");
        insert(&conn, "s1", 1000, "user", "text", "first");
        insert(&conn, "s1", 2000, "assistant", "thinking", "second");

        let batch = build_turns_batch(&conn, 0, 10, &HashSet::new()).unwrap();
        let contents: Vec<&str> = batch.sessions[0].chunks[0]
            .iter()
            .map(|t| t.content.as_str())
            .collect();

        assert_eq!(contents, ["first", "second", "third"]);
    }

    /// `turnIndex` is a position in the session and the server's conflict key. Publishing
    /// only a new tail would renumber it from zero and overwrite the start of the
    /// transcript with its end — while still returning 200 and still reading as a full
    /// session. Whole-session publishing is what prevents that.
    #[test]
    fn republishes_the_whole_session_when_only_the_tail_is_new() {
        let conn = db();
        insert(&conn, "s1", 1000, "user", "text", "first");
        insert(&conn, "s1", 2000, "assistant", "text", "second");
        let first = build_turns_batch(&conn, 0, 10, &HashSet::new()).unwrap();
        assert_eq!(first.sessions[0].chunks[0].len(), 2);

        insert(&conn, "s1", 3000, "user", "text", "third");
        let second = build_turns_batch(&conn, first.session_event_id, 10, &HashSet::new()).unwrap();
        let turns = &second.sessions[0].chunks[0];

        assert_eq!(turns.len(), 3, "the whole session must be republished");
        assert_eq!(turns[0].content, "first");
        assert_eq!(turns[0].turn_index, 0);
        assert_eq!(turns[2].content, "third");
        assert_eq!(turns[2].turn_index, 2);
    }

    #[test]
    fn chunks_a_long_session_below_the_server_cap() {
        let conn = db();
        for i in 0..(MAX_TURNS_PER_REQUEST + 25) {
            insert(&conn, "s1", 1000 + i as i64, "user", "text", "hello");
        }

        let batch = build_turns_batch(&conn, 0, 10, &HashSet::new()).unwrap();
        let chunks = &batch.sessions[0].chunks;

        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].len(), MAX_TURNS_PER_REQUEST);
        assert_eq!(chunks[1].len(), 25);
        // Indices stay absolute across the chunk boundary, or the second request
        // overwrites the first at indices 0..25.
        assert_eq!(chunks[1][0].turn_index, MAX_TURNS_PER_REQUEST as i64);
    }

    #[test]
    fn skips_empty_text_but_still_advances_the_watermark() {
        let conn = db();
        insert(&conn, "s1", 1000, "assistant", "tool_use", "   ");
        insert(&conn, "s1", 2000, "user", "text", "real");

        let batch = build_turns_batch(&conn, 0, 10, &HashSet::new()).unwrap();

        assert_eq!(batch.sessions[0].chunks[0].len(), 1);
        assert_eq!(
            batch.session_event_id, 2,
            "watermark must pass the skipped row"
        );
    }

    #[test]
    fn incognito_sessions_are_skipped_but_do_not_stall_the_watermark() {
        let conn = db();
        insert(&conn, "secret", 1000, "user", "text", "private");
        insert(&conn, "s2", 2000, "user", "text", "public");

        let incognito: HashSet<String> = ["secret".to_string()].into_iter().collect();
        let batch = build_turns_batch(&conn, 0, 10, &incognito).unwrap();

        let ids: Vec<&str> = batch
            .sessions
            .iter()
            .map(|s| s.session_id.as_str())
            .collect();
        assert_eq!(ids, ["s2"]);
        assert_eq!(batch.session_event_id, 2);
    }

    /// The watermark must cover the sessions actually taken. A table-wide MAX would skip
    /// every session sorting after the budget cut — they would never be published, and
    /// the run would still report success.
    #[test]
    fn budget_does_not_skip_sessions_past_the_cut() {
        let conn = db();
        insert(&conn, "s1", 1000, "user", "text", "one");
        insert(&conn, "s2", 2000, "user", "text", "two");
        insert(&conn, "s3", 3000, "user", "text", "three");

        let first = build_turns_batch(&conn, 0, 1, &HashSet::new()).unwrap();
        assert_eq!(first.sessions.len(), 1);
        assert_eq!(first.sessions[0].session_id, "s1");

        let second = build_turns_batch(&conn, first.session_event_id, 1, &HashSet::new()).unwrap();
        assert_eq!(
            second.sessions[0].session_id, "s2",
            "s2 must not be skipped"
        );

        let third = build_turns_batch(&conn, second.session_event_id, 1, &HashSet::new()).unwrap();
        assert_eq!(third.sessions[0].session_id, "s3");
    }

    #[test]
    fn budget_does_not_skip_interleaved_sessions() {
        let conn = db();
        insert(&conn, "a", 1000, "user", "text", "a first");
        insert(&conn, "b", 2000, "user", "text", "b first");
        insert(&conn, "a", 3000, "assistant", "text", "a last");

        let first = build_turns_batch(&conn, 0, 1, &HashSet::new()).unwrap();
        assert_eq!(first.sessions[0].session_id, "a");
        assert_eq!(first.sessions[0].chunks[0].len(), 2);
        assert_eq!(first.session_event_id, 1);
        assert_eq!(
            build_turns_batch(&conn, 0, 1, &HashSet::new()).unwrap(),
            first,
            "a retry before acknowledgment preserves the batch for unchanged storage"
        );
        let second = build_turns_batch(&conn, first.session_event_id, 1, &HashSet::new()).unwrap();
        assert_eq!(second.sessions[0].session_id, "b");
        assert_eq!(second.session_event_id, 2);
        let third = build_turns_batch(&conn, second.session_event_id, 1, &HashSet::new()).unwrap();
        assert_eq!(third.sessions, first.sessions);
        assert_eq!(third.session_event_id, 3);
        assert!(
            build_turns_batch(&conn, third.session_event_id, 1, &HashSet::new())
                .unwrap()
                .sessions
                .is_empty()
        );
    }

    #[test]
    fn interleaved_backlog_drains_in_stable_order_with_bounded_sessions() {
        for budget in [0, 1, 2, 3, 20, usize::MAX] {
            let conn = db();
            let order = ["a", "b", "c", "a", "d", "b", "e", "c", "a", "e"];
            for (index, session) in order.iter().enumerate() {
                insert(&conn, session, index as i64, "user", "text", session);
            }
            let mut cursor = 0;
            let mut observed = HashSet::new();
            let mut batches = 0;
            while cursor < order.len() as i64 {
                let expected: Vec<&str> = order[cursor as usize..]
                    .iter()
                    .copied()
                    .fold(Vec::new(), |mut sessions, session| {
                        if !sessions.contains(&session) {
                            sessions.push(session);
                        }
                        sessions
                    })
                    .into_iter()
                    .take(budget.max(1))
                    .collect();
                let batch = build_turns_batch(&conn, cursor, budget, &HashSet::new()).unwrap();
                let actual: Vec<&str> = batch
                    .sessions
                    .iter()
                    .map(|s| s.session_id.as_str())
                    .collect();
                assert_eq!(actual, expected);
                assert!(batch.session_event_id > cursor);
                assert!(batch.session_event_id <= order.len() as i64);
                for session in &batch.sessions {
                    let expected_turns =
                        order.iter().filter(|id| **id == session.session_id).count();
                    assert_eq!(session.chunks[0].len(), expected_turns);
                    observed.insert(session.session_id.clone());
                }
                batches += 1;
                assert!(
                    batches <= order.len(),
                    "every batch must make durable progress"
                );
                cursor = batch.session_event_id;
            }
            assert_eq!(observed.len(), 5);
            let exhausted = build_turns_batch(&conn, cursor, budget, &HashSet::new()).unwrap();
            assert!(exhausted.sessions.is_empty());
            assert_eq!(exhausted.session_event_id, cursor);
        }
    }

    #[test]
    fn interleaved_suppressed_sessions_cannot_skip_public_history() {
        let conn = db();
        for (index, (session, text)) in [
            ("secret", "private"),
            ("public", "visible"),
            ("secret", "private tail"),
            ("empty", "   "),
            ("public-two", "also visible"),
            ("empty", ""),
        ]
        .into_iter()
        .enumerate()
        {
            insert(&conn, session, index as i64, "user", "text", text);
        }
        let incognito = HashSet::from(["secret".to_string()]);
        let mut cursor = 0;
        let mut published = Vec::new();
        for _ in 0..6 {
            let batch = build_turns_batch(&conn, cursor, 1, &incognito).unwrap();
            assert!(batch.session_event_id > cursor);
            published.extend(batch.sessions.into_iter().map(|s| s.session_id));
            cursor = batch.session_event_id;
        }
        assert_eq!(published, ["public", "public-two"]);
        assert_eq!(cursor, 6);
        assert!(build_turns_batch(&conn, cursor, 1, &incognito)
            .unwrap()
            .sessions
            .is_empty());
    }

    #[test]
    fn equal_session_ids_from_different_sources_have_independent_budget_positions() {
        let conn = db();
        insert(&conn, "shared", 1, "user", "text", "claude first");
        insert(&conn, "shared", 2, "user", "text", "codex");
        conn.execute(
            "UPDATE session_events SET source = 'codex' WHERE id = 2",
            [],
        )
        .unwrap();
        insert(&conn, "shared", 3, "user", "text", "claude last");
        let first = build_turns_batch(&conn, 0, 1, &HashSet::new()).unwrap();
        assert_eq!(first.sessions[0].source, "claude");
        assert_eq!(first.session_event_id, 1);
        let next = build_turns_batch(&conn, first.session_event_id, 1, &HashSet::new()).unwrap();
        assert_eq!(next.sessions[0].source, "codex");
        assert_eq!(next.sessions[0].chunks[0][0].content, "codex");
        assert_eq!(next.session_event_id, 2);
    }

    #[test]
    fn transcript_build_does_not_commit_the_callers_transaction() {
        let mut conn = db();
        let transaction = conn.transaction().unwrap();
        insert(&transaction, "pending", 1, "user", "text", "not committed");
        let batch = build_turns_batch(&transaction, 0, 1, &HashSet::new()).unwrap();
        assert_eq!(batch.session_event_id, 1);
        assert!(!transaction.is_autocommit());
        transaction.rollback().unwrap();
        assert!(build_turns_batch(&conn, 0, 1, &HashSet::new())
            .unwrap()
            .sessions
            .is_empty());
        assert!(conn.is_autocommit());
    }

    #[test]
    fn concurrent_appends_remain_pending_beyond_the_read_snapshot() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("history.db");
        let mut reader = ai_hist::open_db(&path).unwrap();
        insert(&reader, "a", 1, "user", "text", "a first");
        let writer = ai_hist::open_db(&path).unwrap();
        let snapshot = reader.transaction().unwrap();
        let count: i64 = snapshot
            .query_row("SELECT COUNT(*) FROM session_events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);

        // WAL permits ingestion while the delivery reader holds its snapshot.
        insert(&writer, "b", 2, "user", "text", "b first");
        insert(&writer, "a", 3, "assistant", "text", "a last");
        let first = build_turns_batch(&snapshot, 0, 1, &HashSet::new()).unwrap();
        assert_eq!(first.session_event_id, 1);
        assert_eq!(first.sessions[0].chunks[0].len(), 1);
        snapshot.commit().unwrap();

        let next = build_turns_batch(&reader, first.session_event_id, 2, &HashSet::new()).unwrap();
        assert_eq!(next.session_event_id, 3);
        assert_eq!(next.sessions[0].session_id, "b");
        assert_eq!(next.sessions[1].session_id, "a");
        assert_eq!(next.sessions[1].chunks[0].len(), 2);
    }

    #[test]
    fn malformed_event_fails_the_batch_instead_of_acknowledging_past_it() {
        let conn = db();
        insert(&conn, "broken", 1, "user", "text", "bad timestamp");
        insert(&conn, "valid", 2, "user", "text", "must not hide the error");
        conn.execute(
            "UPDATE session_events SET ts_ms = 'invalid' WHERE id = 1",
            [],
        )
        .unwrap();
        assert!(build_turns_batch(&conn, 0, 2, &HashSet::new()).is_err());
        assert!(conn.is_autocommit(), "a failed read releases its snapshot");
    }

    #[test]
    fn attributes_every_turn_to_a_non_empty_actor() {
        let conn = db();
        insert(&conn, "s1", 1000, "user", "text", "hi");
        insert(&conn, "s1", 2000, "assistant", "text", "hello");
        insert(&conn, "s1", 3000, "tool_result", "tool_result", "ok");

        let batch = build_turns_batch(&conn, 0, 10, &HashSet::new()).unwrap();

        for turn in &batch.sessions[0].chunks[0] {
            assert!(
                !turn.actor_name.trim().is_empty(),
                "the server rejects an empty actorName: {turn:?}",
            );
        }
    }

    #[test]
    fn no_new_events_is_a_no_op_that_holds_its_watermark() {
        let conn = db();
        insert(&conn, "s1", 1000, "user", "text", "hi");
        let first = build_turns_batch(&conn, 0, 10, &HashSet::new()).unwrap();

        let second = build_turns_batch(&conn, first.session_event_id, 10, &HashSet::new()).unwrap();

        assert!(second.sessions.is_empty());
        assert_eq!(second.session_event_id, first.session_event_id);
    }
}

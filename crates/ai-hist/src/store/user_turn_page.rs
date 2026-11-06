//! Paged user-turn headers, read forward from the cursor.
//!
//! `GROUP BY` the turn key aggregates every user-turn row of the session
//! before `HAVING` and `LIMIT`, so each page of an N-turn session regroups
//! all N. This walk does not. It also cannot close a turn when the key
//! changes: blocks that share a message id are not a contiguous span, and
//! the anchor is `(MIN(ts_ms), MIN(id))`, which can pair two different rows.
//! A later row of an earlier turn would be dropped or reported as a second
//! anchor.
//!
//! Each newly seen key is aggregated from its own rows. Named rows are
//! found through `idx_session_events_session_message` (the message id is
//! not a turn-key index, and one is not added). The cross-session message
//! index stays for continuity: Codex reuses a line-derived id across
//! sessions, and that index would return every one of them. An empty
//! message id keys the row as `event:<id>`: that is the row itself, plus
//! any row that stored that same string as its message id.
//!
//! Once every user-turn row at a timestamp has been seen, those turns are
//! a prefix of the page order — a turn not yet seen has a later
//! `MIN(ts_ms)`. The walk stops when that prefix holds the page and one
//! later turn, or the session ends. Rows after that later turn are not
//! read, and a turn whose anchor is at or before the cursor is not grouped
//! into the page.

use std::collections::{BTreeMap, HashSet};

use rusqlite::{params, Connection, Statement};

use super::{SessionEventCursor, USER_TURN_ROW_FILTER};

use anyhow::Result;

/// One turn's anchor, identity, and the timestamp of its last block.
#[derive(Debug, Clone)]
pub(super) struct UserTurnHeader {
    pub(super) key: String,
    pub(super) ts_ms: i64,
    pub(super) id: i64,
    pub(super) order: i64,
    pub(super) message_id: Option<String>,
    pub(super) last_ts_ms: i64,
}

type HeaderPage = (Vec<UserTurnHeader>, bool);

/// User-turn rows from a position, in `(ts_ms, id)` order.
///
/// The turn filter is a select expression, not a `WHERE` term, so the
/// range stays a seek on `idx_session_events_source_page` — the same shape
/// as the block pass. `after` selects `(ts_ms, id) > cursor`.
pub(super) fn user_turn_walk_sql(after: bool) -> String {
    let range = if after {
        " AND (ts_ms, COALESCE(source_order,id), id) > (?, ?, ?)"
    } else {
        ""
    };
    format!(
        "SELECT id, ts_ms, NULLIF(message_id, ''), \
                CASE WHEN {USER_TURN_ROW_FILTER} THEN 1 ELSE 0 END, source_order \
         FROM session_events \
         WHERE source = ? AND session_id = ?{range} \
         ORDER BY ts_ms ASC, COALESCE(source_order,id) ASC, id ASC"
    )
}

/// Every user-turn row of one message in one session.
///
/// `INDEXED BY` seeks `(source, session_id, message_id)`. The cross-session
/// message index matches only `(source, message_id)`, so a line-derived id
/// Codex reuses would return every session that stored it.
pub(super) fn user_turn_message_rows_sql() -> String {
    message_rows_sql("idx_session_events_session_message")
}

/// The same lookup through whichever index this database has.
///
/// The session-message index seeks the rows. A database that predates it
/// still has the page index, which seeks the session and filters the id.
/// Naming the cross-session message index here would read every session
/// that reused the id.
fn message_rows_sql_for(conn: &Connection) -> Result<String> {
    let present = conn
        .prepare("SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = ?1 LIMIT 1")?
        .exists(["idx_session_events_session_message"])?;
    Ok(if present {
        user_turn_message_rows_sql()
    } else {
        message_rows_sql("idx_session_events_source_page")
    })
}

fn message_rows_sql(index: &str) -> String {
    format!(
        "SELECT id, ts_ms, NULLIF(message_id, ''), source_order \
         FROM session_events INDEXED BY {index} \
         WHERE source = ? AND session_id = ? AND message_id = ? \
           AND {USER_TURN_ROW_FILTER}"
    )
}

/// The unnamed row an `event:<id>` key collides with, by primary key.
///
/// A provider message id can be the literal `event:<id>` that an unnamed
/// row uses as its fallback key. Those rows are one turn. The lookup is
/// the row itself, not a scan.
pub(super) fn user_turn_collided_row_sql() -> String {
    format!(
        "SELECT id, ts_ms, source_order FROM session_events \
         WHERE id = ? AND source = ? AND session_id = ? \
           AND NULLIF(message_id, '') IS NULL AND {USER_TURN_ROW_FILTER}"
    )
}

/// At most `limit` turn headers after `after`, and whether another turn
/// follows them. `limit` is already clamped by the caller.
pub(super) fn page_headers(
    conn: &Connection,
    source: &str,
    session_id: &str,
    limit: i64,
    after: Option<&SessionEventCursor>,
) -> Result<HeaderPage> {
    let mut walk = conn.prepare(&user_turn_walk_sql(after.is_some()))?;
    let mut named = conn.prepare(&message_rows_sql_for(conn)?)?;
    let mut collided = conn.prepare(&user_turn_collided_row_sql())?;
    let after_order = after
        .map(|cursor| super::event_cursor_order(conn, session_id, Some(source), cursor))
        .transpose()?;
    let after_position = after
        .zip(after_order)
        .map(|(cursor, order)| (cursor.ts_ms, order, cursor.id));
    let mut rows = match after {
        Some(cursor) => walk.query(params![
            source,
            session_id,
            cursor.ts_ms,
            after_order,
            cursor.id
        ])?,
        None => walk.query(params![source, session_id])?,
    };
    let mut state = PageState::new(limit as usize);
    while let Some(row) = rows.next()? {
        let scanned = ScannedRow {
            id: row.get(0)?,
            ts_ms: row.get(1)?,
            message_id: row.get(2)?,
            is_turn: row.get::<_, i64>(3)? == 1,
            source_order: row.get(4)?,
        };
        if let Some(page) = state.observe(
            &mut named,
            &mut collided,
            source,
            session_id,
            after_position,
            scanned,
        )? {
            return Ok(page);
        }
    }
    Ok(state.finish())
}

struct ScannedRow {
    id: i64,
    ts_ms: i64,
    message_id: Option<String>,
    is_turn: bool,
    source_order: Option<i64>,
}

struct PageState {
    limit: usize,
    current_ts: Option<i64>,
    /// The first `limit` turns are final: any turn not yet seen sorts after
    /// them. The walk continues only until one such turn shows there is a
    /// next page.
    frozen: bool,
    extra: bool,
    seen: HashSet<String>,
    candidates: BTreeMap<(i64, i64, i64), UserTurnHeader>,
}

impl PageState {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            current_ts: None,
            frozen: false,
            extra: false,
            seen: HashSet::new(),
            candidates: BTreeMap::new(),
        }
    }

    fn observe(
        &mut self,
        named: &mut Statement<'_>,
        collided: &mut Statement<'_>,
        source: &str,
        session_id: &str,
        after: Option<(i64, i64, i64)>,
        row: ScannedRow,
    ) -> Result<Option<HeaderPage>> {
        if self.starts_later_timestamp(row.ts_ms) {
            if let Some(page) = self.prefix_if_complete() {
                return Ok(Some(page));
            }
            self.freeze_if_page_full();
        }
        self.current_ts = Some(row.ts_ms);
        if row.is_turn {
            self.consider(named, collided, source, session_id, after, &row)?;
        }
        if self.extra {
            return Ok(Some(self.page(true)));
        }
        Ok(None)
    }

    fn finish(self) -> HeaderPage {
        let has_more = self.candidates.len() > self.limit;
        self.page(has_more)
    }

    /// A higher timestamp means every turn with `MIN(ts_ms)` at or before
    /// the previous one has already been seen.
    fn starts_later_timestamp(&self, ts_ms: i64) -> bool {
        self.current_ts.is_some_and(|prev| ts_ms > prev)
    }

    fn prefix_if_complete(&self) -> Option<HeaderPage> {
        if self.candidates.len() > self.limit {
            Some(self.page(true))
        } else {
            None
        }
    }

    fn freeze_if_page_full(&mut self) {
        if self.candidates.len() == self.limit {
            self.frozen = true;
        }
    }

    fn consider(
        &mut self,
        named: &mut Statement<'_>,
        collided: &mut Statement<'_>,
        source: &str,
        session_id: &str,
        after: Option<(i64, i64, i64)>,
        row: &ScannedRow,
    ) -> Result<()> {
        let key = turn_key(row.message_id.as_deref(), row.id);
        if !self.seen.insert(key.clone()) {
            return Ok(());
        }
        let header = aggregate(named, collided, source, session_id, &key, row)?;
        if !past_cursor(&header, after) {
            return Ok(());
        }
        let anchor = (header.ts_ms, header.order, header.id);
        if self.candidates.insert(anchor, header).is_some() {
            anyhow::bail!("two user turns share an anchor");
        }
        if self.frozen {
            self.extra = true;
        }
        Ok(())
    }

    fn page(&self, has_more: bool) -> HeaderPage {
        let headers = self.candidates.values().take(self.limit).cloned().collect();
        (headers, has_more)
    }
}

fn past_cursor(header: &UserTurnHeader, after: Option<(i64, i64, i64)>) -> bool {
    after.is_none_or(|cursor| (header.ts_ms, header.order, header.id) > cursor)
}

fn turn_key(message_id: Option<&str>, id: i64) -> String {
    match message_id {
        Some(message_id) if !message_id.is_empty() => message_id.to_owned(),
        _ => format!("event:{id}"),
    }
}

/// `event:<id>` only when the suffix is that id's own decimal form.
/// `event:012` is a provider message id, not row 12.
fn colliding_event_id(key: &str) -> Option<i64> {
    let suffix = key.strip_prefix("event:")?;
    let id = suffix.parse::<i64>().ok()?;
    (format!("event:{id}") == key).then_some(id)
}

struct Fold {
    ts_ms: i64,
    id: i64,
    last_ts_ms: i64,
    named: bool,
    anchor: (i64, i64, i64),
    known: bool,
}

impl Fold {
    fn seed(row: &ScannedRow) -> Self {
        Self {
            ts_ms: row.ts_ms,
            id: row.id,
            last_ts_ms: row.ts_ms,
            named: row.message_id.is_some(),
            anchor: (row.ts_ms, row.source_order.unwrap_or(row.id), row.id),
            known: row.source_order.is_some(),
        }
    }

    fn observe(&mut self, id: i64, ts_ms: i64, named: bool, source_order: Option<i64>) {
        self.ts_ms = self.ts_ms.min(ts_ms);
        self.id = self.id.min(id);
        self.last_ts_ms = self.last_ts_ms.max(ts_ms);
        self.named |= named;
        self.anchor = self.anchor.min((ts_ms, source_order.unwrap_or(id), id));
        self.known |= source_order.is_some();
    }

    fn header(self, key: &str) -> UserTurnHeader {
        UserTurnHeader {
            key: key.to_owned(),
            ts_ms: self.ts_ms,
            id: if self.known { self.anchor.2 } else { self.id },
            order: if self.known { self.anchor.1 } else { self.id },
            message_id: self.named.then(|| key.to_owned()),
            last_ts_ms: self.last_ts_ms,
        }
    }
}

fn aggregate(
    named: &mut Statement<'_>,
    collided: &mut Statement<'_>,
    source: &str,
    session_id: &str,
    key: &str,
    row: &ScannedRow,
) -> Result<UserTurnHeader> {
    let mut fold = Fold::seed(row);
    fold_named(named, source, session_id, key, &mut fold)?;
    if let Some(row_id) = colliding_event_id(key) {
        fold_collided(collided, source, session_id, row_id, &mut fold)?;
    }
    Ok(fold.header(key))
}

fn fold_named(
    stmt: &mut Statement<'_>,
    source: &str,
    session_id: &str,
    key: &str,
    fold: &mut Fold,
) -> Result<()> {
    let mut rows = stmt.query(params![source, session_id, key])?;
    while let Some(row) = rows.next()? {
        let named: Option<String> = row.get(2)?;
        fold.observe(row.get(0)?, row.get(1)?, named.is_some(), row.get(3)?);
    }
    Ok(())
}

fn fold_collided(
    stmt: &mut Statement<'_>,
    source: &str,
    session_id: &str,
    row_id: i64,
    fold: &mut Fold,
) -> Result<()> {
    let mut rows = stmt.query(params![row_id, source, session_id])?;
    while let Some(row) = rows.next()? {
        fold.observe(row.get(0)?, row.get(1)?, false, row.get(2)?);
    }
    Ok(())
}

//! Paged user-turn headers, read forward from the cursor.
//!
//! `GROUP BY` the turn key aggregates every user-turn row of the session
//! before `HAVING` and `LIMIT`, so each page of an N-turn session regroups
//! all N. This walk does not. It also cannot close a turn when the key
//! changes: blocks that share a message id are not a contiguous span.
//! Each anchor is the first actual row in `(ts_ms, COALESCE(source_order,id),
//! id)` order; combining independent minima can create an invalid cursor.
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
    let after_position = after
        .map(|cursor| turn_cursor_position(conn, source, session_id, cursor))
        .transpose()?;
    let mut rows = match after_position {
        Some((ts, order, id)) => walk.query(params![source, session_id, ts, order, id])?,
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

/// Old synthetic turn anchors cannot safely continue after the anchor policy
/// changes: remapping them can skip unseen turns or repeat earlier ones.
/// Actual row cursors retain the generic event cursor policy unchanged.
fn turn_cursor_position(
    conn: &Connection,
    source: &str,
    session_id: &str,
    cursor: &SessionEventCursor,
) -> Result<(i64, i64, i64)> {
    let order = super::event_cursor_order(conn, session_id, Some(source), cursor)?;
    let synthetic: bool = conn.query_row(
        &format!(
            "SELECT EXISTS(SELECT 1 FROM session_events WHERE id=?1 AND source=?2 \
         AND session_id=?3 AND ts_ms<>?4 AND {USER_TURN_ROW_FILTER})"
        ),
        params![cursor.id, source, session_id, cursor.ts_ms],
        |row| row.get(0),
    )?;
    anyhow::ensure!(!synthetic, "STALE_EVENT_CURSOR: legacy user-turn anchor combines different rows; restart user-turn pagination");
    Ok((cursor.ts_ms, order, cursor.id))
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
    last_ts_ms: i64,
    named: bool,
    anchor: (i64, i64, i64),
}

impl Fold {
    fn seed(row: &ScannedRow) -> Self {
        Self {
            last_ts_ms: row.ts_ms,
            named: row.message_id.is_some(),
            anchor: (row.ts_ms, row.source_order.unwrap_or(row.id), row.id),
        }
    }

    fn observe(&mut self, id: i64, ts_ms: i64, named: bool, source_order: Option<i64>) {
        self.last_ts_ms = self.last_ts_ms.max(ts_ms);
        self.named |= named;
        self.anchor = self.anchor.min((ts_ms, source_order.unwrap_or(id), id));
    }

    fn header(self, key: &str) -> UserTurnHeader {
        UserTurnHeader {
            key: key.to_owned(),
            ts_ms: self.anchor.0,
            id: self.anchor.2,
            order: self.anchor.1,
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

#[cfg(test)]
mod tests {
    use super::super::*;

    fn seed(conn: &Connection, ranked: bool) {
        init_db(conn).unwrap();
        for (id, ts, message, role, rank) in [
            (1, 200, "split", "user", None),
            (2, 100, "split", "user", None),
            (3, 100, "assistant", "assistant", ranked.then_some(0)),
            (4, 100, "second", "user", ranked.then_some(1)),
            (5, 300, "last", "user", None),
        ] {
            conn.execute("INSERT INTO session_events(id,source,session_id,event_uid,ts_ms,message_id,role,kind,text,source_order) VALUES(?1,'claude','s',?2,?3,?4,?5,'text',?4,?6)",params![id,format!("e{id}"),ts,message,role,rank]).unwrap();
        }
    }

    #[test]
    fn mixed_order_turn_pages_use_real_row_anchors_and_preserve_neighbours() {
        let conn = Connection::open_in_memory().unwrap();
        seed(&conn, true);
        let mut turns = Vec::new();
        let mut cursor = None;
        for _ in 0..4 {
            let page = session_user_turns_page(&conn, "claude", "s", 1, cursor.as_ref()).unwrap();
            turns.extend(page.user_turns);
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        assert!(cursor.is_none(), "pagination must terminate");
        assert_eq!(
            turns.iter().map(|turn| turn.id).collect::<Vec<_>>(),
            [4, 2, 5]
        );
        assert_eq!(turns, session_user_turns_all(&conn, "claude", "s").unwrap());
        assert_eq!(turns[0].preceding_message_id.as_deref(), Some("assistant"));
        assert_eq!(turns[0].following_message_id.as_deref(), Some("split"));
        assert_eq!(turns[1].blocks.len(), 2);
        assert_eq!(turns[1].preceding_message_id.as_deref(), Some("second"));
        assert_eq!(turns[1].following_message_id.as_deref(), Some("last"));
        assert_eq!(turns[2].preceding_message_id.as_deref(), Some("split"));
    }

    #[test]
    fn legacy_composite_turn_cursor_requires_restart_but_actual_rows_continue() {
        let conn = Connection::open_in_memory().unwrap();
        seed(&conn, false);
        let all = session_user_turns_all(&conn, "claude", "s").unwrap();
        assert_eq!(
            all.iter().map(|turn| turn.id).collect::<Vec<_>>(),
            [2, 4, 5]
        );
        let first = session_user_turns_page(&conn, "claude", "s", 1, None).unwrap();
        assert_eq!(first.user_turns, all[..1]);
        assert_eq!(
            first.next_cursor,
            Some(SessionEventCursor { ts_ms: 100, id: 2 })
        );
        let old = SessionEventCursor { ts_ms: 100, id: 1 };
        let stale = session_user_turns_page(&conn, "claude", "s", 1, Some(&old))
            .unwrap_err()
            .to_string();
        assert!(stale.contains("STALE_EVENT_CURSOR"));
        assert!(stale.contains("restart user-turn pagination"));
        let resumed =
            session_user_turns_page(&conn, "claude", "s", 1, first.next_cursor.as_ref()).unwrap();
        assert_eq!(resumed.user_turns, all[1..2]);
        assert_eq!(resumed.user_turns[0].id, 4);
        let tail =
            session_user_turns_page(&conn, "claude", "s", 1, resumed.next_cursor.as_ref()).unwrap();
        assert_eq!(tail.user_turns, all[2..]);
        assert_eq!(
            super::super::event_cursor_order(&conn, "s", Some("claude"), &old).unwrap(),
            1,
            "generic event cursors retain positional legacy semantics"
        );
        conn.execute("UPDATE session_events SET source_order=0 WHERE id=3", [])
            .unwrap();
        assert!(session_user_turns_page(&conn, "claude", "s", 1, Some(&old))
            .unwrap_err()
            .to_string()
            .contains("INVALID_EVENT_CURSOR"));
    }
}

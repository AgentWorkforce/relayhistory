//! OpenCode 2's session tables, `session_v2` and `session_message`, in the
//! same `opencode.db`.
//!
//! OpenCode 2 copies every V1 session into these tables the first time it
//! opens a store, keeping session and message ids and `time_updated`, and from
//! then on writes only here. The V1 `session`, `message` and `part` rows stay
//! as they were. So a session belongs to OpenCode 2 when V1 does not hold it,
//! or when its `session_v2` row is newer than its V1 row (a V1 session resumed
//! under OpenCode 2). Every other session is still read from the V1 tables and
//! its evidence does not move.
//!
//! The loader here rewrites each OpenCode 2 message into the V1 message and
//! part payloads, so [`super::normalize`] stays the one parser for every
//! layout.

use super::{finish, has_columns, parse_message, table_columns, OpencodeMessage, OpencodePart};
use super::{OpencodeSession, OpencodeSessionInfo};
use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet};

/// A V1 message payload and its V1 parts.
type V1Message = (Map<String, Value>, Vec<Map<String, Value>>);

const SESSION_COLUMNS: &[&str] = &["id", "directory", "time_created", "time_updated"];
const MESSAGE_COLUMNS: &[&str] = &["id", "session_id", "type", "seq", "time_created", "data"];

/// Whether this store has OpenCode 2's session tables with the columns read
/// here.
pub(crate) fn present(src: &Connection) -> Result<bool> {
    Ok(
        has_columns(&table_columns(src, "session_v2")?, SESSION_COLUMNS)
            && has_columns(&table_columns(src, "session_message")?, MESSAGE_COLUMNS),
    )
}

/// The predicate, over `session_v2` aliased `v`, that OpenCode 2 owns the
/// session. Empty when the store has no V1 `session` table to defer to.
fn owned_predicate(src: &Connection) -> Result<&'static str> {
    let v1 = table_columns(src, "session")?;
    Ok(match (v1.contains("id"), v1.contains("time_updated")) {
        (false, _) => "",
        (true, true) => {
            " AND NOT EXISTS (SELECT 1 FROM session s WHERE s.id = v.id \
             AND COALESCE(s.time_updated, 0) >= v.time_updated)"
        }
        (true, false) => " AND NOT EXISTS (SELECT 1 FROM session s WHERE s.id = v.id)",
    })
}

/// `SELECT <columns> FROM session_v2 v` over the sessions OpenCode 2 owns.
pub(crate) fn owned_sessions_sql(src: &Connection, columns: &str) -> Result<String> {
    Ok(format!(
        "SELECT {columns} FROM session_v2 v WHERE v.id IS NOT NULL AND v.id <> ''{}",
        owned_predicate(src)?
    ))
}

/// The clause a query over the V1 `session` table appends so it leaves out
/// the sessions OpenCode 2 owns. Empty on a store without OpenCode 2.
pub(crate) fn v1_exclusion(src: &Connection) -> Result<&'static str> {
    if !present(src)? {
        return Ok("");
    }
    Ok(
        match table_columns(src, "session")?.contains("time_updated") {
            true => {
                " AND NOT EXISTS (SELECT 1 FROM session_v2 v WHERE v.id = session.id \
             AND v.time_updated > COALESCE(session.time_updated, 0))"
            }
            false => "",
        },
    )
}

/// The V1 `session` table's columns, and the query over its sessions that
/// OpenCode 2 does not own (selecting what the V1 loaders read).
pub(crate) fn v1_sessions(src: &Connection) -> Result<(BTreeSet<String>, String)> {
    let columns = table_columns(src, "session")?;
    let sql = format!(
        "SELECT {} FROM session WHERE id IS NOT NULL AND id <> ''{}",
        OpencodeSessionInfo::columns(&columns),
        v1_exclusion(src)?
    );
    Ok((columns, sql))
}

/// Whether OpenCode 2 owns `session_id` in this store.
pub(crate) fn owns(src: &Connection, session_id: &str) -> Result<bool> {
    if !present(src)? {
        return Ok(false);
    }
    let sql = format!("{} AND v.id = ?", owned_sessions_sql(src, "1")?);
    Ok(src.prepare_cached(&sql)?.exists([session_id])?)
}

/// Whether the store holds the session in either layout. A store OpenCode 2
/// created has no V1 `session` table at all.
pub(crate) fn holds(src: &Connection, session_id: &str) -> Result<bool> {
    let held = table_columns(src, "session")?.contains("id")
        && src
            .prepare("SELECT 1 FROM session WHERE id = ?")?
            .exists([session_id])?;
    Ok(held || owns(src, session_id)?)
}

/// The ids of every session OpenCode 2 owns.
pub(crate) fn owned_ids(src: &Connection) -> Result<Vec<String>> {
    if !present(src)? {
        return Ok(Vec::new());
    }
    let sql = owned_sessions_sql(src, "v.id")?;
    let mut stmt = src.prepare(&sql)?;
    let ids = stmt
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(ids)
}

/// Add every session OpenCode 2 owns to a whole-store load. `session_message`
/// is indexed by session, so they are read one at a time on either plan.
pub(crate) fn load_owned(
    src: &Connection,
    mut load: super::OpencodeStoreLoad,
) -> Result<super::OpencodeStoreLoad> {
    for session_id in owned_ids(src)? {
        match self::load(src, &session_id) {
            Ok(Some(session)) => load.sessions.push(session),
            Ok(None) => {}
            Err(error) => load.failures.push(super::OpencodeSessionFailure {
                session_id,
                error: format!("{error:#}"),
            }),
        }
    }
    Ok(load)
}

/// Read one OpenCode 2 session. `None` when the store has no such session or
/// no OpenCode 2 tables.
pub(crate) fn load(src: &Connection, session_id: &str) -> Result<Option<OpencodeSession>> {
    super::super::check_capture_cancelled()?;
    if !present(src)? {
        return Ok(None);
    }
    let Some(info) = read_info(src, session_id)? else {
        return Ok(None);
    };
    let mut messages = Vec::new();
    let mut parts_by_message = BTreeMap::new();
    let mut stmt = src.prepare_cached(
        "SELECT id, type, time_created, data FROM session_message \
         WHERE session_id = ? AND json_valid(data) ORDER BY seq",
    )?;
    let rows = stmt
        .query_map([session_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<i64>>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()
        .with_context(|| format!("reading OpenCode 2 messages of {session_id}"))?;
    for (id, kind, created, data) in rows {
        super::super::check_capture_cancelled()?;
        let Ok(Value::Object(object)) = serde_json::from_str::<Value>(&data) else {
            continue;
        };
        let Some((envelope, parts)) = as_v1(&kind, &object) else {
            continue;
        };
        let Some(message) = parse_message(&id, &envelope, created) else {
            continue;
        };
        parts_by_message.insert(message.id.clone(), numbered(&message, parts));
        messages.push(message);
    }
    Ok(Some(finish(info, messages, parts_by_message)))
}

fn read_info(src: &Connection, session_id: &str) -> Result<Option<OpencodeSessionInfo>> {
    let columns = table_columns(src, "session_v2")?;
    let optional = |name| super::optional_column(&columns, name);
    let sql = format!(
        "SELECT id, {}, directory, time_created, time_updated, {} FROM session_v2 WHERE id = ?",
        optional("parent_id"),
        optional("title"),
    );
    let info = src
        .query_row(&sql, [session_id], |row| {
            Ok(OpencodeSessionInfo {
                id: row.get(0)?,
                parent_id: row.get::<_, Option<String>>(1)?.filter(|id| !id.is_empty()),
                directory: row.get(2)?,
                created_ms: row.get(3)?,
                updated_ms: row.get(4)?,
                title: crate::ingest::titles::opencode_title(
                    row.get::<_, Option<String>>(5)?.as_deref(),
                ),
            })
        })
        .optional()
        .with_context(|| format!("reading OpenCode 2 session {session_id}"))?;
    Ok(info)
}

/// Part ids that sort in content order, since [`finish`] orders parts by id.
fn numbered(message: &OpencodeMessage, parts: Vec<Map<String, Value>>) -> Vec<OpencodePart> {
    parts
        .into_iter()
        .enumerate()
        .map(|(index, raw)| OpencodePart {
            id: format!("{}:{index:04}", message.id),
            kind: raw
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            raw,
        })
        .collect()
}

/// One OpenCode 2 message as the V1 message payload and its V1 parts. `None`
/// for the kinds that are not transcript (`system` notices, `idle` markers).
fn as_v1(kind: &str, data: &Map<String, Value>) -> Option<V1Message> {
    let time = data.get("time").cloned().unwrap_or(Value::Null);
    let text = |synthetic: bool| {
        let mut part = object(json!({"type": "text", "text": data.get("text")?.as_str()?}));
        if synthetic {
            part.insert("synthetic".into(), Value::Bool(true));
        }
        Some(part)
    };
    let user = object(json!({"role": "user", "time": time}));
    match kind {
        "user" => Some((user, text(false).into_iter().collect())),
        "synthetic" => Some((user, text(true).into_iter().collect())),
        "compaction" => {
            let auto = data.get("reason").and_then(Value::as_str) == Some("auto");
            Some((
                user,
                vec![object(json!({"type": "compaction", "auto": auto}))],
            ))
        }
        "assistant" => Some(assistant(data, time)),
        _ => None,
    }
}

fn assistant(data: &Map<String, Value>, time: Value) -> V1Message {
    let model = data.get("model").and_then(Value::as_object);
    let field = |key: &str| model.and_then(|model| model.get(key)).cloned();
    let mut envelope = object(json!({
        "role": "assistant",
        "time": time,
        "providerID": field("providerID"),
        "modelID": field("id"),
    }));
    for key in ["tokens", "finish"] {
        if let Some(value) = data.get(key) {
            envelope.insert(key.into(), value.clone());
        }
    }
    let parts = data
        .get("content")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_object)
        .filter_map(content_part)
        .collect();
    (envelope, parts)
}

/// One `content` item as the V1 part: `text` and `reasoning` keep their text
/// (and provider `native` metadata as V1's `metadata`), and a `tool` item
/// becomes a V1 tool part whose `state.output` is the result's text.
fn content_part(item: &Map<String, Value>) -> Option<Map<String, Value>> {
    let kind = item.get("type")?.as_str()?;
    let mut part = match kind {
        "text" | "reasoning" => object(json!({"type": kind, "text": item.get("text")})),
        "tool" => tool_part(item)?,
        _ => return None,
    };
    if let Some(native) = item.get("native") {
        part.insert("metadata".into(), native.clone());
    }
    Some(part)
}

fn tool_part(item: &Map<String, Value>) -> Option<Map<String, Value>> {
    let state = item.get("state").and_then(Value::as_object);
    let get = |key: &str| state.and_then(|state| state.get(key)).cloned();
    let mut v1_state = object(json!({
        "status": get("status"),
        "input": get("input"),
    }));
    if let Some(metadata) = get("metadata") {
        v1_state.insert("metadata".into(), metadata);
    }
    if let Some(output) = state.and_then(result_text) {
        v1_state.insert("output".into(), Value::String(output));
    }
    if let Some(error) = state.and_then(error_message) {
        v1_state.insert("error".into(), error.clone());
    }
    Some(object(json!({
        "type": "tool",
        "callID": item.get("id")?.as_str()?,
        "tool": item.get("name")?.as_str()?,
        "state": v1_state,
    })))
}

/// What the model read back from a finished call, as V1's `state.output`:
/// the result's text items joined, or for a failed call without content its
/// error message. A call still running has no result.
fn result_text(state: &Map<String, Value>) -> Option<String> {
    let status = state.get("status").and_then(Value::as_str);
    if !matches!(status, Some("completed" | "error")) {
        return None;
    }
    let texts: Vec<&str> = state
        .get("content")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|item| item.get("type").and_then(Value::as_str) == Some("text"))
        .filter_map(|item| item.get("text").and_then(Value::as_str))
        .collect();
    if texts.is_empty() && status == Some("error") {
        return error_message(state)
            .and_then(Value::as_str)
            .map(str::to_string);
    }
    Some(texts.join("\n"))
}

/// A failed call's `error.message`, or the `error` value itself when the
/// provider wrote a bare one.
fn error_message(state: &Map<String, Value>) -> Option<&Value> {
    let error = state.get("error")?;
    error.get("message").or(Some(error))
}

fn object(value: Value) -> Map<String, Value> {
    match value {
        Value::Object(map) => map,
        _ => Map::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A store OpenCode 2 has migrated: `old` untouched since, `resumed` with
    /// a newer `session_v2` row, `fresh` only in the OpenCode 2 tables.
    fn migrated_store() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            r#"CREATE TABLE session (id TEXT PRIMARY KEY, parent_id TEXT, directory TEXT,
                                     time_created INTEGER, time_updated INTEGER, title TEXT);
               CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT,
                                     time_created INTEGER, data TEXT);
               CREATE TABLE part (id TEXT PRIMARY KEY, message_id TEXT, session_id TEXT,
                                  time_created INTEGER, data TEXT);
               CREATE TABLE session_v2 (id TEXT PRIMARY KEY, parent_id TEXT, directory TEXT NOT NULL,
                                        title TEXT, time_created INTEGER NOT NULL,
                                        time_updated INTEGER NOT NULL);
               CREATE TABLE session_message (id TEXT PRIMARY KEY, session_id TEXT NOT NULL,
                                             type TEXT NOT NULL, seq INTEGER NOT NULL,
                                             time_created INTEGER NOT NULL,
                                             time_updated INTEGER NOT NULL, data TEXT NOT NULL);
               INSERT INTO session VALUES ('old', NULL, '/w', 1, 10, 'Old'),
                                          ('resumed', NULL, '/w', 1, 10, 'Resumed');
               INSERT INTO session_v2 VALUES ('old', NULL, '/w', 'Old', 1, 10),
                                             ('resumed', NULL, '/w', 'Resumed', 1, 20),
                                             ('fresh', 'old', '/w', 'Fresh', 30, 40);"#,
        )
        .unwrap();
        let rows = [
            (
                "u1",
                "user",
                json!({"time": {"created": 31}, "text": "fix the bug"}),
            ),
            (
                "s1",
                "synthetic",
                json!({"time": {"created": 32}, "text": "harness note"}),
            ),
            (
                "a1",
                "assistant",
                json!({
                    "time": {"created": 33, "completed": 34},
                    "model": {"providerID": "fireworks-ai", "id": "glm-5p3", "variant": "default"},
                    "content": [
                        {"type": "reasoning", "text": "look first"},
                        {"type": "tool", "id": "call-1", "name": "read",
                         "state": {"status": "completed", "input": {"path": "src/lib.rs"},
                                   "content": [{"type": "text", "text": "line one"},
                                               {"type": "text", "text": "line two"}]}},
                        {"type": "tool", "id": "call-2", "name": "shell",
                         "state": {"status": "error", "input": {"command": "false"},
                                   "error": {"type": "tool.execution", "message": "exit 1"}}},
                        {"type": "text", "text": "done"}
                    ],
                    "finish": "stop",
                    "tokens": {"input": 5, "output": 2, "reasoning": 1, "cache": {"read": 0, "write": 0}}
                }),
            ),
            (
                "c1",
                "compaction",
                json!({"time": {"created": 35}, "reason": "auto", "summary": "s"}),
            ),
            (
                "x1",
                "system",
                json!({"time": {"created": 36}, "text": "tools changed"}),
            ),
            (
                "i1",
                "idle",
                json!({"time": {"created": 37}, "outcome": "succeeded"}),
            ),
        ];
        for (seq, (id, kind, data)) in rows.into_iter().enumerate() {
            conn.execute(
                "INSERT INTO session_message VALUES (?, 'fresh', ?, ?, 30, 30, ?)",
                rusqlite::params![id, kind, seq as i64, data.to_string()],
            )
            .unwrap();
        }
        conn
    }

    #[test]
    fn opencode_2_owns_its_own_sessions_and_the_v1_sessions_it_resumed() {
        let conn = migrated_store();
        let mut owned = owned_ids(&conn).unwrap();
        owned.sort_unstable();
        assert_eq!(owned, ["fresh", "resumed"]);
        assert!(!owns(&conn, "old").unwrap());
        let v1: Vec<String> = conn
            .prepare(&format!(
                "SELECT id FROM session WHERE 1{} ORDER BY id",
                v1_exclusion(&conn).unwrap()
            ))
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(v1, ["old"]);
    }

    #[test]
    fn a_store_without_opencode_2_tables_is_v1_only() {
        let conn = migrated_store();
        conn.execute_batch("DROP TABLE session_message").unwrap();
        assert!(!present(&conn).unwrap());
        assert!(owned_ids(&conn).unwrap().is_empty());
        assert_eq!(v1_exclusion(&conn).unwrap(), "");
    }

    #[test]
    fn messages_become_the_v1_payloads_the_parser_reads() {
        let conn = migrated_store();
        let session = super::super::load_from_sqlite(&conn, "fresh")
            .unwrap()
            .unwrap();
        assert_eq!(session.session.parent_id.as_deref(), Some("old"));
        assert_eq!(session.session.title.as_deref(), Some("Fresh"));
        let roles: Vec<&str> = session.messages.iter().map(|m| m.role.as_str()).collect();
        assert_eq!(roles, ["user", "user", "assistant", "user"]);
        assert_eq!(session.first_user_text(), Some("fix the bug"));
        assert_eq!(
            session.first_model().as_deref(),
            Some("fireworks-ai/glm-5p3")
        );

        let assistant = &session.messages[2];
        assert_eq!(assistant.finish.as_deref(), Some("stop"));
        assert_eq!(assistant.tokens.as_ref().unwrap()["input"], 5);
        let parts = session.parts("a1");
        let kinds: Vec<&str> = parts.iter().map(|p| p.kind.as_str()).collect();
        assert_eq!(kinds, ["reasoning", "tool", "tool", "text"]);
        let read = as_tool_part_of(parts, "call-1");
        assert_eq!(read["tool"], "read");
        assert_eq!(read["state"]["output"], "line one\nline two");
        let shell = as_tool_part_of(parts, "call-2");
        assert_eq!(shell["state"]["status"], "error");
        assert_eq!(shell["state"]["error"], "exit 1");
        assert_eq!(shell["state"]["output"], "exit 1");

        assert_eq!(session.parts("s1")[0].raw["synthetic"], true);
        assert_eq!(session.parts("c1")[0].kind, "compaction");
        assert_eq!(session.parts("c1")[0].raw["auto"], true);
    }

    #[test]
    fn a_resumed_session_is_read_from_opencode_2_and_an_untouched_one_from_v1() {
        let conn = migrated_store();
        assert!(super::super::load_from_sqlite(&conn, "resumed")
            .unwrap()
            .unwrap()
            .messages
            .is_empty());
        assert_eq!(
            super::super::load_from_sqlite(&conn, "old")
                .unwrap()
                .unwrap()
                .session
                .title
                .as_deref(),
            Some("Old")
        );
        let all = super::super::load_all_from_sqlite(&conn).unwrap();
        let mut ids: Vec<&str> = all.sessions.iter().map(|s| s.session.id.as_str()).collect();
        ids.sort_unstable();
        assert_eq!(ids, ["fresh", "old", "resumed"]);
        assert!(all.failures.is_empty());
    }

    #[test]
    fn a_store_opencode_2_created_has_no_v1_tables_and_still_reads() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("opencode.db");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE session_v2 (id TEXT PRIMARY KEY, parent_id TEXT, directory TEXT NOT NULL,
                                      title TEXT, time_created INTEGER NOT NULL,
                                      time_updated INTEGER NOT NULL);
             CREATE TABLE session_message (id TEXT PRIMARY KEY, session_id TEXT NOT NULL,
                                           type TEXT NOT NULL, seq INTEGER NOT NULL,
                                           time_created INTEGER NOT NULL,
                                           time_updated INTEGER NOT NULL, data TEXT NOT NULL);
             INSERT INTO session_v2 VALUES ('only', NULL, '/w', 'Only', 1, 2);
             INSERT INTO session_message VALUES
               ('u1', 'only', 'user', 0, 1, 1, '{\"time\":{\"created\":1},\"text\":\"hi\"}');",
        )
        .unwrap();
        assert!(super::super::sqlite_store_holds_session(&path, "only").unwrap());
        assert!(!super::super::sqlite_store_holds_session(&path, "missing").unwrap());
        let loaded = super::super::load_from_sqlite(&conn, "only")
            .unwrap()
            .unwrap();
        assert_eq!(loaded.first_user_text(), Some("hi"));
        let all = super::super::load_all_from_sqlite(&conn).unwrap();
        assert_eq!(all.sessions.len(), 1);
    }

    fn as_tool_part_of<'a>(parts: &'a [OpencodePart], call: &str) -> &'a Map<String, Value> {
        &parts
            .iter()
            .find(|part| part.raw.get("callID").and_then(Value::as_str) == Some(call))
            .unwrap()
            .raw
    }
}

//! OpenCode 2's session tables, `session_v2` and `session_message`, in the
//! same `opencode.db`.
//!
//! OpenCode 2 copies every V1 session, messages and parts included, into
//! these tables the first time it opens a store, and from then on writes only
//! here. So a session with a `session_v2` row is read from OpenCode 2, and any
//! other session (a store OpenCode 2 has not finished migrating) from the V1
//! `session`, `message` and `part` tables. [`Layout`] makes that choice, so
//! discovery, hydration and the sweep never decide it themselves.
//!
//! The loader here rewrites each OpenCode 2 message into the V1 message and
//! part payloads, so [`super::normalize`] stays the one parser for every
//! layout.

use super::OpencodeSession;
use super::OpencodeSessionInfo;
use super::{finish, has_columns, parse_message, table_columns, OpencodeMessage, OpencodePart};
use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;
use std::path::Path;

/// A V1 message payload and its V1 parts.
type V1Message = (Map<String, Value>, Vec<Map<String, Value>>);

const SESSION_COLUMNS: &[&str] = &["id", "directory", "time_created", "time_updated"];
const MESSAGE_COLUMNS: &[&str] = &["id", "session_id", "type", "seq", "time_created", "data"];

/// Which session tables a store has, read once per open connection.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Layout {
    v1: bool,
    v2: bool,
}

/// The tables one session is read from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Envelope {
    V1,
    V2,
}

impl Layout {
    pub(crate) fn detect(src: &Connection) -> Result<Self> {
        Ok(Self {
            v1: table_columns(src, "session")?.contains("id"),
            v2: has_columns(&table_columns(src, "session_v2")?, SESSION_COLUMNS)
                && has_columns(&table_columns(src, "session_message")?, MESSAGE_COLUMNS),
        })
    }

    pub(crate) fn has_v1(self) -> bool {
        self.v1
    }

    pub(crate) fn has_v2(self) -> bool {
        self.v2
    }

    /// Where `session_id` is read from, or `None` when the store does not
    /// hold it.
    pub(crate) fn envelope(self, src: &Connection, session_id: &str) -> Result<Option<Envelope>> {
        let held = |table: &str| -> Result<bool> {
            let sql = format!("SELECT 1 FROM {table} WHERE id = ?");
            Ok(src.prepare_cached(&sql)?.exists([session_id])?)
        };
        if self.v2 && held("session_v2")? {
            return Ok(Some(Envelope::V2));
        }
        Ok((self.v1 && held("session")?).then_some(Envelope::V1))
    }

    /// The V1 sessions not read from OpenCode 2, selecting the columns the V1
    /// loaders read; `None` when the store has no V1 `session` table.
    pub(crate) fn v1_sessions_sql(self, src: &Connection) -> Result<Option<String>> {
        if !self.v1 {
            return Ok(None);
        }
        let columns = OpencodeSessionInfo::columns(&table_columns(src, "session")?);
        Ok(Some(format!(
            "SELECT {columns} FROM session WHERE id IS NOT NULL AND id <> ''{}",
            self.v1_filter()
        )))
    }

    /// The bulk scan of the V1 `message` table, leaving out the sessions read
    /// from OpenCode 2 so their V1 rows can neither load nor fail.
    pub(crate) fn v1_messages_sql(self, fallback_created: &str) -> String {
        let skip = match self.v2 {
            true => " AND NOT EXISTS (SELECT 1 FROM session_v2 v WHERE v.id = message.session_id)",
            false => "",
        };
        format!("SELECT id, session_id, data, {fallback_created} FROM message WHERE json_valid(data){skip}")
    }

    /// The clause a query over the V1 `session` table appends so it leaves
    /// out the sessions read from OpenCode 2.
    pub(crate) fn v1_filter(self) -> &'static str {
        match self.v2 {
            true => " AND NOT EXISTS (SELECT 1 FROM session_v2 v WHERE v.id = session.id)",
            false => "",
        }
    }
}

/// Whether the OpenCode store at `store` holds `session_id` in either
/// layout: the question that decides which channel store owns a session
/// present in several.
pub(crate) fn store_holds(store: &Path, session_id: &str) -> Result<bool> {
    let src = Connection::open_with_flags(
        store,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_URI,
    )
    .with_context(|| format!("opening {}", store.display()))?;
    src.busy_timeout(std::time::Duration::from_secs(5))?;
    Ok(Layout::detect(&src)?.envelope(&src, session_id)?.is_some())
}

/// `SELECT <columns> FROM session_v2 v` over every OpenCode 2 session.
pub(crate) fn sessions_sql(columns: &str) -> String {
    format!("SELECT {columns} FROM session_v2 v WHERE v.id IS NOT NULL AND v.id <> ''")
}

/// The ids of every OpenCode 2 session.
fn session_ids(src: &Connection) -> Result<Vec<String>> {
    let mut stmt = src.prepare(&sessions_sql("v.id"))?;
    let ids = stmt
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(ids)
}

/// The session's title, unless it is still OpenCode's placeholder.
pub(crate) fn title(src: &Connection, session_id: &str) -> Result<Option<String>> {
    let raw: Option<String> = src
        .prepare_cached("SELECT title FROM session_v2 WHERE id = ?")?
        .query_row([session_id], |row| row.get(0))
        .optional()?
        .flatten();
    Ok(crate::ingest::titles::opencode_title(raw.as_deref()))
}

/// The first `user` message's text, cut in SQL to `max_chars`, skipping
/// messages that are only `trim` characters. `session_message` is indexed by
/// session, so this is one seek.
pub(crate) fn first_prompt(
    src: &Connection,
    session_id: &str,
    max_chars: usize,
    trim: &str,
) -> Result<Option<String>> {
    let max_chars = i64::try_from(max_chars).unwrap_or(i64::MAX);
    Ok(src
        .prepare_cached(
            "SELECT substr(json_extract(data, '$.text'), 1, ?) FROM session_message \
             WHERE session_id = ? AND type = 'user' AND json_valid(data) \
             AND json_type(data, '$.text') = 'text' \
             AND trim(substr(json_extract(data, '$.text'), 1, ?), ?) <> '' \
             ORDER BY seq ASC LIMIT 1",
        )?
        .query_row(params![max_chars, session_id, max_chars, trim], |row| {
            row.get::<_, String>(0)
        })
        .optional()?)
}

/// The first assistant message's model, which OpenCode 2 nests as
/// `{providerID, id}`.
pub(crate) fn first_model(src: &Connection, session_id: &str) -> Result<Option<String>> {
    Ok(src
        .prepare_cached(
            "SELECT json_extract(data, '$.model.providerID'), json_extract(data, '$.model.id') \
             FROM session_message WHERE session_id = ? AND type = 'assistant' \
             AND json_valid(data) \
             AND (NULLIF(json_extract(data, '$.model.providerID'), '') IS NOT NULL \
                  OR NULLIF(json_extract(data, '$.model.id'), '') IS NOT NULL) \
             ORDER BY seq ASC LIMIT 1",
        )?
        .query_row([session_id], |row| {
            Ok((
                row.get::<_, Option<String>>(0)?,
                row.get::<_, Option<String>>(1)?,
            ))
        })
        .optional()?
        .and_then(|(provider, model)| super::build_model(provider.as_deref(), model.as_deref())))
}

/// Add every OpenCode 2 session to a whole-store load. `session_message` is
/// indexed by session, so they are read one at a time on either plan.
pub(crate) fn load_all(
    src: &Connection,
    layout: Layout,
    mut load: super::OpencodeStoreLoad,
) -> Result<super::OpencodeStoreLoad> {
    if !layout.has_v2() {
        return Ok(load);
    }
    for session_id in session_ids(src)? {
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

/// Read one OpenCode 2 session. `None` when the store has no such session.
pub(crate) fn load(src: &Connection, session_id: &str) -> Result<Option<OpencodeSession>> {
    super::super::check_capture_cancelled()?;
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

    /// A store OpenCode 2 is migrating: `old` not copied yet, `migrated` in
    /// both layouts with different content (as a session resumed under
    /// OpenCode 2 is, plus a V1 row no loader can map), `fresh` only in the
    /// OpenCode 2 tables.
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
                                          ('migrated', NULL, '/w', 1, 10, 'Before');
               INSERT INTO message VALUES
                 ('om', 'old', 2, '{"role":"user","time":{"created":2}}'),
                 ('mm', 'migrated', 2, '{"role":"user","time":{"created":2}}'),
                 (NULL, 'migrated', 3, '{"role":"user","time":{"created":3}}');
               INSERT INTO part VALUES
                 ('op', 'om', 'old', 2, '{"type":"text","text":"old prompt"}'),
                 ('mp', 'mm', 'migrated', 2, '{"type":"text","text":"v1 prompt"}');
               INSERT INTO session_v2 VALUES ('migrated', NULL, '/w', 'After', 1, 20),
                                             ('fresh', 'old', '/w', 'Fresh', 30, 40);
               INSERT INTO session_message VALUES
                 ('mu', 'migrated', 'user', 0, 2, 2, '{"time":{"created":2},"text":"v2 prompt"}'),
                 ('mu2', 'migrated', 'user', 1, 15, 15, '{"time":{"created":15},"text":"resumed turn"}');"#,
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
    fn a_session_with_a_session_v2_row_is_read_from_opencode_2() {
        let conn = migrated_store();
        let layout = Layout::detect(&conn).unwrap();
        let envelope = |id| layout.envelope(&conn, id).unwrap();
        assert_eq!(envelope("old"), Some(Envelope::V1));
        assert_eq!(envelope("migrated"), Some(Envelope::V2));
        assert_eq!(envelope("fresh"), Some(Envelope::V2));
        assert_eq!(envelope("missing"), None);
        let v1: Vec<String> = conn
            .prepare(&format!(
                "SELECT id FROM session WHERE 1{} ORDER BY id",
                layout.v1_filter()
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
        let layout = Layout::detect(&conn).unwrap();
        assert!(!layout.has_v2());
        assert_eq!(layout.v1_filter(), "");
        assert_eq!(
            layout.envelope(&conn, "migrated").unwrap(),
            Some(Envelope::V1)
        );
        assert_eq!(layout.envelope(&conn, "fresh").unwrap(), None);
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
    fn a_session_in_both_layouts_reads_its_opencode_2_content() {
        let conn = migrated_store();
        let migrated = super::super::load_from_sqlite(&conn, "migrated")
            .unwrap()
            .unwrap();
        assert_eq!(migrated.session.title.as_deref(), Some("After"));
        assert_eq!(migrated.first_user_text(), Some("v2 prompt"));
        assert_eq!(migrated.messages.len(), 2);
        assert_eq!(
            first_prompt(&conn, "migrated", 4096, " ")
                .unwrap()
                .as_deref(),
            Some("v2 prompt")
        );
        assert_eq!(title(&conn, "migrated").unwrap().as_deref(), Some("After"));
        assert_eq!(
            first_model(&conn, "fresh").unwrap().as_deref(),
            Some("fireworks-ai/glm-5p3")
        );

        let old = super::super::load_from_sqlite(&conn, "old")
            .unwrap()
            .unwrap();
        assert_eq!(old.first_user_text(), Some("old prompt"));

        let all = super::super::load_all_from_sqlite(&conn).unwrap();
        let mut texts: Vec<(&str, Option<&str>)> = all
            .sessions
            .iter()
            .map(|s| (s.session.id.as_str(), s.first_user_text()))
            .collect();
        texts.sort_unstable();
        assert_eq!(
            texts,
            [
                ("fresh", Some("fix the bug")),
                ("migrated", Some("v2 prompt")),
                ("old", Some("old prompt")),
            ]
        );
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

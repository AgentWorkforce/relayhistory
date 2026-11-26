//! Synthetic editor databases in Cursor's layout, for tests. No real data.
use std::path::{Path, PathBuf};

use rusqlite::Connection;
use serde_json::{json, Value};

use super::state_db_path;

/// Creates (or reopens) `<home>/<Cursor user dir>/globalStorage/state.vscdb`
/// with Cursor's tables, in WAL mode as Cursor keeps it.
pub(crate) fn open_state_db(home: &Path) -> anyhow::Result<(PathBuf, Connection)> {
    let path = state_db_path(home);
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let conn = Connection::open(&path)?;
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         CREATE TABLE IF NOT EXISTS ItemTable (key TEXT UNIQUE ON CONFLICT REPLACE, value BLOB);
         CREATE TABLE IF NOT EXISTS cursorDiskKV (key TEXT UNIQUE ON CONFLICT REPLACE, value BLOB);",
    )?;
    Ok((path, conn))
}

/// Writes one key the way Cursor does: an insert that replaces.
pub(crate) fn put(conn: &Connection, key: &str, value: &Value) -> anyhow::Result<()> {
    conn.execute(
        "INSERT INTO cursorDiskKV (key, value) VALUES (?1, ?2)",
        rusqlite::params![key, value.to_string()],
    )?;
    Ok(())
}

/// The ISO 8601 time Cursor writes on a bubble; a time chrono cannot
/// represent is written as the epoch number older builds use.
pub(crate) fn iso(ms: i64) -> Value {
    chrono::DateTime::from_timestamp_millis(ms).map_or(json!(ms), |at| {
        json!(at.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string())
    })
}

pub(crate) fn user_bubble(id: &str, text: &str, at: i64) -> Value {
    json!({"_v": 3, "type": 1, "bubbleId": id, "text": text, "createdAt": iso(at)})
}

pub(crate) fn reply_bubble(id: &str, text: &str, at: i64) -> Value {
    json!({"_v": 3, "type": 2, "bubbleId": id, "text": text, "createdAt": iso(at)})
}

pub(crate) fn thinking_bubble(id: &str, thinking: &str, at: i64) -> Value {
    json!({"_v": 3, "type": 2, "bubbleId": id, "text": "", "capabilityType": 30,
           "thinking": {"text": thinking, "signature": ""}, "createdAt": iso(at)})
}

pub(crate) fn tool_bubble(
    id: &str,
    name: &str,
    call: &str,
    params: Value,
    result: Value,
    at: i64,
) -> Value {
    json!({"_v": 3, "type": 2, "bubbleId": id, "text": "", "capabilityType": 15,
           "createdAt": iso(at), "completedAtMs": at + 500,
           "toolFormerData": {"tool": 15, "toolCallId": call, "status": "completed", "name": name,
                              "rawArgs": "", "params": params.to_string(), "result": result.to_string()}})
}

/// A short editor conversation: a prompt, thinking, a file read with its
/// result, an edit, and a reply.
pub(crate) fn tool_conversation(prompt: &str, start: i64) -> Vec<Value> {
    vec![
        user_bubble("b-user", prompt, start),
        thinking_bubble("b-user/s:0", "Read the note first.", start + 1_000),
        tool_bubble(
            "b-user/t:call-1",
            "read_file_v2",
            "call-1",
            json!({"targetFile": "/work/app/note.txt"}),
            json!({"contents": "hello\n"}),
            start + 2_000,
        ),
        tool_bubble(
            "b-user/t:call-2",
            "edit_file_v2",
            "call-2",
            json!({"relativeWorkspacePath": "/work/app/note.txt"}),
            json!({"afterContentId": "x"}),
            start + 3_000,
        ),
        reply_bubble("b-user/s:2", "note.txt says hello", start + 4_000),
    ]
}

/// Writes a composer, its bubbles and its header chunk, as current builds do.
pub(crate) fn write_composer(
    conn: &Connection,
    id: &str,
    name: &str,
    cwd: &str,
    bubbles: &[Value],
    created_ms: i64,
) -> anyhow::Result<()> {
    for bubble in bubbles {
        let Some(bubble_id) = bubble["bubbleId"].as_str() else {
            anyhow::bail!("a test bubble needs a bubbleId: {bubble}");
        };
        put(conn, &format!("bubbleId:{id}:{bubble_id}"), bubble)?;
    }
    let headers: Vec<Value> = bubbles
        .iter()
        .map(|bubble| json!({"bubbleId": bubble["bubbleId"], "type": bubble["type"]}))
        .collect();
    if !headers.is_empty() {
        put(
            conn,
            &format!("composerConvHeaders:{id}:000000"),
            &Value::Array(headers),
        )?;
    }
    let mut data = json!({
        "_v": 18,
        "composerId": id,
        "name": name,
        "createdAt": created_ms,
        "fullConversationHeadersOnly": [],
        "conversationMap": {},
        "headerLog": {"chunkSize": 256, "chunkCount": 1, "count": bubbles.len()},
    });
    if !cwd.is_empty() {
        data["workspaceIdentifier"] = json!({
            "id": "0123456789abcdef0123456789abcdef",
            "uri": {"$mid": 1, "fsPath": cwd, "external": format!("file://{cwd}"), "path": cwd, "scheme": "file"}
        });
    }
    if let Some(last) = bubbles
        .last()
        .and_then(|b| b["createdAt"].as_str())
        .and_then(crate::parse_iso_ms)
    {
        data["lastUpdatedAt"] = json!(last);
    }
    put(conn, &format!("composerData:{id}"), &data)
}

/// Checkpoints and closes the database, as Cursor leaves it when it quits:
/// no `-wal` or `-shm` beside it.
pub(crate) fn close_idle(path: &Path, conn: Connection) -> anyhow::Result<()> {
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
    drop(conn);
    let _ = std::fs::remove_file(crate::ingest::cursor_store::sidecar(path, "-wal"));
    let _ = std::fs::remove_file(crate::ingest::cursor_store::sidecar(path, "-shm"));
    Ok(())
}

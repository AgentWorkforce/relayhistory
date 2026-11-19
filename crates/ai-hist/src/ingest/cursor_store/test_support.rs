//! Synthetic chat stores in Cursor's layout, for tests.
use std::path::{Path, PathBuf};

use rusqlite::Connection;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use super::read::encode_hex;
use super::{sidecar, CHATS_DIR, STORE_FILE};

fn put_varint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

fn put_bytes(out: &mut Vec<u8>, field: u64, bytes: &[u8]) {
    put_varint(out, field << 3 | 2);
    put_varint(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

/// Inserts `data` as a content-addressed blob and returns its id.
fn put_blob(conn: &Connection, data: &[u8]) -> rusqlite::Result<[u8; 32]> {
    let digest: [u8; 32] = Sha256::digest(data).into();
    conn.execute(
        "INSERT OR IGNORE INTO blobs (id, data) VALUES (?1, ?2)",
        rusqlite::params![encode_hex(&digest), data],
    )?;
    Ok(digest)
}

/// Writes `<home>/.cursor/chats/<workspace>/<id>/store.db` holding
/// `messages` in order, and returns its path.
pub(crate) fn write_store(
    home: &Path,
    workspace: &str,
    id: &str,
    cwd: &str,
    messages: &[Value],
    updated_ms: u64,
) -> anyhow::Result<PathBuf> {
    let dir = home.join(CHATS_DIR).join(workspace).join(id);
    std::fs::create_dir_all(&dir)?;
    let path = dir.join(STORE_FILE);
    let _ = std::fs::remove_file(&path);
    let conn = Connection::open(&path)?;
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         CREATE TABLE blobs (id TEXT PRIMARY KEY, data BLOB);
         CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);",
    )?;
    let mut root = Vec::new();
    for message in messages {
        let digest = put_blob(&conn, message.to_string().as_bytes())?;
        put_bytes(&mut root, 1, &digest);
    }
    put_bytes(&mut root, 9, format!("file://{cwd}").as_bytes());
    let mut nested = Vec::new();
    put_bytes(&mut nested, 1, cwd.as_bytes());
    put_bytes(&mut root, 21, &nested);
    put_bytes(&mut root, 22, b"cli");
    put_varint(&mut root, 26 << 3);
    put_varint(&mut root, updated_ms);
    let root_id = encode_hex(&put_blob(&conn, &root)?);
    let meta = json!({
        "agentId": id,
        "latestRootBlobId": root_id,
        "name": "New Agent",
        "mode": "default",
        "createdAt": updated_ms.saturating_sub(1_000),
    });
    conn.execute(
        "INSERT INTO meta (key, value) VALUES ('0', ?1)",
        [encode_hex(meta.to_string().as_bytes())],
    )?;
    // Close cleanly, as Cursor does when a chat goes idle: no -wal or
    // -shm is left behind.
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
    drop(conn);
    let _ = std::fs::remove_file(sidecar(&path, "-wal"));
    let _ = std::fs::remove_file(sidecar(&path, "-shm"));
    Ok(path)
}

/// A short chat in the shape Cursor stores: system prompt, environment
/// preamble, a framed prompt, a tool call, its result and a reply.
pub(crate) fn tool_chat(prompt: &str) -> Vec<Value> {
    vec![
        json!({"role": "system", "content": "You are an AI coding assistant."}),
        json!({"role": "user", "content": "<user_info>\nOS Version: test\n</user_info>"}),
        json!({"role": "user", "content": [{"type": "text", "text": format!(
            "<timestamp>Wednesday, Nov 18, 2026, 11:52 PM (UTC+1)</timestamp>\n<user_query>\n{prompt}\n</user_query>"
        )}], "providerOptions": {"cursor": {"requestId": "r-1"}}}),
        json!({"role": "assistant", "content": [{"type": "tool-call", "toolCallId": "call-1", "toolName": "Read", "args": {"path": "/work/app/note.txt"}}]}),
        json!({"role": "tool", "content": [{"type": "tool-result", "toolCallId": "call-1", "toolName": "Read", "result": "hello\n"}]}),
        json!({"role": "assistant", "content": [{"type": "text", "text": "note.txt says hello"}], "id": "1"}),
    ]
}

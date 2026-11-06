//! Atomic complete-session acquisition and native order capture.
use super::*;

pub(crate) fn normalize(
    conn: &Connection,
    loaded: &OpencodeSession,
    raw_path: &str,
) -> Result<OpencodeIngestCounts> {
    // One session, one atomic replacement. Reading a session is a read of its
    // *current* files, so it has to end with the database agreeing — and the
    // half-state between "the retired rows are gone" and "the new ones are
    // written" must never be visible to a reader.
    //
    // A savepoint rather than a transaction, because the callers differ:
    // hydration already holds one (a nested `BEGIN` would fail), global sync
    // does not. A savepoint nests either way.
    conn.execute_batch("SAVEPOINT ai_hist_opencode_session")?;
    let result = (|| {
        let order = super::super::native_order::begin(conn, "opencode", Some(&loaded.session.id))?;
        let counts = normalize_session(conn, loaded, raw_path)?;
        order.finish()?;
        Ok(counts)
    })();
    if result.is_ok() {
        conn.execute_batch("RELEASE ai_hist_opencode_session")?;
    } else {
        let _ = conn.execute_batch(
            "ROLLBACK TO ai_hist_opencode_session; RELEASE ai_hist_opencode_session;",
        );
    }
    result
}

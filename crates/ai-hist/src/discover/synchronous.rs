use anyhow::Result;
use rusqlite::Connection;

/// Scoped `PRAGMA synchronous = NORMAL` for one discovery write transaction.
///
/// Nesting on one connection avoids redundant pragma reads and writes. The
/// outer guard restores the previous policy before any callback can write.
pub(crate) struct RelaxedSynchronous<'a> {
    conn: &'a Connection,
    previous: Option<i64>,
}

thread_local! {
    static STACK: std::cell::RefCell<Vec<usize>> = const { std::cell::RefCell::new(Vec::new()) };
}

impl<'a> RelaxedSynchronous<'a> {
    pub(crate) fn new(conn: &'a Connection) -> Result<Self> {
        let identity = conn as *const Connection as usize;
        if STACK.with(|stack| stack.borrow().last().copied() == Some(identity)) {
            STACK.with(|stack| stack.borrow_mut().push(identity));
            return Ok(Self {
                conn,
                previous: None,
            });
        }
        let previous = conn.query_row("PRAGMA synchronous", [], |row| row.get(0))?;
        let previous = if previous == 1 {
            None
        } else {
            conn.pragma_update(None, "synchronous", "NORMAL")?;
            Some(previous)
        };
        STACK.with(|stack| stack.borrow_mut().push(identity));
        Ok(Self { conn, previous })
    }
}

impl Drop for RelaxedSynchronous<'_> {
    fn drop(&mut self) {
        let identity = self.conn as *const Connection as usize;
        STACK.with(|stack| debug_assert_eq!(stack.borrow_mut().pop(), Some(identity)));
        // Best effort: a connection that cannot take the pragma any more is
        // being torn down anyway.
        if let Some(previous) = self.previous {
            let _ = self.conn.pragma_update(None, "synchronous", previous);
        }
    }
}

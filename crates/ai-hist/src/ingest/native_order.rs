//! Source order learned only while a caller replays a complete native source.
use super::EventRow;
use anyhow::Result;
use rusqlite::{Connection, Transaction};
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;

#[derive(Default)]
struct Events {
    positions: HashMap<String, usize>,
    ordered: Vec<(i64, String)>,
}
struct Frame {
    id: Rc<()>,
    conn: usize,
    source: String,
    session: Option<String>,
    groups: BTreeMap<String, Events>,
    complete: bool,
}
thread_local! {
    static FRAMES: RefCell<Vec<Frame>> = const { RefCell::new(Vec::new()) };
}

pub(crate) struct Capture<'a> {
    id: Rc<()>,
    conn: &'a Connection,
    source: String,
    session: Option<String>,
    transaction: Option<Transaction<'a>>,
    settled: bool,
}

/// Arm only for complete source reads, before any payload is normalized.
/// An existing caller transaction is reused; otherwise this scope owns one.
pub(crate) fn begin<'a>(
    conn: &'a Connection,
    source: &str,
    session: Option<&str>,
) -> Result<Capture<'a>> {
    let transaction = conn
        .is_autocommit()
        .then(|| conn.unchecked_transaction())
        .transpose()?;
    let id = Rc::new(());
    FRAMES.with(|frames| {
        frames.borrow_mut().push(Frame {
            id: id.clone(),
            conn: std::ptr::from_ref(conn) as usize,
            source: source.into(),
            session: session.map(str::to_owned),
            groups: BTreeMap::new(),
            complete: true,
        })
    });
    Ok(Capture {
        id,
        conn,
        source: source.into(),
        session: session.map(str::to_owned),
        transaction,
        settled: false,
    })
}

pub(super) struct CodexRead<'a> {
    pub reader: super::transcript_cursor::TranscriptReader,
    pub gate: super::codex::ForkReplayGate,
    pub probe_bytes: u64,
    pub order: Option<Capture<'a>>,
}

pub(super) fn codex_read<'a>(
    conn: &'a Connection,
    path: &std::path::Path,
    cursor: &mut super::transcript_cursor::TranscriptCursorState,
    meta: &super::CodexSessionMeta,
) -> Result<CodexRead<'a>> {
    let (reader, gate, probe_bytes) =
        super::rollout_head::open_codex_walk(conn, path, cursor, meta)?;
    let order = optional(
        conn,
        "codex",
        Some(&meta.session_id),
        reader.start_offset() == 0,
    )?;
    Ok(CodexRead {
        reader,
        gate,
        probe_bytes,
        order,
    })
}

pub(super) fn optional<'a>(
    conn: &'a Connection,
    source: &str,
    session: Option<&str>,
    complete: bool,
) -> Result<Option<Capture<'a>>> {
    complete.then(|| begin(conn, source, session)).transpose()
}

pub(super) fn finish_codex(
    capture: Option<Capture<'_>>,
    pass: &super::transcript_cursor::IncrementalPass,
    committed: u64,
    read: u64,
    tail: u64,
) -> Result<()> {
    if let Some(capture) = capture {
        if !pass.superseded && committed == read && tail == 0 {
            capture.finish()?;
        } else {
            capture.discard()?;
        }
    }
    Ok(())
}

/// Called only after a normalized event write succeeded. Repeated upserts of
/// the same canonical event retain its first native position, while its last
/// recorded timestamp follows the canonical payload.
pub(super) fn note(conn: &Connection, event: &EventRow<'_>) {
    FRAMES.with(|frames| {
        for frame in frames.borrow_mut().iter_mut().filter(|frame| {
            frame.conn == std::ptr::from_ref(conn) as usize
                && frame.source == event.source
                && frame
                    .session
                    .as_deref()
                    .is_none_or(|sid| sid == event.session_id)
        }) {
            let group = frame.groups.entry(event.session_id.into()).or_default();
            if let Some(&index) = group.positions.get(event.event_uid) {
                group.ordered[index].0 = event.ts_ms;
            } else {
                group
                    .positions
                    .insert(event.event_uid.into(), group.ordered.len());
                group.ordered.push((event.ts_ms, event.event_uid.into()));
            }
        }
    });
}

/// Forget only a parser's explicitly retired local event share. Do not filter
/// against database membership at finish: an unexplained disappearance or a
/// moved timestamp must still reject the complete-read observation.
pub(super) fn forget(conn: &Connection, source: &str, session: &str, uids: &[String]) {
    if uids.is_empty() {
        return;
    }
    FRAMES.with(|frames| {
        for frame in frames.borrow_mut().iter_mut().filter(|frame| {
            frame.conn == std::ptr::from_ref(conn) as usize
                && frame.source == source
                && frame.session.as_deref().is_none_or(|sid| sid == session)
        }) {
            if let Some(group) = frame.groups.get_mut(session) {
                group.ordered.retain(|(_, uid)| !uids.contains(uid));
                group.positions = group
                    .ordered
                    .iter()
                    .enumerate()
                    .map(|(index, (_, uid))| (uid.clone(), index))
                    .collect();
            }
        }
    });
}

impl Capture<'_> {
    fn take(&self) -> (BTreeMap<String, Events>, bool) {
        FRAMES.with(|frames| {
            let mut frames = frames.borrow_mut();
            let Some(index) = frames
                .iter()
                .position(|frame| Rc::ptr_eq(&frame.id, &self.id))
            else {
                return (BTreeMap::new(), false);
            };
            let frame = frames.remove(index);
            (frame.groups, frame.complete)
        })
    }
    fn poison(&self) {
        // A complete outer wrapper must not certify a nested partial read.
        FRAMES.with(|frames| {
            for frame in frames.borrow_mut().iter_mut().filter(|frame| {
                frame.conn == std::ptr::from_ref(self.conn) as usize
                    && frame.source == self.source
                    && frame
                        .session
                        .as_deref()
                        .zip(self.session.as_deref())
                        .is_none_or(|(left, right)| left == right)
            }) {
                frame.complete = false;
            }
        });
    }
    pub(crate) fn discard(mut self) -> Result<()> {
        self.poison();
        self.take();
        if let Some(transaction) = self.transaction.take() {
            transaction.commit()?;
        }
        self.settled = true;
        Ok(())
    }
    pub(crate) fn finish(mut self) -> Result<()> {
        let (groups, complete) = self.take();
        for (sid, events) in groups.into_iter().filter(|_| complete) {
            crate::providers::file_history::reconcile_event_order(
                self.conn,
                &self.source,
                &sid,
                &events.ordered,
            )?;
        }
        if let Some(transaction) = self.transaction.take() {
            transaction.commit()?;
        }
        self.settled = true;
        Ok(())
    }
}
impl Drop for Capture<'_> {
    fn drop(&mut self) {
        if !self.settled {
            self.poison();
        }
        self.take();
    }
}

#[cfg(test)]
mod tests;

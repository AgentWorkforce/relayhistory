//! Cache archive eligibility for the lifetime of one source-wide writer lease.
//! Feed revisions let subsequent files inspect only sessions changed by earlier
//! files, including deletions that can leave sparse surviving ranks.
use anyhow::Result;
use rusqlite::Connection;
use std::collections::BTreeSet;

#[cfg(test)]
thread_local! { static LOADS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }

const CONSTRAINED: &str = "COUNT(source_order)!=COUNT(*) OR SUM(location!='local')>0 OR MIN(source_order)!=0 OR MAX(source_order)>=COUNT(*) OR COUNT(DISTINCT source_order)!=COUNT(*)";

pub(super) struct Eligibility {
    head: i64,
    constrained: BTreeSet<String>,
}
impl Eligibility {
    pub(super) fn load(conn: &Connection) -> Result<Self> {
        #[cfg(test)]
        LOADS.with(|loads| loads.set(loads.get() + 1));
        let constrained = conn.prepare(&format!("SELECT DISTINCT session_id FROM session_events WHERE source='claude' GROUP BY session_id,ts_ms HAVING {CONSTRAINED}"))?
            .query_map([], |row| row.get(0))?.collect::<rusqlite::Result<_>>()?;
        Ok(Self {
            head: head(conn)?,
            constrained,
        })
    }

    pub(super) fn refresh(&mut self, conn: &Connection) -> Result<()> {
        let head = head(conn)?;
        if head < self.head {
            *self = Self::load(conn)?;
            return Ok(());
        }
        if head == self.head {
            return Ok(());
        }
        let changed: Vec<String> = conn.prepare("SELECT DISTINCT session_id FROM session_events INDEXED BY idx_session_events_revision WHERE revision>?1 AND source='claude' UNION SELECT session_id FROM evidence_tombstones WHERE kind='session_event' AND revision>?1 AND source='claude'")?
            .query_map([self.head], |row| row.get(0))?.collect::<rusqlite::Result<_>>()?;
        for session in changed {
            if constrained(conn, &session)? {
                self.constrained.insert(session);
            } else {
                self.constrained.remove(&session);
            }
        }
        self.head = head;
        Ok(())
    }

    pub(super) fn any(&self) -> bool {
        !self.constrained.is_empty()
    }
}

fn head(conn: &Connection) -> Result<i64> {
    Ok(conn.query_row(
        "SELECT version FROM observation_clock WHERE singleton=1",
        [],
        |row| row.get(0),
    )?)
}

pub(super) fn constrained(conn: &Connection, session: &str) -> Result<bool> {
    Ok(conn.query_row(&format!("SELECT EXISTS(SELECT 1 FROM session_events WHERE source='claude' AND session_id=?1 GROUP BY ts_ms HAVING {CONSTRAINED})"), [session], |row| row.get(0))?)
}

#[cfg(test)]
mod tests;

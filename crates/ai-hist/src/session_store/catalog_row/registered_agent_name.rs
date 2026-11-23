//! Registered agent names: the name an agent was registered under, recorded on
//! a catalogued session so the change feed carries it as `agent_name`.

use super::super::{open_db, Error, SessionStore, Source};

impl SessionStore {
    /// Record the name an agent was *registered* under (for example the name
    /// an Agent Relay worker was spawned with) on a catalogued session, so
    /// the change feed carries it as the session's `agent_name`.
    ///
    /// The provider's own name wins. A name the provider recorded (Claude's
    /// `agent-name`) is never replaced; a registered name only fills a
    /// session whose `agent_name` is empty, or one whose `agent_name` is the
    /// name this method set before (a rename). `None` clears a name this
    /// method set and leaves a provider name alone. The registered name is
    /// remembered beside the catalog, so a rename or clear can tell its own
    /// value from the provider's.
    ///
    /// A real change is an ordinary write of `sessions.agent_name`: the
    /// catalog's update trigger stamps the row at a new revision, so the
    /// change feed reports it like any other metadata change and a consumer
    /// cursor drains it. Returns whether the effective `agent_name` changed;
    /// setting the name already in effect is a no-op returning `false`.
    ///
    /// Errors: [`Error::InvalidArgument`] for an empty name, one over 128
    /// characters, or one with a control character;
    /// [`Error::SessionNotFound`] when the catalog holds no such session;
    /// [`Error::UnsupportedOperation`] on a read-only store.
    pub fn set_registered_agent_name(
        &self,
        source: Source,
        session_id: &str,
        name: Option<&str>,
    ) -> Result<bool, Error> {
        if self.read_only {
            return Err(Error::read_only("set_registered_agent_name"));
        }
        let name = match name {
            Some(raw) => Some(registered_agent_name(raw)?),
            None => None,
        };
        let mut conn = open_db(&self.db_path).map_err(Error::query)?;
        let tx = conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(Error::sql)?;
        tx.execute_batch(
            "CREATE TABLE IF NOT EXISTS registered_agent_names (
                 source TEXT NOT NULL,
                 session_id TEXT NOT NULL,
                 name TEXT NOT NULL,
                 PRIMARY KEY (source, session_id)
             )",
        )
        .map_err(Error::sql)?;
        let source_name = source.as_str();
        let current: Option<Option<String>> = {
            use rusqlite::OptionalExtension;
            tx.query_row(
                "SELECT agent_name FROM sessions WHERE source = ?1 AND session_id = ?2",
                rusqlite::params![source_name, session_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(Error::sql)?
        };
        let Some(current) = current else {
            return Err(Error::SessionNotFound(format!(
                "{source_name} session {session_id} is not catalogued"
            )));
        };
        let previous: Option<String> = {
            use rusqlite::OptionalExtension;
            tx.query_row(
                "SELECT name FROM registered_agent_names WHERE source = ?1 AND session_id = ?2",
                rusqlite::params![source_name, session_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(Error::sql)?
        };
        // Ours to change: empty, or still the value this method set.
        let owned = current.is_none() || (current.is_some() && current == previous);
        match &name {
            Some(value) => {
                tx.execute(
                    "INSERT INTO registered_agent_names (source, session_id, name) \
                     VALUES (?1, ?2, ?3) \
                     ON CONFLICT (source, session_id) DO UPDATE SET name = excluded.name",
                    rusqlite::params![source_name, session_id, value],
                )
                .map_err(Error::sql)?;
            }
            None => {
                tx.execute(
                    "DELETE FROM registered_agent_names WHERE source = ?1 AND session_id = ?2",
                    rusqlite::params![source_name, session_id],
                )
                .map_err(Error::sql)?;
            }
        }
        let changed = owned && current != name;
        if changed {
            // An ordinary write: the update trigger stamps a new revision.
            tx.execute(
                "UPDATE sessions SET agent_name = ?3 WHERE source = ?1 AND session_id = ?2",
                rusqlite::params![source_name, session_id, name],
            )
            .map_err(Error::sql)?;
        }
        tx.commit().map_err(Error::sql)?;
        Ok(changed)
    }
}

/// The longest registered agent name [`SessionStore::set_registered_agent_name`] accepts.
const MAX_REGISTERED_AGENT_NAME: usize = 128;

/// A registered agent name, trimmed, or [`Error::InvalidArgument`].
fn registered_agent_name(raw: &str) -> Result<String, Error> {
    let name = raw.trim();
    if name.is_empty() {
        return Err(Error::InvalidArgument("agent name is empty".to_string()));
    }
    if name.chars().count() > MAX_REGISTERED_AGENT_NAME {
        return Err(Error::InvalidArgument(format!(
            "agent name is over {MAX_REGISTERED_AGENT_NAME} characters"
        )));
    }
    if name.chars().any(char::is_control) {
        return Err(Error::InvalidArgument(
            "agent name contains a control character".to_string(),
        ));
    }
    Ok(name.to_string())
}

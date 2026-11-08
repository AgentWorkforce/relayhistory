//! Identity SQL shared by feed triggers and tombstone reads.
use super::FedTable;

impl FedTable {
    /// Whether the static key includes the session. Native history stores its
    /// session inside the tagged record key; legacy history excludes it.
    pub(super) fn session_keyed(&self) -> bool {
        self.key.contains(&self.session)
    }

    /// The session as a tombstone stores it: the row's, when the session is
    /// part of the identity, else `''`, so a tombstone names exactly the
    /// record's key and a later insert of that key clears it whatever
    /// session it carries.
    pub(super) fn tombstone_session_sql(&self, row: &str) -> String {
        if self.name == "history" {
            return format!(
                "CASE WHEN {row}.event_uid IS NULL THEN '' ELSE COALESCE({row}.session_id,'') END"
            );
        }
        if self.session_keyed() {
            format!("{row}.{}", self.session)
        } else {
            "''".to_string()
        }
    }

    /// True when a write moved the row to another identity.
    pub(super) fn identity_changed_sql(&self) -> String {
        let mut changed = Vec::new();
        if !self.source.starts_with('\'') {
            changed.push(format!("OLD.{0} IS NOT NEW.{0}", self.source));
        }
        if self.session_keyed() {
            changed.push(format!("OLD.{0} IS NOT NEW.{0}", self.session));
        }
        changed.push(format!(
            "{} IS NOT {}",
            self.record_key_sql("OLD"),
            self.record_key_sql("NEW")
        ));
        changed.join(" OR ")
    }
}

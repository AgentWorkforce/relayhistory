//! The name Claude Code gave a session.
//!
//! Claude Code appends title records to the transcript rather than rewriting
//! one: the desktop app re-asserts its title throughout a session, and each
//! rename (`/rename`, or the app's title field) appends a new one. So within a
//! kind the last record wins. Across kinds, a `custom-title` (what the person
//! or the app chose, and what the app shows) outranks an `ai-title` (the CLI's
//! own suggestion), which outranks an `agent-name` (the name other sessions
//! address it by, normally a copy of the custom title).

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The latest title of each kind seen so far. Last-wins per field, so it
/// folds resumably: the same records in the same order from a saved state
/// give the same answer as the whole file.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub(crate) struct ClaudeTitles {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ai: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_name: Option<String>,
}

impl ClaudeTitles {
    /// Fold one record in. A record that is not a title, or whose title is
    /// blank, changes nothing.
    pub(crate) fn observe(&mut self, value: &Value) {
        let (slot, field) = match value.get("type").and_then(Value::as_str) {
            Some("custom-title") => (&mut self.custom, "customTitle"),
            Some("ai-title") => (&mut self.ai, "aiTitle"),
            Some("agent-name") => (&mut self.agent_name, "agentName"),
            _ => return,
        };
        let title = value
            .get(field)
            .and_then(Value::as_str)
            .map(crate::discover::excerpt)
            .filter(|title| !title.is_empty());
        if title.is_some() {
            *slot = title;
        }
    }

    /// The session's title, by the precedence in the module docs.
    pub(crate) fn best(&self) -> Option<String> {
        self.custom
            .clone()
            .or_else(|| self.ai.clone())
            .or_else(|| self.agent_name.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn fold(records: &[Value]) -> Option<String> {
        let mut titles = ClaudeTitles::default();
        for record in records {
            titles.observe(record);
        }
        titles.best()
    }

    #[test]
    fn the_latest_custom_title_wins() {
        assert_eq!(
            fold(&[
                json!({"type": "custom-title", "customTitle": "First name"}),
                json!({"type": "user", "message": {"content": "hi"}}),
                json!({"type": "custom-title", "customTitle": "  Renamed  "}),
            ])
            .as_deref(),
            Some("Renamed")
        );
    }

    #[test]
    fn custom_outranks_ai_outranks_agent_name() {
        let agent = json!({"type": "agent-name", "agentName": "Agent"});
        let ai = json!({"type": "ai-title", "aiTitle": "Suggested"});
        let custom = json!({"type": "custom-title", "customTitle": "Chosen"});
        assert_eq!(fold(std::slice::from_ref(&agent)).as_deref(), Some("Agent"));
        assert_eq!(
            fold(&[ai.clone(), agent.clone()]).as_deref(),
            Some("Suggested")
        );
        assert_eq!(fold(&[custom, ai, agent]).as_deref(), Some("Chosen"));
    }

    #[test]
    fn a_blank_or_missing_title_changes_nothing() {
        assert_eq!(
            fold(&[
                json!({"type": "custom-title", "customTitle": "Kept"}),
                json!({"type": "custom-title", "customTitle": "   "}),
                json!({"type": "custom-title"}),
            ])
            .as_deref(),
            Some("Kept")
        );
        assert_eq!(fold(&[json!({"type": "summary", "summary": "x"})]), None);
    }
}

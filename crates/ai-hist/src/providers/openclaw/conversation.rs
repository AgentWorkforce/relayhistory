//! One ledger session per OpenClaw conversation, as OpenClaw's own session
//! list shows it: a session key and every transcript window under it.
//! Resets, rollovers and each isolated cron or heartbeat run open a new
//! window (session ID) on the same key; an exact-run cron key
//! (`cron:<job>:run:<id>`) belongs to its job (`session-key-utils.ts`).
//! Legacy JSONL transcripts stay one conversation each.
use super::*;
use sha2::{Digest, Sha256};

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct Conversation {
    /// Ledger identity: `<namespace>:<key after agent:<id>:>`.
    pub id: String,
    /// The canonical session key every window shares.
    pub key: String,
    /// Oldest window first.
    pub windows: Vec<Session>,
}

impl Conversation {
    /// A legacy JSONL transcript is its own conversation: its index maps a
    /// key only to its current transcript, so earlier runs stay apart.
    pub(super) fn single(window: Session) -> Self {
        Self {
            id: window.id.clone(),
            key: window.session_key.clone(),
            windows: vec![window],
        }
    }
    /// A legacy transcript whose final record was still being written.
    pub(super) fn partial(&self) -> bool {
        self.windows.iter().any(|w| w.partial)
    }
    pub(super) fn latest(&self) -> anyhow::Result<&Session> {
        self.windows
            .last()
            .ok_or_else(|| anyhow::anyhow!("OPENCLAW_INVALID_RECORD: conversation has no window"))
    }
    pub(super) fn started(&self) -> i64 {
        self.windows.iter().map(|w| w.started).min().unwrap_or(0)
    }
    pub(super) fn last_activity(&self) -> i64 {
        self.windows.iter().map(|w| w.latest).max().unwrap_or(0)
    }
    /// Change stamp over every window: any window's change, a new window or
    /// a removed one changes it. No event payload is read.
    pub(super) fn stamp(&self) -> String {
        let mut digest = Sha256::new();
        for window in &self.windows {
            digest.update(window.native_id.as_bytes());
            digest.update([0]);
            digest.update(window.stamp.as_bytes());
            digest.update([0]);
        }
        let hex: String = digest
            .finalize()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        format!("{}/{hex}", self.windows.len())
    }
    /// `<namespace>:<sessionId>` of every window: the identity each window
    /// had as its own session (generation v2, and the JSONL store's).
    pub(super) fn window_ids(&self) -> Vec<String> {
        let Some(first) = self.windows.first() else {
            return vec![];
        };
        let name = native_name(&self.key, &first.agent_id);
        let Some(namespace) = self.id.strip_suffix(&format!(":{name}")) else {
            return vec![];
        };
        self.windows
            .iter()
            .map(|w| format!("{namespace}:{}", w.native_id))
            .collect()
    }
    /// The tool-call ID scope of one window: native IDs in the oldest
    /// window, `<id>#<sessionId>` in later ones, so two runs that reuse a
    /// call ID stay two calls. Fixed by window order, which a partial read
    /// knows without reading other windows' events.
    pub(super) fn tool_scope(&self, index: usize) -> Option<&str> {
        let window = &self.windows[index];
        (index > 0 && !window.legacy).then_some(window.native_id.as_str())
    }
    pub(super) fn records(&self) -> usize {
        self.windows.iter().map(|w| w.events.len()).sum()
    }
}

impl Conversation {
    /// The newest captured window's working directory.
    pub(super) fn cwd(&self) -> Option<&str> {
        self.windows
            .iter()
            .rev()
            .filter_map(|w| w.events.first().filter(|e| e.entry["type"] == "session"))
            .find_map(|h| h.entry["cwd"].as_str().filter(|c| !c.trim().is_empty()))
    }
    /// Node title, else the newest `session_info` name, newest window first.
    pub(super) fn title(&self) -> Option<&str> {
        self.windows.iter().rev().find_map(|s| {
            s.title.as_deref().or_else(|| {
                s.events
                    .iter()
                    .rev()
                    .find(|e| e.entry["type"] == "session_info")
                    .and_then(|e| e.entry["name"].as_str())
            })
        })
    }
    pub(super) fn entries(&self) -> impl Iterator<Item = &Value> {
        self.windows
            .iter()
            .flat_map(|w| w.events.iter().map(|e| &e.entry))
    }
    /// Models of every captured assistant reply plus each window's own.
    pub(super) fn models(&self) -> Vec<String> {
        let mut models: Vec<String> = self
            .windows
            .iter()
            .flat_map(|w| {
                w.events
                    .iter()
                    .filter(|e| {
                        e.entry["message"]["role"] == "assistant"
                            && !format::is_delivery_mirror(&e.entry["message"])
                    })
                    .filter_map(|e| e.entry["message"]["model"].as_str().map(str::to_owned))
                    .chain(w.model.clone())
            })
            .collect();
        models.sort();
        models.dedup();
        models
    }
}

/// `agent:<agentId>:<rest>`, the cron run scope folded into its job.
pub(super) fn canonical_key(key: &str) -> String {
    let Some(rest) = rest_of(key) else {
        return key.into();
    };
    let parts: Vec<&str> = rest.split(':').collect();
    let run = parts.len() == 4
        && parts[0].eq_ignore_ascii_case("cron")
        && parts[2].eq_ignore_ascii_case("run")
        && parts.iter().all(|p| !p.is_empty());
    if !run {
        return key.into();
    }
    let marker = key.to_ascii_lowercase().rfind(":run:").unwrap_or(key.len());
    key[..marker].into()
}
fn rest_of(key: &str) -> Option<&str> {
    let rest = key.strip_prefix("agent:")?;
    let (agent, rest) = rest.split_once(':')?;
    (!agent.is_empty() && !rest.is_empty() && !rest.starts_with(':')).then_some(rest)
}
/// Longest key text used verbatim in a ledger name; ledger identities are
/// bounded and trimmed.
const MAX_NAME: usize = 512;
/// The ledger name of a key inside an agent database: what follows
/// `agent:<agent>:` for the database's own agent, else `key=<whole key>`.
/// A key that cannot be a ledger identity (too long, edge whitespace or
/// control characters, as some relay peer keys are) is `sha=<SHA-256>`.
pub(super) fn native_name(key: &str, agent: &str) -> String {
    let own = format!("agent:{agent}:");
    let name = match key.strip_prefix(&own) {
        Some(rest)
            if rest_of(key).is_some() && !rest.starts_with("key=") && !rest.starts_with("sha=") =>
        {
            rest.into()
        }
        _ => format!("key={key}"),
    };
    if name.len() <= MAX_NAME && name.trim() == name && !name.chars().any(char::is_control) {
        return name;
    }
    let hex: String = Sha256::digest(key.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    format!("sha={hex}")
}
/// The canonical key a ledger name came from (inverse of `native_name`), or
/// None for a hashed name, which only a listing can resolve.
pub(super) fn key_of(name: &str, agent: &str) -> Option<String> {
    if name.starts_with("sha=") {
        return None;
    }
    Some(match name.strip_prefix("key=") {
        Some(key) => key.into(),
        None => format!("agent:{agent}:{name}"),
    })
}

/// Group windows into conversations, most recent first.
pub(super) fn group(windows: Vec<Session>) -> Vec<Conversation> {
    let mut by_id: std::collections::BTreeMap<String, Conversation> = Default::default();
    for window in windows {
        let key = canonical_key(&window.session_key);
        by_id
            .entry(window.id.clone())
            .or_insert_with(|| Conversation {
                id: window.id.clone(),
                key,
                windows: vec![],
            })
            .windows
            .push(window);
    }
    let mut conversations: Vec<_> = by_id.into_values().collect();
    for c in &mut conversations {
        c.windows.sort_by(|a, b| {
            (a.started, a.latest, &a.native_id).cmp(&(b.started, b.latest, &b.native_id))
        });
    }
    conversations.sort_by(|a, b| {
        b.last_activity()
            .cmp(&a.last_activity())
            .then_with(|| a.id.cmp(&b.id))
    });
    conversations
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cron_runs_fold_into_their_job_and_other_keys_stay_whole() {
        assert_eq!(
            canonical_key("agent:main:cron:job-1:run:r-9"),
            "agent:main:cron:job-1"
        );
        assert_eq!(
            canonical_key("agent:main:cron:job-1"),
            "agent:main:cron:job-1"
        );
        // Only the exact run shape is a run scope, as upstream gates it.
        for key in [
            "agent:main:telegram:direct:run:x",
            "agent:main:cron:job:run:r:extra",
            "agent:main:main:heartbeat",
            "global",
        ] {
            assert_eq!(canonical_key(key), key);
        }
        assert_eq!(native_name("agent:main:relay:dm:x", "main"), "relay:dm:x");
        assert_eq!(native_name("agent:ops:main", "main"), "key=agent:ops:main");
        assert_eq!(native_name("global", "main"), "key=global");
        for key in [
            "agent:main:relay:dm:x",
            "agent:ops:main",
            "global",
            "agent:main:cron:j",
            "agent:main:key=x",
            "agent:main:sha=x",
            "agent:main:main",
        ] {
            assert_eq!(
                key_of(&native_name(key, "main"), "main").as_deref(),
                Some(key)
            );
        }
        // Keys that cannot be ledger identities get a stable hashed name.
        for key in [
            format!("agent:main:relay:dm:{}", "p".repeat(1024)),
            "agent:main:relay:dm:trailing ".to_string(),
            "agent:main:relay:dm:new\nline".to_string(),
        ] {
            let name = native_name(&key, "main");
            assert!(name.starts_with("sha=") && name.len() == 68, "{name}");
            assert_eq!(native_name(&key, "main"), name);
            assert_eq!(key_of(&name, "main"), None);
        }
    }
}

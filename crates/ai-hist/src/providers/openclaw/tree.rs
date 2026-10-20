//! The active branch of a JSONL transcript, as OpenClaw's own session
//! storage resolves it (`packages/agent-core/src/harness/session`
//! `BaseSessionStorage`, 2026.7.x): the visible leaf after every entry, then
//! its path to the root. The database layout stores this projection as
//! `session_transcript_active_events`; JSONL only stores the tree.
use super::*;
use std::collections::{BTreeSet, HashMap};

fn id(entry: &Value) -> Option<&str> {
    entry["id"].as_str()
}
fn side(entry: &Value) -> bool {
    entry["appendMode"] == "side"
}
/// `leafIdUpdateAfterEntry`: the visible leaf an entry moves to, if any.
/// `Some(None)` is a leaf control that clears the leaf.
fn leaf_update(entry: &Value) -> Option<Option<&str>> {
    let kind = entry["type"].as_str()?;
    if kind == "leaf" {
        return Some(entry["targetId"].as_str());
    }
    let tree = matches!(
        kind,
        "message"
            | "thinking_level_change"
            | "model_change"
            | "compaction"
            | "branch_summary"
            | "custom"
            | "custom_message"
            | "label"
            | "session_info"
    );
    (tree && !side(entry)).then(|| id(entry))
}
/// `appendParentIdAfterEntry`: where the next append attaches.
fn append_parent(entry: &Value) -> Option<&str> {
    if entry["type"] == "leaf" {
        return match entry.get("appendParentId") {
            Some(parent) => parent.as_str(),
            None => entry["targetId"].as_str(),
        };
    }
    id(entry)
}

/// Indexes (into `entries`) of the entries on the active branch. The header
/// (index 0) is never part of it, matching the database projection.
pub(super) fn active(entries: &[Value]) -> BTreeSet<usize> {
    let body = entries.get(1..).unwrap_or_default();
    let mut index: HashMap<&str, usize> = HashMap::new();
    // An entry appended to the previous append parent while the visible leaf
    // was elsewhere is logically a child of that leaf.
    let mut logical: HashMap<&str, Option<&str>> = HashMap::new();
    let (mut leaf, mut parent) = (None, None);
    for (i, entry) in body.iter().enumerate() {
        let Some(entry_id) = id(entry) else { continue };
        index.insert(entry_id, i + 1);
        let update = leaf_update(entry);
        if update == Some(Some(entry_id)) && entry["parentId"].as_str() == parent && leaf != parent
        {
            logical.insert(entry_id, leaf);
        }
        if let Some(update) = update {
            leaf = update;
        }
        parent = append_parent(entry);
    }
    let mut path = BTreeSet::new();
    let mut seen = BTreeSet::new();
    let mut current = leaf.and_then(|l| index.get(l).copied());
    while let Some(i) = current.filter(|i| seen.insert(*i)) {
        let entry = &entries[i];
        let entry_id = id(entry).unwrap_or_default();
        let next = if entry["type"] == "leaf" {
            entry["targetId"].as_str()
        } else {
            path.insert(i);
            match logical.get(entry_id) {
                Some(parent) => *parent,
                None => entry["parentId"].as_str(),
            }
        };
        current = next.and_then(|p| index.get(p).copied());
    }
    path
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn message(id: &str, parent: Option<&str>) -> Value {
        json!({"type":"message","id":id,"parentId":parent})
    }
    #[test]
    fn linear_branched_leaf_controlled_and_side_appends() {
        let header = json!({"type":"session"});
        let linear = [header.clone(), message("a", None), message("b", Some("a"))];
        assert_eq!(active(&linear), [1, 2].into());
        // A fork back to `a`: the newest entry is the leaf, `b` is off-branch.
        let fork = [
            header.clone(),
            message("a", None),
            message("b", Some("a")),
            message("c", Some("a")),
        ];
        assert_eq!(active(&fork), [1, 3].into());
        // A leaf control moves the visible leaf back to `b`, keeping `c` as
        // the append parent.
        let mut controlled = fork.to_vec();
        controlled.push(
            json!({"type":"leaf","id":"l","parentId":"c","targetId":"b","appendParentId":"c"}),
        );
        assert_eq!(active(&controlled), [1, 2].into());
        // The next append attaches to `c` but is logically a child of the
        // visible leaf `b`.
        controlled.push(message("d", Some("c")));
        assert_eq!(active(&controlled), [1, 2, 5].into());
        // A side append never moves the leaf; a cleared leaf has no branch.
        let mut sided = linear.to_vec();
        sided.push(json!({"type":"custom","id":"s","parentId":"b","appendMode":"side"}));
        assert_eq!(active(&sided), [1, 2].into());
        sided.push(json!({"type":"leaf","id":"n","parentId":"s","targetId":null}));
        assert!(active(&sided).is_empty());
        // A cycle terminates.
        let cycle = [header, message("a", Some("b")), message("b", Some("a"))];
        assert_eq!(active(&cycle), [1, 2].into());
    }
}

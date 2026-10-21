use super::*;
pub(super) fn parse(bytes: &[u8]) -> Result<(Vec<Value>, bool)> {
    let mut rows = Vec::new();
    let mut partial = false;
    for line in bytes.split_inclusive(|b| *b == b'\n') {
        crate::ingest::check_capture_cancelled()?;
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        match serde_json::from_slice::<Value>(line) {
            Ok(row) => rows.push(row),
            Err(e) if e.is_eof() && !line.ends_with(b"\n") => partial = true,
            Err(e) => return Err(e).context("KIMI_INVALID_RECORD: malformed wire JSONL"),
        }
    }
    let head = rows.first().context("KIMI_INVALID_RECORD: empty wire")?;
    anyhow::ensure!(
        head["type"] == "metadata"
            && head["protocol_version"] == "1.5"
            && head["created_at"].as_i64().is_some(),
        "KIMI_UNSUPPORTED_PROTOCOL: requires wire1.5 metadata"
    );
    for row in &rows[1..] {
        string(row, "type")?;
        time(row)?;
        if row["type"] == "agent.message.appended" {
            let m = &row["message"]["message"];
            anyhow::ensure!(
                matches!(m["role"].as_str(), Some("user" | "assistant" | "tool"))
                    && m["content"].is_array(),
                "KIMI_INVALID_RECORD: invalid native message"
            );
        }
    }
    Ok((rows, partial))
}
pub(super) fn string<'a>(v: &'a Value, k: &str) -> Result<&'a str> {
    v[k].as_str()
        .filter(|s| !s.is_empty())
        .with_context(|| format!("KIMI_INVALID_RECORD: {k} missing"))
}
pub(super) fn time(v: &Value) -> Result<i64> {
    v["time"]
        .as_i64()
        .or_else(|| v["created_at"].as_i64())
        .filter(|t| *t > 0)
        .context("KIMI_INVALID_RECORD: missing millisecond time")
}
pub(super) fn text(v: &Value) -> String {
    v.as_array()
        .into_iter()
        .flatten()
        .filter(|p| p["type"] == "text")
        .filter_map(|p| p["text"].as_str())
        .collect::<Vec<_>>()
        .join("\n")
}
pub(super) fn human_text(v: &Value) -> Option<String> {
    if v["type"] != "agent.message.appended"
        || !human_origin(&v["message"]["meta"]["origin"])
        || v["message"]["message"]["role"] != "user"
    {
        return None;
    }
    let s = text(&v["message"]["message"]["content"]);
    (!s.is_empty()).then_some(s)
}

// Kimi2.2 wire/tree/tree.ts: active branches inherit only through their base
// physical line. Paired legacy undo rows must not apply the rewind twice.
pub(super) fn active_messages(rows: &[Value]) -> Result<std::collections::BTreeSet<usize>> {
    use std::collections::{BTreeMap, BTreeSet};
    let mut branches = BTreeMap::from([(
        "main".to_owned(),
        (0usize, rows.len(), None::<(String, usize)>),
    )]);
    let mut current = "main".to_owned();
    let mut paired = BTreeSet::new();
    for (index, row) in rows.iter().enumerate() {
        if row["type"] != "agent.switched" {
            continue;
        }
        let name = string(row, "branch")?.to_owned();
        let base = string(&row["base"], "branch")?.to_owned();
        let line = row["base"]["line"]
            .as_u64()
            .context("KIMI_INVALID_BRANCH: base line")? as usize;
        anyhow::ensure!(
            !branches.contains_key(&name) && branches.contains_key(&base) && line <= index,
            "KIMI_INVALID_BRANCH: duplicate/future branch"
        );
        branches.get_mut(&current).unwrap().1 = index;
        branches.insert(name.clone(), (index + 1, rows.len(), Some((base, line))));
        current = name;
        if let Some(line) = row["legacyUndoLine"].as_u64() {
            paired.insert(line as usize);
        }
    }
    let mut live = BTreeSet::new();
    let mut upper = rows.len();
    loop {
        let (start, end, base) = &branches[&current];
        for (index, row) in rows.iter().enumerate().take((*end).min(upper)).skip(*start) {
            if row["type"] == "context.undo" && !paired.contains(&(index + 1)) {
                anyhow::bail!("KIMI_UNSUPPORTED_REWIND: unpaired legacy undo")
            }
            if row["type"] == "agent.message.appended" {
                live.insert(index);
            }
        }
        if let Some((parent, line)) = base {
            current = parent.clone();
            upper = *line;
        } else {
            break;
        }
    }
    Ok(live)
}

pub(super) fn human_origin(origin: &Value) -> bool {
    origin.is_null()
        || origin["kind"] == "user"
        || (matches!(
            origin["kind"].as_str(),
            Some("skill_activation" | "plugin_command")
        ) && origin["trigger"] == "user-slash")
}

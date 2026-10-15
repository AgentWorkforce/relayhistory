//! Replay the pinned recorder's logical projection without discarding raw records.
use super::*;
use std::collections::{HashMap, HashSet};
#[derive(Default)]
struct Journal {
    order: Vec<String>,
    messages: HashMap<String, Value>,
}
impl Journal {
    fn clear(&mut self) {
        self.order.clear();
        self.messages.clear();
    }
    fn rewind(&mut self, target: &str) {
        let at = self.order.iter().position(|id| id == target).unwrap_or(0);
        for id in self.order.drain(at..) {
            self.messages.remove(&id);
        }
    }
    fn finish(mut self) -> Vec<Value> {
        self.order
            .into_iter()
            .map(|id| self.messages.remove(&id).unwrap())
            .collect()
    }
}
pub(super) fn string<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value[field]
        .as_str()
        .filter(|s| !s.is_empty() && !s.chars().any(char::is_control))
        .with_context(|| format!("GEMINI_INVALID_RECORD: invalid {field}"))
}
pub(super) fn date(value: &Value, field: &str) -> Result<i64> {
    Ok(chrono::DateTime::parse_from_rfc3339(string(value, field)?)
        .with_context(|| format!("GEMINI_INVALID_RECORD: {field}"))?
        .timestamp_millis())
}
pub(super) fn text(value: &Value) -> String {
    if let Some(s) = value.as_str() {
        return s.into();
    }
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|p| p["text"].as_str())
        .collect::<Vec<_>>()
        .join("\n")
}
pub(super) fn header(bytes: &[u8], legacy: bool) -> Result<Value> {
    if legacy {
        // The bounded read cannot require the complete legacy messages array.
        // Stream the top-level metadata prefix and ignore the value after messages.
        let mut de = serde_json::Deserializer::from_slice(bytes);
        use serde::de::{MapAccess, Visitor};
        struct Metadata<'a> {
            values: &'a mut Map<String, Value>,
            stopped: &'a mut bool,
        }
        impl<'de> Visitor<'de> for Metadata<'_> {
            type Value = ();
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("Gemini conversation object")
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> std::result::Result<(), M::Error> {
                while let Some(key) = map.next_key::<String>()? {
                    if key == "messages" {
                        *self.stopped = true;
                        return Err(serde::de::Error::custom("metadata prefix complete"));
                    }
                    self.values.insert(key, map.next_value::<Value>()?);
                }
                Ok(())
            }
        }
        let mut fields = Map::new();
        let mut stopped = false;
        let result = serde::de::Deserializer::deserialize_map(
            &mut de,
            Metadata {
                values: &mut fields,
                stopped: &mut stopped,
            },
        );
        if !stopped {
            result?;
        }
        let value = Value::Object(fields);
        validate_metadata(&value)?;
        return Ok(value);
    }
    let end = bytes
        .iter()
        .position(|b| *b == b'\n')
        .context("GEMINI_INCOMPLETE_HEADER: no terminated metadata within read limit")?;
    let value: Value = serde_json::from_slice(&bytes[..end])?;
    validate_metadata(&value)?;
    Ok(value)
}
fn validate_metadata(v: &Value) -> Result<()> {
    string(v, "sessionId")?;
    string(v, "projectHash")?;
    date(v, "startTime")?;
    date(v, "lastUpdated")?;
    if let Some(kind) = v.get("kind") {
        anyhow::ensure!(
            matches!(kind.as_str(), Some("main" | "subagent")),
            "GEMINI_UNSUPPORTED_SCHEMA: session kind"
        );
    }
    Ok(())
}
fn message(v: &Value) -> Result<()> {
    string(v, "id")?;
    date(v, "timestamp")?;
    anyhow::ensure!(
        matches!(
            v["type"].as_str(),
            Some("user" | "gemini" | "info" | "error" | "warning")
        ),
        "GEMINI_UNSUPPORTED_SCHEMA: message type"
    );
    anyhow::ensure!(
        v["content"].is_string() || v["content"].is_array(),
        "GEMINI_INVALID_RECORD: content"
    );
    if let Some(tokens) = v.get("tokens").filter(|v| !v.is_null()) {
        usage::normalize(tokens).map_err(|e| anyhow::anyhow!("GEMINI_INVALID_RECORD: {e}"))?;
    }
    for key in ["thoughts", "toolCalls"] {
        if let Some(values) = v.get(key) {
            anyhow::ensure!(values.is_array(), "GEMINI_INVALID_RECORD: {key}");
        }
    }
    let mut calls = HashSet::new();
    for call in v["toolCalls"].as_array().into_iter().flatten() {
        anyhow::ensure!(
            calls.insert(string(call, "id")?),
            "GEMINI_INVALID_RECORD: duplicate tool id"
        );
        string(call, "name")?;
        date(call, "timestamp")?;
        string(call, "status")?;
        anyhow::ensure!(call["args"].is_object(), "GEMINI_INVALID_RECORD: tool args");
    }
    Ok(())
}
fn replace(messages: &mut Journal, v: Value) -> Result<()> {
    message(&v)?;
    let id = string(&v, "id")?.to_owned();
    if !messages.messages.contains_key(&id) {
        messages.order.push(id.clone());
    }
    messages.messages.insert(id, v);
    Ok(())
}
fn strings<'a>(value: &'a Value, key: &str) -> Result<Vec<&'a str>> {
    let Some(v) = value.get(key) else {
        return Ok(vec![]);
    };
    v.as_array()
        .context("GEMINI_INVALID_RECORD: expected id array")?
        .iter()
        .map(|v| {
            v.as_str()
                .filter(|s| !s.is_empty())
                .context("GEMINI_INVALID_RECORD: invalid id array")
        })
        .collect()
}
fn fields(value: &Value, allowed: &[&str], kind: &str) -> Result<()> {
    let object = value
        .as_object()
        .with_context(|| format!("GEMINI_INVALID_RECORD: {kind} object"))?;
    anyhow::ensure!(
        object.keys().all(|key| allowed.contains(&key.as_str())),
        "GEMINI_UNSUPPORTED_SCHEMA: {kind} field"
    );
    Ok(())
}
fn update(messages: &mut Journal, patch: &Value) -> Result<()> {
    fields(patch, &["id", "content", "toolCalls"], "message patch")?;
    let id = string(patch, "id")?;
    if let Some(content) = patch.get("content") {
        anyhow::ensure!(
            content.is_string() || content.is_array(),
            "GEMINI_INVALID_RECORD: patch content"
        );
    }
    let calls = patch
        .get("toolCalls")
        .map(|v| {
            v.as_array()
                .context("GEMINI_INVALID_RECORD: patch toolCalls")
        })
        .transpose()?;
    // Validate the complete schema even when upstream's logical no-op targets
    // an unknown message/call. New operations cannot become cached omissions.
    for call in calls.into_iter().flatten() {
        fields(call, &["id", "result"], "tool-call patch")?;
        string(call, "id")?;
    }
    if let Some(msg) = messages.messages.get_mut(id) {
        if let Some(content) = patch.get("content") {
            msg["content"] = content.clone();
        }
        for call in calls.into_iter().flatten() {
            let id = string(call, "id")?;
            if let Some(old) = msg["toolCalls"]
                .as_array_mut()
                .and_then(|a| a.iter_mut().find(|m| m["id"].as_str() == Some(id)))
            {
                if let Some(result) = call.get("result") {
                    old["result"] = result.clone();
                }
            }
        }
        message(msg)?;
    }
    Ok(())
}
fn patch(messages: &mut Journal, p: &Value) -> Result<()> {
    anyhow::ensure!(p.is_object(), "GEMINI_INVALID_RECORD: patch object");
    anyhow::ensure!(
        p.as_object().unwrap().keys().all(|k| [
            "id",
            "content",
            "toolCalls",
            "updates",
            "removeIds",
            "orderIds"
        ]
        .contains(&k.as_str())),
        "GEMINI_UNSUPPORTED_SCHEMA: patch field"
    );
    if p.get("id").is_some() {
        let direct = ["id", "content", "toolCalls"]
            .into_iter()
            .filter_map(|key| p.get(key).map(|v| (key.to_owned(), v.clone())))
            .collect();
        update(messages, &Value::Object(direct))?;
    }
    if let Some(updates) = p.get("updates") {
        for u in updates
            .as_array()
            .context("GEMINI_INVALID_RECORD: updates array")?
        {
            update(messages, u)?;
        }
    }
    let remove: HashSet<_> = strings(p, "removeIds")?.into_iter().collect();
    for id in &remove {
        messages.messages.remove(*id);
    }
    messages.order.retain(|id| !remove.contains(id.as_str()));
    let order = strings(p, "orderIds")?;
    let mut selected = HashSet::new();
    let ordered: Vec<_> = order
        .into_iter()
        .filter(|id| messages.messages.contains_key(*id) && selected.insert((*id).to_owned()))
        .map(str::to_owned)
        .collect();
    messages.order.retain(|id| !selected.contains(id));
    messages.order.extend(ordered);
    Ok(())
}
fn metadata(meta: &mut Value, messages: &mut Journal, record: &Value) -> Result<()> {
    let mut fields = record
        .as_object()
        .context("GEMINI_INVALID_RECORD: metadata object")?
        .clone();
    if let Some(checkpoint) = fields.remove("messages") {
        messages.clear();
        for m in checkpoint
            .as_array()
            .context("GEMINI_INVALID_RECORD: checkpoint")?
        {
            replace(messages, m.clone())?;
        }
    }
    meta.as_object_mut().unwrap().extend(fields);
    Ok(())
}
pub(super) fn parse(bytes: &[u8], legacy: bool) -> Result<Value> {
    let (records, partial) = records(bytes, legacy)?;
    anyhow::ensure!(!records.is_empty(), "GEMINI_INCOMPLETE_HEADER: no metadata");
    validate_metadata(&records[0])?;
    let native = string(&records[0], "sessionId")?.to_owned();
    let mut meta = serde_json::json!({});
    let mut messages = Journal::default();
    for record in &records {
        crate::ingest::check_capture_cancelled()?;
        anyhow::ensure!(record.is_object(), "GEMINI_INVALID_RECORD: journal object");
        anyhow::ensure!(
            record
                .as_object()
                .unwrap()
                .keys()
                .filter(|k| k.starts_with('$'))
                .all(|k| ["$set", "$patch", "$rewindTo"].contains(&k.as_str())),
            "GEMINI_UNSUPPORTED_SCHEMA: journal opcode"
        );
        if record.get("$rewindTo").is_some() {
            let target = string(record, "$rewindTo")?;
            messages.rewind(target);
        } else if let Some(p) = record.get("$patch") {
            patch(&mut messages, p)?;
        } else if record.get("id").is_some() {
            replace(&mut messages, record.clone())?;
        } else if let Some(update) = record.get("$set") {
            metadata(&mut meta, &mut messages, update)?;
        } else if record.get("sessionId").is_some() {
            metadata(&mut meta, &mut messages, record)?;
        } else {
            anyhow::bail!("GEMINI_UNSUPPORTED_SCHEMA: journal opcode");
        }
        anyhow::ensure!(
            meta["sessionId"].as_str().is_none_or(|s| s == native),
            "GEMINI_ID_COLLISION: journal changed native identity"
        );
    }
    validate_metadata(&meta)?;
    meta["messages"] = messages.finish().into();
    Ok(serde_json::json!({"conversation":meta,"records":records,"partial":partial}))
}

fn records(bytes: &[u8], legacy: bool) -> Result<(Vec<Value>, bool)> {
    let mut records = Vec::new();
    let mut partial = false;
    if legacy {
        records.push(serde_json::from_slice::<Value>(bytes)?);
    } else {
        let line_count = bytes.split(|b| *b == b'\n').count();
        for (index, line) in bytes.split(|b| *b == b'\n').enumerate() {
            if line.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            match serde_json::from_slice::<Value>(line) {
                Ok(v) => records.push(v),
                Err(e) if !bytes.ends_with(b"\n") && index == line_count - 1 && e.is_eof() => {
                    partial = true;
                    break;
                }
                Err(e) => {
                    return Err(anyhow::anyhow!(
                        "GEMINI_INVALID_RECORD: complete journal record {}: {e}",
                        index + 1
                    ))
                }
            }
        }
    }
    Ok((records, partial))
}

pub(super) fn recent_time(
    bytes: &[u8],
    starts_mid_record: bool,
    header: &Value,
) -> Result<(i64, u64)> {
    let mut latest = date(header, "lastUpdated")?;
    let mut records = 0;
    let Some(end) = bytes.iter().rposition(|b| *b == b'\n') else {
        return Ok((latest, records));
    };
    let start = if starts_mid_record {
        bytes.iter().position(|b| *b == b'\n').unwrap() + 1
    } else {
        0
    };
    if start > end {
        return Ok((latest, records));
    }
    for line in bytes[start..end].split(|b| *b == b'\n') {
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        records += 1;
        let record: Value =
            serde_json::from_slice(line).context("GEMINI_INVALID_RECORD: shallow tail record")?;
        if let Some(update) = record.get("$set") {
            if let Some(sid) = update.get("sessionId") {
                anyhow::ensure!(
                    sid == &header["sessionId"],
                    "GEMINI_ID_COLLISION: shallow tail identity changed"
                );
            }
            if update.get("lastUpdated").is_some() {
                latest = latest.max(date(update, "lastUpdated")?);
            }
        } else if record.get("id").is_some() {
            message(&record)?;
            latest = latest.max(date(&record, "timestamp")?);
        }
    }
    Ok((latest, records))
}

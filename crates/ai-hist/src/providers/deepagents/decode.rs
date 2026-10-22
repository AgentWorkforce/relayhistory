//! Data-only decoder for pinned JsonPlusSerializer MsgPack. No constructors run.
use super::Message;
use anyhow::{bail, ensure, Context, Result};
use rmpv::Value as Mp;
use serde_json::{Map as JsonMap, Value as Json};
use std::collections::HashMap;
use std::io::Cursor;

pub(super) const MAX_BLOB: usize = 8 * 1024 * 1024;
const MAX_DEPTH: usize = 32;
const MAX_MESSAGES: usize = 100_000;

fn unpack(bytes: &[u8]) -> Result<Mp> {
    ensure!(
        bytes.len() <= MAX_BLOB,
        "DeepAgents MsgPack blob exceeds 8 MiB"
    );
    let mut cursor = Cursor::new(bytes);
    let value = rmpv::decode::read_value_with_max_depth(&mut cursor, MAX_DEPTH)
        .context("malformed DeepAgents MsgPack")?;
    ensure!(
        cursor.position() == bytes.len() as u64,
        "trailing DeepAgents MsgPack bytes"
    );
    Ok(value)
}
fn member<'a>(value: &'a Mp, key: &str) -> Option<&'a Mp> {
    value
        .as_map()?
        .iter()
        .find(|(k, _)| k.as_str() == Some(key))
        .map(|(_, v)| v)
}
fn string<'a>(value: &'a Mp, field: &str) -> Result<&'a str> {
    value
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("DeepAgents {field} must be text"))
}
fn plain(value: &Mp, depth: usize) -> Result<Json> {
    ensure!(depth <= MAX_DEPTH, "DeepAgents value nesting exceeds limit");
    Ok(match value {
        Mp::Nil => Json::Null,
        Mp::Boolean(v) => Json::Bool(*v),
        Mp::Integer(v) => Json::Number(
            v.as_i64()
                .map(Into::into)
                .or_else(|| v.as_u64().map(Into::into))
                .ok_or_else(|| anyhow::anyhow!("unsupported integer"))?,
        ),
        Mp::F32(v) => serde_json::json!(*v),
        Mp::F64(v) => serde_json::json!(*v),
        Mp::String(v) => Json::String(
            v.as_str()
                .ok_or_else(|| anyhow::anyhow!("invalid UTF-8 text"))?
                .to_owned(),
        ),
        Mp::Array(items) => Json::Array(
            items
                .iter()
                .map(|v| plain(v, depth + 1))
                .collect::<Result<Vec<_>>>()?,
        ),
        Mp::Map(items) => {
            let mut map = JsonMap::new();
            for (key, value) in items {
                map.insert(string(key, "map key")?.to_owned(), plain(value, depth + 1)?);
            }
            Json::Object(map)
        }
        Mp::Binary(_) | Mp::Ext(_, _) => bail!("unsupported nested DeepAgents binary or extension"),
    })
}
fn text_content(value: &Mp) -> Result<String> {
    match value {
        Mp::String(_) => Ok(string(value, "content")?.to_owned()),
        Mp::Array(blocks) => {
            ensure!(
                blocks.len() <= 1024,
                "DeepAgents content block count exceeds limit"
            );
            let mut text = Vec::new();
            for block in blocks {
                let kind = member(block, "type").and_then(Mp::as_str);
                let phase = member(block, "phase").and_then(Mp::as_str);
                if matches!(kind, Some("reasoning" | "thinking")) || phase == Some("analysis") {
                    continue;
                }
                if let Some(s) = block
                    .as_str()
                    .or_else(|| member(block, "text").and_then(Mp::as_str))
                    .or_else(|| member(block, "content").and_then(Mp::as_str))
                {
                    text.push(s.to_owned());
                }
            }
            Ok(text.join("\n"))
        }
        Mp::Nil => Ok(String::new()),
        _ => bail!("unsupported DeepAgents message content"),
    }
}
fn tuple(value: &Mp, code: i8) -> Result<Mp> {
    let Mp::Ext(actual, bytes) = value else {
        bail!("expected DeepAgents typed extension")
    };
    ensure!(*actual == code, "unsupported DeepAgents extension {actual}");
    unpack(bytes)
}
fn message(value: &Mp) -> Result<Message> {
    let decoded = tuple(value, 5)?;
    let parts = decoded
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("invalid DeepAgents message tuple"))?;
    ensure!(parts.len() == 4, "invalid DeepAgents message tuple length");
    let module = string(&parts[0], "message module")?;
    let class = string(&parts[1], "message class")?;
    ensure!(
        string(&parts[3], "message method")? == "model_validate_json",
        "unsupported DeepAgents message constructor"
    );
    let role = match (module, class) {
        ("langchain_core.messages.human", "HumanMessage") => "user",
        ("langchain_core.messages.ai", "AIMessage") => "assistant",
        ("langchain_core.messages.tool", "ToolMessage") => "tool",
        ("langchain_core.messages.system", "SystemMessage") => "system",
        ("langchain_core.messages.modifier", "RemoveMessage") => "remove",
        _ => bail!("unsupported DeepAgents message class {module}.{class}"),
    };
    let fields = &parts[2];
    let id = member(fields, "id")
        .ok_or_else(|| anyhow::anyhow!("DeepAgents message lacks native id"))?;
    let id = string(id, "native message id")?.to_owned();
    ensure!(
        !id.is_empty() && id.len() <= 512,
        "invalid DeepAgents native message id"
    );
    let content = text_content(member(fields, "content").unwrap_or(&Mp::Nil))?;
    let tool_call_id = match member(fields, "tool_call_id") {
        Some(Mp::Nil) | None => None,
        Some(value) => {
            let id = string(value, "tool result call id")?;
            ensure!(
                !id.is_empty() && id.len() <= 1024,
                "invalid DeepAgents tool result call id"
            );
            Some(id.to_owned())
        }
    };
    let tool_calls = match member(fields, "tool_calls") {
        Some(Mp::Array(values)) => {
            ensure!(
                values.len() <= 128,
                "DeepAgents tool-call count exceeds limit"
            );
            values
                .iter()
                .map(|value| plain(value, 0))
                .collect::<Result<Vec<_>>>()?
        }
        Some(Mp::Nil) | None => vec![],
        _ => bail!("malformed DeepAgents tool_calls"),
    };
    let status = member(fields, "status")
        .and_then(Mp::as_str)
        .filter(|status| status.len() <= 64)
        .map(str::to_owned);
    Ok(Message {
        id,
        role: role.into(),
        content,
        tool_call_id,
        tool_calls,
        status,
    })
}
fn messages(value: &Mp) -> Result<Vec<Message>> {
    let items = value
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("DeepAgents messages must be an array"))?;
    ensure!(
        items.len() <= MAX_MESSAGES,
        "DeepAgents message count exceeds limit"
    );
    items.iter().map(message).collect()
}
pub(super) fn inline(blob: &[u8]) -> Result<Option<Vec<Message>>> {
    let checkpoint = unpack(blob)?;
    ensure!(
        matches!(member(&checkpoint, "v").and_then(Mp::as_i64), Some(2 | 4)),
        "unsupported DeepAgents checkpoint version"
    );
    let Some(channels) = member(&checkpoint, "channel_values") else {
        bail!("DeepAgents checkpoint lacks channel_values")
    };
    match member(channels, "messages") {
        Some(value) => Ok(Some(messages(value)?)),
        None => Ok(None),
    }
}
pub(super) fn checkpoint_time(blob: &[u8]) -> Result<i64> {
    let checkpoint = unpack(blob)?;
    ensure!(
        matches!(member(&checkpoint, "v").and_then(Mp::as_i64), Some(2 | 4)),
        "unsupported DeepAgents checkpoint version"
    );
    let raw = member(&checkpoint, "ts")
        .and_then(Mp::as_str)
        .ok_or_else(|| anyhow::anyhow!("DeepAgents checkpoint lacks ts"))?;
    Ok(chrono::DateTime::parse_from_rfc3339(raw)
        .context("invalid DeepAgents checkpoint ts")?
        .timestamp_millis())
}
pub(super) enum Delta {
    Append(Vec<Message>),
    Overwrite(Vec<Message>),
}
pub(super) fn write(blob: &[u8]) -> Result<Delta> {
    let value = unpack(blob)?;
    match &value {
        Mp::Ext(2, _) => {
            let decoded = tuple(&value, 2)?;
            let parts = decoded
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("invalid DeepAgents overwrite tuple"))?;
            ensure!(
                parts.len() == 3
                    && parts[0].as_str() == Some("langgraph.types")
                    && parts[1].as_str() == Some("Overwrite"),
                "unsupported DeepAgents overwrite"
            );
            let value = member(&parts[2], "value")
                .ok_or_else(|| anyhow::anyhow!("DeepAgents overwrite lacks value"))?;
            Ok(Delta::Overwrite(messages(value)?))
        }
        Mp::Array(_) => Ok(Delta::Append(messages(&value)?)),
        Mp::Ext(5, _) => Ok(Delta::Append(vec![message(&value)?])),
        _ => bail!("unsupported DeepAgents messages write"),
    }
}
pub(super) struct Fold {
    slots: Vec<Option<Message>>,
    positions: HashMap<String, usize>,
    active: usize,
}
impl Fold {
    pub(super) fn new(initial: Vec<Message>) -> Result<Self> {
        let mut fold = Self {
            slots: Vec::new(),
            positions: HashMap::new(),
            active: 0,
        };
        for message in initial {
            ensure!(
                !fold.positions.contains_key(&message.id),
                "duplicate DeepAgents inline message ID"
            );
            fold.upsert(message)?;
        }
        Ok(fold)
    }
    pub(super) fn apply(&mut self, delta: Delta) -> Result<()> {
        let messages = match delta {
            Delta::Overwrite(messages) => {
                self.clear();
                messages
            }
            Delta::Append(messages) => messages,
        };
        let start = messages
            .iter()
            .rposition(|m| m.role == "remove" && m.id == "__remove_all__")
            .map_or(0, |index| {
                self.clear();
                index + 1
            });
        for message in messages.into_iter().skip(start) {
            self.upsert(message)?;
        }
        Ok(())
    }
    fn upsert(&mut self, message: Message) -> Result<()> {
        if message.role == "remove" {
            if let Some(index) = self.positions.remove(&message.id) {
                self.slots[index] = None;
                self.active -= 1;
            }
        } else if let Some(index) = self.positions.get(&message.id).copied() {
            self.slots[index] = Some(message);
        } else {
            ensure!(
                self.active < MAX_MESSAGES,
                "DeepAgents folded message count exceeds limit"
            );
            self.positions.insert(message.id.clone(), self.slots.len());
            self.slots.push(Some(message));
            self.active += 1;
        }
        Ok(())
    }
    fn clear(&mut self) {
        self.slots.clear();
        self.positions.clear();
        self.active = 0;
    }
    pub(super) fn finish(self) -> Vec<Message> {
        self.slots.into_iter().flatten().collect()
    }
}

//! Codex `token_count` snapshots: the typed form a reader gets, and the
//! compact form `session_markers.payload_json` stores.
//!
//! A rollout writes several snapshots per turn, so they are the most numerous
//! marker a Codex session has, and almost all of each one is key names. The
//! stored form keeps every value and drops the names into fixed positions:
//!
//! ```text
//! [total, last, window, other, info]
//! ```
//!
//! - `total` / `last`: `total_token_usage` / `last_token_usage` when it is an
//!   object, as `[input, cached_input, cache_write_input, output,
//!   reasoning_output, total, rest]` -- each counter as written, `null` when
//!   the key is absent; `rest` an object of every other key, and of a counter
//!   written as an explicit `null`. `null` when the key is absent or holds
//!   something other than an object (that value is in `other`).
//! - `window`: `model_context_window` as written; `null` when absent (an
//!   explicit `null` is in `other`).
//! - `other`: every other key of `info`, as written.
//! - `info`: only for an `info` that is not an object -- never written by
//!   Codex -- which is then kept here whole and every other slot is `null`.
//!
//! Trailing `null` slots and an empty `rest` / `other` are left out, so a
//! typical snapshot is `[[2000,1000,0,300,50,2300],[1000,500,0,100,0,1100],400000]`.
//! Every value is kept exactly as `serde_json` parsed it: a counter above
//! `i64::MAX`, a fractional one and a negative one round-trip unchanged.
//! Key order is not kept; the object form serializes keys sorted.

use serde::de::{self, Deserializer, SeqAccess, Visitor};
use serde::ser::{SerializeMap, SerializeSeq, Serializer};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::fmt;

/// The counter names Codex writes in a token usage object, in stored order.
const COUNTERS: [&str; 6] = [
    "input_tokens",
    "cached_input_tokens",
    "cache_write_input_tokens",
    "output_tokens",
    "reasoning_output_tokens",
    "total_tokens",
];

/// One Codex token usage object (`total_token_usage` or `last_token_usage`).
///
/// Each counter is the value as written -- a JSON number in every snapshot
/// Codex writes, kept exactly (`u64` above `i64::MAX`, and a malformed
/// fractional or negative value, unchanged). `None` when the key is absent;
/// `Some(Value::Null)` when it was written as `null`. A key this type does not
/// name is in `other`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct TokenUsage {
    pub input_tokens: Option<Value>,
    pub cached_input_tokens: Option<Value>,
    pub cache_write_input_tokens: Option<Value>,
    pub output_tokens: Option<Value>,
    pub reasoning_output_tokens: Option<Value>,
    pub total_tokens: Option<Value>,
    /// Every other key, as written.
    pub other: Map<String, Value>,
}

impl TokenUsage {
    fn slots(&self) -> [&Option<Value>; 6] {
        [
            &self.input_tokens,
            &self.cached_input_tokens,
            &self.cache_write_input_tokens,
            &self.output_tokens,
            &self.reasoning_output_tokens,
            &self.total_tokens,
        ]
    }

    fn slot_mut(&mut self, index: usize) -> &mut Option<Value> {
        match index {
            0 => &mut self.input_tokens,
            1 => &mut self.cached_input_tokens,
            2 => &mut self.cache_write_input_tokens,
            3 => &mut self.output_tokens,
            4 => &mut self.reasoning_output_tokens,
            _ => &mut self.total_tokens,
        }
    }

    fn from_object(object: Map<String, Value>) -> Self {
        let mut usage = Self::default();
        for (key, value) in object {
            match COUNTERS.iter().position(|name| *name == key) {
                Some(index) => *usage.slot_mut(index) = Some(value),
                None => {
                    usage.other.insert(key, value);
                }
            }
        }
        usage
    }

    /// The object as the provider wrote it.
    pub fn to_value(&self) -> Value {
        let mut object = self.other.clone();
        for (name, slot) in COUNTERS.iter().zip(self.slots()) {
            if let Some(value) = slot {
                object.insert((*name).to_string(), value.clone());
            }
        }
        Value::Object(object)
    }
}

impl Serialize for TokenUsage {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        for (name, slot) in COUNTERS.iter().zip(self.slots()) {
            if let Some(value) = slot {
                map.serialize_entry(name, value)?;
            }
        }
        for (key, value) in &self.other {
            map.serialize_entry(key, value)?;
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for TokenUsage {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Map::deserialize(deserializer).map(Self::from_object)
    }
}

/// One Codex `token_count` snapshot: the provider's `info` object, typed.
///
/// `total_token_usage` is the session's cumulative spend at that point;
/// `last_token_usage` the request that produced the snapshot. A usage key
/// written as something other than an object stays in `other` under its own
/// name, as does every key this type does not name.
///
/// Serializes as the `info` object it was read from.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct UsageSnapshot {
    pub total_token_usage: Option<TokenUsage>,
    pub last_token_usage: Option<TokenUsage>,
    /// As written: a JSON number when Codex reports it.
    pub model_context_window: Option<Value>,
    /// Every other key, as written.
    pub other: Map<String, Value>,
}

impl UsageSnapshot {
    /// Type an `info` object; `None` for anything else.
    pub(crate) fn from_info(info: &Value) -> Option<Self> {
        info.as_object().cloned().map(Self::from_object)
    }

    fn from_object(object: Map<String, Value>) -> Self {
        let mut snapshot = Self::default();
        for (key, value) in object {
            match (key.as_str(), value) {
                ("total_token_usage", Value::Object(usage)) => {
                    snapshot.total_token_usage = Some(TokenUsage::from_object(usage));
                }
                ("last_token_usage", Value::Object(usage)) => {
                    snapshot.last_token_usage = Some(TokenUsage::from_object(usage));
                }
                ("model_context_window", value) => snapshot.model_context_window = Some(value),
                (_, value) => {
                    snapshot.other.insert(key, value);
                }
            }
        }
        snapshot
    }

    /// The `info` object as the provider wrote it.
    pub fn to_value(&self) -> Value {
        let mut object = self.other.clone();
        if let Some(usage) = &self.total_token_usage {
            object.insert("total_token_usage".into(), usage.to_value());
        }
        if let Some(usage) = &self.last_token_usage {
            object.insert("last_token_usage".into(), usage.to_value());
        }
        if let Some(window) = &self.model_context_window {
            object.insert("model_context_window".into(), window.clone());
        }
        Value::Object(object)
    }
}

impl Serialize for UsageSnapshot {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        if let Some(usage) = &self.total_token_usage {
            map.serialize_entry("total_token_usage", usage)?;
        }
        if let Some(usage) = &self.last_token_usage {
            map.serialize_entry("last_token_usage", usage)?;
        }
        if let Some(window) = &self.model_context_window {
            map.serialize_entry("model_context_window", window)?;
        }
        for (key, value) in &self.other {
            map.serialize_entry(key, value)?;
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for UsageSnapshot {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Map::deserialize(deserializer).map(Self::from_object)
    }
}

// ---------------------------------------------------------------------------
// Stored form
// ---------------------------------------------------------------------------

/// The stored `payload_json` of a snapshot whose `info` is `info` (already
/// bounded per the marker contract).
pub(crate) fn encode(info: &Value) -> serde_json::Result<String> {
    serde_json::to_string(&Stored(info))
}

/// What a stored `usage_snapshot` payload decodes to.
#[derive(Debug, PartialEq)]
pub(crate) enum Decoded {
    Snapshot(Box<UsageSnapshot>),
    /// An `info` that is not an object, kept whole.
    Other(Value),
}

/// Decode a stored `usage_snapshot` payload: the compact form, or `None` when
/// `raw` is not one (a payload an older development build stored as the
/// `info` object itself is typed by the caller).
pub(crate) fn decode(raw: &str) -> Option<Decoded> {
    if !raw.starts_with('[') {
        return None;
    }
    serde_json::from_str::<StoredSnapshot>(raw)
        .ok()
        .map(|stored| stored.0)
}

struct Stored<'a>(&'a Value);

/// One usage object in its stored form.
struct StoredUsage<'a>(&'a Map<String, Value>);

fn null() -> &'static Value {
    static NULL: Value = Value::Null;
    &NULL
}

impl Serialize for Stored<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let Some(info) = self.0.as_object() else {
            let mut seq = serializer.serialize_seq(Some(5))?;
            for _ in 0..4 {
                seq.serialize_element(null())?;
            }
            seq.serialize_element(self.0)?;
            return seq.end();
        };
        let usage = |key: &str| info.get(key).and_then(Value::as_object);
        let total = usage("total_token_usage");
        let last = usage("last_token_usage");
        let window = info.get("model_context_window").filter(|v| !v.is_null());
        let other: Map<String, Value> = info
            .iter()
            .filter(|(key, value)| match key.as_str() {
                "total_token_usage" | "last_token_usage" => !value.is_object(),
                "model_context_window" => value.is_null(),
                _ => true,
            })
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        let len = if !other.is_empty() {
            4
        } else if window.is_some() {
            3
        } else if last.is_some() {
            2
        } else if total.is_some() {
            1
        } else {
            0
        };
        let mut seq = serializer.serialize_seq(Some(len))?;
        if len > 0 {
            match total {
                Some(usage) => seq.serialize_element(&StoredUsage(usage))?,
                None => seq.serialize_element(null())?,
            }
        }
        if len > 1 {
            match last {
                Some(usage) => seq.serialize_element(&StoredUsage(usage))?,
                None => seq.serialize_element(null())?,
            }
        }
        if len > 2 {
            seq.serialize_element(window.unwrap_or(null()))?;
        }
        if len > 3 {
            seq.serialize_element(&other)?;
        }
        seq.end()
    }
}

impl Serialize for StoredUsage<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let usage = self.0;
        let slots = COUNTERS.map(|name| usage.get(name).filter(|value| !value.is_null()));
        let rest: Map<String, Value> = usage
            .iter()
            .filter(|(key, value)| !COUNTERS.contains(&key.as_str()) || value.is_null())
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        let len = if rest.is_empty() {
            slots.iter().rposition(Option::is_some).map_or(0, |i| i + 1)
        } else {
            7
        };
        let mut seq = serializer.serialize_seq(Some(len))?;
        for slot in slots.iter().take(len.min(6)) {
            seq.serialize_element(slot.unwrap_or(null()))?;
        }
        if len == 7 {
            seq.serialize_element(&rest)?;
        }
        seq.end()
    }
}

struct StoredSnapshot(Decoded);

impl<'de> Deserialize<'de> for StoredSnapshot {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct SnapshotVisitor;
        impl<'de> Visitor<'de> for SnapshotVisitor {
            type Value = StoredSnapshot;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a stored usage snapshot array")
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let mut snapshot = UsageSnapshot {
                    total_token_usage: seq
                        .next_element::<Option<StoredTokenUsage>>()?
                        .flatten()
                        .map(|u| u.0),
                    ..Default::default()
                };
                snapshot.last_token_usage = seq
                    .next_element::<Option<StoredTokenUsage>>()?
                    .flatten()
                    .map(|u| u.0);
                snapshot.model_context_window = seq.next_element::<Option<Value>>()?.flatten();
                if let Some(other) = seq.next_element::<Option<Map<String, Value>>>()?.flatten() {
                    for (key, value) in other {
                        if key == "model_context_window" {
                            snapshot.model_context_window = Some(value);
                        } else {
                            snapshot.other.insert(key, value);
                        }
                    }
                }
                if let Some(info) = seq.next_element::<Option<Value>>()?.flatten() {
                    return Ok(StoredSnapshot(Decoded::Other(info)));
                }
                if seq.next_element::<de::IgnoredAny>()?.is_some() {
                    return Err(de::Error::invalid_length(6, &self));
                }
                Ok(StoredSnapshot(Decoded::Snapshot(Box::new(snapshot))))
            }
        }
        deserializer.deserialize_seq(SnapshotVisitor)
    }
}

struct StoredTokenUsage(TokenUsage);

impl<'de> Deserialize<'de> for StoredTokenUsage {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct UsageVisitor;
        impl<'de> Visitor<'de> for UsageVisitor {
            type Value = StoredTokenUsage;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a stored token usage array")
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let mut usage = TokenUsage::default();
                for index in 0..COUNTERS.len() {
                    match seq.next_element::<Option<Value>>()? {
                        Some(slot) => *usage.slot_mut(index) = slot,
                        None => return Ok(StoredTokenUsage(usage)),
                    }
                }
                if let Some(rest) = seq.next_element::<Map<String, Value>>()? {
                    for (key, value) in rest {
                        match COUNTERS.iter().position(|name| *name == key) {
                            Some(index) => *usage.slot_mut(index) = Some(value),
                            None => {
                                usage.other.insert(key, value);
                            }
                        }
                    }
                }
                if seq.next_element::<de::IgnoredAny>()?.is_some() {
                    return Err(de::Error::invalid_length(8, &self));
                }
                Ok(StoredTokenUsage(usage))
            }
        }
        deserializer.deserialize_seq(UsageVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn round_trip(info: Value) -> String {
        let stored = encode(&info).unwrap();
        let decoded = decode(&stored).expect("compact form");
        let back = match decoded {
            Decoded::Snapshot(snapshot) => {
                // The typed form serializes as the object it came from.
                assert_eq!(serde_json::to_value(&snapshot).unwrap(), info);
                let typed: UsageSnapshot = serde_json::from_value(info.clone()).unwrap();
                assert_eq!(typed, *snapshot);
                snapshot.to_value()
            }
            Decoded::Other(value) => value,
        };
        assert_eq!(back, info, "stored as {stored}");
        stored
    }

    #[test]
    fn a_codex_snapshot_stores_its_counters_by_position() {
        let info = json!({
            "total_token_usage": {"input_tokens": 77135, "cached_input_tokens": 38144,
                "cache_write_input_tokens": 0, "output_tokens": 175,
                "reasoning_output_tokens": 0, "total_tokens": 77310},
            "last_token_usage": {"input_tokens": 38714, "cached_input_tokens": 38144,
                "cache_write_input_tokens": 0, "output_tokens": 89,
                "reasoning_output_tokens": 0, "total_tokens": 38803},
            "model_context_window": 258400
        });
        assert_eq!(
            round_trip(info),
            "[[77135,38144,0,175,0,77310],[38714,38144,0,89,0,38803],258400]"
        );
    }

    #[test]
    fn counters_are_exact_whatever_their_range() {
        let info = json!({
            "total_token_usage": {
                "input_tokens": u64::MAX,
                "cached_input_tokens": 9_223_372_036_854_775_808u64,
                "cache_write_input_tokens": 0,
                "output_tokens": 1.5,
                "reasoning_output_tokens": -3,
                "total_tokens": 18_446_744_073_709_551_000u64
            },
            "model_context_window": 400_000
        });
        assert_eq!(
            round_trip(info),
            "[[18446744073709551615,9223372036854775808,0,1.5,-3,18446744073709551000],null,400000]"
        );
    }

    #[test]
    fn absent_and_null_counters_stay_distinct() {
        // Older Codex writes no cache-write counter; an explicit null is kept
        // as one.
        assert_eq!(
            round_trip(json!({"total_token_usage": {"input_tokens": 10, "total_tokens": 10}})),
            "[[10,null,null,null,null,10]]"
        );
        assert_eq!(
            round_trip(json!({"total_token_usage": {"input_tokens": 10, "output_tokens": null}})),
            "[[10,null,null,null,null,null,{\"output_tokens\":null}]]"
        );
        assert_eq!(
            round_trip(json!({"model_context_window": null})),
            "[null,null,null,{\"model_context_window\":null}]"
        );
        assert_eq!(round_trip(json!({"total_token_usage": {}})), "[[]]");
        assert_eq!(round_trip(json!({})), "[]");
    }

    #[test]
    fn unknown_and_malformed_keys_are_kept_whole() {
        round_trip(json!({
            "total_token_usage": {"input_tokens": "12", "cache_write_5m_tokens": 4, "nested": {"a": [1, 2]}},
            "last_token_usage": null,
            "model_context_window": "big",
            "rate_limits": {"primary": {"used_percent": 19.0}},
        }));
        round_trip(json!({"total_token_usage": [1, 2, 3], "last_token_usage": 7}));
        // An `info` that is not an object at all.
        assert_eq!(round_trip(json!([1, 2])), "[null,null,null,null,[1,2]]");
        assert_eq!(round_trip(json!("text")), "[null,null,null,null,\"text\"]");
    }

    #[test]
    fn an_object_payload_is_not_the_compact_form() {
        assert_eq!(decode("{\"total_token_usage\":{}}"), None);
    }
}

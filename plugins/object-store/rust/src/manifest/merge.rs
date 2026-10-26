//! Writing one section while preserving fields this version does not know.
//!
//! A writer's edit replaces its own section with a typed struct. Keys a newer
//! writer added, at any depth this crate models (the section itself,
//! `server.normalized`, `server.retention`, each `raw.chunks[]` and
//! `raw.epochs[]` entry), are carried over from the stored copy unless the
//! edit sets them. Known fields always follow the edit, so clearing an
//! optional field works. Only explicitly listed top-level unknown keys are
//! deleted.

use std::collections::HashMap;

use serde_json::{Map, Value};

use crate::{Error, Result};

type Doc = Map<String, Value>;

/// Fields this version models at each path.
fn known_fields(path: &str) -> &'static [&'static str] {
    match path {
        "session" => &[
            "workspace_id",
            "machine_id",
            "harness",
            "session_id",
            "started_at",
            "ended_at",
            "harness_version",
            "cwd_sha256",
            "parent_session_id",
        ],
        "writer" => &["delivery_instance_id", "plugin_version", "host_version"],
        "raw" => &[
            "owner",
            "state",
            "updated_at",
            "format",
            "compression",
            "chunk_count",
            "raw_bytes",
            "stored_bytes",
            "content_sha256",
            "epoch",
            "epochs",
            "next_offset",
            "chunks",
            "scrub",
        ],
        "raw.scrub" => &["ruleset", "version"],
        "raw.chunks[]" => &[
            "seq",
            "epoch",
            "start_offset",
            "bytes",
            "stored_bytes",
            "sha256",
            "sha256_8",
            "stored_sha256",
            "stored_raw_bytes",
            "raw_sha256",
            "compression",
            "records",
            "first_ts",
            "last_ts",
        ],
        "raw.epochs[]" => &["epoch", "opened_at", "reason", "first_seq", "opened_by"],
        "server" => &[
            "updated_at",
            "normalized",
            "lifecycle",
            "lifecycle_at",
            "lifecycle_reason",
            "retention",
        ],
        "server.normalized" => &[
            "version",
            "source_content_sha256",
            "source_epoch",
            "built_at",
            "manifest_key",
        ],
        "server.retention" => &["delete_after", "legal_hold"],
        _ => &[],
    }
}

/// Nested objects (or arrays of objects, keyed by an identity field) that
/// are merged recursively under `path`.
fn nested(path: &str) -> &'static [(&'static str, Option<&'static str>)] {
    match path {
        "raw" => &[
            ("chunks", Some("seq")),
            ("epochs", Some("epoch")),
            ("scrub", None),
        ],
        "server" => &[("normalized", None), ("retention", None)],
        _ => &[],
    }
}

/// Replace `doc[name]` with `section`, carrying over unknown fields from the
/// stored copy and deleting the top-level unknown keys in `remove`.
pub(crate) fn put_section<T: serde::Serialize>(
    doc: &mut Doc,
    name: &str,
    section: &T,
    remove: &[String],
) -> Result<()> {
    let mut value =
        serde_json::to_value(section).map_err(|e| Error::InvalidArgument(e.to_string()))?;
    if let Some(stored) = doc.get(name) {
        carry_unknown(stored, &mut value, name);
    }
    if let Value::Object(next) = &mut value {
        let known = known_fields(name);
        next.retain(|k, _| known.contains(&k.as_str()) || !remove.contains(k));
    }
    doc.insert(name.to_string(), value);
    Ok(())
}

fn carry_unknown(stored: &Value, next: &mut Value, path: &str) {
    let (Value::Object(stored), Value::Object(next)) = (stored, next) else {
        return;
    };
    let known = known_fields(path);
    for (k, v) in stored {
        if !known.contains(&k.as_str()) && !next.contains_key(k) {
            next.insert(k.clone(), v.clone());
        }
    }
    for (field, identity) in nested(path) {
        let (Some(old), Some(new)) = (stored.get(*field), next.get_mut(*field)) else {
            continue;
        };
        match identity {
            None => carry_unknown(old, new, &format!("{path}.{field}")),
            Some(id) => carry_unknown_in_array(old, new, &format!("{path}.{field}[]"), id),
        }
    }
}

/// Match array entries by their identity field (`seq` for chunks, `epoch`
/// for epochs), never by position, so a mismatched list cannot graft one
/// entry's fields onto another. Stored entries are indexed once, so a
/// 10,000-chunk manifest costs O(n), not O(n²), per write.
fn carry_unknown_in_array(stored: &Value, next: &mut Value, path: &str, id: &str) {
    let (Value::Array(stored), Value::Array(next)) = (stored, next) else {
        return;
    };
    // Entries whose identity is not an unsigned integer cannot be matched
    // and are skipped (the validator rejects them anyway).
    let by_id: HashMap<u64, &Value> = stored
        .iter()
        .filter_map(|o| Some((o.get(id)?.as_u64()?, o)))
        .collect();
    for entry in next.iter_mut() {
        let Some(old) = entry
            .get(id)
            .and_then(Value::as_u64)
            .and_then(|k| by_id.get(&k))
        else {
            continue;
        };
        carry_unknown(old, entry, path);
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn nested_unknown_fields_survive_a_fresh_struct() {
        let mut doc: Doc = serde_json::from_value(json!({
            "server": {
                "lifecycle": "idle",
                "normalized": {"version": 1, "future_n": "keep"},
                "retention": {"legal_hold": true, "future_r": 1},
                "future_s": [1]
            },
            "raw": {
                "chunks": [{"seq": 0, "future_c": "c0"}, {"seq": 1, "future_c": "c1"}],
                "epochs": [{"epoch": 0, "future_e": "e0"}]
            }
        }))
        .unwrap();
        let server = json!({
            "lifecycle": "active",
            "normalized": {"version": 2},
            "retention": {"legal_hold": false}
        });
        put_section(&mut doc, "server", &server, &[]).unwrap();
        let s = &doc["server"];
        assert_eq!(s["future_s"], json!([1]));
        assert_eq!(s["normalized"]["future_n"], "keep");
        assert_eq!(
            s["normalized"]["version"], 2,
            "known fields follow the edit"
        );
        assert_eq!(s["retention"]["future_r"], 1);

        // Chunks matched by `seq`, not position; a new chunk gets nothing.
        let raw = json!({
            "chunks": [{"seq": 1}, {"seq": 0}, {"seq": 2}],
            "epochs": [{"epoch": 0}, {"epoch": 1}]
        });
        put_section(&mut doc, "raw", &raw, &[]).unwrap();
        let r = &doc["raw"];
        assert_eq!(r["chunks"][0]["future_c"], "c1");
        assert_eq!(r["chunks"][1]["future_c"], "c0");
        assert!(r["chunks"][2].get("future_c").is_none());
        assert_eq!(r["epochs"][0]["future_e"], "e0");
        assert!(r["epochs"][1].get("future_e").is_none());
    }

    #[test]
    fn large_chunk_lists_merge_in_linear_time() {
        let n = 20_000u64;
        let chunk = |seq: u64, extra: bool| {
            let mut c = json!({"seq": seq, "bytes": 1});
            if extra {
                c["future"] = json!(seq);
            }
            c
        };
        let stored: Vec<Value> = (0..n).map(|s| chunk(s, true)).collect();
        let mut doc: Doc = serde_json::from_value(json!({"raw": {"chunks": stored}})).unwrap();
        let next: Vec<Value> = (0..=n).map(|s| chunk(s, false)).collect();
        let started = std::time::Instant::now();
        put_section(&mut doc, "raw", &json!({"chunks": next}), &[]).unwrap();
        // Quadratic matching took seconds at this size; linear takes milliseconds.
        assert!(started.elapsed() < std::time::Duration::from_secs(2));
        assert_eq!(doc["raw"]["chunks"][(n - 1) as usize]["future"], n - 1);
        assert!(doc["raw"]["chunks"][n as usize].get("future").is_none());
    }

    #[test]
    fn listed_top_level_unknown_keys_are_removed() {
        let mut doc: Doc =
            serde_json::from_value(json!({"server": {"lifecycle": "idle", "x": 1, "y": null}}))
                .unwrap();
        put_section(
            &mut doc,
            "server",
            &json!({"lifecycle": "idle"}),
            &["x".into()],
        )
        .unwrap();
        assert!(doc["server"].get("x").is_none());
        assert!(doc["server"].as_object().unwrap().contains_key("y"));
    }
}

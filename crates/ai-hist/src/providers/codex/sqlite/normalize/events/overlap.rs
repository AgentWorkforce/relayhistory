use super::*;
use anyhow::Context;
use std::collections::{HashMap, HashSet};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(in super::super) enum Kind {
    User,
    Assistant,
    Image,
}

impl Kind {
    fn classify(
        uid: &str,
        message: &str,
        provider: Option<&str>,
        role: &str,
        raw: Option<&str>,
    ) -> Option<Self> {
        if provider.is_some_and(|id| !id.is_empty()) {
            return None;
        }
        match (role, raw) {
            ("user", None) if message == uid => Some(Self::User),
            ("assistant", None) if message == uid => Some(Self::Assistant),
            ("user", Some("input_image")) if Self::legacy_image(uid, message) => Some(Self::Image),
            _ => None,
        }
    }

    fn legacy_image(uid: &str, message: &str) -> bool {
        let Some((line, part)) = uid.split_once(":input_image:") else {
            return false;
        };
        let (Ok(line), Ok(_)) = (line.parse::<u64>(), part.parse::<u64>()) else {
            return false;
        };
        if let Some(parent) = message.strip_suffix(":response_item_user_message") {
            return parent.parse::<u64>().ok() == Some(line);
        }
        message
            .strip_suffix(":user_message")
            .and_then(|parent| parent.parse::<u64>().ok())
            .and_then(|parent| parent.checked_add(1))
            == Some(line)
    }

    pub(in super::super) fn role(self) -> &'static str {
        match self {
            Self::User | Self::Image => "user",
            Self::Assistant => "assistant",
        }
    }

    fn raw(self) -> Option<&'static str> {
        (self == Self::Image).then_some("input_image")
    }
}

/// One reconciliation's live legacy claims. Never retained across a transaction
/// or pass: retirement and retention can invalidate the referenced events.
pub(in super::super) struct Overlaps {
    bindings: HashMap<String, (String, Kind)>,
    claimed: HashMap<String, usize>,
    invalid: HashSet<String>,
}

impl Overlaps {
    pub(in super::super) fn load(conn: &Connection, session: &str) -> Result<Self> {
        let mut overlaps = Self {
            bindings: HashMap::new(),
            claimed: HashMap::new(),
            invalid: HashSet::new(),
        };
        let mut stmt = conn.prepare(
            "SELECT m.marker_uid,e.event_uid,e.message_id,e.provider_message_id,e.role,e.kind,e.raw_kind \
             FROM session_markers m LEFT JOIN session_events e \
             ON e.source=m.source AND e.session_id=m.session_id \
             AND e.event_uid=CAST(json_extract(m.payload_json,'$.rollout_event_uid') AS TEXT) \
             WHERE m.source='codex' AND m.session_id=?1 AND m.marker_uid GLOB 'sqlite:overlap:*' \
             AND m.kind='rollout_overlap' AND m.subkind='sqlite'",
        )?;
        let mut rows = stmt.query([session])?;
        while let Some(row) = rows.next()? {
            let marker: String = row.get(0)?;
            let uid: Option<String> = row.get(1)?;
            let message: Option<String> = row.get(2)?;
            let provider: Option<String> = row.get(3)?;
            let role: Option<String> = row.get(4)?;
            let kind: Option<String> = row.get(5)?;
            let raw: Option<String> = row.get(6)?;
            let classified = uid
                .as_deref()
                .zip(message.as_deref())
                .zip(role.as_deref())
                .filter(|_| kind.as_deref() == Some("text"))
                .and_then(|((uid, message), role)| {
                    Kind::classify(uid, message, provider.as_deref(), role, raw.as_deref())
                });
            if let (Some(uid), Some(kind)) = (uid, classified) {
                overlaps.bind(marker, uid, kind);
            } else {
                overlaps.invalid.insert(marker);
            }
        }
        Ok(overlaps)
    }

    fn bind(&mut self, marker: String, target: String, kind: Kind) {
        *self.claimed.entry(target.clone()).or_default() += 1;
        self.bindings.insert(marker, (target, kind));
    }

    pub(in super::super) fn claimed(&self, target: &str) -> bool {
        self.claimed.contains_key(target)
    }

    pub(in super::super) fn mapped(
        &mut self,
        conn: &Connection,
        t: &Thread,
        item: &Item,
        marker: &str,
        kind: Kind,
        text: &str,
    ) -> Result<bool> {
        if let Some((uid, prior_kind)) = self.bindings.get(marker) {
            let target: Option<(String, Option<String>)> = conn.query_row(
                "SELECT message_id,provider_message_id FROM session_events \
                 WHERE source='codex' AND session_id=?1 AND event_uid=?2 AND role=?3 AND kind='text' \
                 AND raw_kind IS ?4 AND text=?5 AND ts_ms BETWEEN ?6 AND ?7",
                params![t.id,uid,kind.role(),kind.raw(),text,
                    item.ts.saturating_sub(IDLESS_ROLLOUT_TIMESTAMP_TOLERANCE_MS),
                    item.ts.saturating_add(IDLESS_ROLLOUT_TIMESTAMP_TOLERANCE_MS)],
                |row| Ok((row.get(0)?,row.get(1)?)),
            ).optional()?;
            if *prior_kind == kind
                && target.is_some_and(|(message, provider)| {
                    Kind::classify(uid, &message, provider.as_deref(), kind.role(), kind.raw())
                        == Some(kind)
                })
            {
                return Ok(true);
            }
        } else if !self.invalid.contains(marker) {
            return Ok(false);
        }
        crate::store::retire_evidence_share(
            conn, "session_markers",
            "source='codex' AND session_id=?1 AND marker_uid=?2 AND kind='rollout_overlap' AND subkind='sqlite'",
            params![t.id,marker], crate::SessionLocation::Local,
        )?;
        self.invalid.remove(marker);
        if let Some((uid, _)) = self.bindings.remove(marker) {
            let count = self
                .claimed
                .get_mut(&uid)
                .context("Codex overlap binding has no claim")?;
            *count = count
                .checked_sub(1)
                .context("Codex overlap claim count is empty")?;
            if *count == 0 {
                self.claimed.remove(&uid);
            }
        }
        Ok(false)
    }

    pub(in super::super) fn claim(
        &mut self,
        conn: &Connection,
        t: &Thread,
        item: &Item,
        uid: &str,
        kind: Kind,
        target: String,
    ) -> Result<()> {
        marker(
            conn,
            t,
            uid,
            item.ts,
            "rollout_overlap",
            &json!({"rollout_event_uid":target}),
            item.turn_id.as_deref(),
        )?;
        self.bind(uid.to_owned(), target, kind);
        Ok(())
    }

    pub(in super::super) fn legacy_image(uid: &str, message: &str, provider: Option<&str>) -> bool {
        Kind::classify(uid, message, provider, "user", Some("input_image")) == Some(Kind::Image)
    }
}

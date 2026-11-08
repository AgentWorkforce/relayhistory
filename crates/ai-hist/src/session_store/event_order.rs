//! Preserve recorded ordering facts on the public block surface.
use super::*;
pub(super) fn block_from_event(event: &SessionEvent, text_bytes: Option<i64>) -> Block {
    Block {
        source_order: event.source_order,
        event_uid: event.event_uid.clone(),
        role: Role::parse(&event.role),
        kind: BlockKind::parse(&event.kind),
        text: event.text.clone(),
        text_bytes,
        tool_use_id: event.tool_use_id.clone(),
        raw_kind: event.raw_kind.clone(),
        control: event.control_kind.as_deref().and_then(ControlKind::parse),
        ts_ms: event.ts_ms,
    }
}

//! Full-session transaction/order scopes for the legacy snapshot readers.
use super::*;

pub(super) fn grok<'a>(
    conn: &'a Connection,
    sid: &str,
    path: &str,
) -> Result<(native_order::Capture<'a>, evidence_reconcile::Guard)> {
    let order = native_order::begin(conn, "grok", Some(sid))?;
    let reconcile = grok_unified::begin_session(conn, sid, path)?;
    Ok((order, reconcile))
}
pub(super) fn muse<'a>(
    conn: &'a Connection,
    sid: &str,
    scope: HistoryScope,
) -> Result<(native_order::Capture<'a>, evidence_reconcile::Guard)> {
    let order = native_order::begin(conn, "muse", Some(sid))?;
    let reconcile = evidence_reconcile::begin_muse(conn, sid, scope)?;
    Ok((order, reconcile))
}
pub(super) fn finish<T>(order: native_order::Capture<'_>, outcome: T) -> Result<T> {
    order.finish()?;
    Ok(outcome)
}

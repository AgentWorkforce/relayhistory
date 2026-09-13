//! Optional RelayHistory authentication, legacy mapping and transports.
//! Local history packages never depend on this crate.
pub mod cloud;
pub mod convergence;
pub mod helper;
pub mod outbox;
pub mod replay;
pub mod turns;
fn parse_iso_ms(value: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|date| date.timestamp_millis())
}

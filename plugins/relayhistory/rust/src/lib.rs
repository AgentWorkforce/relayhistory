//! Optional RelayHistory authentication, legacy mapping and transports.
//! Local history packages never depend on this crate.
pub mod cloud;
pub mod compat;
pub mod convergence;
pub mod destination;
pub mod helper;
pub mod legacy_cli;
pub mod migration;
pub mod outbox;
pub mod relaycast;
pub mod replay;
pub mod source;
pub mod turns;
fn parse_iso_ms(value: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|date| date.timestamp_millis())
}

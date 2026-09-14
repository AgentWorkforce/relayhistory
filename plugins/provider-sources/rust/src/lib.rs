//! Optional provider-native source adapters; explicitly compose them with local history.
pub use ai_hist_engine::discover;
pub use ai_hist_engine::sources;
pub mod helper;
pub mod remote;
fn home_dir() -> std::path::PathBuf {
    std::env::var_os("HOME")
        .map(Into::into)
        .unwrap_or_else(|| ".".into())
}
fn parse_iso_ms(value: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|date| date.timestamp_millis())
}

//! The `history-provider-sources` helper executable's library half: the JSON
//! stdin/stdout protocol in [`helper`]. The connectors themselves live in the
//! `ai-hist` crate behind its `remote-sources` feature; they are re-exported
//! here under their historical paths so existing `history_provider_sources::`
//! imports keep compiling. A Rust host depends on `ai-hist` with that feature
//! instead of on this crate, which is not published to crates.io.
pub use ai_hist::discover;
pub use ai_hist::remote_sources as remote;
pub use ai_hist::sources;
pub mod helper;

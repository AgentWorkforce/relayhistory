//! `manifest.json` schema v1: types, validation, and the two-writer
//! compare-and-swap protocol (`docs/manifest-v1.md`).
//!
//! - [`types`]: serde types that preserve unknown fields.
//! - [`validate`]: every reader-side invariant, plus the `content_sha256` chain.
//! - [`update`]: [`update_manifest`], the read/edit/conditional-write loop.
//! - [`client`]: desktop helpers (first manifest, chunks, epochs, stale writers).
//! - [`server`]: server section, lifecycle, pointer-row fields.

pub mod client;
mod merge;
pub mod server;
pub mod types;
pub mod update;
pub mod validate;

pub use types::*;
pub use update::{update_manifest, ClientRemovals, Edit, UpdateOpts, Updated};
pub use validate::{content_sha256, ensure_valid, parse_manifest, validate, Issue};

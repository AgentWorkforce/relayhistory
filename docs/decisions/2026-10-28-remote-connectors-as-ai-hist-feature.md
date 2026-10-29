# Remote connectors ship in `ai-hist` behind the `remote-sources` feature

- **Status:** Accepted
- **Date:** 2026-10-28
- **Code:** [`crates/ai-hist/src/remote_sources.rs`](../../crates/ai-hist/src/remote_sources.rs),
  [`plugins/provider-sources/rust`](../../plugins/provider-sources/rust)

## Context

The `claude-web`, `codex-cloud` and `devin-cloud` connectors lived only in
`history-provider-sources` (`plugins/provider-sources/rust`), which depends on
`ai-hist` by path with `unstable-internal` and was never published to
crates.io. A Rust host that runs the connectors in-process — the Agent Relay
desktop probe — therefore had to pin **both** `ai-hist` and
`history-provider-sources` to one relayhistory git revision: the connectors
carry `ai-hist` types, so registry `ai-hist` plus git `history-provider-sources`
would build two incompatible copies of the crate. Every relayhistory release
meant a manual git-rev bump in the host instead of a version bump.

The architecture until now also said the local engine contains no remote
transports, and CI proved it by building the core with `plugins/` deleted.

## Decision

1. The connector modules move into `ai-hist` as `ai_hist::remote_sources`,
   compiled only under a new opt-in feature, `remote-sources`, which adds the
   `ureq` HTTP client as an optional dependency. A host writes one registry
   dependency: `ai-hist = { version = "=X.Y.Z", features = ["remote-sources"] }`.
2. `remote-sources` implies `unstable-internal`. Connectors are
   `discover::ShallowSessionProvider`s over a borrowed `DiscoveryEnv`, and
   hosts normalize their evidence with `sources::normalize_source_evidence`;
   all of that is unstable-internal surface. The feature is therefore **not
   covered by semver**, and a host that enables it pins `ai-hist` exactly. The
   default-feature public API (and its snapshot) does not change.
3. The name is `remote_sources`, not `remote`: `ai_hist::remote` is already the
   transport-free selector the local engine uses to refuse remote work it was
   not composed with, and the CLI and napi depend on it.
4. `history-provider-sources` stays, as a thin `publish = false` crate: the
   helper executable's JSON protocol (`helper`) plus re-exports of
   `ai_hist::remote_sources` as `remote` (and `discover`, `sources`) so
   existing import paths keep compiling. The npm plugin packaging, release
   version stamping and helper matrix are unchanged.
5. The transport boundary is now enforced on the dependency graph instead of
   the directory tree: CI fails if `ai-hist-cli`, `ai-hist-napi`, or `ai-hist`
   with every feature except `remote-sources`, resolves `ureq`.

## Alternatives rejected

**Publish `history-provider-sources` to crates.io**, pinned
`ai-hist = "=X.Y.Z"` and published after `ai-hist` in `publish-crate`. Keeps
the code where it was, but a host still writes two lockstep dependencies, the
release gains a second crates.io publish that can fail after the first, and a
new crate name needs a human first publish before trusted publishing can own
it. It solves the git pin, not the "one crate" ask. Rejected.

**Make `ai_hist::remote` the real connectors when the feature is on.** The
CLI and napi call that module's selector and refusal functions; swapping its
behaviour under feature unification would let any crate in a host's graph
turn remote acquisition on inside the local engine. Rejected.

**Expose connectors without `unstable-internal`.** They cannot be: the trait
they implement and the environment they borrow are engine internals, and
stabilising them is a facade change of its own.

## Consequences

- The probe drops its git pins; a relayhistory release is a version bump.
- `ureq` and `rustls` enter the root lockfile and the MSRV check
  (`--all-features`), but no default, CLI or native build compiles them.
- The connector unit tests run in the workspace (`cargo test --workspace
  --all-features`); the helper's protocol tests stay in the plugin crate.
- The appendix in [`sourcing-sdk.md`](../sourcing-sdk.md) names
  `remote-sources` as the one sanctioned route to `unstable-internal` from
  outside this repository.

# ai-hist

Local coding-agent session history for Claude Code, Codex, Cursor, Grok, OpenCode, and Agent Relay.

```rust,no_run
use ai_hist::SessionStore;

fn main() -> Result<(), ai_hist::Error> {
    let store = SessionStore::open(Default::default())?;
    let _report = store.sync(Default::default())?;
    let _turns = store.session_user_turns_page(ai_hist::Source::Claude, "session-id", 100, None)?;
    Ok(())
}
```

Default features expose `SessionStore`, evidence structs, `Source`, and `Error`. Optional features: `delivery`, `opencode-backup`, `git-hooks`. Workspace crates enable `unstable-internal` for connection-level maintenance APIs.

The embedder guide — store options, the sync lifecycle and its locking, the evidence model, accounting semantics, versioning and feature flags — is [`docs/sourcing-sdk.md`](https://github.com/AgentWorkforce/relayhistory/blob/main/docs/sourcing-sdk.md), and [`examples/rust-consumer`](https://github.com/AgentWorkforce/relayhistory/tree/main/examples/rust-consumer) is a standalone project that consumes this crate from crates.io. Cargo semver is the contract; the crate's default-feature public API is snapshotted in the repository at `crates/ai-hist/public-api.txt` and diffed in CI.

See the [repository README](https://github.com/AgentWorkforce/relayhistory) for CLI and Node usage.

# ai-hist

Local coding-agent session history for Claude Code, Codex, Cursor, Grok, OpenCode, and Agent Relay.

```rust,no_run
use ai_hist::SessionStore;

fn main() -> Result<(), ai_hist::Error> {
    let store = SessionStore::open(Default::default())?;
    let _report = store.sync(Default::default())?;
    Ok(())
}
```

Default features expose `SessionStore`, evidence structs, `Source`, and `Error`. Optional features: `delivery`, `opencode-backup`, `git-hooks`. Workspace crates enable `unstable-internal` for connection-level maintenance APIs.

See the [repository README](https://github.com/AgentWorkforce/relayhistory) for CLI and Node usage.

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

Default features expose `SessionStore`, evidence structs, `Source`, and `Error`. Optional features: `export` (consistent snapshots and change capture), `opencode-backup`, `git-hooks`. The legacy `delivery` feature aliases `export`; upload jobs and workers are owned by the probe package, `plugins/relayhistory/rust`. Workspace crates enable `unstable-internal` for connection-level maintenance APIs.

See the [repository README](https://github.com/AgentWorkforce/relayhistory) for CLI and Node usage.

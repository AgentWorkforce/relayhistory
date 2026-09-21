# ai-hist

Local coding-agent session history for Claude Code, Codex, Cursor, Grok, OpenCode, and Agent Relay.

```rust,no_run
use ai_hist::SessionStore;

fn main() -> Result<(), ai_hist::Error> {
    let store = SessionStore::open(Default::default())?;
    let _report = store.sync(Default::default())?;
    let _turns = store.session_user_turns_page(ai_hist::Source::Claude, "session-id", 100, None)?;

    // Incremental: everything since this consumer's last commit, oldest first.
    let query = ai_hist::ChangeQuery::default().consumer("my-ingest");
    let mut changes = store.changes_since(ai_hist::Watermark::CONSUMER, query)?;
    for change in changes.by_ref() {
        let _change = change?; // `Upsert(EvidenceRow)` or `Delete`, keyed by kind + record_key
    }
    changes.commit()?; // the cursor moves only here
    Ok(())
}
```

Default features expose `SessionStore`, evidence structs, `Source`, and `Error`. Optional features: `delivery`, `opencode-backup`, `git-hooks`. Workspace crates enable `unstable-internal` for connection-level maintenance APIs.

See the [repository README](https://github.com/AgentWorkforce/relayhistory) for CLI and Node usage.

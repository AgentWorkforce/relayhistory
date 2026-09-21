# ai-hist

Local coding-agent session history for Claude Code, Codex, Cursor, Grok, OpenCode, and Agent Relay.

```rust,no_run
use ai_hist::{CatalogQuery, SessionQuery, SessionStore};

fn main() -> Result<(), ai_hist::Error> {
    let store = SessionStore::open(Default::default())?;
    let report = store.sync(Default::default())?;
    for changed in &report.changed {
        if let Some(evidence) = store.session(changed, SessionQuery::default())? {
            println!("{} requests, {} tool results", evidence.requests.len(), evidence.tool_results.len());
        }
    }
    for row in store.sessions(CatalogQuery::default()) {
        let row = row?;
        println!("{} {} {:?}", row.source, row.session_id, row.project_key);
    }
    Ok(())
}
```

Default features expose `SessionStore` — `open`, `sync`, `hydrate`, `watch`, `sessions`, `session` — plus `Source::capabilities()`, the typed evidence structs and one `Error` enum; see `docs/sourcing-sdk.md` in the repository. Optional features: `delivery`, `opencode-backup`, `git-hooks`, `fs-events`. Workspace crates enable `unstable-internal` for connection-level maintenance APIs.

See the [repository README](https://github.com/AgentWorkforce/relayhistory) for CLI and Node usage.

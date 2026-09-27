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

    // Incremental: everything since this consumer's last commit, oldest first.
    let query = ai_hist::ChangeQuery::default().consumer("my-ingest");
    let mut changes = store.changes_since(ai_hist::Watermark::CONSUMER, query)?;
    for change in changes.by_ref() {
        let _change = change?; // `Upsert` or `Delete` of `change.key`; `columns` is the stored row
    }
    changes.commit()?; // the cursor moves only here
    Ok(())
}
```

Default features expose `SessionStore` — `open`, `discover`, `sync`, `hydrate`, `watch`, `sessions`, `session`, `session_identities`, `has_session`, `changes_since`, `head_revision` — plus `Source::capabilities()`, the typed evidence structs and one `Error` enum; see `docs/sourcing-sdk.md` in the repository. Optional features: `export` (bounded, resumable NDJSON snapshots keyed like the change feed), `opencode-backup`, `git-hooks`, `fs-events`. The crate keeps no upload state: an uploader reads the change feed. Workspace crates enable `unstable-internal` for connection-level maintenance APIs.

The embedder guide — store options, the sync lifecycle and its locking, the evidence model, accounting semantics, versioning and feature flags — is [`docs/sourcing-sdk.md`](https://github.com/AgentWorkforce/relayhistory/blob/main/docs/sourcing-sdk.md), and [`examples/rust-consumer`](https://github.com/AgentWorkforce/relayhistory/tree/main/examples/rust-consumer) is a standalone project that consumes this crate from crates.io. Cargo semver is the contract; the crate's default-feature public API is snapshotted in the repository at `crates/ai-hist/public-api.txt` and diffed in CI.

See the [repository README](https://github.com/AgentWorkforce/relayhistory) for CLI and Node usage.

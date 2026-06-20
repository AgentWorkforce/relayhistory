# ai-hist Cutover Dispatch Matrix

This cutover makes `ai-hist` a Rust-first wrapper without claiming full Rust
CLI parity. Commands or flags that are not parity-proven route to the legacy
Python CLI with an explicit warning on stderr.

Escape hatches:

- `AI_HIST_CLI=auto` (default): use the routing table below.
- `AI_HIST_CLI=rust`: force the Rust CLI.
- `AI_HIST_CLI=python`: force the legacy Python CLI.
- `AI_HIST_RUST_BIN=/path/to/ai-hist`: use an explicit Rust binary instead of
  `cargo run`.
- `./ai-hist-rust`: direct Rust entrypoint for source checkouts.
- `./ai-hist-python`: direct legacy Python entrypoint for source checkouts.

| Surface | Default route | Notes |
| --- | --- | --- |
| `search QUERY [--source --project --tag --limit --fts --json]` | Rust | Rust output is tested for Python-compatible text/JSON row shape, source validation, no-result exit status, and tag/project/source filters. |
| `recent [n] [--source --project --tag --json]` | Rust | Rust output uses the same row JSON fields and timestamp formatting as the legacy CLI. |
| `session SESSION_ID [--source --tag --json]` | Rust | Basic session output is Rust-default. Missing sessions exit non-zero. |
| `session SESSION_ID --full` | Python fallback | `--full` is not implemented in Rust yet. |
| `resume QUERY [--fts]` | Rust | Text resume command is Rust-default. |
| `resume QUERY --json` | Python fallback | JSON shape differs today, so the wrapper preserves legacy output. |
| `sync-opencode [--opencode-db PATH]` | Rust | Rust-only command for explicit OpenCode sync. Full `sync` remains Python fallback. |
| `sync` | Python fallback | Full-source sync, `.sync-state.json`, trajectory, relay, Cursor, Claude session, Codex metadata, and OpenCode state remain legacy Python-owned. |
| `show ID [--json]` | Python fallback | Not implemented in Rust yet. |
| `context ID [--window N]` | Python fallback | Not implemented in Rust yet. |
| `stats [--tag --json]` | Python fallback | Rust stats JSON/text shape is not yet parity-compatible. |
| `pack QUERY [...]` | Python fallback | Not implemented in Rust yet. |
| `watch [--interval N]` | Python fallback | Uses legacy Python sync loop. |
| `export [...]` | Python fallback | Rust export is narrower; legacy JSONL/gzip/SQLite/filter behavior is preserved through Python. |
| `import [...]` | Python fallback | Rust import is narrower; legacy dry-run/gzip/SQLite behavior is preserved through Python. |
| `tag`, `untag`, `tags` | Python fallback | Rust tagging exists but output shape and richer `tags` flags are not full parity yet. |

Compatibility fixes included in this cutover:

- Rust default DB path now honors `XDG_DATA_HOME` before falling back to
  `~/.local/share/ai-hist/ai-history.db`, matching the legacy Python default.
- Rust DB initialization now creates the legacy `history.git_branch` column and
  `sessions` table/indexes so fresh DBs remain compatible with sync metadata.
- The legacy Python CLI and tests no longer use PEP 604 annotations that broke
  on the local `/usr/bin/python3` 3.9.6 runtime.
- Top-level wrapper help lists both Rust-default and Python-fallback commands so
  fallback commands do not appear to disappear during the cutover.

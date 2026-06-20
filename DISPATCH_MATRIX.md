# ai-hist Rust Parity Matrix

`ai-hist` now defaults to the Rust CLI for the public command surface. The
legacy Python CLI remains installed as an explicit compatibility escape hatch
(`AI_HIST_CLI=python` or `ai-hist-python`), but auto mode does not route normal
commands to Python.

Escape hatches:

- `AI_HIST_CLI=auto` (default): use Rust for the user-facing command surface.
- `AI_HIST_CLI=rust`: force the Rust CLI.
- `AI_HIST_CLI=python`: force the legacy Python CLI.
- `AI_HIST_RUST_BIN=/path/to/ai-hist-rust-bin`: use an explicit Rust binary
  instead of `cargo run`.
- `ai-hist-rust`: direct Rust launcher installed by `install.sh`.
- `ai-hist-python`: direct legacy Python launcher installed by `install.sh`.

| Surface | Default route | Parity evidence |
| --- | --- | --- |
| `sync` | Rust | Full-source sync covers Claude, Codex, Cursor, OpenCode, Agent Relay when configured, and trajectories. Local E2E verifies Claude/Codex/Cursor/OpenCode/trajectory ingestion, `.sync-state.json`, WAL-safe OpenCode reads, and shared DB compatibility. |
| `search QUERY [--source --project --tag --limit --fts --json]` | Rust | Wrapper tests cover JSON shape, source validation, no-result exit status, tag/project/source filters, and FTS behavior through the shared Rust core. |
| `recent [n] [--source --project --tag --json]` | Rust | Uses the same row JSON fields and timestamp formatting contract as the legacy CLI. |
| `show ID [--json]` | Rust | Includes full prompt, tags, session count, resume command, and context hint. |
| `session SESSION_ID [--source --tag --full --json]` | Rust | `--full` and JSON output are Rust-default; missing sessions exit non-zero. |
| `context ID [--window N]` | Rust | Shows same-session entries plus nearby entries within the requested minute window. |
| `stats [--tag --json]` | Rust | JSON includes `total`, `by_source`, `top_projects`, first/last timestamps, and tag filter. |
| `pack QUERY [--source --project --tag --limit --tokens --fts --json]` | Rust | Builds a search evidence bundle with resume commands and optional prompt truncation. |
| `resume QUERY [--fts --json]` | Rust | Text and JSON output include the legacy-compatible `resume_cmd` field. |
| `watch [--interval N]` | Rust | Runs the Rust sync loop repeatedly using the same sync implementation as `sync`. |
| `export [output] [--format jsonl/sqlite --source --project --since]` | Rust | Supports JSONL, `.gz`, SQLite export, filters, stdout, and active DB overwrite protection. |
| `import FILE [--dry-run]` | Rust | Supports JSONL, `.jsonl.gz`, SQLite import, dry-run preview, dedupe, and legacy rows without `prompt_hash` or `id`. |
| `tag SESSION TAG [--source --color --json]` | Rust | JSON includes matched sessions and created assignment count. |
| `untag SESSION TAG [--source --json]` | Rust | JSON includes removed assignment count. |
| `tags [--tag --sessions --json]` | Rust | Lists tag metadata and optional tagged sessions. |
| `sync-opencode [--opencode-db PATH]` | Rust | Explicit OpenCode sync command remains available for targeted OpenCode-only sync. |

Compatibility fixes included in this cutover:

- Rust default DB path honors `XDG_DATA_HOME` before falling back to
  `~/.local/share/ai-hist/ai-history.db`, matching the legacy Python default.
- Rust DB initialization creates the legacy `history.git_branch` column and
  `sessions` table/indexes so fresh DBs remain compatible with existing SDK and
  metadata readers.
- Rust DB initialization sets WAL mode, matching the legacy Python database
  behavior.
- `install.sh` installs deterministic `ai-hist`, `ai-hist-rust`, and
  `ai-hist-python` launchers so users do not manually run Cargo commands.
- Top-level wrapper help lists the Rust-default command surface and explicit
  legacy escape hatches.

# AGENTS.md — RelayHistory (`ai-hist`)

Entry point for agents and humans working in this repository. Read this, then
the document your change touches.

## What this repository is

RelayHistory indexes coding-agent sessions from every harness on a machine into
one local SQLite database (`ai-history.db`) and exposes them through a Rust
crate, a Node native addon, a TypeScript SDK, a CLI and an MCP server. See
[`README.md`](README.md) for the product surface and
[`docs/architecture.md`](docs/architecture.md) for the call graph.

## Ownership boundary — read before adding or changing a parser

**RelayHistory is the single owner of acquiring, parsing and storing session
evidence for every harness.** Providers are added here and nowhere else.
Downstream consumers — including
[`AgentWorkforce/burn`](https://github.com/AgentWorkforce/burn), which owns
pricing, cost, activity classification and analytics — read that evidence
through the `ai-hist` crate's `SessionStore` facade instead of writing a second
parser or a second schema.

`ai-hist` is an **in-process** crate, so this is one writer _implementation_,
not one writer process: a consumer that calls `sync`, `hydrate` or `watch`
opens `ai-history.db` read-write in its own process, and only a handle opened
with `StoreOptions { read_only: true }` truly adds no writer. Every mutation
still goes through the crate's own schema, migrations, `SyncRunLock`, hydration
locks and WAL busy handler. See the ADR's [store-shape
section](docs/decisions/2026-09-19-relayhistory-owns-session-sourcing.md#store-shape-one-writer-implementation-not-one-writer-process).

- [ADR: relayhistory owns session sourcing; burn consumes evidence through a
  Rust SDK](docs/decisions/2026-09-19-relayhistory-owns-session-sourcing.md) —
  the decision, the rejected alternatives, and the per-source **capture
  matrix**. A change that adds or extends provider capture must move its cell
  in that matrix in the same PR.
- [`docs/sourcing-contract.md`](docs/sourcing-contract.md) — the record types
  the Rust SDK must expose, mapped onto burn's reader types.

RelayHistory will never own pricing, cost, token estimation, activity
classification, or similarity-based session linking.

## Document map

| Document                                                 | What it covers                                                       |
| -------------------------------------------------------- | -------------------------------------------------------------------- |
| [`docs/architecture.md`](docs/architecture.md)           | Production call graph, package boundaries, ledger and scope          |
| [`docs/session-catalog.md`](docs/session-catalog.md)     | Shallow discovery, per-provider capability matrix, adding a provider |
| [`docs/sourcing-contract.md`](docs/sourcing-contract.md) | Record types the Rust SDK must expose                                |
| [`docs/decisions/`](docs/decisions/)                     | Architecture decision records                                        |
| [`docs/getting-started.md`](docs/getting-started.md)     | Install and first run                                                |
| [`docs/releasing.md`](docs/releasing.md)                 | npm and crates.io release pipelines, semver policy                   |
| [`docs/agent-integration.md`](docs/agent-integration.md) | Wiring RelayHistory into an agent                                    |
| [`crates/ai-hist/README.md`](crates/ai-hist/README.md)   | Embedding from Rust                                                  |

## Decision records

Durable architecture choices go in `docs/decisions/` as
`YYYY-MM-DD-short-slug.md`, with a status, date, context, decision, the
alternatives that were rejected and why, and the consequences. Record the
choice, not the reasoning transcript.

## Before opening a PR

CI (`.github/workflows/ci.yml`) runs, at minimum:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -A clippy::too_many_arguments -D warnings
cargo test --workspace --all-features
cargo build -p ai-hist --no-default-features
cargo publish --dry-run -p ai-hist --allow-dirty
```

plus the TypeScript SDK tests, the native binding contract check, and the
optional-plugin jobs. The plugin crates are not workspace members, so
`cargo test --workspace` does not reach them — run them directly when you touch
`plugins/`.

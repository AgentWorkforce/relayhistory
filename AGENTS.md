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
- [`docs/session-catalog.md` → Adding a
  provider](docs/session-catalog.md#adding-a-provider) — the only place a
  harness is added; the ADR's per-source matrix is the contract of record.
- burn is becoming a consumer rather than a second parser: it prices and
  analyzes the evidence this crate captures, and drops its own harness readers
  in burn 5.0.0 ([burn #562](https://github.com/AgentWorkforce/burn/issues/562)).
  Two CI checks arm as burn gets there — the `burn-contract-drift` job (burn
  built against your change, once burn depends on `ai-hist`) and the weekly
  `burn-reader-tripwire.yml` (burn's parsers must stay gone after its cutover
  tag); see `scripts/burn-guardrails.mjs`.

RelayHistory will never own pricing, cost, token estimation, activity
classification, or similarity-based session linking.

## Document map

| Document                                                 | What it covers                                                       |
| -------------------------------------------------------- | -------------------------------------------------------------------- |
| [`docs/architecture.md`](docs/architecture.md)           | Production call graph, package boundaries, ledger and scope          |
| [`docs/session-catalog.md`](docs/session-catalog.md)     | Shallow discovery, per-provider capability matrix, adding a provider |
| [`docs/sourcing-contract.md`](docs/sourcing-contract.md) | Record types the Rust SDK must expose                                |
| [`docs/sourcing-sdk.md`](docs/sourcing-sdk.md)           | Embedder guide: the `SessionStore` facade, its structs and errors, lifecycle and locks, evidence model, semver |
| [`docs/decisions/`](docs/decisions/)                     | Architecture decision records                                        |
| [`docs/getting-started.md`](docs/getting-started.md)     | Install and first run                                                |
| [`docs/releasing.md`](docs/releasing.md)                 | npm and crates.io release pipelines, semver policy                   |
| [`docs/agent-integration.md`](docs/agent-integration.md) | Wiring RelayHistory into an agent                                    |
| [`crates/ai-hist/README.md`](crates/ai-hist/README.md)   | Embedding from Rust                                                  |
| [`examples/rust-consumer`](examples/rust-consumer)       | Out-of-tree consumer of the published crate; CI smoke test          |

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

plus the TypeScript SDK tests, the native binding contract check, the
optional-plugin jobs, the out-of-tree consumer build (`examples/rust-consumer`
with `[patch.crates-io]` at this checkout) and the public-API snapshot diff
(`node scripts/check-public-api.mjs`; a change to the crate's default surface
updates `crates/ai-hist/public-api.txt` with `--update` and adds a `### Rust
API` changelog entry in the same PR). The plugin crates are not workspace members, so
`cargo test --workspace` does not reach them — run them directly when you touch
`plugins/`.

For a change under `crates/ai-hist/`, the `burn-contract-drift` job checks out
`AgentWorkforce/burn` (main, or the commit in the `BURN_REF` repository
variable), rewrites burn's `ai-hist` requirement to a path dependency on this
checkout — so the question is "does burn pass against this code", whatever
version burn pins — proves burn resolved it, and runs burn's relayhistory
parity suite. A failure there means your change moves burn's ledger identity
(message ids, timestamps, usage dedup): fix it here or document the difference
as intended — do not work around it in burn. If burn itself is broken, re-run
the base branch to confirm, and a maintainer can pin `BURN_REF` to a
known-good burn commit until it is fixed. Until burn depends on `ai-hist`
(burn #557) the job passes with a notice.

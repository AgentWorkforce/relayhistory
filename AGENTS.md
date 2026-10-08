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
| [`docs/releasing.md`](docs/releasing.md)                 | npm and crates.io release pipelines, semver policy, changelog cut    |
| [`docs/agent-integration.md`](docs/agent-integration.md) | Wiring RelayHistory into an agent                                    |
| [`crates/ai-hist/README.md`](crates/ai-hist/README.md)   | Embedding from Rust                                                  |
| [`examples/rust-consumer`](examples/rust-consumer)       | Out-of-tree consumer of the published crate; CI smoke test          |

## Decision records

Durable architecture choices go in `docs/decisions/` as
`YYYY-MM-DD-short-slug.md`, with a status, date, context, decision, the
alternatives that were rejected and why, and the consequences. Record the
choice, not the reasoning transcript.

## Changelog

`CHANGELOG.md` is the one changelog for every package; they all release at one
version.

- Curate the pending section of `CHANGELOG.md` in the same PR as any
  user-visible change.
- An empty post-release changelog starts at `[Unreleased]`. The first pending
  user-visible change sets the heading to `[Unreleased - Patch]`,
  `[Unreleased - Minor]`, or `[Unreleased - Major]` according to its SemVer
  impact. Before 1.0, a breaking change (including a native contract bump or a
  `### Rust API` break) is `Minor`.
- The pending level is monotonic: `Patch < Minor < Major`. Raise the heading
  when a higher-impact change arrives; never lower it, and leave it unchanged
  for another change at the same level.
- Sections, in this order: `### Breaking Changes`, `### Added`, `### Changed`,
  `### Deprecated`, `### Removed`, `### Fixed`, `### Security`, `### Rust API`.
- Do not hand-cut a release. The publish workflow refuses a release smaller
  than the pending level (`scripts/check-release-changelog.mjs`), then
  `scripts/cut-changelog.mjs` moves the pending entries under the released
  version, restores a bare `[Unreleased]`, and updates the comparison links.
  Entries merged while a release runs stay pending.
- Keep entries concise and impact-first: one short, unwrapped bullet per
  user-visible change, no nested bullets.
- Omit PR links, internal review notes, test-only or CI-only work, and
  implementation backstory unless they explain shipped impact.

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

<!-- prpm:snippet:start @agent-relay/merge-train-snippet@1.0.1 -->
## Merging: `trunk` + the `mergeable` label

CI suites do **not** run automatically on feature branches. They run only on
this repository's `trunk` → `main` pull request and on pushes to `main`. The
one check that does run on a feature PR into `main` is `Trunk guard`, which
fails it on purpose; manually dispatched workflows (`workflow_dispatch`) still
run on any branch. (Repos whose default branch is not
`main`, e.g. `master`, use that branch wherever this says `main`.) A merge
agent batches ready PRs into `trunk`, gets that one PR green, and merges it.

**When you open a PR**
1. Branch from `trunk` and open the PR with **base `trunk`**, not `main`.
   A PR into `main` from any other branch fails the `Trunk guard` check.
2. No CI runs on your PR, so verify locally before calling it ready: run the
   typecheck, tests and lint this repo uses, and list the exact commands and
   results in the PR body.

**When the PR is ready**
3. Add the label **`mergeable`** once all of these are true:
   - The change is complete and the local checks above pass.
   - Review feedback (human and bot) is addressed or answered.
   - It is not a draft and does not depend on an unmerged PR.
4. Remove `mergeable` if the PR stops being ready (new work, a failing check, a
   blocking question). The label is read live from GitHub on every sweep.

**What you must not do**
- Do not merge your own PR, and never merge into or push to `trunk` or `main`
  directly.
- Do not re-enable CI for feature branches or edit the `trunk` gates in
  `.github/workflows/`.

**The merge agent** sweeps open `mergeable` PRs with base `trunk` about every
10 minutes. It reads each PR's linked sessions (the `Agent Relay sessions`
block in the PR body, then the session summary) for context, merges them into
`trunk`, opens or updates the `trunk` → `main` PR, fixes CI there, merges when
green, and posts a summary. If your PR conflicts with `trunk`, it may ask you
to rebase on `trunk`; do so and keep the label.

> Interim: the sweep worker is not deployed yet. Until it is, a human or a
> designated agent performs the merge-agent steps manually. Labelling is unchanged.
<!-- prpm:snippet:end @agent-relay/merge-train-snippet@1.0.1 -->

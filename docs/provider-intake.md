# Provider-owned history intake

This is the implementation contract for history coverage setup #426. It is based
on RelayHistory 0.40.0 (`bfd7c90`), with seven admitted sources: `claude`,
`codex`, `cursor`, `grok`, `opencode`, `muse`, `devin`. Their order, spellings,
serialization, evidence snapshots and retirement of `relay` / `trajectory` stay
unchanged. Catalog/hydration/relationship/evidence/usage contracts remain
5/3/2/4/3; the native binding contract remains 26. This setup admits no new agent.

## Ownership and admission

A provider owns:

- `providers/history-sources/<source_id>.json`: one admission record, aliases,
  fixture manifest and fixture-proven capture cells.
- `crates/ai-hist/src/providers/<rust_id>/`: registration, discovery, acquisition,
  normalization and provider-specific tests.
- `crates/ai-hist/tests/fixtures/<source_id>/` and
  `crates/ai-hist/tests/snapshots/<source_id>/`: sanitized evidence and reviewed
  normalized output.
- `crates/ai-hist/tests/provider_<rust_id>.rs`: public conformance cases using
  `mod provider_support;` and the shared `assert_history_conformance` helper.

Use the assigned issue number as the descriptor's `order`: Pi is **428** and
Hermes **429**. Existing sources retain 0–6; 9999 is reserved for the test source.
This yields deterministic order without two agents reserving the same integer.
JSON `id` is the canonical ledger identity; `rust_id` names the module;
`variant` names the non-exhaustive Rust `Source` variant. `aliases` are explicit
spellings accepted by `Source::parse`; persisted IDs, serialization, SDK and CLI
source vocabulary remain canonical. Aliases cannot shadow any admitted ID.

The integration owner owns shared handwritten helpers, root-level documentation,
changelog, public API decisions and package/lockfile changes. Provider agents
request shared additions as a separate prerequisite; they do not copy helpers or
edit shared switches. Each provider PR includes generated output and its own
changelog entry. The integration owner rebases, regenerates and reviews one PR
at a time. Never hand-merge generated arrays. Start the two pilot branches from the reviewed, fully verified setup commit
recorded with its PR in #426. While setup awaits merge, pilots remain stacked
on that exact commit and cannot integrate before setup lands.
Other providers may investigate now, but production implementation waits for the
two pilots to establish the contract against real formats.

## Registration and acquisition contract

Copy [`providers/templates/provider.rs`](../providers/templates/provider.rs).
The working test-only example is
[`providers/test_fixture`](../crates/ai-hist/src/providers/test_fixture/mod.rs).
It deliberately uses a tiny synthetic format; production parsers must enforce
bounded shallow reads and validate every identity/type instead of its fixture
assertions. The source module is an internal child of ingestion, so it uses the
existing normalizers (`EventRow`, `insert_session_event`, `upsert_session`, history,
tool/edit/marker/relationship writers) without a new public storage API.

`LOCAL_SOURCE` supplies the existing discovery, selected parser, allowed roots,
relationships, resume and fixture contracts. `METADATA` declares message-ID
provenance, markers and optional `UsageRegistration`. Usage registration carries
both accounting and the normalization function, so normalization, the public
normalizable-source list and capabilities share the implementation. Missing
usage stays unavailable, never a synthetic zero. Declared evidence kinds come
from `ShallowSessionProvider::evidence_kinds`; declare only kinds the parser
actually writes, and test them. Path-based lifecycle hooks are a separate
capability: new adapters support session-ID hydration and do not add themselves
to `HOOK_HARNESSES` merely because their history lives in files.

`ADAPTER = Some(ProviderAdapter { ... })` supplies:

- `resolve_roots(home, use_env)`: freezes every provider root exactly once in
  `ProviderRoots`. `from_home` ignores the host environment. Old serialized roots
  missing a newly admitted provider receive defaults under their recorded home,
  without reading the process environment. Discovery uses
  `env.provider_roots`, shallow reads use `scan.provider_roots`, and all other
  paths receive the same frozen value. Use `roots.for_provider(ID)` thereafter.
- `acquire(options, locator, context)`: returns an `AcquiredSession` with immutable
  JSON payload, native identity validation, records, measured bytes and all read
  paths. The framework validates the primary locator first. Read every primary
  or sidecar through `context.read`, `file_snapshot(context, paths)` or
  `sqlite_snapshot(context, path, callback)` so canonical containment is checked
  **before** opening it. Discovery and sweeps must apply the same context.
- `watch_roots(roots)`: includes every evidence file the sweep reads. Tree roots
  cover future session files; SQLite watches include the database and WAL/journal
  sidecars, with a narrow file filter. Avoid credentials/configuration roots.
- `sweep(SweepContext)`: shares the hydration normalizer, writes one session
  atomically through existing store functions, honors repair requests and state
  generations, returns `SweepResult { inserted, unread }`. Any absorbed per-file
  read failure increments `unread`, preventing a false complete fingerprint.
  Errors remain provider diagnostics; cancellation is not swallowed.
- `generation`: changes whenever unchanged native bytes normalize differently.
  It participates in full-sweep fingerprints and targeted hydration stamps;
  changing it resets the old byte cursor and sets `parser_upgrade`.

Selected hydration receives the captured payload as `ctx.provider_session`.
It must not reopen a live source after stamping it. SQLite capture pins a
read-only transaction before querying and carries every selected row from that
snapshot into normalization. It never copies only the main DB, uses
`immutable=1`, changes journal mode or migrates provider databases. SQLite bytes
read are unknown unless actually measured: report zero rather than file size.
File-set capture preserves exact bytes in stable path order, including incomplete
final JSONL records for the provider to defer. It is not an atomic filesystem
transaction; providers requiring cross-file consistency must detect change and
retry or report incomplete capture.

Discovery fingerprint inputs must change for **all** source changes, including
WAL-only commits and sidecars. A database file's mtime alone is insufficient.
`enumerate` / `read_shallow` must remain bounded; use `ScanEnv` counters for files,
bytes, queries and records. Missing roots mean no sessions; unreadable roots or
unsupported schema versions produce actionable diagnostics, never an empty
successful import. Passive scans must never launch agents, invoke model turns,
change auth or opt users into remote acquisition.

## Generator and fixtures

```sh
npm run providers:generate
npm run providers:check
```

The generator needs Node and `rustfmt`; Cargo uses the checked-in output and
needs neither Node nor network. `--check` rejects stale output. Admission rejects
alias/id/order/module collisions, unregistered modules, missing fixture files and
missing snapshots. Test-only descriptors live in `providers/test-sources/` and
are `cfg(test)` in every Rust registration and absent from SDK and corpus output.

Generated-file allowlist:

- `crates/ai-hist/src/providers/generated_modules.rs`
- `crates/ai-hist/src/providers/generated_catalog.rs`
- `crates/ai-hist/src/providers/generated_registry.rs`
- `crates/ai-hist/src/session_store/generated_sources.rs`
- `crates/ai-hist/tests/generated/provider_corpus.rs`
- The marked `PROVIDER SOURCES` section of `sdk-ts/src/sdk-common.ts`.
- The marked `PROVIDER FIXTURES` section of `tests/fixtures/README.md`.
- The marked `PROVIDER CAPTURE` section of the sourcing ownership ADR.

Every shard's `fixtures` entries have `source`, `name`, `layout`, `origin`,
`files`, and `quirk`. `HomeTree` copies a provider-owned fixture tree into the
isolated home; `Sqlite` also requires `target`, a relative home path at which to
execute its SQL files. Use SQL fixtures instead of checked-in live databases.
The existing legacy layouts remain available. `capture` is an array of
`{ "evidence": "...", "coverage": "..." }` cells describing what these
fixtures actually prove and what is unavailable. New providers must supply it.
Do not set `legacy: true` on new admissions: it is reserved for the seven
pre-existing handwritten ADR columns.

For a first fixture, create its target snapshot path with temporary `{}` content
so the generator can admit its manifest, then run:

```sh
UPDATE_SNAPSHOTS=1 cargo test -p ai-hist --all-features --test fixture_corpus
cargo test -p ai-hist --all-features --test fixture_corpus --test source_registry
cargo test -p ai-hist --test provider_<rust_id>
cargo test -p ai-hist --features export --test provider_<rust_id>
```

Review the resulting snapshot; never commit the temporary placeholder. Existing
completeness checks still require every fixture file to be claimed and each
snapshot/README entry to match the manifest. Do not update unrelated snapshots.
A full fixture corpus run after adding a provider proves coexistence, not just
its own fixture. For public conformance, stage the provider's own fixture under a
temporary home and call `assert_history_conformance` with expected native
session ID and both user/assistant messages. It covers discover → hydrate →
read → reopen → change feed → full sync; the export feature checks paged
`ExportSnapshot`. There is no public Rust search method; validate search through
SDK/CLI for each pilot alongside the public read assertion.

CI runs generator checks through `node --test scripts/*.test.mjs`. Follow the
full checks in `AGENTS.md`, including native rebuilding before SDK tests and
public API snapshot review for each newly admitted `Source` variant. Include a
negative control: remove the provider's registration or disable its parser and
record the named acceptance failures, then restore and rerun. A compilable
module alone proves no history acquisition.

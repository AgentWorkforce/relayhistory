# Plan 009: Make the local history core independently buildable and installable

> Executor: implement in the six reviewable stages below. Preserve the auth
> behavior established on main by PR #138. Do not combine behavior fixes,
> schema migration, and physical moves into one unreviewable change.
>
> Drift check: `git diff --stat 3155117..HEAD -- Cargo.toml Cargo.lock crates sdk-ts mcp-package scripts .github/workflows docs README.md`.
> Compare changed areas with this plan before executing. Update this plan if
> its assumptions no longer hold; do not restore superseded implementations.

## Status and scope

- Priority: P1; effort: L (multiple PRs); implementation risk: HIGH.
- Category: architecture, migration, correctness, packaging.
- Planned at: `3155117`, 2026-09-13, after fetching and fast-forwarding main.
- Depends on: characterization baseline in stage 1, then the order below.
- Existing plans 006–008 describe native execution, hydration, and relationship
  behavior that must survive. Their completion status was not audited here.
- In scope: root Cargo manifests/lockfile; `crates/ai-hist-core`,
  `crates/ai-hist`, `crates/ai-hist-napi`; new Rust cloud/connector/CLI crates;
  `sdk-ts`; optional destination/source plugin packages; `mcp-package`; affected
  build/release/contract/smoke scripts, workflows, and product documentation.
- Out of scope: cloud server changes, new providers, parser rewrites, identity
  algorithm changes, redesigning auth, credential reset, deleting user data,
  deployment, and publishing packages during implementation.
- Branch naming: `codex/local-history-cloud-boundary` or a separate `codex/` branch
  per stage. Match repository commit style, e.g. `refactor(sdk): ...`.

## Why this matters

“Local history core” means local provider ingestion, SQLite storage, session
identity and relationships, transcript/evidence queries, search, and the native,
SDK, CLI, and MCP interfaces for those operations. It can query cached evidence
with remote provenance offline; the name describes where processing and storage
happen, not a restriction to locally originated sessions. Remote acquisition
adapters and commercial auth, upload, recall, sharing, and hosted integrations
sit outside this boundary. This is an architectural name, not an existing product.

The local history core must be usable without commercial packages, credentials, or transport.
Today cloud participates in the core model, engine, native binding, SDK build,
and MCP server. Authentication also changes which cached rows are visible and
which adapters run. A module split alone would preserve that behavior.

Success means a clean checkout containing only the local history core dependency closure
can build, typecheck, test, pack, install, and execute local operations. A
plugin selected by the user supplies remote acquisition or delivery to a chosen
destination. RelayHistory is one destination implementation, not the cloud model.

## Current state, confirmed on this commit

- `crates/ai-hist-core/src/lib.rs:10` exports `convergence`, `outbox`, and
  `turns`. `turns.rs:1` builds commercial wire batches, not the generic event
  storage model. Keep `session_events` and evidence queries in the local history core.
- `crates/ai-hist/src/lib.rs:26` exports cloud; it also contains `Cli`, `Command`,
  `run`, formatting, sync, provider parsers, and services. `main.rs` calls
  `ai_hist_engine::run()`. The engine manifest depends on clap and ureq.
- `remote.rs:138`, `remote_connector_statuses_at`, calls
  `crate::cloud::recall_auth()` even though its explicit argument is a provider
  home. `configured_remote_providers` registers `CloudProvider` on auth success.
- `discover.rs:378` already defines `ShallowSessionProvider`; its methods include
  `source()`, `location()`, `enumerate()`, and `read_shallow()`. Reuse this seam.
  `discover_sessions_with_providers` already accepts an explicit adapter set,
  but ordinary discovery and hydration hardcode remote adapter resolution.
- `crates/ai-hist-core/src/lib.rs:784` defines both presence and checkpoint keys
  as `(source, session_id, location)`. `upsert_session_presence` executes:

  ```sql
  ON CONFLICT(source, session_id, location) DO UPDATE SET
    raw_locator = COALESCE(excluded.raw_locator, session_presences.raw_locator),
    source_stamp = COALESCE(excluded.source_stamp, session_presences.source_stamp)
  ```

  Provider and recall observations of one remote session overwrite each other.
  `hydrate.rs` also reads/writes checkpoints and locators by location alone.
- `lib.rs:3182` invokes `sync_relaycast` during local sync. That function at
  line 8497 checks Relaycast credentials and fetches remote channels/messages.
- `sdk-ts/src/index.ts:1378` and `:1394` use commercial auth to reject remote
  cached queries or narrow `all` to `local`. Calls exist in search, recent,
  catalog, and stats. Existing `remote-auth.test.ts` asserts these semantics;
  changing those assertions is an intentional behavior fix.
- Main now has `native.ts` and `sdk-common.ts`, and `cloud-client.ts` delegates
  auth to Rust. However, `native.ts:21` imports cloud types and its binding
  interface requires cloud functions. `index.ts:34` re-exports cloud.
- `sdk-ts/package.json` builds with
  `npm run clean && tsc -p tsconfig.json && node scripts/build-cloud-auth.mjs`.
  That script resolves and bundles `@agent-relay/cloud`; the broad tsconfig
  includes its source. A type-only reference can retain this dependency too.
- `mcp-server.ts:13` imports cloud and registers `get_session_thread` in the
  ordinary server. N-API cloud methods begin around `lib.rs:1574`.

## Product model and target dependency direction

The product is a local history tool with optional connections. Users can keep
all history local, export it to a file/pipe, or explicitly send selected history
to one or more destinations, either once or through an explicitly enabled
background delivery worker. An Agent Relay service, another cloud service,
or a service such as heyskip.dev could implement the destination interface.
These are intended extension points, not claims that those services currently
implement a shared API. Changing only a base URL is insufficient when services
have different auth and payload formats; the destination plugin handles that.

```text
provider history -> local ingestion -> SQLite history + durable change journal
                                                 |
                                      durable delivery coordinator
                                      (queue, checkpoints, retries)
                                                 |
                                    selected destination plugin(s)
                                      (auth, mapping, transport)
                                                 |
                                            destination(s)

CLI / MCP / SDK use the same local core and coordinator APIs.
Source plugins optionally feed the existing ingestion interface.
NDJSON export is a separate consumer of the public history/export APIs.
```

The application explicitly supplies configured plugins. Core has no imports of
concrete plugins. Keep the existing local native addon and ordinary CLI/MCP;
loading a plugin extends the same application. No second native addon, parallel
CLI/MCP product, plugin marketplace, or native binary per destination is
required. Background operation uses the same coordinator as a one-shot drain,
with an optional long-running worker mode in the existing application.

Use two small capability interfaces, not an all-purpose cloud API:

- A **source connector** optionally discovers or hydrates remote history through
  the existing ingestion seam. Provider access and commercial recall are source
  implementations; a destination does not have to implement either.
- A **destination** accepts bounded, versioned history export batches. It owns
  remote payload mapping, auth, transport, and translation of remote responses
  into explicit delivery acknowledgments. It does not own queue state, retry
  scheduling, or delivery checkpoints. Upload-only integrations are sufficient.
  Sharing, hosted search, replay, and thread tools are optional plugin features.

A service-independent **delivery coordinator** in the local core owns durable
queue/checkpoint state, retries, worker leases, and progress reporting. It is
inert until the user explicitly configures/enables a delivery job. Local
operations remain auth-free and never call destination transports themselves.
Installing a plugin alone does not authorize sending anything. Once enabled,
a background job may deliver future eligible changes under its saved selection
without prompting for each batch.

The first implementation should use explicit SDK registration of ordinary
objects/functions and CLI/MCP configuration listing installed plugin modules
and instance IDs. Resolve and load only those modules; never scan packages,
auto-install an integration, or enable it because credentials exist. A plugin
is ordinary trusted application code, not a promised sandbox. Source/destination
interfaces belong to public core contracts, not a RelayHistory-branded package.

Expose a generic paged export API and an NDJSON file/stdout path so another
application can pipe history without writing a plugin. Export selection must be
explicit (sessions/sources and evidence categories); preserve existing incognito
exclusions by default. Diagnostics go to stderr. The stdout pipe completes on
successful export only; it does not claim that a downstream service accepted
anything. Dependable background delivery uses the durable coordinator and
destination acknowledgments, not a stdout pipe as its queue.

Keep package changes minimal: core plus optional plugin package(s). Extract
RelayHistory mapping/auth/transport into its own package depending on public
history interfaces. Reuse the existing Rust auth implementation inside that
plugin; do not reimplement its credential handling in TypeScript. If that
particular plugin needs a native/helper bridge to retain Rust auth, justify and
scope it to that plugin. It is not a requirement for other destinations or a
reason to duplicate the local native surface and platform release matrix.

Prefer retaining `ai-hist` / `ai-hist-native` for the core in an announced
breaking release. Existing cloud imports/commands migrate to installation and
explicit configuration of the RelayHistory plugin. Old published versions can
remain available during migration. A temporary compatibility facade is optional
if consumers require it; it must depend on core and plugin, never the reverse.
The previous proposal for new `ai-hist-local` packages alongside a mandatory
full distribution is superseded. Publishing and final package names remain out
of scope for implementation.

## Verification baseline and conventions

On this checkout, `npm --prefix sdk-ts run typecheck` passes. That is a check
with the existing dependencies present, not proof of independent installation.
The previous review's missing-cloud failure was not reproduced here.
Rust/native/full integration tests were not run during this planning pass.

Existing CI commands to retain, adjusting package locations after moves:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -A clippy::too_many_arguments -D warnings
cargo test --workspace
npm --prefix crates/ai-hist-napi ci
npm --prefix crates/ai-hist-napi run build:debug
node scripts/verify-native-contract.mjs ./crates/ai-hist-napi/index.js
npm --prefix sdk-ts ci
npm --prefix sdk-ts run lint
npm --prefix sdk-ts test
node --test scripts/*.test.mjs
```

Every command must exit 0. Generated declarations must match their source.
Follow `session_discovery.rs` for subprocesses with temporary provider homes,
`cloud-auth.test.ts` for local HTTP fixtures and credential compatibility,
and `native-errors.test.ts` / `sdk-common.ts` for typed error behavior.
Keep Rust-owned SQL/parsing and async native blocking-task dispatch. CLI and
MCP call public SDK APIs. Do not add a JS parser, database, or CLI fallback.

## Stage 1 — Characterize the current contracts

Before moves, add fixtures covering local discovery/sync/hydration, cached
scope queries, cursor pagination, relationship/evidence ownership, and exact
native error codes. Use temporary directories; never the operator's stores.

Preserve cloud fixtures for stage URL selection, absent and expired tokens,
refresh under the native stage lock, atomic replacement, redacted failures,
outbox acknowledgments, independent transcript watermarks, replay pagination,
and sharing. Keep the equivalence tests for current root/cloud public imports.
PR #138 deliberately removed legacy credential stores; do not reintroduce them.

Create a baseline matrix distinguishing passing intended behavior from known
violations: cached scopes consulting auth, local Relaycast I/O, implicit recall
registration, and connector metadata collisions. Characterization may capture
current behavior, but those cases must turn into fixed regression tests in the
following stages; do not leave permanent skips as proof of success.

Verify: run the existing CI commands above and record any pre-existing failure
before proceeding. This stage should change tests/fixtures only.

## Stage 2 — Make connector selection explicit and fix offline semantics

1. Remove cloud guards from cached SDK queries. Preserve requested
   `local/remote/all` scope regardless of login, stage ambiguity, or expiry.
   Querying stored remote evidence does not require remote acquisition.
2. Extend the existing adapter seam with a stable connector ID and explicit
   instance key, availability checks, and hydration capability. Share one
   selection path across discovery, sync, and hydration. Accept an explicit
   per-client registry; the application may load configured installed modules.
   Do not build auto-discovery, remote code downloads, or a process-wide registry.
3. Select connectors before probing credentials. Local entrypoints construct
   only local adapters. Provider-only selection never invokes recall auth.
   Plugin-enabled operations accept an explicit connector allowlist. Credentials
   determine whether a selected connector can run, not which connectors exist.
4. Preserve source filters separately: `source=codex` identifies evidence;
   `connector=codex-cloud` identifies acquisition. Remote with no applicable
   selected connector fails before opening/migrating the database. `all` can
   still run local adapters when optional remotes are unavailable, with accurate
   diagnostics. Cloud must never be silently added because someone logged in.
5. Move Relaycast transport out of local sync into an explicitly selected
   commercial connector. Newly acquired Relaycast rows have remote provenance.
   Retain local Relay transcript ingestion. Do not relabel all historical
   `relay` rows: legacy provenance may be ambiguous.

Tests: change `remote-auth.test.ts` to assert scope invariance with absent,
malformed, expired, and ambiguous commercial stores. Add panic/counting auth
and transport fakes for unselected connectors and seed Relaycast variables in
local-only tests. Assert zero reads of commercial credentials and zero calls
to commercial/provider transports for local acquisition. A fake HTTP endpoint
alone does not prove a credential file was never read; instrument that seam.

Verify: `cargo test -p ai-hist-engine` and `npm --prefix sdk-ts test` exit 0.
The new no-auth/no-transport assertions must pass, including with plugins installed
and configured in the same application.

## Stage 3 — Retain connector provenance without duplicating sessions

This migration is required before enabling multiple inbound connectors for one
session. It is not a prerequisite for file export or an upload-only destination:
those features can ship against the existing read APIs first. Do not expand the
first destination plugin into a bidirectional synchronization project.

Add an authoritative `session_observations` table keyed by
`(source, session_id, location, connector_id, connector_instance)` and a
matching observation hydration checkpoint. `connector_instance` separates
different accounts/stages serving the same logical connector; it is a stable,
non-secret identifier, never a bearer token. Keep canonical session identity
unchanged and keep `session_presences` as a compatibility/aggregate scope view.

Move acquisition locator/stamp/state comparisons into observation APIs. Keep
the aggregate presence row for location membership, but do not use its locator
or checkpoint as authoritative when multiple observations exist. Update
discovery cache keys, skip classification, diagnostics, provider summaries,
hydration selection, deletion/cleanup, and schema readiness checks together.
Choose hydration by explicit connector or a documented stable capability
preference; do not let discovery ordering pick the locator. Keep one globally
ordered, deduplicated catalog result and existing bounded pagination behavior.

Backfill transactionally and idempotently. Assign a known connector only when
the old locator gives reliable evidence. Otherwise record `legacy-unknown`;
do not fabricate provenance that has already been overwritten. Treat unknown
checkpoints conservatively and rehydrate through an explicitly selected adapter.
Freeze writes from old binaries during migration rollout; do not promise
mixed-version acquisition safety unless separately tested and implemented.

Tests in core and discovery/hydration: same session observed by local provider,
provider remote, and recall; two recall instances; different scan orders;
independent stamps/checkpoints; repeated migration; legacy unknown locators;
session deletion; stable catalog totals/pages; no accidental new local presence.
Assert three observations survive while one session and two locations remain.

Verify: `cargo test -p ai-hist-core` and `cargo test -p ai-hist-engine` exit 0.
Use fixture databases, including one from the old schema. Repeated migration
must preserve row counts and evidence. Keep migration separate from file moves.

## Stage 4 — Extract Rust packages with one-way dependencies

1. Move `Cli`, command enums, `run`, and presentation/service command handling
   from engine `lib.rs` into `ai-hist-cli`. Move binary integration tests there
   so `CARGO_BIN_EXE_ai-hist` remains valid. Export typed ingestion operations
   needed by CLI/N-API instead of making CLI helpers the new library API.
2. Move provider transport adapters from `remote.rs` to
   `ai-hist-provider-connectors`. Leave adapter interfaces, local parsers,
   discovery orchestration, and evidence ingestion in the engine.
3. Move core convergence/outbox/turn wire mapping plus engine cloud auth,
   push/recall/replay/sharing and commercial integrations to `ai-hist-cloud`.
   Move cloud tests with their implementation. Keep generic evidence, commit
   relationships, and storage in core; audit `git_sdk.rs` by responsibility
   rather than moving every Git function merely because cloud calls it.
4. Add public typed storage scans needed by outbox/turn mapping. Preserve
   ordered row IDs, resume boundaries, transaction snapshot semantics, raw
   evidence fields, and incognito information. Cloud-specific envelopes,
   ownership policy and acknowledgment mapping stay in the plugin. Generic
   durable journaling, delivery queues, and checkpoints belong in the local core.
   Preserve the existing RelayHistory cursor format during a characterized
   migration into the coordinator; never reset its progress or run independent
   old and new schedulers for the same destination. Do not recreate local history
   storage SQL in cloud or expose a generic SQL backdoor.
5. Remove clap/HTTP/commercial dependencies from local history core manifests when their
   consumers move. Never re-export the cloud crate from core/engine: that would
   recreate the dependency cycle. Keep any compatibility Rust facade above both.

Verify: all Rust CI gates pass; dependency checks show core and engine reach
neither cloud, provider connector implementations, CLI, nor HTTP transports.
`cargo test -p ai-hist-provider-connectors` must pass without cloud enabled.
Rerun the characterized outbox/transcript/auth tests against `ai-hist-cloud`.

## Stage 5 — Add a durable delivery coordinator and thin plugins

Keep one local SDK/CLI/MCP and its native addon. Extract concrete cloud code,
its build dependencies, and cloud-only declarations from that dependency graph
into the optional RelayHistory plugin. Reuse current `native.ts` and
`sdk-common.ts`; split normalizers and pagination only as needed for the public
interface. Do not create a new package for each TypeScript responsibility.

Add public contracts for `HistoryExportBatch`, `HistoryDestination`, and plugin
registration. Names are proposed; concrete fields must be derived from the
existing identity/evidence and outbox fixtures before implementation. Required
semantics are:

- Versioned records with canonical source/session identity, stable record IDs,
  observed provenance, evidence kind, and original timestamps. Service-specific
  org/tenant fields and envelopes belong in the plugin. Acknowledge unsupported
  evidence explicitly; do not silently claim complete delivery.
- Bounded pages and an opaque continuation cursor over a documented consistent
  export boundary. Define how revised records are revisited; do not reuse a
  timestamp alone or imply that a one-time snapshot is a continuous change feed.
  Preserve existing RelayHistory outbox update/watermark behavior when moving
  delivery state into the coordinator.
- Stable batch/record idempotency keys for retries. The coordinator advances each
  destination instance's checkpoint only after its adapter confirms that batch.
  Partial acceptance cannot acknowledge the whole batch. Crash after acceptance
  may cause redelivery; do not promise exactly-once behavior. Persist checkpoints
  atomically in core storage, separated by destination instance, remote account/
  tenant identity, and export selection/version. Do not key progress by URL alone.
- Two destinations can consume the same selected history independently. One
  network failure cannot advance the other's checkpoint or block local ingestion.
  Plugin errors identify the selected destination without disclosing credentials.
- Caller-provided history access through public export/ingestion APIs. Plugins
  must not use core private imports, raw SQLite queries, or native pointers.

Wire the same exporter to a documented NDJSON file/stdout command and SDK
iterator. Local sync records eligible changes but never performs uploads itself.
Delivery can be a one-shot drain or an explicitly enabled background job. Both
use the same durable state machine; do not maintain a separate best-effort path
for background sync.

### Durable state and consistency

Implement generic queue/journal storage and transactions in Rust, exposed through
the existing native binding. Plugin registration and worker lifecycle can remain
in the host application. Do not put SQL in TypeScript or plugins.

1. **Capture every supported change durably.** When delivery capture is enabled,
   record an immutable change/revision or a durable reference retaining that
   revision in the same SQLite transaction as history ingestion/update. Provider
   ingestion checkpoints must not advance before the history and journal commit.
   Include later hydration, edits, and revised metadata covered by the selected
   export schema; timestamp-only polling is not sufficient. Keep stable origin,
   record ID, and revision identities. Define whether deletes are propagated via
   tombstones; never present an append-only upload as a deletion mirror.
2. **Bootstrap without a gap.** A newly enabled destination needs a bounded
   historical snapshot plus changes committed while it is being queued. Use a
   documented snapshot/journal cutoff and crash-safe bootstrap progress. Avoid
   unbounded write transactions or assuming an in-memory snapshot survives a
   restart. Capturing enabled jobs, initial backfill, selection changes, and
   newly added destinations must be tested with concurrent ingestion. A changed
   selection starts a defined job generation/backfill, not reuse of a cursor
   that skips newly included records.
3. **Persist before sending.** Materialize immutable bounded batches (or retained
   immutable record references), stable IDs, and their selection/mapping version
   before network I/O. Advance journal-to-queue progress in the same transaction
   as enqueuing. Never reconstruct a retry with different content under the same
   idempotency key. A plugin upgrade must preserve pending payload mapping or
   perform an explicit tested migration before processing those batches.
4. **Acknowledge after acceptance.** Batch states include pending, leased/in-flight,
   acknowledged, retry-wait, and blocked. Persist acknowledgments transactionally.
   Request timeouts, disconnects, process termination, and HTTP success without
   contract-valid acknowledgment cannot imply acceptance. Partial acknowledgments
   must track individual items or retain the whole batch for safe idempotent retry;
   a high-water mark cannot skip unresolved holes. Async remote acceptance requires
   a durable receipt/lookup or polling until the destination's promised durability
   level is reached.
5. **Define the delivery guarantee.** Deliver at least once; require stable
   destination idempotency/upsert support for duplicate-free observable results.
   Scope keys by destination/job and stable origin identity. Receiver writes must
   respect record revision order or reject stale revisions; default to ordered
   delivery per destination until safe concurrency is demonstrated. A destination
   without deduplication may receive duplicates after uncertain outcomes: expose
   this capability honestly, rather than advertising exactly-once delivery. A
   duplicate acknowledgment must correspond to the same record revision/payload.
6. **Control concurrent workers.** Use expiring per-destination leases with fencing
   generations, deadlines, and renewal. A stale worker cannot persist queue state
   or acknowledgments after another claims the job. Fencing cannot revoke a request
   already at the server, so retain remote idempotency. Never hold a SQLite write
   transaction open during network I/O. Local queries/ingestion remain responsive.
7. **Bound and retain work.** Apply byte/record limits and request deadlines. Queue
   and journal compaction may remove revisions only when no active destination or
   pending batch needs them. Disk exhaustion/queue limits must become visible and
   must not silently drop records or advance progress. Record a durable rescan
   obligation only if retained source data can reconstruct the exact revisions;
   otherwise fail the affected capture transaction visibly. Specify the retention
   tradeoff instead of promising both unlimited offline history and bounded disk.
   Pausing keeps pending work; discarding it is a separate explicit operation.
8. **Preserve selection/privacy rules.** Apply configured source/session/evidence
   selection and incognito exclusions before enqueueing. Re-check eligibility
   before dispatch where flags can change; queued now-excluded records must not
   leak through retries. Track suppressed/cancelled work separately from delivery
   success. Already-delivered remote deletion requires an explicit supported
   operation and is not implied by a local flag change.

### Background operation and visibility

Expose a single drain/run API and an opt-in long-running worker command. Persist
job configuration and next-attempt times so a restart resumes work. Retry
transient failures with exponential backoff and jitter; honor server retry hints.
Expired auth can use the plugin's bounded refresh flow; credentials needing user
interaction pause that destination and report action required, never open an
interactive login from an unattended worker. Permanent schema/permission errors
block affected delivery visibly, without silently skipping records or stopping
other destinations. Do not retry permanently rejected payloads forever.

Graceful shutdown stops claiming new work, bounds in-flight waits, and leaves
unconfirmed work retryable. Sleep/wake, offline periods, and worker restarts must
not lose jobs. Foreground drains and background workers coordinate through the
same leases. Start with one worker and bounded batches; no distributed broker
or fleet scheduler. Automatic OS service installation can follow separately;
the initial worker must be usable under an existing process supervisor.

Provide status/pause/resume/retry through the existing application, reporting each
destination's pending records/bytes, oldest pending age, blocked reason, last
attempt, and last acknowledged progress. Distinguish local capture, queueing, and
remote acceptance; “synced” cannot mean merely attempted or written to stdout.
Report the latest durable remote acceptance level the adapter actually guarantees,
not a promise of remote search indexing if the service acknowledges those separately.

### Plugin packaging and compatibility

Move cloud-client/auth bundle/preflight and associated build scripts/dependencies
and tests into the RelayHistory plugin project, with its own manifest, tsconfig,
lockfile, and package file list. The core build/install/prepare/typecheck must
not resolve the plugin, including through type-only imports. Retain Rust auth
stage selection, locking, atomic writes, error redaction, and legacy cursor
compatibility through an explicit migration. Use
only the narrow bridge this plugin actually requires; first produce evidence
for that bridge choice, rather than mandating a second general-purpose addon.
If core native contract 11 changes, bump its version and regenerate declarations.

The existing CLI/MCP loads explicitly configured plugins. Core commands/tools
remain available with no plugins. Plugin-specific commands/tools such as
`get_session_thread` register only when their plugin is enabled. Reject duplicate
command/tool identifiers deterministically. Loading a configured module must not
start login or upload; credentials/transports are touched only by an explicit
operation or a previously enabled delivery job.
Publish common public errors/types once so consumers retain error class identity.

Tests: build a small fake destination using public APIs only, without any
RelayHistory import or core edits. Verify export fidelity, bounded pagination,
selection/incognito rules, NDJSON output, idempotent retry after timeout/crash,
partial acknowledgments, revised records, and independent progress for two
instances. Also test no plugin, configured plugin, missing explicitly configured
plugin, duplicate tools, and local operations with a failing plugin installed.
Keep RelayHistory auth/replay/sharing compatibility tests in its plugin suite;
move root/cloud import-equivalence tests to the documented migration surface.
No live third-party integration is needed to prove the generic contract; the
real RelayHistory adapter must also pass its fixture-based acknowledgment and
auth compatibility suite.

Add fault-injection tests for termination before enqueue, after enqueue, after
remote acceptance but before local acknowledgment, partial acceptance, stale
leases and overlapping workers, remote acceptance with a lost response, and
out-of-order revisions. Restart against the same database after each fault and
assert exact record/revision fidelity, no gaps, stable retry payloads, and
no duplicated remote effects when the fake receiver supports idempotency.
Exercise initial backfill with concurrent writes, a plugin version change with
pending work, two destinations with one offline, rate limits, expired/revoked
auth, a permanently invalid record, disk/queue limits, eligibility changing while
queued, cancellation, and graceful shutdown. Status must distinguish pending,
blocked, suppressed, and acknowledged work in every case.

Verify: core and plugin install/typecheck/build/test independently; the existing
native contract check passes; installed core tarballs run SDK/CLI/MCP with no
plugins; installing/configuring the fixture plugin adds a destination without
rebuilding native code. Run the existing Node 20/22 checks and retain existing
local native platform coverage. Test any RelayHistory-specific bridge only on
platforms claimed by that plugin. Do not silently narrow existing supported
platforms; resolve any bridge compatibility gap before switching consumers.

## Stage 6 — Enforce absence in CI

Add `scripts/verify-local-history-isolation.mjs`, a staging verifier, and its tests.
Run it as `node scripts/verify-local-history-isolation.mjs` (new command). It must:

1. Copy only the local history Rust crates/TS/native/MCP packages into a temporary
   workspace and generate a minimal workspace manifest/lockfile there. Merely
   running `cargo test -p ... --no-default-features` in the full workspace does
   not prove sibling cloud packages can be absent during dependency resolution.
2. Install normal public build dependencies without cloud sibling directories
   or node_modules fallback paths. Compile/test Rust and independently
   install/typecheck/build/test TS and the addon. Inspect resolved dependency
   graphs and declarations, not just manifest text.
3. Pack and install the local tarballs in a fresh consumer outside the repo.
   Load the SDK/native addon, exercise cached reads and local discovery/sync/
   hydration, and launch CLI/MCP using fixture data. Block runtime network;
   dependency installation is a separate phase allowed registry access.
4. Exercise poisoned/missing commercial state and the counting seams from
   stage 2. Local operations must not read commercial state even if it exists.
5. Assert no cloud bundles, cloud dependencies, cloud native exports, or cloud
   MCP tools occur in the local distribution. Scope/connector contracts remain
   explicit and cached remote evidence remains readable offline.

Make that job mandatory alongside plugin integration tests. Include a test of the
verifier itself showing an injected forbidden dependency is rejected. Run
RelayHistory tests only in the plugin job; do not weaken the isolated job to make
them resolvable. Update release workflow tests and the install docs to name
how to use the core alone, pipe an export, and explicitly install/configure a plugin.

Verify: the new isolation command and both CI jobs exit 0. Retain their logs
and tarball inventories in the implementation PRs.

## Done criteria

- [x] All six stages have passing checks; known behavioral violations have
  regression tests, not skips.
- [x] The local history core builds/tests/typechecks/installs with cloud packages physically
  absent from the staged workspace and dependency graph.
- [x] Local operations read no commercial credential store and invoke no remote
  transport, including with Relaycast environment variables set.
- [x] Cached scope reads preserve requested scope regardless of login.
- [x] Discovery/sync/hydration selection and provenance are connector-specific;
  canonical identity, evidence ownership, deduplication, and pagination survive.
- [x] Core/engine depend on neither cloud, adapter implementations, nor CLI.
- [x] Native contracts, declarations, error classes, credential files, locks,
  refresh semantics, and cursor compatibility pass RelayHistory plugin regression tests.
- [x] The core tarball works alone; adding a fixture destination works without
  modifying core or rebuilding its native addon. NDJSON export works independently.
- [x] Two destination instances retain independent acknowledgment/checkpoint state.
- [x] One-shot and background delivery share the same durable coordinator and pass
  crash/restart, partial acknowledgment, concurrency, and offline recovery tests.
- [x] New or revised eligible records cannot fall between ingestion, bootstrap,
  queueing, and acknowledgment checkpoints. Retries preserve payload/identity.
- [x] Delivery status reports backlog and failures accurately; no silent drops or
  unsupported exactly-once claims. Core with delivery disabled remains local-only.
- [x] Core plus the RelayHistory plugin passes installed CLI/MCP smoke tests.
- [x] `git status --short` contains only in-scope changes; update the plan index.

## Stop conditions and maintenance

Stop a stage and report the concrete conflict if migration would drop evidence,
require guessing overwritten provenance, alter public identity, reset auth or
outbox cursors, change the server protocol, or require unplanned cloud-server
work. Do not reset fixtures to make a regression disappear. Report verification
failures that remain after two reasonable fixes before starting the next stage.

Review future changes against the isolation job and explicit connector registry.
Any temporary compatibility facade must remain above core and plugins. Moving another
function into an index file or adding optional imports is not a substitute for
the physically absent-package gate. This is a focused boundary plan; server
security, live third-party service protocols, general performance, and unrelated
repository issues were not audited.


## Approved server companion — dependable RelayHistory delivery

During implementation, inspection of sibling `relayhistory-cloud` at `8f4b8af`
confirmed legacy `/v1/ingest` and `/v1/sessions/:id/turns` use unconditional
upserts. A delayed request from an expired local lease can overwrite a newer
revision. On 2026-09-13 the user explicitly approved a companion server PR.

Implement a versioned delivery endpoint with authenticated tenant authority,
stable origin/record/revision identities, conditional revision upserts,
conflicting-idempotency detection, durable tombstone fences, and exact batch
acknowledgments persisted before success. Duplicate payloads return consistent
receipts; older revisions never replace newer data. Bound requests and provide
paginated readback of delivered records. Distinguish durable storage from
search indexing. Keep legacy endpoints/auth compatible. The optional RelayHistory
plugin uses the new endpoint for dependable jobs and fails clearly against an
older server; it must not silently fall back to the weaker protocol.

Add real database tests for concurrent/out-of-order requests, duplicate/lost
responses, conflicting batch/revision reuse, tombstones, auth tenant isolation,
and bounded readback. Client/server share wire fixtures. Work occurs in a
separate server worktree and PR; no deployment or production migration is
included. Server capability must be available before enabling the new plugin
transport in a release.

## Implemented package layout and compatibility decisions

The local workspace contains `ai-hist-core`, `ai-hist-engine`, `ai-hist-cli`, and
`ai-hist-napi`; the npm SDK remains `ai-hist`. Optional composition lives in
`plugins/relayhistory` (`@agent-relay/relayhistory`) and
`plugins/provider-sources` (`@agent-relay/history-provider-sources`). Each optional
package uses a platform-specific Rust executable and the one local native addon.
These concrete package names replace the provisional names used in earlier steps.

The standalone Rust CLI performs local acquisition. Remote acquisition is
available through the explicit Rust source registry or the SDK/plugin host; the
SDK CLI and MCP server own installed JavaScript plugin loading. Cached remote
queries remain available locally without credentials. This avoids maintaining
another dynamic plugin runtime inside the Rust CLI.

Legacy rows whose provenance was overwritten remain labeled `legacy-unknown`.
Fresh acquisitions retain independent snapshots but cannot safely delete or
reassign unknown canonical rows. The aggregate may retain stale legacy content;
this is documented and tested. Legacy Relaycast remains an explicit incremental
import in the optional package, retaining its original cursor semantics rather
than claiming to provide complete snapshots.

The companion server PR adds a versioned revision-fenced endpoint. Its migrated
SQL and actual SDK/native/helper request path are tested together using PGlite.
A non-production Neon smoke test remains a release gate. No production migration,
deployment, release, or scheduled-service change is performed by this work.

## Review outcome

Implemented and verified on the final integration branch. Local tests, native
contract checks, physically absent-package install/typecheck/tests, optional
package tests, installed tarballs, and SDK/helper/server recovery all pass.
GitHub CI remains responsible for Windows runtime and Linux release-platform
execution. Publishing is deliberately gated on a compatible released core and
updated plugin peer minimum; the currently published core is incompatible.
The companion server migration and non-production Neon validation are release
steps, not actions performed by this implementation. See 009-execution.md.

# Production architecture

RelayHistory has one production call graph:

```text
provider files / SQLite
        │
        ▼
ai-hist-core + Rust ingestion engine
        │ typed Rust functions
        ▼
ai-hist-native (Node-API, async worker tasks)
        │ typed native objects
        ▼
ai-hist TypeScript SDK
        ├── ai-hist Node CLI
        └── ai-hist MCP server
```

Rust owns provider discovery/parsing, schema creation and migration, direct
SQLite connections, catalog queries, history/event queries, search,
statistics, and sync. Blocking filesystem and SQLite work is dispatched away
from Node's event loop. TypeScript validates inputs, validates native contract
version 13, catalog contract version 3, hydration contract version 2,
session-relationship contract version 1, and session evidence contract version
1, normalizes nullable fields, maps native errors, and supplies pagination
helpers.

The CLI and MCP server import only the SDK's public functions. They do not
open SQLite, import `ai-hist-native`, scan providers, or invoke another CLI.

## Cloud authentication

The Rust cloud layer (`crates/ai-hist/src/cloud.rs`) owns RelayHistory token
exchange, stage selection, credential storage, and refresh. The SDK exposes
those operations through the `ai-hist/cloud` entrypoint. The root `ai-hist`
entrypoint re-exports the cloud API; the CLI and MCP server use the SDK. Both
entrypoints use the shared internal native loader and error definitions, and
the cloud module does not depend on the root module.

Each normalized service URL has a credential file and sync cursor under
`$RELAYHISTORY_HOME/stages` (default `~/.agentworkforce/relayhistory/stages`).
Rotation uses a stage lock and atomically persists the new token pair before
retrying. The native binding carries expiry, org, and workspace metadata to
the SDK. See [cloud setup](enable-cloud.md#authentication-and-stages) for
stage selection and transport requirements.

## Session ledger and location scope

There is one session ledger. `local` and `remote` are presences recording where
a logical session was observed, not independent session stores. Collection
operations accept one scope: `local` (the default), `remote`, or `all`. The
`all` view is the union of both presences, deduplicated by canonical session
identity, so materializing a remote session locally does not create a second
user-visible session. Locator, change stamp, and discovery state currently live
on each location presence. Independent observations from two remote connectors
can still overwrite those fields; connector-specific provenance is a separate
migration, not a guarantee of the selection API.

Cached SDK catalog, search, recent, and statistics reads preserve the requested
scope without reading commercial credentials or invoking remote transports.
Stored remote evidence remains readable with absent, malformed, expired, or
ambiguous commercial auth. Direct session and event lookup already names one
session and remains scope-independent. The CLI's existing first-use local
bootstrap can be disabled with `--no-bootstrap`.

Acquisition accepts `sourceConnectors` on discovery, hydration, and sync. Omit it
for the provider defaults (`claude-web`, `codex-cloud`), or pass `[]` to disable
remote acquisition. Commercial `cloud` recall and `relaycast` ingestion are
selected explicitly. Selection precedes credential checks; local scope never
probes any remote connector, even when commercial credentials are present.
`source` identifies the history provider while a connector ID selects its
acquisition path. See [Remote connectors](remote-connectors.md).

Remote acquisition without an available selected connector fails before database
creation. `all` can run local adapters while reporting unavailable selected
remotes. Its `scope` records the request and `locations_run` records the
locations executed. Local sync does not contact Relaycast; explicit Relaycast
sync writes new observations with remote presence. Historical Relay rows are
not relabeled.

This release adds selection among built-in connectors, not dynamically loaded
plugins. Cloud code and build dependencies remain in the existing distribution;
optional package extraction and the destination plugin/durable delivery APIs are
separate work. Native contract 12 prevents older addons from silently ignoring
`sourceConnectors`.

## Operation semantics

| Operation | Provider I/O | Database work | Missing database |
|---|---:|---|---|
| `listSessionCatalog*` (`local` / `remote` / `all`) | none | one indexed cache query | empty page |
| `discoverSessions` (`local`, default) | bounded shallow reads | catalog upserts | creates catalog DB |
| `discoverSessions` (`remote`) | configured remote connectors (error when none) | catalog upserts + presences | creates catalog DB |
| `discoverSessions` (`all`) | local adapters + configured remote connectors | catalog upserts + presences | creates catalog DB |
| `hydrateSession` | one selected provider session and linked evidence | transactional evidence + checkpoint upsert | `SESSION_NOT_FOUND` |
| `search`, `recent` (`local` / `remote` / `all`) | none | indexed reads | empty result |
| `stats` (`local` / `remote` / `all`) | none | indexed aggregate reads | empty result |
| `getSession` | none | indexed identity read | empty result |
| `getSessionEventsPage` | none | bounded keyset page | empty page |
| `getSessionRelationships` | none | indexed relationship reads | empty result |
| `getSessionTree` | none | indexed relationship reads, one child query per emitted node | root-only tree |
| `getSessionChildrenPage` | none | bounded keyset page | empty page |
| `getSessionToolCallsPage`, `getSessionFileEditsPage` | none | bounded keyset page over one source's session | empty page |
| `sync` (`local`, default) | full explicit scan | migrations + ingestion | creates DB |
| `sync` (`remote`) | configured remote connectors (error when none) | catalog upserts + presences | creates DB |
| `sync` (`all`) | full local scan + configured remote connectors | migrations + ingestion | creates DB |

No read operation invokes discovery or sync. A common cold start is:

```ts
await discoverSessions({ limit: 100, scope: 'local' });
const sessions = await listSessionCatalog({ limit: 100, scope: 'all' });
await hydrateSession({ source: sessions[0].source, sessionId: sessions[0].sessionId });
```

Global sync owns enumeration while targeted hydration resolves one persisted
catalog presence. Both call the same Rust provider normalization helpers.
TypeScript never parses a provider source or opens SQLite. Per-session
checkpoints make unchanged calls constant-work after source resolution. Remote
hydration stays provider-bounded: Claude uses the CLI's private
teleport-evidence contract, while Codex indexes the supported cloud diff and
reports partial capability because no transcript export exists.

## Delegation topology

`session_relationships` records one row per observed delegation, keyed by
`(source, parent_session_id, relationship_uid)`. Each row carries what
established the link — `evidence_kind`, the provider file in
`evidence_locator`, and the provider-native reference in `evidence_ref` (a
Claude `toolUseId`, a Codex `parent_thread_id`) — plus whatever the provider
recorded about the child: agent type, agent name, model, spawn depth, and the
provider's own spawn time.

`identity_status` separates two honestly different things. An `observed` row
names the child: `child_session_id` is the provider's own identity for it, and
`child_has_events` says whether that child is independently addressable
through `getSessionEventsPage`. An `unlinked` row means the provider recorded
a delegation but no stable child identity, so `child_session_id` is null and
the child's output stays attributed to the parent. A child id is never
synthesized, never derived from a file name, and never inferred.

What a provider can record is a property of the provider, not of a database,
so every result reports it:

| Source | Stable child identity | Agent type | Spawn time | Evidence locator |
|---|---|---|---|---|
| `codex` | always | yes | yes | yes |
| `claude` | sometimes (only versions that emit a per-child `agentId`) | yes | yes | yes |
| `cursor`, `grok`, `opencode`, `relay` | never | no | no | no |

A linked child's events are stored under the child's own session id and are
never flattened into the parent. The delegated instruction that started a
child is not a human prompt: it never becomes a `history` row, and a delegated
thread never becomes a top-level catalog session.

Children are ordered by `(spawned_at_ms, relationship_uid)` with null spawn
times at the tail; `relationship_uid` is unique per parent, so that is a total
order shared by `getSessionChildrenPage`, `getSessionTree`, and the SDK's
`sessionDescendants` walker. Traversal is pre-order over an explicit stack,
never recursion, and `nodes[0]` is always the root — including for a session
with no recorded delegation and for a database that does not exist yet.

A session appears exactly once, at the position pre-order first reaches it. An
edge back into the current branch's own ancestry is a cycle: it is not expanded
again and emits a `RELATIONSHIP_CYCLE` diagnostic. An edge to a session already
emitted on another branch is a diamond, not a loop; it is simply not expanded a
second time, and is neither diagnosed nor counted as truncation.

`maxDepth` (default 32, maximum 64) and `maxNodes` (default 1000, maximum
10000) bound the work to one indexed child query per emitted node and surface
`RELATIONSHIP_TREE_DEPTH_LIMIT` and `RELATIONSHIP_TREE_TRUNCATED` diagnostics
instead of silently short results. Tree-level `truncated` means a budget
stopped the walk short of the recorded evidence, so a cycle or diamond never
sets it; node-level `truncated` marks every node whose children were left
unexpanded, including all parents still pending when the node budget ran out.
Unlinked rows are reported in `unlinked` with a `RELATIONSHIP_UNLINKED_CHILD`
diagnostic rather than traversed — at every depth, the boundary included, so a
node whose only children are unlinked evidence is complete rather than
truncated. Only a session the tree has not already emitted is charged against
`maxNodes`, so a diamond is never reported as a budget truncation. Deterministic
Claude remote-to-local materialization is also recorded as a
`materialized_local` relationship; title or prompt similarity is never used to
infer one. The SDK's
`sessionDescendants` walker applies the same `childCount` and `truncated`
rules to the nodes it yields.

Events use `(ts_ms, id)` keyset pagination. Tool calls and file edits use the
same keyset shape over `(ts_ms IS NULL, ts_ms, id)`: both tables allow a null
timestamp, so undated rows sort last and the cursor carries a nullable
`ts_ms`. Those two pages require a source as well as a session id, because a
session id alone can name one session per provider. Catalog ordering is
`(last_activity_ms DESC, source ASC, session_id ASC)`, with null timestamps at
the tail. Relationship ordering is `(spawned_at_ms, relationship_uid)`, also
with null timestamps at the tail. These total orders prevent duplicate or
omitted rows at timestamp ties.

## Native errors

The SDK distinguishes unsupported platform, supported platform package
missing, addon load failure, native/SDK contract mismatch, database-open
failure, invalid argument, query failure, discovery failure, and sync failure.
There is no alternate runtime after any native-load error.

## Durable delivery and snapshot export

`ai-hist-core::delivery` owns opt-in journaling, bounded snapshots, immutable
queue/payload persistence, exact acknowledgments, retention, and fenced leases.
The SDK host registers explicitly selected destination modules and runs the same
drain loop for foreground and background delivery. Native contract 13 adds a
typed serialized delivery/export bridge to the existing addon. No TypeScript or
plugin code queries SQLite. [Delivery documentation](history-delivery.md) describes
selection, failure states, background operation, and the independent NDJSON path.

Core maintenance is bounded. The host expires abandoned snapshots and compacts
consumed journal/receipt rows during drains; status exposes retained bytes and
limits. The existing RelayHistory cloud package boundary and outbox migration
remain separate from the generic coordinator.

# ai-hist

The public TypeScript SDK, Node CLI, and MCP server for RelayHistory. Every
operation uses the mandatory `ai-hist-native` Node-API engine; there is no
JavaScript SQLite implementation or provider-file fallback.

```bash
npm install ai-hist
```

```ts
import {
  CATALOG_SOURCES,
  discoverSessions,
  hydrateSession,
  listSessionCatalogPage,
  getSession,
  getSessionEventsPage,
  sessionEvents,
  getSessionToolCallsPage,
  sessionToolCalls,
  getSessionFileEditsPage,
  sessionFileEdits,
  search,
  recent,
  stats,
  sync,
} from 'ai-hist';

// Runtime validation stays in step with the sources supported by this build.
for (const source of CATALOG_SOURCES) console.log(source);

await discoverSessions({ scope: 'all', limit: 100 });
const page = await listSessionCatalogPage({ scope: 'all', limit: 20 });
const first = page.sessions[0];
if (first) {
  await hydrateSession({ source: first.source, sessionId: first.sessionId });
  const prompts = await getSession(first.sessionId);
  const events = await getSessionEventsPage(first.sessionId, { limit: 200 });
  for await (const event of sessionEvents(first.sessionId)) consume(event);
  const tools = await getSessionToolCallsPage(first.source, first.sessionId, { limit: 200 });
  const edits = await getSessionFileEditsPage(first.source, first.sessionId, { limit: 200 });
  for await (const call of sessionToolCalls(first.source, first.sessionId)) consume(call);
  for await (const edit of sessionFileEdits(first.source, first.sessionId)) consume(edit);
}
```

All APIs are async. `listSessionCatalog` and `listSessionCatalogPage` are
cache-only. `discoverSessions` is shallow discovery. `hydrateSession` is
targeted evidence acquisition for one existing catalog row. It returns
`hydrated`, `updated`, `unchanged`, or `capability_limited`, an indexed source
stamp, evidence counts, related native session IDs, and bounded-work metrics.
Targeted remote hydration uses `scope: 'remote'`: Claude web sessions can
return `capability: 'full'` after complete teleport evidence is acquired;
Codex cloud tasks return `partial` when their supported unified diff is indexed
or `shallow_only` when no richer evidence is exposed. Partial results retain
`discoveryState: 'shallow'`. `sync` is full ingestion.
Missing databases return empty read results; they do not trigger provider I/O.

`SOURCES` and `CATALOG_SOURCES` are immutable runtime registries corresponding to
the exported `Source` and `CatalogSource` types. Use `isSource(value)` or
`isCatalogSource(value)` to validate untyped input without maintaining a copied
provider list. `CATALOG_SOURCES` is derived from `SOURCES`; `trajectory` is a
history source but not a discoverable session catalog source, so it appears
only in `SOURCES`.

Session discovery, listing, search, recent history, statistics, and sync accept
`scope: 'local' | 'remote' | 'all'`. Scope defaults to `local`, preserving
offline behavior and making provider-cloud access explicit. The CLI exposes the
same mutually exclusive `--local`, `--remote`, and `--all` flags; omitting them
is equivalent to `--local`.

Cached reads preserve the requested scope and never consult commercial auth.
Stored remote history can be queried with absent, malformed, expired, or
ambiguous credentials. Discovery, hydration, and sync accept `sourceConnectors`:
omit it for provider defaults (`claude-web`, `codex-cloud`), supply an explicit
list, or use `[]` to disable remote acquisition. Local scope never probes remote
credentials. Commercial login does not automatically select cloud recall or
Relaycast.

```ts
await discoverSessions({ scope: 'remote', sourceConnectors: ['cloud'], sources: ['cursor'] });
await sync({ scope: 'remote', sourceConnectors: ['relaycast'] });
await sync({ scope: 'all', sourceConnectors: [] }); // local adapters only
const cached = await stats({ scope: 'remote' }); // no login required
```

```bash
ai-hist sessions discover --remote --source-connector cloud --source cursor
ai-hist sync --remote --source-connector relaycast
ai-hist sync --all --no-source-connectors
```

`--source-connector` is repeatable and cannot be combined with
`--no-source-connectors`. MCP uses `source_connectors`. Invalid selections fail
before acquisition. Remote discovery/sync without an available selected connector
returns `UNSUPPORTED_OPERATION`; targeted hydration returns
`CONNECTOR_NOT_CONFIGURED`. These failures precede database creation. `all`
continues local acquisition and reports unavailable selected remotes.

This selection API addresses built-in connectors; plugin loading and optional
cloud package extraction are separate work. Cloud dependencies remain in the
current package. SDK/native contract 12 is required so older addons cannot
silently ignore connector selection. See `docs/remote-connectors.md` for
connector capabilities and authentication.

Catalog pages, discovery results, statistics, and sync results echo the requested `scope`,
and discovery results additionally report `locationsRun` — the connector
locations that actually executed.
History and catalog rows have `locations`, containing `local`, `remote`, or both, so an
`all` query still returns one logical session while preserving where it was
found. `resumeCommand()` returns `null` for a remote-only history row rather
than emitting a local CLI command; an empty `locations` array retains legacy
local behavior for rows written before provenance tracking.

The event primitive is page-based and uses `{ tsMs, id }` as a deterministic
cursor. The `sessionEvents` async iterator walks pages without accumulating a
large transcript. `getSessionEvents` is an explicit collecting convenience.

Tool calls and file edits follow the same page / iterator / collect trio
(`getSessionToolCallsPage`, `sessionToolCalls`, `getSessionToolCalls`, and the
`...FileEdit...` equivalents), and take both a source and a session ID because
provider session IDs are not unique across providers. The source is half of
that identity, so one outside the `Source` set raises `InvalidArgumentError`
instead of reading as an empty session. Their cursor is
`{ tsMs: number | null, id: number }`: a record may be indexed without a
timestamp, and undated records are ordered last. Feeding a cursor back in
accepts either spelling of that tail — `tsMs: null` as emitted, or no `tsMs` at
all after a transport that drops nulls — so a printed or JSON-serialized cursor
returns unedited. Stored provider JSON is parsed
into `args` and `structuredPatch`; a value that is absent or unparseable
becomes `null` while the raw string stays available as `argsJson` and
`structuredPatchJson`, so an unreadable payload never fails a page.
`parseStoredJson(raw)` is that same parse, exported for callers that hold a raw
stored string of their own: it returns the parsed value, or `null` for anything
that is not a parseable string.

## Delegation topology

Sessions that delegate to subagents form a tree, and it is queryable:

```ts
import {
  getSessionRelationships,
  getSessionTree,
  sessionEventsIncludingDescendants,
} from 'ai-hist';

const { asParent, asChild, capabilities } = await getSessionRelationships({
  source: 'codex',
  sessionId: rootId,
});
const tree = await getSessionTree({ source: 'codex', sessionId: rootId, maxDepth: 8 });

for await (const event of sessionEventsIncludingDescendants({ source: 'codex', sessionId: rootId })) {
  // event.sessionId is the session that actually produced the event: a
  // child's event is never rewritten as the parent's.
  consume(event);
}
```

Each `SessionRelationship` reports its `identityStatus`. `observed` means the
provider named the child, so `childSessionId` is a real session id and
`childHasEvents` says whether its events are independently addressable through
`getSessionEventsPage`. `unlinked` means the provider recorded a delegation but
no stable child identity: `childSessionId` is `null`, the child's output stays
attributed to the parent, and the row keeps the evidence (`evidenceKind`,
`evidenceLocator`, `evidenceRef`) that established it. An identity is never
synthesized.

`capabilities.stableChildIdentity` tells you what to expect from the provider
before you read a single row: `always` for Codex, `sometimes` for Claude — only
provider versions that emit a per-child `agentId` name the child — and `never`
for the remaining sources.

`getSessionTree` returns pre-order `nodes` (the root first), the `unlinked`
evidence found at any depth, and `diagnostics`. `nodes[0]` is always the root,
so a session with no children — and a database that does not exist yet — comes
back as a one-node tree rather than an empty one. Children are ordered by
`(spawnedAtMs, relationshipUid)` with null spawn times last, so repeated calls
against the same database return identical results.

Traversal visits each session once, at the position pre-order first reaches it.
An edge back into the current branch's ancestry is a cycle and emits a
`RELATIONSHIP_CYCLE` diagnostic instead of looping; an edge to a session already
emitted on another branch is a diamond and is quietly not expanded twice.
`maxDepth` (default 32, maximum 64) and `maxNodes` (default 1000, maximum
10000) bound the work and set `truncated` with a
`RELATIONSHIP_TREE_DEPTH_LIMIT` or `RELATIONSHIP_TREE_TRUNCATED` diagnostic
rather than returning a silently short tree. Tree-level `truncated` means a
budget cut the walk short — a cycle or diamond never sets it — while a node's
own `truncated` marks children it did not expand.

For large topologies, `getSessionChildrenPage` is the keyset primitive and
`sessionDescendants` is the async iterator over it, walking descendants
breadth-first without materializing a tree. Its `maxNodes` budget has the same
1,000 default and 10,000 maximum as `getSessionTree`, and includes the root. It
yields the nodes `getSessionTree` emits minus the root, with the same
`childCount` (linked children only, at the depth boundary included) and the
same `truncated` rule.
The order differs: the walker is breadth-first, the tree is pre-order, so a
consumer that depends on either the root node or `getSessionTree`'s ordering
has to account for that. `sessionEventsIncludingDescendants` applies its
`limit` to each session it reads, not to the iteration as a whole.

Native loading failures distinguish unsupported platforms, missing optional
platform packages, addon load failures, SDK/native contract mismatches, and
database open failures through stable `RelayHistoryError` subclasses. Provider
capability failures use `UnsupportedOperationError`. Targeted remote hydration
also distinguishes connector configuration, expired authentication, partial
evidence, and connector/parser failures with dedicated error subclasses.

The old synchronous `AiHist` class and `openAiHist()` API were removed in 1.0.
See [the migration guide](https://github.com/AgentWorkforce/relayhistory/blob/main/docs/native-sdk-migration.md).

## Cloud opt-in

Run `ai-hist enable-cloud` to log in, drain local history and keep pushing. Use `--once` to exit after draining. The npm CLI bundles Agent Relay Cloud login, so no separate `agent-relay` CLI install is required; a run without a TTY fails promptly with interactive-login and token guidance. The async SDK exports `enableCloud`, `pushCloud`, `installGitHooks` and `createShareableTrace`; RelayHistory exchange, transport, and stage-scoped auth stay in Rust. See [cloud setup](https://github.com/AgentWorkforce/relayhistory/blob/main/docs/enable-cloud.md).

`ai-hist/cloud` owns the cloud API wrappers, including `loginCloud` and
`loadStoredRelayhistoryAuth`, and delegates to the Rust cloud layer through N-API.
The root `ai-hist` entrypoint re-exports the cloud API for convenience. Credentials
live in `$RELAYHISTORY_HOME/stages`, defaulting to
`~/.agentworkforce/relayhistory/stages`. The Rust layer selects the stage, checks
transport security, preserves session metadata, and saves rotated tokens atomically.

## Session thread transport

`getSessionThread` accepts `SessionThreadOptions.fetchImpl` for the thread GET
request and its retries. Authentication requests use the Rust HTTP client.
A stored-session 401 can therefore trigger a native refresh request even when
`fetchImpl` is mocked. Custom proxy or TLS settings supplied through `fetchImpl`
apply only to thread requests; the auth endpoint must also be reachable by the
native client for automatic refresh to succeed.

For isolated mocks or caller-managed credentials, supply `resolveSession` that
returns `{ auth }` without the `session: true` marker:

```ts
import { getSessionThread } from 'ai-hist/cloud';

const thread = await getSessionThread(
  { source: 'claude', sessionId: 'example-session' },
  {
    resolveSession: async () => ({
      auth: { baseUrl: 'https://history.example.com', accessToken: 'rth_at_fixture' },
    }),
    fetchImpl: async () => new Response(JSON.stringify({
      session: null, outcomes: [], links: [], nextCursor: null,
    })),
  },
);
```

This resolver disables automatic refresh. A 401 from the selected transport
throws `AuthenticationExpiredError` without an authentication request. The
default `resolveCloudSession` returns the marker for stored credentials and
enables native refresh.

## Cloud token and replay

The npm CLI uses the same Rust engine as the public async SDK:

```bash
# Shell-safe token export (fails if ai-hist token fails)
RTH_TOKEN="$(ai-hist token)" || { echo "Failed to get token" >&2; exit 1; }
export RTH_TOKEN
ai-hist replay SESSION_ID
ai-hist replay SESSION_ID --json --out transcript.json
```

```ts
import { accessToken, replay } from 'ai-hist';

const token = await accessToken(); // Secret: do not log it.
const result = await replay('SESSION_ID', { json: true });
const events = JSON.parse(result.transcript!);
await replay('SESSION_ID', { json: true, out: 'transcript.json' });
```

Both APIs accept `baseUrl` (`--base-url` in the CLI) for explicit stage selection.
Token refresh and persistence run in Rust before returning a token with at least
60 seconds of recorded validity. Piped `token` stdout contains only the token
and one newline; failures leave stdout empty. Replay fetches every page in
server order; `limit` is the page size, and `maxContent` caps each event's content.
File output replaces its destination atomically only after all pages succeed.
Neither command opens or imports into the local history database.

# RelayHistory (`ai-hist`)

RelayHistory indexes coding-agent sessions from Claude Code, Codex, Cursor,
Grok, OpenCode, and Agent Relay in a local SQLite database.

Its production architecture has one implementation:

```text
Rust engine (providers + SQLite + migrations + queries)
  → Node-API addon
    → TypeScript SDK
      → Node CLI / MCP server
```

TypeScript never opens SQLite, scans provider files, or invokes another
executable. The native addon is mandatory and operates on the SQLite file in
place, including WAL-backed databases.

## Install

Node.js 20 or 22 is required. npm installs the SDK, CLI, MCP server, native
loader, and matching prebuilt platform package:

```bash
npm install ai-hist

# Global CLI
npm install --global ai-hist
ai-hist sessions discover --limit 100
ai-hist sessions list --limit 100
ai-hist sessions hydrate codex 01a04f0c-... --json
ai-hist sessions tree codex 01a04f0c-...
ai-hist sessions tools codex 01a04f0c-... --limit 50 --json
ai-hist sessions edits codex 01a04f0c-... --limit 50 --json
ai-hist resume "the feature I was working on"          # prints the best match's native resume command
ai-hist pack "the feature I was working on" --tokens 1500  # compact context to hand another agent
```

`sessions relationships` and `sessions tree` read the delegation topology a
session recorded: which subagent threads it spawned, what evidence established
each link, and whether a child's events are addressable on their own.

Session-set commands share one mutually exclusive location scope:

```bash
ai-hist sessions list --local   # default; identical to omitting a scope flag
ai-hist sessions list --remote  # cached sessions with a remote presence
ai-hist sessions list --all     # union of both, with each session returned once
```

The same `--local` / `--remote` / `--all` contract applies to discovery,
search, recent history, statistics, and sync. Local and remote are presences of a session,
not separate catalogs: every result comes from the same session ledger. Reads
only filter that cached ledger. Remote discovery and remote sync run through
provider connectors: `claude-web` lists your claude.ai/code web sessions using
the OAuth sign-in the Claude Code CLI stored, and `codex-cloud` lists Codex
cloud tasks through `codex cloud list --json`. The `cloud` connector lists
teammate sessions from RelayHistory using the stored `rth_at_` session from
`ai-hist login`, with at least 60 seconds of recorded validity remaining. See
[docs/remote-connectors.md](docs/remote-connectors.md). With no connector
configured, remote-only acquisition returns an unsupported-operation error; it
never silently falls back to local work. `--all` runs the local adapters plus
every configured connector, and an acquisition summary reports the requested
`scope` alongside the `locations_run` that actually executed. Each catalog
row's `locations` array reports where that session was actually observed.

| Remote connector | Source | Stored sign-in |
|---|---|---|
| `claude-web` | `claude` | Claude Code OAuth |
| `codex-cloud` | `codex` | Codex CLI |
| `cloud` | Upstream provider source; org-wide teammate sessions | RelayHistory `rth_at_` session under `RELAYHISTORY_HOME` (default `~/.agentworkforce/relayhistory`) |

Cloud discovery records `cloud://<orgId>/<sessionId>` on the remote presence.
An existing local natural key gains a remote presence, so `--all` returns it
once. Cloud recall shares push's stage selection, credential refresh, and
HTTPS-or-loopback guard. Discovery populates the cached session catalog;
automatic cloud event ingestion is separate.

No Rust toolchain, C/C++ compiler, standalone CLI, curl installer, or runtime
binary download is used.

### Checking your version

`ai-hist --version` prints the installed npm package version. In an interactive
terminal it also checks npm for a newer release, with a three-second timeout and
silent offline fallback. Suppress the notice with `--no-warning` or
`RELAYHISTORY_NO_UPDATE_CHECK=1`:

```bash
ai-hist --version
ai-hist --version --no-warning
```

## TypeScript

```ts
import {
  discoverSessions,
  hydrateSession,
  listSessionCatalog,
  getSessionEventsPage,
  getSessionToolCallsPage,
  getSessionFileEditsPage,
  search,
  sync,
} from 'ai-hist';

await discoverSessions({ limit: 100, scope: 'local' });
const sessions = await listSessionCatalog({ limit: 100, scope: 'all' });
if (sessions[0]) {
  await hydrateSession({ source: sessions[0].source, sessionId: sessions[0].sessionId });
}
const firstEvents = sessions[0]
  ? await getSessionEventsPage(sessions[0].sessionId, {
      source: sessions[0].source,
      limit: 200,
    })
  : null;
const firstTools = sessions[0]
  ? await getSessionToolCallsPage(sessions[0].source, sessions[0].sessionId, { limit: 200 })
  : null;
const firstEdits = sessions[0]
  ? await getSessionFileEditsPage(sessions[0].source, sessions[0].sessionId, { limit: 200 })
  : null;
const matches = await search('migration', { limit: 20, scope: 'all' });
await sync({ scope: 'local' }); // explicit full ingestion; local is the default
```

`listSessionCatalog` is cache-only. `discoverSessions` performs bounded shallow
provider discovery and updates the shared ledger. `hydrateSession` indexes the
richest evidence a connector safely exposes for one existing catalog identity
and reports `full`, `partial`, or `shallow_only` capability with evidence
counts; repeating it is safe as a live session grows. The tool call and file
edit pages read that hydrated evidence back as structured rows and require both
a source and a session ID, because provider session IDs collide. `sync` performs
full global ingestion. Reads never silently turn into discovery, hydration, or
sync. Hydration includes linked subagent evidence by default; CLI callers can
pass `--no-related`.

## MCP

```bash
npx -y ai-hist-mcp
```

MCP exposes thin adapters for search, recent history, catalog listing,
discovery, targeted hydration, sessions, paged events, paged tool calls and
file edits, statistics, and sync.

## Supported production matrix

| OS/runtime | Architectures |
|---|---|
| macOS 12+ | arm64, x64 |
| Linux glibc | arm64, x64 |
| Linux musl | arm64, x64 |
| Windows 10/11 and Server 2022 | x64 MSVC |

Node-API level 4 is used. CI tests Node.js 20 and 22. Windows arm64, FreeBSD,
Bun, Deno, browsers, Electron renderer processes, and other unlisted runtimes
are not supported. Windows arm64 remains follow-up work until it can be built
and executed reliably in CI.

See [architecture](docs/architecture.md), [getting started](docs/getting-started.md),
[migration](docs/native-sdk-migration.md), and [release validation](docs/releasing.md).

## Repository development

The `ai-hist-engine` Rust binary target remains an internal development harness while
provider-ingestion code is being physically separated from command rendering.
It is not published as a second user runtime. Normal users install with npm.

```bash
cargo test --workspace
cd crates/ai-hist-napi && npm ci && npm run build:debug
cd ../../sdk-ts && npm install && npm test
```

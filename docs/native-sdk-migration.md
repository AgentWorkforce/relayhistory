# Migration to ai-hist 1.0

Version 1.0 is deliberately breaking. Remove standalone/curl-installed
`ai-hist` binaries and install the npm package globally if CLI access is
needed:

```bash
npm uninstall --global ai-hist-mcp
type -a ai-hist
npm install --global ai-hist ai-hist-mcp
type -a ai-hist
```

Before installing, remove any standalone executable reported by the first
`type -a` (commonly `~/.local/bin/ai-hist`). After installing, verify the
first result points into npm's global installation rather than the old binary.

The environment variables `AI_HIST_RUST_BIN` and `AI_HIST_CLI` no longer have
meaning. `AI_HIST_DB` remains the database-path override.

Replace the synchronous snapshot API:

```ts
// before
const hist = await openAiHist({ fallback: 'jsonl' });
const rows = hist.search('cursor');
hist.close();

// 1.0
const rows = await search('cursor');
```

Every database API is now async. Remove `fallback`, `sourceKind`, `close`, and
binary-path options. If the database is missing, read APIs return empty data;
call `discoverSessions()` or `sync()` explicitly.

Collection APIs, including statistics, now use a shared `scope` option: `'local'`, `'remote'`, or
`'all'`; omitting it means `'local'`. The CLI equivalents are mutually
exclusive `--local`, `--remote`, and `--all` flags. Local and remote rows live
in one ledger and represent presences of the same session, so `all` is
deduplicated. Direct session and event lookup does not take a scope. Remote
discovery and sync run through provider connectors
([Remote connectors](remote-connectors.md)) and fail explicitly on a machine
where none is configured; `all` acquisition runs all currently configured
adapters.

The acquisition result's `scope` echoes what was requested; the separate
`locationsRun` list reports the connector locations that actually executed.
Use each returned session's `locations` for observed presences. Search,
recent, and direct-session history rows also carry `locations`;
`resumeCommand()` returns `null` when those locations are remote-only.

Rust embedders must also update and recompile. Catalog/discovery option, page,
summary, and row structs gained scope/location fields. The local-named wrapper
functions now reject a non-local option instead of silently coercing it; call
the corresponding `*_scoped*` API for `remote` or `all`. Human CLI parsers must
account for a locations column in catalog rows, a scope line in statistics,
and requested/executed wording in discovery summaries. Remote-only matches do
not produce a local resume command.

Replace unbounded event reads with a page or async iterator:

```ts
const page = await getSessionEventsPage(id, { limit: 200 });
for await (const event of sessionEvents(id, { limit: 200 })) consume(event);
```

The 1.0 CLI covers `sessions list`, `sessions discover`, `search`, `recent`,
`session`, `events`, `stats`, `sync`, `token`, `replay`, and `enable-cloud`.
`accessToken()` and `replay()` expose the existing Rust cloud operations through
NAPI. Token output stays secret-only on stdout; replay pagination, rendering and
atomic `--out` replacement remain in Rust and never open the local history DB.
Removed legacy cloud/Pair/tag/
trajectory convenience commands must migrate to dedicated services or future
native-backed SDK operations; they are not retained through subprocess or
JavaScript-SQL fallbacks.

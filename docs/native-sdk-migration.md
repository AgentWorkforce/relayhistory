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
discovery and sync require explicitly installed source plugins
([Source plugins](remote-connectors.md)); provider or commercial login does not
select them. `all` acquisition runs local adapters and the selected plugins.

The acquisition result's `scope` echoes what was requested; the separate
`locationsRun` list reports the connector locations that actually executed.
Use each returned session's `locations` for observed presences. Search,
recent, and direct-session history rows also carry `locations`;
`resumeCommand()` returns `null` when those locations are remote-only.

Rust embedders must also update and recompile. Catalog/discovery option, page,
summary, and row structs gained scope/location fields. The local-named wrapper
functions now reject a non-local option instead of silently coercing it; call
the corresponding `*_scoped*` API for cached `remote` or `all` reads. Remote
acquisition must compose optional adapters through `SourceRegistry`; the local
engine has no built-in HTTP or auth transports. Human CLI parsers must
account for a locations column in catalog rows, a scope line in statistics,
and requested/executed wording in discovery summaries. Remote-only matches do
not produce a local resume command.

Replace unbounded event reads with a page or async iterator:

```ts
const page = await getSessionEventsPage(id, { limit: 200 });
for await (const event of sessionEvents(id, { limit: 200 })) consume(event);
```

The local npm CLI covers sessions, search, recent, events, statistics, sync,
export, and generic delivery/plugin operations. Cloud auth, sharing, and replay
moved out of the mandatory native binding and SDK into the optional
`@relayhistory/capture` package. Replace imports of `accessToken`, `replay`,
`enableCloud`, and other cloud functions from `ai-hist` or `ai-hist/cloud` with
imports from that package. Local Git linkage remains in `ai-hist`.

Use `relayhistory-plugin login|token|replay|enable-cloud` for the optional legacy
CLI. New dependable background delivery uses `ai-hist plugin relayhistory-enable`
and `ai-hist delivery run` with explicit configuration and selection. Stop old
managed push schedules before enabling a durable generation; existing positional
cursors cannot be converted to acknowledgments. See [cloud setup](enable-cloud.md).

Native contract 15 rejects older addons whose credential-driven source selection
or cloud exports would violate the new boundary. Optional source plugins use the
fixed JSON observation/evidence intake; installing another source does not
require rebuilding that addon. JavaScript and MCP hosts continue to use public
SDK operations rather than opening SQLite themselves.

# Source plugins

Local provider files, storage and cached queries live in the local history
packages. Remote provider acquisition is optional and explicitly composed.
Installing a package does not change local data sources.

| Optional package | Connector IDs | Evidence |
|---|---|---|
| `@relayhistory/provider-sources` | `claude-web`, `codex-cloud` | Claude teleport events; Codex supported task diff |

```json
{"plugins":[{"module":"@relayhistory/provider-sources","options":{"connectors":["claude-web"],"instanceId":"personal"}}]}
```

```sh
ai-hist sessions discover --remote --config history.json --source-connector claude-web
ai-hist sync --all --config history.json --source-connector claude-web
ai-hist sync --all --no-source-connectors
```

The config is resolved beside its installed node_modules. MCP loads it only when
`AI_HIST_PLUGIN_CONFIG` is explicitly set. SDK callers register `createHistoryPlugin`
results in `HistoryPluginRegistry` and pass `plugins` on acquisition calls.
`sourceConnectors` selects IDs, or `id:instance` for one instance; omitted means
all explicitly registered sources. `[]` disables remotes. Default/local scope
never invokes remote callbacks; it runs only the local source plugins described
below. Cached search, recent, catalog and stats preserve
local/remote/all independently of auth.

The local native engine does not contain remote transports. A remote request
without a selected plugin fails explicitly. Discovery with all scope keeps local
results and reports unavailable selected source plugins. Targeted hydration uses
an existing observation without listing other tasks; a missing observation can
be discovered through the selected source before acquisition.

Observations are keyed by `(source,session_id,location,connector_id,connector_instance)`.
`raw_locator` is an opaque acquisition handle, separate from `raw_path` for
display or local database paths. Each connector retains its own stamp, access
state, evidence records and checkpoint. Location presence remains an aggregate
on one canonical session identity, so two connectors do not create duplicate
user-visible sessions or erase one another's provenance.

Plugins submit normalized history, session events, tool calls, file edits,
relationships and commit links through public native intake APIs. A complete
snapshot declares which kinds it covers; omitted kinds remain untouched. Intake
validates identities and fields, fences the observation revision captured before
I/O, and applies the snapshot atomically. An older concurrent response returns
`SOURCE_REVISION_CONFLICT`. Incoming database row IDs are never reused locally.

Provider limits and authentication stay in the
[optional provider helper](../plugins/provider-sources/rust/README.md).
Claude uses an observed private teleport interface which can change. Codex's
supported diff does not imply a full transcript, token log or tool history.
Partial evidence is reported as partial capability.

A plugin may also register destinations (`HistoryDestination`), separately from
sources: a custom service can receive history without providing remote
discovery, or expose a source without accepting history.

## Local source plugins

A source plugin can also read this machine's own files. That is how a harness
store the built-in parsers do not know about — a host application that keeps
Claude Code transcripts in its own directory, say — reaches the catalog
without a core release. Declare `location: 'local'` and the absolute
directories the source reads:

```js
export function createHistoryPlugin({ root }) {
  return { sources: [{
    id: 'host-app', instanceId: 'default', location: 'local', roots: [root],
    supportedSources: ['claude'],
    async discover() { /* { observations: [{ source, session_id, raw_path, source_stamp }] } */ },
    async hydrate(observation) { /* a SourceEvidenceSnapshot */ },
  }] };
}
```

- **Scope.** A local source runs for `local` scope — which is the default — and
  for `all`, beside the built-in parsers. It never runs for `remote`, and a
  remote source still never runs for `local` or default scope. A request that
  registers no local source keeps the native-only path. `sourceConnectors: []`
  opts out of local sources the same way it opts out of remotes.
- **Roots.** Registration refuses a local source without a nonempty `roots`
  list of absolute paths. Every `raw_path` its discovery reports must resolve
  inside one of them; a discovery that names any other path is rejected whole
  and reported as that connector's diagnostic, so a plugin cannot point the
  catalog at files it did not declare.
- **Identity.** Sessions are keyed by an existing source (`supportedSources`
  is a `CatalogSource`) and are presented with `locations: ['local']`. The
  evidence goes through the same intake as a remote snapshot — the same
  validation, revision fence and per-connector provenance — so a session seen
  by both a local plugin and the built-in parser is still one session.
- **Hydration.** The built-in parser is asked first; the plugin then adds what
  it holds for the same identity. A session only the plugin observed hydrates
  from the plugin alone.

`sdk-ts/fixtures/local-source-plugin` is a complete, dependency-free example,
and `sdk-ts/src/local-source-plugins.test.ts` runs it end to end.

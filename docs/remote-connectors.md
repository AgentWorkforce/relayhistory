# Source plugins

Local provider files, storage and cached queries live in the local history
packages. Remote provider acquisition is optional and explicitly composed.
Neither commercial login nor installing a package changes local data sources.

| Optional package | Connector IDs | Evidence |
|---|---|---|
| `@relayhistory/provider-sources` | `claude-web`, `codex-cloud` | Claude teleport events; Codex supported task diff |
| `@relayhistory/capture` | `cloud` | Fresh readback of durably delivered normalized evidence |

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
never invokes remote callbacks. Cached search, recent, catalog and stats preserve
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
Partial evidence is reported as partial capability. For RelayHistory account,
stage, migration and durable readback semantics see the
[optional RelayHistory package](../plugins/relayhistory/sdk/README.md).

Generic destination plugins are separate from source plugins: a custom service
can receive history without providing remote discovery, or expose a source
without implementing delivery. See [delivery](history-delivery.md).

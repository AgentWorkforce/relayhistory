# Optional provider source plugins

`npm install ai-hist @relayhistory/provider-sources`

This optional package registers `claude-web` and `codex-cloud` source adapters.
It uses the provider CLI's existing sign-in only when an acquisition is selected.
Installation and registration do not probe credentials. Local operations remain
in the local SDK and do not invoke these adapters.

```json
{"plugins":[{"module":"@relayhistory/provider-sources","options":{"connectors":["claude-web","codex-cloud"],"instanceId":"personal"}}]}
```

```sh
ai-hist sessions discover --remote --config history.json --source-connector claude-web
ai-hist sessions hydrate claude SESSION_ID --remote --config history.json --source-connector claude-web
ai-hist sync --all --config history.json --source-connector codex-cloud
```

Programmatic callers use `createHistoryPlugin`, register it in a
`HistoryPluginRegistry`, and pass that registry as `plugins` to discovery,
hydration or sync. `sourceConnectors: []` disables remote acquisition. Remote
scope with no installed/selected source fails explicitly. Source identities
include an instance label; each observation keeps its opaque acquisition locator,
stamp, revision, evidence and checkpoint independently from other connectors.

The platform package supplies `history-provider-sources`. Development may set
`HISTORY_PROVIDER_SOURCES_BIN` or `binaryPath`. The helper is a bounded subprocess
protocol, not another native addon. Claude's private teleport contract can change;
Codex exposes a diff rather than a full transcript. Adapters report their covered
evidence kinds and the local engine retains partial capability honestly. See
`../rust/README.md` in the source repository for provider transport limits.

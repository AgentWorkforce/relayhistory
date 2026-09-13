# Optional cloud delivery

Install `ai-hist` for local history. Install and explicitly configure a source
or destination plugin to connect it to a cloud service. Neither login nor package
installation automatically adds sources or starts uploads.

For dependable delivery to RelayHistory, install `@agent-relay/relayhistory`,
create a plugin config and selection, then enable and run a durable job:

```sh
ai-hist plugin relayhistory-enable --config history.json -- --selection selection.json
ai-hist delivery run --config history.json
ai-hist delivery status
```

Follow [cloud setup](enable-cloud.md) for the complete login and legacy-scheduler
migration steps. The same generic coordinator supports another destination
plugin, and [NDJSON export](history-delivery.md#export-a-snapshot) can feed your
own program.

Existing `enableCloud()` and `pushCloud()` integrations move their imports from
`ai-hist` or `ai-hist/cloud` to `@agent-relay/relayhistory`. The optional
`relayhistory-plugin enable-cloud --once` command retains legacy push behavior;
it does not use the new durable delivery protocol.

[Source plugins](remote-connectors.md) explains optional remote discovery and
hydration. Cached local/remote/all reads remain available without cloud packages
or credentials.

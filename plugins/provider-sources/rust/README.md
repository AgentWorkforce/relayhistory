# Optional provider-native history sources

This standalone Cargo workspace installs `history-provider-sources`. It depends
on the public local-history engine/core; neither local package depends on it.
It contains the Claude web-session HTTP adapter and Codex cloud CLI adapter,
including their existing bounded pagination, redirect protection, output limits,
and provider-credential behavior. It never loads RelayHistory commercial auth.

Rust hosts explicitly call `remote::provider(home, connector_id, instance, limit)`
and register the returned `ShallowSessionProvider`. Construction does not read
credentials or use a transport. Supported connector IDs are `claude-web` and
`codex-cloud`; unknown IDs fail before any provider activity. Instances are
non-secret user configuration keys. Provider sign-in remains owned by the
Claude/Codex CLI. `RELAYHISTORY_CLAUDE_CREDENTIALS` and
`RELAYHISTORY_CLAUDE_API_BASE_URL` retain their original meanings.

The binary accepts one JSON stdin request and emits one JSON stdout response:

```json
{"version":1,"operation":"discover","args":{"connectorId":"codex-cloud","connectorInstance":"work","source":"codex","limit":100}}
```

Success is `{version:1,ok:true,value:{observations:[ShallowSession]}}`. `hydrate`
uses the same args plus `observation` containing the stored snake_case
`SessionObservation`. Its source, location, connector, instance, and availability
must match. Success returns the public normalized snapshot
`{source_stamp,source_bytes,covered_kinds,records}`. Claude transcript conversion
runs the existing parser in an isolated temporary database. Codex cloud diff
claims only `file_edit` coverage; it never invents transcript completeness.
Neither helper operation opens the caller's history database or persists a
checkpoint. The SDK's generic intake transaction owns applying observations and
evidence with the observation revision fence.

Request bound: 16 MiB. Response bound: 32 MiB. Helper watchdog: 300 seconds.
Per-request HTTP/CLI limits remain in the adapter; discovery `limit` cannot exceed
10000. Errors use the same `{version:1,ok:false,error:{code,message}}` envelope
and fixed safe messages, without returning provider output or credentials.
The optional SDK package locates this binary through
`HISTORY_PROVIDER_SOURCES_BIN` or its installed platform package.

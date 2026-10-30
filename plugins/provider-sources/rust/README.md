# Optional provider-native history sources

This standalone Cargo workspace installs `history-provider-sources`. It depends
on the public local-history engine/core; neither local package depends on it.
It contains the Claude web-session HTTP adapter, the Codex cloud CLI adapter, the
Devin cloud HTTP adapter and the Cursor Cloud Agents HTTP adapter, including their existing bounded pagination,
redirect protection, output limits, and provider-credential behavior. It never
loads RelayHistory commercial auth.

Rust hosts explicitly call `remote::provider(home, connector_id, instance, limit)`
and register the returned `ShallowSessionProvider`. Construction does not read
credentials or use a transport. Supported connector IDs are `claude-web`,
`codex-cloud`, `devin-cloud` and `cursor-cloud`; unknown IDs fail before any
provider activity.
Instances are non-secret user configuration keys. Provider sign-in remains owned
by the provider CLIs. `RELAYHISTORY_CLAUDE_CREDENTIALS` and
`RELAYHISTORY_CLAUDE_API_BASE_URL` retain their original meanings. Devin reads
`windsurf_api_key` from its `credentials.toml` and `devin.org_id` from its
`config.json`; `RELAYHISTORY_DEVIN_API_KEY`, `RELAYHISTORY_DEVIN_ORG_ID`,
`RELAYHISTORY_DEVIN_CREDENTIALS`, `RELAYHISTORY_DEVIN_CONFIG` and
`RELAYHISTORY_DEVIN_API_BASE_URL` override each input. Devin reads need
`ViewOrgSessions`; `send`'s `POST .../messages` additionally needs
`ManageOrgSessions`, and a denied send answers `PERMISSION_DENIED`.

`cursor-cloud` lists the API key owner's Cursor Cloud Agents through
`GET /v1/agents` (archived agents included) and hydrates one through the v0
`GET /v0/agents/{id}/conversation` endpoint — the only route that returns every
user prompt and assistant reply; v1 run streams expire. Conversation messages
carry no timestamps, so each turn takes its v1 run's `createdAt` (the prompt)
and `updatedAt` (the replies) from `GET /v1/agents/{id}/runs`. Evidence is
prompt history and user/assistant text only: no tool calls, thinking, usage or
file edits. Session ids are the agent's `bc-…` id, which never matches a local
Cursor transcript's UUID. The Cloud Agents API takes a Cursor user or service
account API key (Cursor Dashboard → API Keys), not the editor's sign-in:
`RELAYHISTORY_CURSOR_API_KEY`, else `CURSOR_API_KEY`, else the first line of
`RELAYHISTORY_CURSOR_API_KEY_FILE` (default
`$XDG_CONFIG_HOME/relayhistory/cursor-api-key`, `~/.config/...` when unset).
`RELAYHISTORY_CURSOR_API_BASE_URL` overrides `https://api.cursor.com`.

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

`send` posts one message into a remote session:

```json
{"version":1,"operation":"send","args":{"connectorId":"devin-cloud","sessionId":"<32-hex or devin-prefixed>","message":"<text>"}}
```

`message` must be non-empty and at most 64 KiB. Success is
`{version:1,ok:true,value:{sent:true}}`. Only connectors with an upstream
write path answer — today that is `devin-cloud`, posting
`POST /v3/organizations/{org}/sessions/{devin_id}/messages`; others refuse
as unsupported. Errors keep their classification in `error.code`
(`INVALID_ARGUMENT`, `CONNECTOR_NOT_CONFIGURED`, `AUTHENTICATION_EXPIRED`,
`PERMISSION_DENIED`, `SESSION_NOT_FOUND`, `CONNECTOR_FAILURE`) so a host can
tell a hard refusal from a retryable one; the message stays generic.

Request bound: 16 MiB. Response bound: 32 MiB. Helper watchdog: 300 seconds.
Per-request HTTP/CLI limits remain in the adapter; discovery `limit` cannot exceed
10000. Errors use the same `{version:1,ok:false,error:{code,message}}` envelope
and fixed safe messages, without returning provider output or credentials.
The optional SDK package locates this binary through
`HISTORY_PROVIDER_SOURCES_BIN` or its installed platform package.

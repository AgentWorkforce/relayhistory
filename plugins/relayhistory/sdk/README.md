# Optional RelayHistory plugin

Install alongside the local SDK: `npm install ai-hist @agent-relay/relayhistory`.
This package owns RelayHistory authentication, legacy sharing/replay, a durable
destination, and explicit readback/source adapters. It depends on the public
local SDK; installing or configuring it does not log in, read credentials, or
start uploads. `ai-hist` itself has no cloud package dependency.

The matching optional platform package supplies the Rust `relayhistory-plugin`
helper. Development builds may set `RELAYHISTORY_PLUGIN_BIN` or `binaryPath`.
No second native addon is needed. The helper preserves existing stage auth files,
refresh locks, error codes, and legacy cursors. Public error classes come from
`ai-hist`, so `instanceof` retains one identity.

```sh
relayhistory-plugin login --base-url https://history.agentrelay.com
relayhistory-plugin token --base-url https://history.agentrelay.com
```

These commands intentionally remain in the optional package. Replace imports
from `ai-hist/cloud` with `@agent-relay/relayhistory`; local Git hook/commit-link
functions stay in `ai-hist`.

Create an explicit plugin config, located beside the application's node_modules:

```json
{"plugins":[{"module":"@agent-relay/relayhistory","options":{"baseUrl":"https://history.agentrelay.com","instanceId":"personal"}}]}
```

Create a selection file before enabling delivery:

```json
{"all_sources":false,"sources":["claude","codex"],"sessions":[],"kinds":["history","session_event","tool_call","file_edit","session","presence","relationship","commit_link","trajectory","source_observation","observation_evidence"],"excluded_sessions":[]}
```

```sh
ai-hist plugin relayhistory-migration-status --config history.json
ai-hist plugin relayhistory-enable --config history.json -- --selection selection.json
ai-hist delivery run --config history.json
ai-hist delivery status
```

Enable creates a new explicit delivery generation. It never interprets a legacy
positional push cursor as an acknowledgment. Stop/remove old managed launchd or
cron push schedules first: active schedules block delivery and cannot be
acknowledged away. If the inspection is unavailable, explicitly set
`acknowledgeUninspectedLegacySchedules: true` after checking your own schedules;
this acknowledges an uninspected state, not a verified clear state. Arbitrary
user-created supervisors remain their owner's responsibility. Migration checks
are read-only and repeat before prepare/send. Background operation uses the same
`ai-hist delivery run` coordinator under your existing supervisor.

Jobs bind both the canonical service endpoint and the auth-derived organization/
workspace account. Changing endpoint requires a new instance/job; changing
account requires a new job. A swapped token cannot send an old account's batch.
The Rust queue persists the exact prepared bytes before sending. Uncertain
outcomes retry those bytes; the server accepts exact revision IDs durably in one
transaction and fences older revisions. No legacy `/ingest` fallback exists.
Durable acceptance does not promise search indexing. Inspect status for blocked
jobs, pending bytes and last acknowledgment; fix auth/config then retry explicitly.

`deliveryAccount({baseUrl})` resolves the account label. Pin that result as
`expectedAccount` in plugin options before cloud source acquisition. Remote
acquisition is selected with `--source-connector cloud --config history.json`.
The source adapter uses a fresh complete live readback traversal per acquisition;
cursors are not incremental checkpoints. Normalized evidence enters the local
engine through revision-fenced public source APIs, with independent connector
provenance and per-kind snapshots.

```ts
import { getDeliveredSession, getSessionThreadWithHistory } from '@agent-relay/relayhistory';
const evidence = await getDeliveredSession({source:'claude',sessionId:'session-id'}, options);
const thread = await getSessionThreadWithHistory({source:'claude',sessionId:'session-id'}, options);
```

`getDeliveredSession` retains returned evidence records and projects a chronological
transcript, preferring session events over prompt fallback. The explicitly
registered MCP `get_session_thread` includes those records plus legacy lifecycle
links using one pinned account; `legacyStatus` exposes unavailable legacy data.
`read_delivered_history` exposes the live listing. Neither tool is present in the
ordinary local MCP server. Set `AI_HIST_PLUGIN_CONFIG` to register configured tools.

The service applies its documented recursive credential scrubbing and omits opaque
whole-provider blobs. Tool args, patches and normalized evidence remain readable
within that policy; delivered data is not a byte-exact backup of raw secrets.
See the repository's `plans/009-delivery-protocol.md` for the wire contract.

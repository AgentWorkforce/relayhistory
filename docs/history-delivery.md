# Export and durable history delivery

History can stay local, be exported as NDJSON, or be delivered to an explicitly
enabled destination. Export and delivery share versioned Rust evidence records.
Delivery adds a durable queue, immutable mapped payloads, acknowledgments, retry
times, and fenced worker leases. Native contract 20 is required. Upload execution belongs to the probe package;
local history and exports work without it.

## Export a snapshot

Create an explicit selection file:

```json
{
  "all_sources": false,
  "sources": ["claude", "codex"],
  "sessions": [],
  "kinds": ["history", "session_event", "tool_call", "file_edit", "presence"],
  "excluded_sessions": []
}
```

Sources and individual session identities form a union. Select every source
with `all_sources: true`; evidence kinds must still be explicit. Incognito and
excluded sessions are filtered by core. Presence records carry observed location
provenance. Source/session identity, stable record/revision IDs, original fields,
timestamps, and raw evidence strings are preserved.

```bash
ai-hist export --selection selection.json > history.ndjson
ai-hist export --selection selection.json --out history.ndjson
```

Stdout contains one complete JSON record per line, with errors on stderr. File
output is replaced only after a complete export. A successful pipe means export
completed; it does not mean the downstream consumer accepted the history.

```ts
import { exportHistory } from 'ai-hist';
for await (const record of exportHistory(selection, { dbPath })) {
  await consume(record);
}
```

The iterator captures a bounded historical snapshot, not a live change feed.
It closes the snapshot when finished or stopped. For resumable paging, use
`beginHistoryExport`, `readHistoryExportPage`, and `closeHistoryExport`. Opaque
cursors can be replayed until the snapshot expires (one hour by default).
Snapshot retention uses the same bounded storage budget as delivery. A worker
expires abandoned snapshots during maintenance.

## Operate uploads through the probe

`agent-relay-probe` owns sharing selection and the background upload worker.
The `@relayhistory/capture` package exposes the same Rust engine through its
helper. Configure that package explicitly, then use:

```sh
ai-hist plugin relayhistory-enable --config history.json -- --selection selection.json
ai-hist plugin relayhistory-delivery --config history.json -- --action status
ai-hist plugin relayhistory-delivery --config history.json -- --action drain
ai-hist plugin relayhistory-delivery --config history.json -- --action pause --job JOB_ID
ai-hist plugin relayhistory-delivery --config history.json -- --action retry --job JOB_ID
```

The plugin SDK exports `createHistoryDelivery`, `historyDeliveryStatus`,
`controlHistoryDelivery`, `historyDeliveryRetention`, `compactHistoryDelivery`
and `setHistoryDeliveryRetention`. `drainProbeDelivery` runs a bounded drain
with an explicit `instanceId` and `expectedAccount`; its helper owns transport
and leases. Cancellation terminates the helper, leaving durable prepared work
and the lease recoverable. Run the probe for managed background collection.

The old `ai-hist delivery` commands, core SDK upload functions and core MCP
upload tools return `HISTORY_DELIVERY_MOVED`. They do not implicitly start a
probe or discard existing jobs. Generic JavaScript receiver execution through
the local native addon is no longer supported. Local export APIs are unchanged.
See the [plugin guide](../plugins/relayhistory/sdk/README.md) for configuration.

## Delivery guarantees and upgrade behavior

Existing jobs and queues remain in place. A transactional, repeatable migration
creates storage subscriptions without resetting generations, selected cutoffs,
prepared bodies, leases, receipts, retry deadlines or paused/blocked states.
Core-only writes retain subscribed evidence before the probe restarts. Upgrade
both packages together; older Rust writers are not a supported downgrade.

The probe persists the exact mapped body before dispatch. Uncertain outcomes
retry those bytes and IDs; acknowledgments must confirm every revision durably.
Consent, account and lease fences are checked before sending. Re-inclusion uses
a fresh cutoff, and relationships require both endpoints to be eligible.
An already transmitted request cannot be recalled. Cancellation discards local
pending work explicitly; it does not erase previously accepted remote records.

Capture journals a change only when a subscription can deliver it: a job
without session membership (its subscription names no session and is filtered
by its selection at prepare), or a session-job member for that identity, and
the identity is shareable. A relationship is journaled only when its parent
qualifies and its child is unlinked or qualifies too; a child included later
receives its incoming edges as fresh revisions. Excluded and unselected
sessions consume no journal retention. Whether an identity is shareable is one
SQL rule (`capture::shareable`): the capture triggers and the sharing status
query embed it, and prepare, claim, dispatch, file exports and the sharing
change check evaluate it through `capture::is_shareable`, so consent means the
same thing at every step. A session included later starts from a fresh
snapshot rather than from the journal. A session job subscribes through its
members only; it carries no subscription of its own.

Pausing preserves capture and queued work. Bounded maintenance expires exports,
compacts consumed changes and releases completed bodies. An idle session
subscription does not retain unrelated revisions. The evidence and upload queues
share a retention budget; a full budget fails capture visibly and rolls back
rather than silently dropping evidence. No parser or ingestion path uploads.

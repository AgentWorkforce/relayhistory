# Export and durable history delivery

History can stay local, be exported as NDJSON, or be delivered to an explicitly
enabled destination. Export and delivery share versioned Rust evidence records.
Delivery adds a durable queue, immutable mapped payloads, acknowledgments, retry
times, and fenced worker leases. Native contract 21 is required. Upload execution belongs to the probe package;
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
without session membership, or a session-job member for that identity, and the
identity is shareable. A revision of a relationship is journaled only when its
parent qualifies and its child is unlinked or qualifies too; a tombstone names
one identity and carries no payload, so it is journaled on that identity alone
and retracts a delivered edge whose child has since become ineligible. A child
included later receives its incoming edges as fresh revisions. Excluded and
unselected sessions consume no journal retention. Whether an identity is
shareable is one SQL rule (`capture::shareable`): the capture triggers and the
sharing status query embed it, and prepare, claim, dispatch, file exports and
the sharing change check evaluate it through `capture::is_shareable`, so
consent means the same thing at every step. A session included later starts
from a fresh snapshot rather than from the journal. A session job subscribes
through its members only; it carries no subscription of its own.

A job without session membership holds one subscription naming no session, so
it journals every shareable identity and its own source/session selection is
applied at prepare. For the probe's sharing modes this is exact: `all` selects
every source, and `new` and `selected` are session jobs. A legacy job carrying
an explicit source or session list therefore still journals identities it will
never send, until it is adopted as a session job.

Pausing preserves capture and queued work. Bounded maintenance expires exports,
compacts consumed changes and releases completed bodies. An idle session
subscription does not retain unrelated revisions. The evidence and upload queues
share a retention budget. No evidence is silently dropped and no parser or
ingestion path uploads.

## Journal compaction

The journal keeps a captured revision until every subscription that reads its
session has consumed it. Compaction reclaims consumed rows only, so un-uploaded
backlog is never deleted:

- Every row at or below the consumed floor is reclaimed by an indexed range
  delete, in transactions of at most 10,000 rows. The floor is the lowest
  cursor of any subscription that still pins something: one reading every
  session always does; one reading a single session only while that session
  has a row past its cursor, so a finished session's cursor does not hold the
  floor down. The work is proportional to the rows reclaimed, not to the
  journal's length, and a consumed row never waits on a sweep cursor.
- Above the floor, where a lagging session subscription pins its own rows
  among other sessions' reclaimable ones, a persistent sweep cursor examines
  one bounded page per drain with the exact per-session predicate. The sweep
  reaches only as far as the lowest cursor of a subscription reading every
  session, since everything past it is retained; the cursor never sits below
  the floor and wraps back to it whenever a page is short.
- A drain starts by reclaiming the consumed floor in full, so its wall-clock
  budget goes to delivery. It ends, once its acknowledgments have released
  their bodies, by running complete passes (floor plus a sweep from the floor
  to that ceiling) while retained bytes are at or above three quarters of the
  cap and the last pass reclaimed a full page; a pass that reclaimed less has
  caught up with whatever other consumers freed meanwhile.
- If the cap refuses a batch write during a drain, the worker runs one
  complete pass, recovers to the low-water mark and retries the write once.
  The drain reports `DELIVERY_RETENTION_LIMIT` only when nothing was
  reclaimable or the retry is refused again; no cursor moves on a refused
  write.

## Batch materialization reserve

A batch is the deliverable form of journal rows the cap already holds, and the
only way a journal full of unconsumed backlog ever drains. Batch rows are
therefore checked against the cap plus a reserve that is bounded by design:
every non-cancelled job holds at most one unresolved batch of at most its
configured `max_batch_bytes` plus `max_prepared_bytes` (plus 512 bytes of row
accounting), and settled receipts keep their 512 bytes until compaction
releases them. The journal, bootstrap preimages and export pages are checked
against the plain cap, and the low-water mark is three quarters of the plain
cap. `historyDeliveryRetention` therefore reports `usedBytes` above
`limitBytes` by at most the reserve while batches are in flight; the cap
itself never moves, and `setHistoryDeliveryRetention` accepts any value down
to the bytes retained outside batches.

While a pending batch keeps retained bytes above the cap, capture stays
refused for the life of that batch: the drain acknowledges it, which releases
its bodies, and its end-of-drain compaction then frees the rows it carried. A
journal of unconsumed rows at the cap keeps stopping capture until that
happens, the job is cancelled, or the cap is raised with
`setHistoryDeliveryRetention`. Backlog a destination cannot take stays in its
batch, unacknowledged, until it can. Hosts run the same complete compaction
pass on demand through `compactHistoryDelivery` (the `compact_journal_pass`
delivery request).

## Capture backpressure

Capture applies backpressure against the shared budget instead of discovering
the cap inside a session transaction. Before each source pass and each session
transaction it reads the retained bytes; above 90% of the cap it runs the
low-water recovery above, then reads them again. If the budget is still above
90% the pass stops with a typed `retention_limit` failure carrying `used_bytes`
and `limit_bytes` instead of attempting the remaining sessions. A write the
capture trigger refuses inside the pass ends it the same way, with the usage
attached. What that refusal leaves behind is the provider's write granularity:
targeted hydration, OpenCode, Grok and the Claude history log write a session
(or a log chunk) in one transaction, so the refused session rolls back; Codex
rollouts, the Claude transcript walk and trajectories commit statement by
statement, so a refused rollout keeps the rows it wrote before the refusal and
the next pass rewrites them idempotently, because no cursor or stamp is
recorded for it. Sessions committed earlier in the pass stay persisted. The
carried `used_bytes` is the retained total, so while batches are in flight it
can exceed `limit_bytes` by at most the materialization reserve.

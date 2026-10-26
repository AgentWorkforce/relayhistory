# Local history retention

RelayHistory already writes and reads session evidence in its local SQLite
store (`ai-history.db` by default; an embedder may name it `history.db`). These
reads do not contact a network service. Retention adds explicit maintenance;
cleanup does not run during session capture, catalog listing, sync or hydration.
Evidence writes still maintain the additional indexes described below.

## Policy and desktop integration

The shared setting is `store_settings['history.retention_days']` in that store.
An absent key means **30 days**. Positive integer values are days since last
recorded session activity; `0` means forever. The desktop presets 7, 30, 90 and
forever map directly to this interface; other positive `u32` values also work.
The SQLite value is shared by all clients. A desktop JSON setting may be
migrated once, but startup should read the shared value rather than overwrite
a CLI or another client's newer choice with a cached default.
Changing the setting resets the maintenance scan. Increasing retention cannot
restore logs already evicted. Sessions with no known activity time are kept.

```rust,no_run
use ai_hist::retention::{RetentionPolicy, RetentionOptions, write_retention_policy};
use ai_hist::{SessionStore, StoreOptions};
use std::path::Path;

let path = Path::new("/app-data/history.db");
// HistoryRetention::apply runs this on its worker, not the UI/read thread.
write_retention_policy(path, RetentionPolicy::from_days(30))?;
let store = SessionStore::open(StoreOptions::default().db_path(path))?;
// A maintenance worker calls this at startup, periodically and after an
// upload acknowledgement or policy change. Yield between calls; stop a
// burst on busy/deferred/awaiting_upload_ack/cycle_complete and schedule it later.
let progress = store.prune_expired(RetentionOptions::default())?;
# Ok::<(), ai_hist::Error>(())
```

`read_retention_policy(&Path)` and `SessionStore::retention_policy()` read the
same value locally. `SessionStore::set_retention_policy(policy)` writes it.
A policy write only saves the policy: it starts no thread and performs no
pruning. The host must wire its maintenance worker as well as its settings UI.
No release/tag is implied by the presence of this API on a feature branch.

## Upload protection

The delivery coordinator registers **every required upload destination** with
`SessionStore::register_retention_consumer(name)` before capture/delivery.
Registration is idempotent and preserves an existing acknowledgement. Each
new consumer begins unacknowledged, so a newly added destination protects the
existing backlog too. No consumers registered means **keep all logs**.

After durable acceptance, call
`SessionStore::acknowledge_retention(name, Watermark { epoch, revision })`.
The position must cover **all kinds and all sessions through that revision**;
filtered, per-session, per-job or merely read cursors are not safe floors.
For desktop's existing per-job `delivery_jobs.acknowledged_cursor`, the
coordinator must publish a conservative contiguous floor across every required
job, batch, session membership and exclusion. Taking the maximum job cursor
would silently discard unuploaded data. Do not read probe-owned tables from
RelayHistory. A destination with gaps or not-yet-accepted batches must hold its
floor back. The acknowledgement must use this store's epoch, not a different
history database's revision. Unknown consumers and ahead/foreign watermarks
are rejected; acknowledgements within an epoch move monotonically.

Pruning uses the minimum acknowledged position across all registered
consumers. It rechecks under the writer transaction and keeps an entire session
if any of its evidence, catalog, presence, observation, commit-link or
relationship revisions is above that floor. A new write after acknowledgement
therefore protects that session again. Consumers never expire automatically.
Removing a permanently retired destination requires an explicit
`remove_retention_consumer(name)` call; doing that relinquishes its protection.

**Tradeoff:** an offline destination can keep local logs longer than the saved
retention limit. `RetentionReport` exposes `awaiting_upload_ack` and
`protected_sessions` so the host can explain this. Protection can be relinquished
with explicit `RetentionOptions { allow_unuploaded: true, .. }`, or by retiring
an unacknowledged destination while the remaining consumers have acknowledged
the backlog. In either case, that destination may never receive the evicted logs.
The host must require an explicit retirement decision before removing a
consumer. Selecting a shorter retention limit must not enable the override or
retire a destination.

## Maintenance and cloud copies

Each invocation plans at most 32 candidate session identities on its own
read-only connection, leaving the foreground connection pool warm,
then revalidates and processes at most one expired session on a writer. It
removes at most `max_rows` payload/auxiliary rows (default 128, allowed 1–1024),
with individual delete statements limited to 32 rows. Large sessions and
connector metadata span transactions; final catalog and at most two presence
rows are removed when the session is drained. A persistent pending record
retains the activity timestamp even after its last timestamped event was
removed, and a persistent sweep cursor resumes across process restarts. A
changed policy or new activity is checked again before further deletion.
Anonymous prompt-log rows use their own timestamps and acknowledged revisions.
If anonymous cleanup awaits upload, the session sweep resets so other sessions
can still expire as they age or their acknowledgements advance. Full acknowledged
anonymous batches continue draining without restarting the session scan.

Batches have a persisted 100 ms cooldown by default. `deferred` means the host
should schedule its next invocation after that delay. This prevents a tight
maintenance loop from repeatedly winning SQLite's writer lock. Tests and
specialized offline tooling may set `min_interval_ms = 0`; interactive hosts
should keep the default. Do not spin on a deferred/busy report.

Writable connections retry short lock overlaps after 100, 200, 400, 800,
1000 and 1000 microseconds (with bounded jitter), then use the existing
10–500 ms exponential sequence and approximately 30-second grace window.
This avoids imposing a 10 ms minimum wait on short maintenance collisions.

The schema and trigger-heavy delete statements are compiled before acquiring
either writer lock, then reused inside the bounded transaction.

Maintenance tries the existing sync lock and SQLite writer lock once and
returns `busy` when capture or a writer owns them. It never waits for a writer,
opens no provider files and contacts no network service. WAL readers continue
against their committed snapshot while maintenance writes. It never runs
`VACUUM` or a blocking checkpoint; freed pages are available for later writes.
A row budget bounds work, not milliseconds: payload sizes and storage latency
vary. W0 owns the desktop end-to-end 5 ms gate; this change supplies focused
local measurements and the held-writer read-concurrency test.

As with `forget_evidence`, eviction deletes its own change-feed tombstones
**in the same transaction**, before any uploader can see them. Earlier
ordinary deletion tombstones are preserved. Local retention therefore does
not send cloud deletion records or retract uploaded evidence. User-created
tags/commit links and transcript byte cursors survive. Provider originals are
untouched; an explicit hydration or a later provider rewrite can capture them
again, and subsequent maintenance reapplies the policy. Retention is local
cache eviction, not secure erasure or deletion of provider/cloud history.

The schema adds `store_settings`, `retention_consumers`, `retention_sweep` and
`retention_pending`. Ten covering `(source, session identity, revision)` indexes
bound upload validation, including both relationship endpoints. Two history
indexes bound latest-activity and anonymous-prompt selection. Existing timestamp
and tombstone indexes support the other validation seeks; no triggers are added.
Writable open builds missing indexes after change-feed migration; read-only open
does not require or build them. Initial migration takes storage and indexing work,
and subsequent capture maintains these indexes. Validation never scans all payload
rows of a fully acknowledged session while holding the writer lock.

Relationship cleanup reopens the affected continuity evidence by primary key
before removing each edge. This preserves reconciliation of surviving sessions
when an expired origin is later captured again, with at most one evidence update
per edge in the bounded batch.
Partial batches can leave a session with some logs already evicted until its
next batch; clients should not treat an expiring session as an archival copy.

Focused before/after timings and their scope are recorded in
[the W1 measurement report](retention-benchmarks.md).

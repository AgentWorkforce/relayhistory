# Local retention is upload-gated cache eviction

Status: proposed (W1 implementation under review)
Date: 2026-10-25

## Context

Session evidence already resides in the configured local SQLite store. Desktop
needs a saved keep-history limit without adding network waits to local reads,
blocking capture behind bulk cleanup, losing undelivered logs, or retracting
cloud copies. Desktop currently owns per-job delivery acknowledgements and
translates ordinary change-feed deletes into cloud tombstones.

## Decision

Keep the existing store path and writer implementation. Store the shared policy
under `history.retention_days` in `store_settings`: absent means 30 days,
positive integers mean days since last recorded session activity, and zero
means forever. Expose policy read/write and explicit maintenance through the
Rust SDK. The SQLite value is authoritative across clients.

The host schedules `prune_expired` on its maintenance worker. Each call plans
at most 32 identities on a separate read-only connection, rechecks one expired session under an
immediate transaction, and removes bounded chunks. Persist pending activity and
the scan cursor so large sessions resume across batches/restarts. Default
batches have a 100 ms cooldown; compile trigger-heavy statements before taking
the writer locks. Short busy retries precede the existing long-contention grace
window so capture can recover from brief overlap without a 10 ms retry floor.

Uploaders register named retention consumers and publish an all-kinds,
contiguous acknowledged feed watermark after durable acceptance. The minimum
across every required destination protects the backlog. An absent or stale
acknowledgement keeps logs. RelayHistory does not read probe-owned delivery
job tables. The coordinator must derive a conservative floor from its jobs,
accepted batches, memberships and explicit exclusions.

Remove only the pruning transaction's own feed tombstones before committing,
as existing local evidence eviction does. Preserve earlier ordinary tombstones.
Provider originals, user-created tags/commit links and cloud copies remain.
Unknown-age or active sessions remain. By default, logs are protected until all
registered destinations acknowledge them. A caller can relinquish protection by
explicitly enabling `allow_unuploaded`, or by explicitly retiring an
unacknowledged destination while the remaining consumers have acknowledged the
backlog. That retired destination may never receive the evicted logs. Selecting
a shorter retention limit must not enable the override or retire a destination.

Covering revision indexes bound the upload recheck for each session. Reuse
existing timestamp indexes with explicit non-null seeks, add history activity
indexes, and suppress new tombstones through the existing per-kind revision
index. Reopen continuity evidence before removing its relationship, bounded by
the same edge batch. Row limits alone do not bound scans of acknowledged rows.

## Alternatives rejected

- Timestamp-only deletes: they can discard pending uploads.
- Feed ordinary deletes to uploaders: cloud copies would be retracted too.
- Prune during capture or reads: bulk work would enter the visibility path.
- Desktop JSON as the shared authority: other store clients could disagree.
- Read desktop delivery tables directly: that would couple the crate to one
  consumer's private schema and job semantics.

## Consequences

Twelve additional evidence indexes consume disk and add capture write work;
writable schema migration builds them once. The bounded writer-lock queries
avoid repeatedly scanning large acknowledged sessions across cleanup batches.

Offline destinations may retain data past the configured limit. Reports expose
that condition rather than silently choosing data loss. Retired destinations
must be removed explicitly; they never time out automatically.

Desktop integration needs both the policy adapter and a paced maintenance
worker, plus acknowledgement publication by its delivery coordinator. A saved
setting alone does not enforce retention. Expiring sessions can be partially
evicted between batches. Freed SQLite pages are reused; maintenance does not
vacuum or promise secure erasure. Explicit hydration or provider rewrites can
recapture originals, after which maintenance applies the policy again.

See [the integration contract](../retention.md) for signatures, defaults,
reports and scheduling. W0 owns the end-to-end desktop visibility gate; W1's
measurement harness covers the local SQLite write/list path.

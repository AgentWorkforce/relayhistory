# Throttle unchanged probe heartbeats across collector cycles

Status: implemented locally
Date: 2026-09-20

## Context

The collector recreates progress monitors for each capture and delivery cycle.
Every monitor previously sent an opening, periodic, and closing report even when
no work changed. Each request also incurred authentication and presence DB work.

## Decision

Keep a process-local heartbeat schedule per destination directory and History URL.
Compare acknowledged progress across monitor lifetimes, ignoring per-source scan
counters when comparing delivery snapshots. Report changed progress at most every
three seconds, terminal transitions immediately, and unchanged progress every
270–330 seconds. Suppress the opening snapshot of repeat scans until they have
run for three seconds. Failed requests retain the last acknowledged state and
retry after at least 30 seconds. Explicit setup/one-shot final confirmation bypasses
throttling and still waits for receipt within the existing bounded deadline.

## Alternatives and consequences

Increasing the monitor timer alone leaves opening and closing reports on every
empty cycle. Server-side throttling alone still pays for network requests and auth.
A shared local schedule eliminates those requests without a new service or schema.
The schedule resets on process restart, intentionally sending fresh presence.
Presence expires after ten minutes; the UI distinguishes unchanged upload counters
from a disconnected collector. Auth continues checking revocation and expiry on
every request while coalescing approximate last-used writes to five minutes.

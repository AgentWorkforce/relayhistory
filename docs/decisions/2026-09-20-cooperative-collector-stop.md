# Cooperative collector shutdown

Status: accepted
Date: 2026-09-20

## Context

The probe checked its generation-scoped `stop.json` only between complete
capture/delivery cycles. A large local history could keep the collector lock
held beyond the stop command's 45-second deadline. Delivery checked a separate
signal flag, so a normal stop could also drain more queued batches.

## Decision

A collector-owned watcher polls the stop file every 100 ms and latches an
atomic cancellation flag. Capture and delivery consult that flag, as well as
the process signal flag. Stale stop files for other startup IDs remain inert.
The collector releases its lock only after the active operation unwinds and
writes `ready: false`; cancellation does not record an offline cycle.

Core exposes `sync_local_at_cancellable`, with a thread-scoped callback restored
on return, error or panic. Existing capture APIs retain their behavior. Checks
at provider, file, record and catalog-window boundaries abort with the typed
`CaptureCancelled` error. Committed chunks and checkpoints survive; unfinished
transactions roll back, and incomplete transcripts are reread on the next run.

## Alternatives and consequences

An optimistic UI alone hides the wait without stopping capture. Killing the
process discards orderly cleanup. Polling the filesystem per record adds I/O
to the hot path. The watcher plus cooperative checks avoids these tradeoffs.

This is not a hard real-time deadline: individual filesystem/SQLite operations,
OpenCode snapshots and an already-running HTTP request still finish or time
out. Stop prevents continuing through the remaining history and starting more
upload batches. The process regression stops during a 200,000-record capture
and verifies prompt exit, lock release, checkpoint persistence and DB integrity.

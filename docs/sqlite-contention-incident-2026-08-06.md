# SQLite contention incident verdict (2026-08-06)

## Verdict

SQLite contention did not cause the reported `2026-08-06T13:00:19.000Z`
Agent Relay fleet freeze. That timestamp was not an observed process-stop time:
Relaycast applied one `inventory.sync` reconciliation time to 25 `lastSeen`
fields after a transport reconnect. No OS PIDs or process-group states were
captured for that cohort. Follow-up message probes advanced five of five sampled
Finn lanes and the original incident lane, disproving the claimed cohort death.

The historical stopped brokers from relayhistory issue #50 are a separate
problem. An external stop can freeze a process while one of its threads owns a
SQLite transaction, making the write lock permanent until the process resumes.
SQLite cannot deliver a stop signal, so the supported causal direction is:

```text
external process stop -> possibly frozen SQLite transaction
```

It is not:

```text
SQLite contention -> stopped broker or agent fleet
```

## Evidence

- Agent Relay calls `ai-hist-native` from its Reflex capture timer every five
  minutes. The N-API binding runs `sync_and_push` with
  `napi::tokio::task::spawn_blocking`, off the Node event loop. A busy SQLite
  call can delay or fail that worker; it cannot block inbox consumption on the
  event loop.
- `sync_and_push` opens the database for one capture call. A broker listed by
  `lsof` or `ai-hist doctor` therefore has the file open only while that call is
  active. An open file is not proof that the process owns SQLite's write lock.
- The measured 2026-08-06 snapshot reported a broker with the file open while a
  fresh `BEGIN IMMEDIATE; ROLLBACK` succeeded, the WAL was empty, and the write
  lock was available. That broker was not the write-lock owner.
- macOS logs show broker PID 23630 completing TLS traffic across
  `13:00:18-13:00:19Z`, consistent with the Relaycast reconnect/inventory
  explanation. No stop-signal evidence was found for that process.
- Relay's source tree has no `SIGSTOP` or `SIGTSTP` sender for brokers. SQLite
  lock acquisition uses file locking and busy-handler waits; failure returns
  `SQLITE_BUSY`/`SQLITE_LOCKED`, not a process-stop signal.
- Historical reports identified PIDs 13214, 84867, 85652, and 88796 as
  UID-501, PPID-1 processes in `T`/`Ts` state and later described them as
  “root-custodied.” A contemporaneous check reported zero sync writers. This
  establishes stopped processes, but not who originally delivered the signal.

## What remains unknown

The original sender and reason for the Jul 30/Aug 1 stop signals cannot be
recovered from the retained evidence. A `T`/`Ts` state is consistent with a
job-control or explicit stop signal; macOS App Nap or ordinary SQLite waiting
does not explain that state. Because stop signals were not audited when those
processes changed state, distinguishing an operator/supervisor action from an
earlier terminal job-control action would be guesswork.

The brokers did not intentionally hold a write transaction while idle. They
held a database connection for the duration of a periodic in-process capture;
one process happened to stop during a write. JSONL ingestion transactions are
bounded to 2,000 lines, but any externally stopped process remains frozen
regardless of the intended transaction duration.

## Supported remediation

- Retry transient `SQLITE_BUSY`/`SQLITE_LOCKED` conflicts with bounded
  exponential backoff and jitter (#49).
- Continue syncing independent sources after one source fails, preserving
  successful cursors and reporting partial failure (#48).
- Remove abandoned uniquely named sync-state temp files whose writer is gone or
  whose write is at least one day old (#51).
- On contention, automatically run a fresh write-capability probe and report
  stopped holders and WAL growth without treating file-open status as lock
  ownership (#52).

These changes reduce the database impact of transient overlap and make a
genuine frozen transaction actionable. They do not address Relay fleet
liveness because the investigated fleet event was an inventory timestamp, not
a SQLite-induced stop.

## Deferred architecture: issue #47

The proposed spool migration should not be folded into the incident fix. The
current producer model differs from issue #47's premise:

- Agent Relay does not append individual history events to SQLite; it invokes
  the complete `sync_and_push` aggregator.
- The MCP SDK's tag writes use a sql.js in-memory snapshot followed by a
  whole-file export, rather than participating as a normal WAL writer. That is
  independently unsafe to run beside the Rust writer, but an event spool needs
  a tag-operation schema and acknowledgement semantics.

A correct single-writer design therefore needs a cross-repository ADR covering
the spool schema, atomic append rules, per-producer cursors, malformed-record
quarantine, acknowledgement/error behavior, automatic service availability,
cloud-push ownership, and migration of Relay's “works without a separate
service” behavior. Implementing only the consumer or silently disabling the
embedded sync would regress automatic capture without proving anything about
the historical stop. Issue #47 remains open for that explicit architecture
decision.

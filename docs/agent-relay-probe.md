# Native Agent Relay Probe

`agent-relay-probe` is the native collector for Cloud's new Teams onboarding flow.
It signs a computer in, obtains workspace-scoped RelayHistory credentials, captures
local coding sessions and runs durable delivery in the background. It needs no
Node.js, npm, NAPI addon or separately installed Agent Relay CLI.

## Source ownership

Keep the binary in `plugins/relayhistory/rust/src/probe/` in this repository. It
links the existing `ai-hist` evidence engine and owns its upload queue, worker,
sharing policy and transport within `relayhistory-plugin`. Moving the probe to
relay-desktop is a separate follow-up; provider acquisition remains here.
The core local SDK remains independent of the Cloud package.

This binary is separate from `relayhistory-plugin`, whose JSON bridge, package
name and Cargo default executable remain unchanged. The probe's `--version`
output (and the `cli_version` it reports) is the crate's `CARGO_PKG_VERSION`,
which the release workflow stamps with the release version before building, so
a released binary names the release it is attached to. A locally built binary
reports whatever the checked-in crate version is. Agent Relay Cloud
sign-in is not separate: the probe calls the plugin library's single
implementation in `cloud.rs` — the same origin rules, credential reuse and
device flow that back `relayhistory-plugin login` and the SDK helper's
`cloudLogin`.

## Build and run

```sh
cargo build --manifest-path plugins/relayhistory/rust/Cargo.toml --release --bin agent-relay-probe --locked
plugins/relayhistory/rust/target/release/agent-relay-probe --help
```

For local Cloud development, run in your own terminal:

```sh
./agent-relay-probe cloud install --site-url http://127.0.0.1:3100 --workspace WORKSPACE_ID --account USER_ID
```

The account argument asserts the expected signed-in user; it does not grant
access. The workspace bridge authorizes membership and issues the scoped session.
Without a workspace argument, the probe uses Cloud's current workspace.

Setup prints the existing Cloud device-approval URL, waits for browser approval,
then asks whether to share existing and future sessions or only new sessions.
`--include-existing` and `--new-sessions-only` make that choice explicit in a
noninteractive terminal. `--force-login` bypasses eligible existing CLI sign-in.
The shared implementation reuses an unexpired official CLI credential only for
the exact selected Cloud URL, and never writes or refreshes that CLI's auth
file; `CLOUD_API_ACCESS_TOKEN` overrides both. Setup always prints the approval
URL to stdout rather than requiring a terminal on stdin, because the composed
local harness drives it through pipes.

New-only mode snapshots existing identities across the catalog, prompts and
events before creating its delivery generation. It sends session metadata and
session events, omitting the separate prompt-only records because some lack a
session identity. The baseline is stored as durable delivery exclusions, which
the coordinator rechecks whenever a batch is prepared, claimed and dispatched,
so it does not grow the delivery configuration with local history. A setup
abandoned between that snapshot and its delivery generation leaves the baseline
behind; a later setup that chooses to share existing sessions withdraws it, so
the choice that takes effect is always the one just made. Sessions
discovered later are treated as new; this is a local capture baseline, not a
guarantee about the actual creation time of files added later. The saved sharing
choice is immutable on reconnect.

Default setup starts a detached process. `--foreground` keeps it in the terminal;
`--once` captures and delivers one bounded cycle. To inspect or stop it:

```sh
~/.local/bin/agent-relay-probe status --site-url http://127.0.0.1:3100 --workspace WORKSPACE_ID --account USER_ID
~/.local/bin/agent-relay-probe stop --site-url http://127.0.0.1:3100 --workspace WORKSPACE_ID --account USER_ID
```

The executable must already be at that location to use these example paths.
There is no launchd/systemd installation or automatic restart after reboot yet.
Run setup again after restarting the machine. It preserves the existing queue.

## State and delivery

Selected-session delivery runs before targeted hydration and independently of
unrelated provider discovery. An old selected queue at its retention cap can
reclaim consumed journal entries and retry its atomic membership migration.
This preserves the cap, queued batches, snapshot preimages, and other jobs'
unread revisions. If nothing can safely be reclaimed, migration stays pending.

The desktop status `last_cycle` includes an optional, allowlisted `error_class`
and a safe message for local retention, database corruption, disk-space,
contention, and permission failures. These also cover failures before capture
starts. No raw SQLite/provider error, transcript, credential, or path is
included. Capture and delivery are reported separately in `last_cycle.capture`
and `last_cycle.delivery`, each with its own `ok`, `error_class` and message: a
pass that only delivered reports its own delivery outcome and leaves the last
capture verdict standing, so a condition the capture cycle measured stays
visible between those cycles. The top-level `ok`, `error_class` and `message`
are the effective verdict — the capture fault when there is one, otherwise the
delivery fault — and messages are rendered at report time, so a retention
sentence always carries the current usage. A compaction that reports while a
pass is running has re-measured the cap that pass observed, so the pass does
not report the older reading over it. Database corruption requires separate recovery from a preserved copy;
the collector does not delete or recreate a damaged queue automatically.

Each `(site origin, user, workspace)` has an independent SHA-256-named directory
under `~/.agentworkforce/probe/`. It contains an SDK-owned `history.db`, persisted
selection/job metadata, scoped RelayHistory auth, a log, a runtime record and a
collector lock. Directories are mode 0700; credentials/config/logs are mode 0600.
No Cloud bearer credential is persisted by the probe. Tokens are never command
arguments, and provider errors, response bodies and session content are not logged.
The device approval URL is intentionally displayed in the interactive terminal.

Each pass delivers through the probe-owned Rust worker. A pass is bounded by
wall time (15 s), so it moves as many batches as the destination accepts in
that window and the next pass continues the backlog; the collector runs passes
2 s apart while the active job has queued, unqueued or unscanned records it can
attempt now, and 20 s apart once it is caught up, paused, or waiting out a
retry deadline. Stop requests are polled between batches. The plugin helper
uses that same bounded drain and RelayHistory receiver. The worker owns
immutable batches, leases and their keepalive, prepared-byte persistence, the
eligibility recheck immediately before dispatch, retry/backoff, acknowledgment
and compaction; the receiver owns only the consent and destination guards and
the transport, so progress advances only on a validated receipt. Mapping/account
mismatches block delivery. A failed attempt retains unacknowledged data and the
job waits out its own backoff, which is normal operation rather than a fault.
Transport credentials refresh through the existing RelayHistory auth
implementation.

Each new probe version retries a previously blocked job once, before setup
delivery or background collection begins. Later starts of that version preserve
its blocked verdicts. SQLite retains every `(job, build)` retry record and commits
it atomically with the retry, so detached collectors, restarts and an A→B→A
version rollback cannot grant a second attempt. The former `retry_build` column
is imported into this history and maintained for older probes. File markers are
imported only when `verdict-retry.json` explicitly names the same job; unscoped
markers cannot safely identify a generation and are ignored. New jobs start
their own retry history. Failure to save the compatibility file does not stop
collection. If the database transaction fails, automatic recovery is deferred
without committing either the retry or its build record. Paused and cancelled
jobs remain unchanged.

Local capture progress is written to private `progress.json` and emitted as
`event: "progress"` on the desktop install JSON stream every three seconds.
Source names, files processed/total, and sessions captured never leave the Mac.
Cloud heartbeats contain only delivery progress for permitted records; the legacy
capture fields are empty/zero for wire compatibility, and capture-only monitors
never send heartbeats. Changed delivery progress reports at most every three
seconds. Completion and failure transitions report immediately. Unchanged delivery
progress uses a jittered 270–330 second presence heartbeat; empty background
cycles do not produce extra requests. Failed reports retry no faster than every 30 seconds,
except for a new terminal transition or explicit setup/one-shot confirmation.
Cloud determines whether session data has actually arrived. A running process
alone does not mark the dashboard's data step complete.

An OS file lock permits one collector per destination directory. Stop requests
carry the startup identity, so an old stop file cannot stop a new run. There is
no signaling of arbitrary stored PIDs; startup timeout terminates only the child
process launched by that setup invocation.

Setup and every dispatch check legacy managed launchd/cron uploaders without
changing them: the receiver rechecks them before mapping a batch and again
immediately before sending it. An active old uploader blocks setup. An
unavailable inspection requires the explicit `--acknowledge-uninspected-schedules`
flag after the user has checked their older schedules. Arbitrarily named/manual uploaders are not discoverable by this check.

## Companion changes and distribution

This PR supplies the binary, not a hosted release. It depends on:

- **Cloud:** device start/token/approval, `auth/whoami`, and the authenticated
  workspace `relayhistory/session` bridge. The bridge returns an RTH sync session.
- **RelayHistory service:** existing durable `/v1/delivery/batches` and the new
  `/v1/onboarding/heartbeat` endpoint. The dashboard also uses the companion
  onboarding-status route. These server changes are outside this repository.
- **agentrelay.com:** site-root `/install.sh` and platform downloads. The installer
  should select the OS/architecture, verify SHA-256, install atomically to
  `~/.local/bin/agent-relay-probe`, and invoke `cloud install` with the dashboard's
  site/account/workspace. A checksum from the same origin detects corruption;
  it is not a separate publisher signature.
- **Official Relay CLI:** an optional `agent-relay cloud install` alias may delegate
  to the installed probe. The native onboarding path does not depend on it.

### Release assets and download names

The `Publish RelayHistory release` workflow (see
[releasing.md](releasing.md)) builds the probe from this crate on four
platforms, applies the same glibc 2.28 floor as the helpers, and attaches the
binaries to the `sdk-ts-v<version>` GitHub Release. The crate is stamped with
that release version before it is compiled, and the workflow asserts that the
built binary prints `agent-relay-probe <version>`, so `--version` matches the
Release the asset hangs off. A `skip_core` re-run rebuilds from the
`sdk-ts-v<version>` tag, so re-attached assets stay the code that release
shipped. Those assets are the source
of truth; agentrelay.com mirrors them under the paths the installer requests.

| Release asset (`sdk-ts-v<version>`)                                  | Site path                                                 |
| -------------------------------------------------------------------- | --------------------------------------------------------- |
| `agent-relay-probe-darwin-arm64` + `agent-relay-probe-darwin-arm64.sha256` | `/downloads/agent-relay-probe/darwin-arm64/agent-relay-probe{,.sha256}` |
| `agent-relay-probe-darwin-x64` + `.sha256`                           | `/downloads/agent-relay-probe/darwin-x64/agent-relay-probe{,.sha256}`   |
| `agent-relay-probe-linux-x64-gnu` + `.sha256`                        | `/downloads/agent-relay-probe/linux-x64/agent-relay-probe{,.sha256}`    |
| `agent-relay-probe-linux-arm64-gnu` + `.sha256`                      | `/downloads/agent-relay-probe/linux-arm64/agent-relay-probe{,.sha256}`  |

The asset names carry the build platform id used across this repository, so the
Linux assets keep their `-gnu` suffix while the installer's paths do not; only
glibc Linux is published. The `.sha256` files are `sha256sum` lines naming
`agent-relay-probe`, so a mirrored file verifies with `sha256sum -c` next to the
binary. A checksum from the same origin detects corruption; it is not a separate
publisher signature. macOS signing/notarization and versioned release manifests
are still outstanding release work, and Windows is not supported by this
installer. The CI job builds and tests on macOS/Linux but publishes nothing.

## Verification

```sh
cargo test --manifest-path plugins/relayhistory/rust/Cargo.toml --bin agent-relay-probe --locked
cargo test --manifest-path plugins/relayhistory/rust/Cargo.toml --lib cloud:: --locked
cargo test -p ai-hist --features export identity_pages --locked
```

Regression tests cover Cloud's `201 Created` device grant, authorization polling,
origin binding, official-CLI credential reuse, sharing choices, complete exclusion
pagination, private atomic state, collector locking and stale stop requests. The
sign-in tests live with the shared implementation, in the library's `cloud` module.

For a composed local test, start the companion marketing proxy on 3100, Cloud
on 3101 with its development-only Teams login and a local DB, and the local
RelayHistory service. Then:

```sh
node scripts/test-probe-local.mjs --binary /absolute/path/to/agent-relay-probe
```

Node is used only by this test harness. The child probe gets a fresh synthetic
home and `/usr/bin:/bin` PATH with no inherited provider credentials. The harness
approves its own development device grant, sends a uniquely identified synthetic
Claude session, reads that exact stored session, checks Cloud's `receiving` state,
verifies private storage and stops the collector. It keeps credentials and the
captured approval URL out of the test output, then removes its fixture directory.
It refuses remote origins and must never run against production.

## Desktop bridge v1

The macOS companion uses `agent-relay-probe` for authentication, local credential
storage, capture and delivery. All bridge commands accept `--json`; every JSON
object has `bridge_version: 1`. Status output contains no credentials. Existing
human-readable install/status/stop commands remain available.

- `installs --json`: discover configured probe directories.
- `cloud install --json --include-existing|--new-sessions-only|--selected-sessions-only`:
  emit NDJSON `approval`, `authenticated`, `connected`, and `ready` events. The
  browser approves the device; `authenticated` establishes the Agent Relay
  login boundary before the probe provisions RelayHistory upload credentials.
  `connected` confirms those upload credentials are stored. JSON setup requires
  one explicit sharing choice and runs in the background.
- `start`, `status`, `pause`, `resume`, `disconnect`, `compact`: pass
  `--account ID`, `--workspace ID`, and optionally `--site-url URL`, plus
  `--json`.
- `status --json` includes the upload journal's usage against its cap:

  ```json
  { "retention": { "used_bytes": 268433716, "limit_bytes": 268435456 } }
  ```

  When a cycle fails on that cap, `last_cycle.error_class` is
  `retention_limit` and its message carries the same numbers:
  `Upload journal full (256 MB of 256 MB). Compacting consumed records; queued
  sessions are preserved.`
- `compact <target> --json`: reclaims every journal record already consumed by
  all subscriptions and every settled batch receipt, under the desktop control
  lock. Queued and unacknowledged records are untouched, so it is safe while
  the collector runs. It returns the journal rows removed and the usage left:

  ```json
  { "removed_records": 114490, "retention": { "used_bytes": 1048576, "limit_bytes": 268435456 } }
  ```

  Compaction reclaims consumed records only: a cap filled by un-uploaded
  backlog frees as those uploads are acknowledged. A `retention_limit` verdict
  in `last_cycle` is re-measured as part of the pass: a pass that reclaims
  space and leaves the journal under its cap clears the verdict without waiting
  for the next capture cycle, and a pass that reclaims nothing leaves it
  standing. The cap rejects the record that would exceed it without recording
  it, so a journal that is full for the next record still reads below its cap;
  reclaimed space, not usage, is what resolves the condition.

- `sessions list <target> --json --limit 500`: newest sessions with title,
  source, project path, activity and upload status. `uploading` means a record
  from the session is in the currently leased batch; `queued` means a pending
  batch. `shared` is used while a session's acknowledgement is not established.
- `sessions include|exclude <target> --json --session SOURCE:ID …`: explicitly
  share one session or a batch, or stop sharing them. This does not erase
  already uploaded history.
- `sharing set <target> --json --mode all|new|selected`: share everything,
  establish a new-only baseline, or share only explicitly selected sessions.
  Previously selected sessions are retained across mode changes.

Selected mode uses the core's normalized, deny-by-default session membership.
Adding a session creates its own immutable historical snapshot and indexed
journal cursor. Adding B keeps A's acknowledged progress and pending immutable
batch; repeating an inclusion is a no-op. A removal fences an in-flight lease
and is checked again before dispatch. Re-inclusion takes a fresh snapshot, so
previously skipped records are backfilled. New discoveries stay private without
writing an exclusion for every catalog row. The selected-session manifest is
still written for desktop compatibility; its size follows explicit selections,
not the machine's history. Session lists and status remain read-only.

Selected delivery drains captured records before targeted hydration and keeps
draining a backlog before reading provider files. Hydration uses the core's
parser, observation locks, cancellation and checkpoints. A separate periodic
shallow inventory worker refreshes catalog metadata; slow provider enumeration
never runs on the selected delivery scheduler. Selected setup does not run a
full-history capture. Paused selected jobs do not hydrate or deliver; the
background inventory can still discover metadata. All/new sharing modes retain
their capture behavior, with eligible delivery attempted before capture.

Sharing mutations serialize on `desktop.lock`, stop the collector, and take
its lock. A durable `sharing-change.json` records the intended change before
any writes; replay completes it before delivery can restart. Selected-mode
include/exclude operations update only changed membership. Explicit sharing
mode changes retain generation replacement semantics and preserve pause state.

On the first writable use of an older selected install, core adopts the same
job in place: destination generation, pending/prepared batches, acknowledgments,
retry state, paused/blocked state, historical bounds and preimages survive.
This one-time upgrade copies only explicitly selected snapshots and can require
retention headroom; a capacity error rolls the entire adoption back. New delivery
indexes are built once during schema upgrade. Existing exclusions remain
privacy guards, including exclusions needed by another active destination.
No destructive queue migration occurs. Read-only commands also accept the old
schema before the writable upgrade.

`capture-diagnostic.json` reports capture stage, elapsed time, counts and an
allowlisted error class; inventory diagnostics use a separate file. Logs never
include raw provider errors, transcript content, paths or credentials. A
successful include command means consent and preparation were recorded, not
that a receiver acknowledged the session. `shared`, `queued`, `uploading` and
`uploaded` retain their existing bridge meanings. The historical 77/86-second
capture failures cannot be diagnosed from the old generic logs; their cause
remains unknown until a redacted diagnostic is reproduced.

Disconnect stops the collector, cancels jobs, best-effort revokes the workspace
RelayHistory token, and removes this install's stage credentials/configuration.
The local history database is retained. It does not remove shared Cloud login
credentials belonging to other Cloud clients.

### Local title preview

`agent-relay-probe sessions preview --json --limit 100 [--refresh]` has no
Cloud target, credentials, or transport. `--refresh` uses RelayHistory's existing
shallow discovery adapters and a separate private metadata catalog under
`~/.agentworkforce/session-preview/`; it does not contend with full capture's
writer or change inclusion. The cache-only form lists recent metadata and writes
`sessions.json` for immediate desktop startup. Both return the desktop session
shape with `included: false` and `status: "unknown"`. These rows are previews;
only the normal target-scoped session list can make them selectable for upload.

The desktop prewarms discovery during sign-in and displays cached titles before
awaiting capture or delivery status. The preview is limited to 100 recent sessions;
full browsing continues through the normal session list when preparation finishes.

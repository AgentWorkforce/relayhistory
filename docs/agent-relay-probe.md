# Native Agent Relay Probe

`agent-relay-probe` is the native collector for Cloud's new Teams onboarding flow.
It signs a computer in, obtains workspace-scoped RelayHistory credentials, captures
local coding sessions and runs durable delivery in the background. It needs no
Node.js, npm, NAPI addon or separately installed Agent Relay CLI.

## Source ownership

Keep the binary in `plugins/relayhistory/rust/src/probe/` in this repository. It
links the existing `ai-hist` capture engine, `ai-hist` delivery queue
and optional `relayhistory-plugin` transport. A separate repository would need to
coordinate versions of these same components without adding a runtime boundary.
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
`--include-existing`, `--new-sessions-only` and `--selected-sessions-only` make
that choice explicit in a noninteractive terminal. `--force-login` bypasses eligible existing CLI sign-in.
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
choice is immutable on reconnect: changing it is a separate, explicit
`sharing set` that replaces the delivery generation (see
[Desktop bridge](#desktop-bridge)).

Default setup starts a detached process. `--foreground` keeps it in the terminal;
`--once` captures and delivers one bounded cycle. To inspect or stop it:

```sh
~/.local/bin/agent-relay-probe status --site-url http://127.0.0.1:3100 --workspace WORKSPACE_ID --account USER_ID
~/.local/bin/agent-relay-probe stop --site-url http://127.0.0.1:3100 --workspace WORKSPACE_ID --account USER_ID
```

The executable must already be at that location to use these example paths.
The probe installs no launchd/systemd unit of its own and does not restart
itself after a reboot. Relay Desktop supervises it instead: the app starts at
login and runs `agent-relay-probe start` whenever `status --json` reports that
the collector is not running (see [Desktop bridge](#desktop-bridge)). Without
that app, run setup again after restarting the machine. Either way the existing
queue is preserved.

## Desktop bridge

Relay Desktop (the native menu-bar app) drives the probe through machine-readable
commands rather than by scraping its terminal output. Every one of them prints a
single JSON object on stdout — `cloud install --json` prints NDJSON, one object
per line — and every object carries `"bridge_version": 1`. Failures keep the
existing rule: exit code 1 and one safe sentence on stderr, never a response
body, credential, URL from a response or session content. Human output is
unchanged when `--json` is absent, and every existing command and flag still
behaves as before.

`<target>` below is the existing `--site-url URL --account ID --workspace ID`
selection of one install.

| Command | Purpose |
| --- | --- |
| `installs --json` | every connected workspace on this computer |
| `cloud install --json [--include-existing\|--new-sessions-only\|--selected-sessions-only]` | connect, as NDJSON events |
| `start <target> [--json]` | start the background collector, without signing in again |
| `status <target> --json` | running/paused state, delivery progress, session counts |
| `pause` / `resume <target> [--json]` | stop and resume uploading without disconnecting |
| `sessions list <target> --json [--limit N]` | the captured sessions and whether they are shared |
| `sessions include` / `sessions exclude <target> --json --session SOURCE:ID …` | change which sessions are shared |
| `sharing set <target> --mode all\|new\|selected --json` | change what the install uploads |
| `disconnect <target> [--json]` | disconnect this workspace, keeping local history |

```jsonc
// installs --json
{"bridge_version":1,"installs":[{"directory":"/Users/x/.agentworkforce/probe/ab12…","site_url":"https://agentrelay.com","account_id":"usr_…","workspace_id":"ws_…","org_id":"org_…","sharing_mode":"new","running":true,"paused":false}]}

// cloud install --json --selected-sessions-only …   (NDJSON)
{"bridge_version":1,"event":"approval","verification_uri":"https://agentrelay.com/…","user_code":"ABCD-EFGH"}
{"bridge_version":1,"event":"connected","account_id":"usr_…","workspace_id":"ws_…","org_id":"org_…","directory":"/Users/x/.agentworkforce/probe/ab12…"}
{"bridge_version":1,"event":"ready","running":true}

// start <target> --json
{"bridge_version":1,"running":true,"started":false}

// status <target> --json
{"bridge_version":1,"probe_version":"0.18.8","running":true,"paused":false,
 "sharing_mode":"new","site_url":"https://agentrelay.com","account_id":"usr_…",
 "workspace_id":"ws_…","org_id":"org_…","directory":"…",
 "delivery":{"state":"active","bootstrap_complete":true,"pending_records":12,
   "pending_bytes":40960,"acknowledged_records":3401,"unqueued_changes":0,
   "suppressed_records":0,"last_attempt_ms":1758300000000,
   "last_acknowledged_ms":1758300000000,"next_attempt_ms":0,"failure":null},
 "sessions":{"total":210,"shared":57,"excluded":153},
 "last_cycle":{"at_ms":1758300000000,"ok":true,"message":null}}

// pause / resume <target> --json
{"bridge_version":1,"paused":true}

// sessions list <target> --json
{"bridge_version":1,"sharing_mode":"new","sessions":[
 {"source":"claude","session_id":"29284179-…","title":"Fix auth rewrite…",
  "cwd":"/Users/x/code/app","git_branch":"main","first_activity_ms":1758200000000,
  "last_activity_ms":1758300000000,"included":false,"status":"not_shared"}]}

// sessions include / exclude <target> --json --session claude:29284179-…
{"bridge_version":1,"included":["claude:29284179-…"],"regenerated":true}
{"bridge_version":1,"excluded":["claude:29284179-…"],"regenerated":false}

// sharing set <target> --mode selected --json
{"bridge_version":1,"sharing_mode":"selected","regenerated":true}

// disconnect <target> --json
{"bridge_version":1,"disconnected":true}
```

`user_code` is `null` when the approval URL already carries it. `--json` never
reads stdin, so a first-time `cloud install --json` must pass one of the three
sharing flags, and it always starts the detached collector even if `--foreground`
was also given. `start` is idempotent: `started` is true only when that call
launched a process. `last_cycle` is what the collector recorded in `cycle.json`
after its last cycle, with `message` holding the same safe sentence the terminal
would have shown. A session's `status` is `not_shared` when it is excluded,
`queued` while the generation still has records in flight, and otherwise
`shared`; per-session acknowledgement is not cheaply derivable from the delivery
journal, so no row claims `uploaded`. `title` is the first prompt (whitespace
collapsed, 120 characters), then the last assistant message, then the session id.

### Sharing modes

`sharing_mode` is stored in `config.json` beside the original `include_existing`
boolean, which stays authoritative for the delivery selection and is kept in
step (true only for `all`); a configuration written before the bridge has no
mode and derives one from that boolean.

- `all` — existing and future sessions. This is the only mode whose delivery
  selection includes the prompt-only rows, some of which carry no session
  identity.
- `new` — everything known at the moment of the choice is excluded; sessions
  discovered later are delivered.
- `selected` — as `new`, and each cycle the collector also excludes every newly
  discovered session unless it is listed in `selected.json`.

`selected.json` is the durable list of sessions the user picked by hand. It
survives mode changes, so a later baseline never withdraws a deliberate choice.
`sessions exclude` adds exclusions, which delivery rechecks when a batch is
prepared, claimed and dispatched, so it needs no new generation.

`sessions include` and `sharing set` may need one: withdrawing an exclusion that
a live job selects is refused, and only `all` changes the job's own selection.
When that happens, the command — not the collector — owns the replacement: it
stops a running collector, takes the collector lock, cancels every live
generation for this destination, converges the exclusion table, creates the new
generation, writes `config.json` and `selected.json`, releases the lock and
starts the collector again. The command returns only once that is durable, and
its JSON reports `regenerated`. A change that only adds exclusions skips all of
that and reports `regenerated: false`. A replacement generation rescans local
history; the service is idempotent per record revision, so re-sending already
acknowledged rows is work rather than duplication.

`disconnect` stops the collector, cancels the generation, best-effort revokes the
stored RelayHistory session against `/v1/auth/token/revoke`, deletes the stored
credentials and `config.json`, and keeps `history.db`: the local capture is the
user's own data. Afterwards `installs` no longer lists the directory.

## State and delivery

Each `(site origin, user, workspace)` has an independent SHA-256-named directory
under `~/.agentworkforce/probe/`. It contains an SDK-owned `history.db`, persisted
selection/job metadata, scoped RelayHistory auth, a log, a runtime record and a
collector lock. Directories are mode 0700; credentials/config/logs are mode 0600.
No Cloud bearer credential is persisted by the probe. Tokens are never command
arguments, and provider errors, response bodies and session content are not logged.
The device approval URL is intentionally displayed in the interactive terminal.

Each cycle delivers through the shared core delivery worker — the same bounded
drain the SDK uses — carrying the RelayHistory receiver. The worker owns
immutable batches, leases and their keepalive, prepared-byte persistence, the
eligibility recheck immediately before dispatch, retry/backoff, acknowledgment
and compaction; the receiver owns only the consent and destination guards and
the transport, so progress advances only on a validated receipt. Mapping/account
mismatches block delivery. A failed attempt retains unacknowledged data and the
job waits out its own backoff, which is normal operation rather than a fault.
Transport credentials refresh through the existing RelayHistory auth
implementation.

The probe sends the onboarding heartbeat only after a successful capture and
delivery cycle. Cloud determines whether session data has actually arrived. A
running process alone does not mark the dashboard's data step complete.

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
cargo test -p ai-hist --features delivery identity_pages --locked
```

Regression tests cover Cloud's `201 Created` device grant, authorization polling,
origin binding, official-CLI credential reuse, sharing choices, complete exclusion
pagination, private atomic state, collector locking and stale stop requests. The
desktop bridge adds parsing tests for every new command, the JSON shapes above,
a paused generation reported as a healthy capture-only cycle, `selected` mode
withholding sessions discovered after its baseline, and `sessions include`
replacing the generation while keeping `selected.json`. The
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

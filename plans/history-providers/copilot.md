# GitHub Copilot CLI history research — #432

Status: research complete for ordinary imported messages and tool joins; no
provider admitted. Issue [#432](https://github.com/AgentWorkforce/relayhistory/issues/432)
remains open. This work targets local CLI history, excluding IDE and hosted
agent history. Base: RelayHistory main `cc11ea2a`, researched 2026-10-11.

## Exact product and evidence

The official npm distribution is `@github/copilot@1.0.95`, build revision
`f5ba50575c2`. The actual writer/reader exercised is the matching official
`@github/copilot-darwin-arm64@1.0.95` executable on macOS ARM64. Package integrity
and the executable SHA-256 are recorded in
[provenance.json](fixtures/copilot/provenance.json). The loader package is a
small launcher; the platform package contains the native binary rather than a
public JavaScript journal writer. The pinned official SDK source is
[`github/copilot-sdk@6be623b01b194a61466385e3f708639ec98ce8ac`](https://github.com/github/copilot-sdk/tree/6be623b01b194a61466385e3f708639ec98ce8ac).
SDK schema observations below are separate from executable persistence proof.

The official CLI has an offline `sessions import` command. Its semantic JSONL
input is a different format from the native event journal. A session header
uses `type: session`, interchange `version: 1`, `externalId` and `createdAt`.
Message records have an ID, ISO timestamp, role and text-block content array.
`tool-call` / `tool-result` records use their semantic IDs for input joins.
The official dry-run validator rejected an orphan call and a dangling result
while developing the synthetic input. No input came from private history.

The executable wrote the checked-in [events.jsonl](fixtures/copilot/events.jsonl)
using [semantic-input.jsonl](fixtures/copilot/semantic-input.jsonl). A separate
headless process then resumed that session and called `session.getMessages`.
Its first six returned records exactly matched every persisted journal record.
The [official reader oracle](fixtures/copilot/official-reader.json) additionally
contains one in-memory `session.resume` record. After `session.detach`, the
journal was byte-identical to the file produced by the importer. A second
independent capture reproduced these assertions. No auth, prompts, tool
execution, model requests or existing Copilot home were used.

The fixture preserves writer-produced UUIDs and timestamps. Only the synthetic
temporary project path is replaced by `/fixture/copilot`. The captured native
byte count is the size before this path substitution. Do not treat the
sanitized fixture size as that original measurement.

## Passive acquisition shape

Official [configuration documentation](https://docs.github.com/en/copilot/reference/copilot-cli-reference/cli-config-dir-reference)
places the primary log at `$COPILOT_HOME/session-state/<id>/events.jsonl`,
defaulting to `~/.copilot`. `COPILOT_HOME` replaces that entire root; legacy
`--config-dir` has higher precedence in the CLI. A passive adapter should freeze
explicit roots before environment/default resolution and never launch the CLI
to discover an override. `session-store.db` is derived cross-session data,
while `workspace.yaml`, checkpoints and tracked files accompany each journal.
Our isolated writer/reader produced no `session-store.db`; native history was
read successfully without it. Metadata and auth/config files are not transcript
sources. `COPILOT_CACHE_HOME` is separate from the session root.

The native fixture header is `session.start` with `data.version: 1`,
`data.producer: copilot-agent`, `data.copilotVersion`, native `data.sessionId`,
`data.startTime` and `data.context.cwd`. All six events carry native UUID `id`,
ISO timestamp and `parentId` linking the chronological preceding event. The
session-directory UUID agrees with the header identity; `workspace.yaml`
contains the same ID and cwd. Do not infer an alternate profile's identity
from directory names alone or overwrite a same-ID live locator.

The importer mints new session/event/message/tool IDs and new event timestamps.
It retains input `createdAt` as `data.startTime`; input message timestamps are
not preserved. Therefore native event time and session start time have distinct
provenance, and semantic input IDs are not native evidence IDs.

`user.message.data.content` and `assistant.message.data.content` preserve all
three synthetic displayed messages. The assistant also carries native
`data.messageId`; the event envelope's ID remains a separate identity. Tool
requests on that assistant and `tool.execution_start` / `tool.execution_complete`
join by the same native `toolCallId`. Start records preserve name/arguments;
complete records preserve `success: true` and `result.content`. These tools
were imported semantic facts: the CLI did not actually run a `read` tool.
File edits, failures and live-agent request accounting remain unproven.

## Coverage limits and next probes

| Evidence | Current proof | Admission constraint |
| --- | --- | --- |
| User/assistant messages | Native writer and independent reader agree after restart | Ready for future adapter fixtures; not yet searchable through RelayHistory |
| Tool requests/results | One successful imported native join | Capture failed/partial/cancelled live tool facts before claiming full fidelity |
| Working directory/session ID | Header and writer-generated workspace metadata agree | Test missing cwd, alternate roots and same-ID collisions during implementation |
| Resume | Reader returns an extra native resume event; journal remains unchanged | This fixture does **not** prove persisted resume markers or a new session relationship |
| Usage | No usage-bearing persisted events in this fixture | Keep unavailable; never turn missing usage into zero |
| Delegation | No subagent events captured | No child session or stable relationship claims |
| Edits | No committed file mutation observed | A tool path is not proof of an edit |
| Compaction/rewrites/tails | Not produced by the importer | Still require journal lifecycle probes and passive parser regression cases |

Pinned official [event definitions](https://github.com/github/copilot-sdk/blob/6be623b01b194a61466385e3f708639ec98ce8ac/nodejs/src/generated/session-events.ts)
mark `assistant.usage` as always ephemeral and describe it as excluded from the
on-disk event log. `assistant.message.outputTokens` is optional; usage receipts,
checkpoints and shutdown metrics also exist in the schema. Their availability,
deduplication and accounting semantics must be established from real persisted
bytes before choosing an accounting mode. Context occupancy and accumulated
premium-request/credit metrics are not per-request token evidence.

The same schema exposes `agentId`, subagent lifecycle records, deprecated
`parentToolCallId`, task-registry parent IDs and detached-parent session IDs.
Those identities are different concepts; an `agentId` is not automatically a
child session ID. No delegation fixture was fabricated from these types.
Unknown future event kinds should remain raw markers; reject or diagnose an
unsupported header version before interpreting it. The next probe should
capture an isolated genuine tool/delegation/usage turn with explicitly approved
access, then restart the official reader and compare its persisted event
projection. Research scripts must continue to avoid user history.

Official SDK [getEvents](https://github.com/github/copilot-sdk/blob/6be623b01b194a61466385e3f708639ec98ce8ac/nodejs/src/session.ts#L2422)
uses the `session.getMessages` RPC exercised here. Native resume is documented
as `copilot --resume SESSION-ID`; it is available in the product but is not
advertised as a RelayHistory capability by this research.

## Reproduction and validation

Download the exact platform tarball without running npm install scripts:

```sh
curl -fsSL https://registry.npmjs.org/@github/copilot-darwin-arm64/-/copilot-darwin-arm64-1.0.95.tgz -o /tmp/copilot-1.0.95.tgz
mkdir -p /tmp/copilot-1.0.95
tar -xzf /tmp/copilot-1.0.95.tgz -C /tmp/copilot-1.0.95
python3 plans/history-providers/fixtures/copilot/capture.py --cli /tmp/copilot-1.0.95/package/copilot --out /tmp/copilot-recapture-432
python3 plans/history-providers/fixtures/copilot/validate.py
```

`capture.py` verifies the pinned binary SHA-256, creates an empty temporary
configuration and project, invokes only offline import/version plus the RPC
allowlist `ping`, `session.resume`, `session.getMessages`, `session.detach`,
and terminates its process. It refuses an existing output directory. It writes
no provider code, source registration, shared schema or package lockfiles.

Checked results: official capture/restart reader assertions pass; six offline
fixture tests pass. Those tests check displayed content against semantic input,
complete native ID chains, exact reader/journal agreement, native tool joins,
unknown richer coverage and rejection of unsupported/malformed/partial records.
Mutating a result's native tool ID causes the join validator to fail; removing
the newline or changing the header version also fails. These checks validate
the research artifact; they do not prove an implemented passive adapter,
bounded discovery, rewrite retirement, containment, watch, search or export.
No Rust/native/SDK production surface changed, so their implementation gates
are reserved for the eventual admitted provider PR after both pilots land.

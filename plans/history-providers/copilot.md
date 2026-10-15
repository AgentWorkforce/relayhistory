# GitHub Copilot CLI historical acquisition (#432)

Source `copilot` reads GitHub Copilot CLI's native session journals. It covers
the CLI only: VS Code Copilot Chat and hosted coding-agent history are separate
products with separate stores.

## Pinned product and evidence

- Writer-proven fixture: the official `@github/copilot-darwin-arm64@1.0.95`
  executable (build `f5ba50575c2`), whose package integrity and binary SHA-256
  are in [`provenance.json`](../../crates/ai-hist/src/providers/copilot/provenance.json).
  Its offline `sessions import` wrote
  [`official-importer`](../../crates/ai-hist/tests/fixtures/copilot/official-importer)
  from [`semantic-input.jsonl`](../../crates/ai-hist/src/providers/copilot/semantic-input.jsonl);
  a separate headless process resumed the session and returned
  [`official-reader.json`](../../crates/ai-hist/tests/fixtures/copilot/official-reader.json)
  through `session.getMessages`. The reader agrees with the journal record
  for record and adds one in-memory `session.resume` the journal never
  persisted.
- Shape-proven fixture: [`cli-usage`](../../crates/ai-hist/tests/fixtures/copilot/cli-usage)
  is authored from the persisted shape of Copilot CLI 1.0.86 journals — event
  types and field names observed read-only on a real install, every value
  synthetic. It carries what the importer cannot produce: per-message model,
  `reasoningText` and `outputTokens`, a failed tool, a model change, a
  subagent, a usage checkpoint and two `session.shutdown` totals across a
  resume.
- Schema reference: [`github/copilot-sdk@6be623b0`](https://github.com/github/copilot-sdk/blob/6be623b01b194a61466385e3f708639ec98ce8ac/nodejs/src/generated/session-events.ts).

No private history, authentication, model request or tool execution was used.

## Acquisition

`$COPILOT_HOME/session-state/<id>/events.jsonl`, with `COPILOT_HOME`
defaulting to `~/.copilot` and replacing the whole root when set. Discovery
lists each session directory's `events.jsonl` and never walks checkpoints,
file snapshots or `session.db`. `workspace.yaml` and the derived
`session-store.db` are not read: the journal header carries the identity and
working directory, and `workspace.yaml`'s `name` is a generated label rather
than a title.

The header is `session.start` with `data.version: 1`; any other first record
or version is `COPILOT_UNSUPPORTED_SCHEMA`. Every record is an envelope of
`type`, `data`, native UUID `id`, ISO `timestamp` and `parentId` (the
chronologically preceding event). A malformed complete record fails the file
with `COPILOT_INVALID_RECORD`; an unterminated final record is deferred with
`COPILOT_PARTIAL_TAIL` and prior evidence is kept. A native session ID already
owned by another live journal is `COPILOT_ID_COLLISION`.

Shallow discovery reads a bounded 64 KiB head (header, first prompt, title)
and a 16 KiB tail (last activity).

## Normalization

| Journal record | Stored as |
| --- | --- |
| `user.message` | user `text` event; a history prompt only when `data.source` is absent (a person typed it); otherwise (`agent-<id>`, `skill-<name>`) `control_kind: synthetic`, outside prompts and user turns |
| `assistant.message` | assistant `thinking` (`reasoningText`), `text` (`content`) and `tool_use` (`toolRequests`) events; `model`, `messageId` as provider message ID, `requestId`, `turnId`; `parentToolCallId` marks a subagent message as sidechain |
| `tool.execution_complete` | `tool_result` event joined by `toolCallId`; `success: false` is `errored` with `error.message` as its text |
| `tool.execution_start` | marker; also the tool call when no assistant message requested it |
| everything else | raw marker, `kind` = the native event type |

A subagent numbers its tool calls independently of the main agent — real
journals hold a main-agent `bash_14` and a subagent `bash_14` in one session —
so a call whose record carries `parentToolCallId` is stored as
`<parentToolCallId>/<toolCallId>`. Requests, starts and results all carry the
same parent, so the join stays exact.

Every event carries `copilotVersion` as agent version and the header
context's `cwd` / `branch`. `parentId` names the chronologically preceding
record, often a turn boundary or other marker; an event's parent is the
nearest record up that chain stored as an event, while markers keep their raw
`parentId`. A subagent's reply never becomes the session preview, and a
journal that starts declaring another `sessionId` retires the identity it
used to back.

## Usage

Copilot persists one per-message counter, `assistant.message.data.outputTokens`.
It is stored verbatim as the message's usage (`{"outputTokens": n}`) and
normalizes to output-only `per-message` usage; input and cache counters stay
unknown per request, never zero. Some builds omit it entirely.

`session.shutdown.data.modelMetrics` holds per-model `requests` and `usage`
(`inputTokens` — cache-inclusive — `outputTokens`, `cacheReadTokens`,
`cacheWriteTokens`, `reasoningTokens`). The SDK documents it as session-wide
accumulated, restored from `session.usage_checkpoint` on resume, so each
shutdown is a cumulative snapshot, not a segment. These records are raw markers
and never request usage; a consumer that accounts sessions reads them there.

`assistant.usage` is ephemeral by schema and never persisted. The
OpenTelemetry file exporter (`COPILOT_OTEL_FILE_EXPORTER_PATH`) is opt-in,
not retroactive and carries no message content; it is not a source.

## Coverage limits

| Evidence | Status |
| --- | --- |
| User/assistant messages | Writer- and reader-proven |
| Tool requests/results | Writer-proven success join; failure shape from real journals |
| Per-request input/cache usage | Not persisted by Copilot |
| File edits | Not captured: `tool.execution_complete.fileEdits` is unobserved in real journals |
| Delegation | Subagent records are markers; no child session or relationship |
| Resume | Persisted `session.resume` is a marker; native resume is not advertised |
| Compaction / truncation / rewind | Raw markers when present; no active-context projection |

## Reproduction

```sh
curl -fsSL https://registry.npmjs.org/@github/copilot-darwin-arm64/-/copilot-darwin-arm64-1.0.95.tgz -o /tmp/copilot-1.0.95.tgz
mkdir -p /tmp/copilot-1.0.95 && tar -xzf /tmp/copilot-1.0.95.tgz -C /tmp/copilot-1.0.95
python3 crates/ai-hist/src/providers/copilot/capture.py --cli /tmp/copilot-1.0.95/package/copilot --out /tmp/copilot-recapture-432
```

`capture.py` verifies the pinned binary, uses an empty temporary
`COPILOT_HOME`, runs only the offline importer and the `ping`,
`session.resume`, `session.getMessages` and `session.detach` RPCs, and refuses
an existing output directory.

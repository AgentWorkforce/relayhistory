# Codex 0.161.0 offline persistence characterization

Actual installed native Codex app-server writer, macOS arm64. The native binary
and generated protocol schemas are checksum-pinned in provenance.json. The
capture uses a fixed system-only PATH, an isolated TMPDIR, an empty temporary HOME/CODEX_HOME, an explicit read-only sandbox,
synthetic base/developer instructions, and a custom provider whose only endpoint
is localhost port 9. It never sends turn/start, authenticates, runs a tool or
makes a model request. The recorded function call/result are synthetic injected
Responses items, not executed read-tool output. Usage is unavailable.

`capture.py /absolute/path/to/pinned/native-codex [temporary-output-directory]` starts a thread,
injects five items (user, assistant, function call, function result, assistant),
shuts down app-server, restarts, resumes the same native thread and injects a
second user/assistant pair. `sanitize.py OUTPUT` replaces temporary paths,
wall-clock values and random UUIDs consistently across persisted rows and RPC
responses. It retains the native row schema, ordinal and reference structure.
The sanitized session UUID is 019b76da-a800-7000-8000-000000000161; the actual
call/result join ID remains offline-call-1. The writer produced session.jsonl
with thirteen rows, including runtime context/control records.

The independent official reader attempt is a documented gap. The writer uses
historyMode=paginated, and all of `thread/read {includeTurns:true}`,
`thread/turns/list {itemsView:"full"}`, and `thread/items/list` return empty
visible-turn/item projections before and after process restart. Injection
persists model-visible response_item history but does not create a visible turn.
`reader.json`, `turns.json`, `items.json`, `reopened-items.json` and
`resumed-items.json` preserve those responses. Empty projection does not mean no
persisted history, and this probe does not certify an independent transcript
oracle. expected-messages.json comes from the authored injection inputs. A public
RelayHistory characterization test may compare it with native-writer acquisition
while retaining this reader limitation explicitly.

No SQLite database, credentials or operator history is included. The full
sanitized RPC transcript records the exact methods requested; no turn/start is
present. No authentication or model runtime is needed to reproduce the writer.

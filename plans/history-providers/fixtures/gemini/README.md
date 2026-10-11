# Gemini CLI research fixtures

Research only for [history issue #433](https://github.com/AgentWorkforce/relayhistory/issues/433). These files are outside RelayHistory's admitted fixture corpus. They do not enable a provider or claim public discovery/search coverage.

The pin is google-gemini/gemini-cli `9b6e0265d16bbd29ca51e33c9e0c01dc4cec5e83`, whose package reports `0.65.0-nightly.20261006.gfb972b2f8`. Capture ran on Darwin arm64, Node `v22.22.2`, TypeScript `5.9.3`, 2026-10-11. `provenance.json` hashes every inspected source; `manifest.json` hashes each fixture file.

`capture.mjs` transpiles and executes the complete pinned `ChatRecordingService` and its official `loadConversationRecord` reader. It extracts complete upstream helper function AST nodes for project hashing, filename sanitization and ignored user content, and executes upstream content rendering and recording constants. Serialization, append, resume, tool updates, history reconciliation, rewind and legacy migration are upstream code. The official reader runs separately against the resulting file bytes; its projection is saved in `official-reader.json`.

The harness injects a synthetic config/project slug and tool registry, deterministic clock/UUID generator and an error logger. Session deletion is forbidden. The root oracle executes pinned `Storage` static path methods and the upstream home resolver with injected environment objects; it supplies a registry slug instead of invoking project migration. No agent, CLI session, auth flow, model request or private user history is involved.

| Case | Origin and assertion |
| --- | --- |
| ordinary | Actual current writer: user/assistant, duplicate message revisions, thought summary, explicit response usage, tool call/result and info record. |
| short-prefix-collision | Actual current writer: two full session UUIDs share the same eight-character filename prefix; the second filename gains a collision index. |
| tool-result-patched | Actual current writer: history synchronization masks a tool result with `$patch`, joined by the original call ID. |
| resumed | A fresh recorder instance reloads the journal and appends under the original full session UUID. |
| child | Actual current writer: child UUID file nested under the complete parent UUID, `kind: subagent` and workspace directories. |
| compacted | Synthetic compaction-shaped history passed to the actual writer's `updateMessagesFromHistory`, the persistence method used by `GeminiChat`. Exercises remove/order/content patches; does not invoke a live compressor. |
| rewound | Actual current writer: `$rewindTo` excludes its target and trailing logical turns; original bytes remain. |
| legacy | Synthetic serialized `ConversationRecord`, accepted by the pinned official legacy reader; not a capture from an older writer revision. |
| legacy-migrated | Actual current writer resumes that legacy JSON into JSONL and appends without changing its UUID. |

Offline verification:

```sh
node --test plans/history-providers/fixtures/gemini/validate.test.mjs
```

Reproduction needs a checkout at the exact upstream pin and a TypeScript 5.9.3 module. No upstream install or Gemini configuration is needed:

```sh
git clone --filter=blob:none https://github.com/google-gemini/gemini-cli.git /tmp/gemini-source
git -C /tmp/gemini-source checkout 9b6e0265d16bbd29ca51e33c9e0c01dc4cec5e83
node plans/history-providers/fixtures/gemini/capture.mjs \
  --source /tmp/gemini-source \
  --typescript /path/to/typescript/lib/typescript.js \
  --output /tmp/gemini-output
```

The output directory must be absent or empty. Selected upstream source hashes are checked before execution. Compare `cases/`, `official-reader.json` and `root-oracle.json` with the checked-in equivalents. Two independent captures produced byte-identical outputs. Do not regenerate the official reader oracle from `replay.mjs`; that module is an independent research model checked against the oracle, not the oracle itself.

The offline suite has 11 checks. Omitting tool-result patch application in a disposable copy produces exactly two failures: the complete official-reader projection comparison and `masked tool results replay actual $patch updates by tool id`. Restoring the research model passes all checks. This proves the fixture validator detects lost patch semantics; it does not prove a RelayHistory adapter exists.

The suite also defers an incomplete final JSON record and rejects complete malformed records/missing identity. This is an explicit proposed acquisition safety rule. Upstream's reader skips malformed JSONL lines, so production ingestion must not inherit its tolerance and report complete capture after dropping corruption.

# Gemini CLI historical session research (#433)

Status: research and synthetic fixtures complete; production admission deferred until the Pi and Hermes pilots establish the intake interface. Issue #433 remains open. No adapter, descriptor, SDK vocabulary, runtime/schema change or admitted corpus entry is included here.

The inspected variant is Google Gemini CLI at commit `9b6e0265d16bbd29ca51e33c9e0c01dc4cec5e83`, package `0.65.0-nightly.20261006.gfb972b2f8`, inspected/captured on 2026-10-11. Synthetic capture ran on Darwin arm64 with Node 22.22.2 and TypeScript 5.9.3. The complete current writer and independent official file reader are executed from that pin, with isolated config/clock/UUID/tool-registry dependencies. Details, source hashes, captured bytes and the reproducible harness are under [fixtures/gemini](fixtures/gemini/README.md).

## Primary evidence

All source links below are pinned; live documentation paths alone are insufficient:

- [ChatRecordingService writer and JSONL/legacy reader](https://github.com/google-gemini/gemini-cli/blob/9b6e0265d16bbd29ca51e33c9e0c01dc4cec5e83/packages/core/src/services/chatRecordingService.ts).
- [Recording types/opcodes](https://github.com/google-gemini/gemini-cli/blob/9b6e0265d16bbd29ca51e33c9e0c01dc4cec5e83/packages/core/src/services/chatRecordingTypes.ts).
- [Storage roots and hash-to-slug migration](https://github.com/google-gemini/gemini-cli/blob/9b6e0265d16bbd29ca51e33c9e0c01dc4cec5e83/packages/core/src/config/storage.ts), [project registry](https://github.com/google-gemini/gemini-cli/blob/9b6e0265d16bbd29ca51e33c9e0c01dc4cec5e83/packages/core/src/config/projectRegistry.ts), and [home/project-hash helpers](https://github.com/google-gemini/gemini-cli/blob/9b6e0265d16bbd29ca51e33c9e0c01dc4cec5e83/packages/core/src/utils/paths.ts).
- [Session UI documentation](https://github.com/google-gemini/gemini-cli/blob/9b6e0265d16bbd29ca51e33c9e0c01dc4cec5e83/docs/cli/session-management.md). Its hash-only path description lags the current slug migration; the writer is the acquisition authority.
- [GeminiChat history persistence and response usage](https://github.com/google-gemini/gemini-cli/blob/9b6e0265d16bbd29ca51e33c9e0c01dc4cec5e83/packages/core/src/core/geminiChat.ts), [client compression restart](https://github.com/google-gemini/gemini-cli/blob/9b6e0265d16bbd29ca51e33c9e0c01dc4cec5e83/packages/core/src/core/client.ts), [chat compressor](https://github.com/google-gemini/gemini-cli/blob/9b6e0265d16bbd29ca51e33c9e0c01dc4cec5e83/packages/core/src/context/chatCompressionService.ts), and [tool-output compression](https://github.com/google-gemini/gemini-cli/blob/9b6e0265d16bbd29ca51e33c9e0c01dc4cec5e83/packages/core/src/context/contextCompressionService.ts).
- [Official recording tests](https://github.com/google-gemini/gemini-cli/blob/9b6e0265d16bbd29ca51e33c9e0c01dc4cec5e83/packages/core/src/services/chatRecordingService.test.ts), including legacy checkpoint, rewind, resume fallback and bounded in-memory-window cases.

## Frozen roots and native identities

`GEMINI_CLI_HOME`, when non-empty, replaces the home base; otherwise upstream uses `os.homedir()`. The normal runtime tree is `<base>/.gemini`. With exactly `SANDBOX=sandbox-exec`, runtime data moves to `<base>/.cache/.gemini`; the home override applies before this switch. Other sandbox values do not themselves change this resolver, though a container may mount ephemeral runtime storage. The checked-in `root-oracle.json` executes these actual functions with five injected environment cases, without modifying process environment or reading a user's configuration.

Current `Storage.initialize()` obtains a project slug from `<runtime>/projects.json`, whose map is normalized absolute project path to slug. Ownership markers are `<runtime>/tmp/<slug>/.project_root` (also under history), and older SHA256 project directories migrate to the slug. The recorder therefore writes `<runtime>/tmp/<project-identifier>/chats/`; discover both slug and legacy hash directories rather than assuming a 64-character hash. `projectHash` inside session metadata still hashes the project root and is not the current directory name. Registry/ownership marker resolution is source-inspected, not exercised by the fixture harness, which injects a known slug. A future passive adapter must never call initialization or migration to discover files.

Main filenames are `session-<minute timestamp>-<first-eight-session-characters>.jsonl`, optionally with a collision index. The full `sessionId` comes from file metadata, not the short suffix. The actual writer fixture proves a filename-prefix collision creates two distinct full UUID sessions. Message identity is the stored `id`; append revisions reuse it. Main IDs in these fixtures are UUIDs, but the writer accepts the context's non-empty sanitized ID, so implementation must not infer that every child ID is necessarily UUID-shaped.

Subagent files are `<chats>/<sanitized complete parent id>/<sanitized child id>.jsonl`; metadata contains the child's full `sessionId`, `kind: subagent` and workspace directories, without a `parentSessionId` field. The folder is relationship evidence only when it can be joined to a parent's verified full ID using the upstream sanitizer. Do not invent tool/delegation joins from a basename or similarity. Distinct explicitly configured home roots need collision-safe namespaces, using frozen resolver provenance rather than repeated environment reads.

## Journal and legacy semantics

The current file is an append-only mutation journal, not a line-per-message transcript:

| Record | Official logical projection |
| --- | --- |
| Initial metadata | `sessionId`, `projectHash`, times, kind and optional directories. |
| Message with `id` | Insert or replace that ID while preserving its existing insertion position. |
| `$set` | Merge metadata; deprecated `messages` checkpoints replace the full logical message set. |
| `$patch.id` / `$patch.updates` | Replace message content and update existing tool results by actual call ID. |
| `$patch.removeIds` | Remove named logical messages. |
| `$patch.orderIds` | Reorder referenced existing IDs after the unreferenced prefix. |
| `$rewindTo` | Remove the target ID and all following logical messages; unknown target clears the set. |

Resume reloads the file, preserves the full native ID/start time and appends metadata and new turns. The recorder's 50-message in-memory window does not mean only 50 messages persist; the official reader reconstructs full history unless given bounded options. A passive parser must replay all admitted mutations coherently, deduplicate repeated IDs, and handle later patches against earlier messages. Fingerprints must cover exact bytes and filename/rotation changes; incomplete final lines should be deferred without sealing a complete generation.

Legacy `.json` contains one `ConversationRecord`. The current reader accepts it, and resume migrates it to a sibling `.jsonl`, retaining the `.json` file and full UUID. The fixtures prove this current migration path using a synthetic legacy object. An old real writer version remains unpinned; do not describe the JSON fixture as captured by that historical writer. A production adapter must deduplicate the retained JSON/JSONL revisions using metadata identity and coherent logical precedence, rather than importing both as separate sessions.

Compression has two relevant paths. Full chat compression creates synthetic summary user/model turns and restarts the chat with the existing recording data; `GeminiChat` reconciles the new history through recorder patches. Tool-output compression alters function responses, which updates the original tool result by its call ID. The fixture exercises both forms through the actual persistence API with synthetic histories and results; it performs no summarizing model call. Removed/rewound turns remain physically present in the journal while the official logical view drops them.

There is no dedicated persisted compaction opcode or per-message synthetic flag: `recordSyntheticMessage` uses the regular message shape. A summary may be a `user` record, and ordinary user text can resemble summary markup. Production design must distinguish finalized logical messages from superseded journal evidence without presenting synthetic summaries/tool responses as human requests or treating every `removeIds` as proven compaction. The exact archived-turn projection is a remaining admission decision; preserve the raw mutation evidence and provenance rather than guessing the user's intent.

## Evidence available and limits

- User and `gemini` assistant text are available as strings or content-part arrays; preserve text/thought/tool blocks and do not flatten function responses into human prompts. `info`, `error` and `warning` records are status evidence, not human turns.
- `thoughts` contain subject/description/timestamp summaries, not necessarily raw reasoning. Preserve the recorded summary and its provenance.
- Tool records carry their real ID, name, args, timestamp, status and optional result; results may be part arrays, strings or null. Current patches update results without replacing the call identity. Status requires explicit mapping to existing result states; the fixture proves `executing`→`success`, while remaining status spellings need scheduler-source verification before admission. No file edits are inferred from a tool's name/path.
- GeminiChat records the last usage metadata from the successful response attempt, once into the assistant message. The writer maps prompt/output/cache/thought/tool/total counts and defaults missing individual fields to zero when usage metadata exists. A missing/null `tokens` object stays unavailable. These are recorded response measurements, not a complete account of failed retries or independent compression requests; do not promise complete billing. Cached counts may be a subset of input and must not be double-added. No request ID or pricing is invented.
- Times are ISO strings generated at recording/metadata update; model is stored per assistant message. Project hash is not cwd. Main cwd recovery needs verified registry/ownership metadata; child `directories` are explicit workspaces, not necessarily execution cwd.
- Files have no schema-version field. Unsupported opcodes, bad identities/types and complete corruption need diagnostics. Upstream skips malformed JSONL lines; using that reader's successful result alone cannot prove complete acquisition. The research validator deliberately rejects complete malformed records and defers an incomplete final record.
- Passive roots/tree acquisition appears feasible. No authenticated export is required for the tested files. Native CLI resume is documented but not executed, and no resume or live capability is admitted here.

## Checks and next gate

`node --test plans/history-providers/fixtures/gemini/validate.test.mjs`: **11 pass**. Nine fixture cases match the complete durable projection returned by the pinned official reader. Root oracle covers default/home override/seatbelt precedence. Repeating actual-writer capture into a second empty directory produces identical fixture/oracle bytes. Negative control omitting tool-result patch application produces **2 failures**: oracle projection comparison and the named patch-result assertion; restore yields 11 pass.

This signal cannot distinguish production discovery, hydration, public search, permission handling, containment, watch scoping or cross-store coexistence: none is implemented. After the pilots land, promote reviewed fixtures into the admitted corpus alongside an adapter, then prove public discover→hydrate→read/search→reopen/change feed→export, source filtering, exact registry/docs/native/API contracts, coherent rewrites/partial tails, root absence/denial/overrides, symlink/blob containment, loss repair, cancellation and idempotent append/rotation. Finish scheduler status verification, old-writer pinning and the archived-turn/control projection decision before claiming their corresponding coverage cells. Do not close #433 on this research commit.

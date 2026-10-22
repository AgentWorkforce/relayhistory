# Cortex Code local history acquisition (#440)

The target is Snowflake Cortex Code CLI **1.2.18** (build
`1.2.18+192728.aa2238062732`), macOS arm64. Snowflake's [official
installer](https://ai.snowflake.com/static/cc-scripts/install.sh) resolves
`stable_version.txt` and a [release manifest](https://sfc-repo.snowflakecomputing.com/cortex-code-cli/a4643c4278/1.2.18%2B192728.aa2238062732/manifest.json).
The pinned `coco-1.2.18+192728.aa2238062732-darwin-arm64.tar.gz` SHA-256 is
`0dc9e79466020ab550bc3639c2814fa3d2a5090b3f14dfddd834c3520202364d`.
This is the CoCo CLI local format, not Snowsight, Desktop or account usage.

The released executable embeds a Node bundle. Its `ConversationHistory` class
(`fZ` in the minified bundle) is the native writer. `addMessage` queues
`appendHistoryEntry` into `<session-id>.history.jsonl`; `flushAllWrites` writes
metadata to `<session-id>.json`. A new session uses the `merged` history format:
one message per JSONL line with `role`, `id`, `content`, optional
`user_sent_time`, `assistant_sent_time`, `model` and request fields. Consecutive
messages of the same role can rewrite the same native ID with expanded content;
the released `Otr` history reader retains the **last occurrence of each ID**.
`resumeSession` loads the sidecar and rewrites it. Its `flat` format emits one
content block per row with `message_id`, `content_block_index` and
`source_role`; the parser must recognize that variant separately. The pinned
capture uses the native writer in both configurations and compares its
result with the released `Otr` restart reader: four merged messages, three
flat messages, a one-message sidecar after `removeLastMessage` rewrote a
two-message session, and an empty zero-byte sidecar after removing the final
message. Metadata can still report `history_length: 1` for that empty sidecar,
so the sidecar determines complete history. The raw reader oracles are in
`plans/history-providers/fixtures/cortex/`. The sidecar
is authoritative when present. Older snapshots can carry `history` or
`messages` arrays; the native migration writes them to the sidecar and removes
them from metadata. Thus `history_length` and `last_updated` in metadata are
not a safe JSONL watermark.

An offline synthetic capture calls this extracted release class with no
Snowflake account, authentication or model request. The independent
[`cortex-replay` reader](https://github.com/dataprofessor/cortex-replay/blob/main/src/parser.mjs)
parses the resulting snapshot and sidecar, returning the user prompt, assistant
text and tool result joined by `tool_use_id`. It corroborates the ordinary
merged shape, but its current implementation does not handle the native `flat`
variant or last-ID deduplication; those semantics come from the pinned native
reader. A duplicate ID in append history is retained at its last occurrence
position. An interrupted final JSONL line is deferred by RelayHistory until
complete rather than interpreted as a full capture; the native reader tolerates
some malformed lines, so this is an intentional safety boundary, not a claim
of identical error recovery. [Snowflake's ACP documentation](https://docs.snowflake.com/en/user-guide/cortex-code/cortex-code-acp)
corroborates the two paths and the snapshot/save versus incremental-log split.

The root is `<CORTEX_HOME>/conversations` when `CORTEX_HOME` is set; otherwise
`<SNOWFLAKE_HOME>/cortex/conversations`, otherwise
`~/.snowflake/cortex/conversations`. An explicit RelayHistory `ProviderRoots`
override wins. The provider only reads this tree. It makes no connection,
model or resume call.

The local records provide native message IDs, timestamps when written,
assistant model, tool IDs and result status. The `token_usage` metadata and
`session_stats` fields do not prove billable per-message use; usage accounting
stays unknown. The fixture contains no proven file-edit or child relationship
record. Native `/resume` behavior beyond this storage class remains outside
the acquisition claim.

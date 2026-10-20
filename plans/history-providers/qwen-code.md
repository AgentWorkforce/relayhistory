# Qwen Code history acquisition (#434)

The passive adapter targets the ordinary interactive recording service at
[QwenLM/qwen-code revision 82bd240](https://github.com/QwenLM/qwen-code/tree/82bd240ad0d31fd1d3a83dd803625118019ba35d).
The pinned package reports 0.25.3. Native synthetic recorder captures and an
independent upstream UUID-chain reader establish the supported schema; they do
not certify an authenticated released CLI execution.

The [fixture evidence](fixtures/qwen-code/README.md) records exact source hashes,
reproduction instructions, six native cases, root precedence oracle and limits.
The adapter honors QWEN_RUNTIME_DIR before QWEN_HOME, with a default of ~/.qwen;
explicit projects roots represent in-process runtime configuration. It never
loads private settings, credentials, a model client or a live harness.

Ordinary JSONL supports native messages, UUID fragments, parent-chain branch
selection, recorded cwd/branch/model and identified tool calls/results. Every raw
record remains a marker. Only native real-user provenance supports searchable
prompts; hook context follows the native display projection. Stored token
counters are preserved with cache-exclusive normalized input. Cache-write and
measured reasoning provenance do not survive native JSONL and remain unknown.

Managed transactional journals are explicitly diagnosed as unsupported. Native
resume execution, parent-session associations, compaction token semantics and
attachment interpretation remain outside the captured contract. Partial tails
preserve the entire prior complete snapshot, including catalog metadata. Selected
hydration defers partial replacements with a retryable source error until complete;
a first partial capture may expose its validated prefix with a diagnostic. Bounded
discovery preserves existing catalog metadata. Malformed records, missing parents, cycles,
missing human provenance and root escapes fail visibly. Complete rewrites retire
normalized evidence no longer on the selected branch. Tied recorded timestamps
keep native chain insertion order, including one-event pagination and complete
block rewrites. Changed local timestamp buckets replay atomically; remote-backed
buckets fail retryably with `FILE_HISTORY_SHARED_ORDER_CONFLICT` and retain the
prior snapshot. Complete discovery derives dates from the selected branch;
first-time bounded/partial activity remains unknown.

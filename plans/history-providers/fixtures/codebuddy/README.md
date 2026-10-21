# Codebuddy Code native history evidence

Pinned official Tencent `@tencent-ai/codebuddy-code@2.171.0`, Node 22.22.2,
macOS arm64. `provenance.json` records npm integrity and the exact bundle hash.
The package was downloaded and selectively extracted, inspected, and run without
installing dependencies. This is the official Tencent CLI, not similarly named
third-party packages.

`ordinary`, `resumed`, `tool`, `spill`, and `error` were emitted by the actual published
CLI against a synthetic 127.0.0.1 OpenAI-compatible SSE server. Every model
variant used that endpoint, a dummy key, and a fresh isolated HOME. Tools were
disabled except the captured Read call on synthetic files. The resumed process
reopened the first process's persisted session. `spill` used the published
`CODEBUDDY_TOOL_RESULT_THRESHOLD_KB=1` option and genuinely wrote a sidecar.
The native full inline `providerData.toolResult.content` equals that sidecar;
`output.text` contains the native pointer/preview. The adapter uses the full
inline value and **never follows paths embedded in transcript text**. Older
sidecar-only results fail explicitly rather than pretending a preview is full.

`fork`, `pending-rewind`, `compact`, `reasoning`, `fragments`, `metadata`, and
`string-content` use the published native
`LocalJsonlSessionStore` recorder (`append`, `appendFactAndMoveLane`); these are
source-native recorder captures, not CLI model-loop captures. Fresh high-level
`SessionStoreImpl.deserializeSessionFromPath` plus native
`HistoryUtils.resolveReplayHistory` supplies the historical oracle. The lower
level `loadLane` context oracle is retained separately: it intentionally drops
pre-compaction context while the historical reader keeps those messages.
The fixture proves discarded branches, a pending rewind, and a compaction root
with a logical parent. Earlier retained historical messages stay searchable.
Native adjacent fragments, opaque reasoning, string contents and native
custom/AI/topic title precedence are captured too. Summary/control text is retained as markers and does not
become a human prompt. All raw inactive records remain markers.

Independent reader: a fresh Node process invokes the published
`SessionStoreImpl.deserializeSessionFromPath` for CLI captures and a separate
high-level historical restore/replay for controls. Native
`isExternalUserPromptItem` supplies the independent human-message identity
oracle, including literal control-tag lookalikes. `read-native.cjs` injects
only logger/factory/restore stubs to avoid application startup; the parser and
history backfill are the package's original code. To expose these internal
exports, `capture.py` replaces only the pinned webpack terminal entry with
`return __webpack_require__(81102)` in a separate file. No writer or reader
implementation is copied or modified.

Reproduce after independently downloading and verifying the pinned npm tarball:

```sh
python3 capture.py /path/to/extracted/package /fresh/output /absolute/path/to/node
python3 sanitize.py /fresh/output /absolute/path/to/node /path/to/extracted/package/dist/codebuddy-fixture-api.cjs
node --test validate.test.mjs
```

The sanitizer changes only IDs, clock offset, synthetic paths, and transient
`_meta` baggage. Links and the first process's resumed IDs remain consistent.
The manifest hashes raw CLI transcripts and every sanitized fixture byte.
`validate.test.mjs` checks fixture copies against saved native-reader oracles,
resume identity, actual spill equivalence and native control outcomes. It does
not use the RelayHistory parser as its oracle.

Default root is `$HOME/.codebuddy/projects`. Standard product
`CODEBUDDY_CONFIG_DIR` replaces `.codebuddy`, with blank values ignored and
relative values frozen against the resolution-time working directory. Tilde
is literal. `ProviderRoots::from_home` ignores environment overrides. Project
folder names are compressed cwd keys, not reversible paths; recorded `cwd`
provides project metadata. Root+project namespaces prevent identical native
session filenames in different projects from collapsing. `history.jsonl` is
an input index, and `sessions/` is runtime metadata; neither is a transcript.

Unverified: other versions/customized distributions, subagent/delegation
relationships, runtime-only lane moves without durable facts. Unknown control
facts fail closed; no agent process runs during history acquisition. Cache and
reasoning counters remain unknown unless the native raw usage reports them;
OpenAI cached input is excluded from normalized input and retained separately.

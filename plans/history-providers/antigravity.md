# Antigravity history acquisition

Tracked by #431, implemented in #498, part of #425. Antigravity replaces the withdrawn Gemini CLI work (#433). The original roster retains 41 rows for traceability, with 40 active agents after this scope change; no Gemini compatibility or alias is planned.

## Acquisition contract

RelayHistory owns the `antigravity` provider. Discover persisted conversations beneath the documented roots:

- App and default SDK: `~/.gemini/antigravity/brain`
- CLI: `~/.gemini/antigravity-cli/brain`
- IDE: `~/.gemini/antigravity-ide/brain`

Each native conversation folder contains `.system_generated/logs/transcript_full.jsonl` and/or `transcript.jsonl`. Prefer the full file, otherwise read the compact file. Do not ingest both. Freeze roots at store creation and namespace conversation IDs by root so variants cannot collide. Explicit provider roots are supported; undocumented environment override precedence is not assumed.

Parse timestamped JSONL steps with native `step_index`, `source`, `type`, `status` and `created_at`. Project recorded user text and completed assistant responses; remove only the validated native outer user envelope. Preserve every row as raw marker evidence, including control steps, reasoning, tool arguments/results and media references. Step-derived message IDs are synthesized, timestamps are recorded, and missing usage/model/cwd/parent information remains unknown. The JSONL omits tool-call IDs, so tool evidence cannot become joined tool-call rows.

Native steps may be rewritten. Acquire an immutable byte snapshot and reconcile complete captures; do not use an append-only cache. Incomplete EOF or compact truncation cannot retire earlier complete evidence. A truncated message remains marker evidence rather than searchable complete text. If the selected full file disappears, discover/sync selects the compact fallback before direct hydration; direct hydration of a missing stored locator returns the existing source error.

The CLI last-conversation cache is a workspace lookup index, not transcript evidence. Do not treat it, opaque backend state or metadata-only discovery as successful history coverage.

## Evidence and acceptance

Five cases come from official SDK 0.1.22 native writer output and its independent reader after process restart: ordinary, tool, resumed, literal tags and long/truncated. The capture uses an isolated HOME and a loopback synthetic model endpoint. See [fixture provenance](fixtures/antigravity/README.md).

Public Rust acceptance covers discover → hydrate → events/search, full preference, compact fallback, root separation, complete rewrites, incomplete/truncated preservation, large first rows, path validation and deletion. Compare normalized messages with the official reader without importing reader-only tool IDs. Disable the message projection to prove the public conformance test fails, then restore it. Verify SDK, CLI and MCP through the same native implementation.

The CLI 1.4.3 binary's documented transcript schema matches these timed fields. Actual App/CLI/IDE writer captures remain separate certification work; SDK fixtures do not establish their writer parity. Older untimed schemas are not covered. Keep #431 open for that remaining certification.

## Work outward

First land the RelayHistory provider and its public evidence contract. Relay Desktop should consume the registered source, discovery and history APIs; it should not parse Antigravity files. Cloud should consume the same normalized evidence and source identity through the existing ingestion contract. Live launch, native resume, authentication, pricing and activity inference are separate capabilities and are not implied by history support.

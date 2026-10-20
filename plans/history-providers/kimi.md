# Kimi Code history acquisition

Current writer: Kimi Code CLI 2.2.0, official tag
`@moonshot-ai/kimi-code@2.2.0`, commit
`472192c34c1efad89a369870eff0da0b708ce9bb` (darwin-arm64).
The archived Python 1.52 product uses a different root/schema and is not covered.

Primary sources: pinned `apps/kimi-code/src/utils/paths.ts`,
`packages/agent-core-v2/src/wire/record.ts`, `wire/wireService.ts`,
`wire/tree/{tree,fork}.ts`, `agent/replayBuilder/fold.ts`, and
`docs/en/configuration/data-locations.md`.
Native source and binary checksums, synthetic writer capture, independent
branch/restart reader, sanitizer and limits are documented in
[fixtures/kimi/README.md](fixtures/kimi/README.md).

Passive acquisition uses current state v2 plus wire v1.5, never launches the CLI,
reads credentials, consults the global session index, or uses resume to discover.
Each agent journal gets a root/bucket/native-session/agent identity. Native
message events dominate overlapping context/stream records. Active branch base
lines retire rewound messages; unsupported unpaired legacy undo is diagnosed.
All original records remain bounded markers. Unknown/unsupported formats retain
prior complete evidence rather than replacing it with successful emptiness.

Scope limits: no Python context/checkpoint acquisition, no old wire migration,
no model-cost inference, no normalized usage accounting, no inferred file edits,
no parent relationship extraction or native resume capability. Exact native tool
call IDs are retained; missing message IDs are explicitly synthesized. Native
subagent wire storage is proven independently of a parent-link capability.

Pinned primary artifacts: [official source](https://github.com/MoonshotAI/kimi-code/tree/472192c34c1efad89a369870eff0da0b708ce9bb) and [official CLI 2.2.0 release](https://github.com/MoonshotAI/kimi-code/releases/tag/%40moonshot-ai%2Fkimi-code%402.2.0).

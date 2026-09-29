# Grok CLI (Grok Build) session fixtures

Grok was **not installed** on the machine these fixtures were written on, so no
real `~/.grok/sessions/**` directory was read. Every shape here reproduces what
public sources describe; which fields are corroborated, which are inferred and
which are guesses is recorded per field in
[`docs/session-catalog.md`](../../../../../docs/session-catalog.md)
("How each adapter works → grok"), together with the `jq` checklist a
maintainer with Grok installed runs to confirm or correct it.

| Layout | What it is for |
|---|---|
| `events-session/` | The documented Grok Build layout: `chat_history.jsonl` (`system`, `user`, `reasoning`, `assistant` + `tool_calls[]`, `tool_result`), `updates.jsonl` as an ACP `session/update` stream with `turnStartMs`, `agentTimestampMs` and `turn_completed` `totalTokens`, plus `summary.json`, `signals.json`, `prompt_context.json`, `subagents/` and `compaction_checkpoints/`. |
| `unified-usage/` | One Grok home with two sessions and the process-wide `logs/unified.jsonl` (#212). `grok-uni-0001` is covered by the log: its rows include two distinct inferences sharing one `eventId`, an exact duplicate row, a row whose model comes from its pid's process-start row, and one with its counters at the top level; its `updates.jsonl` carries `turn_completed.usage` with `modelUsage`, `_meta.modelId` on an update, and one `eventId` reused by two prompts. `grok-uni-0002` is not covered and has no `summary.json`, so its model and start time come from `events.jsonl`. The log also holds a row that names no session and one for a session (`grok-uni-9999`) that is not on disk, which is retained, not attached. The row shape of `unified.jsonl` is inferred — nothing public shows one — see `docs/session-catalog.md`. |
| `full-session/` | The older, Claude-shaped variant: per-record `timestamp` fields, `tool_use` blocks inside `content`, `updates.jsonl` as plain `file_changed` rows. Taken **byte-identically** from [#192](https://github.com/AgentWorkforce/relayhistory/pull/192) so the two branches merge without a conflict. |

`full-session/` also carries a `usage` object with `input_tokens` /
`output_tokens` on its assistant records. RelayHistory deliberately **does not
read it**: [#489 in burn](https://github.com/AgentWorkforce/burn/issues/489)
established that Grok does not log per-turn billing tokens, so those fields are
unverified invention rather than evidence, and the only token fact recorded
here is the `totalTokens` context proxy from `updates.jsonl`. See
[#167](https://github.com/AgentWorkforce/relayhistory/issues/167).

Timestamps in `events-session/` are real epoch values around
`2026-09-16T12:00:00Z`, so a test can assert that a prompt's stored timestamp
is the one `updates.jsonl` recorded rather than anything derived from the
file's mtime or its position in the file.

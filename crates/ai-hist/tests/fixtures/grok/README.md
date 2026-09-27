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

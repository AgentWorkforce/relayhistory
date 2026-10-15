# Codex: read SQLite thread history when the rollout is gone (proposal)

The `codex` source reads `~/.codex/sessions/**/rollout-*.jsonl`,
`archived_sessions/` and the prompts in `history.jsonl`. Codex 0.15x+ also
projects every thread into two SQLite databases, and on the machines surveyed
on 2026-10-15 those databases are the **only** surviving copy of most threads:
rollout files are removed after roughly four to six weeks while the SQLite
history is kept.

| Machine | Threads in state DB | Rollout files on disk | Threads with SQLite history | SQLite-only (no rollout file) |
| --- | --- | --- | --- | --- |
| kjg-lap | 3444 | 252 | 1160 | 975 |
| sf-mini | 5818 | 50 | 548 | 358 |
| finn-mini | 3893 | 106 | 489 | 244 |
| dogpatch-mini (fresh install) | 107 | 107 | 101 | 0 |

For a SQLite-only thread relayhistory today holds the user prompts from
`history.jsonl` and nothing else: no assistant text, tool calls, file changes
or markers. This plan adds the SQLite store as a second layout of the existing
`codex` source. It is not a new source, and it does not touch pricing or
classification.

Status: implemented by the `feat/codex-sqlite-history` PR for issue #502.

## The two databases

Both live in `$CODEX_HOME` (default `~/.codex`), WAL mode, written by
`codex app-server` and the CLI. Versioned file names; read the highest
`state_<N>.sqlite` and `thread_history_<N>.sqlite` present and refuse an
unknown `N` with a diagnostic rather than guessing.

`state_5.sqlite` (schema via `_sqlx_migrations`, 59 applied):

- `threads(id, rollout_path, created_at, updated_at, created_at_ms,
  updated_at_ms, source, thread_source, originator, cli_version, model,
  model_provider, reasoning_effort, cwd, git_sha, git_branch, git_origin_url,
  title, name, first_user_message, preview, tokens_used, history_mode,
  archived, archived_at, agent_nickname, agent_role, agent_path, …)`.
  `history_mode` is `legacy` or `paginated`; only `paginated` threads are
  guaranteed to have item rows. `source` is `cli`, `exec`, `vscode`,
  `unknown` or a JSON `{"subagent": …}` object naming the parent thread.
  `tokens_used` is a cumulative total; there is no per-message usage.
- `thread_spawn_edges(parent_thread_id, child_thread_id, status)` — the
  delegation graph, same facts the rollout `session_meta` carries.
- `thread_attachments(thread_id, attachment_type, identity_key, payload)`.

`thread_history_1.sqlite`:

- `thread_items(thread_id, turn_id, item_id, rollout_ordinal, created_at_ms,
  item_type, item_json, started_at_ms, completed_at_ms)`; unique on
  `(thread_id, rollout_ordinal)`, so `rollout_ordinal` is the read order.
- `thread_turns(thread_id, turn_id, rollout_ordinal, status, error_json,
  started_at, completed_at, duration_ms, first_user_item_id,
  final_agent_item_id, rollout_byte_offset, rollout_end_ordinal, root_turn_id)`.
- `thread_history_projection_state(thread_id, next_rollout_byte_offset,
  next_rollout_ordinal)` — how far the projection has consumed the rollout;
  the natural change fingerprint per thread.
- `thread_realtime_items` — voice session boundaries and transcripts.

Item types observed (kjg-lap counts): `reasoning` 233967, `commandExecution`
182315, `agentMessage` 36776, `fileChange` 18060, `mcpToolCall` 7208,
`subAgentActivity` 6000, `userMessage` 4484, `collabAgentToolCall` 4396,
`contextCompaction` 1608, `sleep` 1384, `webSearch` 803, `imageView` 47,
`hookPrompt` 5. Key sets per type:

| `item_type` | `item_json` keys | Normalize as |
| --- | --- | --- |
| `userMessage` | `id, clientId, content[]` (text/image blocks) | user event + `history` row |
| `agentMessage` | `id, text, phase, delivery, questions, memoryCitation` | assistant event; `phase` (`final_answer`, …) as a fact |
| `commandExecution` | `id, command, cwd, status, exitCode, durationMs, aggregatedOutput, commandActions, source, processId` | tool call `shell` with result and error status |
| `mcpToolCall` | `id, server, tool, arguments, result, error, status, durationMs, readOnlyHint` | tool call `mcp:<server>.<tool>` |
| `fileChange` | `id, changes[], status` | file edits |
| `reasoning` | `id, summary[], content[]` | reasoning event (often empty) |
| `contextCompaction` | `id` | compaction marker |
| `subAgentActivity` | `id, agentThreadId, agentPath, kind` | relationship evidence to the child thread |
| `collabAgentToolCall` | `id, tool, prompt, senderThreadId, receiverThreadIds, agentsStates, model, reasoningEffort` | tool call + relationship |
| `webSearch` | `id, query, action, results` | tool call `web_search` |
| `imageView`, `sleep`, `hookPrompt` | — | markers |
| `thread_turns.error_json` | `message, codexErrorInfo` | turn error marker (e.g. `usageLimitExceeded`) |

Item ids are Codex's native ids (`msg_…`, `rs_…`, `exec-…`, UUIDv7 for user
messages). The rollout parser often uses a line-derived `event_uid`, and a
patch's file-edit id can differ from the SQLite `fileChange` id. Reconciliation
therefore uses the projection's byte offset when a rollout finishes instead
of assuming every ledger key matches across layouts.

## Design

- **Precedence.** The rollout parser stays primary. The SQLite layout is used
  for a thread when its `rollout_path` does not exist on disk, or when the
  rollout exists but is shorter than `next_rollout_byte_offset` says it should
  be. A thread with a complete rollout is never re-read from SQLite, so the
  existing snapshots do not change.
- **Discovery.** Enumerate `threads` where `history_mode = 'paginated'` and
  the rollout is missing. Candidate locator: `sqlite:<state_db>#<thread_id>`.
  Stamp: `(updated_at_ms, next_rollout_ordinal, next_rollout_byte_offset)`.
  Keep `SELECT`s bounded by `rollout_ordinal` ranges; never load a whole
  thread's items for a shallow read.
- **Acquisition.** Per the intake contract: open read-only, pin one read
  transaction, select the rows for the thread, carry them into normalization,
  never copy only the main file, never set `immutable=1` on a live WAL
  database, never change journal mode. Treat a WAL that cannot be read as
  "incomplete capture, retry", not as an empty thread.
- **Identity and catalog.** `session_id = threads.id`; `cwd`, `git_branch`,
  `title`/`name`, `originator`, `cli_version`, `model`, `first_ts` from
  `created_at_ms`, `last_ts` from `updated_at_ms`, `archived` honoured.
  Relationships from `thread_spawn_edges` and the `{"subagent": …}` source,
  with the same cells the rollout path already proves.
- **Usage.** No per-request usage exists; expose `threads.tokens_used` as a
  single cumulative snapshot marker and nothing else. Do not synthesize
  per-message usage.
- **Writes.** Upsert SQLite items by native id. The SQLite provenance marker
  records `next_rollout_byte_offset`; once the rollout reaches that offset,
  retire provisional SQLite events, tools, edits and markers in one
  per-session pass. Relationship rows share the provider child identity and
  remain one row when the rollout records the same delegation.
- **Resume.** `codex resume <id>` still applies.

## Fixtures

`Sqlite` layout fixtures (as `hermes` does): SQL files that create both
databases at `.codex/state_5.sqlite` and `.codex/thread_history_1.sqlite`
with synthetic rows. Cover: a paginated thread with no rollout (every item
type above, one failed command, one turn with `error_json`); a thread whose
rollout exists and must not be read from SQLite; a thread whose rollout is
truncated below the projection offset; a `legacy` thread without items; a
`{"subagent": …}` child joined through `thread_spawn_edges`; an archived
thread. Invent every value.

## Checklist

1. Issue for the gap (title: "Codex: SQLite thread history is the only copy
   of threads whose rollout was pruned"); link the table above.
2. `crates/ai-hist/src/providers/codex/` gains a `sqlite` module; discovery
   and hydration branch on rollout presence. Shared helpers only where the
   intake contract allows; request additions separately.
3. Fixtures and snapshots as above; `codex.json` shard gains the fixtures and
   capture cells; `docs/session-catalog.md` codex row notes the second layout.
4. Changelog `### Added` entry under `[Unreleased - Minor]`; public API
   snapshot only if the surface changes.
5. Negative control: disable the SQLite branch and record the conformance
   failures for a rollout-less fixture.

Negative control result: with SQLite database detection temporarily disabled,
`corpus_snapshots_match_committed_evidence` failed for
`codex/sqlite-archived-thread` (missing history),
`codex/sqlite-pruned-all-items` (missing file edits),
`codex/sqlite-subagent-edge` (missing history), and
`codex/sqlite-truncated-rollout` (missing projected assistant evidence).
The temporary switch was removed after the check.

## Out of scope

Pricing, token estimation, activity classification; any network or account
access; cloud-hosted ("Dot"/Orbit) threads, which have no local content.

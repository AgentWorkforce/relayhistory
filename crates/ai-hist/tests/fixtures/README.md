# Harness fixture corpus

Real coding-agent log shapes, checked in, so every parser change in this repo
can be judged against the same evidence instead of against a fresh inline
string in whichever test the author happened to open.

`tests/fixture_corpus.rs` stages each fixture below into an isolated provider
`HOME`, runs the acquisition path a host actually uses — local sync, shallow
discovery, targeted hydration — and writes a canonical JSON dump of `sessions`,
`session_events`, `tool_calls`, `file_edits`, `session_relationships` and
`history` to `tests/snapshots/<source>/<fixture>.json`.

**The snapshots record current behaviour, gaps included.** They are not a
statement of what relayhistory *should* extract. When a parity issue from
[#160](https://github.com/AgentWorkforce/relayhistory/issues/160) changes a
parser, its PR regenerates the snapshots it moves, and that diff is what a
reviewer reads to see what the change actually did to every log shape we know
about.

## Running it

```sh
cargo test -p ai-hist --all-features --test fixture_corpus
```

Review the behaviour change first, then regenerate:

```sh
UPDATE_SNAPSHOTS=1 cargo test -p ai-hist --all-features --test fixture_corpus
```

Facts the corpus contains but relayhistory does not capture yet are still
written as tests, carrying `#[ignore = "closed by #<issue>"]`. The issue that
closes the gap removes the attribute and regenerates the snapshot.

## Provenance

The fixtures marked **burn** below were copied verbatim from
[`AgentWorkforce/burn`](https://github.com/AgentWorkforce/burn)
(`tests/fixtures/{claude,codex,opencode}`), which is licensed **Apache-2.0**.
File names are unchanged so the two corpora stay diffable; burn's expectations
for them live in `crates/relayburn-sdk/src/reader/{claude,codex,opencode}/tests.rs`
and the raw-fact assertions among them are ported into `fixture_corpus.rs`.
Everything marked **relayhistory** was authored here, for a log shape burn's
corpus does not cover.

## Determinism

Every staged file's mtime is pinned (base `2026-04-20T00:00:00Z`, plus one
second per file in sorted-path order). Discovery change stamps, cursor prompt
timestamps and grok's fallback timestamps are all filesystem-derived, so
without pinning the snapshots would differ on every checkout — and the pinned
ordering is also what makes "which of two transcripts sharing a session id wins
the catalog row" reproducible. Snapshots redact the temp `HOME` prefix and the
epoch component of `source_stamp`; byte lengths inside a stamp survive, because
they are a real fact about the fixture.

## Layouts

| Layout | Staging |
| --- | --- |
| `ClaudeTranscript` | flat `.jsonl` into `~/.claude/projects/corpus/` |
| `CodexRollout` | flat `.jsonl` into `~/.codex/sessions/2026/04/20/`, renamed to the `rollout-` prefix the codex adapter enumerates |
| `HomeTree` | directory copied verbatim into `HOME` — for fixtures whose provider layout is itself the point |
| `OpencodeSqlite` | `.sql` executed into `~/.local/share/opencode/opencode.db` |
| `OpencodeLegacyJson` | burn's older OpenCode JSON layout, copied under `~/.local/share/opencode/` |
| `Reference` | kept for provenance, never staged and never snapshotted |

A fixture may name more than one corpus file. That is deliberate: the
`*-reconciliation` fixtures exist because the quirk only appears *across*
transcripts, so the same `original-session.jsonl` is staged beside a different
partner in each.

## Adding a provider

`docs/session-catalog.md` is the rule; the short version is that every
`SOURCE_CHOICES` entry needs a fixture here and a committed snapshot, or an
exemption in `every_source_choice_has_a_fixture_or_an_exemption`. Adding a
source without deciding which one fails the test.

## The corpus

### `claude`

| Fixture | Origin | Corpus files | Quirk it encodes |
| --- | --- | --- | --- |
| `claude/simple-turn` | burn | `claude/simple-turn.jsonl` | one user turn and one complete assistant turn with full usage, preceded by a `permission-mode` control record |
| `claude/multi-block-turn` | burn | `claude/multi-block-turn.jsonl` | four assistant records share one `message.id` and one `requestId`; only the first carries the usage payload |
| `claude/multi-block-turn-no-request-id` | relayhistory | `claude/multi-block-turn-no-request-id.jsonl` | assistant records omit `requestId` but share one `message.id`, which is the fallback request identity |
| `claude/padded-request-id` | relayhistory | `claude/padded-request-id.jsonl` | two request ids differ only by surrounding whitespace and must remain distinct grouping keys |
| `claude/multi-record-request` | relayhistory | `claude/multi-record-request.jsonl` | two assistant records copy one request's usage, which prompt attribution must charge exactly once |
| `claude/multi-record-bad-copy` | relayhistory | `claude/multi-record-bad-copy.jsonl` | one request has a readable usage copy and an unreadable sibling, so its measurement is disputed |
| `claude/multi-record-broken-sibling` | relayhistory | `claude/multi-record-broken-sibling.jsonl` | one request record has broken ancestry while a sibling establishes both ownership and usage |
| `claude/interleaved-turns` | burn | `claude/interleaved-turns.jsonl` | two assistant messages interleave their blocks instead of arriving contiguously |
| `claude/incomplete-then-complete` | burn | `claude/incomplete-then-complete.jsonl` | a complete assistant message is followed by an in-progress one (`stop_reason: null`) |
| `claude/files-touched` | burn | `claude/files-touched.jsonl` | two Read tool uses and a Grep in one assistant message — three calls, no file mutated |
| `claude/retry-loop` | burn | `claude/retry-loop.jsonl` | the same Bash command is retried four times, every attempt returning `is_error: true` |
| `claude/consecutive-failures` | burn | `claude/consecutive-failures.jsonl` | three different tools fail back to back, each with its own errored tool_result |
| `claude/edit-revert` | burn | `claude/edit-revert.jsonl` | an Edit is applied and then reverted; tool_results carry pre/post file hashes |
| `claude/missing-output-tokens` | burn | `claude/missing-output-tokens.jsonl` | usage carries `input_tokens` only — `output_tokens` is absent, which is not the same as zero |
| `claude/user-turn-blocks` | burn | `claude/user-turn-blocks.jsonl` | user records carrying tool_result blocks of very different sizes, one of them errored |
| `claude/compact-boundary` | burn | `claude/compact-boundary.jsonl` | a `system` record with `subtype: compact_boundary` splits the transcript |
| `claude/sidechain-turn` | burn | `claude/sidechain-turn.jsonl` | every record is `isSidechain: true` — a subagent sidecar, not a session of its own |
| `claude/sidechain-leading-then-main` | burn | `claude/sidechain-leading-then-main.jsonl` | sidechain records precede the first main-chain record in the same file |
| `claude/nested-subagent` | burn | `claude/nested-subagent.jsonl` | a subagent spawns a subagent, in one file, joined by `agentId` |
| `claude/system-subagent-notification` | burn | `claude/system-subagent-notification.jsonl` | a `system`/`subagent_completed` record reports a child session id the transcript never contains |
| `claude/task-notification` | burn | `claude/task-notification.jsonl` | Task tool notifications arrive as their own records |
| `claude/slash-command-triad` | burn | `claude/slash-command-triad.jsonl` | a slash command expands into a `<command-name>`/`<command-message>`/`<command-args>` triad of user records |
| `claude/replacement-meta` | burn | `claude/replacement-meta.jsonl` | edit metadata only reaches the log on the tool_result, not on the tool_use |
| `claude/oversized-bash-output` | burn | `claude/oversized-bash-output.jsonl` | an 80 KB Bash tool_result — the byte size is the fact, and no record is pretty-printed |
| `claude/resume-marker` | burn | `claude/resume-marker.jsonl` | the first user record is a `/resume <sessionId>` marker naming the prior session |
| `claude/parent-chain-out-of-order` | burn | `claude/parent-chain-out-of-order.jsonl` | records arrive out of `parentUuid` order, so turn grouping cannot rely on file order |
| `claude/parent-chain-interrupt-resume` | burn | `claude/parent-chain-interrupt-resume.jsonl` | an interrupted turn is resumed, and both halves hang off the same parent record |
| `claude/original-session` | burn | `claude/original-session.jsonl` | the root transcript that the fork and continuation fixtures point back at |
| `claude/cross-file-parent-reconciliation` | burn | `claude/original-session.jsonl`<br>`claude/cross-file-parent.jsonl` | a continuation whose first record's `parentUuid` only exists in the other file — the link is cross-file, not in-file |
| `claude/explicit-continuation-reconciliation` | burn | `claude/original-session.jsonl`<br>`claude/explicit-line-relationships.jsonl` | a continuation that states `continuedFromSessionId` on the record itself |
| `claude/fork-reconciliation` | burn | `claude/original-session.jsonl`<br>`claude/fork-branch-a.jsonl`<br>`claude/fork-branch-b.jsonl` | two transcripts share one source session id: a fork, not a continuation |
| `claude/settings-reference` | burn | `claude/settings/oversized-bash-output-length.json` | burn's Claude settings input that sets the Bash output cap; relayhistory reads no settings file, so it is kept for provenance only |
| `claude/summary-record` | relayhistory | `claude/summary-record.jsonl` | a `type: "summary"` record with a `leafUuid` between two ordinary turns |
| `claude/sidecar-subagent` | relayhistory | `claude/sidecar-subagent` | a subagent transcript in `<sessionId>/subagents/agent-<id>.jsonl` with its `agent-<id>.meta.json` sidecar, carrying the PARENT's sessionId |

### `codex`

| Fixture | Origin | Corpus files | Quirk it encodes |
| --- | --- | --- | --- |
| `codex/simple-turn` | burn | `codex/simple-turn.jsonl` | one turn: `session_meta`, `turn_context`, `task_started`, a null `token_count`, a populated one, `task_complete` |
| `codex/multi-turn` | burn | `codex/multi-turn.jsonl` | two turns in one rollout, each with its own cumulative `total_token_usage` |
| `codex/with-tool-call` | burn | `codex/with-tool-call.jsonl` | function calls and their outputs as `response_item` records |
| `codex/with-spawn-agent` | burn | `codex/with-spawn-agent.jsonl` | a `spawn_agent` function call: delegation stated in the tool call, not in session metadata |
| `codex/compaction` | burn | `codex/compaction.jsonl` | a `compacted` record with `replacement_history`, followed by `context_compacted` and a fresh turn |
| `codex/session-meta-relationships` | burn | `codex/session-meta-relationships.jsonl` | `sourceSessionId` / `forkSessionId` / `continuedFromSessionId` on a repeated `session_meta` |
| `codex/user-turn-blocks` | burn | `codex/user-turn-blocks.jsonl` | user input arriving as `response_item` message blocks rather than `event_msg` |
| `codex/oversized-shell-output` | burn | `codex/oversized-shell-output.jsonl` | an 80 KB shell function-call output |
| `codex/parent-thread-id` | relayhistory | `codex/parent-thread-id` | a subagent rollout naming its root through `parent_thread_id` plus `thread_source: subagent` |
| `codex/archived-session` | relayhistory | `codex/archived-session` | a rollout under `~/.codex/archived_sessions/`, the second codex discovery root |
| `codex/counter-regressed` | relayhistory | `codex/counter-regressed.jsonl` | a cached-input counter grows faster than total input, making the cumulative delta unnormalizable |
| `codex/counter-negative` | relayhistory | `codex/counter-negative.jsonl` | a cumulative snapshot contains a negative input counter that must be preserved and refused |
| `codex/counter-fractional` | relayhistory | `codex/counter-fractional.jsonl` | a cumulative snapshot contains a fractional output counter that must be preserved and refused |
| `codex/counter-above-i64` | relayhistory | `codex/counter-above-i64.jsonl` | a valid cumulative counter exceeds `i64::MAX` and must survive parsing as a `u64` |
| `codex/counter-recovers` | relayhistory | `codex/counter-recovers.jsonl` | an unreadable cumulative snapshot is superseded by a later valid snapshot for the same waiting turn |
| `codex/counter-unusable-then-new-turn` | relayhistory | `codex/counter-unusable-then-new-turn.jsonl` | a later advancing delta covers an earlier unreadable snapshot and clears only refusals in that span |
| `codex/reasoning-then-unreadable` | relayhistory | `codex/reasoning-then-unreadable.jsonl` | a reasoning row waits through an unreadable snapshot before a valid delta measures the turn |
| `codex/refusal-across-baseline-reinstall` | relayhistory | `codex/refusal-across-baseline-reinstall.jsonl` | a refusal predating a cumulative-baseline reinstall must not be cleared by a later delta |
| `codex/refusal-overwritten-after-reinstall` | relayhistory | `codex/refusal-overwritten-after-reinstall.jsonl` | two refusals for one turn straddle a baseline reinstall and remain tied to their own generations |
| `codex/resume-baseline-corrupt` | relayhistory | `codex/resume-baseline-corrupt.jsonl` | a resumed rollout starts with an unreadable baseline, so carried-over totals cannot become one request's delta |
| `codex/two-unreadable-turns` | relayhistory | `codex/two-unreadable-turns.jsonl` | two unrecovered turns each retain their own unreadable usage refusal |
| `codex/one-request-three-rows` | relayhistory | `codex/one-request-three-rows.jsonl` | one API call written as reasoning, a tool call and a message is one request, not three |
| `codex/recovered-span-covers-two-turns` | relayhistory | `codex/recovered-span-covers-two-turns.jsonl` | a readable snapshot recovers a span an unreadable one left open, so its delta measures both turns as one request |
| `codex/two-requests-one-turn` | relayhistory | `codex/two-requests-one-turn.jsonl` | a tool loop makes two API calls inside one turn_id, so the turn is not the request |

### `cursor`

| Fixture | Origin | Corpus files | Quirk it encodes |
| --- | --- | --- | --- |
| `cursor/prompt-transcript` | relayhistory | `cursor/prompt-transcript` | `agent-transcripts/<id>/<id>.jsonl` with string, block-array and `<user_query>`-wrapped prompts, assistant text, a tool_use and a tool_result; the provider records no timestamps |
| `cursor/observed-3-13-25` | relayhistory | `cursor/observed-3.13.25.jsonl` | the reported Cursor IDE 3.13.25 record shape: id-less `tool_use`, a `turn_ended` marker and `<timestamp>`/`<user_query>` framing; staged by the ai-hist cursor parser tests rather than by this harness |
| `cursor/legacy-string-content` | relayhistory | `cursor/legacy-string-content.jsonl` | the older row shape, `message.content` as a bare string with no framing; staged by the ai-hist cursor parser tests rather than by this harness |
| `cursor/extended-unverified` | relayhistory | `cursor/extended-unverified.jsonl` | an **unverified** build that also writes `message.model`, `message.usage`, `thinking` blocks, `tool_use` ids and `tool_result` blocks; it proves the parser records them when present, not that Cursor writes them |

### `grok`

| Fixture | Origin | Corpus files | Quirk it encodes |
| --- | --- | --- | --- |
| `grok/full-session` | relayhistory | `grok/full-session` | older Claude-shaped grok directory: per-record timestamps, `tool_use` blocks in `content`, `updates.jsonl` as `file_changed` rows, plus `prompt_context.json`, `signals.json` and `subagents/` |
| `grok/events-session` | relayhistory | `grok/events-session` | documented Grok Build layout: `chat_history.jsonl` with `tool_calls[]`, ACP `updates.jsonl` with real `agentTimestampMs` times, `compaction_checkpoints/`, `subagents/`, `signals.json` and `prompt_context.json` |

### `opencode`

| Fixture | Origin | Corpus files | Quirk it encodes |
| --- | --- | --- | --- |
| `opencode/sqlite-store` | relayhistory | `opencode/sqlite-store.sql` | the current SQLite store: two sessions (one a child through `parent_id`), text/tool/step-finish parts, provider+model and token payloads on the assistant messages |
| `opencode/legacy-json-simple` | burn | `opencode/legacy-json-simple` | legacy `storage/{session,message,part}` JSON layout: one session, three parts |
| `opencode/legacy-json-multi-turn` | burn | `opencode/legacy-json-multi-turn` | legacy layout with a `ses_child` session carrying `parentID` |
| `opencode/legacy-json-with-tool` | burn | `opencode/legacy-json-with-tool` | legacy layout with a `tool` part and its completed state |
| `opencode/legacy-json-with-compaction` | burn | `opencode/legacy-json-with-compaction` | legacy layout with a summarized/compacted message |
| `opencode/legacy-json-user-turn-blocks` | burn | `opencode/legacy-json-user-turn-blocks` | legacy layout with several tool parts of different sizes, one errored |

### `devin`

| Fixture | Origin | Corpus files | Quirk it encodes |
| --- | --- | --- | --- |
| `devin/sqlite-store` | relayhistory | `devin/sqlite-store.sql`<br>`devin/transcripts/fixture-devin-session.json` | the SQLite store: epoch-second timestamps, `chat_message` JSON per node, ACP `tool_call_state` with a completed read and a failed edit, a `summarized_from` node marker and a transcript `agent` envelope |
| `devin/malformed-and-hidden` | relayhistory | `devin/malformed-and-hidden.sql` | a `chat_message` that is not JSON is skipped per record, an in-progress tool call stays `running`, an orphan `tool_call_state` row is still indexed, and `hidden` sessions are excluded entirely |

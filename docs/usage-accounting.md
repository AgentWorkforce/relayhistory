# Usage accounting

RelayHistory stores what each provider wrote about token usage, and normalizes
it into one shape so a consumer does not have to know each provider's
conventions. This document is the contract for that normalization.

Two rules govern everything below.

**Cost is never estimated here.** `reportedCostUsd` is populated only when the
source data itself carried a cost. No price table, no model lookup, no
per-token arithmetic. Pricing is burn's job; RelayHistory's job is to say what
happened. Likewise `providerTotalTokens` is the total the provider wrote and is
never recomputed from the parts — when it disagrees with the sum, that
disagreement is a fact worth seeing, not a rounding error to hide.

**Nothing is silently clamped.** A counter that is negative, fractional,
non-finite, or larger than `u64` is a normalization *error* with a stable code,
not a zero. A clamped counter produces a well-formed number that is wrong, and
nothing downstream can tell it apart from a real measurement.

## The shape

`NormalizedUsage` (Rust `ai_hist::NormalizedUsage`, TypeScript
`NormalizedUsage`):

| Field | Meaning |
| --- | --- |
| `inputTokens` | Ordinary input, **always excluding cache reads** |
| `outputTokens` | Output tokens |
| `reasoningTokens` | Reasoning tokens, or `null` when the provider does not report them separately |
| `cacheReadTokens` | Tokens served from cache |
| `cacheWriteTokens` | Total cache-write tokens across every TTL bucket |
| `cacheWrite5mTokens` / `cacheWrite1hTokens` | Anthropic's `cache_creation.ephemeral_*` split, or `null` when the provider does not distinguish TTLs |
| `providerTotalTokens` | As reported; never recomputed |
| `reportedCostUsd` | Only when the source data carried a cost |
| `accounting` | What one record stands for — see below |
| `has*Tokens` | Coverage: whether the provider actually wrote that counter |

The `5m`/`1h` split is kept rather than collapsed because the two are priced
differently. `null` there means "the provider did not split it", which is a
different fact from "it split it and this bucket is zero".

## Accounting modes

`accounting` says what one stored record stands for, which is what tells a
consumer whether summing records is meaningful.

| Mode | Sources | What one record is |
| --- | --- | --- |
| `per-request` | (none yet) | One API request, reported once |
| `per-message` | `claude` | One assistant message, **copied onto every content block of that message** |
| `cumulative-delta` | `codex` | A cumulative counter differenced into a per-request delta at parse time |
| `context-proxy` | (none yet) | Context-window occupancy, not a billed request — never sum |

A session summary may report several modes. When it does, its totals mix units
and should be read per mode rather than as one number.

`cursor`, `grok`, `relay`, `trajectory` and `opencode` record no usage this
crate can normalize; asking for one is `USAGE_UNKNOWN_SOURCE` rather than a
zero.

### Claude

`message.usage` is written verbatim into `token_json`. `input_tokens` already
excludes cache reads and writes, so it is carried through unchanged —
subtracting them again would under-report ordinary input.

### Codex

`token_count` events carry cumulative `total_token_usage` snapshots. The parser
differences consecutive strictly-advancing snapshots into per-request deltas and
attaches each to the nearest assistant event, so summing a session's deltas
reproduces its final cumulative total. A snapshot that repeats or goes backwards
is not a delta: the prior baseline is kept, so the next advancing snapshot
covers exactly the spend since it.

`input_tokens` in a Codex record stays *inclusive* of `cached_input_tokens`
even after differencing. Normalization makes it exclusive, so a consumer that
sums the categories cannot count cache reads twice.

## The dedup rule

Claude copies one request's usage onto every content block of the message, and
writes one JSONL record per block. Counting rows therefore multiplies one API
call by the number of records it was split across.

`session_requests` is the grouping that prevents this. It is a **view** over
`session_events`, one row per `(source, session_id, request_key)`:

- `request_key` is the provider's `request_id` when `session_events` carries
  that column, and the event `message_id` otherwise. `requestKeySource` says
  which.
- Only assistant rows with a message id participate. A user turn is not a
  request.
- `usage_variants` counts the distinct non-null `token_json` blobs in the
  group. The expected value is 1. Anything higher means the copies disagree,
  which does not establish what the request cost, so the request reports no
  usage and carries the `ambiguous-usage-copies` diagnostic.

Because it is a view rather than a materialized table, it cannot drift from the
events it is derived from, and there is exactly one implementation of the
normalization rules. The view is rebuilt whenever `session_events` gains or
loses the `request_id` column, and a view built for a different column set
counts as outstanding migration work — otherwise an upgraded store would keep
over-counting, silently and plausibly.

Until `request_id` is captured, `message_id` holds Claude's per-record `uuid`,
so a turn split across records reads as one request per record. That is
recorded in the test suite as a characterization rather than hidden.

## Missing counters

An absent counter is **not** a zero.

A request that reports `input_tokens` but no `output_tokens` normalizes to
`outputTokens: 0` with `hasOutputTokens: false`. The number is zero because
there is nothing else to put there; the coverage flag is the part that says the
provider never reported it. A session summary merges coverage across its
requests, so `hasOutputTokens: false` on a summary means *no* request in the
session reported an output count.

A session with no usage evidence at all returns `null` — `usage` on
`getSessionUsage`, `None` from `session_usage_summary` — rather than a
zero-filled record. Zero is a claim.

## Error codes

`normalize_usage` returns these under `UsageError::code()`; a request whose
usage failed to normalize carries the code in `usageError` and the
`unnormalizable-usage` diagnostic.

| Code | Condition |
| --- | --- |
| `USAGE_MALFORMED` | The stored blob is not valid JSON |
| `USAGE_NOT_AN_OBJECT` | It parsed but is not a JSON object |
| `USAGE_UNKNOWN_SOURCE` | No accounting rule for that source |
| `USAGE_NON_INTEGER_COUNTER` | A counter was negative, fractional, non-finite, or out of range |
| `USAGE_COUNTER_REGRESSED` | Codex `cached_input_tokens` exceeded `input_tokens`, so cache-exclusive input would be negative |
| `USAGE_COUNTER_OVERFLOW` | Two reported counters could not be combined within `u64` |
| `USAGE_INVALID_COST` | A reported cost was negative or non-finite |

## Reading it

Rust:

```rust
use ai_hist::{session_requests_page, session_usage_summary};

let summary = session_usage_summary(&conn, "claude", session_id)?;
let page = session_requests_page(&conn, "claude", session_id, 200, None)?;
```

TypeScript:

```ts
import { getSessionUsage, getSessionRequestsPage, sessionRequests } from 'ai-hist';

const summary = await getSessionUsage('claude', sessionId);
for await (const request of sessionRequests('claude', sessionId)) { /* ... */ }
```

MCP: `get_session_usage` and `get_session_requests`.

Pages keyset on `(firstTsMs, id)`. The `id` tiebreak is load-bearing: requests
inside one session routinely share a timestamp, and ordering on the timestamp
alone leaves their order undefined between calls, so a paged walk silently drops
or duplicates rows at the boundary.

`session_usage_summary` streams the view into a fixed-size accumulator rather
than loading a session's events, so a hundred-thousand-event session costs what
a ten-event one does.

## Prompt attribution

`attribute_usage_to_prompts` charges each assistant response to the prompt that
caused it, for consumers that need usage per history row rather than per
request. It walks `parent_id` to the owning user message, and for Codex — which
persists no parent ids — uses the ordered human-turn stream.

Its defining property is that it **refuses**. Broken or cyclic ancestry, a tie
at a turn boundary, two prompts with identical text and timestamp, a message
whose rows disagree about their own timestamp or parent: each of these means the
evidence does not establish ownership, and the response contributes nothing
rather than being charged to a plausible neighbour. A missing number is
recoverable; a confident wrong one is not.

It still groups on `message_id`, so it inherits the same per-record over-count
described above until `request_id` lands.

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
covers exactly the spend since it. A snapshot that cannot be read at all is
treated the same way — see below.

Counters are read as non-negative integers and differenced with checked
arithmetic. A snapshot carrying a counter that is negative, fractional, or
outside `u64` cannot be differenced at all, so the provider's own object is
stored verbatim in its place and the baseline is left alone — the request then
reports `USAGE_NON_INTEGER_COUNTER` rather than a delta of zeros that is
indistinguishable from a reported zero. A counter above `i64::MAX` is still a
valid count and is preserved; it is refused later, at the JavaScript boundary
that genuinely cannot carry it.

### When an unreadable snapshot refuses a turn

An unreadable snapshot is a transient glitch, like a regressed one. It decides
nothing on arrival: it is **recorded**, against the turn that was waiting for a
measurement and the baseline in force at that moment. Baselines are numbered,
and the number changes every time `prev_totals` is replaced. Nothing is ever
rewritten or removed while the rollout is read.

> **The invariant.** A turn keeps a refusal exactly when no measured delta was
> differenced from the baseline generation that refusal was recorded under.

Everything else follows from it, and `ingest.rs::surviving_refusals` is the one
place that applies it — a reviewer has a single function to check.

- An unreadable snapshot does not advance the baseline, so a delta differenced
  from generation *g* spans every refusal recorded under *g*. Those turns are
  owed nothing: their spend is reported inside that delta's request. This is
  what stops a glitch arriving while `agent_reasoning` holds the waiting slot
  from flagging a turn its own `agent_message` was measured for.
- A baseline **reinstall** advances the baseline without measuring anything,
  absorbing every earlier span into itself, so no later delta can account for a
  refusal recorded under an older generation. Those survive.
- A turn refused more than once keeps the earliest surviving refusal — the
  first thing that went wrong for it. A later refusal never erases an earlier
  one, which holds only because the record is append-only.

Refusals are applied at the end of the rollout, each to the turn it was
recorded against. The store therefore says three distinct things, and they are
different answers:

| the turn's `token_json` | what it means |
| --- | --- |
| a delta | the measurement for that request |
| absent | the spend was folded into a later request |
| the provider's own unreadable object | the figure was rejected, and nothing recovered it |

If the *first* snapshot is unreadable there is no baseline at all, so the next
readable one installs one and measures nothing. Differencing it from zero would
report a resumed session's whole carried-over total as a single request's
spend.

This rule replaced four successive attempts to decide refusals while reading
the rollout — attach on arrival, one slot per rollout, clear every held refusal
on a delta, clear by generation. Each was a correct response to the defect
before it and introduced the next, because the rule lived in three places and
nowhere in full. Record the facts, decide once.

`input_tokens` in a Codex record stays *inclusive* of `cached_input_tokens`
even after differencing. Normalization makes it exclusive, so a consumer that
sums the categories cannot count cache reads twice.

## The dedup rule

Claude copies one request's usage onto every content block of the message, and
writes one JSONL record per block. Counting rows therefore multiplies one API
call by the number of records it was split across.

`session_requests` is the grouping that prevents this. It is a **view** over
`session_events`, one row per `(source, session_id, request_key)`:

- `request_key` is the provider's own identity for the API call: `request_id`
  (Claude's `requestId`) first, then `provider_message_id` (`message.id`), then
  `request_span` for a provider that ends a call without naming it, and the
  event's `message_id` only as a last resort. `requestKeySource` says which of
  the four was used.
- **`request-span` is for a provider that delimits its requests instead of
  naming them.** Codex records no request id and no message id, but it reports
  a cumulative `token_count` after each API call, so one snapshot ends one
  call and every assistant row since the previous snapshot belongs to it —
  `agent_reasoning`, each `function_call`, then `agent_message`. The parser
  numbers those spans per session as it reads the rollout, because the
  boundary is knowable only in order. Keyed on the record id instead, each of
  those rows became a request of its own and a session reported several
  requests, most carrying no usage, for one API call.

  The span is deliberately **not** the turn. A Codex turn runs a tool loop and
  holds as many API calls as it made round trips, each with its own snapshot.
  Grouping by `turn_id` would merge calls with different measurements into one
  request, which is `ambiguous-usage-copies` — the session would report no
  usage where it now reports a correct total. The boundary is the snapshot.

  A span is closed by **every** snapshot the provider reported, whether or not
  it could be differenced. Two turns whose snapshots were both unreadable are
  two refused requests; folding them into one span would merge their refusals
  into a single request holding two disagreeing blobs, reported as ambiguous
  rather than as two rejections. What a span *cost* is a separate question,
  settled by the refusal rule above.
- The key is **namespace-qualified** — `request-id:req_1`, not `req_1`. Those
  three namespaces are separate and can carry the same text, and a bare value
  merged one call whose `request_id` was `msg_1` with an older call whose
  `provider_message_id` was `msg_1` into a single request with one usage blob
  standing for two. Qualifying keeps the key unique on its own;
  `requestKeySource` still names the namespace without parsing it.
- `messageIds` is a list, carried across the boundary as a JSON array. A
  provider id may contain a comma, so it is never joined into one string and
  split back.
- `message_id` is **not** a request identity. It holds the JSONL record's own
  `uuid`, and one Claude request is written as several records with different
  uuids. A request keyed on it, from a source that spreads requests across
  records, carries the `unresolved-request-identity` diagnostic — and its
  session rollup reports no totals at all rather than a figure that is one per
  content block.
- Only assistant rows with a message id participate. A user turn is not a
  request.
- `usage_variants` counts the distinct non-null `token_json` blobs in the
  group. The expected value is 1. Anything higher means the copies disagree,
  which does not establish what the request cost, so the request reports no
  usage and carries the `ambiguous-usage-copies` diagnostic.

Because it is a view rather than a materialized table, it cannot drift from the
events it is derived from, and there is exactly one implementation of the
normalization rules. A view left over from a build whose grouping key differed
counts as outstanding migration work — otherwise an upgraded store would keep
over-counting, silently and plausibly.

Identities are stored **verbatim**. `"req"` and `" req "` are two requests,
not one: trimming on the way in would merge them and report a total belonging
to neither. A value that is empty once trimmed is not an identity and is
stored as absent.

They travel with the evidence contract too, so a remotely hydrated session
keeps the grouping its records had rather than arriving with null identities
and reading as one request per content block.

### Upgrading an existing store

`request_id` and `provider_message_id` are added by migration, but their
*values* live only in the transcripts. Rows already indexed keep them null
until their transcript is re-parsed, and `ai-hist sync` does not re-read a
transcript whose bytes have not changed. `HYDRATION_PARSER_VERSION` is bumped
so an explicit hydrate does re-parse; until one runs, those Claude sessions
report their requests and the `unresolved-request-identity` diagnostic with no
totals. A gap is recoverable. A multiplied total presented as a measurement is
not.

## Missing counters

An absent counter is **not** a zero.

A request that reports `input_tokens` but no `output_tokens` normalizes to
`outputTokens: 0` with `hasOutputTokens: false`. The number is zero because
there is nothing else to put there; the coverage flag is the part that says the
provider never reported it. A session summary merges coverage across its
requests, so `hasOutputTokens: false` on a summary means *no* request in the
session reported an output count.

`usage` is `null` — on `getSessionUsage`, and on the Rust
`SessionUsageSummary` — whenever the totals are not established: no request
carried usage, every request's usage was rejected, the totals overflowed, or
the requests are not known to be one per API call. Zero is a claim, so nothing
is zero-filled.

The **summary itself** is still returned in all of those cases, carrying the
request counts, models, timestamps and `diagnostics`. Only a session nothing
was ever recorded for answers with nothing at all. A session whose usage is
unreadable and a session that does not exist are different answers, and
collapsing them would leave a caller unable to tell corrupt evidence from
absent evidence.

### Aggregation is all-or-nothing per field

An optional field is reported on a total only when **every** contributing
request reported it. Folding an unreported `None` in as a zero is how a sum of
one split cache-write record and one unsplit one ends up describing the whole
session as split: the unsplit tokens vanish from the TTL buckets that are
priced, and the result looks complete. So a partial split reports `null` plus
`partial-cache-write-split`, and a partial cost reports `null` plus
`partial-reported-cost`. The one exception is a record that wrote no cache
tokens at all — its split is not unreported, it is known to be zero, and it
must not erase a real one.

### Counts on the JavaScript boundary

Every count the SDK returns is a safe integer. Core accepts counters up to
`u64::MAX`; a value above `Number.MAX_SAFE_INTEGER` cannot cross a `number`
intact, so the native boundary refuses it — `usage: null`, `usageError:
"USAGE_COUNT_NOT_REPRESENTABLE"`, and the `count-not-representable`
diagnostic — instead of saturating to something that still looks like a
measurement.

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
| `USAGE_COUNT_NOT_REPRESENTABLE` | A count exceeded `Number.MAX_SAFE_INTEGER` at the JavaScript boundary |

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

Ownership is resolved per **record**, because that is where the parent links
are, but the measurement is added once per **request**, under the same identity
rule `session_requests` groups by. One Claude API response is written as several
records with distinct uuids and a full copy of `message.usage` on each, so
folding the records charged a prompt its own cost multiplied by the response's
block count. When the records of one request disagree — on the measurement, or
on the prompt they resolve to — the request contributes nothing.

A record can be silent or contradictory, and the two are not the same.
A record with no usage, or with a broken ancestry, says nothing: Claude chains
a request's records through each other and the parser stores no row for an
empty block, so a mid-chain gap is ordinary and a sibling may still establish
the request. A record whose usage is present but unreadable, or that names an
owner the evidence cannot pin down, *contradicts*: the request's cost or owner
is in dispute and a sibling whose copy happens to parse is not the tie-breaker.

The identity rule exists twice, once in Rust (`usage::request_key`) and once in
the view's SQL. They are pinned against each other by a test, because drift
between them is exactly how one API request came to be counted once by the
session rollup and once per record here.

# Session Catalog — shallow coding-agent session discovery

**Answer "which coding-agent sessions have been found, newest first?" in
milliseconds, without indexing a single transcript.**

`ai-hist sync` builds the deep index: every message, tool call, file edit and
full-text row. That is the right thing for search, and the wrong thing for a
session picker — it reads every transcript end to end. The **session catalog**
is the shallow half: it enumerates provider sessions cheaply, extracts only the
metadata needed to *identify* a session, and caches the result in the `sessions`
table so the next listing touches no provider file at all.

The two layers coexist. Discovery never blocks or downgrades a full sync, and a
fully indexed session keeps its richer state when discovery re-reads it.

The catalog is one ledger, not separate local and remote catalogs. A session
may have a `local` presence, a `remote` presence, or both. Its presences are
stored in `session_presences`, while callers still receive one session row.
Collection commands select a location with exactly one of `--local`,
`--remote`, or `--all`; omitting a scope flag means `--local`. `--all` is the
deduplicated union, so the same `(source, session_id)` is never returned twice
just because it has both presences. Combining scope flags is an invalid
argument; provider `--source` filters remain orthogonal to location scope.

---

## The two operations

After those two catalog operations, targeted hydration acquires the richest
available evidence for one selected identity:

```ts
const sessions = await listSessionCatalog({ limit: 100 });
await hydrateSession({ source: sessions[0].source, sessionId: sessions[0].sessionId });
```

Hydration requires the catalog row and never invokes discovery or global sync.
It upgrades `discoveryState` to `full` when the acquisition it was asked for
was indexed through its recorded source stamp. `full` is therefore a statement
about discovery being complete for that request, not that a live coding
session has ended, and not that every evidence kind exists: kinds the request
never acquired -- including ones declined by an option such as
`includeRelated: false` -- are described by `coverage` and `capability` below,
which is where a consumer looks to find out what was left out. A remote
connector that could not return its evidence at all stays `shallow` and reports
its capability explicitly. File providers validate the saved locator against
the expected provider root; OpenCode uses session-keyed queries against its
live read-only database.

`capability` and `coverage` answer a different question from `discoveryState`,
and hydration contract 3 made the local path compute both rather than assert
them. `coverage` lists the evidence kinds the selected provider's parser can
produce; `capability` is `full` only when that list contains every kind in
`FULL_SESSION_KINDS` (`history`, `session_event`, `tool_call`, `file_edit`,
`relationship`), and `partial` otherwise, with a `HYDRATION_PARTIAL_COVERAGE`
diagnostic naming the missing ones. A zero count for a *covered* kind means
this session has none of it; a kind absent from `coverage` means nothing
looked. So a completed Cursor hydration reports:

```json
{
  "capability": "partial",
  "coverage": ["history"],
  "diagnostics": [{ "code": "HYDRATION_PARTIAL_COVERAGE", "message": "cursor evidence covers history; this hydration produces no session_event, tool_call, file_edit, relationship" }]
}
```

Coverage is narrowed by the request as well as by the provider.
`includeRelated: false` asks for the selected thread alone, and hydration
honours that literally -- Claude subagent sidecars are not walked and Codex
child rollouts are not read -- so `relationship` drops out of `coverage` and a
Claude or Codex session hydrated that way reports `partial`. That is the
difference between "this session has no delegation" and "nothing looked"; the
diagnostic says which by naming `include_related`.

Codex child acquisition reads the selected session's date directory and the
following date in both the active and archived rollout stores, which finds
children spawned across midnight or moved between stores without making an
old session scan years of newer rollouts. If later date directories exist in
either store, the
bounded search may miss delayed descendants; the result omits `relationship`
from `coverage`, reports `partial`, and includes
`HYDRATION_BOUNDED_RELATIONSHIPS`. A full sync can index relationships across
the archive.

A source plugin declares its own `coverage` the same way, and the same
distinction applies to it: `covered_kinds` names what the acquisition
*examined*, so a complete export of a session that has no file edits still
covers `file_edit` and reports `full`. A connector also receives
`includeRelated` and must omit `relationship` from both its coverage and its
records when it is `false` -- otherwise a `scope: 'all'` merge would union the
kind back in and report `full` despite the opt-out.

`discoveryState` stays `full` for that row: it records that the session was
indexed through its recorded source stamp, which is what the unchanged
short-circuit reads. A parser upgrade still forces a re-parse, because the
stored `parser_version` is checked independently of `discoveryState`.

Codex child rollouts, and Claude subagent transcripts whose records carry a
per-child `agentId`, retain their provider-native IDs and are linked through
`session_relationships`: their events are indexed under the child's own
session id rather than flattened into the parent, and delegated task prompts
are not stored as human prompt history. Claude evidence from a provider
version that does not name the child is recorded as an unlinked relationship —
never as a synthesized identity — and its output stays attributed to the
parent. The hydration stamp covers every file that evidence came from — the
selected transcript, each subagent transcript, and the
`agent-<agentId>.meta.json` describing it — so a metadata sidecar that arrives
or changes on its own still re-hydrates the session. Set
`includeRelated: false` or use CLI `--no-related` to acquire only the selected
thread.

| | `ai-hist sessions list` | `ai-hist sessions discover` |
|---|---|---|
| Reads | the cached ledger only | configured provider locations, with bounded reads |
| Provider I/O | none | head/tail of the newest candidates |
| Writes | nothing (read-only handle when the schema is current) | upserts `sessions` rows |
| Output | one JSON object | JSONL session, diagnostic, and summary records |
| Use it for | every repaint of a session picker | refreshing the catalog |

Use **`list`** whenever you are rendering: it is a single indexed query, it
cannot contend with a running `sync`, and it still works when the provider
files have been deleted.

Use **`discover`** when the catalog may be stale — on app launch, on a manual
refresh, or on a timer. It is safe to run beside `ai-hist sync`.

```bash
# Refresh from local provider locations (the default).
ai-hist sessions discover
ai-hist sessions discover --local

# Run every configured discovery adapter: local adapters plus any
# configured remote connectors (see remote-connectors.md).
ai-hist sessions discover --all --limit 20

# Read back what the catalog holds — no provider file is opened.
ai-hist sessions list
ai-hist sessions list --remote --limit 100 --source codex --source claude
ai-hist sessions list --all --limit 100

# Page backwards by recency (keyset, not OFFSET).
ai-hist sessions list --limit 50 --before-ms 1781949900000

# Machine-readable forms.
ai-hist sessions list --json          # one JSON object
ai-hist sessions discover --json      # JSONL: sessions, diagnostics, summary
```

---

## The output contract

Both operations carry `contract_version` — currently **4**
(`SESSION_CATALOG_CONTRACT_VERSION`). It is bumped whenever the shape or the
meaning of a row changes in a way a consumer must notice, so parse it and fail
loudly on a version you do not know rather than guessing.

### `sessions list --json`

One object, never a bare array, so the version travels with the payload:

```jsonc
{
  "contract_version": 4,
  "scope": "local",
  "sessions": [
    {
      "source": "codex",
      "session_id": "0198c2ad-codex",
      "cwd": "/Users/you/Projects/api",
      "git_branch": "feature/retries",
      "first_activity_ms": 1782039600000,
      "last_activity_ms": 1782039603000,
      "first_prompt": "make the backoff configurable",
      "last_assistant_text": null,
      "models": ["gpt-5-codex"],
      "originator": "codex_cli_rs",
      "agent_version": "0.148.0",
      "repo_url": "git@github.com:acme/api.git",
      "initial_commit": "abc1234def",
      "workspace_roots": ["/Users/you/Projects/api"],
      "raw_path": "/Users/you/.codex/sessions/2026/06/21/rollout-codex.jsonl",
      "source_stamp": "v2:1788042670103317900:569",
      "discovery_state": "shallow",
      "project_key": "github.com/acme/api",
      "project_key_method": "remote",
      "locations": ["local"],
      "from_cache": true
    }
  ],
  "next_cursor": {
    "last_activity_ms": 1782039603000,
    "source": "codex",
    "session_id": "0198c2ad-codex"
  }
}
```

Keys are `snake_case`. `models`, `workspace_roots`, and `locations` are always
arrays (possibly empty); every other absent value is `null`, never an invented
placeholder or an empty string.

`project_key` is the canonical project identity: the `origin` remote
canonicalized to `host/owner/repo`, or the working directory when no remote
resolves. **Group by it rather than by `cwd`** — two checkouts, two worktrees
or two subdirectories of one repository share a key but never share a path, and
a path-keyed rollup splits them. `project_key_method` says how the key was
arrived at, and the three are not interchangeable:

| `project_key_method` | meaning |
| --- | --- |
| `remote` | canonicalized `origin` remote; comparable across machines |
| `path` | no remote resolved, so the key is the directory and is only meaningful on the machine that produced it |
| `inherited` | adopted from the delegating parent session, because the child's own directory resolved to nothing canonical |

The rules match burn's `crates/relayburn-sdk/src/reader/git.rs` vector for
vector, so `burn --group-by project` and a RelayHistory rollup agree on the
same checkout. No `git` subprocess is involved: git's configuration is read
directly, in the scopes and precedence git itself uses — system
(`$GIT_CONFIG_SYSTEM` or `/etc/gitconfig`, unless `$GIT_CONFIG_NOSYSTEM`), then
global (`$GIT_CONFIG_GLOBAL`, else **both** `$XDG_CONFIG_HOME/git/config` and
`~/.gitconfig`, in that order — git reads both, and `git config --global
--list` showing only the latter is about where git *writes*), then the
repository's own, following a linked worktree's
`gitdir:` pointer. `include.path` and `includeIf` (`gitdir:`, `gitdir/i:`,
`onbranch:`) are expanded at the position of their own line, so a value written
before an include is overridden by it and one written after it wins — as git
resolves them — and `url.<base>.insteadOf` rewrites are applied
longest-prefix-first, as `git remote get-url` does. The outer scopes matter as
much as the repository's own: the rewrite that makes `gh:Org/Repo.git`
resolvable is almost always configured once in `~/.gitconfig` for the whole
machine.

For a linked worktree, `config` is read from the shared directory `commondir`
names while `HEAD` is read from the worktree's own git directory, and
`includeIf` conditions are evaluated against that same per-worktree directory.
A worktree exists to be on a different branch from the checkout it shares a
repository with, so asking the shared directory would answer about the wrong
tree. When the shared config enables `extensions.worktreeConfig`, that
worktree's `config.worktree` is read after the shared config, as git reads it.

Both spellings of a subsection are understood: `[remote "origin"]` and the
legacy `[remote.origin]` name the same remote, with git's differing case rules
— the quoted subsection is case-sensitive, the dotted header folds entirely,
so `[remote.ORIGIN]` is `origin` while `[remote "ORIGIN"]` is a different
remote.

A remote's URL is a list, not a single value: git accumulates every
`remote.<name>.url` it reads, across scopes as well as within a file, and the
remote *is* the head of that list — `git remote get-url origin` prints it while
`git config --get remote.origin.url` prints the last. The canonical key follows
`get-url`, so a repository with a mirror configured after its origin keys to
the origin. An IPv6 authority keeps its brackets (`[2001:db8::1]/acme/app`), so
an address is never cut at the first colon of its own body.

One thing is deliberately left out: `includeIf "hasconfig:remote.*.url:"` is
not evaluated, because its answer depends on how much configuration has been
read so far. It cannot turn a resolvable remote into a wrong one — it only
leaves a rewrite unapplied, which falls back to a path key.

Both are `null` while a session's identity has not been resolved yet — a
database that predates the columns migrates without inventing keys, and the
next sync or hydration fills them. `null` means "not resolved", never "no
project".

A key is resolved from the working directory, or from a remote the provider
recorded (Codex's `session_meta.payload.git.repository_url`). A `path` key is
never final: every sync reconsiders it, so a session whose checkout was deleted
picks up the canonical key as soon as a recorded remote makes one available. A
`remote` key is never downgraded.

Filter a listing to one project with `ai-hist sessions list --project <key>`
(SDK: `listSessionCatalogPage({ projectKey })`). It is an exact match on the
key, not a path or a prefix. `ai-hist stats` groups `top_projects` by the key
and reports `grouped_by`; `--by-cwd` (SDK: `stats({ byCwd: true })`) restores
the per-directory grouping.

For this cache-only operation, top-level `scope` is the filter applied to the
ledger. `locations` contains observed presences only; legacy rows that predate
presence tracking may therefore have an empty array while still appearing in
the compatibility-preserving default local view.

`next_cursor` is the continuation for the next page, or `null` once the catalog
is exhausted (the page came back short of its limit). See
[Pagination](#pagination).

### `sessions discover --json`

JSONL, one object per line (the `events` command's precedent). Three line
types, in this order:

```jsonc
// 0..n session rows, in global recency order (newest first)
{"type": "session", "source": "codex", "session_id": "0198c2ad-codex", "from_cache": false, /* …same fields as above… */ }

// 0..n non-fatal failures — one provider or one malformed session
{"type": "diagnostic", "source": "grok", "locator": "/Users/you/.grok/sessions/…/chat_history.jsonl", "error": "…"}

// exactly one closing summary — emitted even when every provider failed,
// so a consumer always sees the reason before the non-zero exit
{
  "type": "summary",
  "contract_version": 4,
  "scope": "local",
  "locations_run": ["local"],
  "discovered": 2,
  "skipped_unchanged": 0,
  "providers": {
    "claude":   {"candidates": 1, "discovered": 1, "skipped_unchanged": 0, "failed": false},
    "codex":    {"candidates": 1, "discovered": 1, "skipped_unchanged": 0, "failed": false},
    "cursor":   {"candidates": 0, "discovered": 0, "skipped_unchanged": 0, "failed": false},
    "grok":     {"candidates": 0, "discovered": 0, "skipped_unchanged": 0, "failed": false},
    "opencode": {"candidates": 0, "discovered": 0, "skipped_unchanged": 0, "failed": false},
    "relay":    {"candidates": 0, "discovered": 0, "skipped_unchanged": 0, "failed": false}
  },
  "exempt_sources": [
    {"source": "trajectory", "reason": "derived trajectory records, not provider sessions"}
  ],
  "counters": {
    "candidates_enumerated": 2,
    "shallow_reads": 2,
    "skipped_unchanged": 0,
    "files_opened": 2,
    "bytes_read": 978,
    "provider_queries": 0,
    "records_inspected": 0
  }
}
```

`counters` is the run's bill of work, and it is the honest way to check that
discovery stayed cheap. `bytes_read` counts explicit reads from file-backed
providers only; SQLite does not report exact filesystem bytes, so OpenCode
leaves it at zero instead of substituting the database file size.
`provider_queries` counts OpenCode's bounded data queries (schema-capability
introspection is excluded), and `records_inspected` counts the rows those data
queries return. `shallow_reads` is `0` on a rescan where nothing changed.
Summary `scope` echoes the requested acquisition scope, and `locations_run`
enumerates the connector locations that actually executed — `["local"]` on a
machine with no remote connector configured, `["local", "remote"]` when an
`all` run also executed one. `providers` groups source adapters rather than
locations, so under `all` a source served by a local adapter and a remote
connector reports their merged tallies; row `locations` still report observed
presences.

Diagnostics are their own lines and never appear inside the `summary` object,
so a consumer that only wants the tally can read the last line and stop. In
human (non-`--json`) mode they go to stderr instead, leaving stdout clean:

```text
  2026-06-21 11:00  codex    local        shallow  0198c2ad-codex   /Users/you/Projects/api  make the backoff configurable
  2026-06-20 10:05  claude   local        shallow  3f6c1b7a-claude  /Users/you/Projects/api  add a retry to the http client
  2 session(s): 0 discovered, 2 unchanged (0 file(s) opened, 0 shallow read(s)); requested scope: local, connector locations run: local
```

A provider that fails contributes a `diagnostic` and nothing else; the run
continues and exits `0`. The command fails only when **every** selected
provider failed.

---

## Field semantics

Every value is one of three things, and the distinction is part of the
contract:

| Field | Kind | Notes |
|---|---|---|
| `source` | observed | `claude`, `codex`, `cursor`, `grok`, `opencode`, `relay` |
| `session_id` | observed | provider-native; `(source, session_id)` is the primary key, so the same native id under two providers is two rows |
| `cwd` | observed | working directory the provider recorded |
| `git_branch` | observed | last branch the provider recorded |
| `first_activity_ms` | observed | `null` when the provider records no timestamps at all |
| `last_activity_ms` | observed, or filesystem-derived | file mtime for providers that record no timestamps |
| `first_prompt` | **derived** | bounded excerpt (≤ 4096 chars) of the first *substantive* human turn; provider control/meta/sidechain turns are skipped. For remote rows it is the provider's own session/task title — the listing's only human-readable identifier, which both providers derive from the opening prompt |
| `last_assistant_text` | observed | **only** written by full indexing — always `null` on a shallow-only row |
| `models` | observed, best effort | model ids seen inside the bounded read; empty means "not seen cheaply", not "no model" |
| `originator` | observed | the client that started the session (codex only) |
| `agent_version` | observed | agent CLI version |
| `repo_url` | observed | remote URL, when the provider records one |
| `initial_commit` | observed | commit the session started from |
| `workspace_roots` | observed | extra workspace roots, when the provider records them |
| `raw_path` | observed | provider file this row came from; the session/task URL for remote rows; `null` for database-backed sources |
| `source_stamp` | internal | change marker; see [rescan behaviour](#rescans-and-source-stamps) |
| `discovery_state` | internal | `"shallow"` or `"full"` |
| `locations` | derived | sorted presences from `session_presences`: `"local"`, `"remote"`, or both |
| `from_cache` | per-response | `true` when the row was served without re-reading the source |

Absent metadata stays `null`. Nothing is ever invented to fill a column.

### What the catalog deliberately does not have

Except for targeted remote hydration — full Claude teleport evidence or the
partial Codex cloud diff — these require a full `ai-hist sync` through an
available provider connector:

- per-message events (`session_events`) and tool calls; file edits are partial
  for a Codex cloud row whose available diff has been hydrated
- token usage and cost
- session → commit links
- full-text search over transcripts
- the full transcript body, and `last_assistant_text`

`discovery_state` tells you which you have: `"full"` means a full ingest has
run for that session, `"shallow"` means catalog metadata only. Full ingest
always wins — a shallow rescan refreshes a `full` row's metadata and stamp but
never downgrades its state. Local connectors provide full sync today. Claude
remote rows can become full through targeted teleport-evidence hydration;
Codex remote rows remain shallow after their available diff is indexed because
the CLI exposes no transcript export (see [Remote connectors](remote-connectors.md)).

### Product boundary

Discovery reports *which sessions exist* and identifying metadata. It does not
infer project membership, work status, health, risk or success, and it does not
summarize outcomes.

### Scope and connector availability

Scope filtering is consistent across catalog listing, search, recent history,
statistics, packs, and resume selection: `local` selects sessions with a local presence, `remote` selects those
with a remote presence, and `all` returns their deduplicated union. These are
cache-only queries over the same ledger.

Discovery and sync are acquisition operations. Local adapters are always
available; remote acquisition runs through provider connectors —
`claude-web` for claude.ai/code web sessions and `codex-cloud` for Codex
cloud tasks — that are configured by the provider CLI's own stored sign-in
(see [Remote connectors](remote-connectors.md)). Explicit remote acquisition
on a machine with no connector configured returns an error rather than
silently doing local work. `--all` means every configured adapter: the local
adapters plus whichever connectors are configured.

The discovery summary preserves the requested scope and reports the executed
locations separately in `locations_run`, so `--all` reports `scope: "all"`
with `locations_run: ["local"]` on a machine without connectors. That is
different from each row's observed `locations`. A remote-only session can be
selected by resume search, but it cannot yield a usable local resume command;
materialize it locally first. Sessions with both presences remain locally
resumable.

Direct `session` and `events` lookups already name a `(source, session_id)` and
therefore remain scope-independent.

---

## Per-provider capability matrix

What each adapter can actually extract from a cheap read. `✓` = populated when
the provider recorded it; `–` = the provider does not expose it to a shallow
read.

This table covers *shallow discovery only*. The full-evidence picture — which
record types each source captures, stores and exposes after `ai-hist sync` —
is the capture matrix in [ADR: relayhistory owns session
sourcing](decisions/2026-09-19-relayhistory-owns-session-sourcing.md#capture-matrix),
which this table must stay consistent with.

| Source | `session_id` | `cwd` | `git_branch` | `first_activity` | `last_activity` | `first_prompt` | `models` | `originator` | `agent_version` | `repo_url` | `initial_commit` | `workspace_roots` |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| **claude** | ✓ | ✓ | ✓ | ✓ | ✓ (tail) | ✓ | ✓ (head) | – | ✓ (record `version`) | – | – | – |
| **codex** | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| **cursor** | ✓ (dir name) | ✓ (decoded path) | – (never) | ✓ (injected `<timestamp>`) | ✓ (injected `<timestamp>`, else mtime) | ✓ | – (never written) | – | – | – | – | – |
| **grok** | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ (if present) | – | – | – | – | – |
| **opencode** | ✓ | ✓ (directory) | – | ✓ | ✓ | ✓ | ✓ | – | – | – | – | – |
| **relay** | ✓ | – (never) | – | ✓ (synced min ts) | ✓ (synced max ts) | ✓ (earliest synced prompt) | – | – | – | – | – | – |

Evidence coverage is the hydration-side version of that matrix: what each
local parser writes, and therefore the `coverage` a completed local hydration
reports. It is declared per adapter (`ShallowSessionProvider::evidence_kinds`),
so a provider that grows a parser flips one entry and the reported capability
follows.

| Source | `history` | `session_event` | `tool_call` | `file_edit` | `relationship` | `capability` |
|---|---|---|---|---|---|---|
| **claude** | ✓ | ✓ | ✓ | ✓ | ✓ | `full` |
| **codex** | ✓ | ✓ | ✓ | ✓ | ✓ when the bounded child search is complete | `full` or `partial` |
| **cursor** | ✓ | ✓ | ✓ | ✓ | – (never: a `Task` block names no child transcript) | `partial` |
| **grok** | ✓ | – | – | – | – | `partial` |
| **opencode** | ✓ | – | – | – | – | `partial` |
| **relay** | – | – | – | – | – | targeted hydration unsupported |

### Per-message raw facts on `session_events`

The envelope facts a harness records per message or per API request, kept
verbatim on every event so a consumer can group, price and time turns without
re-reading the transcript. `stop_reason` is the provider's own wire string,
never a normalized enum, and its *absence* is the signal that a turn is still
in flight.

| Source | `request_id` | `stop_reason` | `agent_version` | `is_sidechain` | `is_meta` | `turn_id` |
|---|---|---|---|---|---|---|
| **claude** | ✓ (`requestId`) | ✓ (`message.stop_reason`) | ✓ (`version` / `sourceVersion`) | ✓ (`isSidechain`) | ✓ (`isMeta`) | – |
| **codex** | – | – | – | – | – | ✓ (`turn_context.turn_id`, carried to the next `turn_context`) |
| **opencode** | – | ✓ (`step-finish.reason`, pending event-level parity) | – | – | – | – |
| **cursor**, **grok**, **relay** | – | – | – | – | – | – |

A null is "the provider did not record it", which is not the same as `false`
or as an empty string: a Claude record with no `isSidechain` key stores null,
while `"isSidechain": false` stores `0`.

Because of that, none of the six can answer "was this row indexed before the
facts existed?" -- a real record legitimately has no `request_id`, no
`stop_reason` and no `turn_id`, and Codex records none of the other three.
`raw_facts_version` answers it instead: the local parser stamps it on every
event it writes, so a full sync can pick out the transcripts whose rows predate
the facts and re-read them. It is bookkeeping rather than a provider fact and is
not part of the session-event evidence spec, so a row an installed source
adapter contributed is permanently unstamped.

That is why the column selects files but does not bound the work. Local and
remote observations of one session share `(source, session_id)`, so a contributed
row would otherwise hold an unchanged local transcript off the stamp fast path on
every sync while never being stamped itself. What ends the work is a per-provider
generation recorded in the sync state (`claude_raw_message_facts`,
`codex_raw_message_facts`), written only after a walk completes **and only when
every archive root the state already names was present on that run**, so the
backfill runs exactly once, an interrupted sync retries it, and a walk that
could not read the files it was meant to repair does not retire it. That check
is per file, not per root: a mount point exists whether or not anything is
mounted on it, and a partially mounted archive returns some of the paths the
state names and not others. A path this run did not see withholds the
generation **and** loses its stamp, so if it comes back it is read afresh rather
than skipped on a stamp nothing watched. Dropping the stamp is also what bounds
the deleted-file case: it costs one further sync, after which the path is no
longer one the state knows about. Removing the entry from the in-memory map is
not enough to achieve that — the checkpoint merge folds a run's keys over
what is on disk and has no way to express a delete, so a dropped path would
come back on every write. The run carries the removals as an instruction the
merge applies and then discards. A transcript that is enumerated but cannot be
read counts as unobserved too, not as an empty one — the parsers read with
`unwrap_or_default()`, so without that check a file that became unreadable
between the walk and the read would be stamped as seen. A root the state never knew about — an install
with no `.codex/archived_sessions` — is not a missing archive and does not hold
the pass open. Claude reaches a subagent
sidecar's rows through `session_relationships.evidence_locator`, because a
sidecar never gets a `sessions` row of its own.

Delegation is a separate capability, reported on every relationship result as
`capabilities.stableChildIdentity`:

| Source | Stable child identity | Agent type | Spawn time | Evidence locator |
|---|---|---|---|---|
| **codex** | always | ✓ | ✓ | ✓ |
| **claude** | sometimes | ✓ | ✓ | ✓ |
| **cursor**, **grok**, **opencode**, **relay** | never | – | – | – |

Cursor is `never` for a different reason from grok, opencode and relay: it does
record the spawn — a `Task` / `functions.Subagent` tool call, preserved as a
`tool_calls` row — but the block names no child transcript, so there is no
identity to link. See ["cursor → Delegation"](#cursor).

Claude is `sometimes` because a subagent transcript carries the *parent's*
`sessionId` on every record; the child's own identity is the per-child
`agentId`, which only newer provider versions emit. When it is present the
child is indexed under it; when it is absent the delegation is recorded as
unlinked evidence and the child id is left null — it is never taken from the
`agent-<id>.jsonl` file name.

How each adapter works:

- **claude** — `$CLAUDE_CONFIG_DIR/projects/**/*.jsonl` (default
  `~/.claude/projects/**/*.jsonl`). Head for identity, `cwd`,
  branch, `version`, models and the first human prompt; tail for the last
  timestamp and the final branch. Meta rows, slash-command wrappers, bash
  wrappers and sidechain (subagent) turns are skipped when picking
  `first_prompt`. A subagent *sidecar* — a separate file whose records all
  carry the parent's `sessionId` — is not a session of its own: it is detected
  in the head read and skipped, so a session is emitted once per run and its
  row keeps pointing at its own transcript. A transcript whose complete records
  parse as nothing is reported as a diagnostic rather than published under its
  file name; an empty one is simply not a session yet.
- **codex** — `rollout-*.jsonl` under `$CODEX_HOME/sessions` and
  `$CODEX_HOME/archived_sessions` (defaulting under `~/.codex`). The first line
  is a `session_meta` record, which
  makes codex the richest source: originator, `cli_version`, git remote, initial
  commit, workspace roots and model all come from it. Subagent threads are real
  rollouts but not user sessions, so they are excluded — exactly as the full
  sync excludes them — and remembered in `discovery_skips` so a rescan does not
  re-read them.
<a id="cursor"></a>

- **cursor** — `~/.cursor/projects/<encoded-path>/agent-transcripts/<id>/<id>.jsonl`.
  The session id is the directory name and `cwd` is decoded from the project
  directory name; neither appears inside the file. The head read takes the
  first human prompt and the first readable turn time, the tail read takes the
  last assistant prose and the last readable turn time, and `models` comes from
  `message.model` for the builds that write one — an empty list means "not seen
  cheaply", never "no model". Full details, and what Cursor does **not** write,
  are below.

  ### Cursor record shapes

  Cursor publishes no schema for this file, so the shapes below were
  characterized from public descriptions of real transcript corpora rather than
  from Cursor's documentation. **Sources are cited per field, and nothing here
  was verified against a real Cursor install on the machine this was written
  on** — see "Verification status" at the end of this section.

  A record is a bare object with the role at the **top level**:

  ```jsonl
  {"role":"user","message":{"content":[{"type":"text","text":"<timestamp>Wednesday, Sep 16, 2026, 3:37 PM (UTC-4)</timestamp>\n<user_query>ship it</user_query>"}]}}
  {"role":"assistant","message":{"content":[{"type":"text","text":"Reading the file."},{"type":"tool_use","name":"Read","input":{"path":"CHANGELOG.md"}}]}}
  {"role":"assistant","message":{"content":[{"type":"turn_ended","status":"success"}]}}
  ```

  There is no `message.role` and no record `type` — reading the role from the
  Claude Code nesting finds nothing and labels every turn as unknown. Older
  rows carry `message.content` as a bare string with no framing at all.

  | Field | Status | What RelayHistory does |
  |---|---|---|
  | top-level `role` | **Corroborated** by several independent adapters | Drives `session_events.role`; `user` turns also become `history` rows |
  | `message.content` as an array of blocks | **Corroborated** | Each block becomes one `session_events` row |
  | `message.content` as a bare string | **Corroborated** (the shape the pre-existing parser was written against) | Normalized into one synthetic `text` block |
  | `{"type":"text","text"}` | **Corroborated** | `session_events.kind = 'text'` |
  | `{"type":"tool_use","name","input"}`, **no `id`** | **Corroborated**; one report of `"id": null` in the CLI | `session_events.kind = 'tool_use'` plus a `tool_calls` row, keyed on the record's byte offset because there is no provider id |
  | `{"type":"turn_ended","status"}` | **Corroborated** | Skipped: it is a marker, and no `session_events.kind` honestly fits it |
  | `<timestamp>…</timestamp>` in the human turn's text | **Corroborated**; a localized human string, e.g. `Wednesday, Sep 16, 2026, 3:37 PM (UTC-4)` | Parsed explicitly (English month, 12- or 24-hour clock, required `(UTC±H[:MM])`). The records answering that turn inherit it |
  | `<user_query>…</user_query>` in the human turn's text | **Corroborated** | Stripped, so the stored prompt is what the person typed |
  | `message.model` | **Not written** by any reported build | Recorded if a build ever writes it; otherwise `models` is empty and the matrix says unavailable |
  | `message.usage` | **Not written** by any reported build | Same: recorded when present, never synthesized |
  | `{"type":"tool_result",…}` | **Not written** — "not one line of tool output is persisted" | Parsed when present; the observed corpus produces none |
  | `{"type":"thinking",…}` | **Not written** by any reported build | Parsed when present |
  | record `timestamp` field | **Not written** | Preferred over the injected tag when present |
  | `sessionId`, `cwd`, `gitBranch` inside the file | **Not written** | Taken from the path layout instead; `git_branch` stays null |
  | `parentMessageId`, `isSidechain`, `message.id` | **Not written** (an early adapter assumed these and was rejected against a real corpus) | `message.id` is used if present; the rest are not read |

  Tool names come in three dialects across Cursor versions and served models,
  and all three are classified: the unprefixed set (`Read`, `Grep`, `Glob`,
  `Shell`, `AwaitShell`, `Write`, `StrReplace`, `Delete`, `EditNotebook`,
  `ReadLints`, `TodoWrite`, `Task`, `SwitchMode`, `WebSearch`, `WebFetch`,
  `GetMcpTools`, `CallMcpTool`, `FetchMcpResource`, `GenerateImage`), the
  `functions.*` namespaced set used by GPT-served models (including
  `functions.ApplyPatch`, `functions.rg` and `functions.Subagent`), and an older
  snake_case set (`edit_file`, `search_replace`, `run_terminal_cmd`,
  `codebase_search`). `Write`, `StrReplace`, `ApplyPatch`, `Delete` and
  `EditNotebook` (plus their namespaced and snake_case spellings) produce
  `file_edits` rows. `ApplyPatch` is the awkward one: its `input` is the patch
  **text**, not an object, so the file paths come out of the patch headers and
  the line counts come from the same `count_patch_text` the Claude parser uses.
  One patch routinely rewrites several files, so it produces **one `file_edits`
  row per file**, each keyed `<tool_use_id>#<path>` and carrying only that
  file's slice of the patch — the same shape the Codex `patch_apply_end` path
  uses — under a single `tool_calls` row whose `target` names the first file.
  `StrReplace` carries old/new strings rather than a diff, so its edit is
  recorded with no line counts rather than guessed ones. `Shell` records only
  the command string, so no file is attributed to it.

  ### Identity and re-reads

  Because Cursor writes no record uuid and no `tool_use` id, every
  `session_events.event_uid`, `tool_calls.tool_use_id` and
  `file_edits.tool_use_id` is derived from the record's **byte offset** in the
  file. An offset is stable across an incremental read, a whole-file re-parse
  and a re-hydration, and it resets exactly when Cursor rewrites the file —
  which is also when the byte cursor resets. This is what makes a repeat read
  upsert in place instead of duplicating.

  Stability *within* a generation is not enough, though, because a rewrite
  reuses those offsets for different records. Any read that starts at offset 0
  — a first sight of the file, a Cursor rewrite, or the one re-read a retired
  state key forces after a parser upgrade — is therefore a **rebuild**: it
  clears the session's `history`, `session_events`, `tool_calls` and
  `file_edits` together, inside the writing transaction, before indexing.
  Upserting on top would leave every row the previous, longer generation wrote
  past the new end of the file, so the session would keep tool calls and edits
  it never made. Targeted hydration always re-reads the whole transcript, so it
  always rebuilds.

  `history` additionally cannot be upserted even within a generation: a
  prompt's identity is `(source, timestamp_ms, prompt)`, and a turn with no
  readable time is stamped with a file mtime that moves on every append. Plain
  `sync` therefore inserts prompts only for records at or after the offset it
  resumed from, except on a rebuild, where it re-inserts all of them.

  Only newline-terminated records are indexed, matching `CompleteJsonlReader`
  and `complete_jsonl_records`. A record Cursor has flushed but not yet
  terminated is left alone: publishing it would put evidence at a byte offset
  the checkpoint does not consider consumed.

  For the same reason, indexing stops at the byte position the scan consumed
  through. Cursor writes continuously, so it can append between the scan and
  the whole-file read; those bytes are past the checkpoint this run commits, so
  indexing them would publish evidence the next sync reads again from the
  checkpoint — and an untimed prompt re-read after the mtime moved is a
  duplicate, not an upsert. The append is picked up by the next sync instead,
  from the offset that still points at it.

  An incremental read also leaves the timestamps of records it is merely
  re-reading alone. The mtime moves on every append, and it is the fallback
  stamp for a record with no recorded time, so re-deriving it for an older
  record would silently redate evidence stored under a different one — and
  since `history` rows before the resumed offset are deliberately not
  rewritten, the event would end up disagreeing with the prompt of its own
  turn. Only records at or past the resumed offset take the current mtime;
  earlier untimed records keep the stamp they already have.

  A transcript that cannot be read — it vanished between the scan and the
  index, or it is not valid UTF-8 — **fails** the sync. It is not indexed as an
  empty session. That matters because the rebuild has already deleted the rows
  it is about to recreate: swallowing the read error would commit an empty
  session and advance the byte checkpoint past content nobody read. The error
  rolls the whole transaction back, including the checkpoint, so the next sync
  retries the same offset.

  A rebuild also **replaces** both ends of `sessions.first_activity_ms` /
  `last_activity_ms`, and `sessions.last_assistant_text`, rather than merging
  into them. The usual merge widens the window and keeps whatever prose it
  already had, which is right for an incremental read that saw only the tail —
  but a `MAX()` can never retract an endpoint an earlier, prompt-only parser
  derived from the file mtime, because a real recorded timestamp is almost
  always smaller than it; and a rebuild that has just re-read the whole source
  and found no assistant prose would otherwise leave the catalog quoting a
  reply the transcript no longer contains.

  ### Timestamps

  No prompt or event is stamped with the file mtime unless its turn carried no
  readable time. When that happens, targeted hydration reports a
  `CURSOR_TIMESTAMP_FROM_MTIME` diagnostic naming the fallback; it is never
  silent. Shallow discovery reports `first_activity_ms` from the first readable
  turn time and leaves it `null` when the build wrote none, and
  `last_activity_ms` falls back to the mtime.

  A **human turn** opens a new turn, so it replaces the inherited time even
  when it has none of its own; everything answering that turn inherits. An
  undated human turn that kept the previous turn's time would be dated to a
  conversation that had already ended, and — worse — would look dated, so the
  mtime fallback would never fire and the diagnostic would never be reported
  for a transcript that plainly needed it.

  The `role` alone does not identify a human turn. Cursor writes tool results
  back as **user-role** records, and a user record carrying only a
  `tool_result` — or only a marker such as `turn_ended` — answers the turn that
  is already open rather than starting a new one. A user record is treated as a
  human turn only when it carries a `text` block; the rest inherit, unless they
  carry a recorded time of their own. Treating every user record as a new turn
  sent tool results to the mtime, dating a result hours after the call it
  answers and putting the two halves of one exchange in disagreement.

  The session's activity window covers every record a pass stamps **and stores
  evidence for**, not only the ones that carried a recorded time. An undated
  turn still produces events, at the mtime, so leaving it out of the window made
  `sessions.last_activity_ms` claim a recency older than the session's own
  newest event — and the session then sorted behind siblings that were
  genuinely older. Where a time was recorded it is still the one used, so a
  fully dated session reports its recorded times rather than the file mtime.

  Two kinds of record stay out of the window. One that stores nothing — a
  `turn_ended` marker is the whole record — has no event to be the recency
  *of*, and on a re-read it also has no stored event to recover its original
  time from, so it fell back to the *current* mtime and dragged the window to
  "now" on every single sync. And a record before the resumed offset whose
  original stamp cannot be recovered at all is stamped with a guess; a guess is
  not evidence of when anything happened, so it does not set the window either.

  ### Delegation

  Cursor's `Task` / `functions.Subagent` calls are recorded as ordinary
  `tool_calls` rows, so the spawn is visible. The block carries **no child
  transcript id**, so there is nothing to link to and
  `relationship_capabilities("cursor").stableChildIdentity` stays `never`;
  targeted hydration reports `CURSOR_SUBAGENT_SPAWN_UNLINKED` when a transcript
  contains such a call. Third-party reports describe subagent sidecars at
  `agent-transcripts/<parent>/subagents/<child>.jsonl`, but that layout is
  **unverified here** and no relationship row is written on the strength of it.

  ### Verification status

  Cursor was not installed on the machine this adapter was written on, so no
  real `~/.cursor/.../agent-transcripts/*.jsonl` was read. The fixtures under
  `crates/ai-hist/tests/fixtures/cursor/` reproduce the shapes described by the
  sources below; `extended-unverified.jsonl` is named so nobody mistakes it for
  evidence about Cursor.

  Sources consulted (all public, September 2026):

  - [agitHQ/agit PR #165](https://github.com/agitHQ/agit/pull/165) — the
    strongest source: an adapter built to a shape histogram over **104 real
    transcripts** (60 sessions + 44 subagents; 5,335 records, 11,162 content
    blocks) from **Cursor IDE 3.13.25**. Origin of "bare `{role, message}`,
    id-less `tool_use`, `turn_ended`, no `tool_result`/`thinking`/`timestamp`/
    `model`/`usage`", and of `ApplyPatch` taking patch text directly.
  - [agitHQ/agit PR #118](https://github.com/agitHQ/agit/pull/118) — the
    rejected predecessor. Useful as a negative result: it assumed an
    Anthropic-shaped `{role, type, message:{id, model, usage}, parentMessageId,
    isSidechain, timestamp}` record and was rejected because *none* of those
    fields exist in a real transcript.
  - [clickety-clacks/engram issue #19](https://github.com/clickety-clacks/engram/issues/19)
    — independent confirmation of `role` + `message.content[]` with `text` and
    `tool_use` blocks and no top-level `type`/`session_id`.
  - [ArcadeAI/safeword issue #4594](https://github.com/ArcadeAI/safeword/issues/4594)
    — independent confirmation that `role` is top-level and `message` has no
    `role`, measured against a real 564 KB transcript (76 user, 333 assistant).
  - [nixfred/infomarchy PR #34](https://github.com/nixfred/infomarchy/pull/34)
    — the `<timestamp>` tag, its exact wording
    (`Wednesday, Sep 16, 2026, 3:37 PM (UTC-4)`), the warning that `Date.parse`
    drops the offset on some builds, the `turn_ended` end-of-turn marker, and
    that hooks/rules inject turns indistinguishable from human prompts.
  - [entireio/cli `agent/cursor`](https://pkg.go.dev/github.com/entireio/cli/cmd/entire/cli/agent/cursor)
    — the `Write`/`StrReplace` file-modification pair, and that `Shell` records
    only a command string.
  - [fitchmultz/pi-cursor-sdk evidence, 2026-08-02](https://github.com/fitchmultz/pi-cursor-sdk/blob/main/docs/evidence/cursor-system-prompts-2026-08-02/README.md)
    — the full 19-tool inventory in both the unprefixed and `functions.*`
    dialects.
  - [cavi-ai/secure-agent PR #104](https://github.com/cavi-ai/secure-agent/pull/104)
    and [microsoft/AI-Engineering-Coach PR #264](https://github.com/microsoft/AI-Engineering-Coach/pull/264)
    — independent confirmation of "no timestamps, no tool results, no token
    usage, no model ID", and of the `subagents/` sidecar directory.
  - [kenn-io/agentsview issue #1627](https://github.com/kenn-io/agentsview/issues/1627)
    — reports `"id": null` on `tool_use` blocks in the CLI JSONL and absent
    tool results.

  **Checklist for a maintainer who has Cursor installed.** Run a short agent
  session, then against
  `~/.cursor/projects/*/agent-transcripts/<id>/<id>.jsonl`:

  1. `jq -r 'keys|join(",")' … | sort -u` — confirm the top-level key set is
     exactly `role,message` (and record any extra key).
  2. `jq -r '.message|keys|join(",")' … | sort -u` — confirm `content` is the
     only key, or capture `id`/`model`/`usage` if your build writes them.
  3. `jq -r '.message.content[]?.type' … | sort | uniq -c` — capture the real
     block-type histogram; confirm whether `tool_result` or `thinking` ever
     appear.
  4. `jq -r '.message.content[]? | select(.type=="tool_use") | .name' … | sort | uniq -c`
     — capture the real tool-name dialect for your model.
  5. `jq -r '.message.content[]? | select(.type=="tool_use") | .id' … | sort -u`
     — confirm ids are absent or null.
  6. `grep -c '<timestamp>' <file>` and eyeball one tag — confirm the wording
     and offset format in your locale.
  7. Delegate to a subagent and check whether
     `agent-transcripts/<id>/subagents/` exists and whether the parent's `Task`
     block names the child.

  Anything that comes back different should replace the matching row in the
  table above, move the fixture out of `extended-unverified.jsonl`, and update
  the capability matrix.
- **grok** — `$GROK_HOME/sessions/<encoded-path>/<id>/` (default
  `~/.grok/sessions/<encoded-path>/<id>/`). Identity, `cwd`, branch and
  both timestamps come from `summary.json`; the first prompt comes from the head
  of `chat_history.jsonl`, skipping synthetic reminder turns.
- **opencode** — the SQLite store at `$OPENCODE_DB` (default
  `~/.local/share/opencode/opencode.db`). Discovery opens the live store with
  SQLite read-only and `query_only` enforcement, then holds one deferred read
  transaction for the run. Candidate enumeration and every selected-session
  query therefore share one committed SQLite snapshot. In WAL mode OpenCode's
  writer continues normally. Cross-process `SQLITE_BUSY` uses the repository's
  bounded, jittered retry policy (roughly 30 seconds); non-retryable
  `SQLITE_LOCKED` conflicts remain distinct diagnostics. RelayHistory never
  backs up the database or WAL, creates a temporary database, issues provider
  DDL, adds indexes, or runs provider migrations.

  With `--limit N`, one indexed query returns at most N candidate sessions.
  Selected candidates then use the provider's existing `message(session_id,
  …)`, `part(session_id, …)`, or `part(message_id, …)` indexes for prompt and
  model extraction. If those indexes or optional tables/columns are absent,
  discovery still returns session metadata but omits the affected prompt/model
  field rather than scanning a historical table.

  Exact newest-*updated* enumeration uses an existing index ordered by
  `session.time_updated DESC, id ASC`, so the global deterministic tie-break is
  applied before `LIMIT`. Some supported OpenCode schemas do not ship that
  index. On those schemas the bounded safe fallback reads `session` through its
  primary key in ascending order, which matches OpenCode's descending,
  time-encoded generated session IDs and therefore finds newest-created
  sessions. The limitation is that a much older session resumed recently can
  be delayed until OpenCode provides the compound recency index; RelayHistory
  will not mutate the provider database to repair that gap.
- **relay** — a **network** source with no local transcript, and discovery must
  work offline. The adapter therefore derives rows only from `history` rows a
  previous `ai-hist sync` already stored locally; it opens no socket. If nothing
  was ever synced it discovers nothing, which is the correct answer rather than
  a failure. A relay thread has no working directory, so `cwd` is always `null`.

---

## Ordering and limits

The ordering and the limit are **global**, not per provider:

1. Every selected provider adapter in the requested scope enumerates its
   candidates **cheaply** — a directory walk plus `stat`, or one indexed query.
   Database providers may cap their own candidate page at the global limit:
   no single provider can contribute more than that many winners. No file
   content is read.
2. Candidates from all providers are merged and sorted by recency hint,
   descending. Candidates with no recency signal sort last; ties break on
   `(source, locator)` so a run is reproducible.
3. The global `--limit` truncates that merged list.
4. Only the survivors get a bounded shallow read.

So `--limit 3` across two providers returns the three newest sessions
*overall*, not three from whichever provider happened to be enumerated first —
and the cost of a limited request is set by the limit, not by the size of the
archive.

Read budgets per source, so one enormous transcript cannot dominate a run:

| Budget | Value |
|---|---|
| head read | ≤ 256 KB and ≤ 400 complete JSONL records |
| tail read | ≤ 64 KB |
| text excerpt (`first_prompt`) | ≤ 4096 characters |

Files inside the head budget are read once and serve as their own tail. Only
newline-terminated records are parsed: a transcript being appended to right now
has a partial trailing line, and that line is not yet a record.

<a id="pagination"></a>

`sessions list` paginates by recency instead. The catalog's total order is

```sql
ORDER BY last_activity_ms DESC, source ASC, session_id ASC
```

with rows of unknown recency (`last_activity_ms IS NULL`, e.g. a cursor session
whose file has no mtime) last. Recency alone is not a key — one discovery pass
stamps many sessions with the same mtime-derived millisecond — so the cursor
carries the identity columns too:

```jsonc
{"last_activity_ms": 1782039603000, "source": "codex", "session_id": "0198c2ad-codex"}
```

Feed it back as `--after-ms` / `--after-source` / `--after-session-id` (the two
identity flags are required together; omit `--after-ms` to continue through the
undated tail). `--before-ms` still works as a coarse "older than" cutoff, but it
cannot separate rows that share a millisecond, so it is not a paging key; it is
ignored when a cursor is given.

The whole order is carried by `idx_sessions_recency`, or
`idx_sessions_source_recency` when `--source` is given, so a page is an indexed
read with no sort.

---

## Rescans and source stamps

Each connector presence stores a `source_stamp` —
`v{scanner version}:{provider change marker}`:

| Source | Change marker |
|---|---|
| claude, codex, cursor | `{mtime nanoseconds}:{file length}` |
| grok | the chat file's marker, `\|`, the `summary.json` marker |
| opencode | `{database identity}:{schema version}:{time_created}:{time_updated}` |
| relay | `{newest synced timestamp}:{synced row count}` |

On a rescan, a candidate whose stamp matches the stamp for that same location
in `session_presences` is served straight from the catalog: no read, no parse,
`skipped_unchanged` incremented, `from_cache: true` on the emitted row. The
top-level catalog row retains a canonical `source_stamp` summary for backward
compatibility, but connector change detection never relies on that merged
copy. In the benchmark below, a rescan of 450 unchanged sessions performs
**zero** shallow reads.

The `v{N}` prefix is the *scanner* version (`SHALLOW_SCANNER_VERSION`), separate
from `parser_version` (the full-ingest parser generation). Bumping it
invalidates every stored stamp, so a scanner taught to extract a new field
re-reads sources whose bytes never changed. It is at **4**: version 3 shipped
the prompt-only Cursor reader, and 4 adds that provider's injected turn times,
models and last assistant reply. Without the bump those rows would be served
from cache with the new fields null forever, because a Cursor transcript'''s
bytes do not change when the release does. The cost is one re-read per source,
once.

---

## Concurrency

Discovery deliberately does **not** take the `sync` advisory lock, so a session
picker refreshing itself never has to wait behind a 60-second background sync.

That is safe because every write discovery performs is an idempotent,
stamp-guarded upsert into `sessions`:

- the shallow upsert never nulls a value the catalog already holds, never
  raises `first_activity_ms` above what a fuller pass observed, and never
  downgrades a fully indexed row to `'shallow'` — including a row from a
  database that predates `discovery_state`, whose `NULL` readers interpret as
  `'full'`;
- the full-sync path only ever upgrades a row to `'full'`;
- writes go through the normal busy-retry connection, and the opencode
  provider reads one coherent snapshot from a live read-only connection.

A concurrent `sync` and `discover` therefore converge on the same row rather
than fighting over it.

`sessions list` is classified read-only, so it takes a read-only handle when
the schema is current: it cannot block the writer and cannot be blocked by it.

### Schema

The catalog lives in the existing `sessions` table, extended with
`first_prompt`, `models_json`, `originator`, `agent_version`, `repo_url`,
`initial_commit`, `workspace_roots_json`, `source_stamp` and `discovery_state`,
plus the `idx_sessions_raw_path`, `idx_sessions_recency` and
`idx_sessions_source_recency` indexes — every extra index on `sessions` is one
more btree a discovery upsert must update, so only indexes a query actually
reads exist. A companion
`discovery_skips` table remembers sources already examined and found not to be
sessions (a codex subagent thread, a Claude subagent sidecar), keyed by
`(source, locator)` with the stamp, so a rescan costs a primary-key lookup
instead of re-reading them every run.
Databases created by an older release are migrated in place by a serialized
missing-column check in `init_db`, and `schema_is_current` knows about the new
columns, so a read-only handle over an old database is upgraded instead of
failing with `no such column`.

`session_presences` is the location child table. It is keyed by
`(source, session_id, location)`, where `location` is `local` or `remote`, and
is joined to the corresponding `sessions` row by `(source, session_id)`.
Each presence also keeps its connector-specific `raw_locator`, `source_stamp`,
and `discovery_state`, so local and cloud change detection cannot overwrite
one another. Existing identities found in local catalog and evidence tables
are backfilled with a local presence during migration. Scope queries use this
table to select sessions and aggregate their `locations`; they do not duplicate
the session or its events.

`session_relationships` is the delegation table, keyed by
`(source, parent_session_id, relationship_uid)`. `relationship_uid` is
`child:<child_session_id>` for an observed child and
`evidence:<evidence_kind>:<evidence_locator>` for unlinked evidence, which
makes repeated ingestion idempotent and gives each unlinked sidecar its own
row. Beside the identity columns (`child_session_id`, nullable, and
`identity_status`) it stores `relationship`, `child_agent_type`,
`child_agent_name`, `child_model`, `spawn_depth`, `evidence_kind`,
`evidence_locator`, `evidence_ref`, `child_has_events`, `spawned_at_ms`,
`created_ms`, and `updated_ms`; re-ingestion refreshes mutable fields and
preserves first-observation time. It is read through
`idx_session_relationships_parent` and `idx_session_relationships_child`.
Databases written before this shape are rebuilt in place by the
`session_relationships_v2` marker migration, which copies every existing edge
forward as an observed `legacy_hydration` row; the marker is required, so an
unmigrated database is routed through the writable open instead of being read
as current.

---

## Adding a provider

Providers are added **here and nowhere else**. RelayHistory is the single owner
of acquiring, parsing and storing session evidence for every harness;
downstream consumers read it through the `ai-hist` crate's `SessionStore`
facade rather than writing a second parser. See [ADR: relayhistory owns session
sourcing](decisions/2026-09-19-relayhistory-owns-session-sourcing.md). That
matrix has record types as rows and sources as columns, so a new or extended
provider must add or update its source column in the same change, and satisfy
the record types in [`sourcing-contract.md`](sourcing-contract.md).

Every entry in `SOURCE_CHOICES` must be covered by **exactly one** of:

- an adapter in `shallow_providers()` — implement `ShallowSessionProvider`
  (`enumerate` may stat but not read; `read_shallow` stays inside the head/tail
  budgets and returns `Ok(None)` for "this candidate is not a session"), or
- an entry in `DISCOVERY_EXEMPTIONS`, which is machine-readable and carries a
  reason.

A registry test enforces the pairing, so adding a source without deciding which
list it belongs to fails the build. Today the only exemption is `trajectory`
("derived trajectory records, not provider sessions"). It is enforced at both
ends: `sessions discover --source trajectory` fails with that reason, and
`sessions list` filters `trajectory` rows out defensively, so a trajectory can
never be presented as a session.

The exemption list also travels in the `summary` line as `exempt_sources`, so a
consumer can tell "this source has no sessions" apart from "this source is not
discoverable".

### Add a fixture and a snapshot

A provider is not added until its log shape is in the checked-in corpus. Add at
least one fixture under `crates/ai-hist/tests/fixtures/<source>/`, register it
in the `CORPUS` manifest in `crates/ai-hist/tests/fixture_corpus.rs` with the
quirk it encodes, list it in `tests/fixtures/README.md`, and commit the
generated snapshot under `crates/ai-hist/tests/snapshots/<source>/`:

```sh
UPDATE_SNAPSHOTS=1 cargo test -p ai-hist --all-features --test fixture_corpus
```

`every_source_choice_has_a_fixture_or_an_exemption` enforces the same pairing
the discovery registry does: every `SOURCE_CHOICES` entry has a fixture, or a
documented fixture exemption for a source that has no provider log on disk
(`trajectory`, `relay`). `corpus_manifest_covers_every_fixture_file` and
`corpus_readme_lists_every_fixture_and_quirk` stop a fixture from being added
without being described, and `no_orphaned_snapshots` stops a snapshot from
outliving its fixture.

The snapshots are the *current* extraction, gaps included — they are the
review artifact for a parser change, not a statement of intent. Facts a
provider's logs contain that relayhistory does not capture yet are written as
`#[ignore = "closed by #<issue>"]` tests in the same file.

---

## Programmatic access

- **Native (napi)** — `listSessionCatalogPage(options?)` returns
  `{contractVersion, scope, sessions, nextCursor}`;
  `discoverSessions(options?)` runs a shallow scan and returns the rows plus
  the summary. The CLI renders the same collected
  result as JSONL when line-oriented records are more convenient. Both run on
  a blocking worker thread and accept `scope` / `sources` / `limit`, with `beforeMs` and
  `after` (the previous page's `nextCursor`) on the listing.
- **Native (napi), delegation** — `getSessionRelationships(options)` returns one
  session's edges in both directions plus the provider's capabilities;
  `getSessionTree(options)` returns the pre-order descendant tree bounded by
  `maxDepth` / `maxNodes`; `getSessionChildrenPage(options)` returns one keyset
  page of direct children. All three are cache-only, and a missing database is
  an empty result rather than an error — for the tree, the root-only result a
  session with no recorded delegation also returns.
- **TypeScript SDK** — `listSessionCatalog()` / `discoverSessions()` wrap the
  same contract for Node consumers, as do `getSessionRelationships()`,
  `getSessionTree()`, `getSessionChildrenPage()`, and the `sessionDescendants()`
  / `sessionEventsIncludingDescendants()` iterators; see the SDK's own
  documentation for the exact signatures.
- **MCP** — the stdio server exposes the cache-only listing as a `list_sessions`
  tool, so an agent can enumerate recent sessions without triggering any
  provider I/O, and delegation topology as the read-only
  `get_session_relationships` and `get_session_tree` tools. See the MCP
  package's documentation for their arguments.

Whatever the surface, `contract_version` means the same thing: check it, and
fail loudly on a version you do not know.

---

## Performance validation

The claims above are validated by a benchmark harness rather than by wall-clock
assertions in the test suite:

```bash
cargo test -p ai-hist-cli --test discovery_bench -- --ignored --nocapture
```

It builds a synthetic multi-provider archive in a temp directory, runs both
catalog operations plus a full `ai-hist sync` against it, and prints a
measurement table. Every assertion it makes is an *operation count* — bytes
read, files opened, shallow reads, rows returned — because those hold on any
machine; timings are printed for the reader and never asserted.

Representative multi-provider numbers from one run (debug build,
450-session archive, 14.7 MB):

| Measurement | Result |
|---|---|
| Cached listing, `--limit 20` at 1 000 catalog rows | 0.13 ms |
| Cached listing, `--limit 20` at 20 000 catalog rows + 100 000 history/event rows | 0.13 ms |
| Cached listing, `--limit 2000` at 20 000 rows | 8.6 ms |
| `discover --limit 5` over a 90-session / 6.2 MB archive | 5 shallow reads, 1.6 MB read (26% of the archive) |
| `discover --limit 5` over the same archive grown to 450 sessions / 14.7 MB | 5 shallow reads, **the same 1.6 MB** (11%) |
| `sessions discover` (cold, whole archive) | 11.9 MB read, 450 rows, ~1.7 s |
| `sessions discover` (rescan, nothing changed) | 0 file-backed shallow reads, ~0.08 s |
| `ai-hist sync` (full ingest, same archive) | reads all 14.7 MB, writes 62 451 history/event rows, ~46 s |
| Cached listing after the entire archive is deleted | same 50 rows, 0.41 ms |

The shape is what matters, not the absolute numbers: the cached listing tracks
the rows you asked for and ignores both catalog size and event volume; a
bounded request's cost is fixed by its limit; and a shallow refresh of a whole
archive cost roughly 1/27th of a full ingest of the same archive. OpenCode's
fixed-limit benchmark is documented separately in `benchmarks.md`; its
operation counters stay at 20 candidates, 41 queries, 60 returned records, and
zero claimed bytes for both 1,000- and 10,000-session stores.

Complementary structural coverage lives in the unit tests:
`the_catalog_listing_is_served_by_an_index_not_a_table_scan` (EXPLAIN QUERY
PLAN), `the_catalog_query_reads_only_the_sessions_table`,
`bounded_reads_do_not_grow_with_the_size_of_the_archive`, and
`a_head_read_stays_inside_its_budget_on_a_very_large_transcript`. OpenCode adds
`opencode_selected_session_queries_use_provider_indexes` and the ignored
`opencode_fixed_limit_scaling_report` benchmark.

---

See also: [`getting-started.md`](getting-started.md) (human setup) ·
[`agent-integration.md`](agent-integration.md) (agent-facing surfaces) ·
the `Schema` section of the top-level `README.md`.


### Limited discovery and connector aliases

A limited discovery run is a bounded preview, not a complete inventory of every
connector observation. After its session budget is filled, it may reconcile
additional candidates that explicitly identify an already emitted session. It
does not read every remaining opaque path to discover whether it aliases that
session: doing so would turn a small preview into a full archive scan. Unread
candidates do not withdraw or delete observations from previous scans.

To collect observations from another connector that enumerates opaque paths,
select that connector in a separate discovery call, or run discovery without a
limit. Its observation is then retained independently alongside existing
connectors. The read-budget and source-registry tests cover both behaviors.

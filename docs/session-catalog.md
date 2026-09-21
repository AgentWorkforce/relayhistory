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
| **cursor** | ✓ (dir name) | ✓ (decoded path) | – (never) | ✓ (injected `<timestamp>`) | ✓ (injected `<timestamp>`, else mtime) | ✓ | ✓ (if a build writes `message.model`) | – | – | – | – | – |
| **grok** | ✓ | ✓ | ✓ | ✓ (`updates.jsonl`, else `summary.json`) | ✓ (`updates.jsonl`, else `summary.json`) | ✓ | ✓ | – | – | – | – | – |
| **opencode** | ✓ | ✓ (directory / message `path.cwd`) | – | ✓ | ✓ | ✓ | ✓ (`providerID/modelID`) | – | – | – | – | – |
| **relay** | ✓ | – (never) | – | ✓ (synced min ts) | ✓ (synced max ts) | ✓ (earliest synced prompt) | – | – | – | – | – | – |

### Session markers

Not every provider record is a message. Compaction and summary boundaries,
system rows, non-text content blocks and agent lifecycle events all describe a
session without being a turn in it, and the normalized `session_events` model
has no `kind` for any of them. They are recorded in `session_markers` instead
of being dropped, and read with `session_markers_page` — the same
`(ts_ms IS NULL, ts_ms, id)` keyset the tool call and file edit pages use.

`kind` is the classified vocabulary below; `subkind` is the provider-native
type verbatim. A record type no classifier knows yet is stored as
`kind = "unknown"` with its real name in `subkind`, so it is recoverable
later.

`payload_json` is always bounded — every string at 128 characters and every
container at 32 entries, recursively — so an `image` or `document` block
contributes its size and never its bytes. How it is *built* depends on whose
keys they are. Where this parser classifies the record it names the fields it
keeps, which is the tighter contract. Where the payload is the provider's own
document and its keys are theirs — Grok's `signals` sidecar, a compaction
checkpoint — the document is bounded whole instead: enumerating the provider's
keys would silently drop whatever it adds next, which is the failure this
table exists to end. Either way the bound is the promise the column makes, and
it holds for every kind.

| `kind` | claude | codex | grok | `subkind` examples |
|---|---|---|---|---|
| `compaction_boundary` | ✓ `type:"system"`, `subtype:"compact_boundary"` | ✓ top-level `compacted`, `context_compacted` | ✓ | `compact_boundary`, `compacted` |
| `summary` | ✓ `type:"summary"` | – | – | `summary` |
| `subagent_notification` | ✓ system rows with `parent_tool_use_id`; tool results carrying `toolUseResult.agentId` | ✓ `subagent_*` | – | `subagent_completed`, `tool_use_result_agent_id`, `subagent_message_complete` |
| `task_started` | – | ✓ | – | `task_started` |
| `task_complete` | – | ✓ | – | `task_complete` |
| `turn_diff` | – | ✓ | – | `turn_diff` |
| `stream_error` | – | ✓ | – | `stream_error` |
| `tool_begin` | – | ✓ any `*_begin` | – | `exec_command_begin`, `patch_apply_begin`, `mcp_tool_call_begin` |
| `review_mode` | – | ✓ | – | `entered_review_mode`, `exited_review_mode` |
| `unsupported_block` | ✓ any content block with no event `kind`, plus thinking signatures | – | – | `image`, `document`, `redacted_thinking`, `server_tool_use`, `thinking_signature` |
| `encrypted_reasoning` | – | ✓ `response_item/reasoning` | ✓ an opaque reasoning trace with no summary | `reasoning` |
| `tool_replacement` | ✓ `_meta.replaces` / `_meta.collapsedCalls` | – | – | `tool_result` |
| `system` | – | – | ✓ a system preamble, in `text` | – |
| `synthetic_turn` | – | – | ✓ a turn the harness wrote, in `text` | – |
| `signals` | – | – | ✓ | – |
| `prompt_context` | – | – | ✓ | – |
| `unknown` | ✓ any unclassified record type, plus a `user`/`assistant` record that produced no event at all | ✓ any unclassified payload type, including `agent_reasoning_raw_content` and `agent_reasoning_section_break` | – | the provider type, verbatim |

`text` is the provider's own readable prose for a marker, and `payload_json`
its structure; a marker may carry either, both or neither. Grok records a
system preamble and a synthetic turn as prose, which is why the column exists
rather than that prose being flattened into JSON that no longer reads as prose.
Where a `kind` is written by more than one provider, the writers populate the
same columns for it, so `compaction_boundary` reads the same whether Claude,
Codex or Grok produced it.

Every provider line leaves a row behind: an event, a marker, or both. A record
type is silent here only when some other part of the parser is known to store
it — a claim that is checked, not assumed, by
`every_codex_line_leaves_an_event_or_a_marker_behind`. A record's outer `type`
is classified independently of whether it also carries message content, so a
future record type that happens to carry some is not filed away as an ordinary
text event with its native type recorded nowhere.

The invariant is enforced by *counting the rows a record produced*, not by
predicting them from the shape of its content. Content can be present and still
reach nothing — `""`, `[]`, or blocks that are all blank — and each such shape
is one more rule to miss. A Claude record that wrote no row falls back to
`unknown` carrying its provider type, and a Codex line measured against
SQLite's own `total_changes` does the same: a blank `agent_message` or a
`*_end` with no `call_id` is stored by nothing, whatever the handler list says.

Codex keeps one explicit exception list, for lines that are state updates
rather than records and whose information is stored elsewhere: `session_meta`
and `turn_context` populate the catalog, `token_count` is folded into the
adjacent assistant event's `token_json`, `thread_settings_applied` carries the
model forward, a `*_delta` is a fragment of an event recorded whole, and an
assistant `message` is the mirrored twin of the `agent_message` that stores the
text.

A **user** message is deliberately not on that list, and the reason is the
lesson the list itself taught. Codex writes a user turn in two representations
and a deduplicator stores one row for the pair — but only when it accepts the
turn. It refuses blank text, application-injected control wrappers, and content
with no `input_text` part, such as an image-only turn. Exempting user messages
by type alone therefore asserted a row had been written when none had, and
those lines vanished. The exemption is now *earned*: it applies to a mirrored
twin, where the deduplicator reports that its partner really did write, and
every other user line is settled by measurement like anything else.

That is the general shape to keep: an exception list must be oriented so a
wrong entry costs a redundant marker rather than a vanished line, and an entry
that asserts "something else stored this" has to be checked against what was
stored.

Markers are evidence, so they are removed with the rest when a complete remote
snapshot replaces a session: a marker left behind would tell a caller that
something is still there which the provider has stopped sending.

They also have to *survive* that path. A remote Claude snapshot is parsed by
the same local parser into a temporary database and then projected back out as
evidence records, so whatever the projection list omits is written during
normalization and discarded before anything durable sees it. Markers are in
that list — `PARSED_SESSION_KINDS`, everything this crate's parser writes —
which is deliberately separate from `FULL_SESSION_KINDS`, the set a remote
connector must supply for its snapshot to count as complete. A source plugin
cannot derive markers, so requiring them there would silently demote every
third-party connector to partial.

A compaction boundary states no size of its own, so a Claude
`compaction_boundary` payload carries `tokens_before_compact` taken from the
`cache_read_input_tokens` of the assistant message immediately before it.

`session_events.raw_kind` names the provider-native record or block an event
came from. Two very different records normalize to `kind = "tool_result"` — a
`tool_result` content block (`raw_kind = "tool_result_block"`) and a Claude
`type: "system"` subagent notification
(`raw_kind = "system_subagent_notification"`) — and `raw_kind` is what keeps
them apart without widening the `kind` vocabulary readers switch on.
Tool-result fidelity is a separate capability. The columns live on the
`session_events` rows whose `kind` is `tool_result`, and every one of them is
null when the provider does not record it — never a stand-in zero or a guessed
status, because a fabricated measurement reads exactly like a real one:

| Source | `payload_bytes` / `payload_hash` | `payload_truncated` | `call_index` / `event_index` | `result_status` | `event_source` | `error_signal` | `subagent_session_id` / `agent_id` |
|---|---|---|---|---|---|---|---|
| **claude** | ✓ (raw `content`) | ✓ (harness markers) | ✓ | ✓ | `tool_result`, `subagent_notification` | `tool_result.is_error`, `subagent_status` | ✓ (system subagent notifications) |
| **codex** | ✓ (raw `output`) | ✓ (harness markers) | ✓ | ✓ (settled at `task_complete`) | `function_call_output` | `exit_code`, `patch_apply`, `mcp_err` | – (no notification rail) |
| **cursor**, **grok**, **opencode**, **relay** | – | – | – | – | – | – | – |

`payload_bytes` is the raw UTF-8 length of what the provider handed back —
a string payload as-is, any other JSON payload stable-stringified with sorted
keys — measured before the `text` column is materialized, and `payload_hash`
is the first 16 hex characters of that payload's sha256. Both match
relayburn's `stable_stringify` / `content_hash`, so a value measured on either
side compares equal rather than merely looking alike. `payload_truncated`
records that the *harness* had already cut the output, which is the difference
between "this tool returned 8 KB" and "this tool returned far more and 8 KB is
a floor".

Codex reports how a call ended out of band (`exec_command_end`,
`patch_apply_end`, `mcp_tool_call_end`), so its result rows are written with
`result_status = 'unknown'` and settled when the turn closes at
`task_complete`. End of file is **not** a turn boundary: a live rollout's last
turn can still receive the `exec_command_end` that fails one of its calls after
the bytes a sync read, so a partial read records the failures it saw and leaves
anything else `unknown`. Only `task_complete` can call a result a success. The
remaining providers land with their parity issues; they share the
`ToolResultFacts::from_payload` helper, so the columns will mean the same thing
for them.

A result with nothing displayable in it — a silent command's empty string, a
structured payload carrying no text — is still recorded. `payload_bytes = 0` is
a measurement; dropping the row would lose the call's linkage and its place in
the ordering too.

Because the columns are nullable and the schema migration marks itself
complete, an upgraded install would otherwise keep skipping unchanged
transcripts on the sync fast path and leave every historical tool result null.
Plain `sync` therefore runs one backfill pass per provider, recorded in the
sync state, during which a transcript whose indexed tool results have no
`event_index` is re-read. Selecting files that way is narrower than
invalidating the whole stamp map, which would re-read the entire archive.

The generation is recorded only after a pass that both completed and covered
everything it discovered: every rollout root the database has indexed from was
reachable, and every transcript it found was read. A provider read that fails
is propagated rather than defaulted to an empty string, because an I/O or
UTF-8 failure reduced to `""` is indistinguishable from a file that genuinely
holds nothing. The walk indexes the rest of the tree and then reports the
failure, so the sync is classified as failed for that source rather than
reporting a complete cache it does not have.

Only a file that the walk would otherwise *skip* next time holds the pass
open — one whose recorded stamp still matches, which is the case where nothing
about the file will change to reopen it. A file with no stamp, or a stamp that
has moved, is revisited by the walk itself, so keeping the generation pending
would buy it nothing while keeping the per-session probe live, which re-reads
any session carrying a contributed null row on every sync.

The pass is bounded by a recorded generation rather than by "some row is still
null", and that distinction matters: `session_events` is keyed by
`(source, session_id)`, local and remote observations of one session share
that identity, and an adapter may contribute a tool result with no fidelity at
all. Re-reading the local transcript never populates a row that came from
somewhere else, so a null-row condition could stay true forever and re-read an
unchanged file on every sync without ever repairing it.
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
| **grok** | ✓ | ✓ | ✓ | ✓ | ✓ | `full` |
| **opencode** | ✓ | ✓ | ✓ | ✓ | ✓ | `full` |
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
| **opencode** | always | – | ✓ | ✓ |
| **claude** | sometimes | ✓ | ✓ | ✓ |
| **grok** | sometimes | ✓ | ✓ | ✓ |
| **cursor**, **relay** | never | – | – | – |

OpenCode is `always` because a subagent session is a session in its own right
and its own record names the parent, in `session.parentID`. Nothing is
inferred from file names or ordering, so the edge is recorded with
`evidence_kind = "opencode_parent_id"` and `identity_status = "observed"`.

OpenCode is the one source whose four columns do not move together, which is
the point of reporting them separately: it names the parent and the spawn
time and keeps the evidence locator, but records no *type* for the child, so
`childAgentType` and `childAgentName` are always null and the capability says
so rather than promising a field the adapter never writes.

Grok is `sometimes` for the same shape of reason: a `subagents/` metadata
entry that records a child session id links to a child session in the normal
sessions tree, and one that does not is stored as unlinked evidence. The id is
never taken from the entry's file name. A `Task` call inside the transcript
names no child at all. See ["grok"](#grok).

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
  | `<timestamp>…</timestamp>` in the human turn's text | **Corroborated**; a localized human string, e.g. `Wednesday, Sep 16, 2026, 3:37 PM (UTC-4)` | Parsed explicitly (English month, 12- or 24-hour clock, required `(UTC±H[:MM])`), **only out of a human turn's own `text` blocks**. The records answering that turn inherit it |
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

  The tag is a clock only where Cursor's client puts it: in the text a person
  submitted. It is not a field, so the same characters can appear in an
  assistant reply — a model explaining this format, quoting the turn it is
  answering, or reading a log back — and that is prose. Scanning every role's
  blocks for it let such a reply supply a turn time, which re-dated that record
  and every record after it until the next turn, and suppressed the mtime
  fallback that should have fired. Shallow discovery ran the same scan, so the
  same prose also moved `first_activity_ms` and `last_activity_ms` and re-sorted
  the session in the catalog. Both paths now share one rule
  (`cursor::injected_turn_time`): the tag is read only from a `text` block of a
  **`user`** record. A record `timestamp` field, when a build writes one, is a
  real provider field and is believed whatever the role.

  What no rule can separate, because nothing in the record does, is a person
  who pastes a transcript containing the tag — the same ambiguity recorded
  below for Cursor's hooks and rules, which inject turns structurally identical
  to human prompts.

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
<a id="grok"></a>

- **grok** — `~/.grok/sessions/<encoded-path>/<id>/`. Identity, `cwd` and
  branch come from `summary.json`; the first prompt comes from the head of
  `chat_history.jsonl`, skipping synthetic reminder turns; both activity
  timestamps come from the first and last record of `updates.jsonl` when it is
  there, and from `summary.json`'s `created_at` / `updated_at` when it is not.
  Full hydration reads the whole directory — transcript, update stream,
  signals, compaction checkpoints and subagent metadata. Details below.

  ### What Grok writes, and where relayhistory reads it

  A Grok Build session directory holds `summary.json`, `updates.jsonl`,
  `chat_history.jsonl`, `system_prompt.txt`, `prompt_context.json`,
  `tool_definitions.json`, `plan.json`, `rewind_points.jsonl`, `signals.json`
  and `feedback.jsonl`, plus the directories `compaction_checkpoints/` and
  `subagents/`. RelayHistory reads six of those and ignores the rest.

  **`chat_history.jsonl` has the content; `updates.jsonl` has the time.** Grok's
  own guide calls `updates.jsonl` "the authoritative conversation log that
  drives `/resume`", and `chat_history.jsonl` carries no timestamps at all, so
  the two are joined:

  1. A record that carries its own `timestamp` uses it. The documented Grok
     Build shape has none; an older layout does.
  2. Tool calls and results join **by id** — `tool_calls[].id` and
     `tool_result.tool_call_id` against `toolCallId` on a `tool_call` /
     `tool_call_update` row. Exact.
  3. Prose joins **by ordinal**: the *n*-th non-synthetic `user` record takes
     the time of the *n*-th `user_message_chunk` group, and likewise
     `assistant` prose against `agent_message_chunk` and `reasoning` summaries
     against `agent_thought_chunk`. Consecutive rows of one kind are one group,
     because Grok streams a message in chunks.
  4. What is left takes its turn's `turnStartMs`, then the nearest preceding
     record's time, then `summary.json`'s `created_at`. Every step below an
     exact match is counted and reported as a `GROK_TIMESTAMP_FROM_TURN`
     hydration diagnostic.

  An update kind this parser does not interpret (`plan`, `hook_execution`,
  `retry_state`) is **not** a boundary in that join. Grok interleaves those
  rows into a streaming message, and counting one as a break would split the
  message into two groups — after which every later ordinal join reads one
  group too far and hands the next message somebody else's timestamp. Such a
  row still bounds the session's activity; it just does not divide it.

  **No timestamp is derived from a record's position in the file.** Until this
  landed, Grok prompt *n* was stamped `created_at + n` milliseconds: a
  fabricated ordering, in a ledger whose value is that it does not fabricate.
  A session with no update stream and no record timestamps now gives its
  events the one time Grok did record, and says so, rather than spreading them
  a millisecond apart.

  ### Record shapes

  | Field | Status | What RelayHistory does |
  |---|---|---|
  | `chat_history.jsonl` line = `{"type", "content"}` | **Corroborated** by Grok's own user guide and three independent adapters | One `session_events` row per record |
  | types `system`, `user`, `assistant`, `tool_result`, `backend_tool_call`, `reasoning` | **Corroborated** | `user` → user/text + `history`; `assistant` → assistant/text; `reasoning` → assistant/thinking; `tool_result` → tool_result; `system` → a `system` marker |
  | `user` prompt wrapped in `<user_query>…</user_query>` | **Corroborated** | Stripped, so the stored prompt is what the person typed |
  | `synthetic_reason` on a `user` record | **Corroborated** | Kept out of `history` (unchanged) **and** out of `session_events`; recorded as a `synthetic_turn` marker carrying the reason, because nobody typed it |
  | `assistant.model_id` | **Corroborated** | `session_events.model`, and `sessions.models_json` |
  | `assistant.tool_calls[] = {id, name, arguments}` | **Corroborated**; explicitly *not* an OpenAI `function` wrapper | One assistant/tool_use event and one `tool_calls` row each, keyed on the provider's `id` |
  | `tool_result = {tool_call_id, content}` | **Corroborated** | A tool_result event paired to the call by id |
  | `tool_result.is_error` | **Inferred** — the failure signal is documented on the ACP `tool_call_update` `status` | Both are read; either marks `tool_calls.is_error` |
  | `reasoning.summary` / `reasoning.encrypted_content` | **Corroborated** ("reasoning is encrypted_content") | The summary is the thinking text; an encrypted-only record becomes an `encrypted_reasoning` marker and is never given invented text |
  | `updates.jsonl` envelope `{timestamp, method, params:{sessionId, update:{sessionUpdate,…}, _meta:{eventId, agentTimestampMs}}}` | **Corroborated** by three independent adapters | The timing source for the join; `eventId` also becomes the event's identity |
  | envelope `method` `session/update` **or** `_x.ai/session/update` | **Corroborated** | Both read; the method is not required to be either |
  | kinds `user_message_chunk`, `agent_message_chunk`, `agent_thought_chunk`, `tool_call`, `tool_call_update`, `plan`, `turn_completed`, `hook_execution`, `retry_state` | **Corroborated** | The first five and `turn_completed` are read; the rest are counted and reported as `GROK_UPDATES_ROWS_UNREAD` |
  | `timestamp` in epoch **seconds**, `agentTimestampMs` in **milliseconds** | **Corroborated** | A field named `…Ms` is read as milliseconds; a bare `timestamp` is scaled if it is below 10¹² |
  | `turnStartMs` | **Stated in [#167](https://github.com/AgentWorkforce/relayhistory/issues/167)** from burn #489; not seen in a public sample | Read from `params.update`, `params._meta` or the envelope; the turn's fallback time |
  | `turn_completed.totalTokens` | **Stated in #167**, and corroborated as a `turn_completed`-borne total | `token_json = {"context_total_tokens": n, "source": "updates.jsonl"}` on the turn's last assistant message |
  | `turn_completed.usage.{inputTokens, outputTokens, cachedReadTokens, reasoningTokens, costUsdTicks, modelUsage}` | **Reported by two community adapters for recent builds**, and contradicted by #167's "Grok does not log per-turn input/output tokens" | **Not read.** See "Usage" below |
  | `summary.json` `info.id`, `info.cwd`, `info.model`, `git_root_dir`, `head_branch`, `created_at`, `updated_at` | **Corroborated** | Identity, project, branch, model and the fallback timestamps |
  | `summary.json` parent-session references for forked/restored sessions | **Corroborated** (named in the guide, field spelling unknown) | **Not read yet** — no field name to read |
  | `signals.json` `contextTokensUsed`, `turnCount`, `compactionCount` | **Stated in #167**; the guide says the file holds "token usage and tool/turn counters" | A `signals` marker whose `detail_json` is the file verbatim |
  | `prompt_context.json` | **Corroborated** ("inputs the system prompt was rendered from") | A `prompt_context` marker with the path, SHA-256 and size. The AGENTS.md body is never copied into the database |
  | `compaction_checkpoints/<entry>` | **Corroborated** as a directory; the entry's own fields are **unverified** | One `compaction_boundary` marker per entry, timed from `created_at`/`timestamp`/`turnStartMs` or the entry's numeric file name, with the parsed file in `detail_json` |
  | `subagents/<entry>` | **Corroborated** as "per-subagent metadata; child sessions live in the normal sessions tree"; the entry's own fields are **unverified** | One `session_relationships` row per entry, `evidence_kind = "grok_subagent_dir"` |
  | `system_prompt.txt`, `tool_definitions.json`, `plan.json`, `rewind_points.jsonl`, `feedback.jsonl` | **Corroborated** as present | Not read |

  Tool names observed by burn #489 are `Shell`, `Read`, `Write`,
  `StrReplace`/`Edit`, `Grep`, `Glob`, `Task`, `WebSearch`/`WebFetch` and
  `CallMcpTool`. `Write`, `StrReplace`, `Edit` and their snake_case and
  `functions.`-namespaced spellings produce `file_edits` rows; the edit is
  recorded when the call is made, as it is for Claude, so a call that later
  failed is a `file_edits` row whose `tool_calls` row carries `is_error`.
  `Shell` records only its command, so no file is attributed to it.

  ### Usage: a context proxy, never billing

  Grok logs **no per-turn input/output token counts** (burn #489). The one
  token fact recorded here is `updates.jsonl`'s `totalTokens`, stored as
  `{"context_total_tokens": n, "source": "updates.jsonl"}` on that turn's last
  assistant event — its last message, or, for a turn answered entirely with
  tool calls, its last tool use — so a consumer can see both the number and
  what it is. It is a context-window snapshot: it
  **decreases** after a compaction, and summing it across turns is meaningless.
  Every Grok hydration therefore reports `GROK_USAGE_CONTEXT_PROXY_ONLY`,
  present or not. Nothing here estimates tokens — that is burn's job, from its
  own estimator and `xai` pricing.

  Two community adapters report that recent Grok builds *do* write a
  `turn_completed.usage` breakdown (`inputTokens`, `outputTokens`,
  `cachedReadTokens`, `reasoningTokens`, `costUsdTicks`, `modelUsage`). That
  contradicts #167, and neither claim was checked against a real session here,
  so **nothing is read from it**: recording a number this repo cannot vouch for
  is exactly the failure this parser exists to stop. Confirming it is the first
  item on the checklist below.

  ### Identity and re-reads

  `chat_history.jsonl` writes no record id. An event that joined to an update
  is keyed on that update's ACP `eventId` (`ev:<id>`), which survives the
  rebuild Grok performs on a format upgrade; an event that did not is keyed on
  its record index (`r<n>`), which does not. Tool calls and results are keyed
  on the provider's own call id (`tool:<id>`, `result:<id>`).

  **A Grok read is a replacement, not a merge.** Grok rewrites
  `chat_history.jsonl` in place on a format upgrade or a compaction, and prunes
  `compaction_checkpoints/` and `subagents/`, so the directory is a snapshot of
  what the session *currently* says happened. Every read therefore clears this
  session's Grok-owned evidence — `session_events`, `tool_calls`, `file_edits`,
  `session_markers`, `history` and the relationships whose evidence is this
  session's own `subagents/` directory — inside the same transaction that
  rebuilds it. Upserting alone would leave a tool call that is no longer in the
  transcript, a checkpoint whose file was deleted, and every row whose `r<n>`
  identity shifted when the file was rewritten, sitting in the database
  forever with nothing to distinguish them from live evidence. A relationship
  another session recorded about *this* one is not this session's to delete,
  and is left alone.

  The change stamp covers **every file the read consumes**: the transcript, the
  summary and the update stream each keep a readable marker, and
  `signals.json`, `prompt_context.json` and the sorted contents of
  `compaction_checkpoints/` and `subagents/` are folded into one digest (so a
  session with many checkpoints does not grow an unbounded stamp). Discovery,
  plain `sync` and targeted hydration all take the same stamp from the same
  function, so none of them can call a session unchanged on evidence the others
  would have re-read — a new checkpoint written after the last update row is
  new evidence, and is read as such.

  Every read in this path answers in three states: **absent** (nothing to
  record), **malformed but present** (the file's existence is itself evidence,
  so the marker is written with no detail rather than deleted), and
  **unreadable** (an error). `summary.json` is the one exception to the middle
  state, because it carries **identity** rather than detail — see below. A file the read consumes and cannot read is never
  an absent one: an unreadable `updates.jsonl` taken as "no stream" would replace exact
  event times with fallbacks and drop the token snapshots, and an unreadable
  sidecar directory taken as "no entries" would delete the checkpoints already
  stored — both while the metadata stamp, still perfectly readable,
  checkpointed that loss as the session's settled state. Only `NotFound` is
  absence. Plain `sync` isolates such a session, names it, counts it, and
  fails the source outright when nothing in the store could be read.

  A hydration that parses nothing still reports what Grok does not record: the
  provider diagnostics are stored with the hydration checkpoint and replayed on
  an `unchanged` result. An `unchanged` reader is looking at exactly the rows a
  parsing reader saw, so it is told the same things about them — above all that
  a token count it can see is a context proxy and not billing usage. A
  checkpoint written before those were persisted has none stored, and the usage
  caveat is rebuilt from the stored `token_json` instead.

  Grok hydration reports `capability: "full"` because its parser covers every
  evidence kind. The usage caveat travels with it as
  `GROK_USAGE_CONTEXT_PROXY_ONLY` on parsed and cached reads alike: Grok writes
  no per-turn billing tokens, and `updates.jsonl`'s running context total is
  stored as a labelled proxy, never as usage. Capability is defined by
  `coverage` (hydration contract 3); usage is not one of those kinds, so a
  `partial` result that covered every kind would be a contract mismatch the
  SDK rejects. The diagnostic is what tells a cost report not to add the
  proxy up.

  **A finished JSONL row that does not parse fails the read.** Grok ingestion
  *replaces* a session's evidence rather than appending to it, so a row that is
  silently skipped is a turn deleted from the stored transcript — and the
  change stamp saved after it would checkpoint that deletion as the session's
  settled state, so no later run would look again. Both `chat_history.jsonl`
  and `updates.jsonl` therefore treat a newline-terminated row that is not JSON
  as a scan error: the session is named and counted, its previous transaction
  stands untouched, and its stamp is left behind for the next run. The single
  exception is the last line of a file when it has no newline — a record still
  being written. That one is parsed if it parses (not every writer terminates
  its final line, and discarding a whole record over a missing newline loses
  evidence just as surely) and ignored if it does not. Discovery's bounded
  head/tail scans stay lenient by design: a bounded tail read can legitimately
  begin mid-record, and discovery never deletes evidence.

  **An unchanged stamp is not on its own proof a session is indexed.**
  `.sync-state.json` sits beside `history.db`, so deleting or rebuilding the
  database leaves the state file behind, and a stamp read alone would answer
  "already done" for a session with no rows at all — for a finished session,
  forever. Plain `sync` records the session id beside each stamp and re-reads
  the directory when the evidence is gone, the same guard the Codex and Claude
  walks apply. "The evidence" means what Grok actually writes, and that is
  every table this ingestion drives: not every session produces
  `session_events` — one made only of `system` lines, synthetic turns or
  encrypted reasoning is stored entirely as markers, and one whose transcript
  is empty but whose `subagents/` directory names a child has only a
  relationship. Asking about a subset re-reads such a session on every run for
  ever, which is the thing the guard exists to prevent. The state entry
  records whether the indexing run wrote any evidence at all; a session that
  wrote none is checked against its **catalog row**, which every ingestion
  writes and which for such a session is the whole of what indexing produced.
  Skipping it without asking anything is what let an empty session disappear
  permanently when `history.db` was rebuilt. The catalog row is deliberately
  *not* the check for a session that did write evidence — discovery writes
  catalog rows too, so it would stand over missing rows.

  **A malformed `summary.json` fails the read; only an absent one falls back
  to the directory name.** `info.id` becomes the session id, which keys every
  evidence row and scopes every delete a replacing read performs, so it is the
  one sidecar where "malformed but present is evidence" does not hold. A
  summary caught mid-write used to fall back to the encoded-cwd folder name:
  the transcript was stored under `local-folder`, and once the file was
  repaired the next read stored the same session under `grok-123`, leaving the
  first set of rows keyed to an id no read would ever name again — and so
  never deletable, because deletes are scoped by the id that produced them.
  Absence keeps the fallback: a session directory with no summary at all is a
  real shape, and its folder name is the only identity there is.

  **The shallow read strips the `<user_query>` envelope, because the full read
  does.** Discovery writes `sessions.first_prompt` and hydration writes
  `history.prompt` from the same typed prompt, so a wrapper stripped in one
  and kept in the other shows the XML envelope in the catalog and the typed
  text in the transcript, for one session, with nothing to say which is the
  prompt. Both go through `unwrap_user_query`.

  **Replacing one session never deletes another session's prompt.** Because
  `history` is keyed `(source, timestamp_ms, prompt)` with no `session_id`, a
  prompt two sessions both contain is one row, attributed to whichever was
  indexed first. Deleting the replaced session's rows outright took that
  shared row with it, so an unchanged session's prompt vanished from search —
  permanently, since its own files never change again. A row the replaced
  session no longer owns is therefore **re-attributed** to a session whose
  stored `session_events` still carry that prompt at that millisecond, and
  only what is left is deleted. Re-attribution rather than skipping: skipping
  would leave the row filed under a session that no longer contains the
  prompt, which is untrue and also undeletable, since the only session that
  could clean it up is the one that no longer evidences it.

  **Markers are delivered like any other evidence.** `session_markers` is in
  `delivery::schema::TABLES`, so durable delivery bootstraps and journals it
  and an export of a Grok session carries its compaction boundaries alongside
  its events. The entry is **appended** to that list and must stay last:
  `delivery_jobs.bootstrap_kind` is a persisted index into it, so inserting a
  row anywhere else silently re-points every in-flight job's bootstrap cursor
  at a different table. The delivery kind is `session_marker`, in
  `SUPPORTED_KINDS` and in the TypeScript `HistoryEvidenceKind`.

  **One rule decides what a JSONL row is, in `ingest::jsonl`.** Two passes read
  the same files — the parse, which turns rows into evidence, and the count,
  which reports `records_parsed` — and when they each decided for themselves
  they disagreed: the parse read a valid final record with no trailing
  newline, while the count stopped at the missing newline without testing it,
  so a transcript ending in an unterminated record reported one record fewer
  than had just been read, and that figure was checkpointed. Both now call
  `jsonl::classify`. The count still streams, because an `updates.jsonl` must
  not be held in memory; only the rule is shared.

  **Repeated prompts at one timestamp are one `history` row.** `history` is
  keyed `UNIQUE(source, timestamp_ms, prompt)`, without `session_id`, so two
  turns with the same text at the same millisecond collapse. Grok makes this
  visible rather than causing it: a session with no `updates.jsonl` and no
  per-record times has exactly one real timestamp, `created_at`, so two
  `continue` turns collide where another provider's per-record clock would
  separate them. Spacing them out by a millisecond each is the synthesized
  `first_ts + index` ladder this work deleted, and is not an option — a
  fabricated time is worse than a collapsed rollup. The transcript, keyed per
  record, keeps both turns; the prompt rollup keeps one. Widening that key is a
  change to a table every provider shares and is tracked separately.

  A hydration's `source_bytes` and `records_parsed` describe the whole
  directory, not the transcript: an `updates.jsonl` is routinely the largest
  file in a busy session, and reporting the transcript alone understates the
  read by orders of magnitude. Records are the complete JSONL records of
  `chat_history.jsonl` and `updates.jsonl`, plus **one per whole-file JSON
  sidecar** — `summary.json`, `signals.json`, `prompt_context.json` and each
  `compaction_checkpoints/` and `subagents/` entry — because a sidecar is one
  document, and counting it as zero would make a directory of fifty
  checkpoints look like no work at all. A `.jsonl` entry inside those
  directories is counted by record, as the older layout writes subagent
  transcripts that way. The same directory walk produces the numbers and the
  change stamp, so the two can never describe different sets of files.

  ### Delegation

  `relationship_capabilities("grok").stableChildIdentity` is **`sometimes`**,
  for the same reason as Claude's: a `subagents/` entry that records a child
  session id is a linked delegation, and the child session lives in the normal
  sessions tree; one that does not is stored as unlinked evidence, and the
  child id is never taken from the file name. A `Task`-style call in the
  transcript names no child at all — it is a `tool_calls` row, reported as
  `GROK_SUBAGENT_SPAWN_UNLINKED`, and it never invents a relationship row.

  A linked edge records whether the child is **addressable** —
  `child_has_events`, which `session_tree` reads rather than probing — and
  that is a fact about the child, read from its indexed events. Because a
  parent can be read before its child exists in the index at all, indexing a
  Grok session also refreshes the edges that point *at* it, in both
  directions.

  ### Verification status

  Grok was **not installed** on the machine this adapter was written on, so no
  real `~/.grok/sessions/**` directory was read. The fixtures under
  `crates/ai-hist/tests/fixtures/grok/` reproduce the shapes the sources below
  describe. Everything marked **Corroborated** above is stated by at least one
  source that read a real session; everything marked **Stated in #167** comes
  from burn #489's format research, which this repo did not re-derive;
  everything marked **unverified** is a shape nobody public has published.

  A maintainer with Grok Build installed can confirm or correct all of it in a
  few minutes:

  ```sh
  S=~/.grok/sessions/<encoded-cwd>/<session-id>

  # 1. Which record types and fields chat_history.jsonl really writes.
  jq -r '.type' "$S/chat_history.jsonl" | sort | uniq -c
  jq -r 'to_entries[].key' "$S/chat_history.jsonl" | sort | uniq -c
  # 2. Tool calls: {id, name, arguments}, or something else?
  jq -c 'select(.tool_calls) | .tool_calls[0]' "$S/chat_history.jsonl" | head -3
  # 3. Does a tool_result carry is_error?
  jq -c 'select(.type=="tool_result") | {keys: keys}' "$S/chat_history.jsonl" | head -3
  # 4. Which sessionUpdate kinds the stream carries, and how often.
  jq -r '.params.update.sessionUpdate' "$S/updates.jsonl" | sort | uniq -c
  # 5. Does turnStartMs exist, and where?
  jq -c 'select(.params._meta.turnStartMs or .params.update.turnStartMs) | {meta: .params._meta, update: .params.update}' "$S/updates.jsonl" | head -2
  # 6. THE IMPORTANT ONE: what a turn_completed actually carries.
  jq -c 'select(.params.update.sessionUpdate=="turn_completed") | .params.update' "$S/updates.jsonl" | head -3
  #    If that prints inputTokens/outputTokens, this repo is under-recording
  #    usage on purpose and issue #167 needs correcting — say so there.
  # 7. What signals.json, a compaction checkpoint and a subagent entry hold.
  jq -c . "$S/signals.json"
  jq -c . "$S/compaction_checkpoints/"* | head -2
  jq -c . "$S/subagents/"* | head -2
  # 8. Does summary.json name a parent for a forked or restored session?
  jq -c 'with_entries(select(.key|test("parent|fork|restore";"i")))' "$S/summary.json"
  ```

  Sources consulted (all public, September 2026):

  - [`xai-org/grok-build`, `docs/user-guide/17-sessions.md`](https://github.com/xai-org/grok-build/blob/main/crates/codegen/xai-grok-pager/docs/user-guide/17-sessions.md)
    — the vendor's own description of the session directory: every file name
    above, `updates.jsonl` as "the authoritative conversation log that drives
    `/resume`", `prompt_context.json` as "inputs the system prompt was rendered
    from", `signals.json` as "token usage and tool/turn counters",
    `subagents/` as "per-subagent metadata; child sessions live in the normal
    sessions tree", and "per-turn token and cost totals are available through
    `grok usage`".
  - [tenequm/pond #171](https://github.com/tenequm/pond/issues/171) — an
    adapter built on `updates.jsonl` *because* `chat_history.jsonl` "lacks
    per-row timestamps and undergoes in-place rebuilds"; the envelope shape,
    the chunk-coalescing rule, and correlating tool calls by ACP id.
  - [DrazThan/hermon #105](https://github.com/DrazThan/hermon/issues/105) —
    `tool_calls` as `{id, name, arguments}` "not OpenAI-style wrappers",
    `tool_result` as `{type, tool_call_id, content}`, and percent-decoded
    project directories.
  - [ferraroroberto/app-launcher #1012](https://github.com/ferraroroberto/app-launcher/issues/1012)
    — the envelope verbatim, both `method` spellings, and the kind list
    `user_message_chunk` / `agent_thought_chunk` / `agent_message_chunk` /
    `turn_completed` / `hook_execution`.
  - [princess-pi/wtft #184](https://github.com/princess-pi/wtft/issues/184) —
    `updates.jsonl` as the authoritative parse source, one turn spanning many
    events, and `costUsdTicks` at 10¹⁰ ticks per USD.
  - [telemetry-dev/stats #8](https://github.com/telemetry-dev/stats/pull/8) and
    [BrokkAi/mjolnir #989](https://github.com/BrokkAi/mjolnir/issues/989) — the
    `turn_completed.usage` breakdown recent builds are reported to write. Read
    here as a **contradiction to resolve**, not as a licence to record tokens.
  - [paperboytm/spool #512](https://github.com/paperboytm/spool/issues/512) and
    [Ishannaik/agent-sweep #219](https://github.com/Ishannaik/agent-sweep/pull/219)
    — independent confirmation of the directory layout and of
    `{type, content}` records, the latter verified against a real Windows
    install.
  - [AgentWorkforce/burn #489](https://github.com/AgentWorkforce/burn/issues/489),
    via [#167](https://github.com/AgentWorkforce/relayhistory/issues/167) — the
    original format research: no per-turn `input_tokens`/`output_tokens`, a
    `totalTokens` context proxy that decreases on compaction, the models
    `grok-composer-2.5-fast` and `grok-build`, and the tool-name list.

- **opencode** — **two storage layouts**, because both are in the field.
  RelayHistory prefers `opencode.db` when the host has it and falls back to the
  legacy JSON tree when it does not; it never reads both, because a host that
  upgraded has a stale tree sitting beside a live database.

  | Layout | Location | Environment override |
  |---|---|---|
  | SQLite (current releases) | `~/.local/share/opencode/opencode.db` | `OPENCODE_DB` |
  | Legacy JSON tree (older installs) | `~/.local/share/opencode/storage` | `OPENCODE_STORAGE_DIR` |

  The JSON tree is laid out as `session/<scope>/<sessionId>.json`,
  `message/<sessionId>/<messageId>.json` and `part/<messageId>/<partId>.json`.
  The *payloads* are identical to the `data` columns in the SQLite tables, so
  one normalizer parses both and the evidence a session produces does not
  depend on how it was stored. `crates/ai-hist/tests/opencode_parity.rs`
  asserts that by deriving a JSON tree from the SQLite fixture and comparing
  every row apart from the provenance path.

  A hydrated OpenCode session yields, per assistant message: `session_events`
  of kind `text` for each non-synthetic `text` part, `tool_use` plus a
  `tool_calls` row for each `tool` part (`tool_use_id` = `callID`, `is_error`
  from `state.status == "error"` or `state.metadata.exit != 0`), a
  `tool_result` event from `state.output`, and a `file_edits` row for
  `write`/`edit`/`patch`. Each event carries `model` as
  `"<providerID>/<modelID>"`, `provider` as the bare `providerID`, `token_json`
  as the message's `tokens` object verbatim
  (`{input, output, reasoning, cache:{read, write}}`), and `stop_reason` from
  the message's last `step-finish.reason`. A `compaction` part records a
  `session_markers` row of kind `compaction_boundary`.

  Global sync reads the live store with session-keyed queries and copies
  nothing. The old whole-database `Connection::backup` is now opt-in at both
  compile time (the `opencode-backup` feature) *and* run time
  (`AI_HIST_OPENCODE_BACKUP=1`); on a large store it is hundreds of megabytes
  of I/O for evidence the default path already reads, so it exists only as an
  escape hatch for a provider schema whose indexes make the bounded path slow.

  For the SQLite layout, discovery opens the live store with
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
| grok | the chat file's marker, `\|`, the `summary.json` marker, `\|`, the `updates.jsonl` marker, `\|x:`, a digest over `signals.json`, `prompt_context.json` and the sorted entries of `compaction_checkpoints/` and `subagents/` |
| opencode (SQLite) | `{database identity}:{schema version}:{time_created}:{time_updated}` |
| opencode (JSON tree) | `{total bytes}:{file count}:{newest mtime nanoseconds}:{digest}` over the session file, its messages and their parts |
| relay | `{newest synced timestamp}:{synced row count}` |

The OpenCode JSON-tree marker is an aggregate for a reason: the provider
appends a turn by writing *new* files under `message/` and `part/` and does
not touch the session JSON, so a marker over that file alone reports an active
session as unchanged forever. All four components earn their place —
modification time because a birth time cannot see an in-place rewrite, the
file count because a coarse filesystem clock can give an appended turn the
same mtime as the read before it, and the byte total because an in-place edit
can preserve the count. The digest folds each file's name, length and mtime
together with the contents of recently modified files, catching same-length
rewrites that land in the same filesystem clock tick. Discovery and hydration compute it with the same
function, so the catalog and the checkpoint cannot disagree about whether a
session has moved.

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
re-reads sources whose bytes never changed. It is at **5**: version 3 shipped
the prompt-only Cursor reader, 4 added that provider's injected turn times,
models and last assistant reply, and 5 qualifies OpenCode model IDs with their
provider. Without these bumps, unchanged sources would keep serving the older
cached shape forever. The cost is one re-read per source, once.

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

`session_events` carries the per-tool-result fidelity columns `tool_use_id`,
`payload_bytes`, `payload_truncated`, `payload_hash`, `call_index`,
`event_index`, `result_status`, `event_source`, `error_signal`,
`subagent_session_id` and `agent_id`, and `session_hydration_checkpoints`
carries `last_tool_result_index`. They are not all TEXT, so they are added by
the `session_events_tool_result_fidelity_v1` marker migration rather than by
the missing-column check that serves the catalog's TEXT columns. The marker is
required, so a database written before this shape is routed through the
writable open instead of being read as current and failing with
`no such column`. Rows indexed before the migration keep their text and report
no measurement.

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
- **Native (napi), tool-result fidelity** — `getSessionEventsPage(...)` carries
  the columns above on every event; `getSessionUserTurnsPage(source,
  sessionId, options?)` returns one keyset page of user turns, each with the
  ordered `[{kind, toolUseId, byteLen, isError}]` blocks its message carried.
  A block's `isError` collapses five statuses into three states, and keeps
  "known" apart from "not yet known": `true` for `errored` and `cancelled`
  (both terminal, both stated by the provider), `false` for `completed`, and
  `null` only for `running`, `unknown` and rows indexed before the column
  existed. `null` is not "no error" — it is "no answer", and the full
  `result_status` stays on the event for a consumer that needs to tell a
  cancellation from a failure.
  Both are cache-only and derived from `session_events`, so they cannot
  disagree with the transcript. Both reads a page makes — the turn headers and
  each turn's blocks — run inside one deferred read transaction, so a sync
  writing concurrently cannot hand back a header whose blocks have moved or
  vanished behind a cursor that already advanced past it.
  A turn is what arrived on one user message. Each one also names its
  `precedingMessageId` and `followingMessageId` — the nearest messages recorded
  either side of it, from either side of the conversation — so a consumer can
  stitch a turn back into the message stream without re-reading the events.
  Both are `null` only when the session recorded no named message on that
  side; an event the provider left unnamed is passed over rather than nulling
  the field, since it is not a message a consumer could reference and the named
  message behind it still borders the turn. A later block of the same turn is
  never reported as the message that follows it.
  Membership is asserted through `event_source`, never inferred from `role`:
  only `tool_result` means "a block inside a message". A Claude subagent
  notification and a Codex `function_call_output` are both stored with
  `role = 'tool_result'` and both carry their own `message_id`, so grouping on
  role invented turns that never happened — a Codex rollout with one prompt and
  three outputs reported four. Codex has no in-message grouping at all: a Codex
  turn is the prompt alone, and its tool results are read through the event
  APIs, where a standalone result belongs. `approxTokens` is deliberately absent: every
  estimate available here is a bytes-per-token heuristic, and a heuristic
  served beside measured values is indistinguishable from a measurement at the
  call site.
- **Native (napi), delegation** — `getSessionRelationships(options)` returns one
  session's edges in both directions plus the provider's capabilities;
  `getSessionTree(options)` returns the pre-order descendant tree bounded by
  `maxDepth` / `maxNodes`; `getSessionChildrenPage(options)` returns one keyset
  page of direct children. All three are cache-only, and a missing database is
  an empty result rather than an error — for the tree, the root-only result a
  session with no recorded delegation also returns.
- **TypeScript SDK** — `listSessionCatalog()` / `discoverSessions()` wrap the
  same contract for Node consumers, as do `getSessionRelationships()`,
  `getSessionTree()`, `getSessionChildrenPage()`,
  `getSessionUserTurnsPage()` / `getSessionUserTurns()` / `sessionUserTurns()`,
  and the `sessionDescendants()`
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

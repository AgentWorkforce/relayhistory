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
| **cursor** | ✓ (dir name) | ✓ (decoded path) | – | – (never) | mtime-derived | ✓ | – | – | – | – | – | – |
| **grok** | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ (if present) | – | – | – | – | – |
| **opencode** | ✓ | ✓ (directory) | – | ✓ | ✓ | ✓ | ✓ | – | – | – | – | – |
| **relay** | ✓ | – (never) | – | ✓ (synced min ts) | ✓ (synced max ts) | ✓ (earliest synced prompt) | – | – | – | – | – | – |

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
| **cursor** | ✓ | – | – | – | – | `partial` |
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
- **cursor** — `~/.cursor/projects/<encoded-path>/agent-transcripts/<id>/<id>.jsonl`.
  Cursor transcripts carry **no timestamps at all**, so `first_activity_ms` is
  always `null` and `last_activity_ms` is the file mtime. `cwd` is decoded from
  the project directory name.
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
re-reads sources whose bytes never changed.

### Transcript byte cursors

Stamps answer *whether* a file changed. They cannot answer *where*, so any
change meant re-reading the whole file — a live Claude session that appends two
kilobytes made relayhistory re-parse every byte written before it, and a fleet
machine's multi-hundred-megabyte transcript made that unaffordable.

Every transcript now carries a **byte cursor**: the offset through which its
database work has committed, the file generation that offset belongs to, and
whatever per-source parser state a resumed pass cannot re-derive from the bytes
it is about to read.

| Where | Key | What it covers |
|---|---|---|
| `session_hydration_checkpoints` / `observation_hydration_checkpoints` | `(source, session_id, location)` | the session's own primary transcript |
| `transcript_cursors` | `(source, locator)` | subagent sidecars, their `agent-*.meta.json`, child Codex rollouts, and the files the global sync walk meets |

Both carry the same four columns. `parser_state_json` is the cursor document
and the source of truth; `committed_offset`, `prefix_hash` and `dev_ino` are
projections of it written in the same statement, so a cursor can be inspected
without parsing JSON. A transcript reached from both directions — hydrated as a
session and also walked by a global sync — holds two independent cursors over
the same bytes. That is deliberate: each is one consumer's own committed
position, and every insert on the path is an idempotent upsert keyed by
provider-native identity, so a region read twice writes the same rows.

A cursor is discarded and the file re-read from zero when

```text
inode changed || mtime < cursor.mtime || size < committed_offset
  || prefix_hash mismatch
```

and the hydration result then carries a `HYDRATION_SOURCE_ROTATED` diagnostic.

`prefix_hash` is **not** the hash of every byte before the offset. It is
SHA-256 over a domain-separated header, the offset itself, the first 64 KiB of
the file and the last 64 KiB before the offset. Hashing the whole prefix would
be stronger, but it costs a read of the entire committed region on every open —
a 200 MB read to discover that one kilobyte arrived, which is the cost cursors
exist to remove. The window catches truncation and regrowth, replacement, a
rewritten head and a rewritten tail. It does not catch an edit strictly between
the two windows that preserves the total length and leaves mtime at or above
the recorded one; no provider in this catalog rewrites a transcript's middle in
place.

### Messages that are still being written

A Claude assistant message is written as several JSONL records over time, one
per content block, and only the last carries a filled-in `stop_reason`. Records
of a message whose `stop_reason` is present and `null` are **held and not
indexed**, and the committed offset backs up to the first byte of the earliest
held message, so the next pass reads it again and indexes it once, complete,
with its usage. The held count is reported as `HYDRATION_IN_PROGRESS_MESSAGES`.

A record with **no** `stop_reason` key at all is treated as finished, not as
streaming: older record shapes and sidechain records omit the field, and
deferring those would hold them back on every pass forever.

Deferral is bounded — 8 MiB or 512 messages held — and past that the oldest
held message is indexed as it stands, reported as
`HYDRATION_IN_PROGRESS_OVERFLOW`. Memory stays bounded and the reader keeps
making progress; the blocks that arrive later land as further rows under their
own record identity rather than as corrections.

Codex has no per-message completion marker, so its cursor follows the same rule
burn's `CommittedSnapshot` does: the committed offset and parser state advance
only at a `task_complete` record. A turn's token accounting is not final until
the turn is, and committing inside an open turn would freeze a cumulative
baseline mid-turn. The open turn's events are still indexed as they are read —
this is an evidence store, and a live session should be visible before its turn
ends — and re-derived on the next pass from the last committed boundary.

### A record that never got its newline

A transcript's last line may have no `\n`, and nothing in the bytes says
whether that is a record the writer has finished or half of one it is still
writing. The reader used to withhold every such line, which is right for the
second case and silently drops the last record of a complete transcript in the
first — the plugin SDK's 525-record fixture is built with `join('\n')` and
indexed 524.

The rule now: an unterminated trailing record is **indexed if it parses as
complete JSON**, because a half-written line does not. The cursor still
commits past it, so a file nobody has touched compares equal to its cursor and
is skipped outright; `resume_from` in the cursor remembers where that record
began, and if the file later grows the next pass rewinds there and reads it
again rather than resuming after a record it only half saw. Re-reading is
harmless — the row is an idempotent upsert under the same identity.

Codex keeps the older rule and withholds an unterminated line: its rollouts are
newline-terminated, and its cursor already advances only at `task_complete`.

### A message the writer abandoned

Holding a message back is a bet that the provider will finish it. If the file
stops changing the bet has lost, and holding it again on every pass would turn
"deferred" into "lost". A pass that finds the file byte-for-byte where its
cursor left it — same size, same mtime — stops deferring and indexes what it
held. A session with records still held is never reported `unchanged`, which is
what lets that pass run at all.

### Identity and metadata resume too

A Claude transcript is walked twice: once for identity and metadata
(`ClaudeMetaFold`), once to index its records. Both resume from the same
cursor document, under separate positions, because the record walk holds
records back for a message still being written and the metadata walk has no
reason to.

They have to resume together. While the metadata walk read the whole file, a
kilobyte appended to a 200 MB transcript still cost a 200 MB read, and
`bytesRead` reported the kilobyte — the shape of a success, computed over one
of the two walks. `bytesRead` is now the total across both.

The same trap caught three more reads, all of them a whole file behind a call
that looked bounded:

| Read | Was | Is |
|---|---|---|
| Codex rollout identity | `read_to_string`, then `lines().next()` | one record from the head |
| Claude sidecar enumeration | every sidecar scanned from byte zero, every hydration | each sidecar's metadata walk resumes from its own cursor |
| A sidecar's first record | `read_to_string`, then `find_map` | a bounded head read |

**Every provider read a hydration cannot avoid is in `bytesRead`.** A counter
that omits one is worse than no counter: it reports the work that was
optimised instead of the work that was done, and the omitted read is exactly
the one nobody is watching. That includes the reads that *find* the related
sessions — each Codex sibling rollout's head record during enumeration, and a
Claude sidecar's head record and metadata document while its evidence is built.

A bounded head read is bounded by a limit on the reader, not by a check
between lines: `read_until` appends until a newline or EOF, so a budget
consulted only before starting another line lets one record the size of the
file allocate the size of the file.

A record is read under a ceiling of its own. `read_until` extends its buffer
until a newline or EOF, so any cap checked *around* the call — the 8 MiB
deferral budget, for instance — can only ever notice an allocation that
already happened. One record over `MAX_RECORD_BYTES` (16 MiB) is drained past
in fixed-size chunks, never held, and reported as
`HYDRATION_OVERSIZED_RECORDS`; a record that large is evidence of corruption
rather than of a long turn.

Record bytes are decoded strictly. `from_utf8_lossy` turns an invalid byte
inside a JSON string into U+FFFD and leaves the syntax valid, so a corrupted
record parsed cleanly and was indexed as though the replacement character were
what the provider wrote. A record that is not valid UTF-8 now takes the same
path as one that is not valid JSON: skipped.

**Releasing deferred records waits out a grace window, not one observation.**
A model pauses between streamed records constantly — thinking, running a tool,
waiting on a network call — and those pauses are routinely longer than the
interval between two hydrations. Releasing on the first pass that sees an
unchanged file therefore fires during ordinary operation and publishes half a
message that nothing will retract. The cursor carries `unchanged_since_ms`,
and records are released only once the file has been still for
`QUIESCENT_GRACE_MS` (two minutes). The asymmetry justifies erring long:
releasing late costs latency on a genuinely abandoned message, releasing early
publishes a partial one.

A cheap "has this changed?" check that compares only size, mtime and inode is
not enough to skip a file. A writer that restores timestamps, or a filesystem
whose clock puts both writes in one tick, produces a rewrite with an identical
stat; the bounded prefix window is validated before a file is skipped, which
costs two seeks and at most 128 KiB and only on files that were about to be
skipped anyway.

A record the reader did not commit does not consume its line index either. The
fallback identity for a record with neither `uuid` nor `message.id` is derived
from that index, so advancing it past a half-written record gave the record a
different identity once it completed than a re-parse from zero would derive —
and the record then existed twice.

### One deferral state machine

Three separate defects turned out to be the same state machine seen from three
sides, so it is written down once:

- **A cursor at offset 0 is still a cursor.** It records the file's identity,
  size and mtime, which is what says whether anything has been appended.
  Reading "offset is zero" as "there is no cursor" meant a pass that held back
  a file's *first* record could never see the file go quiet, and deferred the
  same record forever.
- **An unterminated trailing record goes through the same deferral decision as
  any other record.** Indexing it because it parsed skipped deferral precisely
  at the tail, where a file is most likely to be mid-write.
- **Every transcript that defers gets a pass to release what it held.** The
  session's own cursor is not the only one: each Claude sidecar keeps its
  parser state in its own locator-keyed cursor, and the unchanged shortcut
  consults all of them.

A transcript's two walks share one cursor row, and the record walk never
touches the metadata walk's state — including when the record walk restarts
from zero, because rotation is something the metadata walk detects for
itself.

### What an existing install does on the first sync after upgrading

`HYDRATION_PARSER_VERSION` is 3. A checkpoint written by an earlier generation
has no cursor, and the `claude_sessions*` path → stamp maps that the global sync
walk used are dropped from the sync state file. So the first sync or hydration
after upgrading **reads every transcript once, in full, from offset 0** —
exactly what a stamp-map generation bump always did. From the second pass on,
each file is either skipped on a `stat` and one indexed point query, or resumed
from its cursor.

`codex_rollouts_v5` is deliberately **not** retired. It is the marker for the
selective user-message repair, which is a statement about the parser rather
than about file positions, and it keeps working unchanged beside the cursors.

### `bytesRead`

`hydrateSession` now returns `bytesRead`: what that call actually read from
provider files, across every walk and every file it touched — and, when more
than one source contributed, summed across them rather than taken from
whichever one won the capability rank. About the size of the append for an
incremental pass, the whole file when a cursor was rejected or the parser
generation changed, and small but **not zero** for an `unchanged` one. It is
the number a watch loop reads to tell "the tail grew" from "the whole file was
re-read". `HydrateSessionResult`'s contract version is 3.

Deciding a session is unchanged is not free and does not report as though it
were. Building the stamp reads the Codex root's `session_meta`, one head record
from every sibling rollout, and each Claude sidecar's head and metadata
document; validating the cursors reads two bounded windows per file. Those
bytes are in `bytesRead` on the unchanged path exactly as they are on every
other. A counter that omits the reads a pass could not avoid reports the work
that was optimised instead of the work that was done — and a well-formed zero
is indistinguishable from a pass that really read nothing.

### Skipping a file without reading it

Two places skip a transcript on its stat: the sync walk's
`transcript_unchanged` and targeted hydration's stamp shortcut. Both ask the
same question through `committed_prefix_matches` — are the bytes behind the
cursor still the bytes on disk? — because size, mtime and inode do not prove
byte equality, and a writer that restores timestamps produced a rewrite that
was skipped forever. Neither path advances parser state to answer it, and
hydration pays the window only when the stamp was about to skip the file: a
pass that is going to read the transcript anyway validates the same cursor
inside `TranscriptReader::open`.

### Validating costs provider reads, and they are counted

A pass hashes bounded windows to satisfy itself that the file is the one its
cursor describes: the saved cursor's window and the file's own at `open`, and
the opened region again at `commit` (reused as the stored cursor's hash when
the pass consumed exactly the file it opened). Those are provider reads, so
they are in `bytesRead` like every other — counted inside the digest function,
which is the only place they are spent, rather than added by hand at each call
site. Internally a pass also reports `validation_bytes`, so "how much of this
transcript did we read" and "what did checking it cost" are not one number
hiding the other.

The cost is **bounded by the window, not by the file**: a fixed handful of
digests per walk, each at most 128 KiB, whatever the transcript's size. On the
200 MB transcripts this work exists for it is noise; on a small transcript the
windows can add up to more than the file, which is the accepted trade for a
constant-cost check. A hydration of an unchanged 200 MB transcript reads
kilobytes, not megabytes, and never the file.

### The window is compared on every commit

Not only when the stat moved. A rewrite that preserves length and restores
mtime is exactly the rewrite a stat cannot see — the skip path already assumes
writers do that — so gating the comparison on a moved stat left the one case
nothing else catches free to publish a cursor over stale rows.

### A transcript that moved under any walk is reported

`HYDRATION_SOURCE_REWRITTEN` covers the session's own transcript, its sidecars,
their metadata documents and Codex children, and it covers the metadata walk as
well as the record walk. A pass that recorded no cursor is news; which of the
two walks noticed is an implementation detail.

### The quiescence clock

`unchanged_since_ms` is stamped from the same instant as the size and mtime it
is compared against. A pass stamps it at `open` and re-stats at `commit`, so a
stat that moved during the walk restarts the clock: otherwise the grace window
covered the walk as well, and a full re-parse of a large live transcript — on
its own longer than the window — let the next pass treat a tail that settled
seconds ago as abandoned and release a message that was still streaming.

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

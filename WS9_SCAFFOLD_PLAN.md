# WS-9 — `relayhistory` Local Capture CLI: Plan

> **Owner:** relayhistory (this agent)
> **Source of truth:** `relayhistory-cloud/docs/product-direction.md` (§6 WS-9) +
> `decisions/2026-06-20-data-trust-vendor-read-flywheel.md`.

## 0. Reality correction — WS-9 is NOT greenfield

The brief (§2 convergence map) lists `../relayhistory` as **"Does not exist yet (no
local CLI)"**. **That is factually wrong.** The recall lens already exists and ships — it
is **this repo** (`ai-hist`; `Cargo.toml` → `repository = github.com/AgentWorkforce/relayhistory`):

- **Python CLI** (`./ai-hist`, 1678 lines, zero-deps, shipping): captures **all five
  sources** the brief's Capture list wants — Claude Code, Codex, Cursor, Agent Relay,
  Trajectories — with **incremental byte-offset sync** (`.sync-state.json` — the cursor
  primitive), FTS5 search, `export`/`import`, `tag`/`untag`, `pack`, `resume`, `watch`.
- **TypeScript SDK + MCP server** (`sdk-ts/`, `mcp-package/` → `ai-hist-mcp`):
  `search_history`, `recent_entries`, `get_session`, `get_context`, `stats`,
  `search_trajectories`, `why_for_task`.
- **Rust core port in progress** (`crates/ai-hist-core` + `crates/ai-hist`, Phase 1,
  Issue #8): parity-first migration to kill Python/TS duplication and enable FTS5-in-WASM.
  Currently **scaffold only** (lib.rs/main.rs stubs; design in `PHASE1_DESIGN.md`).
- Its **trajectory parser already extracts** `decisions` + `retrospective`
  (`learnings`, `confidence`) — the exact inputs WS-1 maps. The local schema is
  `history(source, session_id, project, prompt, timestamp_ms)` + a `trajectories` table.

**So WS-9's real scope is not "build the CLI" — it is "add the cloud-sync lens to the
existing client":**

1. **Cloud outbox/push (the genuine gap).** Today `sync` imports into *local* SQLite and
   `export`/`import` move portable files; there is **no push to relayhistory-cloud**.
   Build: local-history → WS-1 `ConvergenceEnvelope[]` serialization → `POST /v1/ingest`
   with `rth_` auth + server-confirmed cursor advancement. (Note: Agent Relay is currently
   a *source* pulled via the relaycast API, not a push target — don't confuse the two.)
2. **`rth_` auth** — `login`/refresh + local token storage (none today).
3. **Incognito** — per-session capture suppression (none today).
4. **Schema reconciliation** — map the existing local `history`/`trajectories` rows onto
   the WS-1 convergence envelope (§2a).

**Open placement decision (flag for humans / Issue #8 owner):** does the cloud-sync layer
land in the **Python CLI** (ships today) or the **Rust core** (the canonical future, but
mid-port and parity-gated)? Building cloud-sync in Python now risks a second port later;
building it in Rust couples WS-9 to the Phase-1 parity timeline. **Recommendation:** put
the cloud-sync engine in `ai-hist-core` (Rust) as a *new* module that does **not** block
the parity cutover (it's additive, not a port of existing behavior), with a thin Python
shim only if cloud-sync must ship before the Rust binary reaches parity. Not guessing the
final call — surfacing the trade-off.

> **Single cursor store (burn-cloud-consultant constraint):** wherever the outbox lands,
> there must be **one** cursor store (mirroring burn's `archive_state.upstream_cursors_json`),
> not cursors split across the Python and Rust layers. If the Rust core is canonical
> storage, the outbox+cursor live there even if Python temporarily drives the push.

> **Brief erratum (cloud-expert):** the source-of-truth `product-direction.md` §2 should be
> corrected — its "`../relayhistory`: does not exist yet" row is wrong and will keep future
> agents treating WS-9 as greenfield. This §0 is the durable record; **@relayhistory-cloud /
> @human should update the brief's §2 row** to "exists as `ai-hist` (Python CLI + TS SDK/MCP
> + Rust port in progress); WS-9 = add cloud-sync lens."

---

## 1. Role & boundaries (ratified in #general)
_The sections below describe the cloud-sync lens to add to the existing client._

> **BUILD STATUS (2026-06-21):**
> - ✅ **Increment 1 — map-and-serialize layer DONE & green** in `crates/ai-hist-core/src/convergence.rs`
>   (17 tests, clean clippy, no regressions): `ConvergenceEnvelope` + `IngestRequest`/`IngestResponse`,
>   `map_trajectory` (decisions+retro fan-out, all 6 blob edge cases, ratified `finding`/`reflection`
>   kinds, `source`/`lens="trajectories"`, structured top-level `taskTitle`/`taskDescription`/
>   `taskStatus`/`projectId`/`taskRef`, **no client-side `Task:` prefix** — server enriches),
>   `map_history_entry`, self-contained `epoch_ms_to_iso`, `normalize_home_path`.
> - ✅ **Increment 2a — outbox builder DONE & green** in `crates/ai-hist-core/src/outbox.rs`
>   (6 tests): `build_outbox_batch` reads local history + trajectory rows past a `SyncCursor`
>   (monotonic `history.id` / `trajectories.rowid`), maps via convergence, applies the
>   incognito session-exclusion (skipped rows still advance the cursor), caps per-source by
>   `limit`. Pure sync rusqlite — no network. `SyncCursor` is JSON-persistable.
> - ⏳ **Increment 2b — HTTP/auth/network (binding layer) — GATED on local `wrangler dev`**
>   (now that relayhistory-cloud's `/v1/admin/mint` + `/v1/cli/login` + `/v1/ingest` are
>   code-ready; needs a non-prod Neon `.dev.vars` + `wrangler dev` running): HTTP POST client,
>   `rth_at_`/`rth_rt_` token storage/refresh (token via `/v1/admin/mint` for local), single
>   cursor-store persistence + server-confirmed advancement, the `sync` CLI command.
> - **Deferred:** `taskRef` population + full chapter-stream (`trajevent:*`) — both need ai-hist to
>   persist `task.source` / re-parse via `path` (WS-6 timeframe).

---

## 1. Role & boundaries (ratified in #general)

`relayhistory` is the **recall lens** — the unbuilt local capture client. It mirrors
burn's CLI→cloud sync *pattern* and pushes **recall/convergence records only** to
`relayhistory-cloud`.

Hard boundaries agreed with the team:
- **No cost re-capture.** Cost/usage/tool/file dimensions stay **burn-owned** and enter
  WS-1 through burn's mapping (burn-architect). The relayhistory outbox emits
  reasoning/recall/source-provenance records, never recomputed cost.
- **No parallel event model.** The outbound `records[]` payload is WS-1's convergence
  envelope verbatim. WS-9 treats it as a typed passthrough; WS-1 owns the shape.
- **Server owns tenancy.** Locally persisted `orgId`/`workspaceId` are UX/cache/routing
  only (cloud-expert). Authoritative tenancy is the server's `requireAuth` context.
  The CLI never asserts org authority via the request body.
- **`../burn` has no importable cloud-sync code** (burn-architect correction). burn today
  implements only local ingest cursors (`crates/relayburn-sdk/src/ingest/cursors.rs`,
  persisted in `archive_state.upstream_cursors_json`). `login`/`sync`/outbox are
  follow-up work there. WS-9 therefore **implements its own** auth-token storage +
  outbox + cursor advancement, mirroring burn-cloud's **wire contract** (not burn code).

---

## 2. The contract WS-9 builds against (from burn-cloud + #general confirmations)

**Endpoint (confirmed by relayhistory-cloud):** one heterogeneous batch endpoint.
```
POST /v1/ingest      (requireAuth → requireScope("rth:sync"))
  body: {
    machine:  { id, hostname?, label?, os?, cliVersion? },
    batchId:  string,                 // client UUID, idempotency key
    cursors?: Cursors,                // per-source high-water, for resume
    records:  ConvergenceEnvelope[]   // WS-1 schema — passthrough
  }
  resp: { batchId, received, accepted, cursors }   // server echoes confirmed cursors
```
- `orgId` is **never** in the body — taken from auth context server-side.
- Idempotency: server dedups on `(orgId, machineId, batchId)` in `syncBatches`; records
  dedup on their WS-1 natural key (source/session/event + machine/org), **not**
  content-hash (content-hash is the Enterprise/opaque tier's concern).

**Auth (two-phase, mirrors burn-cloud `tokens.ts`):**
1. `POST /v1/cli/login` with a RelayAuth JWT (device-flow) → mint `rth_at_*` (24h) /
   `rth_rt_*` (90d). Server stores only SHA-256 hashes.
2. Every push: `Authorization: Bearer rth_at_*`; refresh via `POST /v1/auth/token/refresh`.
   Token prefixes `rth_` avoid collision with burn's `brn_`.

---

## 2a. The convergence envelope WS-9 emits (pinned to WS-1 ADR)

> Source: `relayhistory-cloud/docs/decisions/2026-06-21-normalized-agent-event-schema.md`
> (draft for ratification). This is the concrete shape `outbox/envelope.rs` serializes.

Per-record wire shape (one heterogeneous `records[]`):
```jsonc
{
  "v": 1,
  "kind": "trajectory_event",        // recall-lens kinds; burn owns cost kinds
  "source": "trajectories" | "claude_code" | "codex" | "opencode" | ...,
  "sessionId": "…",
  "eventId": "…",                    // stable source key → natural dedup key
  "ts": "ISO-8601",
  "type": "decision" | …,            // permissive-on-read reasoning vocab
  "content": "scrubbed readable text",
  "significance": "low|med|high|critical",   // optional Learn signal
  "confidence": 0.0,                          // optional
  "tags": ["…"],                              // optional
  "record": { /* scrubbed/minimized typed provenance — raw dropped */ }
}
```
- **Natural key (server-side dedup):** `(orgId, machineId, source, sessionId, eventId)`.
  `orgId`/`machineId` come from auth context; the CLI supplies `source/sessionId/eventId`.
- **`eventId` synthesis (CLI responsibility — reviewer catches, WS-1 review pending):**
  - trajectories events have **NO native id** (trajectories-expert: `TrajectoryEventSchema`
    is `{ts,type,content,raw,significance,tags,confidence}`). The CLI must **synthesize a
    deterministic, stable** id so re-pushes hit the same key — `traj:<trajectoryId>:<chapterId>:<arrayIndex>`
    (preferred, human-readable) or a hash of `(ts|type|content)`. Non-deterministic ids
    would duplicate on every sync — this is the one blocking idempotency gap.
  - **Kind-namespace the id — RESOLVED + CORRECTED (team-reviewed).** WS-1 lands `kind` in
    the PK `(orgId, machineId, source, sessionId, kind, eventId)` and the server fallback
    is kind-generic (`${kind}:<chapterId>:<eventIndex>`). WS-9 **always emits explicit,
    deterministic, collision-free ids** so the fallback never fires. The corrected scheme
    (my earlier `reflection:<chapterId>:<retroIndex>` was wrong — see ▸ below):

    Aligned to the **ratified ADR canonical scheme** (all retro-derived use `reflection`
    kind + array sub-namespace; the `:<arrayName>:<i>` segment is the load-bearing
    collision invariant):

    | Source item | kind | eventId |
    |---|---|---|
    | chapter stream event (`Chapter.events[i]`) | event's `type` | `trajevent:<chapterId>:<i>` |
    | retrospective **summary** (single) | `reflection` | `reflection:<trajectoryId>:summary` |
    | retrospective **approach** (single) | `reflection` | `reflection:<trajectoryId>:approach` |
    | retrospective **learning[i]** | `reflection` | `reflection:<trajectoryId>:learning:<i>` |
    | retrospective **suggestion[i]** | `reflection` | `reflection:<trajectoryId>:suggestion:<i>` |
    | retrospective **challenge[i]** | `reflection` | `reflection:<trajectoryId>:challenge:<i>` |
    | retrospective **decision[i]** | `decision` | `decision:<trajectoryId>:<i>` |

    Full retrospective fan-out (no signal dropped): 1 summary + 1 approach + N learnings +
    N suggestions + N challenges + N decisions, all keyed off `<trajectoryId>`.
    `summary`/`approach` are **mapped** (prime Plan/WS-5 "how we did X" embedding targets,
    trajectories-expert). `timeSpent` is a **scalar facet field**, not its own event (it's a
    duration, not narrative).

  - **⚠️ Trajectory-lens fidelity (verified against `crates/ai-hist-core/src/lib.rs:82-95`):**
    the local `trajectories` table stores **only distilled data** — `decisions_json` +
    `retrospective_json` (opaque JSON blobs) + `search_text` + `path`. **There is no
    chapter-event stream table.** Consequences for WS-9:
    - decisions/retro are blobs → WS-9 **parses them at sync time** to fan out the
      `decision:`/`reflection:` events above (the local store hands a blob, not events).
    - the raw chapter event stream (`trajevent:<chapterId>:<i>` — prompts/thinking/tool_calls)
      is **NOT in the local DB**. Emitting those requires WS-9 to **re-parse the source
      trajectory file via the `path` column**, not sync from SQLite rows alone.
    - `persona_id` is the actor source → maps to `actorName`.
    - **Scope decision (human / Issue #8):** trajectory lens contributes **(a)** decisions +
      retrospective only (what's in the DB today — high-value, low-volume), or **(b)** the
      full event stream too (needs the `path` re-parse). **Recommendation:** ship (a) first
      (it's the prime Learn/Plan signal and already persisted), add (b) as a follow-up via
      `path` re-parse. The brief's "reasoning capture already exists" is true for the
      *distilled* layer; the raw event stream is the re-parse delta. Flagging, not guessing.
    - **Product-loop rationale for the sequencing (trajectories-expert):** (a) is the
      highest signal-per-byte the lens has — decisions/retro are the *already-distilled
      "what works"* that **Plan (WS-5)** retrieves, so (a) powers the marquee Plan use case
      at low volume now. (b) the raw `trajevent:*` stream is higher-volume/lower-density and
      is consumed by **Pair (WS-6)** (real-time failure-mode warnings need the actual
      prompt/thinking/tool sequence). So (b)'s re-parse delta is best sequenced *with* WS-6
      (the sequence-last workstream). Net: **(a) now → unlocks Plan + most of Learn; (b)
      re-parse via `path` aligned with the WS-6 timeframe** — matching each data tier to the
      stage that consumes it, not a compromise.
    - **Scrub guardrail for (b) (cloud-expert):** `path` can leak repo/client names + local
      structure, and re-parsed chapter content is raw reasoning. When (b) lands, file paths
      and any rehydrated event content get the **same server-side scrub/minimize** before
      readable Neon storage as everything else — no exception for the re-parse route.

    > ▸ **Why the correction (trajectories-expert / burn-architect / cloud-expert):**
    > **(B)** the Retrospective is **trajectory-level — it has no `chapterId`**, so retro
    > events MUST key off `<trajectoryId>`, not `<chapterId>`.
    > **(C)** `learnings[]` and `suggestions[]` indexed from 0 under one `reflection` kind
    > collide on the PK (`reflection:<id>:0` for both → silent overwrite, dropping half the
    > Learn/Plan signal). Fix is belt-and-suspenders: a **distinct `kind`** (in the PK) AND
    > an **array-name + trajectoryId segment** in the id — collision-free either way.

    Burn's `turn:<messageId>` / `inference:<requestId>` / `tool_result:<fingerprint>` are
    burn's lens, not WS-9's.
- **`ts` conversion (CLI responsibility):** trajectory `ts` is **epoch-ms `number`**
  (`z.number().int().positive()`), but the envelope/burn use ISO-8601. The CLI's mapper
  converts number→ISO before emitting; never pass the raw number.
- **`confidence` encoding — RESOLVED (cloud-expert ratified):** the store column is
  `confidence_basis_points` (int 0–10000), but trajectories emits float 0–1 in **three**
  places (`TrajectoryEvent.confidence`, `Decision.confidence` in `record.decision`,
  `Retrospective.confidence` — the last flows via retrospective→`reflection` events).
  **Contract:** the CLI emits **source-native float 0–1** on the wire (all three sources);
  the **server** ingest mapper is the authoritative owner of `toBasisPoints()` (×10000 +
  round), same as scrubbing and tenant authority. The CLI never pre-converts. If a client
  ever does pre-convert, it must use an explicit versioned field name to avoid drift.
- **`significance` is an enum string, never numeric** (`low|medium|high|critical`) — must
  NOT go through any ×10000 path; emitted as a typed enum/text. (Sits adjacent to
  `confidence`; easy to mis-coerce — calling it out so the CLI mapper keeps them separate.)
- **Actor attribution (carry it):** map `chapter.agentName` / trajectory `agents[]{name,role}`
  into an `actor`/`agentName` provenance field — the product's "which engineer/agent"
  premise needs it (trajectories-expert). Pending WS-1 adding the column.
- **Retrospective → events (don't drop):** `RetrospectiveSchema.learnings[]` /
  `suggestions[]` are the prime Learn/Plan flywheel signal. The capture adapter emits **one
  `reflection`-type convergence event per learning/suggestion** (individually embeddable),
  rather than dropping the retrospective.
- **What WS-9 populates (recall lens):** `v, kind, source, sessionId, eventId, ts, type,
  content` (scrubbed), `significance/confidence/tags`, and a minimized `record`.
- **What WS-9 leaves null (cost lens = burn-owned):** `model`, token columns,
  `costUsdMicros`, `toolName/toolStatus`, `retries`, `durationMs` — these populate from
  burn's lens via burn-architect's mapping, not recaptured here. (`filesTouched`/
  `codeChurn` may be carried when they come from a recall/trace source, e.g. agent-trace,
  but cost stays burn's.)
- **`embedding`** is computed server-side; the CLI never sends vectors.
- **`record`** is already minimized client-side: `raw` dropped wholesale, only bounded
  typed provenance retained — matching the ADR's §Scrubbing two-policy rule.
- **No double-scale shadowing in `record` (trajectories-expert):** any scalar promoted to
  a typed envelope field (esp. `confidence`, `significance`) must **not** be re-stored at a
  different scale/type inside `record.decision`/`record.retrospective`. The CLI strips
  promoted scalars from the minimized `record` (single source of truth), so a reader never
  sees `confidence=8000` in a column and `0.8` in the JSON.

Rust type sketch (`outbox/envelope.rs`): a `ConvergenceEnvelope` struct with
`#[serde(skip_serializing_if = "Option::is_none")]` on every optional field, an
open-vocab `type: String` (permissive-on-read), and `record: serde_json::Value` already
scrubbed/minimized before construction.

---

## 3. Repo topology (`../relayhistory`, new)

Mirror burn's Rust+TS layout (`crates/` SDK + `crates/*-cli` binary). Proposed:

```
relayhistory/
  Cargo.toml                      # workspace
  crates/
    relayhistory-sdk/             # capture + local store + sync engine (lib)
      src/
        store/                    # local SQLite ledger (mirrors burn ledger/)
          schema.rs               #   events table, archive_state (cursors), config
          paths.rs                #   ~/.agentworkforce/relayhistory/ home
          db.rs                   #   WAL + busy_timeout, two-db split if needed
        capture/                  # per-tool source adapters
          claude_code.rs
          codex.rs
          opencode.rs
          mod.rs                  #   Source trait; permissive/open-vocab type field
        cursors.rs                # per-source resume cursors (copy burn pattern)
        outbox/                   # NEW (burn doesn't have this yet)
          envelope.rs             #   ConvergenceEnvelope = WS-1 schema (typed passthrough)
          batch.rs                #   batchId gen, batching, accepted/cursor advance
          client.rs               #   HTTP POST /v1/ingest, retry/backoff
        auth/
          tokens.rs               #   rth_at_/rth_rt_ local storage + refresh
          login.rs                #   device-flow → POST /v1/cli/login
        incognito.rs              # per-session capture suppression gate
        tenancy.rs                # machineId gen/persist; org/wks cache (non-authoritative)
    relayhistory-cli/             # `relayhistory` binary
      src/commands/
        login.rs  capture.rs  sync.rs  status.rs  incognito.rs
  docs/
    decisions/                    # WS-9 ADRs
```

> Repo-vs-`crates` and Rust-vs-TS split should match whatever burn lands on; if the
> team prefers a TS-first client, the same module boundaries apply.

---

## 4. Local store (mirror burn, minus cost tables)

- **Home:** `~/.agentworkforce/relayhistory/` (env override `RELAYHISTORY_HOME`),
  sibling to burn's `~/.agentworkforce/burn/`.
- **`relayhistory.sqlite`:** captured convergence events (pre-scrub, local), plus
  `archive_state` single-row table holding per-source resume cursors
  (`upstream_cursors_json`) — same shape as burn's `Cursors`/`FileCursor` discriminated
  union, extended with relayhistory source kinds.
- **Cursor design copied from burn:** `{kind, inode, offsetBytes, mtime_ms, …}` per
  source file; update only on change; survives renames. This is the one piece of burn
  to copy as a *pattern* (it's the only resumability primitive that actually exists in
  `../burn` today).
- **No `turns`/`inferences`/`ledgerEvents` tables** — those are burn's. Local store holds
  recall/reasoning/source events keyed by WS-1's natural key.

---

## 5. Incognito (the WS-9 acceptance criterion to prove)

- Per-session suppression: when incognito is on for a session, capture adapters **drop
  the event before it is written to the local store** (not "captured then filtered") —
  so incognito data never lands on disk and never enters the outbox.
- Controls: `relayhistory incognito on|off`, an env var for shell-scoped sessions, and a
  per-source config flag. Status surfaced in `relayhistory status`.
- **Verification (DoD):** a session marked incognito produces zero rows in the local
  store and zero records in any `/v1/ingest` batch. This is an explicit acceptance test.

---

## 6. Sync engine (the new code, no burn equivalent to import)

1. `relayhistory capture` (watch or one-shot) scans source files → adapters →
   (incognito gate) → local store, advancing per-source cursors.
2. `relayhistory sync`:
   - load unsent events above the last-confirmed cursor,
   - serialize to WS-1 `ConvergenceEnvelope[]`,
   - generate `batchId`, attach `machine` + `cursors`,
   - `POST /v1/ingest` with bearer `rth_at_*`, retry/backoff on 5xx,
   - on `{accepted, cursors}` response, **advance local cursors to server-confirmed
     values** (durable outbox: a crash mid-sync re-pushes the same `batchId` safely).
3. Token auto-refresh when `rth_at_` is near expiry.

---

## 7. Dependency gates (what unblocks the build)

| Gate | Owner | What I need | Then I can |
|------|-------|-------------|------------|
| **WS-1** | relayhistory-cloud | Ratified convergence event schema (typed envelope, source/session/event natural key, permissive-on-read type vocab) | Fill `outbox/envelope.rs` with the real record shape; finalize local store columns |
| **WS-2** | relayhistory-cloud | `/v1/ingest` + `/v1/cli/login` + `/v1/auth/token/refresh` live on Neon (dev tier) | Wire `outbox/client.rs` + `auth/` against a real endpoint; run end-to-end + incognito acceptance test |
| **WS-3** | relayhistory-cloud | Scrubber lib location/interface | Decide client-side pre-scrub vs. server-only; raw prompt/`raw` passthrough is the top leak risk (trajectories-expert) |

Buildable **before** gates: §3 topology, §4 local store + cursors, §5 incognito,
§6 sync-engine plumbing with `ConvergenceEnvelope` as a typed placeholder, §2 auth-token
local storage. The only schema-coupled surface is the contents of `records[]`.

---

## 8. Open question deferred to humans / WS-1

- **Scrub location (WS-3 open Q):** **Server-side ingest scrubbing is the compliance
  boundary** (cloud-expert) — it is non-negotiable and must run before readable Neon
  storage, because clients can be old, disabled, misconfigured, or bypassed. Any
  client-side scrub WS-9 adds is an *additional preflight safety layer*, not the
  boundary. Final scrub interface owned by WS-3.
- **Two scrub policies, not one (trajectories-expert):** treat `content` and `raw`
  differently —
  - **`content`** (readable narrative, pgvector-embedded): **redact-in-place**, preserve
    the signal. This runs **server-side at ingest** where the WS-3 scrub corpus lives;
    losing it kills default-tier value.
  - **`raw`** (arbitrary tool passthrough — full payloads, transcript blobs, env dumps):
    **drop/minimize by default**, not needed for embedding/retrieval, hardest to scrub
    reliably, top leak risk. WS-9 will **drop/minimize `raw` client-side before it leaves
    the machine** (cheapest, safest place to do it); on the default tier keep only a
    documented allowlist (see below).
  - **Home-dir path normalization — WS-9 owns this client-side (live PII gap,
    trajectories-expert).** The current WS-3 scrubber (`scrub.ts` SECRET_PATTERNS) redacts
    keys/tokens/credentials/emails but **NOT filesystem paths containing usernames** —
    `/Users/khaliqgant/Projects/...` passes through. This leaks **today** via the
    already-mapped `filesTouched`/`filesChanged`/`codeChurn`/`path` fields on the default
    readable tier. The **CLI is the ideal fix point** because it knows its own `$HOME`:
    normalize `/Users/<name>/` and `/home/<name>/` → `~/` (and Windows `C:\Users\<name>\`)
    on all path fields **before emit**. WS-3 must still add the same normalization
    server-side (compliance boundary — clients can be bypassed); logged to known-gaps either
    way. This is a net-new, cheap, high-value item independent of the (b) re-parse decision.
    on the default tier keep only a
    **documented allowlist** of provenance fields WS-1/WS-3 declares needed. Full
    arbitrary `raw` belongs behind an explicit Enterprise/E2E or local-only retention
    boundary unless humans ratify a narrower default-tier exception.
  - **One boundary:** server-side ingest scrub remains the single enforcement point over
    *both* fields before anything readable reaches Neon — client behavior never relaxes
    it. Two policies, one boundary.
  - **`raw` is dropped wholesale on the default tier (trajectories-expert).** The useful
    provenance is *already typed* outside `raw` — event `ts`/`type`/`significance`/
    `confidence`/`tags`, chapter `id` + agent `{name,role}`, trajectory `id`/`task.source`/
    `status`/`commits`/`filesChanged`, trace `contributor.model_id` + file path/ranges.
    These are bounded and low-leak; none require dipping into `raw`. So there is no
    field-allowlist *inside* `raw` (it's untyped by definition) — WS-1 lifts any genuinely
    needed provenance OUT of `raw` into typed columns at mapping time, and WS-9 drops the
    `raw` blob wholesale client-side on the default tier. Full arbitrary `raw` →
    Enterprise/E2E or local-only.
  This gives WS-9 a concrete client-side responsibility (`raw` minimization) without
  taking over the compliance boundary (server-side `content` redaction).

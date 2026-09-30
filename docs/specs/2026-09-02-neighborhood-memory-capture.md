# Neighborhood memory — the capture and client work in `relayhistory`

**Status:** Proposed · **Date:** 2026-09-02 · **Owner:** relayhistory (Rust core, TS SDK)
**Cloud spec:** `relayhistory-cloud/docs/specs/2026-09-02-neighborhood-memory.md` (the system of record, miner, pack API, evaluation)
**Decision record:** `relayhistory-cloud/docs/decisions/2026-09-02-project-scoped-memory-over-convergence-store.md`
**Provenance:** `relayhistory-cloud/docs/research/2026-09-02-agent-memory/README.md` — how these specs were produced

## 1. Why this repo comes first

The hosted design — a project-scoped, cited, bi-temporal memory tier over `convergence_events`, mined off
the hot path and served under a token budget — depends on what this client pushes. Two of the three
research lanes independently read this crate and found that, as pushed today, prompt events carry no
project, trajectories are pushed once and never updated, and file and outcome signals never leave the
machine. With that input the cloud miner's deterministic triage has almost nothing to filter on. The
synthesis ruled: **P0 is capture, not the new table.** Everything in the cloud spec's delivery order
starts after §2 below lands.

This spec is a substrate task in the sense of `docs/substrate-improvements-proposal.md`: relayhistory
stays the lossless ledger; the recall brain lives in the cloud tier.

## 2. P0 — capture fixes (blocking)

Verified against `main` on 2026-09-02.

### P0-1 `project_id` on every history event

`crates/ai-hist-core/src/convergence.rs:156` — `map_history_entry` sets `project_id: None`.

- Populate from `history.project`, falling back to the git remote of the session's cwd, normalized to the
  canonical form the cloud's `project_aliases` expects (repo slug, not a filesystem path).
- Emit `project_id` on `kind=prompt` rows too; the cloud Pair filter currently admits NULL rows precisely
  because these are all NULL, and that admission is removed once this lands.
- Acceptance: a synced session shows a non-null `projectId` on every envelope in the outbox batch; a
  fixture test pins the mapping for a path project, a git-remote project, and an unknown project (explicit
  `unknown`, never NULL).

### P0-2 Outbox: file, outcome and update signals

`crates/ai-hist-core/src/outbox.rs` — header comment, lines 9–12: cursor is `history.id` +
`trajectories.rowid`; "trajectory rows that are updated after first sync are not re-pushed … catching
updates would need an `updated_ms` watermark (deferred)."

- **`filesTouched`** on session and trajectory envelopes, from `file_edits`. This is the structural signal
  the miner's file-overlap gate and the pack's file pool run on.
- **`session_outcome`** envelopes from `session_commit_links` (commit sha, `match_method`, `confidence`,
  numstat). The cloud already accepts `kind=session_outcome` (`ingest.ts:72`) and Learn already reads
  `session_outcomes.reverted`; nothing produces them today.
- **`updated_ms` watermark** beside the rowid cursor so revised trajectories (retrospectives added after
  the run, decisions amended) re-push. The server upsert is already safe for re-push.
- Acceptance: a trajectory edited after first sync appears in the next batch; a session with commits
  produces exactly one `session_outcome` per linked commit; fixtures pin the envelope shapes.

### P0-3 Nothing else changes in the envelope

The WS-1 convergence contract (`relayhistory-cloud/docs/decisions/2026-06-21-normalized-agent-event-schema.md`)
is unchanged. These are field population fixes, not schema changes.

## 3. P1 — client and SDK surfaces

### 3.1 `ai-hist pack` — reuse the shape, replace the internals

The Grok lane reports that local `pack` is FTS over prompts, truncating each hit to roughly
`tokens × 4` characters, and does not pack trajectories; and that the TS SDK's `whyForTask` is a `LIKE`
with `limit 1` because sql.js has no FTS5. The spec author did not locate the `× 4` truncation in the
tree; treat the exact mechanism as lane-reported and the direction as settled:

- Count with a real tokenizer for the target model (the cloud spec requires it for budget charging;
  the local pack should not disagree with the cloud pack about what a token is).
- Pack trajectories (`decision | finding | reflection`) ahead of raw prompts; never dump prompts wholesale.
- Emit the same item shape as the cloud pack (`statement`, `impact`, `evidenceClass`, `citations[]` by
  natural key, `tokens`, `memoryTruncated`) so a flow can run against local SQLite in a sandbox and
  against Neon in the cloud with one consumer.
- `whyForTask` in `sdk-ts`: match the Rust `why_for_task` semantics or delegate to the CLI; a `LIKE … limit 1`
  is the wrong implementation of the right name.

### 3.2 `ai-hist learn` — the extraction prompt family

`crates/ai-hist/src/learn.rs` already distills into `decisions[] / lessons[] / conventions[]` with a
strict JSON schema and fail-closed parsing. The cloud miner's extractor reuses this family with a
project-relevance gate in front and a required `impact_on_subject` field. Keep the schema in one place
(this crate) and version it; the cloud records `extractor_version` per claim.

### 3.3 `f.memory` for Flows

Flows' `f.memory.recall / why / learn` (`flows/docs/SURFACE.md`) compile to the cloud API
(`POST /v1/memory/pack`, and the existing learn path). The SDK helper should:

- take `{ subjectProject, task, files, budgetTokens, asOf }` and return the pack unchanged, with
  `packDigest` and `retrievalLogId` so the consuming step can journal exactly what it was charged for;
- fall back to the local pack when the cloud is unreachable **only** when the caller asks for it, and
  mark the pack `source: local` — a silent fallback would make a machine-local answer look like a
  team answer.

## 4. Enterprise / E2E tier

The vendor-readable cloud miner cannot serve orgs on the opaque tier (`relayhistory-cloud/docs/encryption.md`). For those orgs extraction runs inside the tenant boundary — in
this client or a tenant-hosted worker — and pushes opaque derived artifacts. Whether an opaque derived
memory can preserve verifiable citations without exposing content to the vendor is an open question
(cloud spec §8); confirm the relayfile proof does not target an E2E org before building anything here.

## 5. Measurements to take before the cloud miner is built

Both are client-side questions and neither lane measured them:

1. **Distill coverage.** What fraction of rows this client pushes are `lens=learn` / trajectories versus
   raw `kind=prompt`? Neighborhood quality is bounded by this. `ai-hist coverage` (PR #55) is the natural
   place to report it per machine.
2. **Machines that go mute.** `coverage` already shows machines stop pushing; for the proof, the
   relayfile agent's own sessions must be pushed reliably or the pack will cite stale evidence.

## 6. Acceptance for this repo

- P0-1, P0-2 merged with fixture tests; a synced session from a real machine shows non-null
  `projectId`, `filesTouched`, `session_outcome` rows, and a re-pushed trajectory in the cloud's
  `convergence_events`.
- Coverage reports distill share per machine.
- `ai-hist pack` and the cloud pack agree on item shape and token counting on a shared fixture.

## 7. Provenance, briefly

Produced from a three-lane research run (Claude, Codex, Grok; two subagents each; one synthesizer)
executed as `examples/research/research.flow.ts` in `AgentWorkforce/flows` on 2026-09-02. The full
record — question, three lane reports, synthesis with lane-attributed rulings, and the verification table
for every file-level claim above — is in
`relayhistory-cloud/docs/research/2026-09-02-agent-memory/`. The claims in §2 were re-checked against this
repo's `main` at 20f6250 while writing this spec.

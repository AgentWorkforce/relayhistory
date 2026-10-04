# The History service engine is public; self-hosting is an optional deployment

- **Status:** Accepted
- **Date:** 2026-10-04
- **Related:** [2026-09-19 ADR, relayhistory owns session
  sourcing](2026-09-19-relayhistory-owns-session-sourcing.md); relayhistory-cloud's
  `docs/decisions/2026-09-19-hosted-neon-only.md` (hosted persistence stays Neon-only)

## Context

The History service — durable delivery of normalized session evidence, recall over it,
scoped service-local tokens and the `sessions` PostgreSQL schema — lived only in the
private relayhistory-cloud repository, wired directly to Neon's HTTP driver and
Cloudflare Worker bindings. Nobody could run the service that consumes this
repository's export contract without Agent Relay, Neon and Cloudflare accounts, and any
second implementation would have been a fork of the evidence handling: a second
scrubber, a second receipt protocol, a second schema.

## Decision

`packages/engine` publishes `@relayhistory/engine`, the service's one implementation.
`createHistoryEngine(deps)` returns the complete Hono application; the hosted Worker
and a self-hosted Node server both run it and differ only in `deps`:

| Dependency | Hosted (Worker + Neon) | Self-hosted (Node + PostgreSQL) |
| --- | --- | --- |
| `database` | Neon HTTP Drizzle client per request | node-postgres pool |
| `verifyBearer` | RelayAuth / Agent Relay identity | none: service-local tokens only |
| `embeddings` | configured provider | optional; absent stores events without vectors |
| `enrichSessions`, `observeDeliveryBatch`, `middleware`, extra routes | session evaluation, PostHog, request logs, hosted-only features | none required |

In the engine, and therefore identical in both deployments:

- Delivery protocol 1: request parsing, server-side scrubbing, content digests,
  revision and batch-identity conflicts, tombstones, durable content-addressed receipts
  and their exact replay, bounded readback paging.
- Recall (sessions, events, search, transcript replay, catalog, thread) and
  conversation turns, with tenancy and workspace narrowing taken only from the
  authenticated token.
- Service-local tokens (`rth_at_`, `rth_st_`), scope enforcement and service-token
  management.
- The `sessions` schema: the Drizzle definition and the migration SQL with its checksum
  ledger. The migration files are the hosted service's files byte for byte, so both
  deployments share one ledger history and an applied migration is never edited.

Not in the engine: pricing and cost derivation, activity classification,
similarity-based linking, session analysis and briefs, project and epic rollups,
digests, memory, reflex, identity exchange (`/v1/cli/login`, `/v1/admin/mint`) and
telemetry export. Per-session rollups (`session_rollups`, which `GET /v1/sessions`
reads) and explicit lifecycle links (`GET /v1/sessions/:id/thread`) are recall, and
are in the engine.
They stay hosted features plugged in through `deps`, consistent with the 2026-09-19
ownership boundary. Costs the client reports are stored and read back, never computed.

A self-hosted deployment gets its first credential from `bootstrapServiceToken`, a
database operation run by an operator holding the database credentials. There is no
unauthenticated HTTP bootstrap endpoint: `POST /v1/auth/service-tokens` requires an
existing credential and only narrows it, so nothing reachable from the network creates
a token without already holding one, and the hosted `/v1/admin/mint` stays disabled.

The main entry is runtime-neutral (Web APIs only) so it bundles for Workers; the Node
migration runner is the separate `@relayhistory/engine/migrations` entry.
`hono` and `drizzle-orm` are peer dependencies so a host runs one instance of each.

## Alternatives rejected

- **Keep the service private; document the wire protocol.** Self-hosters would
  reimplement scrubbing, digests and receipt semantics, and every divergence would be a
  data-integrity bug on one side.
- **Move the whole hosted product into this repository.** Cost, classification and
  linking belong to burn or to the hosted product, not to the evidence owner; moving
  them would breach the 2026-09-19 boundary.
- **Fork the schema per deployment.** Two migration histories cannot share a checksum
  ledger, and recall SQL would diverge with them.
- **An HTTP bootstrap endpoint guarded by a shared secret.** A network-reachable minting
  path is exactly what the hosted service disables in production; database access is
  already the operator's root of trust.
- **Bundle a PostgreSQL driver.** The Worker needs Neon HTTP and Node needs a TCP pool;
  the host owns the connection and the engine needs only Drizzle.

## Consequences

- Hosted and self-hosted deployments accept, store and return evidence identically. A
  change to the protocol, scrubbing or schema is made once, here, and both pick it up.
- `@relayhistory/engine` releases in lockstep with the other packages. The hosted
  service depends on a published version, so a change it needs ships in a release first.
- New `sessions` migrations land in `packages/engine/migrations`; the hosted repository
  applies the same files over its Neon transport.
- The engine's tests run against PGlite (with pgvector and pg_trgm) and against real
  PostgreSQL with pgvector in CI, so neither runtime's driver is the only one proven.

# @relayhistory/engine

The RelayHistory service: durable delivery of normalized session evidence, recall over
it, conversation turns, scoped service tokens, and the PostgreSQL `sessions` schema
with its migrations. The hosted service (Cloudflare Worker + Neon) and self-hosted
deployments (Node + PostgreSQL with pgvector) run this same code; they differ only in
the dependencies they inject.

```sh
npm install @relayhistory/engine hono drizzle-orm
```

`hono` and `drizzle-orm` are peer dependencies. The main entry uses only Web APIs, so it
bundles for Workers; the migration runner (`@relayhistory/engine/migrations`) is
Node-only.

## Run it

A Node host also needs a server adapter and a PostgreSQL driver:

```sh
npm install @relayhistory/engine hono drizzle-orm @hono/node-server pg
```

```ts
import { serve } from "@hono/node-server";
import { drizzle } from "drizzle-orm/node-postgres";
import pg from "pg";
import {
  createHistoryEngine,
  embeddingProviderFromEnv,
  expireBabysitterEvidence,
  schema,
} from "@relayhistory/engine";
import { applyMigrations } from "@relayhistory/engine/migrations";

const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });

const migrator = await pool.connect();
try {
  await applyMigrations(migrator);
} finally {
  migrator.release();
}

const db = drizzle(pool, { schema });
const app = createHistoryEngine({
  database: () => db,
  embeddings: () => embeddingProviderFromEnv(process.env), // optional
});
serve({ fetch: app.fetch, port: 8787 });

// Retained control-plane evidence expires on a schedule. A failed tick is retried by
// the next one.
setInterval(() => {
  expireBabysitterEvidence(db).catch(() => console.error("evidence expiry failed"));
}, 60_000);
```

Use `drizzle-orm/node-postgres`: the engine reads raw query results as `{ rows }`.

The database needs the `vector` extension (pgvector) and, for fast substring search,
`pg_trgm`; without `pg_trgm` its two trigram indexes are skipped. PostgreSQL 16 or
newer.

## First credential

A deployment without an external identity provider mints its first token directly in
the database. There is no HTTP endpoint for this.

```ts
import { bootstrapServiceToken } from "@relayhistory/engine";

const issued = await bootstrapServiceToken(db, {
  orgId: "acme",
  workspaceId: "default",
  label: "laptop uploader",
  scopes: ["rth:sync", "rth:read"],
  expiresInDays: 90,
});
// issued.token (rth_st_…) is shown once; only its SHA-256 hash is stored.
```

The token's tenant is the stored row's `orgId`/`workspaceId`; no request can select
another. A holder can mint narrower tokens with `POST /v1/auth/service-tokens`; only
the first credential needs database access.

## API

All built-in `/v1` routes need `Authorization: Bearer <token>`; a host's
`publicRoutes` do not.

| Route | Scope | |
| --- | --- | --- |
| `GET /health` | — | liveness |
| `POST /v1/delivery/batches` | `rth:sync` | delivery protocol 1: upload a batch, get a durable receipt |
| `GET /v1/delivery/limits` | `rth:sync` | batch record and byte limits |
| `GET /v1/delivery/records` | `rth:read` | read back retained records (requires `X-RelayHistory-Expected-Account`) |
| `POST /v1/ingest` | `rth:sync` | legacy convergence-event ingest |
| `GET /v1/sessions` | `rth:read` | sessions, newest activity first |
| `GET /v1/sessions/:id/events` | `rth:read` | one session's transcript, in order |
| `GET /v1/events` | `rth:read` | search across sessions (`?q=`) |
| `GET /v1/sessions/:id/catalog` | `rth:read` | delivered session catalog (`?source=`) |
| `GET /v1/sessions/:id/thread` | `rth:read` | lifecycle links and outcomes (`?source=`) |
| `POST`/`GET /v1/sessions/:id/turns`, `GET /v1/sessions/:id/metadata` | `rth:sync`/`rth:read` | conversation turns |
| `POST`/`GET /v1/auth/service-tokens`, `DELETE /v1/auth/service-tokens/:id` | any | mint (narrowing only), list and revoke service tokens |

A delivery batch's `account_id` must equal `deliveryAccount(auth)` —
`"relayhistory:" + sha256(JSON.stringify([orgId, workspaceId ?? ""]))` — so a client
configured for one tenant can never write into another with a swapped token.

## Dependencies

`createHistoryEngine(deps)`:

| Field | |
| --- | --- |
| `database(c)` | the request's Drizzle database; `undefined` answers 503 `not_configured` |
| `verifyBearer?(token, c)` | a second identity provider for non-service-local bearers; throw `AuthError` for a specific 401 |
| `embeddings?(c)` | embedding provider for `/v1/ingest`; absent stores events without vectors |
| `enrichSessions?(c, db, auth, sessions)` | in-place enrichment of an organization-scoped `GET /v1/sessions` page |
| `observeDeliveryBatch?(c)` | telemetry: returns a callback that receives each delivery batch's outcome |
| `reportError?(error, c)` | receives unexpected request failures; the default logs only the error's name and code, never SQL, rows or bearers |
| `middleware`, `rootRoutes`, `publicRoutes`, `routes` | host middleware and extra routes (`routes` sit behind auth) |

Hosts that assemble their own app can use `createHistoryRoutes(deps)` and
`createRequireAuth(deps)` directly.

## Migrations

`migrations/*.sql` is the schema of record. `applyMigrations(client)` takes a
transaction-scoped advisory lock, applies pending files in name order under a checksum
ledger (`sessions.__migrations`), builds the hot-table trigram indexes concurrently,
and completes the 0029 delivery projection and 0030 session rollup rollouts in short
resumable batches. A changed checksum on an applied file fails the run; files are never
edited once released. Hosts that need budgets on the rollouts call
`rolloutDeliveryProjection` and `rolloutSessionRollups` themselves.

## Development

```sh
npm ci
npm run typecheck
npm test                                   # PGlite with pgvector + pg_trgm
DATABASE_URL=postgres://localhost/postgres npm run test:postgres   # real PostgreSQL
```

`test:postgres` creates and drops one database per test file, so its role needs
`CREATEDB`.

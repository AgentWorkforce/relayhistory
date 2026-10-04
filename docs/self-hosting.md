# Self-hosting the History service

`packages/server` runs the History service — the same `@relayhistory/engine`
implementation the hosted service runs — on Node and ordinary PostgreSQL with
pgvector. It needs no Agent Relay, Neon or Cloudflare account. Machines upload the
sessions you select; you search and read them back over the same HTTP API.

| Route                                  | Scope      | Purpose                                                                             |
| -------------------------------------- | ---------- | ----------------------------------------------------------------------------------- |
| `POST /v1/delivery/batches`            | `rth:sync` | Durable upload (protocol 1): receipts, replay, conflicts, tombstones                |
| `GET /v1/delivery/limits`              | `rth:sync` | Server batch limits                                                                 |
| `GET /v1/sessions`                     | `rth:read` | List sessions, newest first                                                         |
| `GET /v1/events?q=TEXT`                | `rth:read` | Search across sessions                                                              |
| `GET /v1/sessions/:id/events`          | `rth:read` | One session's transcript, in order                                                  |
| `GET /v1/sessions/:id/catalog?source=` | `rth:read` | One session's delivered catalog: branch, repository, models, relationships, markers |
| `GET /health`, `GET /ready`            | none       | Liveness; readiness (database reachable, not draining)                              |

Every `/v1` route authenticates a bearer token and takes the tenant (organization and
workspace) from the token's stored row, never from the request.

## Run with Docker Compose

Requires Docker with Compose v2. From a clone of this repository:

```bash
export POSTGRES_PASSWORD=$(openssl rand -hex 24)   # keep it somewhere safe
docker compose -f packages/server/compose.yaml up -d --build --wait
curl -s http://127.0.0.1:8080/ready
# {"ok":true,"service":"relayhistory"}
```

The server applies database migrations and the projection rollouts at every start, so
the first start on an empty volume creates the schema; after an upgrade the first start
also finishes any projection backfill before serving. PostgreSQL data lives in the `postgres-data` volume;
`docker compose down` keeps it, `down -v` deletes it.

The port binds to loopback. To serve other machines, put a TLS-terminating reverse
proxy (Caddy, nginx, a cloud load balancer) in front of port 8080 and point machines at
its `https://` URL; bearer tokens must not cross a network in clear text. Set
`RELAYHISTORY_BIND=0.0.0.0` only when that proxy runs on another host.

## Run on Node

Requires Node 22.16+ and PostgreSQL with the `vector` extension available (CI runs
PostgreSQL 17 with pgvector); `pg_trgm` is used for faster search when present. The migrations create both
extensions, which needs a role allowed to `CREATE EXTENSION`; on a managed database,
create them once as an administrator and run the server as an ordinary owner role.

```bash
npm ci --prefix packages/engine && npm run --prefix packages/engine build
cd packages/server && npm ci && npm run build
export DATABASE_URL=postgres://history:SECRET@db.internal:5432/history
node dist/cli.js serve
```

It listens on `127.0.0.1:8080`; set `HOST=0.0.0.0` only when the TLS proxy runs on
another host.

`node dist/cli.js migrate` applies migrations without serving, for a deploy step that
runs them separately. The CLI examples below write `relayhistory-server`, which is how
the image names it; on bare Node run `node dist/cli.js` from `packages/server`.

## Configuration

| Variable                                | Default                                              | Meaning                                                                         |
| --------------------------------------- | ---------------------------------------------------- | ------------------------------------------------------------------------------- |
| `DATABASE_URL`                          | required                                             | `postgres://` URL; standard TCP connection                                      |
| `PGPASSWORD`                            | unset                                                | Database password when the URL carries none (any characters; Compose uses this) |
| `HOST` / `PORT`                         | `127.0.0.1` / `8080` (the image sets `HOST=0.0.0.0`) | Listen address                                                                  |
| `RELAYHISTORY_DB_POOL_MAX`              | `10`                                                 | Pool connections                                                                |
| `RELAYHISTORY_SHUTDOWN_TIMEOUT_MS`      | `15000`                                              | Drain time for in-flight requests on SIGTERM                                    |
| `RELAYHISTORY_RETENTION_INTERVAL_MS`    | `60000`                                              | Interval of the job that clears expired retention-bounded evidence              |
| `RELAYHISTORY_RUNTIME_ROLE`             | unset                                                | Role granted access to the `sessions` schema after migrations                   |
| `EMBEDDING_API_KEY` or `OPENAI_API_KEY` | unset                                                | Optional embeddings for `POST /v1/ingest`; upload and recall never need them    |
| `EMBEDDING_API_URL`, `EMBEDDING_MODEL`  | OpenAI defaults                                      | OpenAI-compatible embedding endpoint                                            |

Logs are JSON lines on stderr. They never contain tokens, the database URL, request
bodies or driver error text.

## Credentials

Tokens are minted by whoever holds the database credentials; no HTTP route creates a
token without an existing one. Each token is bound to one organization and workspace.
Machines that upload into one workspace share its history; different workspaces or
organizations never see each other's.

```bash
# Compose: run the CLI inside the server container. Elsewhere: relayhistory-server ...
docker compose -f packages/server/compose.yaml exec -T server \
  relayhistory-server token create --org acme --workspace main \
    --label laptop --scopes rth:sync,rth:read --out - > laptop-token.json
chmod 600 laptop-token.json
```

`--out FILE` writes the token file with mode `0600` and refuses to replace an existing
file; `--out -` writes it to stdout for a pipe. The file holds the secret once — only
its hash is stored — plus the tenant and the `accountId` an uploader must name:

```json
{
  "version": 1,
  "token": "rth_st_…",
  "id": "…",
  "label": "laptop",
  "scopes": ["rth:sync", "rth:read"],
  "expiresAt": "…",
  "orgId": "acme",
  "workspaceId": "main",
  "accountId": "relayhistory:…"
}
```

Give each machine its own token: `rth:sync` to upload, `rth:read` to search and read.
Tokens expire after 90 days by default (`--expires-days`, at most 365).

```bash
relayhistory-server token list --org acme
relayhistory-server token revoke --org acme --id <id>
```

An existing token can mint narrower tokens for its own tenant through
`POST /v1/auth/service-tokens`.

## Backup and restore

Everything the service keeps is in PostgreSQL's `sessions` schema. Back it up with
`pg_dump`; restore into an empty database and start the server.

```bash
# Back up (Compose)
docker compose -f packages/server/compose.yaml exec -T postgres \
  pg_dump -U relayhistory -d relayhistory -Fc > relayhistory-$(date +%F).dump

# Restore: stop the server so nothing reads or writes during the restore
docker compose -f packages/server/compose.yaml stop server
docker compose -f packages/server/compose.yaml up -d --wait postgres
docker compose -f packages/server/compose.yaml exec -T postgres \
  pg_restore -U relayhistory -d relayhistory --clean --if-exists < relayhistory-2026-10-04.dump
docker compose -f packages/server/compose.yaml up -d --wait server
```

Uploaders keep their own cursors and resend anything a restored server lacks: the
protocol is idempotent, so a replayed batch returns the receipt it was first given.

## Upgrades

Pull the new revision and rebuild. Migrations run in one transaction under an advisory
lock with a checksum ledger, so concurrent starts are safe and an edited applied
migration stops the server instead of diverging. The first start after an upgrade
also finishes any projection backfill before it listens; the health check allows an hour
for that (`RELAYHISTORY_START_PERIOD`, e.g. `3h`), so `up --wait` waits rather than failing.
For a very large database, run the schema work as its own step first, with no health
check involved, then start the server:

```bash
docker compose -f packages/server/compose.yaml run --rm server migrate
docker compose -f packages/server/compose.yaml up -d --wait
```

Back up before upgrading.

```bash
git pull
docker compose -f packages/server/compose.yaml up -d --build --wait
```

## Verify a deployment

`packages/server/scripts/smoke.mjs` exercises a clean start end to end: tokens for two
tenants, uploads from two machine origins, replay, conflicts, scope and account
enforcement, tenant isolation, a tombstone, list/search/transcript/catalog reads, and the same
reads after a graceful restart.

```bash
cd packages/server
node scripts/smoke.mjs --admin-url postgres://postgres@127.0.0.1:5432/postgres   # local Postgres
node scripts/smoke.mjs --compose compose.yaml --base-url http://127.0.0.1:8080   # running stack
```

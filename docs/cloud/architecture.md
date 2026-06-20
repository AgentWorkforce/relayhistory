# Cloud Architecture

## System Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                          User's Machine                              │
│                                                                      │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────────┐  │
│  │ Claude Code  │    │  Codex CLI   │    │  Cursor / Relay      │  │
│  │ ~/.claude/   │    │ ~/.codex/    │    │  ~/.cursor/          │  │
│  └──────┬───────┘    └──────┬───────┘    └──────────┬───────────┘  │
│         └──────────────────┬┘                        │              │
│                            ▼                         │              │
│                 ┌──────────────────┐                 │              │
│                 │   ai-hist sync   │◄────────────────┘              │
│                 │  (Python CLI)    │                                 │
│                 │                  │                                 │
│                 │ 1. reads local   │                                 │
│                 │ 2. encrypts      │◄── user key (never leaves)     │
│                 │ 3. POSTs payload │                                 │
│                 └────────┬─────────┘                                │
└──────────────────────────┼──────────────────────────────────────────┘
                           │ HTTPS (ciphertext only)
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         Cloud Backend                                │
│                                                                      │
│  ┌────────────────┐   ┌─────────────────┐   ┌──────────────────┐   │
│  │ Ingestion API  │   │  Storage Layer  │   │  Query API       │   │
│  │                │──►│                 │◄──│                  │   │
│  │ - auth         │   │ Postgres        │   │ - list entries   │   │
│  │ - dedup check  │   │ (ciphertext     │   │ - fetch by id    │   │
│  │ - store        │   │  blobs only)    │   │ - metadata only  │   │
│  └────────────────┘   └─────────────────┘   └──────────────────┘   │
│                                                                      │
│  ┌────────────────┐   ┌─────────────────┐                          │
│  │  Auth Service  │   │  Hosted MCP     │                          │
│  │                │   │  Endpoint       │                          │
│  │ - JWT tokens   │   │                 │                          │
│  │ - team mgmt    │   │ proxies to      │                          │
│  │ - key mgmt     │   │ Query API       │                          │
│  └────────────────┘   └─────────────────┘                          │
└─────────────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         Web Dashboard                                │
│                                                                      │
│  - Decrypts in browser (key derived from password, never sent)      │
│  - Search runs client-side on decrypted data                        │
│  - Session browser, timeline, stats                                  │
│  - Team view: aggregate metadata + opt-in shared sessions           │
│  - Export / Delete controls                                          │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Components

### 1. Ingestion API

Receives sync payloads from the ai-hist CLI. Stateless — all business logic lives in the client.

**Endpoints:**

```
POST /v1/sync
  Body: { user_id, entries: [{ id, ciphertext, iv, tag, metadata }] }
  Returns: { accepted: N, duplicate_ids: [...] }

GET /v1/sync/state
  Returns: { last_synced_at, entry_count }
  (No content — only operational metadata)
```

**Deduplication:** Client sends a hash of the plaintext ID. Server checks against stored hashes. If already present, skips without storing. Hash only — server never sees content.

**Rate limiting:** Per-user token bucket. Batch size max 1000 entries per request.

### 2. Storage Layer

**Database:** Postgres with per-row encryption.

Schema (what the server actually stores):

```sql
CREATE TABLE entries (
  id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id      UUID NOT NULL REFERENCES users(id),
  content_hash TEXT NOT NULL,          -- SHA-256 of plaintext, for dedup
  ciphertext   BYTEA NOT NULL,         -- AES-256-GCM encrypted blob
  iv           BYTEA NOT NULL,         -- 12-byte nonce
  auth_tag     BYTEA NOT NULL,         -- 16-byte GCM authentication tag
  schema_ver   SMALLINT NOT NULL,      -- encryption schema version
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(user_id, content_hash)
);

CREATE TABLE users (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  email           TEXT UNIQUE NOT NULL,
  auth_hash       TEXT NOT NULL,       -- bcrypt of password for auth
  -- NO key material stored here -- key derived client-side
  plan            TEXT NOT NULL DEFAULT 'free',
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  deleted_at      TIMESTAMPTZ          -- soft delete for billing grace period only
);

CREATE TABLE teams (
  id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name       TEXT NOT NULL,
  plan       TEXT NOT NULL DEFAULT 'team',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE team_members (
  team_id    UUID REFERENCES teams(id),
  user_id    UUID REFERENCES users(id),
  role       TEXT NOT NULL DEFAULT 'member',  -- 'owner' | 'member'
  PRIMARY KEY (team_id, user_id)
);
```

**No plaintext content stored anywhere in the database.** The server cannot reconstruct user data.

### 3. Auth Service

- Email + password authentication
- Password is used for both auth and key derivation — but different derivatives (see [encryption.md](./encryption.md))
- JWT tokens for API access (short-lived, 1 hour; refresh tokens 30 days)
- Team management: invite by email, role assignment, seat counting for billing

### 4. Query API

Returns ciphertext blobs to authenticated clients. The client decrypts locally.

```
GET /v1/entries?since=<timestamp>&limit=<n>
  Returns: [{ id, ciphertext, iv, auth_tag, schema_ver, created_at }]
  (All content encrypted — server returns what it stored)

GET /v1/entries/<id>
  Returns: { id, ciphertext, iv, auth_tag, schema_ver, created_at }

DELETE /v1/entries/<id>
  Hard delete — row removed, not flagged. Returns 204.

DELETE /v1/user
  Account deletion — see data-rights.md for full cascade.

GET /v1/export
  Streams all user's ciphertext blobs as NDJSON.
  Client decrypts and writes local file.
```

### 5. Hosted MCP Endpoint

Users can point Claude and other MCP clients at `https://mcp.ai-hist.app/v1/mcp/<user_token>` instead of running a local server.

The MCP server runs server-side but receives the user's encryption key in the session header (never logged, never persisted). It decrypts entries in-memory to fulfill tool calls and discards the key when the session ends.

This is the one place where data is transiently decrypted server-side. It is opt-in, clearly disclosed, and the alternative (local MCP server) remains fully supported.

### 6. Web Dashboard

Single-page app. All decryption happens in the browser using WebCrypto API.

**On login:**
1. User enters password
2. Browser derives encryption key locally (key never sent to server)
3. Auth token fetched separately using password hash
4. Encrypted entries fetched from Query API
5. Browser decrypts entries into an in-memory index
6. Search runs against in-memory index (no server-side search)

**Team view:**
- Aggregate metadata: entry counts per member, last-sync timestamps (no content)
- Shared sessions: team members can explicitly mark sessions as shared, which re-encrypts with the team key; other members can then decrypt and search these

---

## Modifications to Existing Clients

### Python CLI changes

Add `cloud` subcommand group:

```
ai-hist cloud login           # auth, derive + store key in keychain
ai-hist cloud sync            # encrypt + push new entries since last push
ai-hist cloud pull            # fetch + decrypt remote entries to local DB
ai-hist cloud watch           # continuous push loop (like local watch)
ai-hist cloud export          # fetch all ciphertext, decrypt, write local file
ai-hist cloud delete <id>     # hard delete single entry from cloud
ai-hist cloud delete-account  # full account deletion
ai-hist cloud status          # show sync state, entry count, last push
```

**Key storage:** Derived key stored in OS keychain (macOS Keychain, Linux Secret Service, Windows Credential Store) via the `keyring` library. Never written to disk in plaintext.

### TypeScript SDK changes

Add optional `cloudConfig: { endpoint, token, key }` to SDK constructor. When present, SDK fetches from cloud Query API in addition to (or instead of) local DB.

---

## Infrastructure

**V1 (simple, low cost):**
- Single Postgres instance (RDS or Supabase)
- Serverless functions for API (Vercel / Cloudflare Workers)
- Static hosting for web dashboard
- Estimated cost at 1000 users: ~$50/month

**V2 (scale):**
- Read replicas for Query API
- Regional deployments for data residency
- S3/R2 for large export blobs

**On-premises:**
- Docker Compose bundle: Postgres + API server + web dashboard
- Customer manages their own storage; we provide the software
- License key validation (can be air-gapped after initial activation)

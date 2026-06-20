# Cloud Roadmap & Monetization

## Guiding Principle

Ship the smallest thing that is genuinely useful, that delivers the privacy guarantee from day one, and that doesn't compromise the local-first experience for users who don't want the cloud.

The CLI must remain fully functional with zero cloud dependency. Cloud is opt-in.

---

## Dependency: Issue #8 (Rust Crate)

Issue #8 plans to migrate core logic to a Rust crate (`ai-hist-core`) that compiles to a native binary and WASM. **This intersects directly with the cloud encryption layer.**

The encryption module (Argon2id + AES-256-GCM) should ultimately live in `ai-hist-core`, not be written separately in Python and TypeScript. The recommended sequencing:

**Option A (preferred):** Cloud Phase 1 begins in parallel with Issue #8. The Rust crate's `crypto.rs` module is the first deliverable of Issue #8 — it blocks nothing else in the Rust migration but provides the encryption primitive needed for cloud sync. Cloud Phase 1 integrates `ai-hist-core` via its native binary interface.

**Option B (if Issue #8 is delayed):** Implement crypto in Python (`cryptography` lib) and TypeScript (`SubtleCrypto`) as V1 interim implementations, then migrate to the Rust crate when it lands. No re-encryption of stored data needed — ciphertext is algorithm-agnostic.

---

## Phase 1: Foundation (Weeks 1–8)

**Goal:** One user can sync encrypted history to the cloud and retrieve it on another device.

### What we build
- [ ] Argon2id key derivation (in `ai-hist-core` Rust crate if Issue #8 is ready; Python `cryptography` lib as fallback)
- [ ] AES-256-GCM encryption of history entries before sync
- [ ] `cloud login` / `cloud sync` / `cloud pull` CLI commands
- [ ] Minimal backend: Postgres + REST API (ingestion + query)
- [ ] Account creation: email + password
- [ ] OS keychain integration for key storage (`keyring` library)
- [ ] Hard-delete endpoint (`DELETE /v1/entries/<id>` and `DELETE /v1/user`)
- [ ] Export endpoint (streaming ciphertext → client decrypts)

### What we don't build yet
- Web dashboard (CLI only in Phase 1)
- Teams
- Hosted MCP endpoint
- Billing

### Success criteria
- Developer can run `ai-hist cloud sync` and retrieve their history on a second machine
- Encryption is verified: database rows contain only ciphertext
- Deletion is verified: row is gone from DB within the same request
- Export round-trips cleanly: `export | import` produces identical local DB

### Infrastructure
- Single Postgres instance (Supabase free tier or RDS t3.micro)
- Serverless API (Vercel or Cloudflare Workers)
- Estimated cost: ~$0–20/month

---

## Phase 2: Personal Cloud (Weeks 9–16)

**Goal:** A polished solo-user experience with a web UI. Ready for public launch.

### What we build
- [ ] Web dashboard (Next.js or SvelteKit)
  - Login with browser-side key derivation (WebCrypto)
  - Search: fetch all ciphertext → decrypt in-browser → in-memory search
  - Session browser with timeline view
  - Stats page (entry counts, sources, top projects)
  - Export button (downloads decrypted NDJSON)
  - Delete controls (per-entry and full account)
- [ ] `ai-hist cloud watch` command (continuous background sync)
- [ ] Hosted MCP endpoint (opt-in, clearly disclosed as non-zero-trust)
- [ ] Billing integration (Stripe)
  - Free tier enforcement (entry count limit or date limit)
  - Developer plan ($9/mo): unlimited history, all sources, API access
- [ ] Password change with key rotation
- [ ] Email confirmation and deletion receipt emails
- [ ] Privacy page: plain-English explanation of encryption model

### Success criteria
- User can sign up, sync, search, export, and delete entirely from the web UI
- Key never appears in server logs or network traffic (verified by audit)
- Privacy page passes the "explain to a skeptical developer" test

### Infrastructure
- Add read replica for Query API
- CDN for web dashboard (Vercel / Cloudflare Pages)
- Email: Postmark or Resend (transactional only)
- Estimated cost at 500 users: ~$30–50/month

---

## Phase 3: Teams (Weeks 17–28)

**Goal:** Organizations can share AI history across their team.

### What we build
- [ ] Team creation and member invite flow
- [ ] Team key generation and distribution (see [encryption.md](./encryption.md))
- [ ] Session sharing: mark a personal session as shared with team
- [ ] Team search: in-browser search across all shared sessions
- [ ] Team dashboard: aggregate metadata (entry counts, last-sync per member — no content)
- [ ] Team MCP endpoint: shared sessions accessible via one MCP URL
- [ ] Member removal with key rotation
- [ ] Billing: Team plan ($49/mo for 5 seats, $9/seat additional)
- [ ] Owner controls: who can share, seat management

### What we explicitly don't build
- Server-side full-text search across team content (would require server to hold plaintext)
- Admin access to member content (by design — technically impossible)

### Success criteria
- Owner can share a session; teammate can search it without any server-side plaintext
- Member removal is cryptographically enforced (old member cannot access new shared content)
- Team search latency acceptable for 10K entries (target: <500ms in browser)

---

## Phase 4: Enterprise (Weeks 29+)

**Goal:** Enterprise procurement requirements met; on-premises option available.

### What we build
- [ ] SSO: SAML 2.0 / OIDC integration (Okta, Azure AD, Google Workspace)
- [ ] Audit log: who synced, who exported, who deleted — no content, only events
- [ ] Compliance exports: SOC 2 Type II report, data processing addendum
- [ ] Data residency: EU-only or US-only storage options
- [ ] On-premises bundle: Docker Compose (Postgres + API + web dashboard)
  - License key validation (can be air-gapped post-activation)
  - Customer manages their own encryption keys (bring-your-own-key option)
- [ ] SLA: 99.9% uptime guarantee with credit system
- [ ] Dedicated support channel

### Pricing
- Custom contract, typically $X/seat/year
- Minimum seat count (e.g., 25 seats)
- Professional services for on-prem deployment

---

## Monetization Summary

| Tier | Price | Limits | Key features |
|---|---|---|---|
| **Free** | $0 | 30-day retention, 1 sync source | CLI sync, basic web UI, export, delete |
| **Developer** | $9/mo | Unlimited | All sources, full history, API access, hosted MCP |
| **Team** | $49/mo (5 seats) + $9/seat | Unlimited | Shared sessions, team search, team MCP, analytics |
| **Enterprise** | Custom | Unlimited | SSO, audit logs, on-prem, SLA, data residency |

**Free tier rationale:** Generous enough to be genuinely useful (30 days covers most active users' hot history). Constrained enough that any serious user pays. Export and delete always free — never use data hostage as retention.

**Expansion revenue:** Seat-based Team plan grows naturally with team size. No usage-based pricing complexity in V1.

**Open source strategy:** The sync client (Python CLI + TypeScript SDK) remains MIT licensed. The cloud backend is proprietary. This is the standard open-core model. The open client means:
- Community trust (verify the encryption yourself)
- Self-serve on-prem is possible (users run their own backend)
- But self-hosted users don't get our web UI, team features, or hosted MCP

---

## Key Build Decisions

### Why not server-side search?

True server-side search would require the server to hold plaintext, breaking the encryption model. Client-side search (download ciphertext, decrypt in browser, search in-memory) is fast enough for individual user histories (tens of thousands of entries) and acceptable for teams sharing curated sessions. If at-scale team search becomes a bottleneck, searchable encryption schemes exist but are complex — defer until there's evidence the limitation is blocking adoption.

### Why Postgres over distributed SQLite?

The existing local tool uses SQLite. The cloud uses Postgres because:
- Multi-tenant concurrent writes require proper locking
- Postgres row-level security simplifies tenant isolation
- Managed Postgres (RDS, Supabase) is operationally simple
- The TypeScript SDK already uses sql.js (WASM SQLite) for the client side — no change there

### Why not a proxy model (intercept AI tool API calls)?

Langfuse and Helicone work by proxying API calls, which gives them plaintext content. We deliberately don't do this because:
1. It requires routing production traffic through our servers
2. It only covers API-based tools (misses local file–based history)
3. It fundamentally conflicts with the privacy-first model

Reading local files is more work to integrate but gives us a privacy architecture that is genuinely differentiated.

### CASS and the Intelligence Layer Question

[CASS Memory System](https://github.com/Dicklesworthstone/cass_memory_system) (385 stars, alpha) takes a fundamentally different approach: it treats session history as raw material to distill into a "playbook" of procedural rules with confidence decay and evidence gates. CASS is the closest market signal that developers want AI agent memory tools.

ai-hist cloud is not CASS and should not try to be in V1. Our differentiation is multi-tool aggregation, team sharing, and the privacy-first model — none of which CASS provides. However, a future "Insights" feature would move into CASS's territory:

**Potential future Phase 5 — Insights (post-Enterprise):**
- Pattern extraction across your full multi-tool history (not just one agent)
- Surfaces repeated approaches, recurring problems, common prompts
- Team-level: what does your team ask AI agents to do most?
- Unlike CASS, this runs client-side over the decrypted corpus — we never process the content

This is explicitly out of scope until the core storage/search/team product is proven. Note it here because it shapes the data model: storing raw history faithfully (which we do) is the right foundation for adding intelligence later. Don't optimize it away.

### Open Source the Encryption Layer First

Before shipping the cloud, open source the encryption module as part of `ai-hist-core` (see Issue #8). This lets the community audit the crypto before any real user data is encrypted with it. Publish the [encryption.md](./encryption.md) threat model alongside the code. Do not ship the cloud product until the encryption layer has had at least 30 days of public review.

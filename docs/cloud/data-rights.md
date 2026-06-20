# User Data Rights

## Core Commitments

1. **You own your data.** We are custodians, not owners.
2. **We never read your data.** Enforced by encryption, not policy. See [encryption.md](./encryption.md).
3. **You can remove your data at any time.** Deletion is immediate and permanent.
4. **We never share your data.** Not for sale, not for training, not aggregated, not anonymized.
5. **You can take your data with you.** Export is always available, in a format that works without us.

These commitments are grounded in GDPR Articles 7, 17, 20 and CCPA §1798.105–1798.125.

---

## Deletion

### Single Entry Deletion

**User action:** `ai-hist cloud delete <id>` or Delete button in web dashboard.

**What happens:**
1. `DELETE /v1/entries/<id>` sent to API
2. Row removed from `entries` table — hard delete, not a soft flag
3. `content_hash` also removed (no dedup ghost remains)
4. Response: 204 No Content
5. Local DB entry unaffected (user controls their local copy independently)

**Timeline:** Immediate (synchronous with the API call). No async queue.

**Backup retention:** Nightly database backups are retained for 7 days for disaster recovery. Deleted entries are excluded from backups taken after the deletion event. Entries may persist in backups taken before deletion for up to 7 days. This is disclosed explicitly to users. After 7 days, no copy exists anywhere on our infrastructure.

### Account Deletion

**User action:** `ai-hist cloud delete-account` or "Delete my account" in Settings.

**What happens:**
1. `DELETE /v1/user` sent to API (requires re-authentication with password)
2. Cascade hard delete in order:
   - All rows in `entries` for this user
   - All rows in `team_members` for this user
   - Team keys encrypted for this user
   - Billing subscription cancelled via payment provider API
   - User row set to `deleted_at = now()` (retained 7 days for billing disputes only)
   - After 7 days: user row permanently deleted
3. Confirmation email sent with deletion receipt and timestamp
4. Account cannot be re-activated; email can be re-registered fresh after 7 days

**Timeline:** Cascade completes within 60 seconds. Confirmation email sent immediately after.

**Shared team sessions:** Sessions the user shared with a team are deleted. The team loses access to those entries. Other team members' own entries are unaffected.

**What persists after account deletion:**
- Billing records (required by law for 7 years in most jurisdictions — stored with payment processor, not on our servers, and contains no AI history content)
- The fact that an account existed (needed for fraud prevention) — stored as a one-way hash of the email, retained for 12 months

### Team Member Removal

When a team owner removes a member:
1. `team_members` row deleted
2. Team key re-encrypted for remaining members (owner's client handles this)
3. Member's personal entries remain their own — only team-shared sessions become inaccessible to them

---

## Export

Users can export their complete history at any time, regardless of account status or subscription plan. Export is always free.

### CLI Export

```bash
ai-hist cloud export --output my-history.jsonl.gz
```

**What happens:**
1. Authenticates with API
2. Streams all ciphertext blobs from `GET /v1/export` (paginated, no timeout)
3. Decrypts each blob locally using the user's key
4. Writes plaintext entries as NDJSON, gzip-compressed
5. Identical format to local `ai-hist export` — works with local `ai-hist import` without any cloud dependency

**Resume on interruption:** Export uses a cursor so it can resume if interrupted without re-downloading already-exported entries.

### Web Dashboard Export

1. Click "Export all data" in Settings
2. Browser fetches all ciphertext blobs
3. Browser decrypts in-memory using derived key (WebCrypto)
4. Downloads as `ai-history-<date>.jsonl.gz`

**No server-side processing of plaintext occurs during export.**

### Export Format

Each line is a JSON object:

```json
{"source":"claude","session_id":"abc123","project":"/Users/me/myproject","prompt":"refactor auth module","timestamp_ms":1718900000000,"exported_at":"2025-06-20T00:00:00Z"}
```

Trajectory entries export as:

```json
{"type":"trajectory","id":"traj_abc","task_title":"Add JWT auth","decisions":[...],"retrospective":{...},"exported_at":"2025-06-20T00:00:00Z"}
```

The export is self-contained and can be imported into a local `ai-hist` database with `ai-hist import my-history.jsonl.gz`.

---

## Data We Collect (Full Inventory)

### Account Data
| Data | Purpose | Retention |
|---|---|---|
| Email address | Auth, deletion receipt | Until account deleted + 7 days |
| `bcrypt(auth_key)` | Login verification | Until account deleted |
| Argon2 salt | Key derivation (client-side) | Until account deleted |
| Plan type | Feature gating, billing | Until account deleted |
| `created_at` | Support, fraud prevention | Until account deleted |

### Operational Metadata (per entry)
| Data | Purpose | Retention |
|---|---|---|
| `created_at` | Sync ordering | Until entry deleted |
| `content_hash` | Deduplication | Until entry deleted |
| Schema version | Crypto migration | Until entry deleted |

### Content (encrypted)
| Data | Visible to us | Retention |
|---|---|---|
| Prompt text | No | Until entry deleted |
| Project path | No | Until entry deleted |
| Session ID | No | Until entry deleted |
| Source (claude/codex/etc.) | No | Until entry deleted |
| Trajectory decisions | No | Until entry deleted |

### Logs and Analytics
| Data | Purpose | Retention |
|---|---|---|
| API request logs (IP, endpoint, status code, latency) | Security, debugging | 30 days, then deleted |
| Error logs (no request body logged) | Debugging | 14 days, then deleted |
| Aggregate metrics (entry counts, sync frequency) | Product analytics | 12 months, anonymized |

**We do not use third-party analytics SDKs** (no Mixpanel, Amplitude, Segment, etc.) in the sync client. The web dashboard uses a self-hosted, privacy-respecting analytics tool (Plausible or equivalent) that collects only page views and no personal identifiers.

---

## No Training Data Use

User data is never used to train any machine learning model — ours or anyone else's. This applies even to anonymized or aggregated derivatives.

This commitment is:
- Written into our Terms of Service
- Not waivable by any plan or agreement
- Binding regardless of whether we are acquired

---

## Regulatory Compliance

**GDPR (EU):**
- Legal basis: Contract (Art. 6(1)(b)) for service delivery
- Data processor agreements available for enterprise customers
- DPA (Data Processing Agreement) template available on request
- Right of access: export provides this (Art. 15)
- Right to erasure: account deletion provides this (Art. 17)
- Data portability: export in machine-readable format (Art. 20)
- DPO: designated for enterprise plans

**CCPA (California):**
- No sale of personal information (§1798.100)
- Right to delete: account deletion (§1798.105)
- Right to know: export (§1798.110)
- Right to opt-out of sale: N/A (we don't sell)

**Sub-processors:**
Full list published and kept current. Currently:
- Payment processor (Stripe) — billing data only, no AI history
- Cloud infrastructure provider — stores ciphertext only
- Email provider — transactional email only (confirmation, deletion receipts)

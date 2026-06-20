# End-to-End Encryption Design

## Principle

The server stores ciphertext. We hold no decryption keys. Even with full database access, an attacker (or us) cannot read user content. This is enforced by the cryptographic design, not by access controls or policy.

---

## Key Derivation

The user's password is used to derive two separate keys:

```
password
    │
    ▼
Argon2id(password, salt, m=64MB, t=3, p=4)
    │
    ├──► auth_key     (first 32 bytes)
    │    Used to derive the auth token sent to the server.
    │    Server stores bcrypt(auth_key), never the password.
    │
    └──► encryption_key  (next 32 bytes)
         Used for all content encryption.
         Never sent to the server under any circumstances.
```

**Parameters:**
- KDF: Argon2id (memory-hard, resistant to GPU/ASIC attacks)
- Memory: 64 MB
- Iterations: 3
- Parallelism: 4
- Salt: 16 bytes, randomly generated per user, stored server-side (not secret — used to make key derivation user-specific)
- Output: 64 bytes split into two 32-byte keys

**Why Argon2id:** Balances resistance to side-channel attacks (Argon2i) and GPU brute-force (Argon2d). OWASP-recommended for password hashing as of 2024. Adds ~300ms on a modern machine — acceptable for login, too slow for bulk brute-force.

---

## Content Encryption

Each history entry is encrypted independently.

**Algorithm:** AES-256-GCM
- 256-bit key (the `encryption_key` derived above)
- 96-bit (12-byte) random IV per entry — never reuse
- 128-bit authentication tag — provides integrity and authenticity
- Associated data: `{ user_id, schema_ver }` — authenticated but not encrypted, so the server can verify structural integrity without seeing content

**Encrypted payload per entry:**

```json
{
  "schema_ver": 1,
  "iv": "<base64 12 bytes>",
  "auth_tag": "<base64 16 bytes>",
  "ciphertext": "<base64 encrypted JSON blob>"
}
```

**Plaintext JSON blob (before encryption):**

```json
{
  "source": "claude",
  "session_id": "abc123",
  "project": "/Users/me/myproject",
  "prompt": "refactor the auth module to use JWT",
  "prompt_hash": "a1b2c3d4",
  "timestamp_ms": 1718900000000
}
```

The entire JSON object is encrypted. The server sees only the ciphertext blob, IV, and auth tag.

---

## What the Server Can See

| Field | Visible to server | Notes |
|---|---|---|
| User email | Yes | Required for auth |
| Entry count | Yes | Required for billing/plan enforcement |
| Per-entry `created_at` | Yes | Required for sync ordering |
| `content_hash` | Yes | SHA-256 of plaintext, used for deduplication only |
| Source (claude/codex/cursor) | No | Encrypted |
| Project path | No | Encrypted |
| Prompt text | No | Encrypted |
| Session ID | No | Encrypted |
| Trajectory decisions | No | Encrypted |

The `content_hash` reveals nothing about content — it is a one-way function. Knowing two entries have the same hash only tells you they are duplicates (which is the intended use).

---

## Team Encryption

For teams to share sessions, a separate team key is used.

**Team key setup:**
1. When a team is created, the owner generates a random 32-byte `team_key` client-side
2. The `team_key` is encrypted with each member's `encryption_key` and stored server-side (one ciphertext blob per member)
3. When a new member joins, the owner re-encrypts the `team_key` with the new member's `encryption_key`
4. The server never sees the `team_key` in plaintext

**Sharing a session:**
1. User selects a session to share with the team
2. Client decrypts session entries using personal `encryption_key`
3. Client re-encrypts entries using `team_key`
4. Sends re-encrypted blobs to server with `team_id` tag

**Revoking a member:**
1. Owner removes member from team
2. Owner generates a new `team_key`
3. Re-encrypts all shared sessions with new key
4. Re-encrypts new key for remaining members
5. Server purges old team key blobs for removed member

This is the standard approach used by end-to-end encrypted team tools (1Password Teams, Keybase Teams).

---

## Key Rotation

Users can change their password:

1. Client derives new `encryption_key` from new password
2. Client fetches all ciphertext blobs from server
3. Client decrypts each with old key, re-encrypts with new key
4. Client pushes updated blobs to server in a single atomic transaction
5. Server replaces old blobs; old `auth_key` hash invalidated

For large histories this may take a few minutes. The client shows progress. If interrupted, the operation is idempotent and can be resumed.

---

## Hosted MCP Endpoint (Opt-In Transient Decryption)

The hosted MCP endpoint is the only case where data is decrypted server-side. This is opt-in and clearly disclosed.

**Flow:**
1. User generates a session token in the web dashboard
2. Session token is associated with the user's `encryption_key` (encrypted under a server-held session key — see note)
3. MCP server decrypts the entry `encryption_key` on each request, decrypts content in-memory, fulfills the MCP tool call, and discards plaintext immediately
4. Session tokens expire after configurable period (default: 30 days)

**Note on session key:** The session token stores the user's `encryption_key` encrypted under a server-side key stored in an HSM or KMS (AWS KMS / Cloudflare KV with encryption). This means the hosted MCP endpoint requires trust in the server, which is explicitly disclosed. The alternative (local MCP server) provides zero-trust operation and remains the default recommendation.

Users who require zero-trust throughout should run the local MCP server. The hosted endpoint is a convenience trade-off, not the default.

---

## Client Implementation

**Python CLI:** Uses `cryptography` library (libsodium bindings). Key stored in OS keychain via `keyring`.

**TypeScript SDK / Web:** Uses `SubtleCrypto` (WebCrypto API, built into all modern browsers and Node.js 16+). No third-party crypto dependencies.

**Key storage on device:**
- macOS: Keychain Services (encrypted by device password + Secure Enclave on Apple Silicon)
- Linux: SecretService API (GNOME Keyring / KWallet)
- Windows: Windows Credential Manager (DPAPI encrypted)
- Fallback: `~/.ai-hist/keystore` encrypted with OS user password (warn user, not recommended)

---

## Security Assumptions and Threat Model

**Protected against:**
- Server breach — attacker gets ciphertext; no decryption key available
- Legal compulsion — we have nothing readable to produce
- Malicious employee — no employee can access user content
- Network interception — TLS + E2E encryption; plaintext never on wire

**Not protected against:**
- Compromised client device — if the device is compromised, the local key can be extracted
- Weak password — Argon2id slows brute-force but doesn't prevent it against very weak passwords; enforce minimum entropy at registration
- Hosted MCP endpoint compromise — if KMS is breached, session tokens could be used to decrypt; mitigated by token expiry and audit logging

**Out of scope (V1):**
- Forward secrecy per-message (each entry has its own IV; key rotation provides forward secrecy at rotation boundaries)
- Post-quantum cryptography (monitor NIST PQC standards; migrate when AES-256-GCM is deemed insufficient)

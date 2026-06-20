# Browser Security Model

## The Question

Browser-based access to encrypted history introduces a question that deserves an explicit answer: if data is end-to-end encrypted and the server holds no keys, how can a web dashboard work securely — and what are the honest limits of that security?

---

## How It Works

Decryption happens entirely inside the browser. The server never sees plaintext.

```
User enters password
        │
        ▼
Browser derives encryption_key via Argon2id (WASM)
        │                    ↑
        │              key never leaves this tab
        ▼
Browser fetches ciphertext blobs from API
        │
        ▼
Browser decrypts in-memory via SubtleCrypto (AES-256-GCM)
        │
        ▼
Search runs against in-memory plaintext index
        │
        ▼
Tab closed → in-memory key discarded
```

The Argon2id implementation is the same `ai-hist-core` crypto module compiled to WASM — identical algorithm, identical parameters, same key as the CLI. No separate browser crypto implementation.

This is the same model used by Bitwarden's web vault, ProtonMail, and 1Password's web client. It works.

---

## The One Honest Caveat: JavaScript Delivery

This is the known structural weakness of browser-based E2E encryption, and it deserves a straight explanation rather than being buried in a footnote.

With the CLI, you download a binary, verify the checksum, and trust that binary for as long as you use it. With a web app, **every page load the browser executes JavaScript served by the server**. If that JavaScript were malicious — exfiltrating the password before key derivation, for example — the user would not know.

This doesn't break the encryption model. It shifts one trust assumption:

| Client | What you trust |
|---|---|
| CLI / native binary | The binary you downloaded (verifiable via checksum, reproducible build) |
| Browser | That the server is delivering the same JS that's in the open source repo |

The browser introduces a server-honesty dependency that the CLI does not have.

---

## Mitigations

These four controls together reduce the JavaScript delivery risk to a narrow, accepted residual — the same level accepted by every serious E2E encrypted web product:

### 1. Subresource Integrity (SRI)

Every JavaScript bundle is served with a cryptographic hash in its `<script>` tag:

```html
<script
  src="/static/app.js"
  integrity="sha384-<hash>"
  crossorigin="anonymous">
</script>
```

If the file served by the server doesn't match the hash, the browser refuses to execute it. The hash is published separately (in the open source repo's CI output), so a user can independently verify that what the browser received matches what was built from the public source.

This prevents server-side tampering with the delivered JS without detection.

### 2. Strict Content Security Policy

A restrictive CSP prevents the most dangerous categories of attack even if something slips through:

```
Content-Security-Policy:
  default-src 'self';
  script-src 'self';
  connect-src 'self' https://api.ai-hist.app;
  object-src 'none';
  base-uri 'none';
  form-action 'self';
```

Key effects:
- **No inline scripts** — XSS can't inject `<script>alert(key)</script>`
- **No external script sources** — malicious third-party JS can't be loaded
- **connect-src locked to our API** — even if XSS executed, it couldn't exfiltrate data to an attacker's server

### 3. Open Source Web Client

The web dashboard source is published in the same repository as the CLI. Anyone can audit exactly what JavaScript is being served, and CI publishes the build hashes so they can verify the deployed bundle matches the source. Third-party security researchers can and should review it.

### 4. `sessionStorage`, Not `localStorage`

The derived encryption key is held only in `sessionStorage` — scoped to the current tab and cleared when the tab closes. It is never written to `localStorage` (which persists across sessions and is accessible to any JS on the page).

This means:
- Closing the browser tab discards the key — next login re-derives it from the password
- A different tab on the same origin cannot access the key
- Browser persistence attacks (stolen browser profile, etc.) don't yield the key

The UX cost: users re-enter their password each session. Given that Argon2id derivation takes ~300ms, this is acceptable. A "remember me" option is explicitly not offered for the encryption key — only for the authentication token (which is separate and does not grant decryption access).

---

## CLI vs. Browser: The Honest Comparison

| | CLI | Browser |
|---|---|---|
| Key derivation | Native binary | WASM (same algorithm) |
| Key storage | OS keychain (Keychain / SecretService / DPAPI) | `sessionStorage` (tab lifetime only) |
| JS delivery risk | None | Mitigated by SRI + CSP + open source |
| Shared machine risk | Low (OS user session required) | Higher — decrypted content on screen |
| Cloud IDE support | Depends on env | Yes (any browser) |
| Recommended for | Primary dev machine, maximum security | Convenience, secondary devices, lookups |

---

## Shared and Untrusted Machines

The encryption holds on a shared machine — the key is not persisted and the server never sees plaintext. But **decrypted content is on screen** for the duration of the session, and browser memory may retain it briefly after tab close.

The dashboard displays a warning when the session starts:

> *Avoid using the web dashboard on shared or untrusted devices. For maximum security, use the CLI — your key is stored in the OS keychain and never appears in a browser.*

This is the same guidance Bitwarden gives. It is not a weakness in the encryption — it is an honest description of operational risk at the display layer.

---

## What This Means for Feature Design

**Session timeout:** The web dashboard auto-locks after 15 minutes of inactivity, clearing the in-memory key. User must re-derive on return. Configurable, but minimum is 5 minutes.

**No clipboard auto-copy:** The dashboard does not auto-copy decrypted content to the clipboard (which persists across apps). Copy actions are explicit and user-initiated.

**No server-side search:** Search runs in-memory client-side against the decrypted index. No decrypted content is ever sent back to the server for processing — not even search queries.

**Audit log entries for browser sessions:** When a user logs in via browser, an audit event is recorded server-side (timestamp, IP, session ID — no content). This lets users detect unexpected access. Visible in Settings → Security.

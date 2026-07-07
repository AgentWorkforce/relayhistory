# Reflex zero-setup — in-process capture via the `ai-hist-native` addon

Goal: a user runs **`agent-relay reflex on`** and nothing else — their agent
history syncs to relayhistory-cloud automatically. No separate `ai-hist`
install, and **no subprocess / no CLI shell-out** — the work runs in-process.

## How it works

`agent-relay up` runs a periodic in-process loop (gated on the reflex flag) that
calls the **`ai-hist-native`** napi addon's `syncAndPush()`:

```
agent-relay up  ──(every few minutes, if reflex.json.enabled)──▶  require('ai-hist-native').syncAndPush()
                                                                        │  (in-process, worker thread)
                                                                        ▼
                                              ai_hist_cli::sync_and_push()  (Rust)
                                                sync local history → push new records → POST /v1/ingest
```

- **No subprocess:** `ai-hist-native` is a native (napi) Node addon. Relay loads
  it and calls the Rust `sync_and_push` directly via FFI; the blocking work runs
  on a worker thread so the event loop isn't blocked.
- **Single source of truth:** the Rust `ai_hist_cli` library does the sync +
  push. The CLI binary and the addon call the same code.
- **Auth:** the `rth_at_` token written by `reflex on`
  (`~/.agentworkforce/relayhistory/auth.json`). `syncAndPush()` returns
  `authenticated: false` (a no-op) until the user is logged in.

## The packages

- **`crates/ai-hist-napi`** — the napi crate (`#[napi] async fn sync_and_push`)
  plus the generated JS loader (`index.js`/`index.d.ts`). Published as
  **`ai-hist-native`**.
- Per-platform **`ai-hist-native-<platform>-<arch>`** packages carry the
  prebuilt `.node`; the loader picks the right one. Same distribution model as
  `@agent-relay/broker-*`, except the addon is *loaded in-process*, not spawned.
- `.github/workflows/publish-napi.yml` builds the six targets — darwin
  arm64/x64 and linux x64/arm64 in **both** glibc (gnu) and musl — and publishes
  via napi's tooling. (Linux needs both: the loader resolves `-gnu` on
  Ubuntu/Debian and `-musl` on Alpine.)

## Making it live (one-time + per release)

1. **One-time — register OIDC trusted publishers** on npmjs for `ai-hist-native`
   and the six `ai-hist-native-*-*` package names (repo
   `AgentWorkforce/relayhistory` + `publish-napi.yml`). (First publish may need
   an `NPM_TOKEN` if npm won't pre-configure a nonexistent package.)

2. **Per release — publish the addon.** Run the **Publish ai-hist-native (napi)**
   workflow (`dry_run: true` first to validate the cross-compiles, then real).
   Keep the version in step with the `ai-hist` SDK.

3. **One-time, after step 2 publishes — wire it into agent-relay.** In `relay`,
   add to `packages/cli/package.json` (only *after* publish — an unpublished
   optional dep 404s `npm ci`):

   ```json
   "optionalDependencies": {
     "ai-hist-native": "0.4.0"
   }
   ```

   then `npm install`. `ai-hist-native` pulls the right per-platform package
   automatically.

After that, a fresh `agent-relay` install ships the addon, and `reflex on` works
with no extra commands and no subprocess.

## Notes

- The `ai-hist` npm **SDK** (`sdk-ts`) is independent — relay uses the napi addon
  directly and does not depend on the SDK.
- Everything degrades gracefully: if the addon isn't available for the platform
  or the user isn't authenticated, the capture loop is a silent no-op.
- napi cross-compilation (esp. musl) can need per-runner tuning; validate a
  `dry_run` before the first real publish. napi-rs's `napi new` template is a
  good reference for the CI if the provided workflow needs adjustment.

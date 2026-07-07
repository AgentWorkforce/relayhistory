# Reflex zero-setup — shipping the `ai-hist` binary with `agent-relay`

Goal: a user runs **`agent-relay reflex on`** and nothing else — their agent
history syncs to relayhistory-cloud automatically. No separate `ai-hist`
install, no PATH setup.

## How it works

`agent-relay up` runs a periodic in-process loop (gated on the reflex flag) that
drives the `ai-hist` Rust binary: `ai-hist sync` (populate the local DB from the
user's agent history) then `ai-hist push --json` (upload new records). The
binary is resolved by `packages/cli/src/cli/lib/ai-hist-path.ts`:

1. `$AI_HIST_RUST_BIN`
2. the per-platform optional-dependency package `ai-hist-bin-<platform>-<arch>`
3. `~/.local/share/ai-hist/ai-hist-rust-bin` (install.sh)
4. `ai-hist` on `PATH`

Step 2 is what makes it zero-setup: npm auto-installs the matching binary
package as an optional dependency of `agent-relay`.

## The binary packages

`bin-packages/ai-hist-bin-<platform>-<arch>/` (this repo) are npm package
scaffolds — `package.json` (with `os`/`cpu` so npm installs only the matching
one) + `bin/.gitkeep`. The binary is **injected at publish time** by CI, never
committed. Four targets: `darwin-arm64`, `darwin-x64`, `linux-x64`,
`linux-arm64`.

`.github/workflows/publish-bin-packages.yml` builds each target and publishes
`ai-hist-bin-<platform>-<arch>@<version>` via npm OIDC.

## Making it live (one-time + per release)

1. **One-time — register OIDC trusted publishers.** On npmjs.org, for each of
   the four `ai-hist-bin-*` package names, add a trusted publisher pointing at
   `AgentWorkforce/relayhistory` + `publish-bin-packages.yml`. (If npm won't let
   you configure a not-yet-created package, do the first publish with an
   `NPM_TOKEN` secret, then switch to OIDC.)

2. **Per release — publish the binary packages.** Run the **Publish ai-hist
   binary packages** workflow (`workflow_dispatch`) with `version: 0.4.0`,
   `dry_run: true` first to validate, then real. Keep the version in lockstep
   with the `ai-hist` SDK (`publish.yml`).

3. **One-time — wire the optional deps into agent-relay.** In the `relay` repo,
   add to `packages/cli/package.json` (only *after* step 2 publishes — an
   unpublished optional dep 404s `npm ci`):

   ```json
   "optionalDependencies": {
     "ai-hist-bin-darwin-arm64": "0.4.0",
     "ai-hist-bin-darwin-x64": "0.4.0",
     "ai-hist-bin-linux-x64": "0.4.0",
     "ai-hist-bin-linux-arm64": "0.4.0"
   }
   ```

   then `npm install` to update the lockfile, and bump the versions alongside
   future `ai-hist` releases.

After that, a fresh `agent-relay` install ships the binary, and `reflex on`
works with no extra commands.

## Notes

- The `ai-hist` npm **SDK** (`sdk-ts`, `pushToCloud`) is independent — the relay
  runtime spawns the binary directly and does **not** depend on it, so the SDK
  can be published on its own schedule for external consumers.
- Everything degrades gracefully: if the binary isn't resolvable or the user
  isn't authenticated, the capture loop is a silent no-op.

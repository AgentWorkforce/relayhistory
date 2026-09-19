# Release and platform validation

One workflow releases everything: `Publish RelayHistory release`
(`.github/workflows/publish.yml`; the file name is bound to npm OIDC
trusted publishing and must not change -- see "Renaming this workflow" below). All public packages share one version:
the `ai-hist` npm package, `ai-hist-native`, each native platform package, `ai-hist-mcp`,
`@relayhistory/capture`, `@relayhistory/provider-sources` and their
seven platform helper packages, and the `ai-hist` crates.io crate. The SDK checks the current native contract
version at initialization; both halves declare it in source (`sdk-ts/src/native.ts`
and `crates/ai-hist-napi/src/lib.rs`).

## Inputs

| Input            | Default | Meaning                                                             |
| ---------------- | ------- | ------------------------------------------------------------------- |
| `version`        | `patch` | Bump type computed from the highest published core version on npm.  |
| `custom_version` | empty   | Exact version; overrides the bump type.                             |
| `dry_run`        | `false` | Build and validate everything, publish nothing, push nothing.       |
| `plugins`        | `true`  | Also publish the optional history plugins at the same version.      |
| `probe`          | `true`  | Also attach `agent-relay-probe` binaries to the GitHub Release.     |
| `skip_core`      | `false` | Re-run only plugins/probe for an already published `custom_version`. |

## What happens, in order

1. `version` resolves the release version once, before anything is built: the
   bump type applied to the highest published core version on npm, or
   `custom_version`. Every other job takes the version from here, because the
   helper matrix has to stamp it into the Rust crates it compiles.
2. `build` builds and tests the native addon on all seven targets, and
   `helpers` builds the two Rust helpers for seven platforms — plus
   `agent-relay-probe` on the four platforms it ships for — verifying each
   executable and its glibc floor. `helpers` applies
   `scripts/set-release-version.mjs` first, so the executables report the
   release version (`agent-relay-probe --version` is asserted to print exactly
   `agent-relay-probe <version>` wherever the runner can execute it). Both jobs
   run in parallel and upload binaries as artifacts.
3. `publish` applies that version to every manifest and lockfile — the core
   packages and, whatever the `plugins`/`probe` inputs, the plugin manifests
   and both plugin crates via `scripts/set-release-version.mjs`, so the tag
   always carries what the helper matrix built from — prepares the
   version-only commit, then
   publishes the platform packages,
   `ai-hist-native`, `ai-hist` and `ai-hist-mcp` in that order. The SDK root is
   never published before its platform artifacts, because npm multi-package
   publication is not atomic.
4. `publish-crate` publishes the `ai-hist` crate to crates.io at that same
   version (OIDC trusted publishing, or `CARGO_REGISTRY_TOKEN`). If the version
   already exists, the job skips rather than failing. `dry_run` runs
   `cargo publish --dry-run -p ai-hist` and publishes nothing. A crates.io-only
   retry uses `skip_core` with the already-published `custom_version`; the job
   checks out `sdk-ts-v<version>` and publishes the crate from that tag.
5. After the clean registry install and the older-glibc CLI smoke tests pass,
   `publish` pushes the version commit — only if the branch has not advanced —
   and creates the `sdk-ts-v<version>` tag and GitHub Release.
6. `plugins` checks out that persisted commit, packages each helper binary at
   the release version, verifies staged tarballs, verifies the *published* core
   at each plugin's peer minimum, then publishes the seven helpers of each
   plugin before its JavaScript package.
7. `probe` attaches `agent-relay-probe-<platform>` and a matching `.sha256` to
   the same Release. See [agent-relay-probe.md](agent-relay-probe.md) for the
   asset names the website mirrors.

`scripts/set-release-version.mjs <version>` is the only place that knows what a
plugin release version touches (version, the seven helper pins, the `ai-hist`
peer range `^<version>`, the lockfile coordinates, and the `[package]` version
of each plugin's Rust crate plus that crate's own `Cargo.lock` entry — the
helper and probe executables report `CARGO_PKG_VERSION`). It is idempotent, and
the helper matrix, the version commit and the plugin job all run it, so what was
compiled, what was committed and what is published cannot diverge.

## Rust crate

The published crate is `ai-hist` on crates.io. It shares the npm version line,
so `sdk-ts-v0.18.8` is also `ai-hist@0.18.8` on crates.io. `ai-hist-cli` and
`ai-hist-napi` stay unpublished workspace members (`publish = false`).

Cargo semver is the Rust contract — there is no separate Rust contract-version
constant. Any change to a public struct field, enum variant, or behaviour a
consumer observes is a minor bump pre-1.0 and gets a `### Rust API` entry in
`CHANGELOG.md`. Default features expose `SessionStore`, evidence structs,
`Source`, and `Error`. Optional features (`delivery`, `opencode-backup`,
`git-hooks`, `unstable-internal`) stay off for embedders.

```bash
cargo add ai-hist
```

```rust
use ai_hist::SessionStore;
let store = SessionStore::open(Default::default())?;
store.sync(Default::default())?;
```

## Dry runs

Leave `dry_run` enabled to build, verify and stage everything without
publishing, pushing a commit, creating a Release or attaching assets. The
plugin job applies the release version to its own checkout and prints the
staged tarballs; the probe job prints the binary manifest with sizes and
digests. Dry runs never change the checked-in version baseline.

## Re-running plugins or probe alone

Set `skip_core` with an explicit `custom_version` equal to an already published
core version. `build` and `publish` are skipped. `version` verifies that the
`sdk-ts-v<version>` tag exists before anything builds, and every job that builds
or packages (`helpers`, `plugins`) checks out **that tag**, not the commit the
run was dispatched from — otherwise a re-run would publish newer code under an
older version and clobber the Release's probe binaries with it. Plugin manifests
are re-derived from the tag and the assets are attached to the existing
`sdk-ts-v<version>` Release. Turn off `plugins` or `probe` to narrow the re-run
further.

Because the re-run builds from the tag, it only works for releases cut by this
workflow: a tag whose tree predates these scripts fails loudly at checkout or
build time rather than shipping something mismatched.

## Claiming a new package name

npm OIDC trusted publishing can publish a new *version* of a package that
already exists, but it cannot *create* a name. This workflow is tokenless, so a
name's very first publish fails:

```
npm error code E404
npm error 404 Not Found - PUT https://registry.npmjs.org/@relayhistory%2fcapture-darwin-arm64
npm error 404 The requested resource '@relayhistory/capture-darwin-arm64@0.18.3'
              could not be found or you do not have permission to access it.
```

The name must exist once, published from a human npm account; CI owns it after
that. This applies to every name a release publishes — each plugin and each of
its per-platform helpers — so **adding a plugin or a supported platform means
claiming its names before the next release**.

From the repository root, signed in to npm:

```bash
node scripts/claim-plugin-package-names.mjs --dry-run   # what is missing
node scripts/claim-plugin-package-names.mjs             # claim it
```

It publishes a placeholder, not the real package: the platform helpers each
carry a cross-compiled binary for their target, so no one machine can build them
all. The real artifacts come from the release that follows.

Placeholders are published at `0.0.0`. A version publishes exactly once, so
claiming a name at a release version would fail the next release with
`EPUBLISHCONFLICT`. `0.0.0` is below every release, is transparently not a real
build, and never resolves — each plugin pins its helpers to its own exact
version, so nothing asks for `0.0.0`.

The script reads names from `scripts/history-package-contract.mjs`, skips
anything already on the registry, and is safe to re-run.

## Renaming this workflow

Don't, unless you are prepared to repoint every package. npm OIDC trusted
publishing binds a package to a repository **and a workflow file name**. Rename
the file and every package whose trusted publisher still names the old file
fails its next publish with:

```
npm error code E404
npm error 404 The requested resource '<package>@<version>' could not be found
              or you do not have permission to access it.
```

That message reads as "no such package". It is npm declining to say whether a
package exists, and the real cause is authorization. **A name that plainly
exists, failing with E404, means the trusted publisher does not match the
workflow that is asking.**

This file was `publish-napi.yml` until the optional plugins were first
published. Every other AgentWorkforce repository publishes from `publish.yml`,
and the mismatch cost a release cycle: the plugins' trusted publishers were set
to `publish.yml` by analogy with the sibling repos, and the E404 that followed
was read as a missing package rather than a wrong workflow name.

If it has to change again, the cutover is not atomic, and the order is:

1. Rename the file and merge, with no release in flight.
2. Repoint every package's trusted publisher to the new file name.
3. Release.

Between 1 and 2 nothing can publish, so keep the gap short. Every package this
workflow publishes is listed at the top of this document.

## Supported matrix

- Node.js: minimum 20; tested 20 and 22.
- Node-API: level 4.
- macOS: 12+, arm64 and x64.
- Linux: glibc and musl, arm64 and x64.
- Windows: Windows 10/11 and Server 2022, x64 MSVC.
- Windows arm64: unsupported until an executable CI test is reliable.

To validate a local artifact:

```bash
cargo test --workspace
cd crates/ai-hist-napi && npm ci && npm run build:debug
node -e "const n=require('./index.js'); console.log(n.nativeContractVersion())"
cd ../../sdk-ts && npm install && npm test && npm pack
```

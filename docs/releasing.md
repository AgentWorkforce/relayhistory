# Release and platform validation

One workflow releases everything: `Publish RelayHistory release`
(`.github/workflows/publish-napi.yml`; the file name is bound to npm OIDC
trusted publishing and must not change). All public packages share one version:
`ai-hist`, `ai-hist-native`, each native platform package, `ai-hist-mcp`,
`@agent-relay/relayhistory`, `@agent-relay/history-provider-sources` and their
seven platform helper packages each. The SDK checks the current native contract
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
3. `publish` applies that version to every manifest and lockfile (including
   the plugin manifests and both plugin crates, via
   `scripts/set-release-version.mjs`), prepares the version-only commit, then
   publishes the platform packages,
   `ai-hist-native`, `ai-hist` and `ai-hist-mcp` in that order. The SDK root is
   never published before its platform artifacts, because npm multi-package
   publication is not atomic.
4. After the clean registry install and the older-glibc CLI smoke tests pass,
   `publish` pushes the version commit — only if the branch has not advanced —
   and creates the `sdk-ts-v<version>` tag and GitHub Release.
5. `plugins` checks out that persisted commit, packages each helper binary at
   the release version, verifies staged tarballs, verifies the *published* core
   at each plugin's peer minimum, then publishes the seven helpers of each
   plugin before its JavaScript package.
6. `probe` attaches `agent-relay-probe-<platform>` and a matching `.sha256` to
   the same Release. See [agent-relay-probe.md](agent-relay-probe.md) for the
   asset names the website mirrors.

`scripts/set-release-version.mjs <version>` is the only place that knows what a
plugin release version touches (version, the seven helper pins, the `ai-hist`
peer range `^<version>`, the lockfile coordinates, and the `[package]` version
of each plugin's Rust crate plus that crate's own `Cargo.lock` entry — the
helper and probe executables report `CARGO_PKG_VERSION`). It is idempotent, and
the helper matrix, the version commit and the plugin job all run it, so what was
compiled, what was committed and what is published cannot diverge.

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

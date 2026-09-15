# Release and platform validation

All public packages use one version: `ai-hist`, `ai-hist-native`, each native
platform package, and `ai-hist-mcp`. The SDK checks native contract version 15
at initialization.

The `Publish RelayHistory npm packages` workflow:

1. builds Rust and all seven native targets;
2. executes the addon on native CI runners where the target matches;
3. collects `.node` artifacts;
4. creates and publishes per-platform packages;
5. publishes `ai-hist-native` with exact optional dependencies;
6. builds and publishes `ai-hist` (SDK + CLI);
7. publishes `ai-hist-mcp` last;
8. performs a clean registry install and CLI/MCP smoke test;
9. creates the `sdk-ts-v<version>` tag and GitHub Release.

The SDK root is never published before required platform artifacts. This is
important because npm multi-package publication is not atomic.

When manually dispatching the workflow, choose `patch`, `minor`, or `major`.
The workflow computes the next version from the currently published `ai-hist`
version on npm and applies that exact version to the native root, every platform
package, the SDK/CLI, and the MCP wrapper. For example, from `0.6.0`, `patch`
produces `0.6.1` and `minor` produces `0.7.0`. `custom_version` is available for
an explicit version and overrides the selected bump type. Leave `dry_run`
enabled to build and validate without publishing packages or creating a tag.

Before a non-dry release publishes any package, the workflow verifies that the
selected branch has not advanced and pushes a version-only commit containing
the updated package manifests and lockfiles. This makes partial publication
recovery deterministic and prevents a late non-fast-forward push from leaving
published packages without source metadata. Dry runs never change the checked-in
version baseline. After registry validation, the workflow creates the GitHub tag
and Release from the persisted version commit.

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

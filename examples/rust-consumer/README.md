# rust-consumer

A standalone Cargo project — not a member of the repository workspace — that
consumes the published `ai-hist` crate the way an embedder does: from
crates.io, on the default features, with its own `Cargo.lock`.

```sh
cd examples/rust-consumer
cargo run
```

It stages a handful of transcripts from `crates/ai-hist/tests/fixtures` into a
throwaway `HOME`, opens a `SessionStore` there, runs a `sync`, and for each
staged session prints the usage rollup, the normalized totals folded by model
from the request pages, and how many user turns and markers the store holds for
it. It exits non-zero when no session reported usage, so it is a real smoke
test of the published artefact and not only of its compiler.

`RELAYHISTORY_FIXTURES` points it at the corpus when it is built away from the
repository checkout. The provider-root environment overrides (`CLAUDE_CONFIG_DIR`,
`CODEX_HOME`, …) are cleared at startup so a developer's real sessions never
reach the temporary store.

Two CI jobs build it:

- `rust-consumer` in `.github/workflows/ci.yml`, on every pull request, with
  `[patch.crates-io]` redirected at `../../crates/ai-hist` — and an assertion
  that the patch was applied, since Cargo only warns when it cannot be.
- `Published crate consumer` (`.github/workflows/published-crate-consumer.yml`),
  nightly and on dispatch, against the crate crates.io serves.

The `ai-hist` requirement here is stamped by `scripts/set-release-version.mjs`
at each release. Two lines are marked `TODO` for facade methods that are
decided but not published yet: `sessions()` (#178) and the change-feed drain
(#179). See [`docs/sourcing-sdk.md`](../../docs/sourcing-sdk.md).

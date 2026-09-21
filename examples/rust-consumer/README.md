# rust-consumer

A standalone Cargo project — not a member of the repository workspace — that
consumes the published `ai-hist` crate the way an embedder does: from
crates.io, on the default features, with its own `Cargo.lock`.

```sh
cd examples/rust-consumer
cargo run
```

It stages a handful of transcripts from `crates/ai-hist/tests/fixtures` into a
throwaway `HOME`, opens a `SessionStore` there, runs a `sync`, walks the
catalog with `sessions()`, and for each catalogued session prints the usage
rollup from `session()`, the normalized totals folded by model over
`SessionEvidence::requests`, and how many user turns, markers and classified
control blocks the store holds for it. It then drains `changes_since` under a
named consumer cursor and commits it. It exits non-zero when the sweep did not
catalogue a staged session, or when no session reported usage, so it is a real
smoke test of the published artefact and not only of its compiler.

`RELAYHISTORY_FIXTURES` points it at the corpus when it is built away from the
repository checkout. The store is opened with an explicit
`StoreOptions::roots` (`ProviderRoots::from_home`), which reads nothing from
the environment, so a developer's exported `CODEX_HOME` — or any other
provider-root override — cannot reach the temporary store.

Two CI jobs build it:

- `rust-consumer` in `.github/workflows/ci.yml`, on every pull request, with
  `[patch.crates-io]` redirected at `../../crates/ai-hist` — and an assertion
  that the patch was applied, since Cargo only warns when it cannot be.
- `Published crate consumer` (`.github/workflows/published-crate-consumer.yml`),
  nightly and on dispatch, against the crate crates.io serves.

The `ai-hist` requirement here is stamped by `scripts/set-release-version.mjs`
at each release, so the nightly job follows the facade onto each published
version. See [`docs/sourcing-sdk.md`](../../docs/sourcing-sdk.md).

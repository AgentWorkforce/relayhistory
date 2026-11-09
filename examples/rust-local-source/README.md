# Rust local source example

A standalone consumer of the default public `ai-hist` API. Run from the repository:

```sh
cargo run --locked --manifest-path examples/rust-local-source/Cargo.toml
```

This stages synthetic host-owned Claude evidence in a temporary directory,
submits a local observation, reads its revision fence and hydrates it through
public source intake. It verifies native message identity, content and local
presence after reopening the store, including replay and scope controls. No
operator history or network service is accessed at runtime.

The example uses an explicit path dependency on this checkout and requires no
dependency version bump; release automation updates its lockfile entry. It is its own Cargo workspace with a checked-in lockfile.
It is a source extension example, not a published-artifact compatibility test.

See [Rust local connectors](../../docs/source-plugins.md#rust-local-connectors)
for the explicit host-driven acquisition boundary and source vocabulary.

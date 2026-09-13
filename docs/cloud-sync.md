# Cloud sync

Run `ai-hist enable-cloud` to authenticate, sync local history, and keep pushing
while the command runs. Use `--once` to drain and exit:

```sh
ai-hist enable-cloud --once
```

The npm CLI uses the async TypeScript SDK backed by the Rust engine. The Rust
cloud layer manages RelayHistory credentials, stage selection, token rotation,
and sync cursors. The SDK's `enableCloud()` runs the same workflow; `pushCloud()`
pushes using a stored session.

See [Cloud setup](enable-cloud.md) for login, stage selection, Git hooks, and
sharing, and the [SDK guide](../sdk-ts/README.md#cloud-token-and-replay) for token
export and transcript replay. [Remote connectors](remote-connectors.md) explains
how cloud sessions are discovered into the local session ledger.

# Local history CLI

`cargo run -p ai-hist-cli -- sync` ingests local provider history. This binary and
its native library do not load source plugin credentials or perform remote
acquisition. `--all` runs local acquisition; signing in to a provider or cloud
service never adds a data source.

Explicit `--remote` acquisition fails before opening the history database. For
remote acquisition, use the SDK CLI or an SDK host with installed and explicitly
configured source plugins. The optional `history-provider-sources` helper provides
Claude web and Codex cloud adapters; the RelayHistory helper provides commercial
recall and an explicit legacy Relaycast operation. Those packages depend on the
local history interfaces and are built separately.

Cached session listing still supports `--local`, `--remote`, and `--all`, including
remote observations previously committed through the generic source intake API.
Source connector flags remain accepted for compatibility and diagnostics; they do
not load plugins into this standalone binary.

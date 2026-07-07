# ai-hist-bin-darwin-x64

Prebuilt `ai-hist` binary for **darwin-x64**, published as an optional dependency
of `agent-relay`. The binary in `bin/ai-hist` is injected at publish time by
CI (`.github/workflows/publish-bin-packages.yml`); it is not committed.

Resolved at runtime by agent-relay's `ai-hist-path` resolver. Do not depend on
this package directly.

# Plan 009 execution record

Base: `b20fd1e` (0.16.0), reconciled from plan baseline `3155117`.
Only release version/lockfile changes occurred between those commits.

## Baseline

- `cargo test --workspace`: 460 passed, 2 existing benchmark tests ignored.
- `cargo fmt --all -- --check`: passed.
- `cargo clippy --workspace --all-targets -- -A clippy::too_many_arguments -D warnings`: passed.
- `node --test scripts/*.test.mjs`: 13 passed.
- Native debug build and contract 11 verification: passed; declarations unchanged.
- SDK lint and `npm test`: 130 passed on Node 22.
- Tests using localhost/process inspection were rerun outside the filesystem/network
  sandbox after initial permission failures. No production service was exercised.

## Characterization matrix

| Contract | Coverage | Follow-up |
| --- | --- | --- |
| Canonical session identity, local versus remote presence | Existing discovery tests plus new core scan-order tests | Replace remote connector overwrite behavior in observation migration |
| Connector-specific hydration progress | New checkpoint-key characterization | Add authoritative observation checkpoints |
| Retry after reopening unchanged storage | New outbox test | Preserve through coordinator extraction |
| Retry after evidence changes | New known-limitation characterization | Persist immutable queued payloads before dispatch |
| SDK cached scope/auth behavior | Existing remote-auth tests and new public-contract tests | Remove commercial auth from cached queries |
| Evidence identity and tied cursor ordering | New SDK public-contract fixtures | Preserve through export/native boundaries |
| Native auth, refresh, atomic replacement, redaction | Existing cloud/auth suites pass | Preserve in RelayHistory plugin |
| Transcript cursor with interleaved sessions | Reproduced A1/B2/A3 budget=1 data loss outside baseline | Separate cursor fix PR before extraction |

## PR sequence

1. Characterization tests and implementation plan.
2. Transcript cursor data-loss fix.
3. Explicit source connector selection and offline query semantics.
4. Observation migration, Rust extraction, durable delivery, plugin packaging,
   and isolation gate in dependent reviewable changes as specified by plan 009.

The plan remains IN PROGRESS until all acceptance criteria are implemented and
verified. Characterization intentionally records several existing violations;
passing those tests does not mean the boundary is already fixed.

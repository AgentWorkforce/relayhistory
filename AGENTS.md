# Working in relayhistory

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
npm install
```

- This repo owns all harness acquisition/parsing/storage. Burn consumes SessionStore and owns pricing/analytics; never add those product policies here.
- SessionStore is an in-process writer implementation, not a single writer process; preserve its schema/migration/locking/WAL boundaries. Update the capture matrix with provider changes.
- Plugin crates are outside the workspace; cargo test --workspace does not cover them. Run their direct suites when touched.
- Public API changes update crates/ai-hist/public-api.txt via scripts/check-public-api.mjs and add a Rust API changelog entry. All packages share one version; pending release severity only rises.
- Never raise rust-oleum ceilings. A burn-contract-drift failure may be ledger identity drift: investigate against base and consumer evidence rather than bypassing it.
- Read docs/sourcing-contract.md and the ownership ADR for sourcing changes; docs/releasing.md plus `docs/agent-reference.md` for release rules.

## Agency and guidance

- Act autonomously on routine assigned work; ask forgiveness rather than permission. Preserve explicit production, release and credential gates.
- For guidance, ask the engineering leads, the agent that spawned you, or the layered owner on Agent Relay.
- Only a truly pressing or blocking question goes to `khaliq-gant` or `will-washburn` (CODEOWNERS) on Agent Relay.

## Cross-repo context

- Before and during changes, understand how this repo fits the wider system; consult its repo map and related-repo docs where available.
- Check relevant agents’ summaries on Agent Relay and proactively contact owning agents about dependencies, adjacent changes and work in flight.

## Pull requests (when used)

- Open PRs as ready for review by default. If merging requires Khaliq’s OK, use `merge-train:hold` instead of draft status.
- If you open a PR, immediately subscribe to its feedback through Agent Relay using [this skill](https://github.com/AgentWorkforce/skills/blob/main/skills/subscribing-relay-webhooks-and-writebacks/SKILL.md), and verify subscription readiness.
- Evaluate each review finding. Address applicable feedback and explain declined changes in the thread.
- A PR is not ready while feedback remains unaddressed or merge conflicts remain.
- When ready, alert the CODEOWNERS and the agent that spawned you on Agent Relay.
- Keep related open PRs from going stale: find their authoring agent on Agent Relay and coordinate; if that agent is no longer on the relay, escalate to the CODEOWNERS or the layered owner.

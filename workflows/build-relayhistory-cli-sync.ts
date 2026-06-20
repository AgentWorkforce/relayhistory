/**
 * build-relayhistory-cli-sync.ts  (#3)
 *
 * Adds the cloud sync commands to the ai-hist CLI (this repo, crates/ai-hist):
 *   - `ai-hist cloud login`  — Login with Relay (RelayAuth device flow), token in keychain
 *   - `ai-hist cloud sync`   — read local history, ENCRYPT locally via ai-hist-core,
 *                              POST ciphertext to history.agentrelay.com /v1/sync
 *   - `ai-hist cloud pull`   — GET /v1/entries, decrypt locally, merge into the local db
 *
 * This is the client half that completes Phase 1 end-to-end ("sync encrypted history,
 * retrieve on another device"). All encryption happens HERE (or in ai-hist-core); the
 * cloud service only ever sees ciphertext.
 *
 * ⚠️ Requires #1 (relayhistory-cloud /v1 API) and #2 (ai-hist-core crypto + encrypted
 *    search) to exist. The workflow reads the live API contract + core surface at run
 *    time so it adapts. Dry-run first:
 *      relayflows run --dry-run workflows/build-relayhistory-cli-sync.ts
 *
 * Self-contained (different repo from the cloud helpers). Robustness model + the
 * interactive-path deviation are the same as relayhistory-cloud/workflows/README.md:
 * non-interactive agents, DAG edges, repair-before-failure, only the terminal
 * verify-commit is failOnError:true (passes committed OR handled-BLOCKED).
 */

import { workflow } from '@relayflows/core';

const NAME = 'build-relayhistory-cli-sync';
const ART = '.workflow-artifacts/build-relayhistory-cli-sync';
const CLOUD = '../relayhistory-cloud';
const W = 900_000;
const G = 600_000;

async function runWorkflow() {
  const result = await workflow(NAME)
    .description('Add `ai-hist cloud login/sync/pull` to the Rust CLI: Login with Relay, local encryption via ai-hist-core, ciphertext push/pull to history.agentrelay.com.')
    .pattern('dag')
    .channel('wf-build-relayhistory-cli-sync')
    .maxConcurrency(3)
    .timeout(7_200_000)
    .idleNudge({ nudgeAfterMs: 120_000, escalateAfterMs: 120_000, maxNudges: 1 })
    .repairable()
    .agent('planner', { cli: 'claude', preset: 'analyst', role: 'Plans the cloud subcommands from the live API + core surface.', retries: 2 })
    .agent('builder', { cli: 'codex', preset: 'worker', role: 'Implements the Rust cloud subcommands.', retries: 2 })
    .agent('tester', { cli: 'codex', preset: 'worker', role: 'Writes Rust roundtrip tests (HTTP mocked).', retries: 2 })
    .agent('fixer', { cli: 'codex', preset: 'worker', role: 'Repairs gate/review output until green.', retries: 2 })
    .agent('claude-reviewer', { cli: 'claude', preset: 'reviewer', role: 'First-pass reviewer (crypto/auth focus).', retries: 1 })
    .agent('codex-reviewer', { cli: 'codex', preset: 'reviewer', role: 'Second-pass reviewer.', retries: 1 })

    .step('preflight', {
      type: 'deterministic',
      command: [
        'set -e',
        'git rev-parse --show-toplevel >/dev/null',
        'test -f crates/ai-hist-core/src/lib.rs || (echo "ERROR: ai-hist-core missing"; exit 1)',
        'test -f crates/ai-hist/src/main.rs || (echo "ERROR: ai-hist CLI missing"; exit 1)',
        'command -v cargo >/dev/null || (echo "ERROR: cargo not available"; exit 1)',
        `test -f ${CLOUD}/docs/architecture.md || (echo "ERROR: sibling ${CLOUD} (the API contract) not found"; exit 1)`,
        `mkdir -p ${ART}`,
        'echo PREFLIGHT_OK',
      ].join(' && '),
      captureOutput: true,
      failOnError: true,
      timeoutMs: G,
    })
    .step('read-contract', {
      type: 'deterministic',
      dependsOn: ['preflight'],
      command: [
        `echo "=====API=====" && (sed -n '1,200p' ${CLOUD}/docs/architecture.md || true)`,
        `echo "=====KEYCUSTODY=====" && (sed -n '1,80p' ${CLOUD}/docs/encryption.md || true)`,
        'echo "=====CORE=====" && (sed -n \'1,140p\' crates/ai-hist-core/src/lib.rs || true)',
        'echo "=====SYNC=====" && (grep -n "fn sync\\|sync_state\\|RELAYCAST" crates/ai-hist/src/main.rs | head -30 || true)',
      ].join(' && '),
      captureOutput: true,
      failOnError: false,
      timeoutMs: G,
    })
    .step('plan', {
      agent: 'planner',
      dependsOn: ['read-contract'],
      task: [
        `Write ${ART}/plan.md for the \`cloud\` subcommands in crates/ai-hist, adapted to the LIVE contract + core surface:`,
        '{{steps.read-contract.output}}',
        'Cover: (1) `cloud login` — RelayAuth device flow, store the relay_at_/relay_rt_ token pair in the OS keychain. (2) `cloud sync` — read local history rows, derive content_hash, ENCRYPT each entry via ai-hist-core (master key + AES-256-GCM), and POST the ciphertext batch to /v1/sync (Bearer relay_at_), idempotent by content_hash. (3) `cloud pull` — GET /v1/entries (org-scoped), decrypt locally, merge into the local SQLite db. (4) build the encrypted search index entries on sync (per ai-hist-core). The CLI never sends plaintext or keys to the server. End with PLAN_COMPLETE.',
      ].join('\n'),
      verification: { type: 'output_contains', value: 'PLAN_COMPLETE' },
      timeoutMs: W,
      retries: 2,
    })
    .step('impl-cloud', {
      agent: 'builder',
      dependsOn: ['plan'],
      task: [
        `Implement per ${ART}/plan.md: add a \`cloud\` subcommand group to crates/ai-hist (login/sync/pull), wired into the CLI dispatch. Use ai-hist-core for all crypto (do not reimplement). Keep HTTP simple (curl/reqwest consistent with the existing relaycast client). Tokens via the keychain. Never transmit plaintext or keys.`,
      ].join('\n'),
      verification: { type: 'exit_code', value: '0' },
      timeoutMs: W,
      retries: 2,
    })
    .step('impl-tests', {
      agent: 'tester',
      dependsOn: ['plan'],
      task: [
        'Add Rust tests (HTTP mocked): encrypt->sync payload contains only ciphertext + metadata (no plaintext, no key); sync is idempotent by content_hash; pull->decrypt roundtrips back to the original plaintext locally. Runnable via `cargo test --workspace`.',
      ].join('\n'),
      verification: { type: 'exit_code', value: '0' },
      timeoutMs: W,
      retries: 2,
    })
    .step('gate-impl', {
      type: 'deterministic',
      dependsOn: ['impl-cloud', 'impl-tests'],
      command: [
        'test -n "$(git status --short -- crates)" || (echo "NO_CHANGES"; exit 1)',
        'grep -rq "cloud" crates/ai-hist/src/main.rs || (echo "MISSING cloud wiring"; exit 1)',
        'echo GATE_IMPL_OK',
      ].join(' && '),
      captureOutput: true,
      failOnError: false,
      timeoutMs: G,
    })
    .step('fix-impl', {
      agent: 'fixer',
      dependsOn: ['gate-impl'],
      task: ['If the gate is not GATE_IMPL_OK, finish the missing work. Output:', '{{steps.gate-impl.output}}'].join('\n'),
      verification: { type: 'exit_code', value: '0' },
      timeoutMs: W,
      retries: 2,
    })
    .step('build', {
      type: 'deterministic',
      dependsOn: ['fix-impl'],
      command: 'cargo build -q -p ai-hist-cli 2>&1 | tail -40; echo "EXIT:$?"',
      captureOutput: true,
      failOnError: false,
      timeoutMs: G,
    })
    .step('fix-build', {
      agent: 'fixer',
      dependsOn: ['build'],
      task: ['If the build failed, fix it and rerun `cargo build -p ai-hist-cli` until clean. Output:', '{{steps.build.output}}'].join('\n'),
      verification: { type: 'exit_code', value: '0' },
      timeoutMs: W,
      retries: 2,
    })
    .step('test', {
      type: 'deterministic',
      dependsOn: ['fix-build'],
      command: 'cargo test --workspace 2>&1 | tail -60; echo "EXIT:$?"',
      captureOutput: true,
      failOnError: false,
      timeoutMs: G,
    })
    .step('fix-test', {
      agent: 'fixer',
      dependsOn: ['test'],
      task: ['If tests failed, fix the test or source and rerun `cargo test --workspace` until green. Output:', '{{steps.test.output}}'].join('\n'),
      verification: { type: 'exit_code', value: '0' },
      timeoutMs: W,
      retries: 2,
    })
    .step('test-final', {
      type: 'deterministic',
      dependsOn: ['fix-test'],
      command: 'cargo test --workspace 2>&1 | tail -60; echo "EXIT:$?"',
      captureOutput: true,
      failOnError: false,
      timeoutMs: G,
    })
    .step('fix-test-final', {
      agent: 'fixer',
      dependsOn: ['test-final'],
      task: [`If the rerun is still red, fix until green. If unfixable, write ${ART}/BLOCKED_NO_COMMIT.md with exact evidence. Output:`, '{{steps.test-final.output}}'].join('\n'),
      verification: { type: 'exit_code', value: '0' },
      timeoutMs: W,
      retries: 2,
    })

    // ── Fresh-eyes review: Claude then Codex (crypto/auth focus) ──
    .step('claude-review', {
      agent: 'claude-reviewer',
      dependsOn: ['fix-test-final'],
      task: [
        'Fresh-eyes review the cloud subcommands from scratch. Read the actual files + "git diff" + the contract: {{steps.read-contract.output}}',
        'Confirm: NO plaintext or key is ever sent to the server; all crypto goes through ai-hist-core (not reimplemented); tokens stay in the keychain; sync is idempotent by content_hash; pull decrypts locally only.',
        `Write ${ART}/claude-review.md with findings or NO_ISSUES_FOUND.`,
      ].join('\n'),
      verification: { type: 'exit_code', value: '0' },
      timeoutMs: G,
      retries: 1,
    })
    .step('claude-fix', {
      agent: 'fixer',
      dependsOn: ['claude-review'],
      task: [`Read ${ART}/claude-review.md. Fix every valid finding, add tests, rerun cargo build+test until green. Write ${ART}/claude-fix.md. If NO_ISSUES_FOUND, record that.`].join('\n'),
      verification: { type: 'exit_code', value: '0' },
      timeoutMs: W,
      retries: 2,
    })
    .step('claude-review-final', {
      agent: 'claude-reviewer',
      dependsOn: ['claude-fix'],
      task: [`Fresh post-fix review from scratch. Write ${ART}/claude-review-final.md with findings or NO_ISSUES_FOUND.`].join('\n'),
      verification: { type: 'exit_code', value: '0' },
      timeoutMs: G,
      retries: 1,
    })
    .step('claude-fix-final', {
      agent: 'fixer',
      dependsOn: ['claude-review-final'],
      task: [`If findings remain, fix + add tests + rerun until green. If unfixable, write ${ART}/BLOCKED_NO_COMMIT.md. If NO_ISSUES_FOUND, write ${ART}/claude-signoff.md.`].join('\n'),
      verification: { type: 'exit_code', value: '0' },
      timeoutMs: W,
      retries: 2,
    })
    .step('codex-review', {
      agent: 'codex-reviewer',
      dependsOn: ['claude-fix-final'],
      task: [`Second-pass fresh-eyes review of the post-Claude-fix state from scratch. Write ${ART}/codex-review.md with findings or NO_ISSUES_FOUND.`].join('\n'),
      verification: { type: 'exit_code', value: '0' },
      timeoutMs: G,
      retries: 1,
    })
    .step('codex-fix', {
      agent: 'fixer',
      dependsOn: ['codex-review'],
      task: [`Read ${ART}/codex-review.md. Fix every valid finding, add tests, rerun cargo build+test until green. If unfixable, write ${ART}/BLOCKED_NO_COMMIT.md. If NO_ISSUES_FOUND, write ${ART}/codex-signoff.md.`].join('\n'),
      verification: { type: 'exit_code', value: '0' },
      timeoutMs: W,
      retries: 2,
    })

    // ── Acceptance + green-only commit (handled-blocked, never crash) ──
    .step('final-acceptance', {
      type: 'deterministic',
      dependsOn: ['codex-fix'],
      command: `test -f ${ART}/BLOCKED_NO_COMMIT.md && echo "BLOCKED" || (cargo build -q -p ai-hist-cli && cargo test --workspace && echo ACCEPTANCE_OK) 2>&1 | tail -40`,
      captureOutput: true,
      failOnError: false,
      timeoutMs: G,
    })
    .step('repair-acceptance', {
      agent: 'fixer',
      dependsOn: ['final-acceptance'],
      task: [
        'If acceptance did not print ACCEPTANCE_OK and is not BLOCKED, fix and rerun cargo build+test until ACCEPTANCE_OK.',
        `If genuinely unfixable, ensure ${ART}/BLOCKED_NO_COMMIT.md exists. Else do nothing.`,
        'Output:',
        '{{steps.final-acceptance.output}}',
      ].join('\n'),
      verification: { type: 'exit_code', value: '0' },
      timeoutMs: W,
      retries: 2,
    })
    .step('commit', {
      type: 'deterministic',
      dependsOn: ['repair-acceptance'],
      command: [
        'set +e',
        `if [ -f ${ART}/BLOCKED_NO_COMMIT.md ]; then echo "BLOCKED — skipping commit"; exit 0; fi`,
        '( cargo build -q -p ai-hist-cli && cargo test --workspace )',
        'GREEN=$?',
        `if [ "$GREEN" != "0" ]; then echo "NOT_GREEN — writing BLOCKED"; printf "%s\\n" "Acceptance not green at commit time." > ${ART}/BLOCKED_NO_COMMIT.md; exit 0; fi`,
        `git add crates ${ART}`,
        'git commit -m "feat(ai-hist): cloud login/sync/pull — Login with Relay + local-encrypt ciphertext sync" || echo "nothing to commit"',
        'echo COMMIT_DONE',
      ].join('\n'),
      captureOutput: true,
      failOnError: false,
      timeoutMs: G,
    })
    .step('verify-commit', {
      type: 'deterministic',
      dependsOn: ['commit'],
      command: [
        `if [ -f ${ART}/BLOCKED_NO_COMMIT.md ]; then echo "HANDLED_BLOCKED — see ${ART}/BLOCKED_NO_COMMIT.md"; exit 0; fi`,
        'if git log -1 --pretty=%s | grep -q "cloud"; then echo "COMMIT_OK"; else echo "COMMIT_MISSING"; exit 1; fi',
      ].join('\n'),
      captureOutput: true,
      failOnError: true,
      timeoutMs: G,
    })

    .onError('retry', {
      maxRetries: 2,
      retryDelayMs: 10_000,
      repairAgent: 'fixer',
      repairRetries: 2,
      onExhaustion: 'needs-human',
    })
    .run({ cwd: process.cwd() });

  console.log('Workflow status:', result.status);
}

runWorkflow().catch((error) => {
  console.error(error);
  process.exit(1);
});

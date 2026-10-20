# OMP acquisition research (#430)

Status: implementation complete; PR review and final CI in progress. OMP is separate
from Pi. Inspection on macOS arm64, 2026-10-11 pins
[`bd4dffd5e617ce732f8a14249b0be1e6c7621886`](https://github.com/can1357/oh-my-pi/tree/bd4dffd5e617ce732f8a14249b0be1e6c7621886),
`@oh-my-pi/pi-coding-agent` 18.9.2. Sources and executed-helper digests are in
[fixtures/omp/provenance.json](fixtures/omp/provenance.json). No private history, authentication, model calls or agent CLI execution was used; the isolated public SessionManager persistence library was executed with synthetic inputs. The merged Pi/Hermes pilots establish the intake used by this provider.

## Physical and logical format

[Official session documentation](https://github.com/can1357/oh-my-pi/blob/bd4dffd5e617ce732f8a14249b0be1e6c7621886/docs/session.md)
and `session-entries.ts` define current logical version **3**. Physical files
start with a **256 UTF-8-byte title JSON slot including newline**, followed by
ordinary header/entry JSONL; the reader also accepts legacy direct headers. The
slot folds title/titleSource into the header, including clearing an old title.
It is not a conversation entry. Unicode titles are truncated at code points to
fit the byte budget. Same-size in-place title changes invalidate stat-only or
append-only assumptions.

Headers have a native string ID (new writer UUIDv7), cwd, timestamp and optional
additionalDirectories, previousSessionFiles, providerPromptCacheKey and opaque
parentSession. Entry IDs usually use eight hex characters but collision handling
can use a longer Snowflake; never reject IDs solely on fixed width. `parentId`
forms a tree. Physical files retain branches, while active-context readers choose
a leaf; an acquisition layer must distinguish all retained history from active
rendered context.

Native message roles include user, developer, assistant and camel-case
`toolResult`, plus execution/hook/file-mention records. Assistant content
`toolCall.id` joins `toolResult.toolCallId`. `custom_message`, compaction and
branch summaries are control/context evidence, not automatically human prompts.
`model_usage` records auxiliary model work; do not fabricate assistant messages.
Persisted usage contains input/output/cacheRead/cacheWrite/totalTokens/cost when
written. Official context normalization fills absent counters with zeros; raw
acquisition must retain missingness instead. The synthetic partial-usage case
makes that distinction explicit.

## Blobs, containment and persistence

`blob-store.ts` writes canonical extensionless files under the current agent data
`blobs/<sha256>` root, with optional extension sidecars. `blob:sha256:` references
must contain exactly 64 lowercase hex digits; official `parseBlobRef` rejects
traversal suffixes. This syntax check does not establish containment through
symlinks. Acquisition constrains session roots before reading; blob references are retained
as raw markers without opening their targets or claiming resolved image bytes.
The official resolver retains unresolved refs when a blob is missing; absence
must not become fabricated image bytes. Staging names are hidden temporary files
renamed atomically into the digest address.

Persistence can externalize images (including image URLs/generation/frame cases).
It also truncates large ordinary text at 500,000 characters with a persistence
notice; the notice cannot recover omitted bytes. These are distinct mechanisms.
The one-pixel synthetic PNG proves a content digest, not all image codecs or
filesystem resolution paths.

## Roots and precedence

`packages/utils/src/dirs.ts` and `session-paths.ts` own roots. The default is
`~/.omp/agent/sessions/<encoded-cwd>`, with blobs in the same agent data root.
`PI_CONFIG_DIR` changes the configuration directory name. `OMP_PROFILE` takes
precedence over legacy `PI_PROFILE`; an explicitly empty OMP value selects the
default profile. Named profiles are managed beneath `.omp/profiles/<name>/agent`.
A named profile ignores the default-profile `PI_CODING_AGENT_DIR` override.
The resolver applies existing `XDG_DATA_HOME/omp` data directories on Linux **and
macOS** when default conditions hold; profile-specific data directories must
exist. Follow pinned code rather than stale platform comments.

`cli/args.ts` seeds `sessionDir` from `PI_CODING_AGENT_SESSION_DIR`; explicit CLI
session-directory/file options and SessionManager's explicit sessionDir can
select custom paths. Session storage and blob roots are separate: a custom
session path does not imply blobs are adjacent to it. Custom-file registry paths
are not permission to follow unrestricted external files. Intake freezes the selected roots/provenance once; phase 2 tests the documented
override/profile combinations.
No cross-platform root behavior was executed in this research.

Pi also uses `PI_CODING_AGENT_DIR` and `PI_CODING_AGENT_SESSION_DIR`.
The OMP adapter tests co-installation with those overrides: an explicit
shared directory and a `version: 3` header alone do not establish which product
wrote a legacy transcript. Current OMP title slots provide a physical distinction,
but legacy direct headers need an evidence-based classification policy. The completed policy admits only title-slot OMP files and diagnoses ambiguous
legacy direct headers. The shared-root regression proves no file is admitted
under both providers.

## Fork, move and rewrite semantics

`SessionManager.fork`, `forkFrom` and branched-session creation create a new header
identity while copying entry IDs. parentSession can be a source ID **or path**,
so preserve the raw fact before interpreting relationships; a fork alone is not
proof of agent delegation. Forks may inherit prompt-cache keys. `moveTo` preserves
identity while changing cwd, managed filename and previousSessionFiles. Atomic
`rewriteEntries`, discard/truncation and title overlays alter old data; the provider reconciles removals while preserving independent remote evidence,
with separate complete-rewrite, partial-tail, shared-prompt and deletion probes.

## Initial research evidence (superseded by phase 2 below)

Five authored files are checked by checksum-pinned official
`parseSessionContent`/`parseJsonlLenient`, actual title-slot serialization/overlay,
and actual blob-reference parser. Bun **1.3.14** was downloaded into `/private/tmp`
from the official release without changing the installed runtime. Upstream utils
requires Bun >=1.3.14; the workspace advertises >=1.4. This proves compatibility of
the executed pure helpers, not the whole upstream application on 1.3.14.

```sh
node --test plans/history-providers/fixtures/omp/validate.test.mjs
# 6 tests pass; damaged width/parent/tool-ID controls are rejected
/private/tmp/omp-bun-1.3.14/bun-darwin-aarch64/bun \
  plans/history-providers/fixtures/omp/oracle.mjs /private/tmp/relayhistory-omp-upstream
# 5 sessions match pinned official parser; title/blob probes pass
```

Replacing the changed title with the original title makes the official-oracle
assertion fail; restoring the fixture passes. This negative control is recorded
in `/private/tmp/sol-omp-title-negative.log`. The oracle reports one malformed
final record while preserving preceding entries.
The checked results distinguish equal-size title mutation, copied fork entries
and destructive rewrite. At the initial research stage, writer capture, full rendered transcript and
public-store reconciliation remained unverified. Phase 2 below records which
requirements were completed and which remain intentional coverage limits.

## Phase 2 implementation (2026-10-14)

The provider now acquires the frozen, contained title-slot v3 JSONL snapshot
through discovery, selected hydration and scoped/full sweeps. Every physical
entry remains available, including inactive branches; only native user,
assistant and toolResult records become conversation evidence. Other message
roles, control entries, image/blob references and parentSession remain bounded
raw markers. Blob contents are intentionally not opened or advertised as
captured images. Usage counters retain field coverage and missing usage remains
unavailable. Native entry IDs and actual toolCallId joins are preserved.

OMP requires its 256-byte title-slot signature. Legacy direct v3 headers are
ambiguous with Pi and produce OMP_AMBIGUOUS_LEGACY rather than being imported
twice under shared PI_CODING_AGENT_* roots. Pi's existing parser rejects the
physical title slot; a shared-root conformance case proves each product admits
only its distinct format. Unknown legacy ownership is an explicit coverage gap.

Root resolution freezes PI_CODING_AGENT_SESSION_DIR first; otherwise OMP_PROFILE
(including explicit empty) precedes PI_PROFILE. Named profiles ignore the default
agent override. PI_CONFIG_DIR changes the config-relative root. Existing
XDG_DATA_HOME/omp and profile-specific data directories are selected on macOS
and Linux under the same default-agent condition as upstream. from_home ignores
all environment settings. Invalid profiles are frozen as provider configuration
errors, survive serialization, and fail the OMP sweep before filesystem access.
Aggregate sync can still complete when sibling providers succeed; it logs the
actionable OMP failure and retains prior OMP evidence.
No credentials, config files or custom-file registries are inspected.

The new capture.mjs runs the checksum-pinned published 18.9.2 SessionManager in
an isolated temporary data directory: create → user/tool/assistant → title →
flush/close → reopen/resume → forkFrom. It sanitizes random session/entry IDs and
wall-clock times consistently, preserving the official serialized title slot.
writer-session.jsonl and writer-fork.jsonl are the persisted artifacts;
writer-oracle.json contains independent loadSessionMessagesReadOnly output and
reopened getEntries output. The npm writer source exactly matches pinned git
source SHA256 6ea1265b55a945b5ea9c9031497252b839960c37237e1f8c7c7284ddc2df6135.
The real writer fixture is admitted alongside the earlier authored partial-usage
and blob-reference fixture, whose official pure-reader oracle remains separate.

Reproduction (no auth or model request):

```sh
npm install --prefix /private/tmp/omp-writer-phase2 --ignore-scripts @oh-my-pi/pi-coding-agent@18.9.2
/private/tmp/omp-bun-phase2/bun-darwin-aarch64/bun plans/history-providers/fixtures/omp/capture.mjs /private/tmp/omp-writer-phase2/node_modules/@oh-my-pi/pi-coding-agent /private/tmp/omp-capture-final
```

Acceptance results and named negative controls are recorded below once complete.
Remaining intentional limits: ambiguous legacy headers, migration of v1/v2,
active-context projection, resolved blob bytes, custom-file registry traversal,
Windows runtime execution, inferred relationships/file edits and native resume.

## Verification record

The native writer capture was reproduced twice; sanitized session, fork and
reader-oracle outputs match byte-for-byte. The initial official pure-reader
oracle and six research fixture validation tests remain successful.

Before the final coexistence-retirement regression was added, thirteen provider
unit/lifecycle tests, seven public conformance tests, all 67 corpus tests and ten
source-registry tests passed. The public suite includes export paging through
the shared helper, real writer/independent reader comparison, repeat stability,
partial tails, title-only rewrites, primary symlink refusal, collision handling,
shared Pi roots and frozen invalid profile errors. The root-error seam's named
round-trip test ran one real case. Full SDK verification: 280 passed, zero
failed, one skipped; native contract 28 verified. Public SDK → CLI → MCP surface
proof passed in an isolated synthetic child home. Strict all-target/all-feature
Clippy passed; rust-oleum 0.1.2 passes with no baseline increase.

Negative control: changing the admitted slot version from 1 to 2 makes these six
public acceptance cases fail: `omp_and_pi_shared_override_admit_each_file_once`,
`omp_append_partial_tail_rewrite_and_metadata_changes`,
`omp_discover_hydrate_read_reopen_changes_export_fullsync`,
`omp_invalid_frozen_root_configuration_is_not_successfully_empty`,
`omp_missing_empty_unsupported_malformed_and_collision_are_truthful`, and
`omp_real_writer_reopen_and_independent_full_reader_oracle`. Restoring version 1
restores all seven. This distinguishes working acquisition from a registered but
unimplemented module. Damaged title width, partial-tail and unknown-usage controls
are separate from the disabled-parser probe.

Final coexistence fix: a completely validated legacy direct v3 header remains
an actionable ambiguous-ownership discovery diagnostic, but its intentionally
nonadmitted format does not count as an unread OMP file during a sweep. Therefore
a Pi file in an explicitly shared root cannot indefinitely block retirement of
a confirmed missing OMP locator. Malformed or incomplete OMP evidence still
increments unread and blocks whole-source retirement. The added public
`omp_shared_root_pi_file_does_not_block_missing_omp_retirement` case exercises
OMP+Pi → delete OMP → full sync while retaining Pi. Final broad checks are
recorded in the implementation handoff after they complete.

Final identity-replacement repair: changing a complete locator's native header
from A to B now retires A's local share in the same capture transaction, after
B's events have been written. Partial replacement preserves A, independent
remote shares survive, and shared prompt ownership can move to B. Generation
`omp_v3_2` forces old unchanged-byte checkpoints to run the repaired normalizer.
The public suite now has nine cases; the provider unit/lifecycle suite has
fifteen, including explicit denied-root and primary-only blob-reference reads.

The native SDK red probe `same_locator_native_identity_replacement_control`
reproduced A+B surviving one locator before the fix. Omitting the single
`retire_locator_sessions` call makes
`omp_same_locator_identity_replacement_preserves_remote_share` fail: A wrongly
retains `[local, remote]` instead of `[remote]`. Restoring the call restores that
case and the named public complete-versus-partial replacement regression. A
fresh independent review approved the complete-only helper ordering, remote
preservation and generation bump without findings.

## Final local checks and CI follow-up

After the identity repair, the nine public OMP cases and fifteen provider unit
cases passed, including both identity-replacement regressions after restoring
the omitted-helper negative control. Strict Rust **1.99.0** Clippy passed with
`--workspace --all-targets --all-features -- -D warnings`; the CI-only octal
permissions lint was corrected. The quality gate passes without a baseline
increase after extracting complete-capture title/first-prompt refresh into a
small private helper. Generation-v2 corpus regeneration changed only the stamp
prefix in the two owned snapshots; the payload SHA256 values stayed identical.
The final corpus run passed all 67 cases and the public OMP run passed all nine.

Native bindings were rebuilt for the v2 identity repair. The full SDK suite
passed **280**, failed **0**, skipped **1**. The strictly isolated SDK/CLI/MCP
surface proof additionally verifies replacing the native identity at the same
locator removes the old catalog identity. Native contract **28** remains valid.
Rust 1.99 formatting, provider generation checks, public API (1059 items), the
six research controls and both official writer/reader fixture oracles passed.
The public API addition is the non-exhaustive `Source::Omp` variant.

Broader local runs are deliberately distinguished from those final focused
checks. With elevated local socket permissions and canonical
`TMPDIR=/private/tmp`, the no-default-features run completed **1558 passed, zero
failed, two ignored** on the tree compiled before the identity repair. The full
workspace run on that earlier tree reached **1706 passed**, then stopped at four
timing-sensitive live-capture failures under concurrent build/test load. A
serial rerun of the entire live-capture target passed **75/75**. This is not a
claim of a clean final-head full workspace run; the PR's Linux CI supplies that
separate gate. Initial runs using macOS's long aliased temporary path also hit
socket-length and gitdir-alias failures; canonical short TMPDIR eliminated them.

`cargo publish --dry-run -p ai-hist --allow-dirty` completed package verification
without publishing (188 files, 6.0 MiB uncompressed). The package's OMP source
members were checked byte-for-byte against the v2 identity-repair source before
the private quality-only helper extraction. No package versions, credentials,
remote services, merge switches or deployment state were changed.

At PR head 60ec5896, CI identified the two stale generation prefixes and a new
complexity violation in `normalize::write`; both are fixed above. No automated
inline review findings were present at that inspection. New CI/review feedback
is handled against the final pushed head, separately from this capture record.


## Review fixes at generation v3

Concrete review regressions reproduced stale catalog titles after a complete
empty title-slot rewrite and missing opaque evidence for image-only tool results.
Both named `omp_review_` tests failed against the preceding implementation
(two failures), then passed with explicit complete-capture title replacement and
bounded opaque tool-result markers. Partial tails preserve the prior title and
remote-shared event evidence. The tool-result capture records only its session
file read; blob references remain opaque and removed blocks retire on rewrite.

Explicit session-directory overrides now bypass irrelevant invalid profile
selection, matching upstream directory precedence. Isolated process tests also
cover canonical empty/default profile selection discarding an inherited legacy
profile-derived agent override, as pinned upstream does. No shared title helper
or Pi provider behavior changed.

Final focused verification: **17** OMP unit/lifecycle cases, **10** public
provider cases and **67** corpus cases passed. Actual corpus regeneration moved
only the two source-stamp prefixes to `omp_v3_3`; payload hashes are unchanged.
Generation v3 forces prior captures to reacquire the newly preserved evidence.
Strict Rust 1.99 workspace Clippy, the unchanged quality ratchet and generated
provider drift checks are checked again for this review batch. Earlier v2 SDK,
package and broad-suite results above retain their stated compiled-tree scope;
final-head CI supplies native/SDK and Linux workspace verification.

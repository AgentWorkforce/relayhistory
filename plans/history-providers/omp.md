# OMP acquisition research (#430)

Status: research only; **no production provider or registration**. OMP is separate
from Pi. Inspection on macOS arm64, 2026-10-11 pins
[`bd4dffd5e617ce732f8a14249b0be1e6c7621886`](https://github.com/can1357/oh-my-pi/tree/bd4dffd5e617ce732f8a14249b0be1e6c7621886),
`@oh-my-pi/pi-coding-agent` 18.9.2. Sources and executed-helper digests are in
[fixtures/omp/provenance.json](fixtures/omp/provenance.json). No private history,
authentication, model calls or agent execution was used. Provider implementation
must wait for pilot integration and the acceptance gaps below.

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
symlinks. Future acquisition must canonicalize and constrain roots before reading,
record missing/denied blobs truthfully, and never follow arbitrary references.
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
are not permission to follow unrestricted external files. Future intake must
freeze selected roots/provenance once and test every override/profile combination.
No cross-platform root behavior was executed in this research.

Pi also uses `PI_CODING_AGENT_DIR` and `PI_CODING_AGENT_SESSION_DIR`.
The future OMP adapter must test co-installation with those overrides: an explicit
shared directory and a `version: 3` header alone do not establish which product
wrote a legacy transcript. Current OMP title slots provide a physical distinction,
but legacy direct headers need an evidence-based classification policy. This
research does not claim that policy is solved or allow duplicate admission of
one ambiguous file under both sources.

## Fork, move and rewrite semantics

`SessionManager.fork`, `forkFrom` and branched-session creation create a new header
identity while copying entry IDs. parentSession can be a source ID **or path**,
so preserve the raw fact before interpreting relationships; a fork alone is not
proof of agent delegation. Forks may inherit prompt-cache keys. `moveTo` preserves
identity while changing cwd, managed filename and previousSessionFiles. Atomic
`rewriteEntries`, discard/truncation and title overlays alter old data; a provider
must reconcile removals and preserve independent remote evidence. Pi append-only
assumptions cannot be imported without these probes.

## Executed evidence and remaining limits

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
and destructive rewrite. No current install capture, official full rendered
transcript (`loadSessionMessagesReadOnly`), migration matrix, active-leaf/compaction
context, filesystem blob resolution, symlink escape, Windows roots or destructive
public-store reconciliation has been verified. These remain implementation
acceptance requirements. Fixtures are deliberately outside the admitted corpus;
there is no evidence/capability claim in production yet.

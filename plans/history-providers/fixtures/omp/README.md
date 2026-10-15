# OMP research fixtures

Synthetic, not captured operator history. Revision/version and SHA-256 hashes of
executed upstream sources are in `provenance.json`. The one-pixel PNG is synthetic;
its extensionless blob filename is its content digest. No agent/model calls or
credentials are required. The initial authored fixtures began outside the registered corpus; phase 2
below records the actual writer capture and admitted corpus.

From the repository root:

```sh
node --test plans/history-providers/fixtures/omp/validate.test.mjs
bun plans/history-providers/fixtures/omp/oracle.mjs /path/to/pinned/oh-my-pi
```

Use Bun 1.3.14 or another compatible runtime exposing `Bun.JSONL.parseChunk`;
installed Bun 1.3.4 cannot execute that reader. The oracle refuses changed
upstream source hashes before executing extracted, transpiled pure functions.
It executes the official `parseSessionContent`, `parseJsonlLenient`, title-slot
parser/serializer and `parseBlobRef`, without loading the agent application.
It compares results with checked `oracle-results.json`. It does **not** execute
`loadSessionMessagesReadOnly`, migrations, active-leaf context building, lazy
image hydration or filesystem blob-resolution behavior.

`generate.mjs /path/to/pinned/oh-my-pi` authors the files using the official title
serializer/overlay. Then run `oracle.mjs ... --write` only for an intentional
reviewed oracle refresh. Inputs are authored v3 shapes, not writer captures.

Cases: ordinary user/assistant/native tool-result; equal-size title replacement;
fork with retained entry IDs; rewrite dropping descendants; incomplete tail.
Raw assistant usage has only `input:12`, so missing output/cost cannot silently
become known zero. Structural negative controls reject a damaged title width,
missing parent and mismatched tool identity. Hash syntax validation is not a
symlink-containment proof; filesystem acquisition remains future acceptance work.

Phase 2 adds actual persistence-library capture. `capture.mjs` imports the
checksum-pinned public `@oh-my-pi/pi-coding-agent@18.9.2` SessionManager, writes
synthetic user/assistant/tool/title history, closes and reopens it, appends a
resumed answer and forks it. `writer-session.jsonl` and `writer-fork.jsonl` are
its sanitized outputs; `writer-oracle.json` records both the reopened native
entry reader and the independent `loadSessionMessagesReadOnly` transcript.
Only random identities and current wall-clock timestamps are sanitized, with
all parent/entry references changed consistently. The real writer fixture and
the initial authored partial-usage fixture now also live under the registered
OMP corpus; the initial pure-helper research limitation above applies only to
`oracle.mjs`, not the writer capture.

```sh
npm install --prefix /private/tmp/omp-writer-phase2 --ignore-scripts @oh-my-pi/pi-coding-agent@18.9.2
bun plans/history-providers/fixtures/omp/capture.mjs /private/tmp/omp-writer-phase2/node_modules/@oh-my-pi/pi-coding-agent /private/tmp/omp-synthetic-output
# after this checkout's native debug build and SDK build:
node plans/history-providers/fixtures/omp/surface-proof.mjs .
```

The surface proof isolates HOME and all provider overrides in a child process,
then checks public SDK discovery/hydration/evidence/search, CLI search, and MCP
source schema/capabilities/search with positive OMP and negative Pi filters.
It never reads provider auth/configuration or invokes a model.

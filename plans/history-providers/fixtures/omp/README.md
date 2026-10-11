# OMP research fixtures

Synthetic, not captured operator history. Revision/version and SHA-256 hashes of
executed upstream sources are in `provenance.json`. The one-pixel PNG is synthetic;
its extensionless blob filename is its content digest. No agent/model calls or
credentials are required. These fixtures are outside the registered corpus and
do not admit an OMP provider.

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

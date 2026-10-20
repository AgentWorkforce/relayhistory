# Qwen Code native history evidence

Upstream QwenLM/qwen-code revision `82bd240ad0d31fd1d3a83dd803625118019ba35d`
identifies itself as version 0.25.3. `provenance.json` pins every source module
and fixture byte digest. No published package, model endpoint, credentials, or
private history was used.

`capture.cjs` transpiles the pinned official `Storage`, `ChatRecordingService`,
`SessionWriterLease` and independent `prepareTranscriptRecords` implementation.
It invokes the real filesystem writer lease in interactive mode with synthetic
configuration, deterministic UUIDs and a frozen clock. Automatic model titles
are disabled. Git branch, current-agent identity, logging, and tool-boundary
telemetry are isolated test dependencies. Missing runtime dependencies throw;
the unused legacy async-mutex writer path throws if entered. These fixtures prove
the native recording service, not an authenticated CLI/model execution.

Reproduce with an existing TypeScript compiler (no dependency install required):

```sh
python3 plans/history-providers/fixtures/qwen-code/fetch.py /tmp/qwen-pinned-source
QWEN_CAPTURE_SOURCE=/tmp/qwen-pinned-source TYPESCRIPT_PATH=/absolute/path/to/typescript/lib/typescript.js \
  node plans/history-providers/fixtures/qwen-code/capture.cjs /tmp/qwen-new-capture
node --test plans/history-providers/fixtures/qwen-code/validate.test.cjs
```

Six actual recorder cases cover ordinary text/usage, identified tools/results,
restart with restored parent, rewind with discarded branch, trailing title and
non-conversation artifacts, and fourteen messages sharing one millisecond.
`official-reader.json` is the separate upstream reader's result, not an adapter
projection. Rust compares every active native record with that oracle. Public
conformance additionally exercises discovery/hydration/read/search, reopen,
repeated hydration, export, source filtering and one-event pagination.
Malformed tails, fragmented UUIDs, missing parents, unknown provenance, symlink
escape and rewrite tests are explicitly derived mutations of native records.

The adapter supports ordinary `projects/<sanitized-project>/chats/<session>.jsonl`
files. `QWEN_RUNTIME_DIR` overrides `QWEN_HOME`; the default is `~/.qwen`.
Resolver snapshots are immutable. Explicit projects roots represent runtime
configuration supplied through upstream static/async configuration APIs; those
in-process settings cannot be discovered after a process exits. Identity combines
runtime-root/project namespace with native session ID. Complete records at EOF
without a newline are accepted; partial final JSON is deferred, malformed JSON
fails. Missing parents and cycles fail without inserting a sparse answer first.
A bounded 64KiB prefix/tail may leave activity/preview unknown until hydration;
it never invents file-mtime message timestamps or active-branch previews.

UUID fragments concatenate parts; the last native conversation leaf selects
its parent chain. Complete replay retires old normalized branch evidence while
raw records remain markers. Typed tool arguments/results join only through
recorded IDs; opaque/missing-ID parts remain markers. Reasoning text stays raw.
Unknown provenance is control evidence; absent legacy user/assistant provenance
fails explicitly rather than pretending to recover human messages.

Usage retains the recorder's stored counters. Normalized input subtracts cached
content from inclusive prompt count, with underflow rejected. Provider total is
preserved. Upstream cache-write/reporting provenance is a WeakMap and is absent
from JSONL; upstream can estimate thoughtsTokenCount. Therefore cache-write,
measured reasoning and billing provenance are unknown, not reconstructed.

Transactional `managed_session_*_v1` journals use a separate authority/commit
projector and are explicitly unsupported with a diagnostic. Native execution,
parent-session linking, compaction accounting, attachment interpretation and
actual released CLI/UI writer runs are not certified by this capture.

`derived-reader.json` separately labels a UUID-fragment mutation with later
usage/time and a trailing non-conversation artifact. The independent native
reader produces its expected aggregate; this is a derived robustness control,
not an additional native writer capture.

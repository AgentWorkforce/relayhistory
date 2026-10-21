# Kimi Code native fixtures

These are **current Kimi Code CLI 2.2.0**, official `MoonshotAI/kimi-code` commit
`472192c34c1efad89a369870eff0da0b708ce9bb`, not archived Python `kimi-cli` 1.52.
The released darwin-arm64 binary creates state v2 sessions and agent wire v1.5 files.
`provenance.json` pins downloaded source, release archive and executable hashes.
`manifest.json` records raw and sanitized native file hashes.

`capture.py` runs four cases in isolated HOME directories, with synthetic keys,
telemetry disabled and a localhost OpenAI-compatible fixture server. The first
run writes ordinary messages, a real Read call on a synthetic file, a known-ID
restart continuation, and an actual Agent child. No operator configuration,
credentials or history are read. No authenticated or external model is called.
Read-only `kimi export` runs after process exit, before continuation; its raw ZIP
is retained in the capture directory. Resume uses only the ID just created by
the fixture writer; it is never a discovery mechanism.

`append-rewind.mjs` adds a fifth case by invoking the **official pinned source
`buildUndoSwitchRecords` constructor** on a copied native ordinary journal.
This proves constructor-shaped branch/paired-undo semantics, not an interactive
TUI undo capture. `read-oracle.mjs` executes official `flattenChain` followed by
`foldWireRecords` after the writer process has exited. It does not import
RelayHistory. The oracle preserves raw source message, tool ID and argument
semantics independently of our adapter. Global/injected instructions are
controls, rather than human prompts.

Reproduce with the pinned source checkout and checksum-verified release binary:

```sh
python3 capture.py /path/to/pinned/kimi /tmp/kimi-fixture-capture
npm install --prefix /tmp/kimi-reader-deps --ignore-scripts immer@11.1.0 zod@4.3.6
export KIMI_SOURCE=/path/to/pinned/kimi-code-source
export KIMI_READER_DEPS=file:///tmp/kimi-reader-deps
node --experimental-transform-types --loader ./source-loader.mjs append-rewind.mjs /tmp/kimi-fixture-capture
node --experimental-transform-types --loader ./source-loader.mjs read-oracle.mjs /tmp/kimi-fixture-capture
python3 sanitize.py /tmp/kimi-fixture-capture
node --test validate.test.mjs
```

Sanitation consistently replaces generated UUIDs/ULIDs and fixture workspace
paths, and shifts millisecond clock values without changing relative timing or
rounding. Native protocol records, tool output, provider-supplied synthetic
usage and duplicate stream/context records remain intact. Runtime latency
measurements remain recorded, so independent captures are not claimed to be
byte-identical. No timestamps, usage or relationships are inferred from mtime,
context-window totals, token estimates or tool names.

Current roots are `~/.kimi-code/sessions` or `$KIMI_CODE_HOME/sessions`; literal
relative override paths resolve against the process working directory.
`KIMI_SHARE_DIR`, context.jsonl/checkpoints and old subagents/ belong to archived
Python history. They are intentionally not certified by these fixtures.

The adapter reads canonical message events once, preserving other wire records
as bounded markers. Only native human origins (ordinary input or explicit
user-slash skill/plugin activation) become human history. Tool/model activations
and subagent instructions remain controls. Recorded usage stays in raw markers;
this initial provider does not expose measured usage accounting, file edits or
parent relationships. Bounded discovery may leave a large journal's preview
unknown; hydration reads its full native evidence.

Pinned primary artifacts: [official source](https://github.com/MoonshotAI/kimi-code/tree/472192c34c1efad89a369870eff0da0b708ce9bb) and [official CLI 2.2.0 release](https://github.com/MoonshotAI/kimi-code/releases/tag/%40moonshot-ai%2Fkimi-code%402.2.0).

# Cursor agent-transcript fixtures

These reproduce the record shapes Cursor writes to
`~/.cursor/projects/<encoded-path>/agent-transcripts/<id>/<id>.jsonl`.

**They are authored, not captured.** Cursor is not installed on the machine
these were built on, so no real transcript was available to copy. Every shape
here was reconstructed from public descriptions of real transcript corpora, and
the provenance of each field — verified against a real corpus by a third party,
inferred from a description, or unverified — is recorded in
`docs/session-catalog.md` under "How each adapter works → cursor", together
with the checklist a maintainer with Cursor installed should run to confirm or
correct them.

| File | What it represents | Confidence |
|---|---|---|
| `observed-3.13.25.jsonl` | The shape a 104-transcript corpus of Cursor IDE 3.13.25 was reported to carry: bare `{role, message:{content:[…]}}` records, `text` and **id-less** `tool_use` blocks, a `turn_ended` marker, `<timestamp>`/`<user_query>` framing on human turns, and **no** `tool_result`, `thinking`, `model` or `usage`. | Shapes corroborated by several independent third-party adapters. Not verified here against a real file. |
| `legacy-string-content.jsonl` | The older row shape: `message.content` as a bare string with no framing at all. | Corroborated — this is the shape the pre-existing `parse_cursor_text` was written against and its tests still cover. |
| `extended-unverified.jsonl` | A build that also writes `message.model`, `message.usage`, `thinking` blocks, `tool_use` ids and `tool_result` blocks. | **Unverified.** No source shows Cursor writing these. The fixture exists to prove the parser records them *when present* rather than to claim Cursor writes them. |

`extended-unverified.jsonl` is deliberately named so that nobody reads it as
evidence about Cursor. If a maintainer confirms a real Cursor build writes any
of it, move that record into an `observed-*` fixture and update the capability
matrix; if a real corpus shows it never happens, delete it and the matrix entry
stays "unavailable".

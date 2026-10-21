# Hermes pre-intake transport fixtures

These are captured RelayHistory artifacts, not a hand-written approximation of
its old index. They were produced by RelayHistory commit
`d6211bf47f8fc8f72047b8b720c1840525cbe686`, before the tool-use ID intake
correction, using only the checked-in synthetic Hermes `../../fixtures/hermes/schema31.sql`.
That native fixture was written by NousResearch/hermes-agent
`97a9ae8ee55d31bebce49f1fb6b5ef775323976a` with its schema-31 SessionDB API;
see `plans/history-providers/hermes.md` and the adjacent official export oracle.

- `pre-intake-index.sql`: SQLite schema and stored-row dump after public discovery and
  selected hydration, including the persisted `hermes_schema31_v4` checkpoint,
  native call IDs, change-feed revisions and original ledger schema. Only the
  temporary synthetic home prefix was replaced by
  `/synthetic/hermes-pre-intake-home`; evidence identities and values were not
  edited. Reopening this ledger in the acceptance test does not stage native
  history, force a sync, remove an index, or renormalize its rows.
- `pre-intake-export.json`: actual paginated `ExportSnapshot` output from that
  index, restricted to the parent fixture session: nine session events, one
  tool call and eight markers. Both call/result events retain `call/native:1`.
- `capture.rs`: the historical integration-test probe used to create both
  artifacts. It also submits the actual export to the historical validator and
  requires `INVALID_ARGUMENT: tool_use_id is only valid on a tool_result event`.
  The historical probe passed, confirming that exact rejection.

Reproduce in a disposable checkout of the pinned RelayHistory revision:

```sh
cp /path/to/transport/capture.rs crates/ai-hist/tests/hermes_transport_capture.rs
HERMES_CAPTURE_OUT=/absolute/new/empty/output \
  cargo +1.99.0 test -p ai-hist --features export,unstable-internal \
  --test hermes_transport_capture -- --nocapture
```

Create the output directory first. Run `python3 dump_index.py /absolute/output
pre-intake-index.sql` to dump `legacy-index.db`. The dumper disables foreign-key
checks while restoring alphabetically ordered tables and omits redundant virtual
FTS content INSERTs; the original FTS definitions and shadow index are retained.
It replaces only the output `home` prefix as above. The generated `archive.json`
is copied unchanged. Store epochs, observation
clock timestamps and export revisions may vary between captures; compare native
record identities, payloads and checkpoint generation rather than expecting
byte-identical captures. No authentication, model calls, operator history, or
provider network access is used.

The current public test re-exports the old index and compares every exported
payload with the archived export, then accepts both through a fresh receiver.
It separately tests current native acquisition, remote-first and local-first
ownership, unchanged canonical replay, stale acquisition fencing, and atomic rejection of
conflicting native rewrites of remote-backed evidence. Observation bookkeeping
may advance its own feed revisions on replay; canonical event/tool/marker rows
must remain unchanged.

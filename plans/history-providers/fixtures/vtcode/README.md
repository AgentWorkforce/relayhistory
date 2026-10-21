# VT Code 0.180.2 canonical journal capture

This fixture uses the published `vtcode-memory` and `vtcode-exec-events` 0.180.2 crates. Their tagged source is `a568dbb4b662617dad2b4914a2455cb34c474673`; the event schema constant is `0.18.0`. The checked-in `native-capture/input.json` is authored synthetic event input. It contains no user history, credentials, network activity, or model request. The `events.jsonl` fixture bytes were **written by** `SessionEventLog::append`, not composed by the RelayHistory parser. A separate invocation reopened the native store after writer exit and read it through `SessionEventLog::visit_snapshot`; `native-reader.json` is that independent oracle.

Reproduce in a disposable synthetic workspace:

```sh
cargo +1.99.0 run --manifest-path plans/history-providers/fixtures/vtcode/native-capture/Cargo.toml -- write /tmp/vtcode-capture vt-synthetic-session plans/history-providers/fixtures/vtcode/native-capture/input.json
cargo +1.99.0 run --manifest-path plans/history-providers/fixtures/vtcode/native-capture/Cargo.toml -- read /tmp/vtcode-capture vt-synthetic-session > /tmp/vtcode-reader.json
```

The native reader reported 12 events and two completed turns. Expected public history is two user prompts (`Inspect the synthetic fixture`, `Run the synthetic verification`) and two assistant messages (`I found one safe fixture file.`, `Verification returned fixture.`), with native turn/item IDs. One Bash invocation (`invocation-b`, model tool call `call-b`) has output `output-b`. The two `turn.completed` rows report per-turn usage; `thread.completed` reports the aggregate and is retained as a marker. The compaction boundary is also a marker. This evidence does not establish a file edit, model name, child session identity, or native resume capability.

SHA-256: canonical journal `eeaf60a7645c990ba992c50c01596497958e01d1c471efdce38487f5995e9f35`; native reader oracle `2894f2aeac2ec4bac014ffebe6f2f59f7665c086bae4cc80099378b829c225a4`; synthetic input `a55818f00a146c514e6c64399b5a17da98e6d6364b046d6f2eccbdb9d18cafe7`. Writer manifest timestamps are host-generated, so only the canonical event journal is staged as fixture evidence.

The canonical path is workspace `.vtcode/sessions/<id>/events.jsonl`. `agent.harness.event_log_path` is an optional export sink and `derived/open-responses.jsonl` is a derived projection; neither is scanned. Capture and tests use `ProviderRoots::from_home` with an isolated synthetic workspace. The CLI's `ProviderRoots::from_env` reads only workspaces explicitly listed in `RELAYHISTORY_VTCODE_WORKSPACES`, a RelayHistory variable. No private workspace or home-wide scan is part of this capture.

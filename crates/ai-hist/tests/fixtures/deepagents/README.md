# DeepAgents Code synthetic native capture

Pinned upstream `langchain-ai/deepagents` commit `0324ff31d18171fceddbd3084ff388b7a964c51b`, `deepagents-code` 0.1.88, macOS arm64. `capture.py` uses LangGraph `AsyncSqliteSaver` 3.1.2, the writer that `deepagents_code.sessions.get_checkpointer()` constructs, plus LangChain `HumanMessage`, `AIMessage`, and `ToolMessage`. It creates two repeated full checkpoints and a pending `messages` write on the latest checkpoint. Run:

```
/private/tmp/rh-deepagents-venv/bin/python capture.py
python <pinned-upstream>/libs/code/deepagents_code/built_in_skills/deepagents-thread-inspector/scripts/inspect_sessions.py 019a0000-0000-7000-8000-000000000443 --db sessions.db --mode transcript --max-content 10000
```

`official-443.json`, `official-444.json`, and `official-445.json` is the independently decoded oracle (only its absolute database path is replaced). A fresh interpreter reads six messages: user, assistant tool call, tool result, assistant, later user, later assistant. The first two checkpoints contain duplicate IDs; the latest pending write adds the resumed turn. Thread `444` has no inline messages and ends in a native `Overwrite`, so only its compacted two-message state survives. Thread `445` has no inline messages and applies same-ID replacement plus `RemoveMessage`; its oracle retains the prompt and revised answer. Thread `446` runs the pinned SDK `DeltaChannel` reducer through a deterministic compiled graph for 57 invocations, crossing its 50-step snapshot frequency (171 native checkpoints, two inline snapshots). The compiled graph’s `aget_state` gives `native-446-state.json`: 111 final messages after a post-snapshot same-ID revision and removal. The bundled inspector independently reports exactly those 111 IDs/content in `official-446.json`. The native tool ID is `call-1`. No network, model, authentication, or private history was used.

`history.jsonl` is input recall and never a conversation transcript. This fixture covers root namespace only. The adapter does not claim child namespace, offloaded history, legacy CLI, token usage, file edits, or native resume fidelity absent a separate pinned proof.

The checked-in database is `sessions.sql`, a SQLite dump with hex BLOB literals. The binary `sessions.db` is a local capture artifact and is deliberately excluded. Recreate the fixture through `capture.py`, then regenerate `sessions.sql` with Python `sqlite3.Connection.iterdump()` and delete the binary. The fixture uses `deepagents==0.7.30`, `langgraph-checkpoint-sqlite==3.1.2`, `langgraph-checkpoint==4.3.0`, and `langchain-core==1.6.13`.

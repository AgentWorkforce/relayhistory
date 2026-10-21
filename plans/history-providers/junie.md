# Junie historical acquisition (#451)

Target: JetBrains Junie CLI release **3110.7**, distributed by npm package
`@jetbrains/junie@3110.7.0` on macOS arm64. The package pins the GitHub
release archive `junie-release-3110.7-macos-aarch64.zip`; its
`junie-release-3110.7.jar` has SHA-256
`24f2a09a3beb6e8b7bc6d53e55455b0d7f8e31436548f328abcd505fb7f58b9b`.
The npm tarball has SHA-256
`373d8622d280bdd51a6b329c819a746564a41574ebb7a8fbb7d8f2874c0a9992`.

Native persistence is `SessionStore.storeEvent` in the pinned jar's
`SessionIndex.kt` bytecode. It appends a Kotlin-serialized `SessionEvent` with
top-level `kind` and `timestampMs` to
`$JUNIE_HOME/sessions/<sessionId>/events.jsonl`. `AgentStateUpdatedEvent`
instead atomically replaces `state.json`, so state is distinct from the
message journal. `SessionStore.storeSessionSummary` atomically rewrites
`sessions/index.jsonl`; `SessionStore.readStoredEventsFromFile` is Junie's own
restart reader. These are the writer and independent reader used in the
[fixture procedure](fixtures/junie/README.md). Official docs confirm the
transcript sibling, subagent folder, `/history` resume and `JUNIE_HOME` override:
[quickstart](https://junie.jetbrains.com/docs/junie-cli.html),
[environment variables](https://junie.jetbrains.com/docs/environment-variables.html).

The synthetic native writer capture contains two user prompts with request IDs,
Markdown and result blocks with step IDs, a repeated tool block with native
step ID and completion/output, and a subagent-spawn notice. Two separate JVM
processes append to the same session to prove restart persistence. The pinned
official reader in another process returns all eight journal rows and their
identity/text/recorded timestamps. No model, authentication, network, private
history, or Junie runtime startup participated in the capture.

For this format, `requestId` identifies a user prompt and `stepId` identifies
an A2UX block. The event journal has no separate per-event ID in these rows;
the importer uses a zero-padded stable journal position for the event UID (so
same-millisecond export order remains the native append order) and keeps native
request/step IDs as message/tool IDs. Tool updates with the same `stepId` fold
to a single tool call; their raw updates remain separate markers. The final
tool block's native `output` is projected as a linked tool-result evidence row;
Junie does not store a separate tool-result event in this capture. Billable
usage is not proven.
The subagent notice records a name, task and `parentId`; it does not prove a
child session ID or a relationship. The derived `transcript.md` and child
Markdown transcript are not treated as independent authoritative messages.

Root precedence: `JUNIE_HOME` overrides the default `~/.junie` in a live
process; `ProviderRoots::from_home` isolates a supplied home and ignores the
process environment. Explicit roots are frozen with their namespace before
scanning. Discovery is passive and reads only bounded metadata and event
prefixes. One bounded `index.jsonl` read is shared by the sessions of a root
within a discovery or sweep pass; a new pass reads fresh index metadata. For
a journal beyond the 64 KiB shallow window, fresh index activity and title
are still applied while a saved prompt preview fills any missing excerpt.
If the first row itself exceeds that window, a new catalog entry records the
native directory identity and index summary with no prompt preview; selecting
it for full hydration reads the complete prompt. Existing catalog previews
remain cached until hydration refreshes them. An optional index above 8 MiB
is skipped after a bounded read; the native journal remains discoverable and
hydratable, and selected hydration reports `JUNIE_INDEX_TOO_LARGE` with index
metadata unknown. Malformed or unreadable optional index data likewise yields
a diagnostic while verified journal evidence remains readable. An index path
escaping the declared root remains a hard containment error. A complete native
index title rename refreshes the catalog
during discovery, including a previously fully hydrated row; blank or
unavailable index titles do not clear it. When index metadata is unavailable,
a full local capture at the same journal locator also preserves its verified
project directory for event, history and catalog reconciliation. A remote-only
or discovery-only catalog row does not supply that fact; an explicit native
summary without `projectDir` does not use the fallback.
Unsupported event shapes fail visibly, while unknown evidence is
preserved as bounded markers. A complete journal rewrite retires stale local
evidence; an incomplete final JSONL line defers replacement. No native resume
control, usage accounting, file edits or live execution is claimed.

The native jar is proprietary released bytecode, so source line links are not
available. The checked-in Java harness compiles against the exact released jar
and invokes its public `SessionStore` methods. The post-restart reader oracle
is independent of RelayHistory and can be rerun as described in the fixture
README.

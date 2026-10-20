# Codebuddy Code (#439)

Passive acquisition of the pinned official Tencent CLI 2.171.0 project JSONL
format. Native writer and independent historical reader evidence, isolation,
regeneration and remaining limits are recorded in
[the fixture evidence](fixtures/codebuddy/README.md).

Selected files are only `projects/<project-key>/<native-session-id>.jsonl`.
Discovery uses bounded head/tail reads and path-derived identity; hydration
captures exact immutable bytes. Full sweep and selected hydration share one
normalizer. Partial final records retain previous complete evidence; complete
rewrites retire missing events, and successful partial reads do not prevent
retiring unrelated deleted files. Failed captures prevent uncertain whole-root
retirement. Change stamps use namespace plus exact bytes, not temporary HOME.

Native message IDs and millisecond clocks are preserved. Historical replay
matches native high-level restore/replay: retained pre-compaction messages stay
searchable, while inactive fork messages, summaries and
control facts remain markers. Tool calls/results join on native callId. Full
inline spill content is read without opening embedded pointer paths. Usage is
counted once per native upstream response ID, never per tool-block copy.
Unknown fields remain unknown; unknown control fact types fail closed.

No executable resume, model call, authentication, provider config mutation,
subagent discovery, delegation inference or private operator history is part of
acquisition. Future version/product certification remains independent work.

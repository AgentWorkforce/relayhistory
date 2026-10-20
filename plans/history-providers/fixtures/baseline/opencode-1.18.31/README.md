# OpenCode 1.18.31 offline writer/export baseline

Captured 2026-10-14 on macOS arm64 using the installed 1.18.31 binary; digests
and the pinned public importer source are in `provenance.json`. `input.json` is
entirely authored synthetic user/assistant/tool evidence. The real CLI's
`import --pure` wrote its native database in a new isolated HOME with separate
XDG data/config/cache directories and model fetching/updating disabled. A new
CLI process ran `export --pure ses_baseline11831`, producing
`official-export.json`. No authentication, model request or private history.

`native-store.sql` preserves the native `project`, `session`, `message` and `part` schemas
and rows. Unrelated application tables/indexes were omitted; no transcript
values were invented by RelayHistory. This is an offline importer compatibility probe, not proof
of a live ordinary/tool/resume agent run. Importing synthetic usage proves its
preservation, not that every actual model writes the same counters.

Reproduce with a separately installed pinned binary (no installation by script):

```sh
python3 plans/history-providers/fixtures/baseline/opencode-1.18.31/capture.py \
  --binary /path/to/opencode --out /private/tmp/opencode-baseline-recapture
```

Native timestamps and isolated directory metadata change on recapture; compare
message/part IDs, roles, text, tool IDs and payloads, not whole-file digests.
`provider_baseline` stages the native SQL and compares public evidence with the
independent export, including the tool call. It also runs the shared restart,
repeat sync, change-feed and paged-export assertions. This capture is kept under
research fixtures because it is baseline-owned and does not change the shared
descriptor/corpus registration.

# Muse 1.4.7 independent offline reader

On 2026-10-14/macOS arm64 the installed exporter identified itself as Muse Code
1.4.7 (`d8c53d25bd`). Its documented offline `muse export --session <path>`
read only the checked-in synthetic `muse/cli-capture` log under an isolated HOME
with auto-update disabled. No model call, auth or operator history. This is a
fresh independent reader oracle for that older captured format, **not** a fresh
1.4.7 writer capture. The fixture's original provenance remains unchanged.

`official-export-projection.json` retains every exported human prompt, committed
assistant text and native tool-call record, along with the full export digest
and exporter identity. The projection is selected from official export fields,
not from RelayHistory parser output. `provider_baseline` compares all projected
texts and tool-call IDs through the public evidence API and shared conformance
checks. Rich image/approval/lineage fields remain covered by the original corpus;
this projection does not certify them or manufacture image bytes.

Reproduce without installing or launching an agent:

```sh
python3 plans/history-providers/fixtures/baseline/muse-1.4.7/capture.py \
  --binary /path/to/muse --out /private/tmp/muse-export-projection.json
```

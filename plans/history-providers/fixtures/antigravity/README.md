# Antigravity native transcript captures

These five synthetic cases were written by the native `localharness` bundled with the official `google-antigravity==0.1.22` Python SDK. After closing the writer, a fresh official SDK process restored the conversation; its `conversation.history` is the independent reader oracle. The model responses came from a loopback HTTP fixture server, not a live model. No operator history or credentials were used.

The cases cover ordinary messages, a custom tool, resumed conversation, literal envelope tags, and a response long enough to trigger compact-transcript truncation. `manifest.json` records raw and sanitized transcript hashes. `oracle/` contains the restart-reader output; the Rust corpus reference combines these files.

## Provenance and limits

See `provenance.json` for the package, source revision and native binary pin. The capture script rejects a different package version or native binary. This pin is macOS arm64-specific; a different platform needs its own reviewed provenance.

These are **SDK native-writer captures**, not captures from the standalone CLI or desktop app. The CLI 1.4.3 shipped binary describes the same timed JSONL fields. The documented App, CLI and IDE roots are discovery targets, but direct writer parity for those products remains unverified. Older desktop builds describing an untimed schema are not certified by these fixtures.

The native transcript does not contain the SDK reader's tool-call IDs. Tool calls/results therefore remain raw marker evidence; the importer must not manufacture joins from the oracle. Measured usage, model, cwd and parent relationships are not evidenced by these transcripts.

Sanitation replaces native conversation IDs with fixed 32-character hex IDs, shifts clock values to a fixed origin while retaining offsets and relative deltas, rounds timestamps to whole seconds, and replaces isolated capture paths. It does not reconstruct transcript rows or tool data. Copies are placed under the documented SDK default `.gemini/antigravity/brain` layout; the capture itself uses an explicitly isolated `app_data_dir`. Raw hashes vary between runs because the native writer chooses IDs and clocks.

## Reproduce

Create a new temporary directory and an empty `home` inside it. Install the pinned SDK into a virtual environment there, then run (substitute the paths):

```sh
uv venv /tmp/antigravity-capture/venv
uv pip install --python /tmp/antigravity-capture/venv/bin/python google-antigravity==0.1.22
mkdir /tmp/antigravity-capture/home
env -i HOME=/tmp/antigravity-capture/home PATH=/usr/bin:/bin:/usr/sbin:/sbin TMPDIR=/private/tmp /tmp/antigravity-capture/venv/bin/python plans/history-providers/fixtures/antigravity/capture.py /tmp/antigravity-capture/cases
python3 plans/history-providers/fixtures/antigravity/sanitize.py /tmp/antigravity-capture/cases
node --test plans/history-providers/fixtures/antigravity/validate.test.mjs
```

The capture only enables the synthetic `fixture_echo` custom tool and denies other tools. The local model endpoint is bound to `127.0.0.1`. Output must be a new directory. Review newly captured artifacts and hashes before committing them.

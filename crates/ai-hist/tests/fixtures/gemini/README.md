# Gemini CLI admitted fixtures

Pinned google-gemini/gemini-cli `9b6e0265d16bbd29ca51e33c9e0c01dc4cec5e83`, package `0.65.0-nightly.20261006.gfb972b2f8`, Darwin arm64. All nine case trees are byte-for-byte copies of the synthetic recorder/official-reader capture under `plans/history-providers/fixtures/gemini`; its manifest, source hashes, capture harness, independent reader projection and root oracle are the durable provenance.

Legacy JSON is authored input accepted by the current official reader, not an old-writer capture. Compaction-shaped persistence calls used synthetic histories and made no model request. No private operator history or authentication was accessed.

The provider projects finalized messages and preserves every raw journal revision as a bounded marker. Recorded user role has no trustworthy synthetic/human discriminator, so summaries are not identified by markup. No inferred cwd, relationship, file edits, compaction boundary, native resume or live execution is advertised. See `plans/history-providers/gemini.md` for precise roots, acquisition rules and limits.

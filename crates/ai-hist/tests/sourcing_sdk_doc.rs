//! The per-source population table in `docs/sourcing-sdk.md` is generated
//! from what the crate declares, and this test is what keeps it that way.
//!
//! Like `public_marker_reads.rs`, this file has no `[[test]]` entry in
//! `Cargo.toml`, so it builds on the crate's *default* features: the table is
//! derived through `declared_evidence_kinds` and `source_accounting`, which an
//! embedder can call, and not through anything `unstable-internal` re-exports.
//! A provider that starts or stops declaring a kind moves a cell here and the
//! guide has to move with it, in the same pull request.

use ai_hist::{declared_evidence_kinds, source_accounting, EvidenceKind, Source};
use std::path::Path;

/// Every `Source`, in the order the guide lists them. `Source` is
/// `#[non_exhaustive]`, so a new variant is added here by hand — and the guide
/// then gains a row.
const SOURCES: &[Source] = &[
    Source::Claude,
    Source::Codex,
    Source::Cursor,
    Source::Grok,
    Source::OpenCode,
    Source::Relay,
    Source::Trajectory,
];

/// The evidence kinds a provider adapter can declare, as table columns.
const COLUMNS: &[EvidenceKind] = &[
    EvidenceKind::History,
    EvidenceKind::SessionEvent,
    EvidenceKind::ToolCall,
    EvidenceKind::FileEdit,
    EvidenceKind::Relationship,
];

const START: &str = "<!-- sourcing-sdk-population-table:start -->";
const END: &str = "<!-- sourcing-sdk-population-table:end -->";

fn generated_table() -> String {
    let mut lines = vec![
        format!(
            "| Source | {} | usage accounting |",
            COLUMNS
                .iter()
                .map(|kind| kind.as_str())
                .collect::<Vec<_>>()
                .join(" | ")
        ),
        format!("| --- |{} --- |", " --- |".repeat(COLUMNS.len())),
    ];
    for source in SOURCES {
        let declared = declared_evidence_kinds(source.as_str());
        let cells = COLUMNS
            .iter()
            .map(|kind| {
                if declared.contains(kind) {
                    "✓"
                } else {
                    "—"
                }
            })
            .collect::<Vec<_>>()
            .join(" | ");
        let accounting = source_accounting(source.as_str()).map_or("none", |mode| mode.as_str());
        lines.push(format!("| {} | {cells} | {accounting} |", source.as_str()));
    }
    lines.join("\n")
}

#[test]
fn the_guide_population_table_is_what_the_crate_declares() {
    let guide = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/sourcing-sdk.md");
    let text = std::fs::read_to_string(&guide)
        .unwrap_or_else(|error| panic!("reading {}: {error}", guide.display()));
    let start = text
        .find(START)
        .unwrap_or_else(|| panic!("{} has no {START} marker", guide.display()));
    let end = text[start..]
        .find(END)
        .map(|offset| start + offset)
        .unwrap_or_else(|| panic!("{} has no {END} marker", guide.display()));
    let documented = text[start + START.len()..end].trim();
    let generated = generated_table();
    assert_eq!(
        documented, generated,
        "docs/sourcing-sdk.md's population table differs from what the crate declares; \
         replace the block between the markers with:\n\n{generated}\n"
    );
}

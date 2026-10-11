//! Exercise the provider-owned conformance helper on an existing real format.
//! New adapters import `mod provider_support` in their own integration test.
mod provider_support;

use ai_hist::{Role, Source};
use provider_support::{assert_history_conformance, HistoryCase};

#[test]
fn public_history_lifecycle_preserves_the_fixture() {
    let home = tempfile::tempdir().unwrap();
    let target = home
        .path()
        .join(".claude/projects/conformance/simple-turn.jsonl");
    std::fs::create_dir_all(target.parent().unwrap()).unwrap();
    std::fs::copy(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/claude/simple-turn.jsonl"),
        target,
    )
    .unwrap();
    assert_history_conformance(
        home.path(),
        HistoryCase {
            source: Source::Claude,
            session_id: "11111111-1111-1111-1111-111111111111",
            messages: &[(Role::User, "hello"), (Role::Assistant, "Hello!")],
        },
    );
}

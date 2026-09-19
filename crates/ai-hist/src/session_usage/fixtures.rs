//! End-to-end usage checks against real provider transcripts.
//!
//! These run the production parsers over the same fixtures burn reads, so a
//! change to either the parser or the normalizer that quietly alters what a
//! session cost fails here rather than in a cost report. Unit tests elsewhere
//! in this module seed `session_events` directly; these do not, because the
//! grouping being tested is exactly what the parser's row layout provokes.
use crate::ingest::{ingest_claude_transcript, ingest_codex_rollout, read_codex_session_meta};
use crate::session_usage::{
    session_requests_page, session_usage_summary, RequestKeySource, UsageDiagnostic,
};
use crate::store::init_db;
use crate::usage::UsageAccounting;
use rusqlite::Connection;
use std::path::PathBuf;

fn fixture(relative: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(relative)
}

fn claude_store(relative: &str) -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    init_db(&conn).unwrap();
    ingest_claude_transcript(&conn, &fixture(relative)).unwrap();
    conn
}

/// The same transcript on a store that has the raw-facts `request_id` column.
///
/// The column and its parser live in the sibling raw-facts change; this only
/// needs a store shaped like one that has it, so the grouping this module
/// owns can be proven now rather than after that lands. `init_db` is called
/// again after the `ALTER` because the view is derived from the column set
/// and has to be rebuilt when it changes — which is the behaviour under test
/// as much as the grouping itself.
fn claude_store_with_request_ids(relative: &str, request_id: &str) -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    init_db(&conn).unwrap();
    conn.execute_batch("ALTER TABLE session_events ADD COLUMN request_id TEXT")
        .unwrap();
    init_db(&conn).unwrap();
    ingest_claude_transcript(&conn, &fixture(relative)).unwrap();
    conn.execute(
        "UPDATE session_events SET request_id = ?1 WHERE role = 'assistant'",
        [request_id],
    )
    .unwrap();
    conn
}

fn codex_store(relative: &str) -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    init_db(&conn).unwrap();
    let path = fixture(relative);
    let meta = read_codex_session_meta(&path).unwrap().unwrap();
    ingest_codex_rollout(&conn, &path, &meta).unwrap();
    conn
}

const CLAUDE_SESSION: &str = "22222222-2222-2222-2222-222222222222";

fn assistant_rows_with_usage(conn: &Connection) -> i64 {
    conn.query_row(
        "SELECT COUNT(*) FROM session_events \
         WHERE source='claude' AND role='assistant' AND token_json IS NOT NULL",
        [],
        |row| row.get(0),
    )
    .unwrap()
}

/// The acceptance criterion: one `session_requests` row per `requestId`, and
/// a summary that equals what burn reads off `TurnRecord.usage` for the same
/// fixture — input 3, output 43, cache read 11496, cache create 4773, all of
/// it in the 1h bucket.
#[test]
fn a_multi_block_turn_is_one_request_per_request_id() {
    let conn = claude_store_with_request_ids("claude/multi-block-turn.jsonl", "req_1");
    // The fixture's opening `thinking` block is empty, so the parser stores
    // no row for it; the remaining three records each carry a full copy of
    // the one request's usage.
    assert_eq!(
        assistant_rows_with_usage(&conn),
        3,
        "the fixture really does copy one request's usage onto every record"
    );

    let page = session_requests_page(&conn, "claude", CLAUDE_SESSION, 50, None).unwrap();
    assert_eq!(page.requests.len(), 1);
    let request = &page.requests[0];
    assert_eq!(request.request_key, "req_1");
    assert_eq!(request.request_key_source, RequestKeySource::RequestId);
    assert_eq!(request.model.as_deref(), Some("claude-opus-4-7"));
    assert_eq!(request.message_ids.len(), 3);
    assert_eq!(request.event_count, 3);
    assert!(request.diagnostics.is_empty());
    assert_eq!(
        request.tool_use_ids,
        vec!["toolu_bash_1".to_string(), "toolu_agent_1".to_string()]
    );

    let summary = session_usage_summary(&conn, "claude", CLAUDE_SESSION)
        .unwrap()
        .unwrap();
    assert_eq!(summary.request_count, 1);
    assert_eq!(summary.input_tokens, 3);
    assert_eq!(summary.output_tokens, 43);
    assert_eq!(summary.cache_read_tokens, 11496);
    assert_eq!(summary.cache_write_tokens, 4773);
    assert_eq!(summary.cache_write_5m_tokens, Some(0));
    assert_eq!(summary.cache_write_1h_tokens, Some(4773));
    assert_eq!(summary.accounting, vec![UsageAccounting::PerMessage]);
    assert!(summary.coverage.is_complete());
    assert!(summary.diagnostics.is_empty());
    assert!(!summary.overflowed);
}

/// Characterization of the store as it is *before* the raw-facts column
/// lands. `session_events.message_id` holds Claude's per-record `uuid`, not
/// `message.id`, so a turn split across records has no shared key to group
/// on and each record reads as its own request — an over-count of one API
/// call by the number of records it was split across.
///
/// This is recorded rather than hidden because it is the live defect the
/// grouping above removes, and because a reader who sees several requests on
/// a pre-migration store should find the reason here instead of concluding
/// the view is broken.
#[test]
fn without_a_request_id_a_multi_block_turn_reads_as_one_request_per_record() {
    let conn = claude_store("claude/multi-block-turn.jsonl");
    let page = session_requests_page(&conn, "claude", CLAUDE_SESSION, 50, None).unwrap();
    assert_eq!(page.requests.len(), 3);
    assert!(page
        .requests
        .iter()
        .all(|request| request.request_key_source == RequestKeySource::MessageId));
    let summary = session_usage_summary(&conn, "claude", CLAUDE_SESSION)
        .unwrap()
        .unwrap();
    assert_eq!(summary.output_tokens, 43 * 3);
}

/// A transcript that reports input but no output is missing evidence, not a
/// zero-output session. The number is `0`; the coverage flag is what says so.
#[test]
fn missing_output_tokens_reports_zero_with_a_coverage_note() {
    let conn = claude_store("claude/missing-output-tokens.jsonl");
    let summary = session_usage_summary(&conn, "claude", CLAUDE_SESSION)
        .unwrap()
        .unwrap();
    assert_eq!(summary.request_count, 1);
    assert_eq!(summary.input_tokens, 10);
    assert_eq!(summary.output_tokens, 0);
    assert!(summary.coverage.has_input_tokens);
    assert!(!summary.coverage.has_output_tokens);
    assert!(!summary.coverage.is_complete());
}

/// Codex reports cumulative snapshots; the parser differences them into
/// per-request deltas. Those deltas must still add back up to the provider's
/// final cumulative figures, across a compaction boundary.
#[test]
fn codex_compaction_deltas_sum_to_the_final_cumulative_total() {
    let conn = codex_store("codex/compaction.jsonl");
    let summary = session_usage_summary(&conn, "codex", "sess_codex_compact")
        .unwrap()
        .unwrap();
    assert_eq!(summary.request_count, 2);
    assert_eq!(summary.accounting, vec![UsageAccounting::CumulativeDelta]);
    // Final snapshot: input 6500 (of which 1500 cached), output 450,
    // reasoning 90, total 6950. Normalized input excludes cache reads, so the
    // two have to be added back together to meet the provider's figure.
    assert_eq!(summary.cache_read_tokens, 1500);
    assert_eq!(summary.input_tokens + summary.cache_read_tokens, 6500);
    assert_eq!(summary.output_tokens, 450);
    assert_eq!(summary.reasoning_tokens, Some(90));
    assert_eq!(summary.provider_total_tokens, Some(6950));
    assert!(summary.diagnostics.is_empty());
}

/// A snapshot whose cached counter climbs faster than its input produces a
/// delta with more cache reads than input. Making input cache-exclusive would
/// go negative, so it is refused and reported instead.
#[test]
fn a_regressed_codex_counter_is_a_diagnostic_not_a_negative_delta() {
    let conn = codex_store("codex/counter-regressed.jsonl");
    let page = session_requests_page(&conn, "codex", "sess_codex_regressed", 50, None).unwrap();
    assert_eq!(page.requests.len(), 2);
    let regressed = page
        .requests
        .iter()
        .find(|request| request.usage.is_none())
        .expect("the second delta cannot be normalized");
    assert_eq!(
        regressed.usage_error.as_deref(),
        Some("USAGE_COUNTER_REGRESSED")
    );
    assert_eq!(
        regressed.diagnostics,
        vec![UsageDiagnostic::UnnormalizableUsage]
    );

    // The good request still counts, and the summary carries the refusal
    // rather than a total that quietly omits it.
    let summary = session_usage_summary(&conn, "codex", "sess_codex_regressed")
        .unwrap()
        .unwrap();
    assert_eq!(summary.request_count, 1);
    assert_eq!(summary.total_request_count, 2);
    assert_eq!(summary.input_tokens, 2000);
    assert_eq!(
        summary.diagnostics,
        vec![UsageDiagnostic::UnnormalizableUsage]
    );
}

/// Prompt attribution is a faithful move of the commercial outbox's rule,
/// including its refusal: the multi-block fixture's ancestry runs through the
/// empty `thinking` record, which the parser stores nothing for, so every
/// response in the turn has a broken chain to its prompt and none of them is
/// charged. A missing number, not a plausible wrong one.
#[test]
fn prompt_attribution_refuses_a_response_whose_ancestry_is_broken() {
    let conn = claude_store("claude/multi-block-turn.jsonl");
    let events = crate::store::session_events(&conn, CLAUDE_SESSION, Some("claude")).unwrap();
    assert!(crate::usage::attribute_usage_to_prompts(&events, "claude").is_empty());
}

/// And attributes normally when the chain is intact.
#[test]
fn prompt_attribution_charges_an_intact_turn_to_its_prompt() {
    let conn = claude_store("claude/missing-output-tokens.jsonl");
    let events = crate::store::session_events(&conn, CLAUDE_SESSION, Some("claude")).unwrap();
    let attributed = crate::usage::attribute_usage_to_prompts(&events, "claude");
    assert_eq!(attributed.len(), 1);
    let ((_, prompt), usage) = attributed.iter().next().unwrap();
    assert_eq!(prompt, "hello");
    assert_eq!(usage.input_tokens, 10);
    assert_eq!(usage.output_tokens, 0);
    assert!(!usage.coverage.has_output_tokens);
}

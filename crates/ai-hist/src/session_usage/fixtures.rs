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

fn codex_store(relative: &str) -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    init_db(&conn).unwrap();
    let path = fixture(relative);
    let meta = read_codex_session_meta(&path).unwrap().unwrap();
    ingest_codex_rollout(&conn, &path, &meta).unwrap();
    conn
}

const CLAUDE_SESSION: &str = "22222222-2222-2222-2222-222222222222";

/// Every assistant turn's text and whatever measurement landed on it.
fn assistant_usage(conn: &Connection) -> Vec<(String, Option<String>)> {
    conn.prepare(
        "SELECT text, token_json FROM session_events \
         WHERE source='codex' AND role='assistant' AND kind='text' ORDER BY ts_ms",
    )
    .unwrap()
    .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
    .unwrap()
    .collect::<std::result::Result<_, _>>()
    .unwrap()
}

fn usage_of(stored: &[(String, Option<String>)], text: &str) -> Option<String> {
    stored
        .iter()
        .find(|(stored_text, _)| stored_text == text)
        .unwrap_or_else(|| panic!("no turn {text}"))
        .1
        .clone()
}

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
    let conn = claude_store("claude/multi-block-turn.jsonl");
    // The fixture's opening `thinking` block is empty, so the parser stores
    // no row for it; the remaining three records each carry a full copy of
    // the one request's usage.
    assert_eq!(
        assistant_rows_with_usage(&conn),
        3,
        "the fixture really does copy one request's usage onto every record"
    );
    let raw_row_sum: i64 = conn
        .query_row(
            "SELECT SUM(json_extract(token_json, '$.output_tokens')) FROM session_events",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(raw_row_sum, 129, "summing rows would triple the request");

    let page = session_requests_page(&conn, "claude", CLAUDE_SESSION, 50, None).unwrap();
    assert_eq!(page.requests.len(), 1);
    let request = &page.requests[0];
    assert_eq!(request.request_key, "request-id:req_1");
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
    let usage = summary.usage.as_ref().unwrap();
    assert_eq!(usage.input_tokens, 3);
    assert_eq!(usage.output_tokens, 43);
    assert_eq!(usage.cache_read_tokens, 11496);
    assert_eq!(usage.cache_write_tokens, 4773);
    assert_eq!(usage.cache_write_5m_tokens, Some(0));
    assert_eq!(usage.cache_write_1h_tokens, Some(4773));
    assert_eq!(summary.accounting, vec![UsageAccounting::PerMessage]);
    assert!(usage.coverage.is_complete());
    assert!(summary.diagnostics.is_empty());
    assert!(!summary.overflowed);
}

/// Two requests whose ids differ only in whitespace are two requests.
///
/// Trimming on the way in would make `"req_pad"` and `" req_pad "` the same
/// grouping key, merging both requests into one row and reporting a total
/// that belongs to neither.
#[test]
fn request_ids_differing_only_in_whitespace_stay_distinct() {
    let conn = claude_store("claude/padded-request-id.jsonl");
    let page = session_requests_page(&conn, "claude", CLAUDE_SESSION, 50, None).unwrap();
    let mut keys: Vec<&str> = page
        .requests
        .iter()
        .map(|request| request.request_key.as_str())
        .collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        vec!["request-id: req_pad ", "request-id:req_pad"],
        "the qualified keys stay distinct, padding and all"
    );
    assert!(page
        .requests
        .iter()
        .all(|request| request.request_key_source == RequestKeySource::RequestId));

    // Each kept its own usage rather than one row carrying the pair's sum.
    for request in &page.requests {
        assert_eq!(request.usage.as_ref().unwrap().output_tokens, 7);
    }
    let summary = session_usage_summary(&conn, "claude", CLAUDE_SESSION)
        .unwrap()
        .unwrap();
    assert_eq!(summary.request_count, 2);
    assert_eq!(summary.usage.as_ref().unwrap().output_tokens, 14);
}

/// A transcript old enough to carry no `requestId` still groups correctly on
/// the provider's `message.id`, which is the same for every record of one
/// request.
#[test]
fn a_turn_without_a_request_id_groups_on_the_provider_message_id() {
    let conn = claude_store("claude/multi-block-turn-no-request-id.jsonl");
    let page = session_requests_page(&conn, "claude", CLAUDE_SESSION, 50, None).unwrap();
    assert_eq!(page.requests.len(), 1);
    assert_eq!(
        page.requests[0].request_key,
        "provider-message-id:msg_multi_1"
    );
    assert_eq!(
        page.requests[0].request_key_source,
        RequestKeySource::ProviderMessageId
    );
    assert!(page.requests[0].diagnostics.is_empty());
    let summary = session_usage_summary(&conn, "claude", CLAUDE_SESSION)
        .unwrap()
        .unwrap();
    assert_eq!(summary.usage.as_ref().unwrap().output_tokens, 43);
}

/// A store written before request identities were captured: the rows are
/// still there, keyed only on the record id, and the rollup says so instead
/// of adding them into a total that would be one per content block.
///
/// This is what stops an upgraded database from answering with a multiplied
/// figure between the migration and the re-parse that fills the columns in.
#[test]
fn records_with_no_captured_identity_are_flagged_and_not_summed() {
    let conn = claude_store("claude/multi-block-turn.jsonl");
    // Exactly what an existing row looks like after the column migration but
    // before its transcript is re-parsed.
    conn.execute(
        "UPDATE session_events SET request_id = NULL, provider_message_id = NULL",
        [],
    )
    .unwrap();

    let page = session_requests_page(&conn, "claude", CLAUDE_SESSION, 50, None).unwrap();
    assert_eq!(page.requests.len(), 3, "one row per record, as stored");
    for request in &page.requests {
        assert_eq!(request.request_key_source, RequestKeySource::RecordId);
        assert_eq!(
            request.diagnostics,
            vec![UsageDiagnostic::UnresolvedRequestIdentity]
        );
        // The row still reports what its own record said.
        assert_eq!(request.usage.as_ref().unwrap().output_tokens, 43);
    }

    let summary = session_usage_summary(&conn, "claude", CLAUDE_SESSION)
        .unwrap()
        .unwrap();
    assert_eq!(summary.total_request_count, 3);
    assert_eq!(
        summary.usage, None,
        "3 x 43 output tokens is not this session's total"
    );
    assert!(summary
        .diagnostics
        .contains(&UsageDiagnostic::UnresolvedRequestIdentity));
    assert_eq!(summary.models, vec!["claude-opus-4-7".to_string()]);
}

/// Codex attaches one delta to one event, so a key built from the record id
/// is already one per request and must not be flagged.
#[test]
fn codex_record_keys_are_request_keys_and_are_not_flagged() {
    let conn = codex_store("codex/compaction.jsonl");
    let page = session_requests_page(&conn, "codex", "sess_codex_compact", 50, None).unwrap();
    assert_eq!(page.requests.len(), 2);
    for request in &page.requests {
        assert_eq!(request.request_key_source, RequestKeySource::RecordId);
        assert!(request.diagnostics.is_empty());
    }
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
    let usage = summary.usage.as_ref().unwrap();
    assert_eq!(usage.input_tokens, 10);
    assert_eq!(usage.output_tokens, 0);
    assert!(usage.coverage.has_input_tokens);
    assert!(!usage.coverage.has_output_tokens);
    assert!(!usage.coverage.is_complete());
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
    let usage = summary.usage.as_ref().unwrap();
    // Final snapshot: input 6500 (of which 1500 cached), output 450,
    // reasoning 90, total 6950. Normalized input excludes cache reads, so the
    // two have to be added back together to meet the provider's figure.
    assert_eq!(usage.cache_read_tokens, 1500);
    assert_eq!(usage.input_tokens + usage.cache_read_tokens, 6500);
    assert_eq!(usage.output_tokens, 450);
    assert_eq!(usage.reasoning_tokens, Some(90));
    assert_eq!(usage.provider_total_tokens, Some(6950));
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
    assert_eq!(summary.usage.as_ref().unwrap().input_tokens, 2000);
    assert_eq!(
        summary.diagnostics,
        vec![UsageDiagnostic::UnnormalizableUsage]
    );
}

/// A Codex snapshot whose counter is not a non-negative integer cannot be
/// differenced. It must reach the session API as an explicit refusal, not as
/// a delta of zeros that is indistinguishable from a reported zero.
///
/// The three shapes the old `as_i64().unwrap_or(0)` read silently wrong:
/// negative, fractional, and above `i64::MAX`. The first two are corruption;
/// the third is a perfectly good `u64` the old code turned into a zero, so it
/// is preserved here and refused later, at the boundary that actually cannot
/// carry it.
#[test]
fn an_invalid_codex_counter_is_refused_rather_than_read_as_zero() {
    for (fixture, session, field) in [
        (
            "codex/counter-negative.jsonl",
            "sess_codex_negative",
            "input_tokens",
        ),
        (
            "codex/counter-fractional.jsonl",
            "sess_codex_fractional",
            "output_tokens",
        ),
    ] {
        let conn = codex_store(fixture);
        let stored: String = conn
            .query_row(
                "SELECT token_json FROM session_events \
                 WHERE source='codex' AND token_json IS NOT NULL",
                [],
                |row| row.get(0),
            )
            .unwrap_or_else(|error| panic!("{fixture}: {error}"));
        assert!(
            stored.contains(field),
            "{fixture}: the provider's own object is kept, not a zeroed delta: {stored}"
        );

        let page = session_requests_page(&conn, "codex", session, 50, None).unwrap();
        let refused = page
            .requests
            .iter()
            .find(|request| request.usage.is_none())
            .unwrap_or_else(|| panic!("{fixture}: no request refused its usage"));
        assert_eq!(
            refused.usage_error.as_deref(),
            Some("USAGE_NON_INTEGER_COUNTER"),
            "{fixture}"
        );
        assert!(refused
            .diagnostics
            .contains(&UsageDiagnostic::UnnormalizableUsage));

        let summary = session_usage_summary(&conn, "codex", session)
            .unwrap()
            .unwrap();
        assert_eq!(summary.usage, None, "{fixture}");
        assert!(summary
            .diagnostics
            .contains(&UsageDiagnostic::UnnormalizableUsage));
    }
}

/// A snapshot that cannot be differenced is a transient glitch, exactly like
/// a regressed one — and must be treated like one.
///
/// Refusing on the spot consumed the assistant event that was waiting for its
/// measurement, so the next *valid* snapshot had nowhere to land: the turn
/// was marked unreadable and its real delta went to a different request, or
/// nowhere at all. The provider did report that turn's usage; it just said
/// something unreadable first.
///
/// Because the baseline is deliberately left untouched on a bad snapshot, the
/// next advancing snapshot already covers the whole span, so the number is
/// recoverable and the turn must get it.
#[test]
fn a_bad_snapshot_followed_by_a_good_one_still_measures_the_waiting_turn() {
    let conn = codex_store("codex/counter-recovers.jsonl");
    let page = session_requests_page(&conn, "codex", "sess_codex_recovers", 50, None).unwrap();
    assert_eq!(page.requests.len(), 1, "one turn, one request");
    let request = &page.requests[0];
    assert!(
        request.diagnostics.is_empty(),
        "the glitch was superseded, so nothing is left to report: {:?}",
        request.diagnostics
    );
    let usage = request
        .usage
        .as_ref()
        .expect("the later valid snapshot measures this turn");
    assert_eq!(usage.output_tokens, 200);
    assert_eq!(usage.input_tokens, 2000);
    assert_eq!(usage.cache_read_tokens, 1000);

    let summary = session_usage_summary(&conn, "codex", "sess_codex_recovers")
        .unwrap()
        .unwrap();
    assert_eq!(summary.usage.as_ref().unwrap().output_tokens, 200);
    assert!(summary.diagnostics.is_empty());
}

/// An unreadable snapshot never advances the baseline, so the next readable
/// one measures from the point *before* it: its delta already covers the span
/// the glitch failed to measure. Nothing was lost, and no earlier turn is owed
/// a refusal — its spend is reported inside the recovering turn's request.
/// Marking it rejected would add an unreadable request to a session that was
/// in fact measured end to end.
#[test]
fn a_recovering_delta_supersedes_the_refusals_its_span_covers() {
    let conn = codex_store("codex/counter-unusable-then-new-turn.jsonl");
    let stored = assistant_usage(&conn);
    assert_eq!(stored.len(), 2, "two turns");
    assert_eq!(
        stored
            .iter()
            .filter(|(_, usage)| usage.as_deref().is_some_and(|u| u.contains("-1")))
            .count(),
        0,
        "no turn is marked unreadable: {stored:?}"
    );
    let measured = usage_of(&stored, "Second answer.").expect("the second turn was measured");
    assert_eq!(
        crate::usage::normalize_usage_str("codex", &measured)
            .unwrap()
            .unwrap()
            .output_tokens,
        140,
        "and its delta covers the whole span, the glitch included"
    );
    let summary = session_usage_summary(&conn, "codex", "sess_codex_wrong_turn")
        .unwrap()
        .unwrap();
    assert!(
        !summary
            .diagnostics
            .contains(&UsageDiagnostic::UnnormalizableUsage),
        "the session was measured, so it reports no refusal: {:?}",
        summary.diagnostics
    );
    assert_eq!(summary.usage.as_ref().unwrap().output_tokens, 140);
}

/// The same rule seen from the row that `agent_reasoning` occupies. Reasoning
/// takes the waiting slot, so a glitch while it holds it named *it*; the
/// recovering delta then landed on the turn's `agent_message` and the thinking
/// row was left carrying a refusal for a turn that had been measured — an
/// extra unreadable request in a session with nothing wrong with it.
#[test]
fn a_glitch_while_reasoning_is_waiting_does_not_outlive_the_recovery() {
    let conn = codex_store("codex/reasoning-then-unreadable.jsonl");
    let unreadable: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM session_events \
             WHERE source='codex' AND token_json LIKE '%-1%'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(unreadable, 0, "no row keeps the superseded glitch");
    let summary = session_usage_summary(&conn, "codex", "sess_codex_reasoning_bad")
        .unwrap()
        .unwrap();
    assert!(
        !summary
            .diagnostics
            .contains(&UsageDiagnostic::UnnormalizableUsage),
        "{:?}",
        summary.diagnostics
    );
    let usage = summary.usage.as_ref().expect("the session reports a total");
    assert_eq!((usage.input_tokens, usage.output_tokens), (400, 140));
}

/// "A measured delta supersedes every held refusal" rests on an unreadable
/// snapshot never moving the baseline — and that is false across a baseline
/// *reinstall*. When the opening snapshot is unreadable, the next readable one
/// installs a baseline and measures nothing, silently absorbing everything
/// spent up to that point, including a turn that was refused in between. A
/// later advancing snapshot is then differenced from *that* baseline, so its
/// delta does not cover the refused turn at all — yet it was clearing the
/// refusal, leaving that turn looking unused rather than rejected and its
/// spend nowhere.
#[test]
fn a_refusal_predating_a_baseline_reinstall_is_not_cleared_by_it() {
    let conn = codex_store("codex/refusal-across-baseline-reinstall.jsonl");
    let stored = assistant_usage(&conn);
    assert_eq!(stored.len(), 2, "two turns");

    let first = usage_of(&stored, "First answer.")
        .expect("the refused turn keeps its refusal across the reinstall");
    assert!(
        first.contains("55.5"),
        "and it is that turn's own unreadable object: {first}"
    );
    let second = usage_of(&stored, "Second answer.").expect("the second turn was measured");
    let delta = crate::usage::normalize_usage_str("codex", &second)
        .unwrap()
        .unwrap();
    assert_eq!(
        (delta.input_tokens, delta.output_tokens),
        (400, 140),
        "measured from the installed baseline, not from zero: {second}"
    );

    // The reader sees one refused request and one measured one, rather than a
    // session that reports a total as though nothing had been rejected.
    let page = session_requests_page(&conn, "codex", "sess_codex_reinstall", 50, None).unwrap();
    assert_eq!(
        page.requests
            .iter()
            .filter(|request| request
                .diagnostics
                .contains(&UsageDiagnostic::UnnormalizableUsage))
            .count(),
        1,
        "the refusal is reported: {:?}",
        page.requests
    );
}

/// A turn can be refused twice, once on either side of a baseline reinstall,
/// and the two refusals are owed against *different* baselines. Holding one
/// slot per turn meant the second overwrote the first and inherited the newer
/// generation, so the next measured delta — which covers only that newer span
/// — cleared a refusal that in truth predated the reinstall. The turn ended
/// with no measurement and no refusal again, by a different route.
#[test]
fn a_second_refusal_does_not_erase_one_owed_against_an_older_baseline() {
    let conn = codex_store("codex/refusal-overwritten-after-reinstall.jsonl");
    let stored = assistant_usage(&conn);
    assert_eq!(stored.len(), 2, "two turns");

    let first = usage_of(&stored, "First answer.")
        .expect("the refused turn is not silenced by its own second refusal");
    assert!(
        first.contains("55.5"),
        "it keeps the first thing that went wrong for it, which predates the \
         reinstall and is what the later delta cannot account for: {first}"
    );
    assert!(
        !first.contains("-7"),
        "not the later one, whose span the delta does cover: {first}"
    );
    let second = usage_of(&stored, "Second answer.").expect("the second turn was measured");
    let delta = crate::usage::normalize_usage_str("codex", &second)
        .unwrap()
        .unwrap();
    assert_eq!(
        (delta.input_tokens, delta.output_tokens),
        (400, 140),
        "measured from the installed baseline: {second}"
    );
}

/// A resumed rollout opens with the cumulative total it carried over. If that
/// snapshot is unreadable there is no baseline, and differencing the next good
/// one against zero charges the whole carried-over history to a single
/// request. The baseline is unknown, so the next readable snapshot installs it
/// without emitting a delta — the same treatment a first-ever snapshot gets.
#[test]
fn an_unreadable_resume_baseline_does_not_become_a_delta() {
    let conn = codex_store("codex/resume-baseline-corrupt.jsonl");
    let deltas: Vec<String> = conn
        .prepare(
            "SELECT token_json FROM session_events \
             WHERE source='codex' AND token_json IS NOT NULL",
        )
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<std::result::Result<_, _>>()
        .unwrap();
    for delta in &deltas {
        let Some(usage) = crate::usage::normalize_usage_str("codex", delta).unwrap() else {
            continue;
        };
        assert_ne!(
            usage.input_tokens, 50_000,
            "the resumed session's carried-over total is not one request's delta: {delta}"
        );
        assert_ne!(usage.output_tokens, 9_100, "likewise for output: {delta}");
    }
    let summary = session_usage_summary(&conn, "codex", "sess_codex_resume_corrupt")
        .unwrap()
        .unwrap();
    // The turn is reported, and reported as unmeasured, rather than given a
    // number the evidence does not support.
    assert_eq!(summary.total_request_count, 1, "the turn is still reported");
    assert!(
        summary.usage.is_none(),
        "nothing establishes what this turn cost: {:?}",
        summary.usage
    );
}

/// A counter above `i64::MAX` is still a valid `u64`, so the parser keeps it
/// rather than zeroing it — and the JavaScript boundary is where it is
/// refused, because that is the layer that genuinely cannot carry it.
#[test]
fn a_codex_counter_above_i64_max_survives_the_parser_intact() {
    let conn = codex_store("codex/counter-above-i64.jsonl");
    let summary = session_usage_summary(&conn, "codex", "sess_codex_huge")
        .unwrap()
        .unwrap();
    let usage = summary
        .usage
        .as_ref()
        .expect("a large but valid counter is still usage");
    // 9223372036854775808 == i64::MAX + 1, reported as ordinary input because
    // the snapshot reported no cache reads.
    assert_eq!(usage.input_tokens, 9_223_372_036_854_775_808);
    assert!(summary.diagnostics.is_empty());
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

/// Claude writes one API request as several records with distinct uuids that
/// share a `requestId`, and copies the whole `message.usage` onto each of
/// them. Attribution resolves ownership per record, because that is where the
/// parent links are, but it must add the measurement **once per request** -
/// folding the copies charged the prompt its own cost multiplied by the
/// request's record count.
#[test]
fn a_request_split_across_records_is_charged_to_its_prompt_once() {
    let conn = claude_store("claude/multi-record-request.jsonl");
    let events = crate::store::session_events(&conn, CLAUDE_SESSION, Some("claude")).unwrap();
    // Two stored records, one request, each carrying a full copy of its usage.
    assert_eq!(assistant_rows_with_usage(&conn), 2);
    let attributed = crate::usage::attribute_usage_to_prompts(&events, "claude");
    assert_eq!(attributed.len(), 1);
    let ((_, prompt), usage) = attributed.iter().next().unwrap();
    assert_eq!(prompt, "count once");
    assert_eq!(
        (usage.input_tokens, usage.output_tokens),
        (7, 40),
        "one request's usage, not one copy per record"
    );
    // And the request view agrees, which is the point of sharing the rule.
    let summary = session_usage_summary(&conn, "claude", CLAUDE_SESSION)
        .unwrap()
        .unwrap();
    assert_eq!(summary.request_count, 1);
    let totals = summary.usage.as_ref().expect("one readable request");
    assert_eq!((totals.input_tokens, totals.output_tokens), (7, 40));
}

/// A request is charged only when every record of it that says anything about
/// the measurement agrees. One record carrying an unreadable copy of
/// `message.usage` means the request's cost is in dispute, and a sibling whose
/// copy happens to parse is not the tie-breaker — charging it would report one
/// of two contradicting readings as the measurement.
#[test]
fn a_request_with_one_unreadable_copy_charges_nothing() {
    let conn = claude_store("claude/multi-record-bad-copy.jsonl");
    let events = crate::store::session_events(&conn, CLAUDE_SESSION, Some("claude")).unwrap();
    assert_eq!(
        crate::usage::attribute_usage_to_prompts(&events, "claude"),
        std::collections::HashMap::new(),
        "the copies disagree, so nothing is established"
    );
}

/// But a record whose *ancestry* is broken says nothing about the cost and
/// nothing about the owner — it is silence, not contradiction. Claude chains
/// a request's records through each other and the parser stores no row for an
/// empty block, so a mid-chain gap is ordinary, and refusing the whole request
/// over it would drop measurements that a sibling establishes outright.
#[test]
fn a_broken_sibling_does_not_cancel_a_request_its_siblings_establish() {
    let conn = claude_store("claude/multi-record-broken-sibling.jsonl");
    let events = crate::store::session_events(&conn, CLAUDE_SESSION, Some("claude")).unwrap();
    let attributed = crate::usage::attribute_usage_to_prompts(&events, "claude");
    assert_eq!(attributed.len(), 1);
    let ((_, prompt), usage) = attributed.iter().next().unwrap();
    assert_eq!(prompt, "one link missing");
    assert_eq!(
        (usage.input_tokens, usage.output_tokens),
        (7, 40),
        "once, from the record that does establish it"
    );
}

/// When nothing ever recovers, each unmeasured turn keeps **its own**
/// refusal. Holding a single one meant the second overwrote the first, so the
/// first ended with a null measurement — which reads as evidence that was
/// never reported rather than evidence that was rejected — and the reader was
/// shown one refusal where there were two.
#[test]
fn two_unrecovered_turns_each_keep_their_own_refusal() {
    let conn = codex_store("codex/two-unreadable-turns.jsonl");
    let stored = assistant_usage(&conn);
    assert_eq!(stored.len(), 2, "two turns");
    let first = usage_of(&stored, "First answer.").expect("the first turn is not silent");
    assert!(
        first.contains("-1"),
        "the first turn keeps its own unreadable object: {first}"
    );
    let second = usage_of(&stored, "Second answer.").expect("nor is the second");
    assert!(
        second.contains("90.5"),
        "and the second keeps the one that belongs to it: {second}"
    );

    // Both are visible to the reader, rather than one turn looking as though
    // nothing was ever reported for it.
    let page = session_requests_page(&conn, "codex", "sess_codex_two_bad", 50, None).unwrap();
    let refused = page
        .requests
        .iter()
        .filter(|request| {
            request.usage.is_none()
                && request
                    .diagnostics
                    .contains(&UsageDiagnostic::UnnormalizableUsage)
        })
        .count();
    assert_eq!(refused, 2, "both refusals are reported, not just the last");
}

/// The attribution key and the view's grouping key are the same rule written
/// twice - once in Rust, once in SQL - so they are pinned against each other.
/// A drift here is what let one API request be counted once by the session
/// rollup and several times by prompt attribution.
#[test]
fn the_rust_request_key_agrees_with_the_view() {
    for fixture in [
        "claude/multi-record-request.jsonl",
        "claude/multi-block-turn.jsonl",
        "claude/multi-block-turn-no-request-id.jsonl",
        "claude/padded-request-id.jsonl",
    ] {
        let conn = claude_store(fixture);
        let events = crate::store::session_events(&conn, CLAUDE_SESSION, Some("claude")).unwrap();
        let mut from_rust: Vec<String> = events
            .iter()
            .filter(|event| event.role == "assistant")
            .map(crate::usage::request_key)
            .collect();
        from_rust.sort();
        from_rust.dedup();
        let mut from_view: Vec<String> =
            session_requests_page(&conn, "claude", CLAUDE_SESSION, 100, None)
                .unwrap()
                .requests
                .into_iter()
                .map(|request| request.request_key)
                .collect();
        from_view.sort();
        assert_eq!(from_rust, from_view, "{fixture}");
    }
}

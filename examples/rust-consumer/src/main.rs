//! An out-of-tree embedder of the published `ai-hist` crate.
//!
//! Stages a few transcripts from the repository's fixture corpus into a
//! throwaway `HOME`, opens a `SessionStore` there, syncs, and prints each
//! session's normalized usage totals by model. It is the smoke test for the
//! published artefact: an embedder that cannot do this is broken, whatever
//! the workspace's own tests say.
//!
//! Every read below goes through `SessionStore`. Nothing here opens SQLite,
//! names a table, or enables a feature; that is the surface a consumer gets.
//!
//! Run with `cargo run` from this directory. `RELAYHISTORY_FIXTURES` points
//! at a different corpus directory when the example is built away from the
//! repository checkout.

use ai_hist::{
    NormalizedUsage, SessionRequestCursor, SessionStore, Source, StoreOptions, SyncOptions,
    UsageAccounting,
};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

/// Corpus transcripts staged into the throwaway `HOME`, by fixture stem.
///
/// Claude transcripts land in `~/.claude/projects/corpus/`, Codex rollouts in
/// `~/.codex/sessions/2026/04/20/` under the `rollout-` prefix that adapter
/// enumerates -- the same staging `crates/ai-hist/tests/fixture_corpus.rs`
/// uses, so the sessions printed here are the ones the corpus README describes.
const CLAUDE_FIXTURES: &[&str] = &["simple-turn", "multi-block-turn", "files-touched"];
const CODEX_FIXTURES: &[&str] = &["simple-turn", "multi-turn", "with-tool-call"];

/// Provider-root overrides the crate honours from the environment.
///
/// `SessionStore` resolves each provider's root from `StoreOptions::home`
/// unless one of these names another directory, so a developer whose shell
/// exports `CODEX_HOME` would otherwise sync their real rollouts into this
/// example's temporary store. Cleared before the store is opened.
const PROVIDER_ROOT_OVERRIDES: &[&str] = &[
    "AI_HIST_DB",
    "CLAUDE_CONFIG_DIR",
    "CODEX_HOME",
    "GROK_HOME",
    "OPENCODE_DB",
    "OPENCODE_STORAGE_DIR",
];

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("error: {message}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    for name in PROVIDER_ROOT_OVERRIDES {
        std::env::remove_var(name);
    }

    let fixtures = fixture_root()?;
    let home =
        tempfile::tempdir().map_err(|error| format!("creating a temporary HOME: {error}"))?;
    let sessions = stage_corpus(&fixtures, home.path())?;
    println!(
        "staged {} sessions under {}",
        sessions.len(),
        home.path().display()
    );

    // `StoreOptions` is `#[non_exhaustive]`, so an outside crate sets fields on
    // the default rather than naming them all. With `home` set and no
    // `db_path`, the database is `<home>/.local/share/ai-hist/ai-history.db`.
    let mut options = StoreOptions::default();
    options.home = Some(home.path().to_path_buf());
    let store =
        SessionStore::open(options).map_err(|error| format!("opening the store: {error}"))?;

    // A full local scan. Another process holding the sync lock makes this
    // return without scanning rather than block; see docs/sourcing-sdk.md.
    let report = store
        .sync(SyncOptions::default())
        .map_err(|error| format!("syncing: {error}"))?;
    println!(
        "sync finished ({} refs reported changed)",
        report.changed.len()
    );

    // TODO(#178): iterate `store.sessions()` once the facade enumerates
    // sessions. Until then the ids come from the files this example staged.
    let mut sessions_with_usage = 0;
    for staged in &sessions {
        println!();
        println!(
            "== {} session {}",
            staged.source.as_str(),
            staged.session_id
        );
        if report_session(&store, staged)? {
            sessions_with_usage += 1;
        }
    }

    println!();
    // TODO(#179): drain `store.changes_since(watermark)` once with a named
    // consumer cursor and commit it; the change feed is not on the published
    // crate yet.
    println!("change feed: not available in this ai-hist release (tracked by relayhistory#179)");

    if sessions_with_usage == 0 {
        return Err("no staged session reported usage; the store did not sync the corpus".into());
    }
    println!(
        "{sessions_with_usage} of {} staged sessions reported normalized usage",
        sessions.len()
    );
    Ok(())
}

/// One session this example staged and therefore knows the id of.
struct StagedSession {
    source: Source,
    session_id: String,
}

/// Print a session's usage rollup and its per-model totals. Returns whether
/// the store recorded any request for it.
fn report_session(store: &SessionStore, staged: &StagedSession) -> Result<bool, String> {
    let source = staged.source;
    let id = staged.session_id.as_str();

    // The whole-session rollup. `None` is "no request recorded"; `Some` with
    // `usage: None` is "requests recorded, usage not establishable", and the
    // diagnostics say why.
    let Some(summary) = store
        .session_usage(source, id)
        .map_err(|error| format!("reading usage for {id}: {error}"))?
    else {
        println!("   no requests recorded");
        return Ok(false);
    };
    println!(
        "   requests: {} measured of {} recorded; accounting: {}; models: {}",
        summary.request_count,
        summary.total_request_count,
        join(summary.accounting.iter().map(|mode| mode.as_str())),
        join(summary.models.iter().map(String::as_str)),
    );
    if !summary.diagnostics.is_empty() {
        println!(
            "   diagnostics: {}",
            join(summary.diagnostics.iter().map(|code| code.as_str()))
        );
    }

    // Per-model totals, folded over the request pages. One request is the
    // unit here -- the facade has already grouped the provider's several
    // stored rows into it -- so adding requests together is exact for the
    // `per-request`, `per-message` and `cumulative-delta` modes.
    let mut by_model: BTreeMap<String, NormalizedUsage> = BTreeMap::new();
    let mut modes: BTreeSet<UsageAccounting> = BTreeSet::new();
    let mut cursor: Option<SessionRequestCursor> = None;
    loop {
        let page = store
            .session_requests_page(source, id, 100, cursor.as_ref())
            .map_err(|error| format!("reading requests for {id}: {error}"))?;
        for request in &page.requests {
            let Some(usage) = &request.usage else {
                continue;
            };
            modes.insert(usage.accounting);
            let model = request
                .model
                .clone()
                .unwrap_or_else(|| "(unknown model)".into());
            let total = match by_model.get(&model) {
                Some(existing) => existing
                    .checked_add(usage)
                    .ok_or_else(|| format!("usage total for {model} overflowed"))?,
                None => usage.clone(),
            };
            by_model.insert(model, total);
        }
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    for (model, usage) in &by_model {
        println!(
            "   {model}: input {} / output {} / cache read {} / cache write {}{}",
            usage.input_tokens,
            usage.output_tokens,
            usage.cache_read_tokens,
            usage.cache_write_tokens,
            usage
                .reasoning_tokens
                .map(|tokens| format!(" / reasoning {tokens}"))
                .unwrap_or_default(),
        );
    }
    if modes.contains(&UsageAccounting::ContextProxy) {
        println!("   (context-proxy records are occupancy figures; the totals above must not be read as spend)");
    }

    // Two more reads on the same surface, to show they are there: the human
    // turns with their measured block sizes, and the markers the normalized
    // event model cannot carry.
    let turns = store
        .session_user_turns_page(source, id, 100, None)
        .map_err(|error| format!("reading user turns for {id}: {error}"))?;
    let markers = store
        .session_markers_page(source, id, 100, None)
        .map_err(|error| format!("reading markers for {id}: {error}"))?;
    println!(
        "   {} user turns on the first page, {} markers",
        turns.user_turns.len(),
        markers.markers.len()
    );
    Ok(summary.total_request_count > 0)
}

fn join<'a>(items: impl Iterator<Item = &'a str>) -> String {
    let joined = items.collect::<Vec<_>>().join(", ");
    if joined.is_empty() {
        "-".to_string()
    } else {
        joined
    }
}

/// Where the fixture corpus lives: `RELAYHISTORY_FIXTURES`, or the repository
/// checkout this example ships in.
fn fixture_root() -> Result<PathBuf, String> {
    let root = match std::env::var_os("RELAYHISTORY_FIXTURES") {
        Some(path) => PathBuf::from(path),
        None => Path::new(env!("CARGO_MANIFEST_DIR")).join("../../crates/ai-hist/tests/fixtures"),
    };
    if root.join("claude").is_dir() && root.join("codex").is_dir() {
        Ok(root)
    } else {
        Err(format!(
            "fixture corpus not found at {}; set RELAYHISTORY_FIXTURES to crates/ai-hist/tests/fixtures",
            root.display()
        ))
    }
}

/// Copy the chosen transcripts into the provider layout under `home` and
/// return the session ids they carry.
fn stage_corpus(fixtures: &Path, home: &Path) -> Result<Vec<StagedSession>, String> {
    let mut sessions = Vec::new();

    let claude_dir = home.join(".claude/projects/corpus");
    fs::create_dir_all(&claude_dir)
        .map_err(|error| format!("creating {}: {error}", claude_dir.display()))?;
    for stem in CLAUDE_FIXTURES {
        let from = fixtures.join(format!("claude/{stem}.jsonl"));
        let to = claude_dir.join(format!("{stem}.jsonl"));
        copy(&from, &to)?;
        let session_id = first_string(&to, &["sessionId"])?;
        sessions.push(StagedSession {
            source: Source::Claude,
            session_id,
        });
    }

    let codex_dir = home.join(".codex/sessions/2026/04/20");
    fs::create_dir_all(&codex_dir)
        .map_err(|error| format!("creating {}: {error}", codex_dir.display()))?;
    for stem in CODEX_FIXTURES {
        let from = fixtures.join(format!("codex/{stem}.jsonl"));
        let to = codex_dir.join(format!("rollout-2026-04-20T00-00-00-{stem}.jsonl"));
        copy(&from, &to)?;
        let session_id = first_string(&to, &["payload", "id"])?;
        sessions.push(StagedSession {
            source: Source::Codex,
            session_id,
        });
    }

    Ok(sessions)
}

fn copy(from: &Path, to: &Path) -> Result<(), String> {
    fs::copy(from, to)
        .map(|_| ())
        .map_err(|error| format!("copying {} to {}: {error}", from.display(), to.display()))
}

/// The first record in a JSONL file that carries a string at `path`.
fn first_string(file: &Path, path: &[&str]) -> Result<String, String> {
    let text =
        fs::read_to_string(file).map_err(|error| format!("reading {}: {error}", file.display()))?;
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let Ok(record) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        let mut value = &record;
        for key in path {
            value = &value[*key];
        }
        if let Some(found) = value.as_str() {
            return Ok(found.to_string());
        }
    }
    Err(format!(
        "no record in {} carries {}",
        file.display(),
        path.join(".")
    ))
}

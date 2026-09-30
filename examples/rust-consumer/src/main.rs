//! An out-of-tree embedder of the published `ai-hist` crate.
//!
//! Stages a few transcripts from the repository's fixture corpus into a
//! throwaway `HOME`, opens a `SessionStore` there, syncs, walks the catalog,
//! prints each session's normalized usage totals by model, and drains the
//! change feed under a named consumer cursor. It is the smoke test for the
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
    CatalogQuery, ChangeKind, ChangeOp, ChangeQuery, NormalizedUsage, ProviderRoots,
    SessionEvidence, SessionQuery, SessionStore, Source, StoreOptions, SyncOptions,
    UsageAccounting, Watermark,
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

/// The name this example's change-feed cursor is kept under inside the store.
const CONSUMER: &str = "rust-consumer-example";

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
    let fixtures = fixture_root()?;
    let home =
        tempfile::tempdir().map_err(|error| format!("creating a temporary HOME: {error}"))?;
    let staged = stage_corpus(&fixtures, home.path())?;
    println!(
        "staged {} sessions under {}",
        staged.len(),
        home.path().display()
    );

    // `StoreOptions` is `#[non_exhaustive]`, so an outside crate sets fields on
    // the default rather than naming them all. `home` puts the database at
    // `<home>/.local/share/ai-hist/ai-history.db`; `roots` names every provider
    // root outright, so a developer whose shell exports `CODEX_HOME` cannot
    // have their real rollouts swept into this example's temporary store. The
    // roots are resolved once here and drive `sync` and every read after it.
    let mut options = StoreOptions::default();
    options.home = Some(home.path().to_path_buf());
    options.roots = Some(ProviderRoots::from_home(
        home.path().to_path_buf(),
        home.path().join(".local/share/opencode/opencode.db"),
    ));
    let store =
        SessionStore::open(options).map_err(|error| format!("opening the store: {error}"))?;

    // A full local scan. Another process holding the sync lock is
    // `Error::SyncLocked` after `lock_timeout_ms`, never a silent skip; see
    // docs/sourcing-sdk.md.
    let report = store
        .sync(SyncOptions::default())
        .map_err(|error| format!("syncing: {error}"))?;
    println!(
        "sync finished (swept={}, {} refs reported changed, head revision {})",
        report.swept,
        report.changed.len(),
        report.head_revision,
    );

    // The catalog, newest first. The ids come from the store, not from the
    // files this example staged; the staged set is only what the run is
    // checked against at the end.
    let mut sessions_with_usage = 0;
    let mut catalogued: BTreeSet<(Source, String)> = BTreeSet::new();
    for row in store.sessions(CatalogQuery::default()) {
        let row = row.map_err(|error| format!("walking the catalog: {error}"))?;
        catalogued.insert((row.source, row.session_id.clone()));
        let Some(evidence) = store
            .session(&row.session_ref(), SessionQuery::default())
            .map_err(|error| format!("reading session {}: {error}", row.session_id))?
        else {
            continue;
        };
        println!();
        println!("== {} session {}", row.source.as_str(), row.session_id);
        if report_session(&evidence) {
            sessions_with_usage += 1;
        }
    }

    println!();
    drain_changes(&store)?;

    let missing: Vec<String> = staged
        .iter()
        .filter(|session| !catalogued.contains(&(session.source, session.session_id.clone())))
        .map(|session| format!("{} {}", session.source.as_str(), session.session_id))
        .collect();
    if !missing.is_empty() {
        return Err(format!(
            "the sweep did not catalogue {}",
            missing.join(", ")
        ));
    }
    if sessions_with_usage == 0 {
        return Err(
            "no catalogued session reported usage; the store did not sync the corpus".into(),
        );
    }
    println!();
    println!(
        "{sessions_with_usage} of {} catalogued sessions reported normalized usage",
        catalogued.len()
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
fn report_session(evidence: &SessionEvidence) -> bool {
    // The whole-session rollup. `None` is "no request recorded"; `Some` with
    // `usage: None` is "requests recorded, usage not establishable", and the
    // diagnostics say why.
    let Some(summary) = &evidence.usage else {
        println!("   no requests recorded");
        return false;
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

    // Per-model totals folded over `requests`. One request is the unit here --
    // the facade has already grouped the provider's several stored rows into
    // it -- so adding requests together is exact for the `per-request`,
    // `per-message` and `cumulative-delta` modes.
    let mut by_model: BTreeMap<String, NormalizedUsage> = BTreeMap::new();
    let mut modes: BTreeSet<UsageAccounting> = BTreeSet::new();
    let mut overflowed = false;
    for request in &evidence.requests {
        let Some(usage) = &request.usage else {
            continue;
        };
        modes.insert(usage.accounting);
        let model = request
            .model
            .clone()
            .unwrap_or_else(|| "(unknown model)".into());
        let total = match by_model.get(&model) {
            Some(existing) => match existing.checked_add(usage) {
                Some(total) => total,
                None => {
                    overflowed = true;
                    continue;
                }
            },
            None => usage.clone(),
        };
        by_model.insert(model, total);
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
    if overflowed {
        println!("   (a per-model total exceeded u64 and was left at its last exact value)");
    }
    if modes.contains(&UsageAccounting::ContextProxy) {
        println!("   (context-proxy records are occupancy figures; the totals above must not be read as spend)");
    }

    // Three more reads on the same `SessionEvidence`, to show they are there:
    // the human turns with their measured block sizes, the markers the
    // normalized event model cannot carry, and the user-role blocks the
    // facade classified as something other than a prompt.
    let control_blocks = evidence
        .messages
        .iter()
        .flat_map(|message| message.blocks.iter())
        .filter(|block| block.control.is_some())
        .count();
    println!(
        "   {} user turns, {} markers, {control_blocks} classified control blocks",
        evidence.user_turns.len(),
        evidence.markers.len(),
    );
    println!(
        "   coverage: {}",
        join(evidence.coverage.iter().map(|kind| kind.as_str()))
    );
    summary.total_request_count > 0
}

/// Drain the change feed from the example's named cursor and commit it.
///
/// A consumer keeps its position inside the store: the first run replays from
/// `Watermark::START`, and a later one resumes from what `commit` stored. The
/// cursor moves only on commit, so a consumer that fails mid-drain re-reads
/// its page rather than skipping it.
fn drain_changes(store: &SessionStore) -> Result<(), String> {
    let mut changes = store
        .changes_since(
            Watermark::CONSUMER,
            ChangeQuery::default().consumer(CONSUMER),
        )
        .map_err(|error| format!("opening the change feed: {error}"))?;
    let head = changes.head();
    let mut upserts: BTreeMap<ChangeKind, usize> = BTreeMap::new();
    let mut deletes: BTreeMap<ChangeKind, usize> = BTreeMap::new();
    let mut other: BTreeMap<ChangeKind, usize> = BTreeMap::new();
    for change in changes.by_ref() {
        let change = change.map_err(|error| format!("draining the change feed: {error}"))?;
        // `ChangeOp` is `#[non_exhaustive]`: an op a later release adds is
        // counted as neither an upsert nor a delete here rather than making
        // this example stop compiling on a minor bump.
        match change.op {
            ChangeOp::Upsert(_) => *upserts.entry(change.kind).or_default() += 1,
            ChangeOp::Delete => *deletes.entry(change.kind).or_default() += 1,
            _ => *other.entry(change.kind).or_default() += 1,
        }
    }
    let committed = changes
        .commit()
        .map_err(|error| format!("committing the {CONSUMER} cursor: {error}"))?;
    println!(
        "change feed drained to revision {} (head {})",
        committed.revision, head.revision
    );
    for kind in ChangeKind::ALL {
        let upserted = upserts.get(kind).copied().unwrap_or(0);
        let deleted = deletes.get(kind).copied().unwrap_or(0);
        let unclassified = other.get(kind).copied().unwrap_or(0);
        if upserted > 0 || deleted > 0 || unclassified > 0 {
            println!(
                "   {}: {upserted} upserted, {deleted} deleted, {unclassified} in an op this build does not know",
                kind.as_str()
            );
        }
    }
    Ok(())
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

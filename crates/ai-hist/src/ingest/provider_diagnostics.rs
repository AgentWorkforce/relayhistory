//! Provider skip diagnostics: what a pass left out, and why.
//!
//! A provider that skips part of its history — a Codex SQLite store it
//! cannot read, a related thread it cannot capture — reports the skip here
//! and carries on. The line goes to stderr unless the embedding API's quiet
//! setting is on; the diagnostic goes to the report of the facade call that
//! is collecting (`sync`, `watch` and `hydrate` all do), so a host that
//! silenced stderr still learns what the pass left out.
use std::cell::RefCell;
use std::fmt::Arguments;
use std::sync::atomic::Ordering as AtomicOrdering;
use std::sync::{Arc, Mutex, PoisonError};

use crate::session_store::{split_code, Source, SyncDiagnostic};

use super::SYNC_QUIET;

/// One thing a provider left out of a pass.
pub(crate) struct ProviderSkip<'a> {
    pub source: Source,
    /// The code reported when `detail` carries none of its own.
    pub code: &'static str,
    /// The session or thread the skip names, when it names one.
    pub subject: Option<&'a str>,
    /// What was skipped, e.g. `skipping SQLite history`.
    pub what: Arguments<'a>,
    /// Why: the error chain, or the selection diagnostic.
    pub detail: Arguments<'a>,
}

/// Where the current pass's diagnostics go. Shared with the worker threads a
/// pass spawns, which [`install`] it on their own thread.
pub(crate) type Sink = Arc<Mutex<Vec<SyncDiagnostic>>>;

thread_local! {
    static SINK: RefCell<Option<Sink>> = const { RefCell::new(None) };
}

#[cfg(test)]
thread_local! {
    /// Every line [`route`] printed on this thread.
    pub(crate) static STDERR_LINES: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

/// Report a skip: to stderr unless the pass is quiet, and to the collecting
/// facade call's report whenever one is collecting.
pub(crate) fn provider_diagnostic(skip: ProviderSkip<'_>) {
    route(SYNC_QUIET.load(AtomicOrdering::Relaxed), skip);
}

fn route(quiet: bool, skip: ProviderSkip<'_>) {
    let detail = skip.detail.to_string();
    if !quiet {
        stderr_line(format!(
            "ai-hist: [{}] {}: {detail}",
            skip.source.as_str(),
            skip.what
        ));
    }
    let Some(sink) = SINK.with(|slot| slot.borrow().clone()) else {
        return;
    };
    let (code, detail) = split_code(&detail).unwrap_or((skip.code, detail.as_str()));
    sink.lock()
        .unwrap_or_else(PoisonError::into_inner)
        .push(SyncDiagnostic {
            source: skip.source,
            code: code.to_string(),
            message: format!("{}: {detail}", skip.what),
            subject: skip.subject.map(str::to_string),
        });
}

fn stderr_line(line: String) {
    #[cfg(test)]
    STDERR_LINES.with(|lines| lines.borrow_mut().push(line.clone()));
    eprintln!("{line}");
}

/// Run `run` with a fresh sink on this thread and return what the providers
/// reported into it, in the order they reported.
pub(crate) fn collect<T>(run: impl FnOnce() -> T) -> (T, Vec<SyncDiagnostic>) {
    let sink: Sink = Arc::default();
    let outcome = {
        let _installed = install(Some(sink.clone()));
        run()
    };
    let diagnostics = std::mem::take(&mut *sink.lock().unwrap_or_else(PoisonError::into_inner));
    (outcome, diagnostics)
}

/// The sink this thread reports into, for a worker thread to [`install`].
pub(crate) fn current() -> Option<Sink> {
    SINK.with(|slot| slot.borrow().clone())
}

/// Report into `sink` on this thread until the guard drops, then into
/// whatever this thread reported into before.
pub(crate) fn install(sink: Option<Sink>) -> impl Drop {
    struct Restore(Option<Sink>);
    impl Drop for Restore {
        fn drop(&mut self) {
            SINK.with(|slot| *slot.borrow_mut() = self.0.take());
        }
    }
    Restore(SINK.with(|slot| slot.replace(sink)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn printed() -> Vec<String> {
        STDERR_LINES.with(|lines| std::mem::take(&mut *lines.borrow_mut()))
    }

    #[test]
    fn a_quiet_skip_reaches_the_report_and_not_stderr() {
        printed();
        let ((), diagnostics) = collect(|| {
            route(
                true,
                ProviderSkip {
                    source: Source::Codex,
                    code: "SESSION_SOURCE_UNAVAILABLE",
                    subject: None,
                    what: format_args!("skipping SQLite history"),
                    detail: format_args!("permission denied"),
                },
            );
        });
        assert!(printed().is_empty(), "quiet prints nothing");
        assert_eq!(
            diagnostics,
            vec![SyncDiagnostic {
                source: Source::Codex,
                code: "SESSION_SOURCE_UNAVAILABLE".into(),
                message: "skipping SQLite history: permission denied".into(),
                subject: None,
            }]
        );
    }

    #[test]
    fn a_loud_skip_reaches_stderr_and_the_report() {
        printed();
        let ((), diagnostics) = collect(|| {
            route(
                false,
                ProviderSkip {
                    source: Source::Codex,
                    code: "EVIDENCE_PARTIAL",
                    subject: Some("thread-1"),
                    what: format_args!("skipping related thread thread-1"),
                    detail: format_args!("SESSION_SOURCE_MISMATCH: rollout identity changed"),
                },
            );
        });
        assert_eq!(
            printed(),
            vec!["ai-hist: [codex] skipping related thread thread-1: SESSION_SOURCE_MISMATCH: rollout identity changed".to_string()]
        );
        // A code the detail carries wins over the site's default, and the
        // message no longer repeats it.
        assert_eq!(
            diagnostics,
            vec![SyncDiagnostic {
                source: Source::Codex,
                code: "SESSION_SOURCE_MISMATCH".into(),
                message: "skipping related thread thread-1: rollout identity changed".into(),
                subject: Some("thread-1".into()),
            }]
        );
    }

    #[test]
    fn a_skip_outside_any_collecting_call_is_printed_only() {
        printed();
        route(
            false,
            ProviderSkip {
                source: Source::Codex,
                code: "EVIDENCE_PARTIAL",
                subject: None,
                what: format_args!("skipping SQLite thread t"),
                detail: format_args!("boom"),
            },
        );
        assert_eq!(printed().len(), 1);
        assert!(current().is_none());
    }

    #[test]
    fn a_worker_thread_reports_into_the_sink_it_inherits() {
        let ((), diagnostics) = collect(|| {
            let sink = current();
            std::thread::scope(|scope| {
                scope.spawn(|| {
                    let _installed = install(sink.clone());
                    route(
                        true,
                        ProviderSkip {
                            source: Source::Codex,
                            code: "EVIDENCE_PARTIAL",
                            subject: Some("child"),
                            what: format_args!("skipping related rollout child"),
                            detail: format_args!("unreadable"),
                        },
                    );
                });
            });
            // Restored on the worker: nothing is left installed there, and
            // this thread's sink is untouched.
            assert!(current().is_some());
        });
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0].subject.as_deref(), Some("child"));
        assert!(current().is_none());
    }
}

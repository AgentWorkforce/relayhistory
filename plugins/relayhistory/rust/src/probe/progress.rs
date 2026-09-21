//! Content-free, best-effort progress. A slow/offline status endpoint never blocks capture.
use relayhistory_plugin::cloud;
use relayhistory_plugin::delivery;
use serde::Serialize;
use std::{
    path::Path,
    sync::{mpsc, Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(3);
const REFRESH_TIMEOUT: Duration = Duration::from_secs(2);
// In-flight heartbeat + final token renewal/report + local bookkeeping margin.
pub const COMPLETION_TIMEOUT: Duration =
    Duration::from_secs(HEARTBEAT_TIMEOUT.as_secs() * 2 + REFRESH_TIMEOUT.as_secs() + 1);

#[derive(Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Progress {
    pub phase: String,
    pub source: String,
    pub processed_files: usize,
    pub total_files: Option<usize>,
    pub sessions_captured: i64,
    pub records_uploaded: i64,
    pub records_queued: i64,
    pub backlog_complete: bool,
}

impl Progress {
    fn read_counts(&mut self, db: &Path, job: Option<&str>) {
        let Ok(conn) = ai_hist::open_db_readonly(db) else {
            return;
        };
        let _ = conn.busy_timeout(Duration::from_millis(100));
        self.sessions_captured = conn
            .query_row(
                "SELECT COUNT(*) FROM sessions WHERE COALESCE(discovery_state, 'full') = 'full'",
                [],
                |r| r.get(0),
            )
            .unwrap_or(0);
        if let Some(job) = job {
            if let Ok(status) = delivery::status(&conn, job) {
                self.apply_delivery(&status);
            }
        }
    }
    fn apply_delivery(&mut self, status: &delivery::DeliveryStatus) {
        self.records_uploaded = status.acknowledged_records;
        self.records_queued = status.pending_records;
        self.backlog_complete = status.bootstrap_complete && status.unqueued_changes == 0;
        self.phase = if status.state != "active" || status.failure.is_some() {
            "paused"
        } else if self.backlog_complete && status.pending_records == 0 {
            "watching"
        } else {
            "uploading"
        }
        .into();
    }
    fn line(&self) -> String {
        match self.phase.as_str() {
            "scanning" => match self.total_files {
                Some(total) => format!(
                    "Reading {} session files: {} of {} processed · {} sessions captured",
                    self.source, self.processed_files, total, self.sessions_captured
                ),
                None => format!(
                    "Scanning {}… {} sessions captured",
                    self.source, self.sessions_captured
                ),
            },
            "capture_paused" => format!(
                "Local capture paused; retrying. {} session files processed, {} sessions captured",
                self.processed_files, self.sessions_captured
            ),
            "paused" => format!(
                "Upload paused; retrying. {} records uploaded, {} queued",
                self.records_uploaded, self.records_queued
            ),
            "watching" => format!(
                "Up to date: {} records uploaded. Watching for new sessions.",
                self.records_uploaded
            ),
            _ => format!(
                "Uploading: {} records received, {} queued{}",
                self.records_uploaded,
                self.records_queued,
                if self.backlog_complete {
                    ""
                } else {
                    " · preparing remaining records"
                }
            ),
        }
    }
}

pub struct Monitor {
    snapshot: Arc<Mutex<Progress>>,
    stop: Option<mpsc::Sender<bool>>,
    completion: mpsc::Receiver<bool>,
}
impl Monitor {
    pub fn start(directory: &Path, history_url: &str, job: Option<&str>) -> Self {
        let url = history_url.to_owned();
        Self::start_with_report(
            directory,
            job,
            !super::bridge::json_mode(),
            move |progress, finished| heartbeat(&url, progress, finished),
        )
    }
    fn start_with_report(
        directory: &Path,
        job: Option<&str>,
        show_human_progress: bool,
        mut report: impl FnMut(&Progress, bool) -> bool + Send + 'static,
    ) -> Self {
        let snapshot = Arc::new(Mutex::new(Progress {
            phase: if job.is_some() {
                "uploading"
            } else {
                "scanning"
            }
            .into(),
            source: "local history".into(),
            ..Default::default()
        }));
        let shared = snapshot.clone();
        let db = directory.join("history.db");
        let job = job.map(str::to_owned);
        let (stop, receive) = mpsc::channel();
        let (completed, completion) = mpsc::channel();
        thread::spawn(move || {
            let started = Instant::now();
            let mut finish = None;
            let mut acknowledged = false;
            loop {
                // The next upload monitor now owns progress. Do not send a
                // delayed final "scanning" heartbeat after successful capture.
                if finish == Some(true) && job.is_none() {
                    break;
                }
                let mut progress = shared.lock().unwrap().clone();
                progress.read_counts(&db, job.as_deref());
                if finish == Some(false) {
                    progress.phase = if job.is_none() {
                        "capture_paused"
                    } else {
                        "paused"
                    }
                    .into();
                }
                if let Some(line) =
                    human_progress_line(&progress, started.elapsed(), show_human_progress)
                {
                    println!("{line}");
                }
                acknowledged = report(&progress, finish.is_some());
                if finish.is_some() {
                    break;
                }
                match receive.recv_timeout(Duration::from_secs(3)) {
                    Ok(success) => finish = Some(success),
                    Err(mpsc::RecvTimeoutError::Disconnected) => finish = Some(false),
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                }
            }
            let _ = completed.send(acknowledged);
        });
        Self {
            snapshot,
            stop: Some(stop),
            completion,
        }
    }
    pub fn observer(&self) -> impl Fn(ai_hist::CaptureProgress) + 'static {
        let snapshot = self.snapshot.clone();
        move |capture| {
            let mut p = snapshot.lock().unwrap();
            p.source = capture.source;
            p.processed_files = capture.processed_files;
            p.total_files = capture.total_files;
        }
    }
    pub fn finish_before_exit(mut self, success: bool, timeout: Duration) -> bool {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(success);
        }
        // Setup/--once gets a chance to flush final queue counts, with a strict
        // deadline. Ordinary background finish/drop still never waits.
        self.completion.recv_timeout(timeout).unwrap_or(false)
    }
    // Background/capture callers only signal stop; the worker exits after its
    // bounded request/final report without holding up the caller.
    pub fn finish(mut self, success: bool) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(success);
        }
    }
}

fn human_progress_line(progress: &Progress, elapsed: Duration, enabled: bool) -> Option<String> {
    enabled.then(|| format!("{} ({}s)", progress.line(), elapsed.as_secs()))
}
impl Drop for Monitor {
    fn drop(&mut self) {
        self.stop.take();
    }
}
fn heartbeat(url: &str, progress: &Progress, finished: bool) -> bool {
    // Periodic capture updates only read cached credentials. A final upload
    // update may renew an idle token, without waiting for the refresh lock.
    let token = if finished && progress.phase != "capture_paused" {
        match cloud::try_progress_access_token(url, REFRESH_TIMEOUT) {
            Ok(Some(token)) => token,
            _ => return false,
        }
    } else {
        let Ok(Some(auth)) = cloud::load_selected_auth(Some(url)) else {
            return false;
        };
        let valid = auth
            .access_token_expires_at
            .as_deref()
            .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
            .is_some_and(|expiry| expiry.timestamp() > chrono::Utc::now().timestamp() + 5);
        if !valid
            || !auth.access_token.starts_with("rth_at_")
            || !auth
                .access_token
                .bytes()
                .all(|byte| byte.is_ascii_graphic())
        {
            return false;
        }
        auth.access_token
    };
    let result = ureq::AgentBuilder::new()
        .redirects(0)
        .timeout(HEARTBEAT_TIMEOUT)
        .build()
        .post(&format!("{url}/v1/onboarding/heartbeat"))
        .set("Authorization", &format!("Bearer {token}"))
        .send_json(serde_json::json!({"progress":progress}));
    let acknowledged = result.is_ok_and(|response| (200..300).contains(&response.status()));
    if !acknowledged {
        eprintln!("Cloud progress update unavailable; collection continues locally.");
    }
    acknowledged
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn finish_and_drop_do_not_wait_for_a_slow_reporter() {
        for success in [None, Some(false), Some(true)] {
            let directory = tempfile::tempdir().unwrap();
            let (entered, started) = mpsc::channel();
            let (release, blocked) = mpsc::channel::<()>();
            let (finished, done) = mpsc::channel();
            let mut first = true;
            let monitor =
                Monitor::start_with_report(directory.path(), None, false, move |progress, _| {
                    if first {
                        first = false;
                        entered.send(()).unwrap();
                        blocked.recv().unwrap();
                    } else {
                        finished.send(progress.phase.clone()).unwrap();
                    }
                    true
                });
            started.recv_timeout(Duration::from_secs(2)).unwrap();
            let start = Instant::now();
            if let Some(success) = success {
                monitor.finish(success);
            } else {
                drop(monitor);
            }
            assert!(start.elapsed() < Duration::from_millis(100));
            release.send(()).unwrap();
            let final_phase = done.recv_timeout(Duration::from_secs(2));
            if success == Some(true) {
                assert!(matches!(
                    final_phase,
                    Err(mpsc::RecvTimeoutError::Disconnected)
                ));
            } else {
                assert_eq!(final_phase.unwrap(), "capture_paused");
            }
        }
    }

    #[test]
    fn completion_barrier_waits_for_the_final_report_but_has_a_deadline() {
        let directory = tempfile::tempdir().unwrap();
        let (reported, received) = mpsc::channel();
        let monitor =
            Monitor::start_with_report(directory.path(), Some("job"), false, move |_, finished| {
                if finished {
                    reported.send(()).unwrap();
                }
                true
            });
        assert!(monitor.finish_before_exit(true, Duration::from_secs(1)));
        received.try_recv().unwrap();
        let (release, blocked) = mpsc::channel::<()>();
        let monitor =
            Monitor::start_with_report(directory.path(), Some("job"), false, move |_, _| {
                let _ = blocked.recv();
                true
            });
        let start = Instant::now();
        assert!(!monitor.finish_before_exit(true, Duration::from_millis(50)));
        assert!(start.elapsed() < Duration::from_millis(500));
        drop(release);
        let monitor =
            Monitor::start_with_report(directory.path(), Some("job"), false, |_, _| false);
        assert!(!monitor.finish_before_exit(true, Duration::from_secs(1)));
    }

    #[test]
    fn json_progress_suppresses_console_text_but_still_reports_completion() {
        let progress = Progress {
            phase: "scanning".into(),
            ..Default::default()
        };
        assert!(human_progress_line(&progress, Duration::from_secs(1), false).is_none());
        assert!(human_progress_line(&progress, Duration::from_secs(1), true).is_some());

        let directory = tempfile::tempdir().unwrap();
        let (reported, received) = mpsc::channel();
        let monitor =
            Monitor::start_with_report(directory.path(), Some("job"), false, move |_, finished| {
                if finished {
                    reported.send(()).unwrap();
                }
                true
            });
        assert!(monitor.finish_before_exit(true, Duration::from_secs(1)));
        received.recv_timeout(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn legacy_null_discovery_state_counts_as_captured() {
        let directory = tempfile::tempdir().unwrap();
        let db = directory.path().join("history.db");
        let conn = rusqlite::Connection::open(&db).unwrap();
        conn.execute_batch("CREATE TABLE sessions (discovery_state TEXT); INSERT INTO sessions VALUES (NULL), ('full'), ('partial');").unwrap();
        let mut progress = Progress::default();
        progress.read_counts(&db, None);
        assert_eq!(progress.sessions_captured, 2);
        progress.phase = "capture_paused".into();
        assert!(progress.line().contains("Local capture paused"));
        assert!(!progress.line().contains("uploaded"));
    }

    #[test]
    fn unknown_totals_do_not_claim_completion() {
        let progress = Progress {
            phase: "uploading".into(),
            records_uploaded: 20,
            ..Default::default()
        };
        assert!(progress.line().contains("preparing remaining records"));
        let capture = Progress {
            phase: "scanning".into(),
            source: "claude".into(),
            processed_files: 2,
            total_files: Some(5),
            ..Default::default()
        };
        assert!(capture.line().contains("2 of 5"));
    }
}

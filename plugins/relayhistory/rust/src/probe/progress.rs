//! Content-free, best-effort progress. A slow/offline status endpoint never blocks capture.
use relayhistory_plugin::cloud;
use relayhistory_plugin::delivery;
use serde::Serialize;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{mpsc, Arc, Mutex, OnceLock},
    thread,
    time::{Duration, Instant},
};

const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(3);
const REFRESH_TIMEOUT: Duration = Duration::from_secs(2);
// In-flight renewal/report + final renewal/report + local bookkeeping margin.
pub const COMPLETION_TIMEOUT: Duration =
    Duration::from_secs((HEARTBEAT_TIMEOUT.as_secs() + REFRESH_TIMEOUT.as_secs()) * 2 + 1);

#[derive(Clone, Default, PartialEq, Eq, Serialize)]
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

// Shared across the short-lived capture/delivery monitors in a collector process.
// Otherwise every empty background cycle would reset the idle heartbeat deadline.
const PROGRESS_INTERVAL: Duration = Duration::from_secs(3);
const RETRY_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Default)]
struct HeartbeatSchedule {
    last_sent: Option<(Progress, Instant)>,
    last_attempt: Option<(Progress, Instant, bool)>,
    idle_interval: Duration,
}
impl HeartbeatSchedule {
    fn new() -> Self {
        // RandomState provides a per-instance random seed without a new dependency.
        use std::hash::{BuildHasher, Hasher};
        let jitter = std::collections::hash_map::RandomState::new()
            .build_hasher()
            .finish()
            % 61;
        Self {
            idle_interval: Duration::from_secs(270 + jitter),
            ..Self::default()
        }
    }

    fn comparable(progress: &Progress) -> Progress {
        let mut p = progress.clone();
        if p.phase != "scanning" && p.phase != "capture_paused" {
            // Capture's per-source counters reset when delivery starts.
            p.source.clear();
            p.processed_files = 0;
            p.total_files = None;
        }
        p
    }

    fn report(
        &mut self,
        progress: &Progress,
        elapsed: Duration,
        force: bool,
        now: Instant,
        send: impl FnOnce() -> bool,
    ) -> bool {
        let p = Self::comparable(progress);
        let changed = self.last_sent.as_ref().is_none_or(|(last, _)| last != &p);
        let terminal = matches!(p.phase.as_str(), "watching" | "paused" | "capture_paused");
        let new_terminal = terminal
            && changed
            && self
                .last_attempt
                .as_ref()
                .is_none_or(|(last, _, _)| last != &p);
        let due = self
            .last_sent
            .as_ref()
            .is_none_or(|(_, at)| now.duration_since(*at) >= self.idle_interval);
        let can_retry = self.last_attempt.as_ref().is_none_or(|(_, at, ok)| {
            now.duration_since(*at)
                >= if *ok {
                    PROGRESS_INTERVAL
                } else {
                    RETRY_INTERVAL
                }
        });
        // A quick periodic scan should not turn an idle connection into an
        // upload on every cycle. Initial onboarding still reports immediately.
        let brief_scan =
            p.phase == "scanning" && self.last_sent.is_some() && elapsed < PROGRESS_INTERVAL;
        if !force && (brief_scan || !(new_terminal || (can_retry && (changed || due)))) {
            return self.last_sent.is_some();
        }
        let ok = send();
        self.last_attempt = Some((p.clone(), now, ok));
        if ok {
            self.last_sent = Some((p, now));
        }
        ok
    }
}

type ScheduleKey = (PathBuf, String);
fn heartbeat_schedule(directory: &Path, url: &str) -> Arc<Mutex<HeartbeatSchedule>> {
    static SCHEDULES: OnceLock<Mutex<HashMap<ScheduleKey, Arc<Mutex<HeartbeatSchedule>>>>> =
        OnceLock::new();
    SCHEDULES
        .get_or_init(Default::default)
        .lock()
        .unwrap()
        .entry((directory.to_owned(), url.to_owned()))
        .or_insert_with(|| Arc::new(Mutex::new(HeartbeatSchedule::new())))
        .clone()
}

// Local publication has its own lock, never held during a network request.
// Closing a publisher flushes the terminal snapshot and fences delayed worker
// snapshots before the collector can hand progress ownership to its next monitor.
struct LocalProgress {
    directory: PathBuf,
    closed: Mutex<bool>,
}
impl LocalProgress {
    fn publish(&self, progress: &Progress) {
        let closed = self.closed.lock().unwrap();
        if !*closed {
            self.write(progress);
        }
    }

    fn finish(&self, mut progress: Progress, job: Option<&str>, success: bool) {
        let mut closed = self.closed.lock().unwrap();
        if *closed {
            return;
        }
        *closed = true;
        progress.read_counts(&self.directory.join("history.db"), job);
        if !success {
            progress.phase = if job.is_some() {
                "paused"
            } else {
                "capture_paused"
            }
            .into();
        }
        self.write(&progress);
    }

    fn write(&self, progress: &Progress) {
        let _ = super::save_json(
            &self.directory.join("progress.json"),
            &serde_json::json!({
                "updated_at_ms": chrono::Utc::now().timestamp_millis(), "progress": progress
            }),
        );
        if super::bridge::json_mode() {
            super::bridge::emit(serde_json::json!({"event":"progress", "progress":progress}));
        }
    }
}

pub struct Monitor {
    snapshot: Arc<Mutex<Progress>>,
    stop: Option<mpsc::Sender<bool>>,
    completion: mpsc::Receiver<bool>,
    local: Option<(Arc<LocalProgress>, Option<String>)>,
}
impl Monitor {
    pub fn start(
        directory: &Path,
        history_url: &str,
        job: Option<&str>,
        confirm_completion: bool,
    ) -> Self {
        let url = history_url.to_owned();
        let local = Arc::new(LocalProgress {
            directory: directory.to_owned(),
            closed: Mutex::new(false),
        });
        let publisher = local.clone();
        let report_delivery = job.is_some();
        let schedule = heartbeat_schedule(directory, history_url);
        let started = Instant::now();
        let mut monitor = Self::start_with_report(
            directory,
            job,
            !super::bridge::json_mode(),
            move |progress, finished| {
                // Full capture details stay on this Mac, including during first sign-in.
                publisher.publish(progress);
                if !report_delivery {
                    return true;
                }
                let progress = cloud_delivery_progress(progress);
                schedule.lock().unwrap().report(
                    &progress,
                    started.elapsed(),
                    finished && confirm_completion,
                    Instant::now(),
                    || heartbeat(&url, &progress),
                )
            },
        );
        monitor.local = Some((local, job.map(str::to_owned)));
        monitor
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
            let acknowledged = loop {
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
                let acknowledged = report(&progress, finish.is_some());
                if finish.is_some() {
                    break acknowledged;
                }
                match receive.recv_timeout(PROGRESS_INTERVAL) {
                    Ok(success) => finish = Some(success),
                    Err(mpsc::RecvTimeoutError::Disconnected) => finish = Some(false),
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                }
            };
            let _ = completed.send(acknowledged);
        });
        Self {
            snapshot,
            stop: Some(stop),
            completion,
            local: None,
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
    fn finish_local(&mut self, success: bool) {
        if let Some((local, job)) = self.local.take() {
            local.finish(
                self.snapshot.lock().unwrap().clone(),
                job.as_deref(),
                success,
            );
        }
    }

    pub fn finish_before_exit(mut self, success: bool, timeout: Duration) -> bool {
        self.finish_local(success);
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(success);
        }
        // Setup/--once gets a chance to flush final queue counts, with a strict
        // deadline. Ordinary background finish/drop never waits for the network.
        self.completion.recv_timeout(timeout).unwrap_or(false)
    }
    // Flush local state before handoff. Background callers never wait for the
    // worker's network request or remote final report.
    pub fn finish(mut self, success: bool) {
        self.finish_local(success);
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
        self.finish_local(false);
        self.stop.take();
    }
}
// Cloud sees delivery of permitted records only, never the local capture inventory.
fn cloud_delivery_progress(progress: &Progress) -> Progress {
    Progress {
        phase: progress.phase.clone(),
        records_uploaded: progress.records_uploaded,
        records_queued: progress.records_queued,
        backlog_complete: progress.backlog_complete,
        ..Progress::default()
    }
}

fn heartbeat(url: &str, progress: &Progress) -> bool {
    if matches!(progress.phase.as_str(), "scanning" | "capture_paused") {
        return true;
    }
    // Sanitize at the network boundary too, so future callers cannot leak counts.
    let progress = cloud_delivery_progress(progress);
    // Capture updates only read cached credentials. Every actual delivery or idle
    // report may renew a token: the closing report can now be suppressed. Renewal
    // is bounded and never waits for another transport's refresh lock.
    let token = if progress.phase != "scanning" && progress.phase != "capture_paused" {
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
    fn cloud_delivery_never_contains_local_inventory() {
        let local = Progress {
            phase: "uploading".into(),
            source: "codex".into(),
            processed_files: 481,
            total_files: Some(2357),
            sessions_captured: 1687,
            records_uploaded: 12,
            records_queued: 3,
            backlog_complete: true,
        };
        let cloud = cloud_delivery_progress(&local);
        assert!(cloud.source.is_empty());
        assert_eq!(cloud.processed_files, 0);
        assert_eq!(cloud.total_files, None);
        assert_eq!(cloud.sessions_captured, 0);
        assert_eq!(cloud.records_uploaded, 12);
        assert_eq!(cloud.records_queued, 3);
        for phase in ["scanning", "capture_paused"] {
            let capture = Progress {
                phase: phase.into(),
                ..local.clone()
            };
            // This deliberately invalid endpoint proves capture never tries HTTP/auth.
            assert!(heartbeat("not a URL", &capture));
        }
    }

    fn watching() -> Progress {
        Progress {
            phase: "watching".into(),
            backlog_complete: true,
            records_uploaded: 100,
            sessions_captured: 2,
            ..Default::default()
        }
    }

    #[test]
    fn idle_heartbeat_renews_expired_credentials_without_a_closing_report() {
        // Run in a child test process so synthetic credentials cannot race other
        // tests or touch the user's selected auth store.
        if std::env::var("PROBE_HEARTBEAT_TEST_CHILD").as_deref() != Ok("1") {
            let directory = tempfile::tempdir().unwrap();
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "progress::tests::idle_heartbeat_renews_expired_credentials_without_a_closing_report"])
                .env("PROBE_HEARTBEAT_TEST_CHILD", "1")
                .env("RELAYHISTORY_HOME", directory.path())
                .status().unwrap();
            assert!(status.success());
            return;
        }
        use std::io::{BufRead, BufReader, Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        cloud::save_auth(&cloud::StoredAuth {
            base_url: url.clone(),
            access_token: "rth_at_expired".into(),
            refresh_token: Some("rth_rt_test".into()),
            ..Default::default()
        })
        .unwrap();
        let server = thread::spawn(move || {
            for path in ["/v1/auth/token/refresh", "/v1/onboarding/heartbeat"] {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut reader = BufReader::new(&mut stream);
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                assert!(line.starts_with(&format!("POST {path} ")));
                let mut length = 0;
                let mut authorized = false;
                loop {
                    line.clear();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" {
                        break;
                    }
                    if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                        length = value.trim().parse::<usize>().unwrap();
                    }
                    authorized |=
                        line.to_ascii_lowercase().trim() == "authorization: bearer rth_at_fresh";
                }
                let mut body = vec![0; length];
                reader.read_exact(&mut body).unwrap();
                if path.ends_with("heartbeat") {
                    assert!(authorized);
                    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
                    assert_eq!(body["progress"]["phase"], "watching");
                    assert_eq!(body["progress"]["sessionsCaptured"], 0);
                    assert_eq!(body["progress"]["processedFiles"], 0);
                    assert_eq!(body["progress"]["source"], "");
                    assert!(body["progress"]["totalFiles"].is_null());
                }
                let body = if path.ends_with("refresh") {
                    serde_json::json!({"accessToken":"rth_at_fresh", "refreshToken":"rth_rt_fresh",
                        "accessTokenExpiresAt": (chrono::Utc::now() + chrono::Duration::hours(1)).to_rfc3339()}).to_string()
                } else {
                    "{}".into()
                };
                write!(stream, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body).unwrap();
            }
        });
        let mut schedule = HeartbeatSchedule::new();
        let now = Instant::now();
        assert!(
            schedule.report(&watching(), Duration::ZERO, false, now, || heartbeat(
                &url,
                &watching()
            ))
        );
        schedule.report(&watching(), Duration::ZERO, false, now, || {
            panic!("duplicate closing report")
        });
        server.join().unwrap();
        assert_eq!(
            cloud::load_selected_auth(Some(&url))
                .unwrap()
                .unwrap()
                .access_token,
            "rth_at_fresh"
        );
    }

    #[test]
    fn idle_cycles_share_a_deadline_and_send_about_twelve_reports_per_hour() {
        let directory = tempfile::tempdir().unwrap();
        let schedule = heartbeat_schedule(directory.path(), "http://history.test");
        assert!(Arc::ptr_eq(
            &schedule,
            &heartbeat_schedule(directory.path(), "http://history.test")
        ));
        assert!(!Arc::ptr_eq(
            &schedule,
            &heartbeat_schedule(directory.path(), "http://other.test")
        ));
        let mut state = schedule.lock().unwrap();
        assert!((270..=330).contains(&state.idle_interval.as_secs()));
        let start = Instant::now();
        let mut sent = 0;
        for seconds in (0..3600).step_by(20) {
            let now = start + Duration::from_secs(seconds);
            if seconds > 0 && seconds % 60 == 0 {
                let scan = Progress {
                    phase: "scanning".into(),
                    ..Default::default()
                };
                state.report(&scan, Duration::ZERO, false, now, || {
                    sent += 1;
                    true
                });
            }
            // Every monitor sends both an opening and closing snapshot.
            for _ in 0..2 {
                state.report(&watching(), Duration::ZERO, false, now, || {
                    sent += 1;
                    true
                });
            }
        }
        assert!((11..=14).contains(&sent), "sent {sent} idle requests");
    }

    #[test]
    fn changed_progress_is_throttled_but_completion_and_failure_are_immediate() {
        let mut state = HeartbeatSchedule::new();
        let start = Instant::now();
        let mut p = Progress {
            phase: "uploading".into(),
            ..Default::default()
        };
        let mut sent = 0;
        for seconds in 0..10 {
            p.records_uploaded = seconds;
            state.report(
                &p,
                Duration::from_secs(seconds as u64),
                false,
                start + Duration::from_secs(seconds as u64),
                || {
                    sent += 1;
                    true
                },
            );
        }
        assert_eq!(sent, 4); // 0, 3, 6, 9
        state.report(
            &watching(),
            Duration::ZERO,
            false,
            start + Duration::from_secs(9),
            || {
                sent += 1;
                true
            },
        );
        p.phase = "capture_paused".into();
        state.report(
            &p,
            Duration::ZERO,
            false,
            start + Duration::from_secs(9),
            || {
                sent += 1;
                true
            },
        );
        assert_eq!(sent, 6);
        state.report(
            &p,
            Duration::ZERO,
            false,
            start + Duration::from_secs(20),
            || panic!("unchanged failure"),
        );
    }

    #[test]
    fn unchanged_active_progress_is_suppressed_and_failed_reports_retry() {
        let mut state = HeartbeatSchedule::new();
        let start = Instant::now();
        let p = Progress {
            phase: "uploading".into(),
            ..Default::default()
        };
        assert!(!state.report(&p, Duration::ZERO, false, start, || false));
        state.report(
            &p,
            Duration::ZERO,
            false,
            start + Duration::from_secs(3),
            || panic!("retry storm"),
        );
        let mut retried = false;
        assert!(
            state.report(&p, Duration::ZERO, false, start + RETRY_INTERVAL, || {
                retried = true;
                true
            })
        );
        assert!(retried);
        state.report(
            &p,
            Duration::ZERO,
            false,
            start + Duration::from_secs(60),
            || panic!("unchanged active progress"),
        );
        let mut confirmed = false;
        assert!(state.report(
            &p,
            Duration::ZERO,
            true,
            start + Duration::from_secs(60),
            || {
                confirmed = true;
                true
            }
        ));
        assert!(confirmed); // --once/setup must still prove final connectivity.
    }

    #[test]
    fn finish_and_drop_do_not_wait_for_a_slow_reporter() {
        for success in [None, Some(false), Some(true)] {
            let directory = tempfile::tempdir().unwrap();
            let (entered, started) = mpsc::channel();
            let (release, blocked) = mpsc::channel::<()>();
            let (finished, done) = mpsc::channel();
            let mut first = true;
            let mut monitor =
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
            monitor.local = Some((
                Arc::new(LocalProgress {
                    directory: directory.path().to_owned(),
                    closed: Mutex::new(false),
                }),
                None,
            ));
            started.recv_timeout(Duration::from_secs(5)).unwrap();
            let (returned, completion) = mpsc::channel();
            let finisher = thread::spawn(move || {
                if let Some(success) = success {
                    monitor.finish(success);
                } else {
                    drop(monitor);
                }
                returned.send(()).unwrap();
            });
            // Prove finish/drop returns while the reporter is still blocked.
            // The watchdog bounds a regression, not scheduler or disk latency.
            let completed_while_blocked = completion.recv_timeout(Duration::from_secs(5));
            release.send(()).unwrap();
            finisher.join().unwrap();
            assert!(
                completed_while_blocked.is_ok(),
                "finish/drop waited for the reporter"
            );
            let final_phase = done.recv_timeout(Duration::from_secs(2));
            if success == Some(true) {
                assert_eq!(final_phase.unwrap(), "scanning");
            } else {
                assert_eq!(final_phase.unwrap(), "capture_paused");
            }
        }
    }

    #[test]
    fn capture_finish_flushes_final_counts_before_returning() {
        let directory = tempfile::tempdir().unwrap();
        let conn = rusqlite::Connection::open(directory.path().join("history.db")).unwrap();
        conn.execute_batch("CREATE TABLE sessions (discovery_state TEXT);")
            .unwrap();
        let monitor = Monitor::start(directory.path(), "not a URL", None, false);
        conn.execute_batch("INSERT INTO sessions VALUES (NULL), ('full'), ('partial');")
            .unwrap();
        monitor.observer()(ai_hist::CaptureProgress {
            source: "complete".into(),
            processed_files: 7,
            total_files: Some(7),
        });
        monitor.finish(true);
        let saved: serde_json::Value =
            serde_json::from_slice(&std::fs::read(directory.path().join("progress.json")).unwrap())
                .unwrap();
        assert_eq!(saved["progress"]["source"], "complete");
        assert_eq!(saved["progress"]["processedFiles"], 7);
        assert_eq!(saved["progress"]["totalFiles"], 7);
        assert_eq!(saved["progress"]["sessionsCaptured"], 2);
    }

    #[test]
    fn closed_local_publisher_cannot_overwrite_the_next_monitor() {
        let directory = tempfile::tempdir().unwrap();
        let old = Arc::new(LocalProgress {
            directory: directory.path().to_owned(),
            closed: Mutex::new(false),
        });
        let worker = old.clone();
        let (release, blocked) = mpsc::channel();
        let delayed = thread::spawn(move || {
            // A worker may already have read its scanning snapshot when capture
            // finishes. Let it attempt publication only after delivery starts.
            let stale = Progress {
                phase: "scanning".into(),
                ..Default::default()
            };
            blocked.recv().unwrap();
            worker.publish(&stale);
        });
        old.finish(
            Progress {
                source: "complete".into(),
                ..Default::default()
            },
            None,
            true,
        );
        let next = LocalProgress {
            directory: directory.path().to_owned(),
            closed: Mutex::new(false),
        };
        next.publish(&watching());
        release.send(()).unwrap();
        delayed.join().unwrap();
        let saved: serde_json::Value =
            serde_json::from_slice(&std::fs::read(directory.path().join("progress.json")).unwrap())
                .unwrap();
        assert_eq!(saved["progress"]["phase"], "watching");
        assert_eq!(saved["progress"]["recordsUploaded"], 100);
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

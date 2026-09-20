//! Content-free, best-effort progress. A slow/offline status endpoint never blocks capture.
use ai_hist::delivery;
use relayhistory_plugin::cloud;
use serde::Serialize;
use std::{
    path::Path,
    sync::{mpsc, Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

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
                "SELECT COUNT(*) FROM sessions WHERE discovery_state = 'full'",
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
    thread: Option<thread::JoinHandle<()>>,
}
impl Monitor {
    pub fn start(directory: &Path, history_url: &str, job: Option<&str>) -> Self {
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
        let url = history_url.to_owned();
        let job = job.map(str::to_owned);
        let (stop, receive) = mpsc::channel();
        let thread = thread::spawn(move || {
            let started = Instant::now();
            let mut finish = None;
            loop {
                let mut progress = shared.lock().unwrap().clone();
                progress.read_counts(&db, job.as_deref());
                if finish == Some(false) {
                    progress.phase = "paused".into();
                }
                println!("{} ({}s)", progress.line(), started.elapsed().as_secs());
                heartbeat(&url, &progress);
                if finish.is_some() {
                    break;
                }
                match receive.recv_timeout(Duration::from_secs(3)) {
                    Ok(success) => finish = Some(success),
                    Err(mpsc::RecvTimeoutError::Disconnected) => finish = Some(false),
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                }
            }
        });
        Self {
            snapshot,
            stop: Some(stop),
            thread: Some(thread),
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
    pub fn finish(mut self, success: bool) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(success);
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
impl Drop for Monitor {
    fn drop(&mut self) {
        self.stop.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
fn heartbeat(url: &str, progress: &Progress) {
    let Ok(token) = cloud::access_token(Some(url)) else {
        return;
    };
    let result = ureq::AgentBuilder::new()
        .redirects(0)
        .timeout(Duration::from_secs(3))
        .build()
        .post(&format!("{url}/v1/onboarding/heartbeat"))
        .set("Authorization", &format!("Bearer {token}"))
        .send_json(serde_json::json!({"progress":progress}));
    if result.is_err() {
        eprintln!("Cloud progress update unavailable; collection continues locally.");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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

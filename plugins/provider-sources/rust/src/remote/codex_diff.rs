//! Remote Codex task evidence: the diff `codex cloud diff` prints, read
//! from the CLI under a time and size bound.

use std::io::Read;

use sha2::{Digest, Sha256};

use super::*;

const REMOTE_COMMAND_TIMEOUT: Duration = Duration::from_secs(30);

pub(super) fn acquire_codex_remote_session(session_id: &str) -> Result<RemoteSessionEvidence> {
    acquire_codex_remote_session_with_command(session_id, OsStr::new("codex"))
}

pub(super) fn acquire_codex_remote_session_with_command(
    session_id: &str,
    command: &OsStr,
) -> Result<RemoteSessionEvidence> {
    acquire_codex_remote_session_with_command_timeout(session_id, command, REMOTE_COMMAND_TIMEOUT)
}

pub(super) fn acquire_codex_remote_session_with_command_timeout(
    session_id: &str,
    command: &OsStr,
    timeout: Duration,
) -> Result<RemoteSessionEvidence> {
    if !is_codex_task_id(session_id) {
        anyhow::bail!("INVALID_ARGUMENT: remote Codex task id is malformed");
    }
    let mut child = std::process::Command::new(command)
        .args(["cloud", "diff", session_id])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|error| match error.kind() {
            std::io::ErrorKind::NotFound => {
                anyhow::anyhow!("CONNECTOR_NOT_CONFIGURED: the `codex` CLI is not on PATH")
            }
            _ => anyhow::Error::from(error)
                .context("CONNECTOR_FAILURE: could not run `codex cloud diff`"),
        })?;
    let stdout = child
        .stdout
        .take()
        .context("CONNECTOR_FAILURE: no Codex stdout pipe")?;
    let stderr = child
        .stderr
        .take()
        .context("CONNECTOR_FAILURE: no Codex stderr pipe")?;
    let stdout_reader = read_bounded(Box::new(stdout));
    let stderr_reader = read_bounded(Box::new(stderr));
    let started = Instant::now();
    let status = wait_for_codex_child(&mut child, started, timeout)?;
    let receive = |reader: mpsc::Receiver<std::io::Result<Vec<u8>>>, name: &str| {
        let remaining = timeout.checked_sub(started.elapsed()).unwrap_or_default();
        reader
            .recv_timeout(remaining)
            .map_err(|_| {
                anyhow::anyhow!(
                    "CONNECTOR_FAILURE: Codex {name} pipe remained open past the 30 second timeout"
                )
            })?
            .map_err(anyhow::Error::from)
    };
    let stdout = receive(stdout_reader, "stdout")?;
    let stderr = receive(stderr_reader, "stderr")?;
    anyhow::ensure!(
        stdout.len() <= MAX_REMOTE_EVIDENCE_BYTES && stderr.len() <= MAX_REMOTE_EVIDENCE_BYTES,
        "CONNECTOR_FAILURE: `codex cloud diff` exceeded the 16 MiB response-size limit"
    );
    if !status.success() {
        return Err(codex_diff_failure(status, &stderr));
    }
    let diff = String::from_utf8(stdout)
        .context("CONNECTOR_FAILURE: `codex cloud diff` returned non-UTF-8 output")?;
    if diff.trim().is_empty() {
        return Ok(RemoteSessionEvidence::CapabilityLimited {
            code: "PROVIDER_CAPABILITY_LIMITED",
            message: "Codex exposes no transcript API and this task has no available diff"
                .to_string(),
        });
    }
    let source_stamp = format!("diff:{:x}", Sha256::digest(diff.as_bytes()));
    Ok(RemoteSessionEvidence::CodexDiff {
        source_bytes: diff.len() as i64,
        diff,
        source_stamp,
    })
}

/// Whether `session_id` has the shape of a Codex cloud task id.
fn is_codex_task_id(session_id: &str) -> bool {
    session_id.starts_with("task_")
        && session_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

/// Drain `stream` on its own thread, retaining at most one byte past
/// [`MAX_REMOTE_EVIDENCE_BYTES`], and deliver the result on the returned
/// channel.
fn read_bounded(mut stream: Box<dyn Read + Send>) -> mpsc::Receiver<std::io::Result<Vec<u8>>> {
    let (sender, receiver) = mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let mut bytes = Vec::new();
        let mut buffer = [0u8; 64 * 1024];
        let result = loop {
            match stream.read(&mut buffer) {
                Ok(0) => break Ok(bytes),
                Ok(read) => {
                    // Retain only enough to detect the limit, but keep
                    // draining so an oversized child cannot block on a
                    // full pipe and masquerade as a command timeout.
                    let remaining = (MAX_REMOTE_EVIDENCE_BYTES + 1).saturating_sub(bytes.len());
                    bytes.extend_from_slice(&buffer[..read.min(remaining)]);
                }
                Err(error) => break Err(error),
            }
        };
        let _ = sender.send(result);
    });
    receiver
}

/// Wait for `codex cloud diff` to exit, killing it once `timeout` has passed
/// since `started`.
fn wait_for_codex_child(
    child: &mut std::process::Child,
    started: Instant,
    timeout: Duration,
) -> Result<std::process::ExitStatus> {
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(status);
        }
        if started.elapsed() >= timeout {
            let _ = child.kill();
            let _ = child.wait();
            anyhow::bail!("CONNECTOR_FAILURE: `codex cloud diff` exceeded the 30 second timeout");
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

/// The connector error a failed `codex cloud diff` stands for, read from its
/// stderr.
fn codex_diff_failure(status: std::process::ExitStatus, stderr: &[u8]) -> anyhow::Error {
    let detail = excerpt_one_line(&String::from_utf8_lossy(stderr));
    let lower = detail.to_ascii_lowercase();
    if lower.contains("login") || lower.contains("unauthorized") || lower.contains("expired") {
        return anyhow::anyhow!("AUTHENTICATION_EXPIRED: Codex CLI authentication was rejected");
    }
    if lower.contains("not found") || lower.contains("no task") {
        return anyhow::anyhow!("SESSION_NOT_FOUND: remote Codex task no longer exists");
    }
    anyhow::anyhow!("CONNECTOR_FAILURE: `codex cloud diff` failed ({status}): {detail}")
}

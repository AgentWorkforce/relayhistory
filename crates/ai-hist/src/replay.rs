use crate::cloud;
use anyhow::{Context, Result};
use serde_json::Value;
use std::io::{self, Write};
use std::path::Path;
use tempfile::NamedTempFile;

pub fn run(
    session_id: &str,
    base_url: Option<&str>,
    limit: Option<usize>,
    max_content: Option<usize>,
    json: bool,
    out: Option<&Path>,
) -> Result<()> {
    let base_url = base_url
        .map(String::from)
        .unwrap_or_else(cloud::default_base_url);
    let auth = cloud::load_auth(Some(&base_url))?.context(
        "not authenticated for the selected stage — run `ai-hist login` or `ai-hist admin-mint` first",
    )?;
    let events = cloud::replay_events(&auth, session_id, limit, max_content)?;
    let body = if json {
        // Serialize once and push the newline, rather than `format!`-ing the serialized
        // string into a second one. A long session's transcript is the largest thing this
        // command holds; duplicating it wholesale to append a byte is a needless spike.
        let mut body = serde_json::to_string(&events)?;
        body.push('\n');
        body
    } else {
        render(session_id, &events)
    };
    // Finish fetching before touching the destination: an expired token on page two
    // must not overwrite an existing offline transcript with only page one.
    if let Some(path) = out {
        write_atomically(path, body.as_bytes())
            .with_context(|| format!("writing replay to {}", path.display()))?;
    } else {
        io::stdout().lock().write_all(body.as_bytes())?;
    }
    Ok(())
}

/// Write via a temporary file in the destination's own directory, then rename over it.
///
/// The same reason the fetch completes before the write: an existing transcript must not
/// be destroyed by a replay that does not finish. `fs::write` truncates first, so a full
/// disk or an interrupted process leaves a half-written file where a complete one was.
/// `rename` within a directory is atomic, so the destination only ever holds the old
/// complete transcript or the new one.
///
/// The temp file is a sibling, not in the system temp dir, because `rename` across
/// filesystems fails — and the destination is frequently on a different mount than /tmp.
fn write_atomically(path: &Path, bytes: &[u8]) -> Result<()> {
    let dir = path.parent().filter(|p| !p.as_os_str().is_empty());
    let mut temp = match dir {
        Some(dir) => NamedTempFile::new_in(dir)?,
        None => NamedTempFile::new_in(".")?,
    };
    temp.write_all(bytes)?;
    // Flush to disk before the rename: a rename that lands before the data does would
    // leave an empty file after a crash, which is exactly the loss this guards against.
    temp.as_file().sync_all()?;
    temp.persist(path).map_err(|e| e.error)?;
    Ok(())
}

fn render(session_id: &str, events: &[Value]) -> String {
    let mut output = format!(
        "Session {session_id} — {} event(s), oldest first\n",
        events.len()
    );
    if events.is_empty() {
        output.push_str("No events found.\n");
    }
    for event in events {
        let text = |field: &str| event[field].as_str().unwrap_or("");
        output.push_str(&format!(
            "\n[{}] {} / {} ({})\n",
            text("ts"),
            text("source"),
            text("kind"),
            text("eventId")
        ));
        for (field, label) in [
            ("actorName", "Actor"),
            ("actorRole", "Role"),
            ("toolName", "Tool"),
            ("taskTitle", "Task"),
        ] {
            if let Some(value) = event[field].as_str().filter(|value| !value.is_empty()) {
                output.push_str(&format!("{label}: {value}\n"));
            }
        }
        // The flag is authoritative even if content is empty or lacks the server's
        // inline suffix; a cut transcript must never look like the session ended here.
        if event["contentTruncated"].as_bool() == Some(true) {
            output.push_str("[CONTENT TRUNCATED by server maxContent; this event is incomplete]\n");
        }
        output.push_str(event["content"].as_str().unwrap_or("(no content)"));
        output.push('\n');
    }
    output
}

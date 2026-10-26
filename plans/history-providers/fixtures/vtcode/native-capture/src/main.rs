//! Released VT Code writer and independent native reader fixture oracle.
use std::path::Path;
use vtcode_exec_events::{ThreadEvent, VersionedThreadEvent};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    let [_, command, workspace, session_id, rest @ ..] = args.as_slice() else {
        return Err("usage: capture write|read WORKSPACE SESSION_ID [input.json]".into());
    };
    let workspace = Path::new(workspace);
    match command.as_str() {
        "write" => {
            let [input] = rest else {
                return Err("write needs input.json".into());
            };
            let values: Vec<serde_json::Value> = serde_json::from_slice(&std::fs::read(input)?)?;
            let store = vtcode_memory::open(workspace, session_id, 0)?;
            for value in values {
                let event: ThreadEvent = serde_json::from_value(value)?;
                store.append(&event)?;
            }
            store.flush()?;
        }
        "read" => {
            if !rest.is_empty() {
                return Err("read takes no input".into());
            }
            // Reopen after the writer process exits. This exercises the VT Code
            // manifest/index rebuild and its consistent snapshot reader.
            let store = vtcode_memory::open(workspace, session_id, 0)?;
            let mut events = Vec::new();
            let mut error = None;
            let manifest = store.visit_snapshot(|offset, bytes| {
                match serde_json::from_slice::<VersionedThreadEvent>(bytes) {
                    Ok(event) => events.push(serde_json::json!({
                        "offset": offset,
                        "event": serde_json::to_value(event.into_event()).unwrap(),
                    })),
                    Err(e) => error = Some(e.to_string()),
                }
            })?;
            if let Some(error) = error {
                return Err(error.into());
            }
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "session_id": manifest.session_id,
                    "event_count": manifest.event_count,
                    "turn_count": manifest.turn_count,
                    "events": events,
                }))?
            );
        }
        _ => return Err("expected write or read".into()),
    }
    Ok(())
}

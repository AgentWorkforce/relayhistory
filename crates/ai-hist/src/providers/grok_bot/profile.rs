//! The profile is scoped by the replica key's account slot, never by a global
//! search for a matching bot id. Unavailable optional metadata hides no history.
use super::*;
use crate::session_store::{SessionAvatar, SessionProfile};
use base64::{engine::general_purpose::STANDARD, Engine};

pub(crate) fn name(row: &Value) -> Option<String> {
    text(&row["name"], 120)
        .or_else(|| text(&row["title"], 120))
        .or_else(|| {
            ["name", "title"]
                .iter()
                .any(|key| {
                    row[*key]
                        .as_str()
                        .is_some_and(|value| value.trim().is_empty())
                })
                .then(String::new)
        })
}

fn text(value: &Value, limit: usize) -> Option<String> {
    value
        .as_str()
        .map(str::trim)
        .filter(|s| !s.is_empty() && !s.chars().any(char::is_control))
        .map(|s| s.chars().take(limit).collect())
}

pub(super) fn roster_row(path: &Path, context: &AcquisitionContext<'_>) -> Option<(Value, usize)> {
    let (slot, sid) = format::replica_identity(path)?;
    let (roster, bytes) = super::read_roster(&format::roster_path(path, &slot), context)?;
    let row = roster["value"]["rows"]
        .as_array()?
        .iter()
        .find(|row| row["id"].as_str() == Some(&sid))?
        .clone();
    Some((row, bytes))
}

pub(crate) fn read(path: &Path, session_id: &str, roots: &ProviderRoots) -> Option<SessionProfile> {
    let (_, id) = format::replica_identity(path)?;
    if id != session_id {
        return None;
    }
    let context = AcquisitionContext::new(ID, roots);
    let (row, _) = roster_row(path, &context)?;
    let shape = text(&row["avatarShape"], 64);
    let color = text(&row["avatarColor"], 64);
    let image_data_url = inline_image(&row["avatarDataUrl"]);
    let avatar =
        (shape.is_some() || color.is_some() || image_data_url.is_some()).then_some(SessionAvatar {
            shape,
            color,
            image_data_url,
        });
    let name = name(&row).filter(|name| !name.is_empty());
    (name.is_some() || avatar.is_some()).then_some(SessionProfile { name, avatar })
}

fn inline_image(value: &Value) -> Option<String> {
    let value = value.as_str().filter(|s| s.len() <= 512 * 1024)?;
    let (kind, encoded) = value.split_once(',')?;
    let bytes = STANDARD.decode(encoded).ok()?;
    let valid = match kind {
        "data:image/png;base64" => bytes.starts_with(b"\x89PNG\r\n\x1a\n"),
        "data:image/jpeg;base64" => bytes.starts_with(b"\xff\xd8\xff"),
        "data:image/gif;base64" => bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a"),
        "data:image/webp;base64" => bytes.starts_with(b"RIFF") && bytes.get(8..12) == Some(b"WEBP"),
        _ => false,
    };
    valid.then(|| value.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inline_artwork_must_be_a_local_raster_with_the_claimed_signature() {
        for value in [
            "https://example.com/avatar.gif",
            "data:image/svg+xml;base64,PHN2Zy8+",
            "data:image/png;base64,PHN2Zy8+",
            "data:image/png;base64,not-base64",
        ] {
            assert_eq!(inline_image(&Value::String(value.into())), None);
        }
        let gif = format!("data:image/gif;base64,{}", STANDARD.encode(b"GIF89a"));
        assert_eq!(inline_image(&Value::String(gif.clone())), Some(gif));
    }
}

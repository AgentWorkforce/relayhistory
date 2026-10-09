//! Splitting a task's unified diff into one patch per destination file.

pub(super) struct UnifiedPatch {
    pub(super) path: String,
    pub(super) text: String,
}

pub(super) fn split_unified_diff(diff: &str) -> Vec<UnifiedPatch> {
    let mut patches = Vec::new();
    let mut current_path: Option<String> = None;
    let mut current = String::new();
    for line in diff.lines() {
        if let Some(path) = git_diff_destination_path(line) {
            if let Some(path) = current_path.take() {
                patches.push(UnifiedPatch {
                    path,
                    text: std::mem::take(&mut current),
                });
            }
            current_path = Some(path);
        }
        if current_path.is_some() {
            current.push_str(line);
            current.push('\n');
        }
    }
    if let Some(path) = current_path {
        patches.push(UnifiedPatch {
            path,
            text: current,
        });
    }
    if patches.is_empty() && !diff.trim().is_empty() {
        patches.push(UnifiedPatch {
            path: "(task diff)".to_string(),
            text: diff.to_string(),
        });
    }
    patches
}

fn git_diff_destination_path(line: &str) -> Option<String> {
    let mut rest = line.strip_prefix("diff --git ")?;
    let _source = take_git_path(&mut rest)?;
    let destination = take_git_path(&mut rest)?;
    destination.strip_prefix("b/").map(str::to_string)
}

fn take_git_path(input: &mut &str) -> Option<String> {
    *input = input.trim_start();
    if let Some(quoted) = input.strip_prefix('"') {
        let (path, consumed) = unquote_git_path(quoted)?;
        *input = &quoted[consumed..];
        return Some(path);
    }
    let end = input.find(char::is_whitespace).unwrap_or(input.len());
    let token = input[..end].to_string();
    *input = &input[end..];
    Some(token)
}

/// The C-quoted path git wrote after an opening quote, and how many bytes of
/// `quoted` it spans through the closing quote. `None` when it never closes.
fn unquote_git_path(quoted: &str) -> Option<(String, usize)> {
    let mut bytes = Vec::new();
    let raw = quoted.as_bytes();
    let mut index = 0;
    while index < raw.len() {
        match raw[index] {
            b'"' => return Some((String::from_utf8_lossy(&bytes).into_owned(), index + 1)),
            b'\\' if index + 1 < raw.len() => {
                index = push_git_escape(raw, index + 1, &mut bytes);
            }
            byte => bytes.push(byte),
        }
        index += 1;
    }
    None
}

/// Decode the escape whose first byte after the backslash is `raw[index]`,
/// returning the index of the escape's last byte.
fn push_git_escape(raw: &[u8], mut index: usize, bytes: &mut Vec<u8>) -> usize {
    match raw[index] {
        b'n' => bytes.push(b'\n'),
        b'r' => bytes.push(b'\r'),
        b't' => bytes.push(b'\t'),
        b'0'..=b'7' => {
            let mut value = raw[index] - b'0';
            for _ in 0..2 {
                if index + 1 < raw.len() && matches!(raw[index + 1], b'0'..=b'7') {
                    index += 1;
                    value = value.saturating_mul(8).saturating_add(raw[index] - b'0');
                }
            }
            bytes.push(value);
        }
        escaped => bytes.push(escaped),
    }
    index
}

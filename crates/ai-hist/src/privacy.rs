//! Local path redaction helpers.
pub fn normalize_home_path(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let bytes = s.as_bytes();
    let lower = s.to_ascii_lowercase();
    let mut i = 0;
    while i < s.len() {
        // POSIX: /Users/<name>/  or  /home/<name>/
        let posix = ["/users/", "/home/"]
            .iter()
            .find(|p| lower[i..].starts_with(*p))
            .copied();
        if let Some(prefix) = posix {
            let plen = prefix.len();
            let after = i + plen;
            // username runs until the next '/' or end
            let end = s[after..].find('/').map(|o| after + o).unwrap_or(s.len());
            if end > after {
                out.push_str(&s[i..after]); // keep "/Users/" original case
                out.push('~');
                i = end;
                continue;
            }
        }
        // Windows: C:\Users\<name>\
        if lower[i..].starts_with("\\users\\") {
            let after = i + "\\users\\".len();
            let end = s[after..].find('\\').map(|o| after + o).unwrap_or(s.len());
            if end > after {
                out.push_str(&s[i..after]);
                out.push('~');
                i = end;
                continue;
            }
        }
        // advance one char (UTF-8 safe)
        let ch_len = utf8_len(bytes[i]);
        out.push_str(&s[i..i + ch_len]);
        i += ch_len;
    }
    out
}

fn utf8_len(b: u8) -> usize {
    match b {
        0x00..=0x7f => 1,
        0xc0..=0xdf => 2,
        0xe0..=0xef => 3,
        _ => 4,
    }
}

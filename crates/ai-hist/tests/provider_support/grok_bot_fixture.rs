//! Stage short, synthetic fixture files under Grok Bot's native base32 keys.
use serde::Deserialize;
use std::path::{Path, PathBuf};

#[derive(Deserialize)]
struct Blob {
    file: String,
    key: String,
}

pub(crate) fn persistence(home: &Path) -> PathBuf {
    #[cfg(target_os = "macos")]
    {
        home.join("Library/Application Support/Grok Bot/sand-client-persistence")
    }
    #[cfg(target_os = "windows")]
    {
        home.join("AppData/Roaming/Grok Bot/sand-client-persistence")
    }
    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    {
        home.join(".config/Grok Bot/sand-client-persistence")
    }
}

pub(crate) fn encode_key(key: &str) -> String {
    const ALPHABET: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";
    let mut out = String::new();
    let mut bits = 0u32;
    let mut width = 0u8;
    for byte in key.bytes() {
        bits = (bits << 8) | u32::from(byte);
        width += 8;
        while width >= 5 {
            width -= 5;
            out.push(ALPHABET[((bits >> width) & 31) as usize] as char);
            bits &= (1 << width) - 1;
        }
    }
    if width > 0 {
        out.push(ALPHABET[(bits << (5 - width)) as usize] as char);
    }
    out
}

pub(crate) fn stage(home: &Path) -> PathBuf {
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/grok-bot/replicas");
    let manifest: Vec<Blob> = serde_json::from_slice(
        &std::fs::read(fixtures.join("manifest.json")).expect("Grok Bot manifest"),
    )
    .expect("Grok Bot blob list");
    let target = persistence(home);
    std::fs::create_dir_all(&target).expect("Grok Bot persistence directory");
    let mut complete = None;
    for blob in manifest {
        let path = target.join(format!("{}.blob", encode_key(&blob.key)));
        std::fs::copy(fixtures.join(&blob.file), &path).expect("stage Grok Bot blob");
        if blob
            .key
            .ends_with(".transcript.replicas.fixture-bot-complete")
        {
            complete = Some(path);
        }
    }
    complete.expect("complete Grok Bot replica")
}

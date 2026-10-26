//! The Rust scrubber must reproduce every golden vector generated from the
//! reference `scrubText` (relayhistory-cloud
//! `packages/relayhistory/test/scrub-golden.test.ts`) byte for byte.
//! Refresh the fixture by copying it from there; never regenerate it here.

use std::time::{Duration, Instant};

use relayhistory_object_store::scrub::{scrub_text, MAX_SCRUB_CHARS};
use serde::Deserialize;
use sha2::{Digest, Sha256};

#[derive(Deserialize)]
struct Fixture {
    version: u32,
    ruleset: String,
    max_scrub_chars: usize,
    vectors: Vec<Vector>,
}

#[derive(Deserialize)]
/// Inputs and outputs are masked in the fixture (UTF-8 bytes XOR 0x5A, then
/// base64) so it never holds a secret-shaped string, even base64-encoded:
/// push protection rejects fake test credentials in both forms.
struct Vector {
    id: String,
    input_masked: Option<String>,
    generate: Option<Generated>,
    output_masked: Option<String>,
    output_sha256: Option<String>,
    output_utf16_len: Option<usize>,
    /// The sensitive part: no 6-character window of it may survive.
    secret_masked: Option<String>,
}

/// `prefix + unit * count + suffix`, or the concatenation of each masked
/// `[unit, count]` in `parts_masked`.
#[derive(Deserialize)]
struct Generated {
    prefix: Option<String>,
    unit: Option<String>,
    count: Option<usize>,
    suffix: Option<String>,
    parts_masked: Option<Vec<(String, usize)>>,
}

impl Generated {
    fn expand(&self) -> String {
        if let Some(parts) = &self.parts_masked {
            return parts.iter().map(|(u, n)| unmask(u).repeat(*n)).collect();
        }
        format!(
            "{}{}{}",
            self.prefix.as_deref().unwrap_or(""),
            self.unit
                .as_deref()
                .unwrap_or("")
                .repeat(self.count.unwrap_or(0)),
            self.suffix.as_deref().unwrap_or("")
        )
    }
}

fn fixture() -> Fixture {
    let raw = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/scrub-golden-v2.json"
    ))
    .expect("fixture");
    serde_json::from_str(&raw).expect("fixture parses")
}

#[test]
fn every_golden_vector_matches_the_reference() {
    let f = fixture();
    assert_eq!(f.version, relayhistory_object_store::scrub::RULESET_VERSION);
    assert_eq!(f.ruleset, relayhistory_object_store::scrub::RULESET);
    assert_eq!(f.max_scrub_chars, MAX_SCRUB_CHARS);
    let mut failures = Vec::new();
    for v in &f.vectors {
        let output = v.output_masked.as_deref().map(unmask);
        let input = match (&v.input_masked, &v.generate) {
            (Some(i), _) => unmask(i),
            (None, Some(g)) => g.expand(),
            _ => panic!("{}: no input", v.id),
        };
        let started = Instant::now();
        let got = scrub_text(&input);
        let elapsed = started.elapsed();
        let ok = match (&output, &v.output_sha256) {
            (Some(expected), _) => &got == expected,
            (None, Some(sha)) => {
                hex::encode(Sha256::digest(got.as_bytes())) == *sha
                    && Some(got.encode_utf16().count()) == v.output_utf16_len
            }
            _ => panic!("{}: no expectation", v.id),
        };
        if let Some(window) = v
            .secret_masked
            .as_deref()
            .map(unmask)
            .and_then(|secret| leaked(&got, &secret))
        {
            failures.push(format!("{}: fragment {window:?} survived", v.id));
        }
        if !ok {
            failures.push(format!(
                "{}: input {:?}\n    expected {:?}\n    got      {:?}",
                v.id,
                truncate(&input),
                output.as_deref().map(truncate),
                truncate(&got)
            ));
        }
        // ReDoS guard: every vector, including the 128 KB pathological ones,
        // stays far from quadratic time even in a debug build.
        assert!(
            elapsed < Duration::from_secs(20),
            "{} took {elapsed:?}",
            v.id
        );
    }
    assert!(
        failures.is_empty(),
        "{} of {} vectors differ:\n{}",
        failures.len(),
        f.vectors.len(),
        failures.join("\n")
    );
}

/// The first 6-character window of `secret` (without a newline) found in `out`.
fn leaked(out: &str, secret: &str) -> Option<String> {
    let chars: Vec<char> = secret.chars().collect();
    chars
        .windows(6)
        .map(|w| w.iter().collect::<String>())
        .find(|w| !w.contains('\n') && out.contains(w.as_str()))
}

fn unmask(masked: &str) -> String {
    use base64::Engine as _;
    let bytes: Vec<u8> = base64::engine::general_purpose::STANDARD
        .decode(masked)
        .expect("fixture base64")
        .into_iter()
        .map(|b| b ^ 0x5A)
        .collect();
    String::from_utf8(bytes).expect("fixture UTF-8")
}

fn truncate(s: &str) -> String {
    if s.chars().count() > 160 {
        format!(
            "{}…({} chars)",
            s.chars().take(160).collect::<String>(),
            s.chars().count()
        )
    } else {
        s.to_string()
    }
}

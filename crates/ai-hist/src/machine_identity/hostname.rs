//! The canonical hostname rule and this host's reported name.
use icu_properties::props::{GeneralCategory, GeneralCategoryGroup};
use icu_properties::CodePointMapData;
use sha2::{Digest, Sha256};

/// The version of the [`canonical_hostname`] rule. `machine_name` and every
/// `hostname_aliases` row record the version they were computed with, so a
/// later rule change recanonicalizes them on the next writable open (the
/// machine id stays frozen) instead of leaving old and new names mixed.
///
/// - `1`: apostrophes became hyphens; only the first DNS label was kept.
/// - `2`: apostrophes, straight and curly, are dropped, so macOS's
///   ComputerName (`Khaliq's MacBook Pro`) and LocalHostName
///   (`Khaliqs-MacBook-Pro`) are one name.
/// - `3`: only network suffixes are stripped ([`NETWORK_SUFFIXES`]); every
///   other dot is a separator, so `khaliqs.macbook.pro` keeps all three
///   labels. A `*.internal` cloud name keeps its first label.
/// - `4`: a label with non-ASCII letters becomes its IDNA Punycode (`xn--`)
///   form after NFC normalization, instead of being dropped, so non-ASCII
///   names stay distinct (they all collapsed to [`UNKNOWN_HOSTNAME`] under
///   rule 3) and decode back; combining marks stay on their letters.
pub const CANONICAL_HOSTNAME_RULE_VERSION: u32 = 4;

/// The canonical hostname of a name with no usable character.
pub const UNKNOWN_HOSTNAME: &str = "unknown-host";

/// Longest canonical hostname: one DNS label.
const MAX_HOSTNAME_LEN: usize = 63;

/// The apostrophes [`canonical_hostname`] drops rather than hyphenates.
const APOSTROPHES: &[char] = &['\'', '\u{2018}', '\u{2019}'];

/// Suffixes that name a network rather than a machine: mDNS (`local`), home
/// routers (`lan`, `home`, `home.arpa`), the resolver default
/// (`localdomain`) and cloud-internal DNS (`internal`). Stripped, repeatedly,
/// and only after a dot, so a bare `local` stays a name.
const NETWORK_SUFFIXES: &[&str] = &[
    "home.arpa",
    "localdomain",
    "internal",
    "local",
    "home",
    "lan",
];

/// The canonical hostname: one deterministic spelling per machine name.
///
/// Lowercased, NFC-normalized and trimmed of whitespace and trailing dots;
/// every [`NETWORK_SUFFIXES`] entry stripped; for a cloud `*.internal` name
/// only the first label kept, since the rest names a zone or project. Then
/// one label of the rest, as IDNA would write it: each `xn--` label decoded
/// to Unicode, apostrophes (`'`, `‘`, `’`) dropped, every run of anything but
/// letters, digits and combining marks -- dots, punctuation, spaces,
/// underscores, symbols -- one `-`, and the result, when it has non-ASCII
/// letters, written whole in Punycode behind `xn--`. `café-box`, `café.box`
/// and the IDNA spelling `xn--caf-box-dya` are therefore one name, and a
/// canonical name canonicalizes to itself. At most 63 characters: an ASCII
/// name is cut from the right, so the most specific labels stay; Punycode
/// does not survive a cut, so a non-ASCII name too long to encode is named
/// by a hash. A name with nothing left is [`UNKNOWN_HOSTNAME`].
///
/// `Khaliqs-MacBook-Pro.local` -> `khaliqs-macbook-pro`;
/// `Khaliq's MacBook Pro` -> `khaliqs-macbook-pro`;
/// `khaliqs.macbook.pro` -> `khaliqs-macbook-pro`;
/// `ip-10-0-0-1.ec2.internal` -> `ip-10-0-0-1`;
/// `build_01.corp.example.com` -> `build-01-corp-example-com`;
/// `Café-Box` -> `xn--caf-box-dya`.
pub fn canonical_hostname(hostname: &str) -> String {
    let mut host = nfc(&hostname.trim().trim_end_matches('.').to_lowercase());
    let mut cloud_internal = false;
    while let Some(suffix) = NETWORK_SUFFIXES.iter().find(|suffix| {
        host.len() > suffix.len() + 1
            && host.ends_with(*suffix)
            && host.as_bytes()[host.len() - suffix.len() - 1] == b'.'
    }) {
        cloud_internal |= *suffix == "internal";
        host.truncate(host.len() - suffix.len() - 1);
        host.truncate(host.trim_end_matches('.').len());
    }
    let name = if cloud_internal {
        host.split('.').next().unwrap_or_default()
    } else {
        host.as_str()
    };
    slug(name)
}

fn slug(name: &str) -> String {
    // Every `xn--` label back to Unicode first, so a name and its IDNA
    // spelling meet; then the whole name is one label, its dots separators
    // like any other punctuation.
    let unicode = text(
        &name
            .split('.')
            .map(decode_idna)
            .collect::<Vec<_>>()
            .join("-"),
    );
    if unicode.is_empty() {
        return UNKNOWN_HOSTNAME.to_string();
    }
    if unicode.is_ascii() {
        return unicode[..unicode.len().min(MAX_HOSTNAME_LEN)]
            .trim_end_matches('-')
            .to_string();
    }
    // One Punycode label, so the result is decoded back by the same rule:
    // canonicalizing a canonical name returns it unchanged. Punycode does
    // not survive a cut, so a name too long to encode is named by a hash.
    match idna::punycode::encode_str(&unicode).map(|encoded| format!("xn--{encoded}")) {
        Some(ascii) if ascii.len() <= MAX_HOSTNAME_LEN => ascii,
        _ => overlong_label(&unicode),
    }
}

/// `label` with an IDNA `xn--` prefix decoded to lowercase NFC Unicode; any
/// other label, or one that does not decode, as it is.
fn decode_idna(label: &str) -> String {
    label
        .strip_prefix("xn--")
        .and_then(idna::punycode::decode_to_string)
        .map_or_else(|| label.to_string(), |unicode| nfc(&unicode.to_lowercase()))
}

/// The name's text: apostrophes dropped, letters, digits and combining marks
/// kept, every other run one `-`, no leading or trailing `-`.
fn text(name: &str) -> String {
    let mut text = String::with_capacity(name.len());
    for ch in name.chars() {
        if APOSTROPHES.contains(&ch) {
            continue;
        }
        if is_word_char(ch) {
            text.push(ch);
        } else if !text.is_empty() && !text.ends_with('-') {
            text.push('-');
        }
    }
    text.trim_end_matches('-').to_string()
}

fn nfc(text: &str) -> String {
    icu_normalizer::ComposingNormalizer::new_nfc()
        .normalize(text)
        .into_owned()
}

/// The name of a non-ASCII hostname whose Punycode is longer than a
/// hostname may be: `h-` and 16 hex digits of a SHA-256 of its text.
/// Bounded and deterministic, so the same overlong name always maps to the
/// same canonical name; it cannot be decoded, which only a name longer than
/// DNS allows gives up.
fn overlong_label(label: &str) -> String {
    let digest = Sha256::digest(format!("relayhistory-overlong-hostname-v1:{label}"));
    format!("h-{}", &format!("{digest:x}")[..16])
}

/// Whether `ch` is part of a label's text: a letter or digit, or a combining
/// mark (general category M), so a Devanagari vowel sign stays on its letter
/// rather than splitting it -- `की` and `कु` are different names.
fn is_word_char(ch: char) -> bool {
    ch.is_alphanumeric()
        || GeneralCategoryGroup::Mark.contains(CodePointMapData::<GeneralCategory>::new().get(ch))
}

/// This host's name as the OS reports it, without spawning a process.
pub(crate) fn local_hostname() -> String {
    #[cfg(unix)]
    {
        rustix::system::uname()
            .nodename()
            .to_string_lossy()
            .into_owned()
    }
    #[cfg(not(unix))]
    {
        std::env::var("COMPUTERNAME").unwrap_or_default()
    }
}

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
/// each DNS label on its own, as IDNA treats one: an `xn--` label decoded to
/// Unicode, apostrophes (`'`, `‘`, `’`) dropped, every run of anything but
/// letters, digits and combining marks -- punctuation, spaces, underscores,
/// symbols -- one `-`, and a label with non-ASCII letters written whole in
/// Punycode behind `xn--`, so `café-box` and its IDNA spelling
/// `xn--caf-box-dya` are one name. The labels are joined by `-`, at most 63
/// characters, cut from the right so the most specific labels stay, and
/// never inside an `xn--` label (a lone label too long for the limit is
/// named by a hash). A name with nothing left is [`UNKNOWN_HOSTNAME`].
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
    let mut slug = String::with_capacity(MAX_HOSTNAME_LEN);
    for (unicode, ascii) in name.split('.').filter_map(label) {
        let room = MAX_HOSTNAME_LEN.saturating_sub(slug.len() + usize::from(!slug.is_empty()));
        if ascii.len() > room {
            // Punycode does not survive a cut: an `xn--` label is kept whole
            // or not at all, and one too long even alone is named by a
            // hash. A plain label is cut, keeping its leading characters.
            if !ascii.starts_with("xn--") {
                push_label(&mut slug, ascii[..room].trim_end_matches('-'));
            } else if slug.is_empty() {
                slug.push_str(&overlong_label(&unicode));
            }
            break;
        }
        push_label(&mut slug, &ascii);
    }
    if slug.is_empty() {
        UNKNOWN_HOSTNAME.to_string()
    } else {
        slug
    }
}

/// One DNS label as the rule names it, Unicode and ASCII: an `xn--` label
/// decoded back to Unicode first, so a name and its IDNA spelling meet;
/// apostrophes dropped; every run of anything but letters, digits and marks
/// one `-`, trimmed; then the whole label in Punycode behind `xn--` when it
/// has non-ASCII letters, exactly as IDNA encodes a label. `None` when
/// nothing is left.
fn label(raw: &str) -> Option<(String, String)> {
    let decoded = raw
        .strip_prefix("xn--")
        .and_then(idna::punycode::decode_to_string)
        .map(|unicode| nfc(&unicode.to_lowercase()));
    let raw = decoded.as_deref().unwrap_or(raw);
    let mut unicode = String::with_capacity(raw.len());
    for ch in raw.chars() {
        if APOSTROPHES.contains(&ch) {
            continue;
        }
        if is_word_char(ch) {
            unicode.push(ch);
        } else if !unicode.is_empty() && !unicode.ends_with('-') {
            unicode.push('-');
        }
    }
    let unicode = unicode.trim_end_matches('-').to_string();
    if unicode.is_empty() {
        return None;
    }
    let ascii = if unicode.is_ascii() {
        unicode.clone()
    } else {
        format!("xn--{}", idna::punycode::encode_str(&unicode)?)
    };
    Some((unicode, ascii))
}

fn nfc(text: &str) -> String {
    icu_normalizer::ComposingNormalizer::new_nfc()
        .normalize(text)
        .into_owned()
}

fn push_label(slug: &mut String, label: &str) {
    if label.is_empty() {
        return;
    }
    if !slug.is_empty() {
        slug.push('-');
    }
    slug.push_str(label);
}

/// The name of a non-ASCII label whose Punycode alone is longer than a
/// hostname may be: `h-` and 16 hex digits of a SHA-256 of the label.
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

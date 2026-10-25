//! The canonical hostname rule and this host's reported name.

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
/// - `4`: a word with non-ASCII letters becomes its Punycode (`xn--`) form
///   after NFC normalization, instead of being dropped, so non-ASCII names
///   stay distinct (they all collapsed to [`UNKNOWN_HOSTNAME`] under rule 3)
///   and the original can be decoded back.
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
/// only the first label kept, since the rest names a zone or project;
/// apostrophes (`'`, `‘`, `’`) dropped; split into words on every run of
/// anything but letters and digits -- dots, punctuation, spaces,
/// underscores, symbols -- and the words joined by one `-`, a word with
/// non-ASCII letters in its Punycode (`xn--`) form; cut at 63 characters
/// from the right, so the most specific labels stay. A name with nothing
/// left is [`UNKNOWN_HOSTNAME`].
///
/// `Khaliqs-MacBook-Pro.local` -> `khaliqs-macbook-pro`;
/// `Khaliq's MacBook Pro` -> `khaliqs-macbook-pro`;
/// `khaliqs.macbook.pro` -> `khaliqs-macbook-pro`;
/// `ip-10-0-0-1.ec2.internal` -> `ip-10-0-0-1`;
/// `build_01.corp.example.com` -> `build-01-corp-example-com`;
/// `Ñandú-Box` -> `xn--and-6ma2c-box`.
pub fn canonical_hostname(hostname: &str) -> String {
    let lower = hostname.trim().trim_end_matches('.').to_lowercase();
    let mut host = icu_normalizer::ComposingNormalizer::new_nfc()
        .normalize(&lower)
        .into_owned();
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
    let mut slug = String::with_capacity(name.len());
    let mut words = words(name).into_iter().peekable();
    while let Some((word, separator)) = words.next() {
        // An IDNA `xn--` prefix is one word: its `--` is not two separators.
        let word = if word == "xn" && separator == "--" {
            match words.next() {
                Some((rest, _)) => format!("xn--{rest}"),
                None => word,
            }
        } else {
            word
        };
        let Some(word) = ascii_word(word) else {
            continue;
        };
        if !slug.is_empty() {
            slug.push('-');
        }
        slug.push_str(&word);
    }
    let slug = slug[..slug.len().min(MAX_HOSTNAME_LEN)].trim_end_matches('-');
    if slug.is_empty() {
        UNKNOWN_HOSTNAME.to_string()
    } else {
        slug.to_string()
    }
}

/// `name` split into its words -- runs of letters and digits, apostrophes
/// dropped -- each with the separator run that follows it.
fn words(name: &str) -> Vec<(String, String)> {
    let mut words: Vec<(String, String)> = Vec::new();
    let mut word = String::new();
    let mut separator = String::new();
    for ch in name.chars() {
        if APOSTROPHES.contains(&ch) {
            continue;
        }
        if ch.is_alphanumeric() {
            if !separator.is_empty() && !word.is_empty() {
                words.push((std::mem::take(&mut word), std::mem::take(&mut separator)));
            }
            separator.clear();
            word.push(ch);
        } else {
            separator.push(ch);
        }
    }
    if !word.is_empty() {
        words.push((word, separator));
    }
    words
}

/// `word` as ASCII: itself, or its Punycode form behind the IDNA `xn--`
/// prefix when it has non-ASCII letters. `None` only for a word too long for
/// Punycode to encode.
fn ascii_word(word: String) -> Option<String> {
    if word.is_ascii() {
        return Some(word);
    }
    idna::punycode::encode_str(&word).map(|encoded| format!("xn--{encoded}"))
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

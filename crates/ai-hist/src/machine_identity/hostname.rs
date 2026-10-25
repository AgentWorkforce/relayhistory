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
pub const CANONICAL_HOSTNAME_RULE_VERSION: u32 = 3;

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
/// Lowercased and trimmed of whitespace and trailing dots; every
/// [`NETWORK_SUFFIXES`] entry stripped; for a cloud `*.internal` name only
/// the first label kept, since the rest names a zone or project; apostrophes
/// (`'`, `‘`, `’`) dropped; every other run of anything but `[a-z0-9]` --
/// dots, punctuation, spaces, underscores, non-ASCII -- one `-`; leading and
/// trailing hyphens trimmed; cut at 63 characters from the right, so the
/// most specific labels stay. A name with nothing left is
/// [`UNKNOWN_HOSTNAME`].
///
/// `Khaliqs-MacBook-Pro.local` -> `khaliqs-macbook-pro`;
/// `Khaliq's MacBook Pro` -> `khaliqs-macbook-pro`;
/// `khaliqs.macbook.pro` -> `khaliqs-macbook-pro`;
/// `ip-10-0-0-1.ec2.internal` -> `ip-10-0-0-1`;
/// `build_01.corp.example.com` -> `build-01-corp-example-com`.
pub fn canonical_hostname(hostname: &str) -> String {
    let mut host = hostname.trim().trim_end_matches('.').to_ascii_lowercase();
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
    for ch in name.chars() {
        if APOSTROPHES.contains(&ch) {
            continue;
        }
        if ch.is_ascii_alphanumeric() {
            slug.push(ch);
        } else if !slug.is_empty() && !slug.ends_with('-') {
            slug.push('-');
        }
    }
    let slug = slug[..slug.len().min(MAX_HOSTNAME_LEN)].trim_end_matches('-');
    if slug.is_empty() {
        UNKNOWN_HOSTNAME.to_string()
    } else {
        slug.to_string()
    }
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

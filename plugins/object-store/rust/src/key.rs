use std::fmt;

use crate::{Error, Result};

/// Longest key accepted. S3 and GCS cap keys at 1024 bytes.
const MAX_KEY_BYTES: usize = 1024;

/// A validated object key: `/`-separated, non-empty segments of printable
/// ASCII with no `.`/`..` segment and no leading `.` on the first segment
/// (that namespace is reserved for adapter bookkeeping, e.g. `FsStore`'s
/// `.rh/`).
///
/// The same rules hold on every backend so a key that works against the
/// filesystem works against S3, GCS and Azure unchanged.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectKey(String);

impl ObjectKey {
    pub fn new(key: impl Into<String>) -> Result<Self> {
        let key = key.into();
        validate(&key)?;
        Ok(Self(key))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Append one or more `/`-separated segments.
    pub fn join(&self, rest: &str) -> Result<Self> {
        Self::new(format!("{}/{}", self.0, rest))
    }

    /// The final segment.
    pub fn file_name(&self) -> &str {
        self.0.rsplit('/').next().unwrap_or(&self.0)
    }

    pub(crate) fn to_path(&self) -> object_store::path::Path {
        // Validated keys contain no character `Path::from` would encode, so
        // this is the identity mapping.
        object_store::path::Path::from(self.0.as_str())
    }

    pub(crate) fn from_path(path: &object_store::path::Path) -> Result<Self> {
        Self::new(path.as_ref())
    }
}

fn validate(key: &str) -> Result<()> {
    let invalid = |reason: &str| Err(Error::InvalidKey(format!("{key:?}: {reason}")));
    if key.is_empty() {
        return invalid("empty");
    }
    if key.len() > MAX_KEY_BYTES {
        return invalid("longer than 1024 bytes");
    }
    if key.starts_with('.') {
        return invalid("leading `.` is reserved");
    }
    for segment in key.split('/') {
        if segment.is_empty() {
            return invalid("empty segment (leading, trailing or doubled `/`)");
        }
        if segment == "." || segment == ".." {
            return invalid("`.` or `..` segment");
        }
    }
    // `\` and the characters object_store percent-encodes are rejected so a key
    // means the same object on every backend and on every OS.
    if let Some(c) = key
        .chars()
        .find(|c| !c.is_ascii_graphic() || matches!(c, '\\' | '%' | '*' | '?' | '#' | '[' | ']'))
    {
        return invalid(&format!("character {c:?} is not allowed"));
    }
    Ok(())
}

impl fmt::Display for ObjectKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for ObjectKey {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for ObjectKey {
    type Error = Error;
    fn try_from(value: &str) -> Result<Self> {
        Self::new(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_layout_keys() {
        let key =
            "acme/v1/ws=w1/m=m1/h=claude/d=2026/10/25/s=abc/raw/0000000001-0-deadbeef.jsonl.zst";
        assert_eq!(ObjectKey::new(key).unwrap().as_str(), key);
        assert_eq!(
            ObjectKey::new(key).unwrap().file_name(),
            "0000000001-0-deadbeef.jsonl.zst"
        );
    }

    #[test]
    fn rejects_ambiguous_keys() {
        for bad in [
            "", "/a", "a/", "a//b", "a/./b", "a/../b", ".rh/x", "a b", "a\\b", "a%20b", "a\nb", "é",
        ] {
            assert!(ObjectKey::new(bad).is_err(), "{bad:?} should be rejected");
        }
        assert!(ObjectKey::new("x".repeat(1025)).is_err());
    }

    #[test]
    fn join_validates() {
        let base = ObjectKey::new("a/b").unwrap();
        assert_eq!(base.join("c/d").unwrap().as_str(), "a/b/c/d");
        assert!(base.join("../c").is_err());
    }
}

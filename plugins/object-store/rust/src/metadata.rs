//! User-metadata validation and its mapping onto `object_store` attributes.

use object_store::{Attribute, AttributeValue, Attributes};

use crate::{Error, Metadata, Result, META_CONTENT_SHA256};

/// S3's limit on the total size of user metadata, the strictest of the three.
const MAX_METADATA_BYTES: usize = 2048;

/// Room the adapter keeps for its own `content-sha256` entry (key plus a
/// 64-character hex digest), so caller metadata that validates still fits on
/// S3 once the digest is added.
const RESERVED_DIGEST_BYTES: usize = META_CONTENT_SHA256.len() + 64;

/// Largest total caller metadata accepted.
pub(crate) const MAX_USER_METADATA_BYTES: usize = MAX_METADATA_BYTES - RESERVED_DIGEST_BYTES;

/// Validate user metadata and add the content digest.
pub(crate) fn with_digest(metadata: Metadata, sha: &str) -> Result<Metadata> {
    let mut metadata = checked_user_metadata(metadata)?;
    metadata.insert(META_CONTENT_SHA256.into(), sha.into());
    Ok(metadata)
}

pub(crate) fn checked_user_metadata(metadata: Metadata) -> Result<Metadata> {
    if metadata.contains_key(META_CONTENT_SHA256) {
        return Err(Error::InvalidArgument(format!(
            "`{META_CONTENT_SHA256}` metadata is set by the adapter; pass `content_sha256` instead"
        )));
    }
    validate_metadata(metadata)
}

/// Metadata must survive every backend unchanged: S3 lowercases keys, Azure
/// requires C# identifiers (handled by `Backend::META_SEPARATOR`, which is why
/// `_` is not allowed here), and S3 caps the total at 2 KiB, which includes the
/// adapter's own digest entry.
pub(crate) fn validate_metadata(metadata: Metadata) -> Result<Metadata> {
    let mut total = 0;
    for (k, v) in &metadata {
        let key_ok = !k.is_empty()
            && k.len() <= 128
            && k.starts_with(|c: char| c.is_ascii_lowercase())
            && k.chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-');
        if !key_ok {
            return Err(Error::InvalidArgument(format!(
                "metadata key {k:?} must match [a-z][a-z0-9-]*"
            )));
        }
        if !v.chars().all(|c| c.is_ascii_graphic() || c == ' ') {
            return Err(Error::InvalidArgument(format!(
                "metadata value for {k:?} must be printable ASCII"
            )));
        }
        total += k.len() + v.len();
    }
    if total > MAX_USER_METADATA_BYTES {
        return Err(Error::InvalidArgument(format!(
            "metadata is {total} bytes; the limit is {MAX_USER_METADATA_BYTES} \
             ({MAX_METADATA_BYTES} minus {RESERVED_DIGEST_BYTES} reserved for `{META_CONTENT_SHA256}`)"
        )));
    }
    Ok(metadata)
}

pub(crate) fn attributes(metadata: &Metadata, content_type: &str, separator: char) -> Attributes {
    let mut attrs = Attributes::new();
    attrs.insert(
        Attribute::ContentType,
        AttributeValue::from(content_type.to_string()),
    );
    for (k, v) in metadata {
        attrs.insert(
            Attribute::Metadata(k.replace('-', &separator.to_string()).into()),
            AttributeValue::from(v.clone()),
        );
    }
    attrs
}

pub(crate) fn user_metadata(attrs: &Attributes, separator: char) -> Metadata {
    attrs
        .iter()
        .filter_map(|(attr, value)| match attr {
            Attribute::Metadata(k) => Some((
                k.to_ascii_lowercase().replace(separator, "-"),
                value.as_ref().to_string(),
            )),
            _ => None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sized(total: usize) -> Metadata {
        // key "k" (1 byte) plus a value making up the rest.
        [("k".to_string(), "v".repeat(total - 1))].into()
    }

    #[test]
    fn caller_metadata_leaves_room_for_the_digest() {
        let sha = "a".repeat(64);
        let fits = with_digest(sized(MAX_USER_METADATA_BYTES), &sha).unwrap();
        let total: usize = fits.iter().map(|(k, v)| k.len() + v.len()).sum();
        assert_eq!(
            total, MAX_METADATA_BYTES,
            "digest fills the reserve exactly"
        );
        assert!(with_digest(sized(MAX_USER_METADATA_BYTES + 1), &sha).is_err());
    }
}

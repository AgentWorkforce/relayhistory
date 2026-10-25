use bytes::Bytes;
use sha2::{Digest, Sha256};

/// `Content-Type` for zstd-compressed raw chunks.
pub const CONTENT_TYPE_ZSTD: &str = "application/zstd";

/// zstd level for raw chunks: level 3 (the zstd default) is ~3-5x on JSONL
/// session logs at a few hundred MB/s, which keeps compression far below
/// network cost.
const ZSTD_LEVEL: i32 = 3;

/// Lowercase hex SHA-256.
pub fn sha256_hex(body: &[u8]) -> String {
    hex::encode(Sha256::digest(body))
}

/// Compress a raw chunk body. Raw bytes are stored unchanged inside the zstd
/// frame; the object is *not* sent with `Content-Encoding: zstd`, so presigned
/// downloads return the exact `.zst` object instead of being transparently
/// decoded by HTTP clients.
pub fn compress_zstd(raw: &[u8]) -> std::io::Result<Bytes> {
    zstd::bulk::compress(raw, ZSTD_LEVEL).map(Bytes::from)
}

pub fn decompress_zstd(compressed: &[u8]) -> std::io::Result<Vec<u8>> {
    zstd::stream::decode_all(compressed)
}

pub(crate) fn infer_content_type(key: &str) -> &'static str {
    if key.ends_with(".zst") {
        CONTENT_TYPE_ZSTD
    } else if key.ends_with(".json") {
        "application/json"
    } else {
        "application/octet-stream"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zstd_round_trips() {
        let raw = b"{\"type\":\"user\"}\n".repeat(100);
        let packed = compress_zstd(&raw).unwrap();
        assert!(packed.len() < raw.len());
        assert_eq!(decompress_zstd(&packed).unwrap(), raw);
    }

    #[test]
    fn sha256_is_lowercase_hex() {
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}

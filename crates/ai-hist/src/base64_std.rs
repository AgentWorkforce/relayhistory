//! Standard RFC 4648 base64 encode/decode and raster data-URL header checks.

const ENCODE_TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

const DECODE_TABLE: [u8; 256] = {
    let mut table = [0xFFu8; 256];
    let mut i = 0;
    while i < 64 {
        table[ENCODE_TABLE[i] as usize] = i as u8;
        i += 1;
    }
    table
};

pub(crate) fn encode(input: &[u8]) -> String {
    let mut out = Vec::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = *chunk.get(1).unwrap_or(&0) as u32;
        let b2 = *chunk.get(2).unwrap_or(&0) as u32;
        let triple = (b0 << 16) | (b1 << 8) | b2;
        out.push(ENCODE_TABLE[((triple >> 18) & 0x3F) as usize]);
        out.push(ENCODE_TABLE[((triple >> 12) & 0x3F) as usize]);
        out.push(if chunk.len() > 1 {
            ENCODE_TABLE[((triple >> 6) & 0x3F) as usize]
        } else {
            b'='
        });
        out.push(if chunk.len() > 2 {
            ENCODE_TABLE[(triple & 0x3F) as usize]
        } else {
            b'='
        });
    }
    String::from_utf8(out).expect("base64 table is ASCII")
}

pub(crate) fn decode(input: &str) -> Option<Vec<u8>> {
    let bytes = input.as_bytes();
    if !bytes.len().is_multiple_of(4) {
        return None;
    }
    if bytes.is_empty() {
        return Some(Vec::new());
    }
    let mut out = Vec::with_capacity((bytes.len() / 4) * 3);
    let last_chunk = bytes.len() / 4 - 1;
    for (idx, chunk) in bytes.as_chunks::<4>().0.iter().enumerate() {
        let a = DECODE_TABLE[chunk[0] as usize];
        let b = DECODE_TABLE[chunk[1] as usize];
        if a == 0xFF || b == 0xFF {
            return None;
        }
        let is_last = idx == last_chunk;
        if is_last && chunk[2] == b'=' && chunk[3] == b'=' {
            if b & 0x0F != 0 {
                return None;
            }
            out.push((a << 2) | (b >> 4));
            continue;
        }
        let c = DECODE_TABLE[chunk[2] as usize];
        if c == 0xFF {
            return None;
        }
        if is_last && chunk[3] == b'=' {
            if c & 0x03 != 0 {
                return None;
            }
            out.push((a << 2) | (b >> 4));
            out.push((b << 4) | (c >> 2));
            continue;
        }
        let d = DECODE_TABLE[chunk[3] as usize];
        if d == 0xFF {
            return None;
        }
        out.push((a << 2) | (b >> 4));
        out.push((b << 4) | (c >> 2));
        out.push((c << 6) | d);
    }
    Some(out)
}

pub(crate) fn is_valid_raster_image(header: &str, bytes: &[u8]) -> bool {
    match header {
        "data:image/png;base64" => bytes.starts_with(b"\x89PNG\r\n\x1a\n"),
        "data:image/jpeg;base64" => bytes.starts_with(b"\xff\xd8\xff"),
        "data:image/gif;base64" => bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a"),
        "data:image/webp;base64" => bytes.starts_with(b"RIFF") && bytes.get(8..12) == Some(b"WEBP"),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_rfc4648_vectors() {
        for (plain, encoded) in [
            (b"".as_slice(), ""),
            (b"f", "Zg=="),
            (b"fo", "Zm8="),
            (b"foo", "Zm9v"),
            (b"foob", "Zm9vYg=="),
            (b"fooba", "Zm9vYmE="),
            (b"foobar", "Zm9vYmFy"),
            (b"\x89PNG\r\n\x1a\n", "iVBORw0KGgo="),
        ] {
            assert_eq!(encode(plain), encoded);
            assert_eq!(decode(encoded).as_deref(), Some(plain));
        }
    }

    #[test]
    fn rejects_malformed_and_non_canonical_inputs() {
        for bad in ["a", "Zg=", "Zg==Zg==", "Zh==", "Zm9=", "not-base64", "A=AA"] {
            assert_eq!(decode(bad), None, "expected {bad:?} to be rejected");
        }
    }
}

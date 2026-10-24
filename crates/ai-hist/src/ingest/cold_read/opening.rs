//! Bounded opening-record reads and optional cold-body reuse.
use super::*;

/// The first record of `path`, from bytes already read or from one shared open.
pub(crate) fn opening_record(path: &Path) -> io::Result<Option<(Vec<u8>, u64)>> {
    if let Some(bytes) = fresh(path) {
        return Ok(first_record(&bytes));
    }
    // Outside a sweep the caller only needs the first record. Slurping here
    // would read every sibling rollout a hydration merely classifies.
    if in_sweep() && small_file_len(path)?.is_some() {
        let bytes = read_all(path)?;
        return Ok(first_record(&bytes));
    }
    read_one_record(path)
}

fn first_record(bytes: &[u8]) -> Option<(Vec<u8>, u64)> {
    if bytes.is_empty() {
        return None;
    }
    let end = bytes
        .iter()
        .position(|byte| *byte == b'\n')
        .map(|index| index + 1)
        .unwrap_or(bytes.len());
    Some((bytes[..end].to_vec(), end as u64))
}

fn read_one_record(path: &Path) -> io::Result<Option<(Vec<u8>, u64)>> {
    use std::io::BufRead;
    let mut shared = open_shared(path)?;
    let mut reader = std::io::BufReader::new(shared.file()?);
    let mut raw = Vec::new();
    let ceiling = crate::ingest::transcript_cursor::MAX_RECORD_BYTES;
    let read = reader.by_ref().take(ceiling).read_until(b'\n', &mut raw)? as u64;
    if read == 0 {
        return Ok(None);
    }
    if raw.last() == Some(&b'\n') || read < ceiling {
        return Ok(Some((raw, read)));
    }
    match reader.fill_buf()?.first().copied() {
        Some(b'\n') => {
            reader.consume(1);
            raw.push(b'\n');
            Ok(Some((raw, read + 1)))
        }
        None => Ok(Some((raw, read))),
        Some(_) => {
            let skipped = skip_to_newline(&mut reader)?;
            Ok(Some((Vec::new(), read + skipped)))
        }
    }
}

fn skip_to_newline(reader: &mut impl std::io::BufRead) -> io::Result<u64> {
    let mut skipped = 0u64;
    loop {
        let buffered = reader.fill_buf()?;
        if buffered.is_empty() {
            return Ok(skipped);
        }
        match buffered.iter().position(|byte| *byte == b'\n') {
            Some(index) => {
                reader.consume(index + 1);
                return Ok(skipped + index as u64 + 1);
            }
            None => {
                let len = buffered.len();
                reader.consume(len);
                skipped += len as u64;
            }
        }
    }
}

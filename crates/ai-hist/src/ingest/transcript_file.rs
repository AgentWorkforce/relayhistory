//! A transcript's bytes, stored plain or zstd-compressed.
//!
//! Codex compresses a finished rollout in place: `rollout-….jsonl` becomes
//! `rollout-….jsonl.zst` holding the same records, and resuming the session
//! can write it back out as plain JSONL. Either way it is one transcript, so it
//! is known by one **locator** — its plain name — and every stamp map, cursor,
//! catalog row and evidence locator keyed by that name survives the change of
//! encoding instead of being forgotten and re-ingested as a new file.
//!
//! [`open`] and [`metadata`] take a locator (or a `.zst` path) and find the
//! bytes: the plain file when it exists, else `<locator>.zst`. When both exist
//! — the moment between Codex writing the compressed copy and removing the
//! original, or between decompressing and removing the archive — the plain
//! file wins: it is the source of the copy, or the live transcript being
//! appended to.
//!
//! Every position here is a position in the **decompressed records**: a
//! length is the decompressed length, an offset is an offset into the
//! records, and a seek moves through them. Those are the bytes a cursor's
//! offset and prefix window describe, so a cursor taken over the plain file
//! still describes the compressed one. The stat behind identity, mtime and
//! change time is the stored file's own.
//!
//! A compressed transcript is not seekable. A forward seek decompresses and
//! discards up to the target; a backward one restarts the stream. Each handle
//! reads the shared file positionally, so two handles over one open never
//! move each other's place.

use std::fs;
use std::io::{self, BufReader, Read, Seek, SeekFrom};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// The extension a zstd-compressed transcript adds to its locator.
const ZSTD_EXTENSION: &str = "zst";

/// zstd frame magic, little-endian on disk.
const ZSTD_MAGIC: u32 = 0xFD2F_B528;

/// Skippable frames carry metadata and no records.
const SKIPPABLE_MAGIC_MASK: u32 = 0xFFFF_FFF0;
const SKIPPABLE_MAGIC: u32 = 0x184D_2A50;

/// Whether `path` names a compressed transcript rather than a locator.
pub(crate) fn is_compressed_name(path: &Path) -> bool {
    path.extension().and_then(|ext| ext.to_str()) == Some(ZSTD_EXTENSION)
}

/// The locator a transcript file is known by: its plain name.
pub(crate) fn locator_of(path: &Path) -> PathBuf {
    if is_compressed_name(path) {
        path.with_extension("")
    } else {
        path.to_path_buf()
    }
}

/// Where a locator's compressed copy is stored.
pub(crate) fn compressed_path(locator: &Path) -> PathBuf {
    let mut name = locator.as_os_str().to_os_string();
    name.push(".");
    name.push(ZSTD_EXTENSION);
    PathBuf::from(name)
}

/// The file holding `path`'s bytes and its own stat, from one stat of each
/// name at most. Not-found when neither exists.
pub(crate) fn stored(path: &Path) -> io::Result<(PathBuf, fs::Metadata)> {
    if is_compressed_name(path) {
        return Ok((path.to_path_buf(), fs::metadata(path)?));
    }
    match fs::metadata(path) {
        Ok(metadata) => Ok((path.to_path_buf(), metadata)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let compressed = compressed_path(path);
            match fs::metadata(&compressed) {
                Ok(metadata) => Ok((compressed, metadata)),
                Err(_) => Err(error),
            }
        }
        Err(error) => Err(error),
    }
}

/// The file `path`'s bytes are stored in now.
pub(crate) fn stored_path(path: &Path) -> io::Result<PathBuf> {
    stored(path).map(|(stored, _)| stored)
}

/// Whether a transcript is stored under `path` in either encoding.
pub(crate) fn exists(path: &Path) -> bool {
    stored(path).is_ok_and(|(_, metadata)| metadata.is_file())
}

/// The stored file's stat, with the decompressed length.
///
/// Dereferences to the stored file's [`fs::Metadata`] for mtime, change time,
/// identity and file type. [`Self::len`] is the length of the records.
#[derive(Clone, Debug)]
pub(crate) struct TranscriptMetadata {
    metadata: fs::Metadata,
    len: u64,
    compressed: bool,
}

impl TranscriptMetadata {
    /// The length of the records: the decompressed length when compressed.
    pub(crate) fn len(&self) -> u64 {
        self.len
    }

    pub(crate) fn compressed(&self) -> bool {
        self.compressed
    }

    /// The stored file's own length: the compressed length when compressed.
    pub(crate) fn stored_len(&self) -> u64 {
        self.metadata.len()
    }
}

impl Deref for TranscriptMetadata {
    type Target = fs::Metadata;

    fn deref(&self) -> &fs::Metadata {
        &self.metadata
    }
}

/// The stat of the transcript `path` names, in whichever encoding it is stored.
pub(crate) fn metadata(path: &Path) -> io::Result<TranscriptMetadata> {
    let (stored, metadata) = stored(path)?;
    if !is_compressed_name(&stored) || !metadata.is_file() {
        let len = metadata.len();
        return Ok(TranscriptMetadata {
            metadata,
            len,
            compressed: false,
        });
    }
    let file = fs::File::open(&stored)?;
    let len = decompressed_len(&file, metadata.len())?;
    Ok(TranscriptMetadata {
        metadata,
        len,
        compressed: true,
    })
}

/// Open the transcript `path` names, in whichever encoding it is stored.
pub(crate) fn open(path: &Path) -> io::Result<TranscriptFile> {
    let (stored, _) = stored(path)?;
    let file = fs::File::open(&stored)?;
    if !is_compressed_name(&stored) {
        return Ok(TranscriptFile::Plain(file));
    }
    let stored_len = file.metadata()?.len();
    let len = decompressed_len(&file, stored_len)?;
    Ok(TranscriptFile::Zstd(ZstdFile {
        file: Arc::new(file),
        len,
        position: 0,
        decoder: None,
    }))
}

/// One open transcript, read and seeked in decompressed positions.
#[derive(Debug)]
pub(crate) enum TranscriptFile {
    Plain(fs::File),
    Zstd(ZstdFile),
}

impl TranscriptFile {
    pub(crate) fn metadata(&self) -> io::Result<TranscriptMetadata> {
        match self {
            Self::Plain(file) => {
                let metadata = file.metadata()?;
                let len = metadata.len();
                Ok(TranscriptMetadata {
                    metadata,
                    len,
                    compressed: false,
                })
            }
            Self::Zstd(zstd) => Ok(TranscriptMetadata {
                metadata: zstd.file.metadata()?,
                len: zstd.len,
                compressed: true,
            }),
        }
    }

    /// The compressed file this handle decompresses; `None` when plain.
    pub(crate) fn compressed_file(&self) -> Option<&fs::File> {
        match self {
            Self::Plain(_) => None,
            Self::Zstd(zstd) => Some(&zstd.file),
        }
    }

    /// A second handle on the same open file. A plain clone shares the
    /// descriptor's position, as [`fs::File::try_clone`] does; a compressed
    /// one starts at the beginning. Callers seek it before reading.
    pub(crate) fn try_clone(&self) -> io::Result<Self> {
        match self {
            Self::Plain(file) => Ok(Self::Plain(file.try_clone()?)),
            Self::Zstd(zstd) => Ok(Self::Zstd(ZstdFile {
                file: Arc::clone(&zstd.file),
                len: zstd.len,
                position: 0,
                decoder: None,
            })),
        }
    }
}

#[cfg(test)]
thread_local! {
    static READ_BYTES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn take_read_bytes() -> u64 {
    READ_BYTES.with(|bytes| bytes.replace(0))
}

impl Read for TranscriptFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let read = match self {
            Self::Plain(file) => file.read(buf),
            Self::Zstd(zstd) => zstd.read(buf),
        }?;
        #[cfg(test)]
        READ_BYTES.with(|bytes| bytes.set(bytes.get() + read as u64));
        Ok(read)
    }
}

impl Seek for TranscriptFile {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        match self {
            Self::Plain(file) => file.seek(pos),
            Self::Zstd(zstd) => zstd.seek(pos),
        }
    }
}

type Decoder = zstd::stream::read::Decoder<'static, BufReader<At>>;

/// A compressed transcript's decompressed stream.
pub(crate) struct ZstdFile {
    file: Arc<fs::File>,
    len: u64,
    /// Decompressed bytes behind the stream.
    position: u64,
    /// Started on the first read, so a handle that is only stat'd or seeked
    /// to its end decompresses nothing.
    decoder: Option<Box<Decoder>>,
}

impl std::fmt::Debug for ZstdFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZstdFile")
            .field("len", &self.len)
            .field("position", &self.position)
            .finish_non_exhaustive()
    }
}

impl ZstdFile {
    fn decoder(&mut self) -> io::Result<&mut Decoder> {
        let decoder = match self.decoder.take() {
            Some(decoder) => decoder,
            None => {
                debug_assert_eq!(self.position, 0);
                let source = At {
                    file: Arc::clone(&self.file),
                    offset: 0,
                };
                Box::new(Decoder::with_buffer(BufReader::new(source))?)
            }
        };
        Ok(self.decoder.insert(decoder))
    }

    /// Decompress and discard up to `target`, or to the end of the records.
    fn skip_to(&mut self, target: u64) -> io::Result<()> {
        if target < self.position {
            self.decoder = None;
            self.position = 0;
        }
        let wanted = target - self.position;
        if wanted == 0 {
            return Ok(());
        }
        let skipped = io::copy(&mut self.decoder()?.take(wanted), &mut io::sink())?;
        self.position += skipped;
        Ok(())
    }
}

impl Read for ZstdFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() || self.position >= self.len {
            return Ok(0);
        }
        let read = self.decoder()?.read(buf)?;
        if read == 0 {
            return Err(truncated());
        }
        self.position += read as u64;
        Ok(read)
    }
}

impl Seek for ZstdFile {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let target = match pos {
            SeekFrom::Start(offset) => Some(offset),
            SeekFrom::End(delta) => self.len.checked_add_signed(delta),
            SeekFrom::Current(delta) => self.position.checked_add_signed(delta),
        }
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "seek to a negative or overflowing position",
            )
        })?;
        // Past the end is where a plain file would put it too; reads there
        // return nothing, so there is nothing to decompress on the way.
        let reachable = target.min(self.len);
        self.skip_to(reachable)?;
        if self.position < reachable {
            return Err(truncated());
        }
        self.position = target;
        Ok(target)
    }
}

/// Positional reads over a shared file, so each decoder keeps its own place.
struct At {
    file: Arc<fs::File>,
    offset: u64,
}

impl Read for At {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let read = read_at(&self.file, buf, self.offset)?;
        self.offset += read as u64;
        Ok(read)
    }
}

#[cfg(unix)]
fn read_at(file: &fs::File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    std::os::unix::fs::FileExt::read_at(file, buf, offset)
}

#[cfg(windows)]
fn read_at(file: &fs::File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    std::os::windows::fs::FileExt::seek_read(file, buf, offset)
}

fn read_exact_at(file: &fs::File, buf: &mut [u8], mut offset: u64) -> io::Result<()> {
    let mut filled = 0;
    while filled < buf.len() {
        let read = read_at(file, &mut buf[filled..], offset)?;
        if read == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        filled += read;
        offset += read as u64;
    }
    Ok(())
}

/// The decompressed length of a zstd file.
///
/// Read from the frame headers: each frame states its content size, and its
/// end is found by walking block headers, three bytes per block. A frame that
/// does not state its size is counted by decompressing the file once.
fn decompressed_len(file: &fs::File, stored_len: u64) -> io::Result<u64> {
    match declared_len(file, stored_len)? {
        Some(len) => Ok(len),
        None => {
            let source = At {
                file: Arc::new(file.try_clone()?),
                offset: 0,
            };
            io::copy(
                &mut zstd::stream::read::Decoder::with_buffer(BufReader::new(source))?,
                &mut io::sink(),
            )
        }
    }
}

/// The sum of every frame's declared content size, or `None` when a frame
/// does not declare one.
fn declared_len(file: &fs::File, stored_len: u64) -> io::Result<Option<u64>> {
    let mut offset = 0u64;
    let mut total = 0u64;
    while offset < stored_len {
        let mut magic = [0u8; 4];
        read_exact_at(file, &mut magic, offset)?;
        let magic = u32::from_le_bytes(magic);
        if magic & SKIPPABLE_MAGIC_MASK == SKIPPABLE_MAGIC {
            let mut size = [0u8; 4];
            read_exact_at(file, &mut size, offset + 4)?;
            offset += 8 + u64::from(u32::from_le_bytes(size));
            continue;
        }
        if magic != ZSTD_MAGIC {
            return Err(invalid("not a zstd frame"));
        }
        let Some((content, frame_len)) = frame(file, offset)? else {
            return Ok(None);
        };
        total += content;
        offset += frame_len;
    }
    // A frame or skippable frame that runs past the end is a file still being
    // written, or cut short: its declared records are not on disk.
    if offset > stored_len {
        return Err(invalid("truncated frame"));
    }
    Ok(Some(total))
}

/// One frame's declared content size and stored length, or `None` when it
/// declares no content size.
fn frame(file: &fs::File, start: u64) -> io::Result<Option<(u64, u64)>> {
    // Magic, descriptor, window, dictionary id and content size: at most 18.
    let mut header = [0u8; 18];
    let available = read_at(file, &mut header, start)?;
    let header = &header[..available];
    let descriptor = *header.get(4).ok_or_else(|| invalid("truncated frame"))?;
    let content_size_flag = descriptor >> 6;
    let single_segment = descriptor & 0x20 != 0;
    let checksum = descriptor & 0x04 != 0;
    let dictionary_bytes = [0u64, 1, 2, 4][usize::from(descriptor & 0x03)];
    let content_size_bytes = match content_size_flag {
        0 if single_segment => 1,
        0 => 0,
        1 => 2,
        2 => 4,
        _ => 8,
    };
    let header_len = 5 + u64::from(!single_segment) + dictionary_bytes + content_size_bytes;
    let content = zstd::zstd_safe::get_frame_content_size(header)
        .map_err(|_| invalid("unreadable frame header"))?;
    let mut offset = start + header_len;
    loop {
        let mut block = [0u8; 3];
        read_exact_at(file, &mut block, offset)?;
        let block = u32::from(block[0]) | u32::from(block[1]) << 8 | u32::from(block[2]) << 16;
        let last = block & 1 != 0;
        let size = u64::from(block >> 3);
        offset += 3 + match (block >> 1) & 0x03 {
            // An RLE block stores its one repeated byte.
            1 => 1,
            3 => return Err(invalid("reserved block type")),
            _ => size,
        };
        if last {
            break;
        }
    }
    if checksum {
        offset += 4;
    }
    Ok(content.map(|content| (content, offset - start)))
}

/// The stream ended before the length its frames declared.
fn truncated() -> io::Error {
    io::Error::new(
        io::ErrorKind::UnexpectedEof,
        "compressed transcript ends before its declared length",
    )
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn compress(bytes: &[u8]) -> Vec<u8> {
        zstd::bulk::compress(bytes, 3).unwrap()
    }

    fn records(count: usize) -> Vec<u8> {
        (0..count)
            .map(|index| {
                format!(
                    "{{\"index\":{index},\"pad\":\"{}\"}}\n",
                    "x".repeat(index % 97)
                )
            })
            .collect::<String>()
            .into_bytes()
    }

    #[test]
    fn a_locator_resolves_to_its_compressed_copy() {
        let dir = tempfile::tempdir().unwrap();
        let locator = dir.path().join("rollout-a.jsonl");
        let body = records(50);
        fs::write(compressed_path(&locator), compress(&body)).unwrap();

        assert_eq!(locator_of(&compressed_path(&locator)), locator);
        assert!(exists(&locator));
        let metadata = metadata(&locator).unwrap();
        assert!(metadata.compressed());
        assert_eq!(metadata.len(), body.len() as u64);
        let mut read = Vec::new();
        open(&locator).unwrap().read_to_end(&mut read).unwrap();
        assert_eq!(read, body);
    }

    #[test]
    fn the_plain_file_wins_while_both_exist() {
        let dir = tempfile::tempdir().unwrap();
        let locator = dir.path().join("rollout-a.jsonl");
        let body = records(10);
        fs::write(&locator, &body).unwrap();
        fs::write(compressed_path(&locator), compress(&records(3))).unwrap();

        assert_eq!(stored_path(&locator).unwrap(), locator);
        assert!(!metadata(&locator).unwrap().compressed());
    }

    #[test]
    fn a_missing_transcript_is_not_found_in_either_encoding() {
        let dir = tempfile::tempdir().unwrap();
        let locator = dir.path().join("rollout-a.jsonl");
        assert!(!exists(&locator));
        assert_eq!(
            metadata(&locator).unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
        assert_eq!(open(&locator).unwrap_err().kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn seeks_move_through_decompressed_positions_both_ways() {
        let dir = tempfile::tempdir().unwrap();
        let locator = dir.path().join("rollout-a.jsonl");
        let body = records(2_000);
        fs::write(compressed_path(&locator), compress(&body)).unwrap();
        let mut file = open(&locator).unwrap();
        let at = |file: &mut TranscriptFile, offset: u64| {
            file.seek(SeekFrom::Start(offset)).unwrap();
            let mut buf = [0u8; 16];
            file.read_exact(&mut buf).unwrap();
            buf
        };
        let window = |offset: usize| body[offset..offset + 16].to_vec();
        assert_eq!(at(&mut file, 40_000).to_vec(), window(40_000));
        assert_eq!(at(&mut file, 1_000).to_vec(), window(1_000));
        assert_eq!(at(&mut file, 90_000).to_vec(), window(90_000));
        assert_eq!(file.stream_position().unwrap(), 90_016);
        let end = file.seek(SeekFrom::End(0)).unwrap();
        assert_eq!(end, body.len() as u64);
        assert_eq!(file.read(&mut [0u8; 4]).unwrap(), 0);
    }

    #[test]
    fn handles_cloned_from_one_open_keep_their_own_places() {
        let dir = tempfile::tempdir().unwrap();
        let locator = dir.path().join("rollout-a.jsonl");
        let body = records(1_000);
        fs::write(compressed_path(&locator), compress(&body)).unwrap();
        let mut first = open(&locator).unwrap();
        let mut second = first.try_clone().unwrap();
        let mut a = [0u8; 4096];
        let mut b = [0u8; 4096];
        first.read_exact(&mut a).unwrap();
        second.seek(SeekFrom::Start(8192)).unwrap();
        second.read_exact(&mut b).unwrap();
        first.read_exact(&mut a).unwrap();
        assert_eq!(a.to_vec(), body[4096..8192].to_vec());
        assert_eq!(b.to_vec(), body[8192..12288].to_vec());
    }

    #[test]
    fn concatenated_and_undeclared_frames_count_every_record() {
        let dir = tempfile::tempdir().unwrap();
        let head = records(300);
        let tail = records(700);
        // Two frames, the second written by a streaming encoder that does not
        // know its size up front, behind a skippable frame.
        let mut stored = compress(&head);
        stored.extend_from_slice(&0x184D_2A50u32.to_le_bytes());
        stored.extend_from_slice(&4u32.to_le_bytes());
        stored.extend_from_slice(b"meta");
        let mut encoder = zstd::stream::write::Encoder::new(Vec::new(), 3).unwrap();
        std::io::Write::write_all(&mut encoder, &tail).unwrap();
        stored.extend_from_slice(&encoder.finish().unwrap());
        let path = dir.path().join("rollout-a.jsonl.zst");
        fs::write(&path, stored).unwrap();

        let expected = [head, tail].concat();
        assert_eq!(metadata(&path).unwrap().len(), expected.len() as u64);
        let mut read = Vec::new();
        open(&path).unwrap().read_to_end(&mut read).unwrap();
        assert_eq!(read, expected);
    }

    #[test]
    fn declared_frames_are_measured_without_decompressing() {
        let dir = tempfile::tempdir().unwrap();
        let body = records(5_000);
        let path = dir.path().join("rollout-a.jsonl.zst");
        fs::write(&path, compress(&body)).unwrap();
        let file = fs::File::open(&path).unwrap();
        let stored_len = file.metadata().unwrap().len();
        assert_eq!(
            declared_len(&file, stored_len).unwrap(),
            Some(body.len() as u64)
        );
    }

    #[test]
    fn a_truncated_archive_is_an_error_not_a_declared_length() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rollout-a.jsonl.zst");
        let whole = compress(&records(2_000));
        fs::write(&path, &whole[..whole.len() - 5]).unwrap();
        assert_eq!(
            metadata(&path).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        let mut skippable = compress(&records(10));
        skippable.extend_from_slice(&0x184D_2A50u32.to_le_bytes());
        skippable.extend_from_slice(&64u32.to_le_bytes());
        skippable.extend_from_slice(b"short");
        fs::write(&path, skippable).unwrap();
        assert_eq!(
            metadata(&path).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn a_corrupt_archive_is_an_error_not_an_empty_transcript() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rollout-a.jsonl.zst");
        fs::write(&path, b"not zstd at all").unwrap();
        assert_eq!(
            metadata(&path).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }
}

//! Process-local JSONL scratch files. Avoids a `tempfile` crate dependency on
//! the default feature set.
use anyhow::{bail, Result};
use serde::Serialize;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

pub struct JsonlTemp {
    path: PathBuf,
}

impl JsonlTemp {
    pub fn write(records: impl IntoIterator<Item = impl Serialize>) -> Result<Self> {
        let mut payload = Vec::new();
        for record in records {
            serde_json::to_writer(&mut payload, &record)?;
            payload.write_all(b"\n")?;
        }
        let dir = std::env::temp_dir();
        let pid = std::process::id();
        let start = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        for attempt in 0..32u32 {
            let path = dir.join(format!("ai-hist-{pid}-{start:x}-{attempt:x}.jsonl"));
            match exclusive_create(&path) {
                Ok(mut file) => {
                    file.write_all(&payload)?;
                    file.flush()?;
                    return Ok(Self { path });
                }
                Err(err) if err.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(err) => return Err(err.into()),
            }
        }
        bail!("could not allocate a unique transcript temp file");
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

fn exclusive_create(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    options.open(path)
}

impl Drop for JsonlTemp {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

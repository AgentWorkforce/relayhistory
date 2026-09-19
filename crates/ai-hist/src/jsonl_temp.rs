//! Process-local JSONL scratch files. Avoids a `tempfile` crate dependency on
//! the default feature set.
use anyhow::Result;
use serde::Serialize;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

pub struct JsonlTemp {
    path: PathBuf,
}

impl JsonlTemp {
    pub fn write(records: impl IntoIterator<Item = impl Serialize>) -> Result<Self> {
        let path = std::env::temp_dir().join(format!(
            "ai-hist-{}-{}.jsonl",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));
        let mut file = File::create(&path)?;
        for record in records {
            serde_json::to_writer(&mut file, &record)?;
            file.write_all(b"\n")?;
        }
        file.flush()?;
        Ok(Self { path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for JsonlTemp {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

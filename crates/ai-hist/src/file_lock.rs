//! Exclusive file locks used by sync and hydration.
use std::fs::{File, TryLockError};
use std::io;

pub fn try_lock_exclusive(file: &File) -> io::Result<()> {
    match file.try_lock() {
        Ok(()) => Ok(()),
        Err(TryLockError::WouldBlock) => Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "file is locked by another process",
        )),
        Err(TryLockError::Error(err)) => Err(err),
    }
}

pub fn unlock(file: &File) -> io::Result<()> {
    file.unlock()
}

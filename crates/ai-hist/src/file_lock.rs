//! Exclusive file locks used by sync and hydration.
use std::fs::File;
use std::io;

pub fn try_lock_exclusive(file: &File) -> io::Result<()> {
    imp::try_lock_exclusive(file)
}

pub fn lock_exclusive(file: &File) -> io::Result<()> {
    imp::lock_exclusive(file)
}

pub fn unlock(file: &File) -> io::Result<()> {
    imp::unlock(file)
}

#[cfg(unix)]
mod imp {
    use super::*;
    use rustix::fs::FlockOperation;

    fn flock(file: &File, operation: FlockOperation) -> io::Result<()> {
        rustix::fs::flock(file, operation).map_err(io::Error::from)
    }

    pub fn try_lock_exclusive(file: &File) -> io::Result<()> {
        match flock(file, FlockOperation::NonBlockingLockExclusive) {
            Err(err) if err.raw_os_error() == Some(libc::EAGAIN) => {
                Err(io::Error::new(io::ErrorKind::WouldBlock, err))
            }
            other => other,
        }
    }

    pub fn lock_exclusive(file: &File) -> io::Result<()> {
        flock(file, FlockOperation::LockExclusive)
    }

    pub fn unlock(file: &File) -> io::Result<()> {
        flock(file, FlockOperation::Unlock)
    }
}

#[cfg(windows)]
mod imp {
    use super::*;
    use fs2::FileExt;

    pub fn try_lock_exclusive(file: &File) -> io::Result<()> {
        FileExt::try_lock_exclusive(file)
    }

    pub fn lock_exclusive(file: &File) -> io::Result<()> {
        FileExt::lock_exclusive(file)
    }

    pub fn unlock(file: &File) -> io::Result<()> {
        FileExt::unlock(file)
    }
}

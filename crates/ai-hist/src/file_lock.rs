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
    use std::os::unix::io::AsRawFd;

    fn flock(file: &File, flags: i32) -> io::Result<()> {
        // SAFETY: `file` owns the descriptor for the lifetime of the call.
        let rc = unsafe { libc::flock(file.as_raw_fd(), flags) };
        if rc == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    pub fn try_lock_exclusive(file: &File) -> io::Result<()> {
        match flock(file, libc::LOCK_EX | libc::LOCK_NB) {
            Err(err) if err.raw_os_error() == Some(libc::EAGAIN) => {
                Err(io::Error::new(io::ErrorKind::WouldBlock, err))
            }
            other => other,
        }
    }

    pub fn lock_exclusive(file: &File) -> io::Result<()> {
        flock(file, libc::LOCK_EX)
    }

    pub fn unlock(file: &File) -> io::Result<()> {
        flock(file, libc::LOCK_UN)
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

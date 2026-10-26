//! Bounded ready/deferred filesystem traversal.
use super::*;

impl SourceWalk {
    pub(super) fn finished(&self) -> bool {
        self.finalized && self.enumerated()
    }

    pub(super) fn healthy_drained(&self) -> bool {
        self.pending.is_empty()
            && self.current.is_none()
            && self.paused.is_empty()
            && self.queued.is_empty()
            && self.roots.is_empty()
    }

    pub(super) fn enumerated(&self) -> bool {
        self.healthy_drained() && self.deferred.is_empty()
    }

    fn defer(&mut self, work: DeferredWork) {
        if !self.deferred.iter().any(|old| old.key() == work.key()) {
            self.deferred.push_back(work);
        }
    }

    pub(super) fn rotate_reader(&mut self) {
        if let Some(next) = self.paused.pop_front() {
            if let Some(current) = self.current.replace(next) {
                self.paused.push_back(current);
            }
        }
    }

    pub(super) fn collect(
        &mut self,
        max_entries: usize,
        max_paths: usize,
        entries: &mut usize,
    ) -> Result<(), Error> {
        let had_deferred = !self.deferred.is_empty();
        if had_deferred && (self.deferred_turn || self.healthy_drained()) {
            if let Some(result) = self.retry_deferred(entries) {
                return result;
            }
        }
        self.deferred_turn = had_deferred;
        while *entries < max_entries && self.pending.len() < max_paths {
            if self.current.is_none() {
                self.current = self.paused.pop_front();
            }
            if self.current.is_some() {
                self.next_entry(entries)?;
            } else if !self.open_next(entries)? {
                break;
            }
        }
        Ok(())
    }

    fn open_next(&mut self, entries: &mut usize) -> Result<bool, Error> {
        if let Some((path, root)) = self.queued.pop_front() {
            *entries += 1;
            self.open_dir(path, root)?;
        } else if let Some(root) = self.roots.pop_front() {
            *entries += 1;
            if root.scope == WatchScope::File {
                if root.path.exists() {
                    self.pending.push(root.path.clone());
                }
            } else {
                self.open_dir(root.path.clone(), root)?;
            }
        } else {
            return Ok(false);
        }
        Ok(true)
    }

    fn retry_deferred(&mut self, entries: &mut usize) -> Option<Result<(), Error>> {
        let open = usize::from(self.current.is_some()) + self.paused.len();
        let eligible = self.deferred.iter().position(|item| {
            !matches!(item, DeferredWork::Directory(..)) || open < MAX_OPEN_DIRECTORIES
        })?;
        let item = self.deferred.remove(eligible)?;
        self.deferred_turn = false;
        *entries += 1;
        Some(self.retry(item))
    }

    fn retry(&mut self, work: DeferredWork) -> Result<(), Error> {
        match work {
            DeferredWork::Directory(path, root) => {
                let current = self.current.take();
                let result = self.open_dir(path, root);
                if self.current.is_some() {
                    if let Some(current) = current {
                        self.paused.push_back(current);
                    }
                } else {
                    self.current = current;
                }
                result
            }
            DeferredWork::Entry(path, root) => match deferred_entry_type(&path) {
                Ok(kind) => {
                    self.accept(path, root, kind);
                    Ok(())
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(error) => {
                    self.defer(DeferredWork::Entry(path, root));
                    Err(Error::Discovery(error.to_string()))
                }
            },
        }
    }

    fn open_dir(&mut self, path: PathBuf, root: WatchedPath) -> Result<(), Error> {
        let canonical = match path.canonicalize() {
            Ok(path) => path,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => {
                self.defer(DeferredWork::Directory(path, root));
                return Err(Error::Discovery(error.to_string()));
            }
        };
        if self.visited.contains(&canonical) {
            return Ok(());
        }
        match fs::read_dir(&path) {
            Ok(entries) => {
                self.visited.insert(canonical.clone());
                self.current = Some(OpenDirectory {
                    entries,
                    root,
                    path,
                    canonical,
                });
                Ok(())
            }
            Err(error) => {
                self.defer(DeferredWork::Directory(path, root));
                Err(Error::Discovery(error.to_string()))
            }
        }
    }

    fn read_entry(
        &mut self,
        entries: &mut usize,
    ) -> Result<Option<(std::fs::DirEntry, WatchedPath)>, Error> {
        let Some(current) = self.current.as_mut() else {
            return Ok(None);
        };
        let Some(entry) = read_directory_entry(current) else {
            self.current = None;
            return Ok(None);
        };
        *entries += 1;
        match entry {
            Ok(entry) => Ok(Some((entry, current.root.clone()))),
            Err(error) => {
                let path = current.path.clone();
                let root = current.root.clone();
                let canonical = current.canonical.clone();
                self.current = None;
                self.visited.remove(&canonical);
                // A reopened iterator that fails has not earned another retry
                // ahead of the ready queue. Let healthy traversal advance first.
                self.deferred_turn = false;
                self.defer(DeferredWork::Directory(path, root));
                Err(Error::Discovery(error.to_string()))
            }
        }
    }

    fn accept(&mut self, path: PathBuf, root: WatchedPath, kind: std::fs::FileType) {
        if kind.is_dir() || (kind.is_symlink() && path.is_dir()) {
            if root.scope == WatchScope::Tree {
                self.queued.push_back((path, root));
            }
        } else if !path
            .file_name()
            .is_some_and(|name| name.to_string_lossy().ends_with("-shm"))
        {
            self.pending.push(path);
        }
    }

    fn next_entry(&mut self, entries: &mut usize) -> Result<(), Error> {
        let Some((entry, root)) = self.read_entry(entries)? else {
            return Ok(());
        };
        if !root.admits(&entry.file_name()) {
            return Ok(());
        }
        let path = entry.path();
        match entry_type(&entry) {
            Ok(kind) => {
                self.accept(path, root, kind);
                Ok(())
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => {
                self.defer(DeferredWork::Entry(path, root));
                Err(Error::Discovery(error.to_string()))
            }
        }
    }
}

fn deferred_entry_type(path: &std::path::Path) -> std::io::Result<std::fs::FileType> {
    #[cfg(test)]
    if FAIL_ENTRY_PATH.with(|failed| failed.borrow().as_deref() == Some(path)) {
        return Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied));
    }
    fs::symlink_metadata(path).map(|metadata| metadata.file_type())
}

fn read_directory_entry(current: &mut OpenDirectory) -> Option<std::io::Result<std::fs::DirEntry>> {
    #[cfg(test)]
    if FAIL_READ_DIRECTORY.with(|failed| failed.borrow().contains(&current.path)) {
        FAILED_DIRECTORY_READS.with(|reads| reads.borrow_mut().push(current.path.clone()));
        return Some(Err(std::io::Error::from(
            std::io::ErrorKind::PermissionDenied,
        )));
    }
    current.entries.next()
}

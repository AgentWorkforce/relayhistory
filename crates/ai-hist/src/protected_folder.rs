//! Files & Folders directories a discovery pass can refuse to read.
//!
//! macOS prompts the first time a process stats `~/Desktop`, `~/Documents`,
//! or `~/Downloads`. The check here is textual: the candidate is not
//! canonicalized, and a `..` component is not normalized away. Skipping a
//! path that only *looks* protected is the safe choice.

use std::cell::Cell;
use std::path::{Component, Path};

const PROTECTED_FOLDERS: [&str; 3] = ["Desktop", "Documents", "Downloads"];

std::thread_local! {
    static SKIP: Cell<bool> = const { Cell::new(false) };
}

/// Applies [`crate::discover::DiscoverOptions::skip_protected_folder_identity`]
/// to every project-identity read on this thread until dropped.
///
/// Discovery holds one for the whole pass, including cached-row upgrades and
/// the end-of-pass refresh. `false` resolves every directory, which is what
/// sync and hydrate pass.
#[must_use = "the policy applies only while this guard is held"]
pub(crate) struct Guard {
    previous: bool,
}

pub(crate) fn guard(skip_protected_folder_identity: bool) -> Guard {
    Guard {
        previous: SKIP.with(|skip| skip.replace(skip_protected_folder_identity)),
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        SKIP.with(|skip| skip.set(self.previous));
    }
}

/// The active pass asked to keep `cwd` as a raw path instead of reading it.
pub(crate) fn keeping_raw_path(cwd: &str) -> bool {
    SKIP.with(|skip| skip.get()) && under_protected_user_folder(cwd)
}

/// `cwd`'s first component under the crate's home directory is a protected
/// user folder. Home comes from [`crate::paths::home_dir`]; `cwd` is not
/// canonicalized.
pub(crate) fn under_protected_user_folder(cwd: &str) -> bool {
    under_home_folder(cwd, &crate::paths::home_dir())
}

fn under_home_folder(cwd: &str, home: &Path) -> bool {
    let mut cwd = Path::new(cwd).components();
    for expected in home.components() {
        match cwd.next() {
            Some(component) if component == expected => {}
            _ => return false,
        }
    }
    match cwd.next() {
        Some(Component::Normal(name)) => is_protected_folder(name.to_string_lossy().as_ref()),
        _ => false,
    }
}

fn is_protected_folder(name: &str) -> bool {
    PROTECTED_FOLDERS
        .iter()
        .any(|folder| folder_name_eq(folder, name))
}

#[cfg(target_os = "macos")]
fn folder_name_eq(folder: &str, name: &str) -> bool {
    folder.eq_ignore_ascii_case(name)
}

#[cfg(not(target_os = "macos"))]
fn folder_name_eq(folder: &str, name: &str) -> bool {
    folder == name
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn under(cwd: &str) -> bool {
        under_home_folder(cwd, Path::new("/Users/x"))
    }

    #[test]
    fn only_the_first_component_under_home_is_protected() {
        assert!(under("/Users/x/Desktop"));
        assert!(under("/Users/x/Desktop/"));
        assert!(under("/Users/x/Desktop/example"));
        assert!(under("/Users/x/Desktop/example/../example"));
        assert!(under("/Users/x/Desktop/../Projects"));
        assert!(under("/Users/x/Documents/a"));
        assert!(under("/Users/x/Downloads"));
        assert!(!under("/Users/x/Desktop-backup"));
        assert!(!under("/Users/x/Desktop-backup/example"));
        assert!(!under("/Users/x/Projects"));
        assert!(!under("/Users/x/Projects/Desktop"));
        assert!(!under("/Users/x"));
        assert!(!under("/tmp/Desktop/example"));
    }

    #[test]
    fn folder_name_case_follows_the_operating_system() {
        let protected = under("/Users/x/desktop/example");
        assert_eq!(protected, cfg!(target_os = "macos"));
    }

    #[test]
    fn the_guard_defaults_off_and_does_not_read_the_path() {
        let home = crate::paths::home_dir();
        let protected = format!("{}/Desktop/example/../example", home.display());
        let projects = format!("{}/Projects/app", home.display());
        let backup = format!("{}/Desktop-backup/app", home.display());
        assert!(!keeping_raw_path(&protected));
        let _guard = guard(true);
        assert!(keeping_raw_path(&protected));
        assert!(!keeping_raw_path(&projects));
        assert!(!keeping_raw_path(&backup));
        let lower = format!("{}/desktop/example", home.display());
        assert_eq!(keeping_raw_path(&lower), cfg!(target_os = "macos"));
    }
}

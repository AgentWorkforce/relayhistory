//! Per-operation progress policy. Guards never cross threads; a worker enters
//! an explicit copy of its parent's policy and restores its own on exit.
use std::cell::Cell;
use std::marker::PhantomData;
use std::rc::Rc;

thread_local! {
    static QUIET: Cell<bool> = const { Cell::new(false) };
}

pub(crate) fn quiet() -> bool {
    QUIET.get()
}

#[must_use]
pub(crate) struct Scope {
    previous: bool,
    _same_thread: PhantomData<Rc<()>>,
}

impl Scope {
    pub(crate) fn enter(quiet: bool) -> Self {
        Self {
            previous: QUIET.replace(quiet),
            _same_thread: PhantomData,
        }
    }
}

impl Drop for Scope {
    fn drop(&mut self) {
        QUIET.set(self.previous);
    }
}

pub(crate) fn note(message: std::fmt::Arguments<'_>) {
    if !quiet() {
        println!("{message}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nested_output_scopes_restore_the_callers_policy() {
        assert!(!quiet());
        {
            let _silent = Scope::enter(true);
            assert!(quiet());
            {
                let _progress = Scope::enter(false);
                assert!(!quiet());
            }
            assert!(quiet());
        }
        assert!(!quiet());
    }

    #[test]
    fn facade_controls_do_not_silence_a_later_progress_operation() {
        let result = crate::session_store::controlled(None, None, || {
            assert!(quiet());
            Ok(())
        });
        assert!(result.is_ok());
        assert!(!quiet(), "completed facade call leaked quiet state");
        let home = tempfile::tempdir().unwrap();
        let roots =
            crate::ProviderRoots::from_home(home.path().into(), home.path().join("opencode.db"));
        crate::ingest::sync_tick_at_with_roots(
            &home.path().join("history.db"),
            &roots,
            crate::ingest::SyncOutput::Progress,
            false,
        )
        .unwrap();
        assert!(!quiet(), "progress tick did not restore its caller");
    }

    #[test]
    fn errors_and_unwind_restore_output() {
        let result = std::panic::catch_unwind(|| {
            let _silent = Scope::enter(true);
            panic!("test unwind");
        });
        assert!(result.is_err());
        assert!(!quiet());
        let error = || -> Result<(), ()> {
            let _silent = Scope::enter(true);
            Err(())
        };
        assert!(error().is_err());
        assert!(!quiet());
    }

    #[test]
    fn concurrent_calls_and_worker_policy_are_independent() {
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let other = barrier.clone();
        let silent = std::thread::spawn(move || {
            let _silent = Scope::enter(true);
            other.wait();
            other.wait();
            assert!(quiet());
            let inherited = quiet();
            std::thread::spawn(move || {
                assert!(!quiet());
                let _worker = Scope::enter(inherited);
                assert!(quiet());
            })
            .join()
            .unwrap();
        });
        let _progress = Scope::enter(false);
        barrier.wait();
        assert!(!quiet());
        barrier.wait();
        silent.join().unwrap();
        assert!(!quiet());
    }
}

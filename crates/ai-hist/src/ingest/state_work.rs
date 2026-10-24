//! Test-only accounting for the global sync-state I/O boundary.
thread_local! {
    static LOADS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static SAVES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}
pub(super) fn loaded() {
    LOADS.with(|value| value.set(value.get() + 1));
}
pub(super) fn saved() {
    SAVES.with(|value| value.set(value.get() + 1));
}
pub(crate) fn take() -> (u64, u64) {
    (
        LOADS.with(|value| value.replace(0)),
        SAVES.with(|value| value.replace(0)),
    )
}

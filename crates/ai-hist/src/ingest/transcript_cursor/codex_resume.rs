//! When a pass may resume from a committed Codex position.

use super::{CodexCursorState, TranscriptCursorState};

impl CodexCursorState {
    /// Whether a pass may resume from this position: the rows behind it were
    /// indexed by the current state-marker generation and, for a fork
    /// (`fork`), by the current replay gate.
    fn resumable(&self, fork: bool) -> bool {
        self.state_markers >= super::super::CODEX_STATE_MARKER_GENERATION
            && (!fork || self.fork_replay >= super::super::CODEX_FORK_REPLAY_GENERATION)
    }
}

impl TranscriptCursorState {
    /// Forget a Codex position an older parser committed, so the pass reads
    /// the rollout again from byte zero (see [`CodexCursorState::resumable`]).
    pub(crate) fn restart_unresumable_codex(&mut self, fork: bool) {
        if self
            .codex
            .as_ref()
            .is_some_and(|codex| !codex.resumable(fork))
        {
            *self = Self::default();
        }
    }
}

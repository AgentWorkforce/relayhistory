//! The per-row provider facts every `session_events` insert threads through:
//! envelope fields read verbatim from the record, plus the one derived
//! `control_kind`.

#[derive(Debug, Default, Clone, Copy)]
pub(super) struct RawMessageFacts<'a> {
    pub(super) request_id: Option<&'a str>,
    pub(super) stop_reason: Option<&'a str>,
    pub(super) agent_version: Option<&'a str>,
    pub(super) is_sidechain: Option<bool>,
    pub(super) is_meta: Option<bool>,
    pub(super) turn_id: Option<&'a str>,
    /// Which API request this row belongs to, for a provider that delimits
    /// its requests with usage snapshots instead of naming them. Numbered per
    /// session by the parser; see `codex_request_span` at its call site.
    pub(super) request_span: Option<&'a str>,
    /// Why this user-role row is not a human prompt, from
    /// [`super::control::ControlKind`]; `None` for a prompt and for model output.
    /// The one derived fact carried here, because every insert already
    /// threads this struct and the column is stamped per row like the rest.
    pub(super) control_kind: Option<&'a str>,
    /// Claude: the record's own usage blob where settlement replaced it in
    /// `token_json` -- JSON `null` for a copy that carried none. `None` where
    /// `token_json` already is the record's own, and for every other source.
    pub(super) record_token_json: Option<&'a str>,
    /// Claude: the record's `origin.kind` verbatim (`human`, `peer`,
    /// `task-notification`, ...), who put the message in the transcript.
    /// `None` where the record names no origin, and for every other source.
    pub(super) origin_kind: Option<&'a str>,
}

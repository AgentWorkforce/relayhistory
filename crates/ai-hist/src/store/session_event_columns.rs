//! The columns `session_events` gains by migration: the one list the
//! migration pass adds and the read-only guard checks.

/// Columns [`init_db`] adds to `session_events` after the original DDL, with
/// the SQL type each one is added as.
///
/// Canonical project identity (issue #175): `project_key` is denormalized onto
/// the event so a grouping query does not have to join `sessions` for every
/// row, and `project_key_method` says what kind of key it is. The method is
/// storage, not surface: nothing selects it into [`crate::SessionEvent`]. It
/// exists so the denormalizing pass can rank what it is about to write against
/// what the row already holds, which is the only way to tell a key a delegated
/// child resolved for itself from one it was lent. Without it that pass
/// overwrote the former with the latter.
///
/// Then the per-message raw provider facts: the envelope facts a harness
/// records once per API request or per message and that normalization used to
/// read past. burn groups turns into API requests by `request_id`, detects an
/// in-progress turn by the *absence* of `stop_reason`, attributes sidechain
/// output, and bounds a Codex turn by `turn_id`. Every one is nullable, and
/// the wire value is stored verbatim -- relayhistory preserves, consumers map.
///
/// As with the catalog columns above, this list is both the migration list and
/// the read-only guard list, so the two cannot drift.
pub(super) const REQUIRED_SESSION_EVENT_COLUMNS: &[(&str, &str)] = &[
    ("source_order", "INTEGER CHECK(source_order IS NULL OR (typeof(source_order) = 'integer' AND source_order >= 0))"),
    ("project_key", "TEXT"),
    ("project_key_method", "TEXT"),
    ("provider", "TEXT"),
    ("request_id", "TEXT"),
    ("provider_message_id", "TEXT"),
    ("stop_reason", "TEXT"),
    ("agent_version", "TEXT"),
    ("is_sidechain", "INTEGER"),
    ("is_meta", "INTEGER"),
    ("turn_id", "TEXT"),
    // Which API request a row belongs to, for a provider that delimits its
    // requests without naming them. Codex reports cumulative `token_count`
    // snapshots; one snapshot ends one request, and every assistant row since
    // the previous snapshot belongs to it. The parser numbers those spans per
    // session, because the boundary is knowable only while reading the
    // rollout in order -- a reader cannot recover it from the stored rows
    // without scanning the session. Null for providers that name their
    // requests, which group on `request_id` instead.
    ("request_span", "TEXT"),
    // Not a provider fact: the generation of raw-fact parsing the local parser
    // wrote the row with. It is the only field stamped on every event the
    // parser writes, whatever the provider recorded, which is what lets a
    // plain `sync` tell a row indexed before the facts existed from one whose
    // facts the provider genuinely never recorded. Adapter-supplied rows do
    // not carry it (it is absent from the session-event evidence spec), so the
    // probes that read it exclude sessions with remote provenance.
    ("raw_facts_version", "INTEGER"),
    // The provider-native record or block type an event was derived from,
    // so a `tool_result` that came from a Claude `tool_result` content
    // block stays distinguishable from one synthesized out of a
    // `type: "system"` subagent notification. Both land in the same `kind`;
    // the normalized `kind` vocabulary is deliberately not widened for it.
    ("raw_kind", "TEXT"),
    // Why a user-role row is not a human prompt, from the vocabulary in
    // `ingest::control::ControlKind`. Null on every prompt and every
    // model-output row. Derived by the parser, not copied off the envelope,
    // so it is re-stamped by the same raw-facts backfill that repairs the
    // columns above: a row without it is a row the classifier never saw.
    ("control_kind", "TEXT"),
    // Claude: the usage blob as this one record carried it, verbatim. Claude
    // writes one streamed response as several records whose usage snapshots
    // `token_json` settles into one blob; this keeps a record's own where it
    // differs (JSON `null` for a copy that carried none), and is null wherever
    // `token_json` already is the record's own. Written
    // by the same parser generation as the facts above, so the raw-facts
    // backfill fills it on rows indexed before it existed.
    ("record_token_json", "TEXT"),
    // Who put the message in the transcript, verbatim from the harness
    // (Claude's `origin.kind`). Appended, so the change feed restamps only
    // rows holding a value: rows indexed before it stay NULL and are not
    // re-exported. New and re-parsed rows carry it.
    ("origin_kind", "TEXT"),
];

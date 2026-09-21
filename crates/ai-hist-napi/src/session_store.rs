//! JSON-in/JSON-out dispatcher over the `ai-hist` [`SessionStore`] facade.
//!
//! Every operation here crosses the boundary as one `(op, argsJson)` pair and
//! answers with one JSON document, so a new facade read is a new arm in
//! [`dispatch`] rather than another hand-mirrored `#[napi]` function with its
//! own option and result objects. The TypeScript SDK's `native.ts` is the one
//! place that knows the op names; the typed layer above it validates and
//! normalizes exactly as it does for the older typed functions.
//!
//! The answers carry the same camelCase shapes the typed functions return, so
//! the SDK normalizes both paths with one set of functions and a consumer
//! cannot tell which boundary served a page.
//!
//! This module calls only the public facade and the crate's pure capability
//! tables: no connection, no SQL.
use std::path::{Path, PathBuf};

use ai_hist::{
    declared_evidence_kinds, missing_evidence_kinds, relationship_capabilities, SessionEventCursor,
    SessionEvidenceCursor, SessionRequestCursor, SessionStore, Source, StoreOptions,
    SESSION_EVIDENCE_CONTRACT_VERSION, SESSION_HYDRATION_CONTRACT_VERSION,
    SESSION_RELATIONSHIP_CONTRACT_VERSION, SESSION_USAGE_CONTRACT_VERSION,
};
use napi_derive::napi;
use serde::{Deserialize, Serialize};

use crate::{
    database_error, db_path, native_error, session_usage, validate_identity, validate_limit,
    EventCursor, EvidenceCursor, NativeRelationshipCapabilities, NativeSessionRequest,
    NativeSessionUserTurn, RequestCursor, SessionRequestsPage, SessionUserTurnsPage,
    DEFAULT_EVENT_LIMIT,
};

/// Every op the dispatcher answers, in the spelling the SDK sends.
pub const SESSION_STORE_OPS: &[&str] = &[
    OP_MARKERS,
    OP_REQUESTS,
    OP_USAGE_SUMMARY,
    OP_USER_TURNS,
    OP_CAPABILITIES,
];
const OP_MARKERS: &str = "markers";
const OP_REQUESTS: &str = "requests";
const OP_USAGE_SUMMARY: &str = "usage_summary";
const OP_USER_TURNS: &str = "user_turns";
const OP_CAPABILITIES: &str = "capabilities";

/// The one argument shape every op reads. Unknown keys are rejected rather
/// than ignored: the SDK is the only caller, and an argument it spells wrong
/// would otherwise be silently dropped instead of reported.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CallArgs {
    db_path: Option<String>,
    source: String,
    session_id: Option<String>,
    limit: Option<i64>,
    after: Option<CursorArgs>,
}

/// A continuation as JSON. `tsMs` is optional because marker cursors may sit
/// inside the undated tail; the ops whose cursors are always dated reject an
/// absent timestamp rather than guessing where the page should start.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CursorArgs {
    #[serde(default)]
    ts_ms: Option<i64>,
    id: i64,
}

impl CursorArgs {
    fn dated(self, op: &str) -> napi::Result<(i64, i64)> {
        match self.ts_ms {
            Some(ts_ms) => Ok((ts_ms, self.id)),
            None => Err(native_error(
                "INVALID_ARGUMENT",
                format!("after.tsMs is required for {op}"),
            )),
        }
    }
}

/// One record the normalized event model cannot carry, as the typed session
/// event shapes are spelled: camelCase keys, nullable facts as `null`.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeSessionMarker {
    pub id: i64,
    pub source: String,
    pub session_id: String,
    pub marker_uid: String,
    pub ts_ms: Option<i64>,
    pub message_id: Option<String>,
    pub parent_id: Option<String>,
    pub turn_id: Option<String>,
    /// Classified vocabulary: `compaction_boundary`, `summary`, ..., or
    /// `unknown` with the provider-native type in `subkind`.
    pub kind: String,
    pub subkind: Option<String>,
    pub text: Option<String>,
    /// The bounded payload projection, unparsed. The SDK owns JSON parsing so
    /// an unreadable value cannot fail a whole page at the boundary.
    pub payload_json: Option<String>,
}

impl From<ai_hist::SessionMarker> for NativeSessionMarker {
    fn from(marker: ai_hist::SessionMarker) -> Self {
        Self {
            id: marker.id,
            source: marker.source,
            session_id: marker.session_id,
            marker_uid: marker.marker_uid,
            ts_ms: marker.ts_ms,
            message_id: marker.message_id,
            parent_id: marker.parent_id,
            turn_id: marker.turn_id,
            kind: marker.kind,
            subkind: marker.subkind,
            text: marker.text,
            payload_json: marker.payload_json,
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionMarkersPage {
    pub contract_version: u32,
    pub source: String,
    pub session_id: String,
    pub markers: Vec<NativeSessionMarker>,
    pub next_cursor: Option<EvidenceCursor>,
}

/// What one provider's local parser can record, answered from the crate's
/// own capability tables. A pure answer: no database is opened, so it is
/// correct for a database that does not exist yet.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceCapabilities {
    /// The contract whose `EvidenceKind` vocabulary `evidenceKinds` uses.
    pub hydration_contract_version: u32,
    /// The contract `relationships` is spelled in.
    pub relationship_contract_version: u32,
    pub source: String,
    /// The evidence kinds the parser produces, in the parser's declared order.
    pub evidence_kinds: Vec<String>,
    /// The `FULL_SESSION_KINDS` it does not, in canonical order. Empty means
    /// a hydration of this source can report `full`.
    pub missing_evidence_kinds: Vec<String>,
    pub full_coverage: bool,
    pub relationships: NativeRelationshipCapabilities,
}

fn parse_source(name: &str) -> napi::Result<Source> {
    serde_json::from_value::<Source>(serde_json::Value::String(name.to_string())).map_err(|_| {
        native_error(
            "INVALID_ARGUMENT",
            format!(
                "source must be one of {} (got '{name}')",
                ai_hist::SOURCE_CHOICES.join(", ")
            ),
        )
    })
}

fn parse_args(args_json: &str) -> napi::Result<CallArgs> {
    serde_json::from_str(args_json).map_err(|error| {
        native_error(
            "INVALID_ARGUMENT",
            format!("invalid session_store_call arguments: {error}"),
        )
    })
}

fn required_session_id(args: &mut CallArgs) -> napi::Result<String> {
    let session_id = args
        .session_id
        .take()
        .ok_or_else(|| native_error("INVALID_ARGUMENT", "sessionId is required"))?;
    validate_identity(session_id, "sessionId")
}

fn serialize<T: Serialize>(value: &T) -> napi::Result<String> {
    serde_json::to_string(value).map_err(|error| native_error("DATABASE_QUERY_FAILED", error))
}

fn open_store(path: &Path, read_only: bool) -> Result<SessionStore, ai_hist::Error> {
    let mut options = StoreOptions::default();
    options.db_path = Some(path.to_path_buf());
    options.read_only = read_only;
    SessionStore::open(options)
}

fn query_error(error: ai_hist::Error) -> napi::Error {
    native_error("DATABASE_QUERY_FAILED", format!("{error:#}"))
}

/// Run one facade read against the database at `path`.
///
/// A read-only handle is tried first so a query never contends with a writer.
/// The facade refuses a read-only handle over a database older than the shape
/// it reads, and says so with a typed error; that one failure — and no other
/// — is answered by reopening writable, which migrates the database exactly
/// as the typed functions do, because a Node caller asked for a page, not for
/// a store it promised not to write. Every other failure is the caller's
/// answer as it stands: a writable reopen takes the writer lock and runs
/// initialization, so retrying a genuine query failure through it would at
/// best repeat the failure and at worst replace it with a contention error
/// from a writer the read had no business meeting.
fn with_store<T>(
    path: PathBuf,
    read: impl Fn(&SessionStore) -> Result<T, ai_hist::Error>,
) -> napi::Result<T> {
    match open_store(&path, true) {
        Ok(store) => match read(&store) {
            Ok(value) => return Ok(value),
            Err(error) if error.is_stale_schema() => {}
            Err(error) => return Err(query_error(error)),
        },
        Err(error) if error.is_stale_schema() => {}
        Err(error) => return Err(database_error(&path, format!("{error:#}"))),
    }
    let store =
        open_store(&path, false).map_err(|error| database_error(&path, format!("{error:#}")))?;
    read(&store).map_err(query_error)
}

fn evidence_cursor(cursor: Option<CursorArgs>) -> Option<SessionEvidenceCursor> {
    cursor.map(|cursor| SessionEvidenceCursor {
        ts_ms: cursor.ts_ms,
        id: cursor.id,
    })
}

/// Answer one op. Synchronous so it can be unit-tested without a runtime; the
/// `#[napi]` wrapper moves it onto a blocking worker.
pub(crate) fn dispatch(op: &str, args_json: &str) -> napi::Result<String> {
    let mut args = parse_args(args_json)?;
    let source_name = validate_identity(std::mem::take(&mut args.source), "source")?;
    let source = parse_source(&source_name)?;
    match op {
        OP_CAPABILITIES => {
            let evidence_kinds = declared_evidence_kinds(source.as_str());
            let missing = missing_evidence_kinds(source.as_str());
            serialize(&SourceCapabilities {
                hydration_contract_version: SESSION_HYDRATION_CONTRACT_VERSION,
                relationship_contract_version: SESSION_RELATIONSHIP_CONTRACT_VERSION,
                source: source_name,
                evidence_kinds: evidence_kinds
                    .iter()
                    .map(|kind| kind.as_str().to_string())
                    .collect(),
                full_coverage: missing.is_empty(),
                missing_evidence_kinds: missing
                    .iter()
                    .map(|kind| kind.as_str().to_string())
                    .collect(),
                relationships: relationship_capabilities(source.as_str()).into(),
            })
        }
        OP_MARKERS => {
            let session_id = required_session_id(&mut args)?;
            let limit = validate_limit(args.limit, DEFAULT_EVENT_LIMIT, 1_000)?;
            let after = evidence_cursor(args.after);
            let path = db_path(args.db_path);
            let page = if path.exists() {
                with_store(path, |store| {
                    store.session_markers_page(source, &session_id, limit, after.as_ref())
                })?
            } else {
                ai_hist::SessionMarkerPage {
                    markers: Vec::new(),
                    next_cursor: None,
                }
            };
            serialize(&SessionMarkersPage {
                contract_version: SESSION_EVIDENCE_CONTRACT_VERSION,
                source: source_name,
                session_id,
                markers: page
                    .markers
                    .into_iter()
                    .map(NativeSessionMarker::from)
                    .collect(),
                next_cursor: page.next_cursor.map(crate::evidence_cursor),
            })
        }
        OP_REQUESTS => {
            let session_id = required_session_id(&mut args)?;
            let limit = validate_limit(args.limit, DEFAULT_EVENT_LIMIT, 1_000)?;
            let after = args
                .after
                .map(|cursor| cursor.dated(op))
                .transpose()?
                .map(|(ts_ms, id)| SessionRequestCursor { ts_ms, id });
            let path = db_path(args.db_path);
            let page = if path.exists() {
                with_store(path, |store| {
                    store.session_requests_page(source, &session_id, limit, after.as_ref())
                })?
            } else {
                ai_hist::SessionRequestPage {
                    requests: Vec::new(),
                    next_cursor: None,
                }
            };
            serialize(&SessionRequestsPage {
                contract_version: SESSION_USAGE_CONTRACT_VERSION,
                source: source_name,
                session_id,
                requests: page
                    .requests
                    .into_iter()
                    .map(NativeSessionRequest::from)
                    .collect(),
                next_cursor: page.next_cursor.map(|cursor| RequestCursor {
                    ts_ms: cursor.ts_ms,
                    id: cursor.id,
                }),
            })
        }
        OP_USAGE_SUMMARY => {
            let session_id = required_session_id(&mut args)?;
            let path = db_path(args.db_path);
            let summary = if path.exists() {
                with_store(path, |store| store.session_usage(source, &session_id))?
            } else {
                None
            };
            serialize(&session_usage(source_name, session_id, summary))
        }
        OP_USER_TURNS => {
            let session_id = required_session_id(&mut args)?;
            let limit = validate_limit(args.limit, DEFAULT_EVENT_LIMIT, 1_000)?;
            let after = args
                .after
                .map(|cursor| cursor.dated(op))
                .transpose()?
                .map(|(ts_ms, id)| SessionEventCursor { ts_ms, id });
            let path = db_path(args.db_path);
            let page = if path.exists() {
                with_store(path, |store| {
                    store.session_user_turns_page(source, &session_id, limit, after.as_ref())
                })?
            } else {
                ai_hist::SessionUserTurnPage {
                    user_turns: Vec::new(),
                    next_cursor: None,
                }
            };
            serialize(&SessionUserTurnsPage {
                contract_version: SESSION_EVIDENCE_CONTRACT_VERSION,
                source: source_name,
                session_id,
                user_turns: page
                    .user_turns
                    .into_iter()
                    .map(NativeSessionUserTurn::from)
                    .collect(),
                next_cursor: page.next_cursor.map(|cursor| EventCursor {
                    ts_ms: cursor.ts_ms,
                    id: cursor.id,
                }),
            })
        }
        other => Err(native_error(
            "INVALID_ARGUMENT",
            format!(
                "unknown session_store_call op '{other}' (expected one of {})",
                SESSION_STORE_OPS.join(", ")
            ),
        )),
    }
}

/// One JSON request against the `SessionStore` facade.
///
/// `op` names the read (`markers`, `requests`, `usage_summary`, `user_turns`,
/// `capabilities`) and `args_json` carries `{dbPath?, source, sessionId?,
/// limit?, after?}`. The answer is the same camelCase document the matching
/// typed function returns. A missing database answers an empty page, never an
/// error, and never creates the file.
#[napi]
pub async fn session_store_call(op: String, args_json: String) -> napi::Result<String> {
    napi::tokio::task::spawn_blocking(move || dispatch(&op, &args_json))
        .await
        .map_err(crate::worker_error)?
}

#[cfg(test)]
mod tests {
    use super::{dispatch, SESSION_STORE_OPS};
    use ai_hist::{insert_session_marker, open_db, NewSessionMarker};
    use serde_json::{json, Value};

    fn call(op: &str, args: Value) -> Result<Value, String> {
        dispatch(op, &args.to_string())
            .map(|answer| serde_json::from_str(&answer).expect("dispatcher answers JSON"))
            .map_err(|error| error.reason.clone())
    }

    fn seeded_markers() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        let conn = open_db(&dir.path().join("history.db")).unwrap();
        for (index, (uid, ts_ms, kind)) in [
            ("compaction:1", Some(10), "compaction_boundary"),
            ("summary:1", Some(20), "summary"),
            ("undated", None, "unknown"),
        ]
        .into_iter()
        .enumerate()
        {
            insert_session_marker(
                &conn,
                "claude",
                "sess-1",
                &NewSessionMarker {
                    marker_uid: uid,
                    ts_ms,
                    kind,
                    subkind: Some("provider_type"),
                    text: Some(&format!("marker {index}")),
                    payload_json: Some(r#"{"trigger":"auto"}"#),
                    ..Default::default()
                },
            )
            .unwrap();
        }
        dir
    }

    #[test]
    fn an_unknown_op_and_malformed_arguments_are_invalid_arguments() {
        let error = call("nope", json!({ "source": "claude" })).unwrap_err();
        assert!(error.contains("INVALID_ARGUMENT"), "{error}");
        assert!(error.contains("unknown session_store_call op"), "{error}");
        for op in SESSION_STORE_OPS {
            assert!(error.contains(op), "the error names {op}: {error}");
        }

        let error = call("markers", json!({ "source": "claude", "bogus": 1 })).unwrap_err();
        assert!(error.contains("INVALID_ARGUMENT"), "{error}");

        let error = call("markers", json!({ "source": "claude" })).unwrap_err();
        assert!(error.contains("sessionId is required"), "{error}");

        let error = call("markers", json!({ "source": "claude", "sessionId": " s " })).unwrap_err();
        assert!(error.contains("padded"), "{error}");

        let error = call("markers", json!({ "source": "gemini", "sessionId": "s" })).unwrap_err();
        assert!(error.contains("source must be one of"), "{error}");

        // A dated-cursor op will not guess where an undated cursor starts.
        let error = call(
            "requests",
            json!({ "source": "claude", "sessionId": "s", "after": { "id": 3 } }),
        )
        .unwrap_err();
        assert!(error.contains("after.tsMs is required"), "{error}");
    }

    #[test]
    fn a_missing_database_answers_an_empty_page_without_creating_it() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("absent.db");
        let args = json!({ "dbPath": db, "source": "claude", "sessionId": "s" });
        let page = call("markers", args.clone()).unwrap();
        assert_eq!(page["markers"], json!([]));
        assert_eq!(page["nextCursor"], Value::Null);
        assert_eq!(
            page["contractVersion"],
            json!(ai_hist::SESSION_EVIDENCE_CONTRACT_VERSION)
        );
        let usage = call("usage_summary", args.clone()).unwrap();
        assert_eq!(usage["usage"], Value::Null);
        assert_eq!(usage["totalRequestCount"], json!(0));
        let requests = call("requests", args.clone()).unwrap();
        assert_eq!(requests["requests"], json!([]));
        let turns = call("user_turns", args).unwrap();
        assert_eq!(turns["userTurns"], json!([]));
        assert!(!db.exists(), "a read must not create the database");
    }

    #[test]
    fn markers_page_through_the_facade_with_the_evidence_keyset() {
        let dir = seeded_markers();
        let db = dir.path().join("history.db");
        let first = call(
            "markers",
            json!({ "dbPath": db, "source": "claude", "sessionId": "sess-1", "limit": 2 }),
        )
        .unwrap();
        let markers = first["markers"].as_array().unwrap();
        assert_eq!(markers.len(), 2);
        assert_eq!(markers[0]["kind"], "compaction_boundary");
        assert_eq!(markers[0]["tsMs"], json!(10));
        assert_eq!(markers[0]["subkind"], "provider_type");
        assert_eq!(markers[0]["payloadJson"], r#"{"trigger":"auto"}"#);
        assert_eq!(markers[0]["turnId"], Value::Null);
        let cursor = first["nextCursor"].clone();
        assert_eq!(cursor["tsMs"], json!(20));

        // The printed cursor feeds straight back in, and the undated marker
        // sorts last behind a cursor whose timestamp is null.
        let rest = call(
            "markers",
            json!({ "dbPath": db, "source": "claude", "sessionId": "sess-1", "after": cursor }),
        )
        .unwrap();
        let markers = rest["markers"].as_array().unwrap();
        assert_eq!(markers.len(), 1);
        assert_eq!(markers[0]["markerUid"], "undated");
        assert_eq!(markers[0]["tsMs"], Value::Null);
        assert_eq!(rest["nextCursor"], Value::Null);

        // Another provider's session with the same id is not this page.
        let other = call(
            "markers",
            json!({ "dbPath": db, "source": "codex", "sessionId": "sess-1" }),
        )
        .unwrap();
        assert_eq!(other["markers"], json!([]));
    }

    /// A read that fails for any reason other than a stale schema is the
    /// answer: it is not retried through a writable open, which would take
    /// the writer lock and run initialization for a query that would only
    /// fail again — or fail differently, as `DATABASE_OPEN_FAILED` against a
    /// concurrent writer, hiding the real error.
    #[test]
    fn a_query_failure_is_returned_not_retried_through_a_writable_open() {
        let dir = seeded_markers();
        let db = dir.path().join("history.db");
        let calls = std::cell::Cell::new(0);
        let error = super::with_store::<()>(db, |_store| {
            calls.set(calls.get() + 1);
            Err(anyhow::anyhow!("no such column: payload_json").into())
        })
        .unwrap_err();
        assert_eq!(calls.get(), 1, "the read ran once, on the read-only store");
        assert!(
            error.reason.contains("DATABASE_QUERY_FAILED"),
            "{}",
            error.reason
        );
        assert!(error.reason.contains("no such column"), "{}", error.reason);
    }

    /// The one failure a writable reopen answers: a read-only store refusing
    /// a database older than the shape it reads. The reopen migrates it and
    /// the same read then succeeds.
    #[test]
    fn a_stale_schema_is_migrated_by_a_writable_reopen_and_read_again() {
        let dir = seeded_markers();
        let db = dir.path().join("history.db");
        // Drop the index the marker page depends on, so a read-only store
        // refuses the page as stale while the database itself opens fine.
        open_db(&db)
            .unwrap()
            .execute_batch("DROP INDEX idx_session_markers_page")
            .unwrap();
        let calls = std::cell::Cell::new(0);
        let page = super::with_store(db.clone(), |store| {
            calls.set(calls.get() + 1);
            store.session_markers_page(ai_hist::Source::Claude, "sess-1", 10, None)
        })
        .unwrap();
        assert_eq!(calls.get(), 2, "read-only refusal, then the migrated read");
        assert_eq!(page.markers.len(), 3);
        // And through the dispatcher, the page is simply served.
        let served = call(
            "markers",
            json!({ "dbPath": db, "source": "claude", "sessionId": "sess-1" }),
        )
        .unwrap();
        assert_eq!(served["markers"].as_array().unwrap().len(), 3);
    }

    #[test]
    fn capabilities_are_answered_from_the_pure_tables() {
        let claude = call("capabilities", json!({ "source": "claude" })).unwrap();
        assert_eq!(claude["source"], "claude");
        assert_eq!(claude["fullCoverage"], json!(true));
        assert_eq!(claude["missingEvidenceKinds"], json!([]));
        assert!(claude["evidenceKinds"]
            .as_array()
            .unwrap()
            .contains(&json!("session_event")));
        assert_eq!(claude["relationships"]["stableChildIdentity"], "sometimes");
        assert_eq!(
            claude["hydrationContractVersion"],
            json!(ai_hist::SESSION_HYDRATION_CONTRACT_VERSION)
        );

        let cursor = call("capabilities", json!({ "source": "cursor" })).unwrap();
        assert_eq!(cursor["fullCoverage"], json!(false));
        assert!(!cursor["missingEvidenceKinds"]
            .as_array()
            .unwrap()
            .is_empty());
        assert_eq!(cursor["relationships"]["stableChildIdentity"], "never");
    }
}

//! The `changes` and `commit_changes` ops of the `sessionStoreCall`
//! dispatcher: the revision-stamped change feed
//! ([`SessionStore::changes_since`]) and its named consumer cursors, across
//! the Node boundary.
//!
//! A page is one bounded drain: the dispatcher opens the feed at `from`, takes
//! at most `limit` changes and answers with them, the position after the last
//! one, and the head the drain was bounded to. The next page opens the feed
//! again at that position. Revisions are unique per write, so a position taken
//! mid-drain resumes exactly where the page stopped.
//!
//! A named cursor moves only on `commit_changes`, never as a side effect of
//! reading a page, so a consumer that fails between reading and applying a
//! page re-reads it rather than skipping it — the facade's own rule for
//! `Changes::commit`.
//!
//! A `Watermark` crosses as `{ epoch, revision }` with the epoch as 16 hex
//! digits: it is a random 64-bit store identity, which a JavaScript number
//! cannot hold exactly. The spelling is the export's `origin_id`, so a
//! consumer sees one store identity whichever of the two it reads.
use std::path::PathBuf;

use ai_hist::{
    open_db_readonly, schema_is_current, Change, ChangeKind, ChangeOp, ChangeQuery, SessionStore,
    StoreOptions, Watermark, DEFAULT_CHANGE_BATCH, MAX_CHANGE_BATCH,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{database_error, db_path, native_error, validate_identity};

pub const OP_CHANGES: &str = "changes";
pub const OP_COMMIT_CHANGES: &str = "commit_changes";

/// A feed position as JSON: the two named starts, or a watermark this store
/// issued.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FeedStart {
    Start,
    Consumer,
    At(Watermark),
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum FromArgs {
    Named(String),
    At(WireWatermark),
}

/// A watermark on the wire. `epoch` is 16 lowercase hex digits.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireWatermark {
    pub epoch: String,
    pub revision: u64,
}

impl WireWatermark {
    fn from_watermark(watermark: Watermark) -> Self {
        Self {
            epoch: format!("{:016x}", watermark.epoch),
            revision: watermark.revision,
        }
    }

    fn parse(self, field: &str) -> napi::Result<Watermark> {
        let valid = !self.epoch.is_empty()
            && self.epoch.len() <= 16
            && self.epoch.bytes().all(|byte| byte.is_ascii_hexdigit());
        let epoch = valid
            .then(|| u64::from_str_radix(&self.epoch, 16).ok())
            .flatten()
            .ok_or_else(|| {
                native_error(
                    "INVALID_ARGUMENT",
                    format!(
                        "{field}.epoch must be up to 16 hex digits (got '{}')",
                        self.epoch
                    ),
                )
            })?;
        Ok(Watermark {
            epoch,
            revision: self.revision,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SessionArgs {
    source: String,
    session_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ChangesArgs {
    db_path: Option<String>,
    from: Option<FromArgs>,
    consumer: Option<String>,
    kinds: Option<Vec<String>>,
    session: Option<SessionArgs>,
    limit: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CommitArgs {
    db_path: Option<String>,
    consumer: String,
    kinds: Option<Vec<String>>,
    position: WireWatermark,
}

/// One change, as the typed evidence shapes are spelled: camelCase keys,
/// `columns` the row exactly as stored.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeChange {
    pub kind: &'static str,
    /// The source's name when this build knows it; null for a row a newer
    /// release wrote, which `sourceName` still names.
    pub source: Option<&'static str>,
    pub source_name: String,
    pub session_id: String,
    pub record_key: String,
    pub key: Vec<Value>,
    pub revision: u64,
    /// `upsert` or `delete`.
    pub op: &'static str,
    /// Every stored column but `revision`, in table order; null on a delete.
    pub columns: Option<Value>,
}

impl NativeChange {
    fn from_change(change: Change) -> napi::Result<Self> {
        let op = match change.op {
            ChangeOp::Delete => "delete",
            _ => "upsert",
        };
        let columns = change
            .columns
            .map(|row| serde_json::to_value(&row))
            .transpose()
            .map_err(|error| native_error("DATABASE_QUERY_FAILED", error))?;
        Ok(Self {
            kind: change.kind.as_str(),
            source: change.source.map(|source| source.as_str()),
            source_name: change.source_name,
            session_id: change.session_id,
            record_key: change.record_key,
            key: change.key,
            revision: change.revision,
            op,
            columns,
        })
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChangesPage {
    pub changes: Vec<NativeChange>,
    /// Where the next page starts: the last change's revision, or `head`
    /// once the drain is exhausted. Committing it acknowledges this page.
    pub position: WireWatermark,
    /// The store head the page was bounded to.
    pub head: WireWatermark,
    /// True when no change the query selects is left after `position` at or
    /// below `head`.
    pub done: bool,
    /// The named cursor the page was read for, if any.
    pub consumer: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CommittedCursor {
    pub consumer: String,
    /// The cursor as stored, which a stale commit leaves ahead of the
    /// position it was given.
    pub cursor: WireWatermark,
}

fn parse<T: for<'de> Deserialize<'de>>(args_json: &str, op: &str) -> napi::Result<T> {
    serde_json::from_str(args_json).map_err(|error| {
        native_error(
            "INVALID_ARGUMENT",
            format!("invalid session_store_call arguments for {op}: {error}"),
        )
    })
}

fn parse_kinds(kinds: Option<Vec<String>>) -> napi::Result<Option<Vec<ChangeKind>>> {
    kinds
        .map(|kinds| {
            kinds
                .into_iter()
                .map(|kind| {
                    ChangeKind::ALL
                        .iter()
                        .copied()
                        .find(|known| known.as_str() == kind)
                        .ok_or_else(|| {
                            native_error(
                                "INVALID_ARGUMENT",
                                format!(
                                    "kinds: unknown change kind '{kind}' (expected one of {})",
                                    ChangeKind::ALL
                                        .iter()
                                        .map(|kind| kind.as_str())
                                        .collect::<Vec<_>>()
                                        .join(", ")
                                ),
                            )
                        })
                })
                .collect::<napi::Result<Vec<_>>>()
        })
        .transpose()
}

fn parse_from(from: Option<FromArgs>, consumer: bool) -> napi::Result<FeedStart> {
    match from {
        None if consumer => Ok(FeedStart::Consumer),
        None => Ok(FeedStart::Start),
        Some(FromArgs::Named(name)) => match name.as_str() {
            "start" => Ok(FeedStart::Start),
            "consumer" => Ok(FeedStart::Consumer),
            other => Err(native_error(
                "INVALID_ARGUMENT",
                format!("from must be 'start', 'consumer' or a watermark (got '{other}')"),
            )),
        },
        Some(FromArgs::At(watermark)) => watermark.parse("from").map(FeedStart::At),
    }
}

fn consumer_name(name: String) -> napi::Result<String> {
    validate_identity(name, "consumer")
}

fn open_store(path: &std::path::Path, read_only: bool) -> Result<SessionStore, ai_hist::Error> {
    let mut options = StoreOptions::default();
    options.db_path = Some(path.to_path_buf());
    options.read_only = read_only;
    SessionStore::open(options)
}

fn facade_error(error: ai_hist::Error) -> napi::Error {
    match error.code() {
        "QUERY_FAILED" => native_error("DATABASE_QUERY_FAILED", error.message()),
        code => native_error(code, error.message()),
    }
}

/// Whether the change-feed schema is already in place. `SessionStore::open`
/// read-only checks only the session-evidence indexes, so a database written
/// before the feed existed passes it and is refused later by
/// `changes_since`; checking here lets that database migrate instead.
fn feed_schema_is_current(path: &std::path::Path) -> napi::Result<bool> {
    open_db_readonly(path)
        .and_then(|conn| schema_is_current(&conn))
        .map_err(|error| database_error(path, format!("{error:#}")))
}

/// Read one page. A read-only open first; only a stale schema — the
/// facade's session-evidence gate or the feed's own — reopens writable,
/// exactly as the dispatcher's other reads do.
///
/// `done` is filtered exhaustion, not `position == head`: under a kind or
/// session filter the last matching change can sit below a head that
/// unrelated writes moved, so the page looks one change past its limit
/// before saying another page exists.
fn read_page(
    path: PathBuf,
    from: Watermark,
    query: ChangeQuery,
    limit: usize,
) -> napi::Result<String> {
    let store =
        match open_store(&path, true) {
            Ok(store) if feed_schema_is_current(&path)? => store,
            Ok(_) => open_store(&path, false)
                .map_err(|error| database_error(&path, format!("{error:#}")))?,
            Err(error) if error.is_stale_schema() => open_store(&path, false)
                .map_err(|error| database_error(&path, format!("{error:#}")))?,
            Err(error) => return Err(database_error(&path, format!("{error:#}"))),
        };
    let mut drain = store.changes_since(from, query).map_err(facade_error)?;
    let mut changes = Vec::new();
    while changes.len() < limit {
        match drain.next() {
            Some(change) => changes.push(NativeChange::from_change(change.map_err(facade_error)?)?),
            None => break,
        }
    }
    let head = drain.head();
    let (position, done) = if changes.len() < limit {
        (drain.position(), true)
    } else {
        let position = drain.position();
        match drain.next() {
            Some(Err(error)) => return Err(facade_error(error)),
            Some(Ok(_)) => (position, false),
            None => (drain.position(), true),
        }
    };
    let page = ChangesPage {
        done,
        position: WireWatermark::from_watermark(position),
        head: WireWatermark::from_watermark(head),
        consumer: drain.consumer().map(str::to_string),
        changes,
    };
    serde_json::to_string(&page).map_err(|error| native_error("DATABASE_QUERY_FAILED", error))
}

/// Answer `changes` or `commit_changes`.
pub(crate) fn dispatch(op: &str, args_json: &str) -> napi::Result<String> {
    if op == OP_COMMIT_CHANGES {
        return commit(parse(args_json, op)?);
    }
    let args: ChangesArgs = parse(args_json, op)?;
    let consumer = args.consumer.map(consumer_name).transpose()?;
    let from = parse_from(args.from, consumer.is_some())?;
    if from == FeedStart::Consumer && consumer.is_none() {
        return Err(native_error(
            "INVALID_ARGUMENT",
            "from 'consumer' needs consumer to name the cursor",
        ));
    }
    let limit = crate::validate_limit(
        args.limit,
        DEFAULT_CHANGE_BATCH as i64,
        MAX_CHANGE_BATCH as i64,
    )? as usize;
    // One past the page, so the look-ahead that settles `done` is normally
    // served by the same indexed read.
    let mut query = ChangeQuery::default().batch(limit + 1);
    if let Some(kinds) = parse_kinds(args.kinds)? {
        query = query.kinds(kinds);
    }
    if let Some(name) = &consumer {
        query = query.consumer(name.clone());
    }
    if let Some(session) = args.session {
        let source = validate_identity(session.source, "session.source")?;
        let session_id = validate_identity(session.session_id, "session.sessionId")?;
        query = query.session(source, session_id);
    }
    let path = db_path(args.db_path);
    if !path.exists() {
        // A missing database has no feed; reading it must not create one.
        let start = WireWatermark::from_watermark(Watermark::START);
        return serde_json::to_string(&ChangesPage {
            changes: Vec::new(),
            position: start.clone(),
            head: start,
            done: true,
            consumer,
        })
        .map_err(|error| native_error("DATABASE_QUERY_FAILED", error));
    }
    let from = match from {
        FeedStart::Start => Watermark::START,
        FeedStart::Consumer => Watermark::CONSUMER,
        FeedStart::At(watermark) => watermark,
    };
    read_page(path, from, query, limit)
}

/// Move a named cursor to `position`: open the feed there under the same
/// name and kind set, and commit before reading anything, which is exactly
/// `Changes::commit` at that position. Forward-only and kind-set-bound, as
/// the facade enforces.
fn commit(args: CommitArgs) -> napi::Result<String> {
    let consumer = consumer_name(args.consumer)?;
    let position = args.position.parse("position")?;
    if position == Watermark::START || position.epoch == 0 {
        return Err(native_error(
            "INVALID_ARGUMENT",
            "position must be a watermark a page of this store returned",
        ));
    }
    let mut query = ChangeQuery::default().consumer(consumer.clone()).batch(1);
    if let Some(kinds) = parse_kinds(args.kinds)? {
        query = query.kinds(kinds);
    }
    let path = db_path(args.db_path);
    if !path.exists() {
        return Err(native_error(
            "WATERMARK_AHEAD_OF_STORE",
            format!(
                "no database at {}: the position names no store",
                path.display()
            ),
        ));
    }
    let store =
        open_store(&path, false).map_err(|error| database_error(&path, format!("{error:#}")))?;
    let drain = store.changes_since(position, query).map_err(facade_error)?;
    let cursor = drain.commit().map_err(facade_error)?;
    serde_json::to_string(&CommittedCursor {
        consumer,
        cursor: WireWatermark::from_watermark(cursor),
    })
    .map_err(|error| native_error("DATABASE_QUERY_FAILED", error))
}

#[cfg(test)]
mod tests {
    use crate::session_store::dispatch;
    use ai_hist::{insert_session_marker, open_db, NewSessionMarker};
    use serde_json::{json, Value};

    fn call(op: &str, args: Value) -> Result<Value, String> {
        dispatch(op, &args.to_string())
            .map(|answer| serde_json::from_str(&answer).expect("dispatcher answers JSON"))
            .map_err(|error| error.reason.clone())
    }

    fn seeded(markers: usize) -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        let conn = open_db(&dir.path().join("history.db")).unwrap();
        for index in 0..markers {
            insert_session_marker(
                &conn,
                "claude",
                "sess-1",
                &NewSessionMarker {
                    marker_uid: &format!("m{index}"),
                    ts_ms: Some(index as i64),
                    kind: "summary",
                    text: Some("marker"),
                    ..Default::default()
                },
            )
            .unwrap();
        }
        dir
    }

    fn uids(page: &Value) -> Vec<String> {
        page["changes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|change| change["recordKey"].as_str().unwrap().to_string())
            .collect()
    }

    #[test]
    fn a_missing_database_is_an_empty_finished_feed_and_is_not_created() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("absent.db");
        let page = call("changes", json!({ "dbPath": db })).unwrap();
        assert_eq!(page["changes"], json!([]));
        assert_eq!(page["done"], json!(true));
        assert_eq!(
            page["head"],
            json!({ "epoch": "0000000000000000", "revision": 0 })
        );
        assert!(!db.exists());
    }

    #[test]
    fn pages_resume_from_the_returned_position_without_gaps_or_repeats() {
        let dir = seeded(5);
        let db = dir.path().join("history.db");
        let kinds = json!(["session_marker"]);
        let first = call(
            "changes",
            json!({ "dbPath": db, "kinds": kinds, "limit": 2 }),
        )
        .unwrap();
        assert_eq!(uids(&first), ["m0", "m1"]);
        assert_eq!(first["done"], json!(false));
        let change = &first["changes"][0];
        assert_eq!(change["kind"], "session_marker");
        assert_eq!(change["op"], "upsert");
        assert_eq!(change["source"], "claude");
        assert_eq!(change["sessionId"], "sess-1");
        assert_eq!(change["key"][0], "session_marker");
        assert_eq!(change["columns"]["marker_uid"], "m0");
        assert!(change["columns"].get("revision").is_none());
        assert_eq!(first["head"]["epoch"].as_str().unwrap().len(), 16);

        let mut from = first["position"].clone();
        let mut seen = uids(&first);
        loop {
            let page = call(
                "changes",
                json!({ "dbPath": db, "kinds": kinds, "limit": 2, "from": from }),
            )
            .unwrap();
            seen.extend(uids(&page));
            from = page["position"].clone();
            if page["done"] == json!(true) {
                assert_eq!(page["position"], page["head"]);
                break;
            }
        }
        assert_eq!(seen, ["m0", "m1", "m2", "m3", "m4"]);
    }

    #[test]
    fn a_named_cursor_moves_only_on_commit() {
        let dir = seeded(3);
        let db = dir.path().join("history.db");
        let kinds = json!(["session_marker"]);
        let args = json!({ "dbPath": db, "consumer": "burn", "kinds": kinds, "limit": 2 });
        let first = call("changes", args.clone()).unwrap();
        assert_eq!(uids(&first), ["m0", "m1"]);
        assert_eq!(first["consumer"], "burn");

        // Unacknowledged, the same page is served again.
        let again = call("changes", args.clone()).unwrap();
        assert_eq!(uids(&again), ["m0", "m1"]);

        let committed = call(
            "commit_changes",
            json!({ "dbPath": db, "consumer": "burn", "kinds": kinds, "position": first["position"] }),
        )
        .unwrap();
        assert_eq!(committed["cursor"], first["position"]);

        let rest = call("changes", args).unwrap();
        assert_eq!(uids(&rest), ["m2"]);
        assert_eq!(rest["done"], json!(true));

        // The cursor is bound to the kind set it was committed for.
        let error = call(
            "changes",
            json!({ "dbPath": db, "consumer": "burn", "kinds": ["session"] }),
        )
        .unwrap_err();
        assert!(error.contains("CONSUMER_KINDS_MISMATCH"), "{error}");
    }

    #[test]
    fn a_filtered_page_ending_below_the_head_reports_done() {
        let dir = seeded(1);
        let db = dir.path().join("history.db");
        // An unrelated later write moves the head past the only match.
        let conn = open_db(&db).unwrap();
        insert_session_marker(
            &conn,
            "claude",
            "sess-2",
            &NewSessionMarker {
                marker_uid: "other",
                ts_ms: Some(9),
                kind: "summary",
                text: Some("marker"),
                ..Default::default()
            },
        )
        .unwrap();
        drop(conn);
        let args = json!({
            "dbPath": db,
            "kinds": ["session_marker"],
            "session": { "source": "claude", "sessionId": "sess-1" },
            "limit": 1,
        });
        let page = call("changes", args).unwrap();
        assert_eq!(uids(&page), ["m0"]);
        assert_eq!(page["done"], json!(true));
        assert_eq!(page["position"], page["head"]);

        // A full page with a match still to come is not done, and stops at
        // the last change it returned.
        let dir = seeded(2);
        let db = dir.path().join("history.db");
        let page = call(
            "changes",
            json!({ "dbPath": db, "kinds": ["session_marker"], "limit": 1 }),
        )
        .unwrap();
        assert_eq!(uids(&page), ["m0"]);
        assert_eq!(page["done"], json!(false));
        assert_eq!(page["position"]["revision"], page["changes"][0]["revision"]);
    }

    #[test]
    fn a_database_from_before_the_feed_is_migrated_on_first_read() {
        let dir = seeded(2);
        let db = dir.path().join("history.db");
        // Strip a piece of the feed schema only: the session-evidence
        // indexes the facade's read-only gate checks are all still there.
        let conn = open_db(&db).unwrap();
        conn.execute_batch("DROP TABLE consumer_cursors;").unwrap();
        drop(conn);
        let conn = ai_hist::open_db_readonly(&db).unwrap();
        assert!(!ai_hist::schema_is_current(&conn).unwrap());
        drop(conn);

        let page = call(
            "changes",
            json!({ "dbPath": db, "kinds": ["session_marker"] }),
        )
        .unwrap();
        assert_eq!(uids(&page), ["m0", "m1"]);
        assert_eq!(page["done"], json!(true));
    }

    #[test]
    fn a_watermark_from_another_store_is_refused() {
        let dir = seeded(1);
        let db = dir.path().join("history.db");
        let error = call(
            "changes",
            json!({ "dbPath": db, "from": { "epoch": "0000000000000002", "revision": 1 } }),
        )
        .unwrap_err();
        assert!(error.contains("WATERMARK_AHEAD_OF_STORE"), "{error}");
    }

    #[test]
    fn malformed_feed_arguments_are_invalid_arguments() {
        for (op, args, expected) in [
            ("changes", json!({ "from": "consumer" }), "needs consumer"),
            ("changes", json!({ "from": "later" }), "from must be"),
            (
                "changes",
                json!({ "kinds": ["nope"] }),
                "unknown change kind",
            ),
            ("changes", json!({ "limit": 0 }), "INVALID_ARGUMENT"),
            ("changes", json!({ "bogus": 1 }), "INVALID_ARGUMENT"),
            (
                "changes",
                json!({ "from": { "epoch": "xyz", "revision": 1 } }),
                "hex digits",
            ),
            (
                "commit_changes",
                json!({ "consumer": "c", "position": { "epoch": "0", "revision": 0 } }),
                "position must be",
            ),
        ] {
            let error = call(op, args).unwrap_err();
            assert!(error.contains("INVALID_ARGUMENT"), "{error}");
            assert!(error.contains(expected), "{expected}: {error}");
        }
    }
}

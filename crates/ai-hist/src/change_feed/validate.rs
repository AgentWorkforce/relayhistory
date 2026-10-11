//! The argument checks a change-feed read makes before touching the store,
//! shared by [`SessionStore::changes_since`] and
//! [`SessionStore::resumes_from`] so the second never answers for a query the
//! first refuses.

use super::*;

/// [`Error::InvalidArgument`] for a `from` and `query` no drain accepts.
/// `operation` names the call in the message.
pub(super) fn query(
    from: Watermark,
    query: &ChangeQuery,
    operation: &str,
) -> std::result::Result<(), Error> {
    if from == Watermark::CONSUMER && query.consumer.is_none() {
        return Err(Error::InvalidArgument(format!(
            "{operation}: Watermark::CONSUMER needs ChangeQuery::consumer to name the cursor"
        )));
    }
    let Some(session) = &query.session else {
        return Ok(());
    };
    if query.consumer.is_some() {
        return Err(Error::InvalidArgument(format!(
            "{operation}: ChangeQuery::session is a one-shot read and cannot name a \
             consumer; a cursor committed from one session's changes would skip every \
             other session's. Drain the session from Watermark::START without a \
             consumer, and keep the named cursor for the whole feed"
        )));
    }
    if session.source_name.is_empty() || session.session_id.is_empty() {
        return Err(Error::InvalidArgument(format!(
            "{operation}: ChangeQuery::session needs a nonempty source and session id"
        )));
    }
    Ok(())
}

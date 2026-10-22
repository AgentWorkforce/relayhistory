use super::*;

/// Enumerate every native session before visiting any body, and keep one
/// SQLite snapshot open while independent sessions are decoded. A malformed
/// body is a per-session result; a failed row query is not deletion proof.
pub(crate) fn scan(
    context: &AcquisitionContext<'_>,
    path: &Path,
    mut visit: impl FnMut(&str, Result<Session>) -> Result<()>,
) -> Result<Vec<String>> {
    read(context, path, |conn| {
        let rows = sessions(conn, context.roots, path, MAX_SESSIONS + 1, None)?;
        ensure!(
            rows.len() <= MAX_SESSIONS,
            "Goose session capture exceeds safety limit"
        );
        let ids = rows.iter().map(|row| row.id.clone()).collect();
        for mut row in rows {
            check_capture_cancelled()?;
            let id = row.id.clone();
            let decoded = messages(conn, &row.native_id).map(|messages| {
                row.messages = messages;
                row
            });
            visit(&id, decoded)?;
        }
        Ok(ids)
    })
}

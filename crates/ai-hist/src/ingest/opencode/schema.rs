//! Native OpenCode schema inspection shared by its SQLite loaders.
use super::*;

/// Whether `columns` holds every one of `names`: the columns a loader reads.
pub(crate) fn has_columns(columns: &BTreeSet<String>, names: &[&str]) -> bool {
    names.iter().all(|name| columns.contains(*name))
}

pub(crate) fn table_columns(conn: &Connection, table: &str) -> Result<BTreeSet<String>> {
    Ok(conn
        .prepare(&format!("SELECT name FROM pragma_table_info('{table}')"))?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<BTreeSet<String>>>()?)
}

pub(crate) fn optional_column<'a>(columns: &BTreeSet<String>, name: &'a str) -> &'a str {
    if columns.contains(name) {
        name
    } else {
        "NULL"
    }
}

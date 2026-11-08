//! Convert validated evidence scalars to SQLite values without truncation.
use anyhow::{Context, Result};
use serde_json::Value;

pub(super) fn sql_value(value: &Value) -> Result<rusqlite::types::Value> {
    use rusqlite::types::Value as Sql;
    Ok(match value {
        Value::Null => Sql::Null,
        Value::Bool(value) => Sql::Integer(i64::from(*value)),
        Value::Number(value) => (value.as_i64().map(Sql::Integer))
            .or_else(|| value.as_f64().map(Sql::Real))
            .context("evidence number is not representable as a SQLite REAL")?,
        Value::String(value) => Sql::Text(value.clone()),
        _ => anyhow::bail!("validated evidence contains only SQLite scalars"),
    })
}

//! Convert validated evidence scalars to SQLite values without truncation.
use anyhow::{Context, Result};
use serde_json::Value;

pub(super) fn sql_value(value: &Value) -> Result<rusqlite::types::Value> {
    use rusqlite::types::Value as Sql;
    Ok(match value {
        Value::Null => Sql::Null,
        Value::Bool(value) => Sql::Integer(i64::from(*value)),
        Value::Number(value) => match value.as_i64() {
            Some(number) => Sql::Integer(number),
            None if value.is_u64() => {
                anyhow::bail!("evidence integer exceeds the SQLite INTEGER range")
            }
            None => Sql::Real(
                value
                    .as_f64()
                    .context("evidence number is not representable as a SQLite REAL")?,
            ),
        },
        Value::String(value) => Sql::Text(value.clone()),
        _ => anyhow::bail!("validated evidence contains only SQLite scalars"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::types::Value as Sql;

    #[test]
    fn scalar_numbers_preserve_integer_boundaries_and_fractions() {
        for integer in [i64::MIN, 0, i64::MAX] {
            assert_eq!(
                sql_value(&Value::from(integer)).unwrap(),
                Sql::Integer(integer)
            );
        }
        assert_eq!(sql_value(&Value::from(0.5)).unwrap(), Sql::Real(0.5));
        for integer in [i64::MAX as u64 + 1, u64::MAX] {
            let error = sql_value(&Value::from(integer)).unwrap_err();
            assert!(error
                .to_string()
                .contains("exceeds the SQLite INTEGER range"));
        }
    }
}

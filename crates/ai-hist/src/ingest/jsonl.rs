//! One rule for reading a JSONL row, shared by everything that reads one.
//!
//! Two passes look at the same provider files: the parse, which turns rows
//! into evidence, and the count, which reports `records_parsed`. They used to
//! decide separately what a row was, and they disagreed — the parser read a
//! valid final record that had no trailing newline, and the counter stopped at
//! the missing newline without testing it, so a one-line transcript with no
//! final newline produced an event and a `records_parsed` of zero, which was
//! then checkpointed.
//!
//! So the rule lives in [`classify`], and both passes call it. The count still
//! streams (an `updates.jsonl` is routinely the largest file in a session and
//! must not be held in memory) while the parse works over a whole string, but
//! neither decides for itself what a row is.

use anyhow::{Context, Result};
use serde_json::Value;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::Path;

/// One row of a JSONL file, and whether the writer finished it.
///
/// The distinction is the whole point. A row that ends in a newline is a row
/// the provider finished writing, so if it does not parse the file is damaged.
/// Only the final piece of a file can lack its newline, and that one may be a
/// record still being written.
pub(crate) struct JsonlRow<'a> {
    pub(crate) text: &'a str,
    /// The row was newline-terminated, so the writer finished it.
    pub(crate) complete: bool,
}

/// What one row is.
pub(crate) enum Row {
    /// A record. Parse and count agree that this is one.
    Record(Value),
    /// Nothing: a blank line, or an unterminated tail still being written.
    Skip,
    /// A finished row that is not JSON — the file is damaged.
    Damaged,
}

/// The rule. Every reader of a JSONL row goes through here.
///
/// A blank line is nothing. A row that parses is a record, terminated or not:
/// not every writer terminates its final line, and discarding a whole record
/// over a missing newline loses evidence just as surely as mis-reading one.
/// A row that does not parse is damage **if the writer finished it**, and
/// otherwise a record still being written.
pub(crate) fn classify(text: &str, complete: bool) -> Row {
    if text.trim().is_empty() {
        return Row::Skip;
    }
    match serde_json::from_str(text) {
        Ok(value) => Row::Record(value),
        Err(_) if !complete => Row::Skip,
        Err(_) => Row::Damaged,
    }
}

/// Split a whole JSONL file into rows, saying for each whether it is complete.
pub(crate) fn rows(contents: &str) -> impl Iterator<Item = JsonlRow<'_>> {
    contents
        .split_inclusive('\n')
        .map(|row| match row.strip_suffix('\n') {
            Some(text) => JsonlRow {
                text: text.strip_suffix('\r').unwrap_or(text),
                complete: true,
            },
            // Only the final piece can lack its newline.
            None => JsonlRow {
                text: row,
                complete: false,
            },
        })
}

/// Parse one row for a reader that **replaces** what it reads.
///
/// `Ok(None)` is a row that is nothing. A damaged row is an error, because the
/// caller is about to replace a session's stored evidence with what it read:
/// silently dropping the row would commit a snapshot missing a turn the
/// provider did write, and save a change stamp that stops the next run from
/// ever looking again.
pub(crate) fn parse_row(row: JsonlRow<'_>, path: &Path, number: usize) -> Result<Option<Value>> {
    super::check_capture_cancelled()?;
    match classify(row.text, row.complete) {
        Row::Record(value) => Ok(Some(value)),
        Row::Skip => Ok(None),
        Row::Damaged => Err(anyhow::anyhow!("invalid JSON"))
            .with_context(|| format!("{}: line {number} is not valid JSON", path.display())),
    }
}

/// How many records a JSONL file holds, by the same rule the parse applies.
///
/// Streamed a line at a time: these files are large, and the count runs on
/// every parse. Damage is not counted and is not reported here either — the
/// parse is what fails on a damaged file, and it reads the same rows.
pub(crate) fn count_records(path: &Path) -> Result<i64> {
    let mut reader = BufReader::new(fs::File::open(path)?);
    let mut records = 0;
    let mut line = String::new();
    loop {
        super::check_capture_cancelled()?;
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            break;
        }
        let complete = line.ends_with('\n');
        let text = line
            .strip_suffix('\n')
            .map(|text| text.strip_suffix('\r').unwrap_or(text))
            .unwrap_or(&line);
        if matches!(classify(text, complete), Row::Record(_)) {
            records += 1;
        }
    }
    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The count and the parse answer the same question about the same rows.
    ///
    /// They are two passes over one file, and when they decided separately the
    /// count understated a valid unterminated final record as zero — which was
    /// then checkpointed as the session's record total.
    #[test]
    fn the_count_agrees_with_the_parse_on_every_row_shape() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rows.jsonl");
        let parsed = |contents: &str| -> Option<usize> {
            rows(contents)
                .enumerate()
                .try_fold(0, |seen, (number, row)| {
                    parse_row(row, &path, number + 1)
                        .map(|value| seen + usize::from(value.is_some()))
                })
                .ok()
        };
        let counted = |contents: &str| -> i64 {
            fs::write(&path, contents).unwrap();
            count_records(&path).unwrap()
        };

        for (name, contents, expected) in [
            ("a terminated record", "{\"a\":1}\n", Some(1)),
            // The case that was wrong: valid, final, unterminated.
            ("an unterminated record", "{\"a\":1}", Some(1)),
            ("both", "{\"a\":1}\n{\"b\":2}", Some(2)),
            ("a half-written tail", "{\"a\":1}\n{\"b\":", Some(1)),
            ("a blank line", "{\"a\":1}\n\n", Some(1)),
            ("a trailing CRLF", "{\"a\":1}\r\n", Some(1)),
            ("an empty file", "", Some(0)),
        ] {
            assert_eq!(parsed(contents), expected, "parse disagreed on {name}");
            assert_eq!(
                counted(contents),
                expected.unwrap() as i64,
                "count disagreed on {name}"
            );
        }

        // The one place they differ, and deliberately: a finished row that is
        // not JSON fails the parse, while the count skips it and leaves the
        // failing to the parse that reads the same rows.
        let damaged = "{\"a\":1}\n{\"b\":\n";
        assert_eq!(parsed(damaged), None, "a damaged row must fail the parse");
        assert_eq!(counted(damaged), 1);
    }
}

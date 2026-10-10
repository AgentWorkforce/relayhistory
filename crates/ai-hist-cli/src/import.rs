//! `import`: history rows from an export (JSONL or SQLite) into this database.

use super::*;

/// Imports `path`, or with `dry_run` reports what it would import.
pub(crate) fn import_history(conn: &Connection, path: &Path, dry_run: bool) -> Result<()> {
    let entries = if matches!(
        path.extension().and_then(|s| s.to_str()),
        Some("db" | "sqlite")
    ) {
        load_sqlite_entries(path)?
    } else {
        load_jsonl_entries(path)?
    };
    // Prompts under a retired source (an export from an earlier release) are
    // never written, so they are counted apart rather than as duplicates.
    let (retired, entries): (Vec<_>, Vec<_>) = entries
        .into_iter()
        .partition(|entry| ai_hist::is_retired_source(&entry.source));
    let retired =
        (!retired.is_empty()).then(|| format!("{} skipped (retired source)", retired.len()));
    if entries.is_empty() && retired.is_none() {
        println!("No entries found in file.");
        return Ok(());
    }
    if dry_run {
        let suffix = retired
            .as_ref()
            .map(|r| format!(", {r}"))
            .unwrap_or_default();
        println!(
            "[dry-run] {} entries in {} - none written{suffix}.",
            entries.len(),
            path.display()
        );
        println!();
        for entry in entries.iter().take(5) {
            println!(
                "  {}  ({}){}  {}",
                format_datetime(entry.timestamp_ms),
                entry.source,
                entry
                    .project
                    .as_ref()
                    .map(|p| format!(" [{p}]"))
                    .unwrap_or_default(),
                entry
                    .prompt
                    .chars()
                    .take(80)
                    .collect::<String>()
                    .replace('\n', " ")
            );
        }
        if entries.len() > 5 {
            println!("  ... and {} more", entries.len() - 5);
        }
        return Ok(());
    }
    let total = entries.len();
    let inserted = import_json(conn, &entries)?;
    let skipped = total.saturating_sub(inserted);
    let mut parts = vec![format!("+{inserted} new entries")];
    if skipped > 0 {
        parts.push(format!("{skipped} already existed"));
    }
    parts.extend(retired);
    println!("Imported from {}: {}", path.display(), parts.join(", "));
    Ok(())
}

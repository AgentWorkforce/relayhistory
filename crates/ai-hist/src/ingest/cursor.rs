//! Cursor agent-transcript record parsing.
//!
//! One place for the interpretation of
//! `~/.cursor/projects/<encoded-path>/agent-transcripts/<id>/<id>.jsonl`, so
//! shallow discovery, full sync and targeted hydration cannot drift apart.
//!
//! ## What Cursor actually writes
//!
//! Cursor publishes no schema for this file. The shapes handled here were
//! characterized from public write-ups of real transcript corpora; the
//! provenance of every field — verified against a real corpus, inferred from a
//! description, or unverified — is recorded in `docs/session-catalog.md`
//! ("How each adapter works → cursor"). The short version:
//!
//! * Records are bare `{"role": "user"|"assistant", "message": {"content": …}}`.
//!   `role` is at the **top level**; there is no `message.role` and no record
//!   `type`.
//! * `message.content` is an array of blocks, or (older rows) a bare string.
//! * Observed block types are `text` and an **id-less** `tool_use`
//!   (`{"type":"tool_use","name":…,"input":…}`), plus a `turn_ended` marker.
//! * A corpus of 104 real transcripts from Cursor IDE 3.13.25 carried **no**
//!   `tool_result` block, no `thinking` block, no record `timestamp`, no
//!   session id, no `cwd`, no `model` and no `usage`.
//! * The only time signal inside the file is a localized
//!   `<timestamp>Wednesday, Sep 16, 2026, 3:37 PM (UTC-4)</timestamp>` tag the
//!   client injects next to the `<user_query>` envelope on user turns.
//!
//! This parser therefore reads `model`, `usage`, `thinking`, `tool_result` and
//! block ids **when a build writes them** and reports their absence honestly
//! rather than inventing them.
//!
//! ## Identity
//!
//! Because `tool_use` blocks are id-less and records carry no uuid, event and
//! tool identity is derived from the record's **byte offset** in the file. An
//! offset is stable across incremental reads and across a whole-file re-parse,
//! and it resets exactly when Cursor rewrites the file — which is also when the
//! byte cursor resets.

use serde_json::Value;

/// A Cursor tool-call argument payload that names a file the agent wrote.
///
/// Cursor has shipped three tool-name dialects: the unprefixed
/// `Write`/`StrReplace`/`ApplyPatch` set used by Claude- and Grok-served
/// models, the `functions.*` namespaced set used by GPT-served models, and an
/// older snake_case `edit_file`/`write_file` set. All three are recognized
/// here; a name Cursor does not write simply never matches.
pub(crate) fn is_file_edit_tool(name: &str) -> bool {
    matches!(
        normalize_tool_name(name),
        "write"
            | "strreplace"
            | "applypatch"
            | "delete"
            | "editnotebook"
            | "edit_file"
            | "write_file"
            | "create_file"
            | "delete_file"
            | "search_replace"
            | "apply_patch"
            | "edit_notebook"
    )
}

/// Lower-case the tool name and drop the provider namespace prefix, so
/// `functions.ApplyPatch`, `ApplyPatch` and `apply_patch` classify alike.
fn normalize_tool_name(name: &str) -> &str {
    let bare = name.rsplit('.').next().unwrap_or(name);
    // `str::to_lowercase` would allocate; these names are ASCII, and the
    // matcher above lists both the lowered CamelCase and the snake_case forms.
    LOWERED
        .iter()
        .find(|(camel, _)| camel.eq_ignore_ascii_case(bare))
        .map(|(_, lowered)| *lowered)
        .unwrap_or(bare)
}

/// CamelCase tool names mapped to the lower-case token the matcher uses.
const LOWERED: &[(&str, &str)] = &[
    ("Write", "write"),
    ("StrReplace", "strreplace"),
    ("ApplyPatch", "applypatch"),
    ("Delete", "delete"),
    ("EditNotebook", "editnotebook"),
    ("Read", "read"),
    ("ReadFile", "read"),
    ("Grep", "grep"),
    ("Glob", "glob"),
    ("Shell", "shell"),
    ("AwaitShell", "awaitshell"),
    ("Task", "task"),
    ("Subagent", "subagent"),
];

/// Tools whose call delegates work to a Cursor subagent.
///
/// Cursor records the *spawn* (the call happened) but writes no child
/// transcript id into the block, so the delegation is evidence without a
/// stable child identity. See `docs/session-catalog.md`.
pub(crate) fn is_subagent_tool(name: &str) -> bool {
    matches!(normalize_tool_name(name), "task" | "subagent")
}

/// The file a Cursor tool call touched, or the command it ran.
///
/// `ApplyPatch` is the awkward case: its `input` is the patch **text**, not an
/// object, so the path has to come out of the patch header.
pub(crate) fn pick_tool_target(name: &str, input: &Value) -> Option<String> {
    if let Some(patch) = input.as_str() {
        return patch_target(patch).or_else(|| Some(patch.lines().next()?.trim().to_string()));
    }
    let obj = input.as_object()?;
    let get = |key: &str| {
        obj.get(key)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
    };
    if let Some(patch) = obj.get("patch").and_then(Value::as_str) {
        if let Some(target) = patch_target(patch) {
            return Some(target);
        }
    }
    match normalize_tool_name(name) {
        "shell" | "awaitshell" => get("command").or_else(|| get("cmd")),
        "grep" | "glob" => get("pattern")
            .or_else(|| get("query"))
            .or_else(|| get("path")),
        _ => get("path")
            .or_else(|| get("file_path"))
            .or_else(|| get("filePath"))
            .or_else(|| get("target_file"))
            .or_else(|| get("relative_workspace_path"))
            .or_else(|| get("notebook_path"))
            .or_else(|| get("uri"))
            .or_else(|| get("url"))
            .or_else(|| get("query"))
            .or_else(|| get("command")),
    }
}

/// One file's slice of a patch that may touch several.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PatchFile {
    pub path: String,
    /// Only the lines belonging to this file, so line counts and the stored
    /// patch are per file rather than per call.
    pub patch: String,
}

/// The file named by a patch, from either the `apply_patch` envelope Cursor
/// uses or a unified-diff header.
///
/// This is the *first* file only, and exists for the `tool_calls.target`
/// column, which names one thing. Anything that records edits must use
/// [`split_patch_files`]: a single `ApplyPatch` call routinely rewrites
/// several files, and taking only the first silently drops the rest.
pub(crate) fn patch_target(patch: &str) -> Option<String> {
    split_patch_files(patch).into_iter().next().map(|f| f.path)
}

/// Every file a patch touches, each with its own slice of the patch text.
///
/// Handles both shapes Cursor emits: the `*** Begin Patch` envelope, whose
/// files are introduced by `*** Update/Add/Delete File:`, and a plain unified
/// diff, whose files are introduced by a `--- ` / `+++ ` header pair. Lines
/// before the first header (the envelope preamble) belong to no file and are
/// dropped, as is a trailing `*** End Patch`.
pub(crate) fn split_patch_files(patch: &str) -> Vec<PatchFile> {
    let lines: Vec<&str> = patch.lines().collect();
    // Which shape is this? An envelope names its files with `*** … File:`
    // markers, and inside one a `---`/`+++` pair is ordinary diff content —
    // a Markdown horizontal rule being replaced, say. Only a patch with no
    // envelope markers at all is read as a bare unified diff.
    let envelope = lines.iter().any(|line| {
        let line = line.trim();
        ["*** Update File: ", "*** Add File: ", "*** Delete File: "]
            .iter()
            .any(|marker| line.starts_with(marker))
    });
    let mut files: Vec<PatchFile> = Vec::new();
    let mut current: Option<(String, Vec<&str>)> = None;
    // Inside a hunk, every line is content until the next file header or the
    // envelope terminator, so header detection has to stand down.
    let mut in_hunk = false;
    let mut index = 0;
    while index < lines.len() {
        let raw = lines[index];
        let line = raw.trim();
        let mut header: Option<String> = None;
        // Matched against the *raw* line, not the trimmed one. A unified-diff
        // context line is its content prefixed with a space, so
        // ` *** Update File: example.md` is a quotation of a marker inside a
        // hunk, not a marker. Trimming first turned that into a header,
        // opening a phantom file and carrying the rest of the patch away from
        // the file it belongs to. Only the extracted path is trimmed.
        for marker in ["*** Update File: ", "*** Add File: ", "*** Delete File: "] {
            if let Some(rest) = raw.strip_prefix(marker) {
                let rest = rest.trim();
                if !rest.is_empty() {
                    header = Some(rest.to_string());
                }
                break;
            }
        }
        // A `--- ` line opens a file only when a `+++ ` line follows it. That
        // alone is not enough inside a hunk, where it is content: a deleted
        // `-- old` renders as `--- old` and an added `++ new` as `+++ new`, so
        // reading those as headers opens a phantom file and splits the real
        // file's patch in half. The next file's header also arrives while the
        // previous hunk is still open, though, so "not in a hunk" cannot be
        // the whole rule either. What separates them is what comes next: a
        // header pair is followed by `@@`, hunk content is not.
        if !envelope && header.is_none() && line.starts_with("--- ") {
            let opens_a_hunk = lines
                .get(index + 2)
                .is_some_and(|after| after.trim_start().starts_with("@@"));
            if let Some(next) = lines
                .get(index + 1)
                .map(|next| next.trim())
                .filter(|_| !in_hunk || opens_a_hunk)
            {
                if let Some(rest) = next.strip_prefix("+++ ") {
                    header = Some(unified_diff_path(rest).unwrap_or_else(|| {
                        unified_diff_path(line.strip_prefix("--- ").unwrap_or_default())
                            .unwrap_or_default()
                    }));
                }
            }
        }
        if let Some(path) = header {
            if let Some((path, body)) = current.take() {
                files.push(PatchFile {
                    path,
                    patch: body.join("\n"),
                });
            }
            in_hunk = false;
            if !path.is_empty() {
                current = Some((path, vec![raw]));
                index += 1;
                continue;
            }
        }
        if line == "*** End Patch" {
            if let Some((path, body)) = current.take() {
                files.push(PatchFile {
                    path,
                    patch: body.join("\n"),
                });
            }
            in_hunk = false;
            index += 1;
            continue;
        }
        // `@@` opens a hunk; everything after it is content until the next
        // file header.
        if line.starts_with("@@") {
            in_hunk = true;
        }
        if let Some((_, body)) = current.as_mut() {
            body.push(raw);
        }
        index += 1;
    }
    if let Some((path, body)) = current.take() {
        files.push(PatchFile {
            path,
            patch: body.join("\n"),
        });
    }
    files
}

/// `b/src/main.rs` and `a/src/main.rs` both name `src/main.rs`; `/dev/null`
/// names nothing.
fn unified_diff_path(raw: &str) -> Option<String> {
    // A unified-diff header may carry a tab-separated timestamp after the path.
    let path = raw.trim().split('\t').next()?.trim();
    let path = path
        .strip_prefix("b/")
        .or_else(|| path.strip_prefix("a/"))
        .unwrap_or(path);
    (!path.is_empty() && path != "/dev/null").then(|| path.to_string())
}

/// The patch text a Cursor edit tool carried, if it carried one.
///
/// `ApplyPatch` passes the patch as the whole `input`; the other edit tools
/// pass an object that may carry a `patch`/`diff` field. `StrReplace` carries
/// old/new strings rather than a diff, so it legitimately yields `None` and
/// the edit is recorded with no line counts.
pub(crate) fn patch_text(input: &Value) -> Option<&str> {
    if let Some(patch) = input.as_str() {
        return Some(patch);
    }
    let obj = input.as_object()?;
    for key in ["patch", "diff", "unified_diff", "structuredPatch"] {
        if let Some(text) = obj.get(key).and_then(Value::as_str) {
            if !text.trim().is_empty() {
                return Some(text);
            }
        }
    }
    None
}

/// Strip the `<timestamp>` / `<user_query>` framing Cursor's client injects
/// around a human turn and return the prompt the person actually typed.
///
/// A row with no framing is returned as-is: older rows carry the bare prompt.
pub(crate) fn unwrap_user_text(text: &str) -> String {
    if let Some(inner) = tag_content(text, "user_query") {
        return inner.trim().to_string();
    }
    let mut cleaned = text.to_string();
    while let Some(start) = cleaned.find("<timestamp>") {
        let Some(end) = cleaned[start..].find("</timestamp>") else {
            break;
        };
        cleaned.replace_range(start..start + end + "</timestamp>".len(), "");
    }
    cleaned.trim().to_string()
}

/// The inner text of the first `<tag>…</tag>` pair, if the text has one.
fn tag_content<'a>(text: &'a str, tag: &str) -> Option<&'a str> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = text.find(&open)? + open.len();
    let end = text[start..].find(&close)? + start;
    Some(&text[start..end])
}

/// Milliseconds for the `<timestamp>` tag Cursor injects on a user turn.
///
/// The tag is a **localized** human string — `Wednesday, Sep 16, 2026, 3:37 PM
/// (UTC-4)` — not an ISO instant, so it is parsed explicitly. Anything that
/// does not match (a non-English locale, a build that changes the wording) is
/// unreadable rather than guessed, and the caller falls back to the file mtime
/// and says so in a diagnostic.
pub(crate) fn timestamp_from_text(text: &str) -> Option<i64> {
    parse_cursor_timestamp(tag_content(text, "timestamp")?)
}

/// Parse one localized Cursor timestamp string into epoch milliseconds.
pub(crate) fn parse_cursor_timestamp(raw: &str) -> Option<i64> {
    let raw = raw.trim();
    // Drop the weekday prefix ("Wednesday, ") when present. A weekday is one
    // alphabetic word; "Sep 16" is not, so a transcript without the weekday
    // still parses.
    let rest = match raw.split_once(", ") {
        Some((head, tail)) if head.chars().all(|c| c.is_ascii_alphabetic()) => tail,
        _ => raw,
    };
    // "Sep 16, 2026, 3:37 PM (UTC-4)"
    let (date, rest) = rest.split_once(", ")?;
    let (year_part, clock_part) = rest.split_once(", ")?;
    let (month_name, day) = date.split_once(' ')?;
    let month = month_number(month_name)?;
    let day: u32 = day.trim().parse().ok()?;
    let year: i32 = year_part.trim().parse().ok()?;

    // The zone is required: a build that stops printing it has changed the
    // format, and defaulting to UTC would move every timestamp by hours
    // without ever failing.
    let (clock, zone) = clock_part.split_once('(')?;
    let (clock, zone) = (clock.trim(), zone.trim_end_matches(')').trim());
    let (hour, minute, second) = parse_clock(clock)?;
    let offset_seconds = parse_utc_offset(zone)?;

    let date = chrono::NaiveDate::from_ymd_opt(year, month, day)?;
    let time = chrono::NaiveTime::from_hms_opt(hour, minute, second)?;
    let naive = date.and_time(time);
    let offset = chrono::FixedOffset::east_opt(offset_seconds)?;
    Some(
        naive
            .and_local_timezone(offset)
            .single()?
            .timestamp_millis(),
    )
}

fn month_number(name: &str) -> Option<u32> {
    const MONTHS: [&str; 12] = [
        "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec",
    ];
    let lowered = name.trim().to_ascii_lowercase();
    MONTHS
        .iter()
        .position(|month| lowered.starts_with(month))
        .map(|index| index as u32 + 1)
}

/// `3:37 PM`, `3:37:05 PM` and 24-hour `15:37` all appear across locales.
fn parse_clock(clock: &str) -> Option<(u32, u32, u32)> {
    let mut parts = clock.split_whitespace();
    let digits = parts.next()?;
    let meridiem = parts.next().map(|m| m.to_ascii_uppercase());
    let mut fields = digits.split(':');
    let mut hour: u32 = fields.next()?.parse().ok()?;
    let minute: u32 = fields.next()?.parse().ok()?;
    // Absent seconds mean zero; seconds that are present but unparseable, or
    // a fourth field, mean this is not a clock. Defaulting them to zero would
    // turn a malformed string into a plausible instant, and a plausible
    // instant suppresses the mtime fallback and the
    // `CURSOR_TIMESTAMP_FROM_MTIME` diagnostic that exists to make an undated
    // turn visible.
    let second: u32 = match fields.next() {
        Some(seconds) => seconds.parse().ok()?,
        None => 0,
    };
    if fields.next().is_some() {
        return None;
    }
    // A meridiem constrains the hour to 1..=12. `0 PM` and `15 AM` are not
    // clocks, and turning them into plausible instants would suppress the
    // mtime fallback and the `CURSOR_TIMESTAMP_FROM_MTIME` diagnostic in the
    // same way a malformed seconds field did.
    match meridiem.as_deref() {
        Some("PM") => {
            if !(1..=12).contains(&hour) {
                return None;
            }
            if hour < 12 {
                hour += 12;
            }
        }
        Some("AM") => {
            if !(1..=12).contains(&hour) {
                return None;
            }
            if hour == 12 {
                hour = 0;
            }
        }
        Some(_) => return None,
        None => {}
    }
    (hour < 24 && minute < 60 && second < 60).then_some((hour, minute, second))
}

/// `UTC-4`, `UTC+5:30`, `UTC+05:30`, `GMT-4` and a bare `UTC` all appear.
fn parse_utc_offset(zone: &str) -> Option<i32> {
    let zone = zone.trim();
    let rest = zone
        .strip_prefix("UTC")
        .or_else(|| zone.strip_prefix("GMT"))
        .unwrap_or(zone);
    if rest.is_empty() {
        return Some(0);
    }
    let (sign, digits) = match rest.as_bytes()[0] {
        b'+' => (1, &rest[1..]),
        b'-' => (-1, &rest[1..]),
        _ => return None,
    };
    let (hours, minutes) = match digits.split_once(':') {
        Some((hours, minutes)) => (hours, minutes),
        None => (digits, "0"),
    };
    let hours: i32 = hours.trim().parse().ok()?;
    let minutes: i32 = minutes.trim().parse().ok()?;
    (hours < 24 && minutes < 60).then_some(sign * (hours * 3600 + minutes * 60))
}

/// The role a Cursor record speaks in.
///
/// `role` sits at the **top level**. Reading it from `message.role` — the
/// Claude Code nesting — finds nothing and labels every turn as unknown.
pub(crate) fn record_role(obj: &serde_json::Map<String, Value>) -> Option<&str> {
    obj.get("role")
        .and_then(Value::as_str)
        .or_else(|| obj.get("message")?.get("role")?.as_str())
}

/// The blocks a record carries, normalizing the bare-string content older
/// rows use into a single synthetic text block.
pub(crate) fn record_blocks(obj: &serde_json::Map<String, Value>) -> Vec<Value> {
    let Some(content) = obj.get("message").and_then(|m| m.get("content")) else {
        return Vec::new();
    };
    if let Some(text) = content.as_str() {
        return vec![serde_json::json!({"type": "text", "text": text})];
    }
    content.as_array().cloned().unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn localized_cursor_timestamps_parse_with_their_offset() {
        // The exact shape reported from real transcripts.
        assert_eq!(
            parse_cursor_timestamp("Wednesday, Sep 16, 2026, 3:37 PM (UTC-4)"),
            Some(1_789_587_420_000)
        );
        assert_eq!(
            parse_cursor_timestamp("Tuesday, Jun 2, 2026, 11:20 AM (UTC+8)"),
            Some(1_780_370_400_000)
        );
        // Half-hour offsets and 24-hour clocks both appear across locales.
        assert_eq!(
            parse_cursor_timestamp("Tuesday, Jun 2, 2026, 11:20 AM (UTC+5:30)"),
            Some(1_780_379_400_000)
        );
        assert_eq!(
            parse_cursor_timestamp("Monday, Jan 5, 2026, 15:04 (UTC)"),
            Some(1_767_625_440_000)
        );
    }

    /// A malformed seconds field must reject the timestamp, not silently
    /// become second 0. Accepting it produces a plausible time, which then
    /// suppresses the mtime fallback and the `CURSOR_TIMESTAMP_FROM_MTIME`
    /// diagnostic that exists to make an undated turn visible.
    ///
    /// Reported by CodeRabbit. Positive control: with
    /// `fields.next().and_then(parse).unwrap_or(0)` this failed at
    /// `a malformed seconds field must not parse as second 0` — the string
    /// parsed to a real instant.
    /// A meridiem constrains the hour to 1..=12. `0 PM` and `15 AM` are not
    /// clocks, and accepting them produced a plausible instant — which, as
    /// with the malformed-seconds case, suppresses the mtime fallback and the
    /// `CURSOR_TIMESTAMP_FROM_MTIME` diagnostic.
    ///
    /// Reported by Devin. Positive control: without the range check this
    /// failed at `an hour outside 1..=12 is not a meridiem clock` — both
    /// strings parsed to real instants.
    #[test]
    fn an_impossible_meridiem_hour_rejects_the_timestamp() {
        assert_eq!(
            parse_cursor_timestamp("Wednesday, Sep 16, 2026, 0:37 PM (UTC-4)"),
            None,
            "an hour outside 1..=12 is not a meridiem clock"
        );
        assert_eq!(
            parse_cursor_timestamp("Wednesday, Sep 16, 2026, 15:37 AM (UTC-4)"),
            None,
            "an hour outside 1..=12 is not a meridiem clock"
        );
        // Controls: the meridiem boundaries and the 24-hour path.
        assert_eq!(parse_clock("12:00 AM"), Some((0, 0, 0)));
        assert_eq!(parse_clock("12:00 PM"), Some((12, 0, 0)));
        assert_eq!(parse_clock("3:37 PM"), Some((15, 37, 0)));
        // Without a meridiem the 24-hour reading still stands.
        assert_eq!(parse_clock("15:37"), Some((15, 37, 0)));
        assert_eq!(parse_clock("0:05"), Some((0, 5, 0)));
    }

    #[test]
    fn a_malformed_seconds_field_rejects_the_timestamp() {
        assert_eq!(
            parse_cursor_timestamp("Wednesday, Sep 16, 2026, 3:37:invalid PM (UTC-4)"),
            None,
            "a malformed seconds field must not parse as second 0"
        );
        // A trailing field is malformed too.
        assert_eq!(
            parse_cursor_timestamp("Wednesday, Sep 16, 2026, 3:37:05:99 PM (UTC-4)"),
            None
        );
        // Controls: the shapes that really occur still parse.
        assert_eq!(
            parse_cursor_timestamp("Wednesday, Sep 16, 2026, 3:37 PM (UTC-4)"),
            Some(1_789_587_420_000)
        );
        assert_eq!(
            parse_cursor_timestamp("Wednesday, Sep 16, 2026, 3:37:05 PM (UTC-4)"),
            Some(1_789_587_425_000)
        );
    }

    #[test]
    fn an_unreadable_timestamp_is_none_rather_than_a_guess() {
        // A non-English month is not silently mapped onto a nearby one.
        assert_eq!(
            parse_cursor_timestamp("mercredi, 16 septembre 2026, 15:37 (UTC-4)"),
            None
        );
        assert_eq!(parse_cursor_timestamp(""), None);
        assert_eq!(parse_cursor_timestamp("just now"), None);
        // A missing zone is not assumed to be UTC.
        assert_eq!(parse_cursor_timestamp("Sep 16, 2026, 3:37 PM"), None);
    }

    #[test]
    fn the_timestamp_tag_is_read_out_of_the_user_turn_text() {
        let text = "<timestamp>Wednesday, Sep 16, 2026, 3:37 PM (UTC-4)</timestamp>\n\
                    <user_query>ship it</user_query>";
        assert_eq!(timestamp_from_text(text), Some(1_789_587_420_000));
        assert_eq!(unwrap_user_text(text), "ship it");
        // A turn with no framing keeps its text and yields no time.
        assert_eq!(timestamp_from_text("ship it"), None);
        assert_eq!(unwrap_user_text("  ship it  "), "ship it");
        // Framing with no query envelope still loses the timestamp markup.
        assert_eq!(
            unwrap_user_text(
                "<timestamp>Wednesday, Sep 16, 2026, 3:37 PM (UTC-4)</timestamp>\nship it"
            ),
            "ship it"
        );
    }

    #[test]
    fn every_cursor_tool_dialect_classifies_the_same_way() {
        for name in [
            "Write",
            "StrReplace",
            "ApplyPatch",
            "functions.ApplyPatch",
            "edit_file",
            "search_replace",
        ] {
            assert!(is_file_edit_tool(name), "{name} must be an edit tool");
        }
        for name in ["Read", "Grep", "Shell", "functions.rg", "codebase_search"] {
            assert!(!is_file_edit_tool(name), "{name} must not be an edit tool");
        }
        assert!(is_subagent_tool("Task"));
        assert!(is_subagent_tool("functions.Subagent"));
        assert!(!is_subagent_tool("Shell"));
    }

    #[test]
    fn a_patch_is_split_into_one_entry_per_file_it_touches() {
        let patch = "*** Begin Patch\n\
                     *** Update File: src/a.rs\n\
                     @@\n-one\n+two\n\
                     *** Add File: src/b.rs\n\
                     @@\n+alpha\n+beta\n\
                     *** Delete File: src/c.rs\n\
                     @@\n-gone\n\
                     *** End Patch";
        let files = split_patch_files(patch);
        assert_eq!(
            files.iter().map(|f| f.path.as_str()).collect::<Vec<_>>(),
            vec!["src/a.rs", "src/b.rs", "src/c.rs"]
        );
        // Each slice holds only its own file's lines, so per-file counts are
        // possible at all.
        assert!(files[1].patch.contains("+alpha"));
        assert!(!files[1].patch.contains("-one"));
        assert!(!files[1].patch.contains("-gone"));
        // The preamble belongs to no file and the terminator is dropped.
        assert!(!files[0].patch.contains("*** Begin Patch"));
        assert!(!files[2].patch.contains("*** End Patch"));
        // `patch_target` still names one thing, for `tool_calls.target`.
        assert_eq!(patch_target(patch), Some("src/a.rs".to_string()));
    }

    #[test]
    fn a_multi_file_unified_diff_splits_on_its_header_pairs() {
        let patch = "--- a/one.rs\n+++ b/one.rs\n@@\n-x\n+y\n\
                     --- a/two.rs\n+++ b/two.rs\n@@\n+z\n";
        let files = split_patch_files(patch);
        assert_eq!(
            files.iter().map(|f| f.path.as_str()).collect::<Vec<_>>(),
            vec!["one.rs", "two.rs"]
        );
        assert!(files[1].patch.contains("+z"));
        assert!(!files[1].patch.contains("-x"));
    }

    /// A hunk body can contain lines that look exactly like file headers: a
    /// deleted `-- old` renders as `--- old`, and an added `++ new` as
    /// `+++ new`. Only a header *outside* a hunk opens a file.
    ///
    /// Reported by CodeRabbit. Positive control: without the in-hunk guard
    /// this failed with `left: ["one.rs", "old"], right: ["one.rs"]` — the
    /// deleted line opened a phantom file and split the patch, so the real
    /// file's edit lost everything after it.
    /// An envelope marker is only a marker at the start of its line. A
    /// unified-diff *context* line is the same text with a leading space —
    /// ` *** Update File: example.md` — and trimming before matching turned
    /// that into a header, opening a phantom edit and moving the rest of the
    /// patch away from the file it belongs to.
    ///
    /// Reported by Devin. Positive control: with the marker matched against
    /// the trimmed line this failed with
    /// `left: ["notes.md", "example.md"], right: ["notes.md"]`.
    #[test]
    fn a_context_line_quoting_an_envelope_marker_does_not_open_a_file() {
        // Written without line continuations: `\\<newline>` in a Rust string
        // strips the next line's leading whitespace, which would eat the very
        // space that makes this a context line.
        let patch = concat!(
            "*** Begin Patch\n",
            "*** Update File: notes.md\n",
            "@@\n",
            "-before\n",
            " *** Update File: example.md\n",
            "+after\n",
            "*** End Patch"
        );
        let files = split_patch_files(patch);
        assert_eq!(
            files.iter().map(|f| f.path.as_str()).collect::<Vec<_>>(),
            vec!["notes.md"]
        );
        // The quoted marker stays with the real file, and so does everything
        // after it.
        assert!(files[0].patch.contains("*** Update File: example.md"));
        assert!(files[0].patch.contains("+after"));
    }

    #[test]
    fn hunk_content_that_looks_like_a_file_header_does_not_open_a_file() {
        let patch = "--- a/one.rs\n\
                     +++ b/one.rs\n\
                     @@\n\
                     --- old\n\
                     +++ new\n\
                     -kept\n\
                     +added\n";
        let files = split_patch_files(patch);
        assert_eq!(
            files.iter().map(|f| f.path.as_str()).collect::<Vec<_>>(),
            vec!["one.rs"]
        );
        // The whole hunk stays with its file, including the lines that look
        // like headers.
        assert!(files[0].patch.contains("--- old"));
        assert!(files[0].patch.contains("+++ new"));
        assert!(files[0].patch.contains("+added"));
    }

    #[test]
    fn a_removed_line_of_dashes_does_not_open_a_new_file() {
        // `--- ` only opens a file when a `+++ ` line follows it; otherwise it
        // is content that happens to start with three dashes, which is common
        // in Markdown and YAML.
        let patch = "*** Begin Patch\n\
                     *** Update File: notes.md\n\
                     @@\n--- a horizontal rule\n+++ still content\n\
                     *** End Patch";
        let files = split_patch_files(patch);
        assert_eq!(
            files.iter().map(|f| f.path.as_str()).collect::<Vec<_>>(),
            vec!["notes.md"]
        );
    }

    #[test]
    fn apply_patch_takes_its_path_out_of_the_patch_text() {
        let patch = "*** Begin Patch\n*** Update File: src/main.rs\n@@\n-old\n+new\n*** End Patch";
        assert_eq!(
            pick_tool_target("ApplyPatch", &Value::String(patch.to_string())),
            Some("src/main.rs".to_string())
        );
        assert_eq!(patch_text(&Value::String(patch.to_string())), Some(patch));
        assert_eq!(
            patch_target("--- a/lib.rs\n+++ b/lib.rs\n@@\n+one\n"),
            Some("lib.rs".to_string())
        );
    }

    #[test]
    fn object_shaped_tool_inputs_name_their_file_or_command() {
        assert_eq!(
            pick_tool_target("Write", &serde_json::json!({"path": "docs/a.md"})),
            Some("docs/a.md".to_string())
        );
        assert_eq!(
            pick_tool_target(
                "edit_file",
                &serde_json::json!({"target_file": "docs/b.md"})
            ),
            Some("docs/b.md".to_string())
        );
        assert_eq!(
            pick_tool_target("Shell", &serde_json::json!({"command": "cargo test"})),
            Some("cargo test".to_string())
        );
        assert_eq!(pick_tool_target("Read", &Value::Null), None);
    }

    #[test]
    fn the_role_is_read_from_the_top_level_not_from_the_message() {
        let record: Value = serde_json::from_str(
            r#"{"role":"assistant","message":{"content":[{"type":"text","text":"ok"}]}}"#,
        )
        .unwrap();
        let obj = record.as_object().unwrap();
        assert_eq!(record_role(obj), Some("assistant"));
        assert_eq!(record_blocks(obj).len(), 1);
        // Bare-string content becomes one text block.
        let legacy: Value =
            serde_json::from_str(r#"{"role":"user","message":{"content":"hi"}}"#).unwrap();
        let blocks = record_blocks(legacy.as_object().unwrap());
        assert_eq!(blocks[0]["text"], serde_json::json!("hi"));
    }
}

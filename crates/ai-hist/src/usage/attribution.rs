//! Attributing each response's measured usage to the prompt that caused it.

use super::{normalize_usage, NormalizedUsage};
use crate::store::SessionEvent;
use serde_json::Value;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};

/// `(timestamp_ms, prompt_text)` — the identity a `history` row is keyed by.
pub type PromptKey = (i64, String);

/// The request an event belongs to, under the same rule the `session_requests`
/// view groups by: the provider's own `request_id` first, then its
/// `provider_message_id`, then its span, and the stored record id as a last
/// resort. Codex spans outrank output-item IDs, which do not identify calls.
///
/// Namespace-qualified, because those three namespaces are separate and can
/// carry the same text — a bare value is not unique, and a key that is not
/// unique is not a key.
///
/// This rule exists twice, once here and once in SQL, and the two are pinned
/// against each other by `the_rust_request_key_agrees_with_the_view`. Drift
/// between them is not cosmetic: it is how one API request came to be counted
/// once by the session rollup and once per record by prompt attribution.
pub(crate) fn request_key(event: &SessionEvent) -> String {
    fn present(value: Option<&String>) -> Option<&str> {
        // `NULLIF(x, '')` in the view: empty falls through, anything else is
        // taken verbatim, padding included.
        value.map(String::as_str).filter(|value| !value.is_empty())
    }
    if let Some(id) = present(event.request_id.as_ref()) {
        format!("request-id:{id}")
    } else if let Some(span) =
        present(event.request_span.as_ref()).filter(|_| event.source == "codex")
    {
        format!("request-span:{span}")
    } else if let Some(id) = present(event.provider_message_id.as_ref()) {
        format!("provider-message-id:{id}")
    } else if let Some(span) = present(event.request_span.as_ref()) {
        format!("request-span:{span}")
    } else {
        format!(
            "record-id:{}",
            event.message_id.as_deref().unwrap_or_default()
        )
    }
}

/// What one record says about the cost of the request it belongs to.
///
/// The three cases are not interchangeable, which is the whole point of the
/// enum: `Absent` is a record that says nothing, and `Unreadable` is a record
/// that says something that cannot be believed. Folding the second into the
/// first let a sibling whose copy happened to parse stand in for a request
/// whose copies contradict each other.
enum RecordUsage {
    /// No usage on this record, or usage carrying no recognized counter.
    Absent,
    /// Usage is present and cannot be turned into a measurement: the record's
    /// rows disagree about it, or normalization refused it.
    Unreadable,
    Measured(NormalizedUsage),
}

/// One assistant or user message, rebuilt from the content-block rows that
/// carry it.
struct UsageMessage {
    id: String,
    parent: Option<String>,
    ts: i64,
    role: String,
    text: String,
    usage: RecordUsage,
    /// The API request this record belongs to. Several records can share one
    /// — see [`request_key`].
    request: String,
    /// A user-role record every row of which carries a `control_kind`: a
    /// slash-command wrapper, a task notification, Codex context. Not a
    /// prompt, so never an owner; the walk to the owning prompt steps over
    /// it.
    control: bool,
    /// A Claude sidechain user record: the delegating agent's prompt to a
    /// subagent, or a tool result the subagent received. Not the human's, so
    /// never an owner -- and not a step on the way to one either: a
    /// subagent's usage is the delegated thread's, as a sidecar subagent's is
    /// its own session's, so the walk ends there with no owner.
    sidechain: bool,
}

/// What one record contributes to its request.
enum Contribution<'a> {
    /// Nothing that bears on the request's cost or its owner.
    Silent,
    /// The prompt this record is owed to, with no measurement of its own.
    ///
    /// It still constrains the request: a request whose records resolve to
    /// different prompts cannot be charged to either, and a record carrying no
    /// usage is evidence about ownership all the same. Codex's unmeasured
    /// turns are exactly this — when a later snapshot's delta covers several
    /// turns they become one request, and without this the one row holding the
    /// measurement would charge its own prompt for all of them.
    Owned(PromptKey),
    /// A measurement and the prompt it is owed to.
    Measured(PromptKey, &'a NormalizedUsage),
    /// Evidence that cannot be reconciled: an unreadable measurement, or an
    /// owner the record names but that cannot be pinned down.
    Contradictory,
}

/// Attribute each message's usage to the prompt that caused it.
///
/// Ported unchanged in behaviour from the commercial outbox, which is the
/// only place this logic existed. Its defining property is that it *refuses*:
/// wherever the evidence cannot establish which prompt owns a response, the
/// response contributes nothing rather than being charged to a plausible
/// neighbour. A missing number is recoverable; a confidently wrong one is
/// not.
///
/// A message whose usage cannot be normalized (malformed, unknown source,
/// negative counter) is treated exactly like a message with no usage: it is
/// skipped, and the prompt keeps whatever other requests it owns.
pub fn attribute_usage_to_prompts(
    events: &[SessionEvent],
    source: &str,
) -> HashMap<PromptKey, NormalizedUsage> {
    let mut groups: HashMap<&str, Vec<&SessionEvent>> = HashMap::new();
    for event in events {
        if let Some(id) = event.message_id.as_deref().filter(|id| !id.is_empty()) {
            groups.entry(id).or_default().push(event);
        }
    }
    let messages: HashMap<String, UsageMessage> = groups
        .into_iter()
        .filter_map(|(id, rows)| {
            let message = usage_message(id, &rows, source)?;
            Some((id.to_string(), message))
        })
        .collect();
    // Keep even unidentifiable user events as boundaries; dropping one would
    // incorrectly charge its answer to the preceding identifiable prompt. A
    // control row is the one user row that is *not* a boundary: Codex
    // prepends its context wrapper to the human's turn, and treating it as a
    // turn of its own would hand the answer to the wrapper.
    let mut boundaries: Vec<_> = events
        .iter()
        .filter(|e| e.role == "user" && e.control_kind.is_none() && e.is_sidechain != Some(1))
        .collect();
    boundaries.sort_by_key(|e| e.ts_ms);
    // Parsers use zero when time is missing. Such a turn could fall anywhere,
    // so timestamp-only ownership is unsafe for the session.
    let timestamps_known = boundaries.iter().all(|e| e.ts_ms > 0);
    let mut prompt_counts = HashMap::new();
    for user in messages
        .values()
        .filter(|m| m.role == "user" && !m.control && !m.sidechain)
    {
        *prompt_counts
            .entry((user.ts, user.text.clone()))
            .or_insert(0) += 1;
    }
    // One API request is often written as several records: Claude gives each
    // content block its own uuid, links them in a chain, and copies the whole
    // of `message.usage` onto every one of them. Ownership is resolved per
    // *record*, because that is where the parent links are, but the
    // measurement belongs to the *request* and is added exactly once —
    // grouping on the record id charged a prompt its own cost multiplied by
    // the request's record count, which is the same defect the
    // `session_requests` view exists to prevent, and it must not survive in
    // the path that feeds prompt costs.
    let mut requests: HashMap<&str, RequestState<'_>> = HashMap::new();
    for message in messages.values().filter(|m| m.role == "assistant") {
        let contribution = contribution_of(
            message,
            source,
            &messages,
            &boundaries,
            timestamps_known,
            &prompt_counts,
        );
        record_contribution(&mut requests, message.request.as_str(), contribution);
    }
    sum_requests_by_prompt(requests)
}

/// What a request's records establish so far. `None` marks a request whose
/// records contradict each other. The inner `Option` is the measurement,
/// which a request may not have yet even once its owner is known.
type RequestState<'a> = Option<(PromptKey, Option<&'a NormalizedUsage>)>;

/// Rebuild one record from the content-block rows sharing its id, or `None`
/// when the rows do not agree on what record they are.
fn usage_message(id: &str, rows: &[&SessionEvent], source: &str) -> Option<UsageMessage> {
    let first = rows[0];
    if rows
        .iter()
        .any(|r| r.ts_ms != first.ts_ms || r.role != first.role || r.parent_id != first.parent_id)
    {
        return None;
    }
    // Rows sharing a record id come from one provider record, so they
    // report one request. Disagreement means the row layout is not
    // what it claims, and a measurement built on it is not evidence.
    let request = request_key(first);
    if rows.iter().any(|r| request_key(r) != request) {
        return None;
    }
    let usage = record_usage(rows, source);
    // A `<system-reminder>` row shares its prompt's record id and is
    // not the human's text; leaving it out is what keeps this key
    // equal to the `history` prompt the row was stored beside.
    let text = rows
        .iter()
        .filter(|r| r.kind == "text" && r.control_kind.is_none())
        .filter_map(|r| r.text.as_deref())
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .collect::<Vec<_>>()
        .join("\n");
    let control = first.role == "user" && rows.iter().all(|r| r.control_kind.is_some());
    let sidechain = first.role == "user" && rows.iter().any(|r| r.is_sidechain == Some(1));
    Some(UsageMessage {
        id: id.to_string(),
        parent: first.parent_id.clone(),
        ts: first.ts_ms,
        role: first.role.clone(),
        text,
        usage,
        request,
        control,
        sidechain,
    })
}

/// The one measurement a record's rows report.
fn record_usage(rows: &[&SessionEvent], source: &str) -> RecordUsage {
    // Claude copies message.usage onto every content block. Counting
    // rows would multiply one model request by its thinking/text/tool
    // block count.
    let raw: Vec<&str> = rows
        .iter()
        .filter_map(|r| r.token_json.as_deref())
        .collect();
    let parsed: Option<Vec<Value>> = raw
        .iter()
        .map(|raw| serde_json::from_str(raw).ok())
        .collect();
    match parsed.as_deref() {
        None => RecordUsage::Unreadable,
        Some([]) => RecordUsage::Absent,
        // One record reports one measurement. Rows that disagree about
        // it are not evidence of either reading.
        Some([first, rest @ ..]) if rest.iter().all(|value| value == first) => {
            match normalize_usage(source, first) {
                Ok(Some(usage)) if !usage.is_zero() => RecordUsage::Measured(usage),
                // `{}` and a record with no recognized counter are
                // silence, not a contradiction.
                Ok(_) => RecordUsage::Absent,
                Err(_) => RecordUsage::Unreadable,
            }
        }
        Some(_) => RecordUsage::Unreadable,
    }
}

/// Fold one record's contribution into the state of the request it belongs
/// to.
fn record_contribution<'a>(
    requests: &mut HashMap<&'a str, RequestState<'a>>,
    request: &'a str,
    contribution: Contribution<'a>,
) {
    match contribution {
        // A record that says nothing leaves the request as it found it.
        // A broken ancestry is silence, not contradiction: Claude chains a
        // request's records through each other and the parser stores no
        // row for an empty block, so a mid-chain gap is ordinary. Refusing
        // the request over it would drop measurements that a sibling
        // establishes outright.
        Contribution::Silent => {}
        Contribution::Contradictory => {
            requests.insert(request, None);
        }
        // The copies of one request must agree on both the measurement and
        // the prompt that caused it. If they do not, the evidence does not
        // say which reading is real, so the request contributes nothing
        // rather than one of them.
        Contribution::Owned(key) => match requests.entry(request) {
            Entry::Vacant(slot) => {
                slot.insert(Some((key, None)));
            }
            Entry::Occupied(mut slot) => {
                let agrees = slot.get().as_ref().is_some_and(|(owner, _)| *owner == key);
                if !agrees {
                    slot.insert(None);
                }
            }
        },
        Contribution::Measured(key, usage) => match requests.entry(request) {
            Entry::Vacant(slot) => {
                slot.insert(Some((key, Some(usage))));
            }
            Entry::Occupied(mut slot) => {
                let agrees = slot.get().as_ref().is_some_and(|(owner, seen)| {
                    *owner == key && seen.is_none_or(|seen| seen == usage)
                });
                if agrees {
                    slot.insert(Some((key, Some(usage))));
                } else {
                    slot.insert(None);
                }
            }
        },
    }
}

/// Add each agreed request's measurement, once, to the prompt that owns it.
fn sum_requests_by_prompt(
    requests: HashMap<&str, RequestState<'_>>,
) -> HashMap<PromptKey, NormalizedUsage> {
    let mut attributed: HashMap<PromptKey, Option<NormalizedUsage>> = HashMap::new();
    for (key, usage) in requests
        .into_values()
        .flatten()
        .filter_map(|(key, usage)| usage.map(|usage| (key, usage)))
    {
        // Seed from the first contribution rather than from an all-zero
        // record. `empty()` reports *nothing* optional, and an optional field
        // is only reported when every contributor reported it — so folding
        // through it would strip reasoning, the cache-write split, the
        // provider total and the cost from every prompt.
        match attributed.entry(key) {
            Entry::Vacant(slot) => {
                slot.insert(Some(usage.clone()));
            }
            Entry::Occupied(mut slot) => {
                let folded = slot
                    .get()
                    .as_ref()
                    .and_then(|total| total.checked_add(usage));
                slot.insert(folded);
            }
        }
    }
    attributed
        .into_iter()
        .filter_map(|(key, usage)| usage.map(|u| (key, u)))
        .collect()
}

/// What one assistant record contributes to the request it belongs to.
fn contribution_of<'a>(
    message: &'a UsageMessage,
    source: &str,
    messages: &'a HashMap<String, UsageMessage>,
    boundaries: &[&SessionEvent],
    timestamps_known: bool,
    prompt_counts: &HashMap<PromptKey, i32>,
) -> Contribution<'a> {
    let usage = match &message.usage {
        // No measurement, but possibly an owner — resolved below, because a
        // record that names a different prompt than its request's other
        // records is evidence even when it reports no cost.
        RecordUsage::Absent => None,
        // The request's own cost is in dispute. A sibling whose copy parses is
        // not the tie-breaker: reporting it would publish one of two
        // contradicting readings as the measurement.
        RecordUsage::Unreadable => return Contribution::Contradictory,
        RecordUsage::Measured(usage) => Some(usage),
    };
    let owner = if message.parent.is_some() {
        parent_prompt(message, messages)
    } else if source == "codex" && message.ts > 0 && timestamps_known {
        // Codex persists no parent IDs. Only its ordered human-turn
        // stream establishes ownership: a tie at either boundary is
        // ambiguous, and an explicit but broken parent link must never
        // fall back to time.
        let boundary = boundaries.partition_point(|u| u.ts_ms < message.ts);
        if boundaries
            .get(boundary)
            .is_some_and(|u| u.ts_ms == message.ts)
        {
            None
        } else {
            boundary.checked_sub(1).and_then(|i| {
                let user = boundaries[i];
                if user.ts_ms <= 0 || (i > 0 && boundaries[i - 1].ts_ms == user.ts_ms) {
                    return None;
                }
                messages.get(user.message_id.as_deref()?)
            })
        }
    } else {
        None
    };
    // No owner is missing evidence about *this record*, not about the
    // request: a sibling may establish it.
    let Some(owner) = owner else {
        return Contribution::Silent;
    };
    let key = (owner.ts, owner.text.clone());
    // Identical text and timestamps cannot distinguish two user messages.
    // Assigning both to a single history row would conceal a bad join. The
    // record does name an owner here, so this is a contradiction rather
    // than silence.
    if owner.text.is_empty() || prompt_counts.get(&key) != Some(&1) {
        return Contribution::Contradictory;
    }
    match usage {
        Some(usage) => Contribution::Measured(key, usage),
        None => Contribution::Owned(key),
    }
}

/// Walk `parent_id` to the user message that owns a response.
fn parent_prompt<'a>(
    message: &'a UsageMessage,
    messages: &'a HashMap<String, UsageMessage>,
) -> Option<&'a UsageMessage> {
    let mut current = message;
    let mut visited = HashSet::new();
    while visited.insert(current.id.as_str()) {
        // A slash command's rows sit between the answer and the prompt that
        // caused it. They are chained like any record, and they are not the
        // owner, so the walk continues through them to the prompt.
        if current.role == "user" && current.sidechain {
            return None;
        }
        if current.role == "user" && !current.control {
            return Some(current);
        }
        current = messages.get(current.parent.as_deref()?)?;
    }
    // Broken/cyclic ancestry is missing evidence, not permission to charge
    // the nearest prompt (which could belong to another conversation branch).
    None
}

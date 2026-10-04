/**
 * From change-feed rows to protocol-1 delivery records, and which rows the
 * selection admits. Identities follow `ai-hist export` exactly, so an export and an
 * upload name every record the same way.
 */
import { createHash } from "node:crypto";
import type {
  FeedChange,
  HistoryExportRecord,
  HistoryExportSelection,
} from "ai-hist";

const sha256 = (value: string) =>
  createHash("sha256").update(value, "utf8").digest("hex");

/** The wire record for one change. `originId` is the store's change-feed epoch. */
export function deliveryRecord(
  change: FeedChange,
  originId: string,
): HistoryExportRecord {
  const recordId = sha256(JSON.stringify(change.key));
  return {
    schema_version: 1,
    origin_id: originId,
    record_id: recordId,
    revision_id: sha256(`${originId}:${recordId}:${change.revision}`),
    revision: change.revision,
    kind: change.kind,
    source: change.sourceName,
    session_id: change.sessionId === "" ? null : change.sessionId,
    operation: change.op,
    payload: change.op === "delete" ? null : change.columns,
  };
}

function names(
  list: HistoryExportSelection["sessions"],
  source: string,
  session: string,
) {
  return list.some((id) => id.source === source && id.session_id === session);
}

/**
 * The export selection rule, applied to one change. An excluded session is never
 * sent, nor is a relationship naming it at either end. A change that names no
 * session (a prompt's delete) is admitted only when its whole source is selected,
 * so a deletion is never forwarded for a record the selection cannot attribute.
 */
export function selected(
  change: FeedChange,
  selection: HistoryExportSelection,
): boolean {
  const source = change.sourceName;
  const session = change.sessionId;
  if (session && names(selection.excluded_sessions, source, session))
    return false;
  const child = change.columns?.child_session_id;
  if (
    typeof child === "string" &&
    names(selection.excluded_sessions, source, child)
  )
    return false;
  if (selection.all_sources || selection.sources.includes(source)) return true;
  return Boolean(session) && names(selection.sessions, source, session);
}

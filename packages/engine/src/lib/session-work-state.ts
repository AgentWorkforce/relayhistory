import { and, eq, or, sql } from "drizzle-orm";
import {
  convergenceEvents as events,
  conversationTurns as turns,
} from "../db/schema.js";

// Only conversation/activity and explicit task lifecycle records describe work.
// Discovery/presence records can carry a generic status without reopening a task.
export const isWorkEvent = sql`${events.kind} in ('prompt', 'user', 'assistant', 'history', 'session_event', 'tool_call', 'file_edit', 'trajectory', 'task')`;
const taskState = sql`lower(coalesce(${events.taskStatus},
  case when ${events.kind} = 'trajectory' then ${events.record}->'payload'->>'status' end, ''))`;
export const sessionWorkStatus = sql`case
  when not (${isWorkEvent}) then null
  when ${taskState} in ('completed', 'complete', 'finished', 'done', 'succeeded') then 'finished'
  when ${taskState} in ('idle', 'paused', 'waiting', 'blocked', 'cancelled', 'canceled', 'failed') then 'idle'
  when ${taskState} in ('active', 'working', 'running', 'in_progress') then 'active'
  when ${events.kind} in ('prompt', 'user', 'assistant', 'history', 'session_event', 'tool_call', 'file_edit') then 'active'
  else null end`;

export type SessionIdentity = { source: string; sessionId: string };
export const sessionIdentityKey = (session: SessionIdentity) =>
  JSON.stringify([session.source, session.sessionId]);
export const selectedSessions = (sessions: SessionIdentity[]) =>
  or(
    ...sessions.map((session) =>
      and(
        eq(events.source, session.source),
        eq(events.sessionId, session.sessionId),
      ),
    ),
  );

// A turn's capture source: its `nativeCli`/`source` metadata; else its persisted
// `source` when that differs from its session owner; else its session owner when that
// owner is a source this org recorded activity for under the same session id.
//
// The write path stores the turn's own tag, else a tag from another turn of the same
// request, else the session owner. A persisted source other than the owner therefore
// came from an explicit tag (a harness often tags only the opening turn) and is
// authoritative for the untagged turns stored under it. A persisted source equal to the
// owner carries no such evidence: the retired OSS turns publisher sent
// sessionOwner=<source> and no source metadata, while a human owner name (the broker's
// convention) never matches an event source, so those turns stay unattributed and fall
// to the legacy rule below.
export const conversationSource = sql<string | null>`coalesce(
  nullif(${turns.metadata}->>'nativeCli', ''),
  nullif(${turns.metadata}->>'source', ''),
  case when ${turns.source} <> '' and ${turns.source} <> ${turns.sessionOwner}
    then ${turns.source} end,
  case when exists (
    select 1 from sessions.convergence_events as owner_event
    where owner_event.org_id = ${turns.orgId}
      and owner_event.session_id = ${turns.sessionId}
      and owner_event.source = ${turns.sessionOwner}
  ) then ${turns.sessionOwner} end
)`;
// Legacy turns did not always record a source. They belong to a source-specific
// replay only when the org's activity proves that the ID is unambiguous.
export const selectedSessionTurns = (
  orgId: string,
  sessions: SessionIdentity[],
) =>
  or(
    ...sessions.map((session) =>
      and(
        eq(turns.sessionId, session.sessionId),
        or(
          eq(conversationSource, session.source),
          sql`(${conversationSource} is null and not exists (
    select 1 from sessions.convergence_events as other
    where other.org_id = ${orgId} and other.session_id = ${session.sessionId} and other.source <> ${session.source}
  ))`,
        ),
      ),
    ),
  );

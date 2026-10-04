export type Cursors = Record<string, number>;

export interface MachineIdentity {
  id: string;
  hostname?: string;
  label?: string;
  os?: string;
  relayhistoryVersion?: string;
  /**
   * What the Rust CLI actually sends. `MachineIdentity` in ai-hist-core serializes its
   * version as `cliVersion`, so reading only `relayhistoryVersion` dropped it on every
   * push and left the column NULL for all 16 machines. Accepted as an alias rather than
   * renamed on the client alone, so machines still running an older binary report their
   * version too.
   */
  cliVersion?: string;
}

export interface TaskReference {
  system?: string;
  id?: string;
  url?: string;
}

export interface ConvergenceRecordEnvelope {
  v: number;
  kind: string;
  source: string;
  lens?: "history" | "trajectories" | "burn" | string;
  sessionId: string;
  eventId?: string;
  messageId?: string;
  requestId?: string;
  fingerprint?: string;
  idFingerprint?: string;
  chapterId?: string;
  eventGroup?: string;
  eventIndex?: number;
  ts?: string | number;
  startTs?: string | number;
  writtenAt?: string | number;
  type?: string;
  actorName?: string;
  actorRole?: string;
  subagentId?: string;
  trajectoryId?: string;
  projectId?: string;
  workflowId?: string;
  taskRef?: TaskReference;
  taskTitle?: string;
  taskDescription?: string;
  taskStatus?: string;
  content?: string;
  significance?: "low" | "medium" | "high" | "critical" | string;
  confidence?: number | null;
  tags?: string[];
  model?: string;
  provider?: string;
  usage?: Record<string, unknown>;
  costUsdMicros?: number;
  toolName?: string;
  toolStatus?: string;
  toolCalls?: unknown[];
  retries?: number;
  durationMs?: number;
  filesTouched?: unknown[];
  codeChurn?: Record<string, unknown>;
  record?: Record<string, unknown>;
}

interface OutcomeEnvelopeBase {
  kind: "session_outcome";
  source: string;
  lens?: "history" | "trajectories" | "burn" | string;
  sessionId: string;
  repo?: string;
  branch?: string;
  matchMethod?: string;
  match_method?: string;
  confidence?: number | null;
  /** The session's project, as on event envelopes. */
  projectId?: string | null;
  project_id?: string | null;
  /** Files the session touched (distinct from `files`, the commit's files). */
  filesTouched?: unknown[];
  files_touched?: unknown[];
  numstatJson?: Record<string, unknown>;
  numstat_json?: Record<string, unknown>;
  numstat?: Record<string, unknown>;
  filesJson?: unknown[];
  files_json?: unknown[];
  files?: unknown[];
  shippedAt?: string | number | null;
  shipped_at?: string | number | null;
  reverted?: boolean;
  revertedBySha?: string | null;
  reverted_by_sha?: string | null;
  revertedAt?: string | number | null;
  reverted_at?: string | number | null;
}

export type OutcomeEnvelope = OutcomeEnvelopeBase &
  (
    | { commitSha: string; commit_sha?: string }
    | { commitSha?: string; commit_sha: string }
  );

export interface IngestRequest {
  machine: MachineIdentity;
  batchId: string;
  cursors?: Cursors;
  records: Array<ConvergenceRecordEnvelope | OutcomeEnvelope>;
}

export interface IngestResponse {
  batchId: string;
  received: number;
  accepted: number;
  cursors: Cursors;
}

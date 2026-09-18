/**
 * The shape `ai-hist` expects of a Relayhistory cloud client.
 *
 * This is a **structural** declaration, not an import. `@relayhistory/cloud-client`
 * is the real implementation and satisfies this by shape alone, so the published
 * `ai-hist` package carries no runtime dependency on it — the same arrangement as
 * `@agent-relay/cli-surface`. `relay-cli.test.ts` proves the real client still
 * satisfies it at compile time.
 *
 * It is deliberately narrower than that client's own interface. Payload items are
 * `unknown` because `ai-hist` either serializes them verbatim under `--json` or
 * renders a few fields through `humanLine`, which already reads loose records.
 * Re-declaring the cloud client's eight zod-inferred payload types here would be
 * the duplication this arrangement exists to avoid.
 */

/** Cursor paging, shared by every listing. */
export interface CloudPageQuery {
  limit?: number;
  cursor?: string;
}

/** The filters the recall routes accept. All optional. */
export interface CloudSessionFilter {
  project?: string;
  source?: string;
  kind?: string;
  tag?: string;
  since?: string;
  until?: string;
  q?: string;
}

export interface CloudSessionsPage {
  readonly sessions: readonly unknown[];
  readonly nextCursor: string | null;
}

export interface CloudSessionEventsPage {
  readonly sessionId: string;
  readonly events: readonly unknown[];
  readonly nextCursor: string | null;
}

export interface CloudEventsPage {
  readonly events: readonly unknown[];
  readonly nextCursor: string | null;
}

export interface CloudTurnsPage {
  readonly sessionId: string;
  readonly turns: readonly unknown[];
}

/**
 * The read side of `@relayhistory/cloud-client`.
 *
 * Writes (`ingestTurns`) and the enterprise entry store are deliberately absent:
 * neither is a `sessions` verb, and both belong to callers that already hold a
 * cloud client directly.
 */
export interface RelayhistoryCloudClient {
  /** The normalized base, always ending in `/v1`. */
  readonly baseUrl: string;

  listSessions(query?: CloudSessionFilter & CloudPageQuery): Promise<CloudSessionsPage>;
  getSessionEvents(
    sessionId: string,
    query?: CloudPageQuery & { order?: string; maxContent?: number },
  ): Promise<CloudSessionEventsPage>;
  searchEvents(
    query?: CloudSessionFilter & CloudPageQuery & { session?: string; order?: string; maxContent?: number },
  ): Promise<CloudEventsPage>;
  getSessionThread(
    sessionId: string,
    query: { source: string; since?: string; kinds?: readonly string[]; cursor?: string; limit?: number },
  ): Promise<unknown>;

  /**
   * Cursor paging, done by the client.
   *
   * A single page is a prefix, not a transcript: anything that means "the whole
   * session" must use these rather than one `get`.
   */
  iterateSessions(query?: CloudSessionFilter & CloudPageQuery): AsyncIterable<unknown>;
  iterateSessionEvents(
    sessionId: string,
    query?: CloudPageQuery & { order?: string; maxContent?: number },
  ): AsyncIterable<unknown>;
  iterateEvents(query?: CloudSessionFilter & CloudPageQuery & { session?: string }): AsyncIterable<unknown>;

  listTurns(sessionId: string): Promise<CloudTurnsPage>;
  getSessionMetadata(sessionId: string): Promise<unknown | null>;

  getDailyDigest(query?: { date?: string; tz?: string; project?: string; refresh?: boolean }): Promise<unknown>;
  listMachines(query?: {
    staleAfterSeconds?: number;
    missingAfterSeconds?: number;
    windowHours?: number;
    limit?: number;
  }): Promise<unknown>;
}

/**
 * A cloud failure, read structurally so no error class has to be imported.
 *
 * `status` is the HTTP status; an authentication failure is the one case the CLI
 * answers with a remedy rather than the raw message.
 */
export function cloudErrorStatus(error: unknown): number | undefined {
  if (typeof error !== 'object' || error === null) return undefined;
  const status = (error as { status?: unknown }).status;
  return typeof status === 'number' ? status : undefined;
}

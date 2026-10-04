import type { Context } from "hono";
import { getAuth } from "./auth.js";

export const WORKSPACE_RECALL_HEADER = "X-Relayhistory-Workspace-Id";

function badRequest(c: Context<any>, message: string) {
  return c.json(
    {
      error: { code: "bad_request", message },
      correlationId: c.get("correlationId") ?? "",
    },
    400,
  );
}

function forbidden(c: Context<any>, message: string) {
  return c.json(
    {
      error: { code: "forbidden", message },
      correlationId: c.get("correlationId") ?? "",
    },
    403,
  );
}

const UNSUPPORTED_SELECTORS = ["workspace_id", "workspaceId"] as const;

type WorkspaceRecallScope =
  | { workspaceId: undefined; error?: undefined }
  | { workspaceId: string; error?: undefined }
  | { workspaceId?: undefined; error: Response };

/**
 * Opt into workspace recall without trusting caller-supplied tenancy.
 *
 * Omitting the parameter preserves the established organization-wide API. Supplying it
 * asks the service to prove and enforce the narrower boundary: the value must exactly
 * match the workspace in the authenticated service session, and SQL uses that resolved
 * token value rather than the query value.
 */
export function readWorkspaceRecallScope(
  c: Context<any>,
): WorkspaceRecallScope {
  // Other spellings would otherwise be ignored, silently widening the read to the org.
  for (const selector of UNSUPPORTED_SELECTORS) {
    if (c.req.queries(selector)) {
      return {
        error: badRequest(c, `unsupported selector ${selector}; use workspace`),
      };
    }
  }
  const requested = c.req.queries("workspace") ?? [];
  if (requested.length === 0) return { workspaceId: undefined };
  if (requested.length !== 1 || !requested[0]?.trim()) {
    return {
      error: badRequest(
        c,
        "workspace must be supplied exactly once and be non-empty",
      ),
    };
  }
  // Exact comparison with the token's own value: trimming would let a padded
  // workspace id answer for a different one.
  const authenticated = getAuth(c).workspaceId;
  if (!authenticated?.trim() || requested[0] !== authenticated) {
    return {
      error: forbidden(c, "workspace does not match the authenticated session"),
    };
  }
  return { workspaceId: authenticated };
}

export function attestWorkspace(
  c: Context<any>,
  workspaceId: string | undefined,
): void {
  if (workspaceId) c.header(WORKSPACE_RECALL_HEADER, workspaceId);
}

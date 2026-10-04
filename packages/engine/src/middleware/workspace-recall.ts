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

const UNSUPPORTED_WORKSPACE_SELECTORS = [
  "workspace_id",
  "workspaceId",
] as const;
// Tenancy comes only from the token: an organization selector is never honoured.
const UNSUPPORTED_ORG_SELECTORS = ["org", "org_id", "orgId"] as const;

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
  // Other spellings would otherwise be ignored, silently answering org-wide.
  for (const selector of UNSUPPORTED_WORKSPACE_SELECTORS) {
    if (c.req.queries(selector)) {
      return {
        error: badRequest(c, `unsupported selector ${selector}; use workspace`),
      };
    }
  }
  for (const selector of UNSUPPORTED_ORG_SELECTORS) {
    if (c.req.queries(selector)) {
      return { error: badRequest(c, `unsupported selector ${selector}`) };
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
  const unattestable = workspaceAttestationError(c, authenticated);
  if (unattestable) return { error: unattestable };
  return { workspaceId: authenticated };
}

/**
 * The 403 for a workspace-narrowed read whose workspace cannot be attested, or
 * undefined. The scope is attested in a response header, and a value a header cannot
 * carry exactly would attest a different key than the one the read used. Check before
 * querying; `attestWorkspace` refuses the same values.
 */
export function workspaceAttestationError(
  c: Context<any>,
  workspaceId: string,
): Response | undefined {
  return attestable(workspaceId)
    ? undefined
    : forbidden(c, "authenticated workspace cannot be attested");
}

/**
 * Whether `value` survives as a header value unchanged: no surrounding HTTP whitespace
 * (which is trimmed), no control characters and nothing outside Latin-1 (which a header
 * cannot hold).
 */
function attestable(value: string): boolean {
  if (/^[\t\n\r ]|[\t\n\r ]$/.test(value)) return false;
  for (const character of value) {
    const code = character.codePointAt(0)!;
    if (code < 0x20 || code === 0x7f || code > 0xff) return false;
  }
  return true;
}

/** Attests the workspace a read was narrowed to. Throws for one a header cannot carry. */
export function attestWorkspace(
  c: Context<any>,
  workspaceId: string | undefined,
): void {
  if (!workspaceId) return;
  if (!attestable(workspaceId)) throw new Error("workspace cannot be attested");
  c.header(WORKSPACE_RECALL_HEADER, workspaceId);
}

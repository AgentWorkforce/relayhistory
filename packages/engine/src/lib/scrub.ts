const REDACTED = "[REDACTED]";

type Replacement = string | ((substring: string) => string);

const SECRET_PATTERNS: Array<[RegExp, Replacement]> = [
  [
    /-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z ]*PRIVATE KEY-----/g,
    REDACTED,
  ],
  [/(^|[\s"'(=:/])\/Users\/[^/\s]+(?=\/)/g, "$1~"],
  [/(^|[\s"'(=:/])\/home\/[^/\s]+(?=\/)/g, "$1~"],
  [/(^|[\s"'(=])([A-Z]:\\Users\\[^\\\s]+)(?=\\)/gi, "$1~"],
  [/\bBearer\s+[A-Za-z0-9._~+/=-]{16,}\b/gi, `Bearer ${REDACTED}`],
  // Provider key prefixes. The hyphen form covers OpenAI-style `sk-…`; the underscore form
  // covers Stripe-style `sk_live_…`/`sk_test_…` (and rk_/pk_) which the hyphen pattern misses.
  [/\b(?:sk|rk|pk|ak)-[A-Za-z0-9_-]{20,}\b/g, REDACTED],
  [/\b(?:(?:sk|rk|pk)_(?:live|test)_|whsec_)[A-Za-z0-9]{10,}\b/g, REDACTED],
  [/\bgh[pousr]_[A-Za-z0-9_]{20,}\b/g, REDACTED],
  // AWS access/secret-access key IDs (AKIA long-term, ASIA temporary).
  [/\b(?:AKIA|ASIA)[0-9A-Z]{16}\b/g, REDACTED],
  // Slack tokens (xoxb/xoxp/xoxa/xoxr/xoxs-… and app-level xapp-…).
  [/\b(?:xox[baprs]|xapp)-[A-Za-z0-9-]{10,}\b/g, REDACTED],
  // Service-local relayhistory tokens. Distinct from URL userinfo / Bearer.
  [/\brth_(?:at|rt|st)_[A-Za-z0-9_]{8,}\b/g, REDACTED],
  // OAuth authorize URLs: keep host+path so "stuck at auth.openai.com/oauth/authorize"
  // remains diagnosable, redact the challenge/state/secret query values.
  [
    /([?&](?:code_challenge|code_verifier|state|client_secret|access_token|id_token|refresh_token|assertion)=)[^&\s]*/gi,
    `$1${REDACTED}`,
  ],
  // Google API keys (AIza + 35 chars).
  [/\bAIza[0-9A-Za-z_-]{30,}\b/g, REDACTED],
  // `<name>=<value>` / `<name>: <value>` secret assignments. The identifier may be prefixed
  // (STRIPE_KEY, AWS_SECRET_ACCESS_KEY, GITHUB_TOKEN, DB_PASSWORD) — `(?:[A-Za-z0-9]+_)*`
  // absorbs the prefix segments so the trailing sensitive word still anchors. The leading `\b`
  // + required underscore segments means a bare word like `monkey=…` is NOT matched.
  [
    // `(?:[A-Za-z0-9]+_){0,8}` is bounded, not `*`: an unbounded prefix class in front of a
    // literal is the same quadratic shape fixed in the URL and email patterns. Eight
    // underscore-separated segments is far past any real identifier
    // (`AWS_SECRET_ACCESS_KEY` is three).
    /\b(?:[A-Za-z0-9]+_){0,8}(?:api[_-]?key|apikey|key|secret|token|password|passwd|pwd)\s*[:=]\s*["'`]?[^"'`\s,;]+/gi,
    (match) => {
      // Pick the separator POSITIONALLY (first `:` or `=`) — that is the real name/value
      // delimiter, since the identifier name contains neither. Choosing by `includes("=")`
      // mis-fires on a colon-assigned value whose body has `=` (base64 padding, common in
      // AWS secret keys / k8s/YAML dumps): indexOf would land inside the value and leak the
      // body before it.
      const idx = match.search(/[:=]/);
      const separator = match[idx];
      return `${match.slice(0, idx)}${separator}${REDACTED}`;
    },
  ],
  // A URL scheme is short by definition — RFC 3986 registers nothing close to 32 chars.
  // Leaving the scheme `[a-z0-9+.-]*` unbounded made this pattern QUADRATIC: on any long
  // run of scheme-legal characters the engine consumed to end-of-string hunting for `://`,
  // backtracked the whole way, then repeated from the next offset. That is O(n²), and it is
  // reachable from a single prompt: a 128 KB `AAAA…` prompt cost 21s of CPU here, which took
  // the Worker past its limit (Cloudflare 1102) and 503'd every push from the machine that
  // had it queued — permanently, since a failed batch never advances the cursor.
  // Bounding the scheme makes each start position O(1) and the whole pass linear.
  [/([a-z][a-z0-9+.-]{0,31}:\/\/[^:\s/@]+:)[^@\s/]+(@)/gi, `$1${REDACTED}$2`],
  // Token-only userinfo (`https://<token>@github.com/o/r`) is a credential too. Redact it
  // HERE, keeping the `@host`, rather than leaving it to the email rule below, which would
  // swallow the host along with the token and make the remote undiagnosable. `git@` is
  // the conventional, non-secret SSH user (`ssh://git@github.com/o/r`) and is kept.
  // Userinfo can never contain `?` or `#` (RFC 3986), so an `@` in a query or fragment
  // (`https://host/?to=bob@x.io`) is not mistaken for userinfo and the host survives.
  // Same bounded-scheme shape as the pattern above, so it is linear for the same reason.
  [/([a-z][a-z0-9+.-]{0,31}:\/\/)(?!git@)[^:\s/@?#]+(@)/gi, `$1${REDACTED}$2`],
  // Bounded for the same reason as the URL scheme above, and it is the same failure:
  // `[A-Z0-9._%+-]+` with no upper bound consumed every run of dotted/base64-ish text to
  // end-of-string looking for an `@`, backtracked, then repeated from the next word
  // boundary — quadratic on exactly the kind of blob that shows up in a captured prompt.
  // RFC 5321 caps a local part at 64 characters and a domain at 255, so bounding here
  // costs no real address.
  //
  // The leading lookahead keeps scp-style git remotes (`git@github.com:Org/repo.git`)
  // intact: redacting them turned every SSH remote into `[REDACTED]:Org/repo.git`. Only
  // the conventional, non-secret `git` SSH user followed by `:path` is exempt; any other
  // local part is a real address and stays redacted even when a colon follows it
  // (`bob@example.com:notes`), because a `:path` suffix alone does not make text a remote.
  // The trailing `\.?[A-Z0-9-]` check stops the engine from backtracking to a shorter
  // host so a match always covers the whole address.
  // The leading lookbehind leaves URL userinfo to the userinfo rules above: the only
  // userinfo they keep is `git@` (`ssh://git@github.com/o/r`), which is not an address.
  // Every lookaround is bounded (the remote check by the same RFC limits as the match),
  // so each start position stays O(1) and the pass stays linear.
  [
    /(?<!:\/\/)\b(?!git@[A-Z0-9.-]{1,255}\.[A-Z]{2,24}:[\w~./-])[A-Z0-9._%+-]{1,64}@[A-Z0-9.-]{1,255}\.[A-Z]{2,24}\b(?!\.?[A-Z0-9-])/gi,
    REDACTED,
  ],
];

/**
 * Hard bound on a single string handed to the scrubber. Every path into the store —
 * `content`, the task fields, and each string inside `record` via `scrubJson` — funnels
 * through `scrubText`, so bounding here bounds the whole ingest.
 *
 * 256 KiB is ~10x the p99.9 of real captured prompts (23 KB; nothing in a recent 6k-row
 * sample exceeded 33 KB), so this truncates no genuine history. It exists so that a single
 * pathological record cannot again wedge a machine's push forever the way the quadratic
 * scheme pattern did: even at Cloudflare's 100 MB request-body ceiling the scrub pass now
 * stays well inside the Worker CPU limit.
 */
export const MAX_SCRUB_CHARS = 262_144;

/**
 * Rewind distance allowed when hunting for a safe cut point (see `boundForScrub`).
 */
const SCRUB_CUT_REWIND = 62_144;

const TRUNCATION_NOTE = "[relayhistory: truncated";

/**
 * Truncate over-long input BEFORE the patterns run, cutting at whitespace where possible.
 *
 * Cutting mid-token would leave the leading fragment of a credential in the retained
 * prefix — short enough that no `{20,}`-style pattern matches it, so the scrubber would
 * silently stop protecting the very thing it exists to protect. Rewinding to whitespace
 * means a secret is either wholly retained (and therefore scrubbed) or wholly dropped.
 *
 * If there is no whitespace within the rewind window the input is one enormous unbroken
 * token — a blob or a base64 dump, not a credential — and we cut at the limit. The
 * truncation is always announced in-band so a reader can tell a truncated record from a
 * short one.
 */
function boundForScrub(value: string): string {
  if (value.length <= MAX_SCRUB_CHARS) {
    return value;
  }
  let cut = MAX_SCRUB_CHARS;
  const floor = MAX_SCRUB_CHARS - SCRUB_CUT_REWIND;
  while (cut > floor && !/\s/.test(value[cut - 1] as string)) {
    cut -= 1;
  }
  // JavaScript indexes by UTF-16 code unit, so a cut at the floor can land between the
  // halves of one character and leave a lone surrogate. That is not a display nit:
  // PostgreSQL refuses to convert a lone surrogate to jsonb, so a well-formed prompt
  // that happens to straddle the boundary would fail delivery. Drop the whole pair.
  //
  // Both halves are checked, not just the retained one: a high surrogate that is ALREADY
  // unpaired in the input must survive the cut, so that callers reject it wherever it
  // sits. Dropping it here would let malformed input through at this one offset and
  // nowhere else.
  if (
    /[\uD800-\uDBFF]/.test(value[cut - 1] as string) &&
    /[\uDC00-\uDFFF]/.test(value[cut] as string)
  ) {
    cut -= 1;
  }
  const dropped = value.length - cut;
  return `${value.slice(0, cut)}\n${TRUNCATION_NOTE} ${dropped} characters over the ${MAX_SCRUB_CHARS}-character scrub limit]`;
}

/** Whether `scrubText` will truncate this input (and announce it in band). */
export function exceedsScrubBound(value: string): boolean {
  return value.length > MAX_SCRUB_CHARS;
}

export function scrubText(value: string): string {
  return SECRET_PATTERNS.reduce(
    (text, [pattern, replacement]) => text.replace(pattern, replacement as any),
    boundForScrub(value),
  );
}

export function scrubJson<T>(value: T): T {
  if (typeof value === "string") {
    return scrubText(value) as T;
  }
  if (Array.isArray(value)) {
    return value.map((item) => scrubJson(item)) as T;
  }
  if (value && typeof value === "object") {
    const output: Record<string, unknown> = {};
    for (const [key, item] of Object.entries(value)) {
      output[key] = scrubJson(item);
    }
    return output as T;
  }
  return value;
}

export function scrubRecord<T extends Record<string, unknown>>(
  record: T,
): Record<string, unknown> {
  const scrubbed = scrubJson(record);
  const output: Record<string, unknown> = {};

  for (const [key, value] of Object.entries(scrubbed)) {
    if (key === "confidence") {
      continue;
    }
    if (DEFAULT_RECORD_ALLOWLIST.has(key)) {
      output[key] = stripPromotedConfidence(value);
    }
  }

  return output;
}

function stripPromotedConfidence(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map((item) => stripPromotedConfidence(item));
  }
  if (value && typeof value === "object") {
    const output: Record<string, unknown> = {};
    for (const [key, item] of Object.entries(value)) {
      if (key !== "confidence") {
        output[key] = stripPromotedConfidence(item);
      }
    }
    return output;
  }
  return value;
}

const DEFAULT_RECORD_ALLOWLIST = new Set([
  "id",
  "eventId",
  "messageId",
  "requestId",
  "fingerprint",
  "idFingerprint",
  "source",
  "lens",
  "sessionId",
  "kind",
  "type",
  "ts",
  "startTs",
  "endTs",
  "writtenAt",
  "significance",
  "confidence",
  "tags",
  "model",
  "provider",
  "usage",
  "costUsdMicros",
  "toolName",
  "toolTarget",
  "toolStatus",
  "toolCalls",
  "toolError",
  "activity",
  "stopReason",
  "fidelity",
  "coverage",
  "relationshipType",
  "parentSessionId",
  "rootSessionId",
  "subagentId",
  "actorName",
  "actorRole",
  "filesTouched",
  "filesChanged",
  "codeChurn",
  "trace",
  "decision",
  "task",
  "taskRef",
  "taskTitle",
  "taskDescription",
  "taskStatus",
  "trajectoryId",
  "chapterId",
  "agent",
  "status",
  "commits",
  "sizes",
  "truncated",
]);

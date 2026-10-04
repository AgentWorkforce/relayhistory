const REDACTED = "[REDACTED]";

type Replacement = string | ((substring: string) => string);

/** The sensitive names `<name>=<value>` assignments and structured fields are keyed by. */
const SENSITIVE_NAME =
  "(?:[A-Za-z0-9]+_){0,8}(?:api[_-]?key|apikey|key|secret|token|password|passwd|pwd)";

const SECRET_PATTERNS: Array<[RegExp, Replacement]> = [
  // A private key block, or what is left of one: a block that never reaches its END
  // (cut by truncation, or pasted partially) is redacted up to the next BEGIN, the
  // truncation note, or the end. A block never extends past another BEGIN, so each
  // character is scanned by one block at most and the pass stays linear on input made
  // of repeated, unterminated headers.
  [
    /-----BEGIN [A-Z ]*PRIVATE KEY-----(?:(?!-----BEGIN )[\s\S])*?(?:-----END [A-Z ]*PRIVATE KEY-----|(?=-----BEGIN |\n\[relayhistory: truncated )|$)/g,
    REDACTED,
  ],
  // A home directory ends at the next separator, or, when the path is the directory
  // itself, at whitespace, a quote or the end of the text.
  [/(^|[\s"'(=:/])\/Users\/(?:[^/\s]+(?=\/)|[^/\s"'`]+(?=[\s"'`]|$))/g, "$1~"],
  [/(^|[\s"'(=:/])\/home\/(?:[^/\s]+(?=\/)|[^/\s"'`]+(?=[\s"'`]|$))/g, "$1~"],
  [
    /(^|[\s"'(=])(?:[A-Z]:\\Users\\(?:[^\\\s]+(?=\\)|[^\\\s"'`]+(?=[\s"'`]|$)))/gi,
    "$1~",
  ],
  // The token alphabet ends a value, not `\b`: a value ending in `=` padding has no
  // word boundary after it.
  [
    /\bBearer\s+[A-Za-z0-9._~+/=-]{16,}(?![A-Za-z0-9._~+/=-])/gi,
    `Bearer ${REDACTED}`,
  ],
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
    // A quoted value runs to its closing quote (or the end of the line), spaces and
    // all; an unquoted one to the first space or delimiter.
    new RegExp(
      `\\b${SENSITIVE_NAME}\\s*[:=]\\s*(?:"[^"\\n]+"?|'[^'\\n]+'?|\`[^\`\\n]+\`?|[^"'\`\\s,;]+)`,
      "gi",
    ),
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
    redactAuthorization(boundForScrub(value)),
  );
}

/**
 * An `Authorization` or `Proxy-Authorization` header where a header starts: at the
 * start of a line, after `{`, `,` or `(`, or after the quote opening a quoted header
 * (`-H 'Authorization: …'`, `{"Authorization": …}`). Prose that mentions the header
 * mid-sentence is not one.
 */
const AUTHORIZATION_HEADER =
  /(^|[\r\n{,(]|["'])[ \t]*(?:proxy-)?authorization["']?[ \t]*[:=][ \t]*/gi;
// RFC 7230 token characters, less the quotes that can delimit a quoted header.
const TOKEN = /[A-Za-z0-9!#$%&*+.^_`|~-]/;
// A scheme is followed by its credential, never by the `=` of a parameter.
const AUTHORIZATION_SCHEME =
  /([A-Za-z][A-Za-z0-9!#$%&*+.^_`|~-]*)[ \t]+(?=[^\s=])/y;

/**
 * The quote a header is wrapped in, if any, and the escaping inside it. A header in a
 * JSON string is opened by `\"`, one serialized twice by `\\\"`: each level doubles
 * the backslashes before a character and adds one. A `"`-quoted header's content is
 * one level deeper than its opening quote, so a `"` in it is written with `unit - 1`
 * backslashes, then `unit` more per backslash that escapes it at that level.
 */
interface HeaderQuote {
  char: string;
  /** Backslashes before the header's opening quote. */
  depth: number;
  /** `2^level` for the content's escaping level: 1 unquoted, 2 inside `"…"`. */
  unit: number;
}

/** What a quote at some position is to the credential being read. */
type QuoteRole = "close" | "delimiter" | "content";

/**
 * Redacts each header's credential and keeps its scheme, as the Bearer rule does. The
 * credential has RFC 7235's shape: an auth-param list (`name=value`, comma-separated),
 * or else one token68-like run that ends at whitespace or at the quote closing a
 * quoted header. Text after it is kept. A value is a quoted string or any run up to
 * whitespace, a comma or a quote, since real values are not RFC tokens (AWS SigV4's
 * `Credential=AKID/date/region/s3/aws4_request`).
 *
 * Quotes are decoded by escaping level from the backslash run before them (see
 * `HeaderQuote`): one shallower than the content closes the header; one at the
 * content level opens or closes a quoted value unless an odd number of decoded
 * backslashes escapes it, as in a quoted string's `\"`. So `"foo\\"` ends at its last
 * quote however many times the header around it was serialized.
 *
 * One forward scan: a character is read by the header search and at most three more
 * times by a credential, and a backslash run is read once with the quote it escapes.
 */
function redactAuthorization(text: string): string {
  let output = "";
  let last = 0;
  AUTHORIZATION_HEADER.lastIndex = 0;
  for (
    let header = AUTHORIZATION_HEADER.exec(text);
    header;
    header = AUTHORIZATION_HEADER.exec(text)
  ) {
    let start = header.index + header[0].length;
    const escapes = backslashes(text, start);
    const opening = text[start + escapes];
    let quote: HeaderQuote = { char: "", depth: 0, unit: 1 };
    if (opening === '"' || opening === "'") {
      quote = headerQuote(opening, escapes);
      start += escapes + 1;
    } else if (header[1] === '"' || header[1] === "'") {
      let depth = 0;
      while (text[header.index - depth - 1] === "\\") depth += 1;
      quote = headerQuote(header[1], depth);
    }
    AUTHORIZATION_SCHEME.lastIndex = start;
    const scheme = AUTHORIZATION_SCHEME.exec(text);
    const credential = scheme ? start + scheme[0].length : start;
    const end =
      authParams(text, credential, quote) ?? token68(text, credential, quote);
    if (end === credential) continue;
    output += `${text.slice(last, start)}${scheme ? `${scheme[1]} ` : ""}${REDACTED}`;
    last = end;
    AUTHORIZATION_HEADER.lastIndex = end;
  }
  return output + text.slice(last);
}

function headerQuote(char: string, depth: number): HeaderQuote {
  // A `'` adds no backslash escaping (shell, Python repr), so `"` inside it is read
  // as in an unquoted header.
  if (char !== '"') return { char, depth, unit: 1 };
  return { char, depth, unit: 2 ** (Math.floor(Math.log2(depth + 1)) + 1) };
}

/** The length of the backslash run starting at `at`. */
function backslashes(text: string, at: number): number {
  let run = 0;
  while (text[at + run] === "\\") run += 1;
  return run;
}

/**
 * The role of a quote escaped by `run` backslashes: whether it closes the header,
 * delimits a quoted value, or is part of one. `null` when `char` is not a quote.
 */
function quoteRole(
  char: string | undefined,
  run: number,
  quote: HeaderQuote,
): QuoteRole | null {
  if (char === "'" && quote.char === "'")
    return run <= quote.depth ? "close" : "content";
  if (char !== '"') return null;
  const base = quote.unit - 1;
  if (quote.char === '"' && run < base) return "close";
  if ((run - base) % quote.unit !== 0) return "content";
  return ((run - base) / quote.unit) % 2 === 1 ? "content" : "delimiter";
}

/**
 * Reads one character at `at`, a backslash run and the character it escapes counting
 * as one. Returns the quote role there (null for any other character) and the index
 * after it.
 */
function read(
  text: string,
  at: number,
  quote: HeaderQuote,
): { role: QuoteRole | null; next: number } {
  const run = backslashes(text, at);
  const role = quoteRole(text[at + run], run, quote);
  if (role !== null) return { role, next: at + run + 1 };
  return { role, next: at + Math.max(run, 1) };
}

/** The end of an auth-param list starting at `at`, or null when none starts there. */
function authParams(
  text: string,
  at: number,
  quote: HeaderQuote,
): number | null {
  let end = authParam(text, at, quote);
  if (end === null) return null;
  for (;;) {
    let next: number = end;
    while (text[next] === " " || text[next] === "\t") next += 1;
    if (text[next] !== ",") return end;
    next += 1;
    while (text[next] === " " || text[next] === "\t") next += 1;
    const following: number | null =
      authParam(text, next, quote) ?? strayItem(text, next, quote);
    if (following === null) return end;
    end = following;
  }
}

/**
 * A list item that is not a parameter but holds one (`, stray d=secret`): it runs to
 * the next comma, the end of the line or the header's close, past any quoted value,
 * and is taken whole when it contains `=`, so no `name=value` after a separator is
 * left behind.
 */
function strayItem(
  text: string,
  at: number,
  quote: HeaderQuote,
): number | null {
  let index = at;
  let assigns = false;
  while (
    index < text.length &&
    text[index] !== "," &&
    text[index] !== "\n" &&
    text[index] !== "\r"
  ) {
    if (text[index] === "=") assigns = true;
    const { role, next } = read(text, index, quote);
    if (role === "close") break;
    index = role === "delimiter" ? quotedValue(text, next, quote) : next;
  }
  return assigns ? Math.min(index, text.length) : null;
}

/** The end of one `name=value` auth-param at `at`, or null. */
function authParam(
  text: string,
  at: number,
  quote: HeaderQuote,
): number | null {
  let index = at;
  while (index < text.length && TOKEN.test(text[index]!)) index += 1;
  if (index === at) return null;
  while (text[index] === " " || text[index] === "\t") index += 1;
  if (text[index] !== "=") return null;
  index += 1;
  while (text[index] === " " || text[index] === "\t") index += 1;
  const opening = read(text, index, quote);
  if (opening.role === "delimiter")
    return quotedValue(text, opening.next, quote);
  if (opening.role === "close")
    return looseQuotedValue(text, index, opening.next, quote);
  const value = index;
  while (index < text.length && !/[\s,]/.test(text[index]!)) {
    const { role, next } = read(text, index, quote);
    if (role === "close" || role === "delimiter") break;
    index = next;
  }
  index = Math.min(index, text.length);
  return index === value ? null : index;
}

/**
 * The end of a quoted value whose content starts at `at`: after the next delimiter at
 * the content level, or, unterminated, at the header's close or the end of the line.
 */
function quotedValue(text: string, at: number, quote: HeaderQuote): number {
  let index = at;
  while (index < text.length && text[index] !== "\n" && text[index] !== "\r") {
    const { role, next } = read(text, index, quote);
    if (role === "close") return index;
    if (role === "delimiter") return next;
    index = next;
  }
  return Math.min(index, text.length);
}

/**
 * A value opened by a quote at the header's own level, as in
 * `-H "Authorization: Digest username="alice", …"` where the inner quotes were never
 * escaped. It is a value only when the next quote written the same way closes it and
 * is followed by a separator, the end of the line or the header's close; otherwise
 * the quote closes the header (`{"Authorization":"Digest a=","next":…}`). Returns the
 * value's end, or null when the quote is the header's close.
 */
function looseQuotedValue(
  text: string,
  at: number,
  content: number,
  quote: HeaderQuote,
): number | null {
  const run = content - at - 1;
  let index = content;
  while (index < text.length && text[index] !== "\n" && text[index] !== "\r") {
    const length = backslashes(text, index);
    if (text[index + length] === quote.char) {
      // A shallower quote has already left the header.
      if (length < run) return null;
      if (length !== run) {
        index += length + 1;
        continue;
      }
      const after = index + length + 1;
      const follower = text[after];
      if (
        follower === undefined ||
        follower === "," ||
        /\s/.test(follower) ||
        read(text, after, quote).role === "close"
      )
        return after;
      return null;
    }
    index += Math.max(length, 1);
  }
  return null;
}

/** The end of a single credential: up to whitespace or a quote that is not content. */
function token68(text: string, at: number, quote: HeaderQuote): number {
  let index = at;
  while (index < text.length && !/\s/.test(text[index]!)) {
    const { role, next } = read(text, index, quote);
    if (role === "close" || role === "delimiter") break;
    index = next;
  }
  return Math.min(index, text.length);
}

/** A field name the assignment rule would redact the value of in text, or a header. */
const SENSITIVE_KEY = new RegExp(
  `(?:^|\\b)${SENSITIVE_NAME}$|^(?:proxy-)?authorization$`,
  "i",
);

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
      output[key] = SENSITIVE_KEY.test(key)
        ? redactValues(item)
        : scrubJson(item);
    }
    return output as T;
  }
  return value;
}

/**
 * A structured field never carries its name beside its value the way text does, so a
 * value under a sensitive name is redacted whole: every non-empty string and every
 * number in it. Its shape is kept.
 */
function redactValues(value: unknown): unknown {
  if ((typeof value === "string" && value !== "") || typeof value === "number")
    return REDACTED;
  if (Array.isArray(value)) return value.map(redactValues);
  if (value && typeof value === "object") {
    const output: Record<string, unknown> = {};
    for (const [key, item] of Object.entries(value))
      output[key] = redactValues(item);
    return output;
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

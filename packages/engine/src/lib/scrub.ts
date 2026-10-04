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
 * start of a line (a real one, or `\n` / `\r` escaped into a serialized string), after
 * `{`, `,` or `(`, or after the quote opening a quoted header (`-H 'Authorization: …'`,
 * `{"Authorization": …}`). A key's closing quote may be escaped, as in a header
 * serialized more than once (`{\"authorization\":\"…`). Prose that mentions the header
 * mid-sentence is not one.
 */
const AUTHORIZATION_HEADER =
  /(^|[\r\n{,(]|["']|\\[nr])[ \t]*(?:proxy-)?authorization(\\*["'])?[ \t]*[:=][ \t]*/gi;
// RFC 7230 token characters, less the quotes that can delimit a quoted header.
const TOKEN = /[A-Za-z0-9!#$%&*+.^_`|~-]/;
// Schemes whose credential is token68 (RFC 7235), possibly base64 with `=` padding.
const TOKEN68_SCHEME = /^(?:basic|bearer|negotiate|ntlm)$/i;
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
  /**
   * The backslashes that encode a line break in this header's text: one more than the
   * run of the escaped break the header started after, which proves the text is a
   * serialized message. 0 otherwise, including `"`-quoted headers: a JSON string's `\n`
   * and a literal backslash-n in a shell double-quoted string are the same characters,
   * so a quoted header cannot prove its escapes encode line breaks.
   */
  lineBase: number;
}

/**
 * What a quote at some position is to the credential being read, or `line` for a line
 * break that ends it: a real one, or one escaped into a serialized string (see
 * `escapedBreak`).
 */
type QuoteRole = "close" | "delimiter" | "content" | "line";

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
 * A quoted header's single credential (not a parameter list) runs on to the header's
 * closing quote when that is the next quote on the line, so a credential the shell
 * computes (`"Authorization: Basic $(echo -n user:pass | base64)"`) is redacted whole.
 *
 * One forward scan: a character is read by the header search and at most four more
 * times by a credential, and a backslash run is read once with the quote it escapes.
 * Every lookahead stops at the first quote or line break it meets.
 */
function redactAuthorization(text: string): string {
  let output = "";
  let last = 0;
  const state = headerState();
  AUTHORIZATION_HEADER.lastIndex = 0;
  for (
    let header = AUTHORIZATION_HEADER.exec(text);
    header;
    header = AUTHORIZATION_HEADER.exec(text)
  ) {
    const credential = readCredential(text, header, state, "top");
    if (!credential) continue;
    output += `${text.slice(last, credential.start)}${credential.scheme ? `${credential.scheme} ` : ""}${REDACTED}`;
    last = credential.end;
    state.searched = Math.max(state.searched, credential.end);
    AUTHORIZATION_HEADER.lastIndex = credential.end;
  }
  return output + text.slice(last);
}

/** What one pass over a text carries from header to header. */
interface HeaderState {
  // The quote of the string enclosing the last serialized header line, and where the
  // backward search for it stopped, so the next search covers only the new text.
  enclosing: string;
  searched: number;
  substitutions: SubstitutionScan;
  matches: MatchCache;
}

function headerState(): HeaderState {
  return {
    enclosing: "",
    searched: 0,
    substitutions: { extendedTo: 0, nestedTo: 0 },
    matches: matchCache(),
  };
}

/**
 * One header's credential: where its redaction starts and ends, its scheme, where its
 * substitution failed closed in a region (else -1), and whether that substitution
 * read a window past its fallback without closing.
 */
interface Credential {
  start: number;
  scheme: string | null;
  end: number;
  failedAt: number;
  windowed: boolean;
}

/**
 * How a header is read: from the main pass (`top`), by a sweep (`nested`), by a sweep
 * re-reading a header outside every region (`fresh`), or by a sweep reading a header
 * as the main pass would, against `extendedTo` only (`outer`); see `sweptEnd`. Only
 * `top` and `nested` record failed scans.
 */
type ReadMode = "top" | "nested" | "fresh" | "outer";

/**
 * Reads the credential of the Authorization header matched by `header`: its quoting,
 * scheme, and the parameter list, token68 run or shell substitution it holds, then
 * the header's own close (`closingQuote`, or `serializedLineEnd` on a serialized
 * line) from that credential's end. A substitution that crossed a line break or
 * failed closed inside a region is also extended by `sweptEnd`, and the end is the
 * larger of the two, so the close is never looked for from inside a later secret.
 * A read by a sweep (`mode` other than `top`) does neither: it reads its own
 * credential only, and records failed scans in its own region (`SubstitutionScan`).
 * Null when no credential follows the header.
 */
function readCredential(
  text: string,
  header: RegExpExecArray,
  state: HeaderState,
  mode: ReadMode,
): Credential | null {
  let start = header.index + header[0].length;
  const escapes = backslashes(text, start);
  const opening = text[start + escapes];
  let quote: HeaderQuote = { char: "", depth: 0, unit: 1, lineBase: 0 };
  let before = 0;
  while (text[header.index - before - 1] === "\\") before += 1;
  if (opening === '"' || opening === "'") {
    quote = headerQuote(opening, escapes);
    start += escapes + 1;
  } else if (header[1] === '"' || header[1] === "'") {
    quote = headerQuote(header[1], before);
  }
  if (header[1]!.startsWith("\\")) {
    const lineBase = before + 1;
    if (!quote.char) {
      state.enclosing =
        enclosingQuote(text, header.index - before, state.searched, lineBase) ??
        state.enclosing;
      state.searched = Math.max(state.searched, header.index);
    }
    quote = serializedLine(quote, lineBase, state.enclosing);
  }
  AUTHORIZATION_SCHEME.lastIndex = start;
  const scheme = AUTHORIZATION_SCHEME.exec(text);
  const credential = scheme ? start + scheme[0].length : start;
  // Only these schemes carry a token68 credential whose `=` is base64 padding; any
  // other scheme's `name=` is a parameter (`Digest response=" …"`).
  const padded = scheme !== null && TOKEN68_SCHEME.test(scheme[1]!);
  // A quoted key (`"Authorization":"…"`, `'Authorization': '…'`) holds a value that
  // must escape its own quote, so that value's close is a safe boundary.
  const keyed = header[2] !== undefined && quote.char !== "";
  const substitution = shellSubstitutionEnd(
    text,
    credential,
    quote,
    keyed,
    state.substitutions,
    mode,
  );
  const params =
    substitution === null ? authParams(text, credential, quote, padded) : null;
  const raw = substitution?.end ?? params ?? token68(text, credential, quote);
  let end = raw;
  if (raw === credential) {
    // A scheme followed by something neither grammar reads (`Digest "quoted", …`)
    // still introduces a credential, so the rest of the header is redacted.
    if (!scheme) return null;
    end = unreadCredentialEnd(text, credential, quote);
    if (end === credential) return null;
  } else if (quote.lineBase > 0) {
    // A serialized line's own end is found by `serializedLineEnd`, which keeps the
    // string's close and what follows it.
    end = serializedLineEnd(
      text,
      raw,
      quote,
      params === null && padded && substitution === null ? raw : -1,
    );
  } else if (params === null && quote.char) {
    // A parameter list ends where its grammar does; a single credential that stops
    // short of the header's close was cut at a space the shell would expand.
    end = closingQuote(text, raw, quote) ?? raw;
  } else if (quote.char && read(text, raw, quote).role === "delimiter") {
    // A list cut short by a quoted segment no parameter holds (`a=="x", …`) is
    // malformed; the rest of the header goes with it.
    end = headerEnd(text, raw, quote);
  }
  if (mode === "top" && substitution?.sweep)
    end = Math.max(
      end,
      sweptEnd(
        text,
        credential,
        substitution.sweep.from,
        substitution.sweep.headersFrom,
        quote,
        state,
      ),
    );
  return {
    start,
    scheme: scheme ? scheme[1]! : null,
    end,
    failedAt: substitution?.failed ? substitution.end : -1,
    windowed: substitution?.windowed === true,
  };
}

/**
 * Where a serialized header line ends, at or after its credential's end `at`: the
 * next real or proven escaped line break, the end of the text, or a string's end. A
 * message serialized through several layers of different quoting (a Python repr
 * inside JSON), or with a stray quote before the header that misleads
 * `enclosingQuote`, can stop the credential early; this takes the rest of the header
 * line with it.
 *
 * Shallow quotes (fewer backslashes than a line break, of either kind) are read by
 * position in one forward pass. One right after `=` opens a value, which runs to the
 * next shallow quote of the same kind, whatever it holds (`realm=" a; b"`), except
 * the quote right after a token68 credential, whose `=` is base64 padding
 * (`Basic dXNlcg==",…`), and one the string's structure closes (`structuralClose`:
 * `Token dXNlcg==","level":…`, whatever the scheme). Any other shallow quote may end the string, which
 * `endsString` decides by what follows it, unless a further parameter comes before
 * the next quote of its kind (`realm="Admins' area", response=…`, where the `'` only
 * looked like a close); otherwise it is content. A line break or the end of the text
 * stops the pass in either state, so an unclosed value is redacted to the line's end.
 */
function serializedLineEnd(
  text: string,
  at: number,
  quote: HeaderQuote,
  token68End: number,
): number {
  let index = at;
  let value = "";
  while (index < text.length) {
    const run = backslashes(text, index);
    const char = text[index + run];
    if (char === "\n" || char === "\r") return index + run;
    if (
      run > 0 &&
      (char === "n" || char === "r") &&
      escapedBreak(text, index, run, quote)
    )
      return index;
    if ((char === '"' || char === "'") && run < quote.lineBase) {
      if (value) {
        if (char === value) value = "";
      } else if (
        index !== token68End &&
        afterEquals(text, index) &&
        !structuralClose(text, index + run + 1, char, run)
      ) {
        value = char;
      } else if (
        endsString(text, index + run + 1) &&
        !parameterBefore(text, index + run + 1, char, quote)
      ) {
        return index;
      }
    }
    index += run + 1;
  }
  return text.length;
}

/**
 * Whether a separator and a further `name=` come after `at` before the next shallow
 * quote of kind `char`, a line break or the end. Successive quotes of one kind bound
 * disjoint spans, so the lookahead stays linear.
 */
function parameterBefore(
  text: string,
  at: number,
  char: string,
  quote: HeaderQuote,
): boolean {
  let index = at;
  while (index < text.length) {
    const run = backslashes(text, index);
    const next = text[index + run];
    if (next === "\n" || next === "\r") return false;
    if (next === char && run < quote.lineBase) return false;
    if (
      run > 0 &&
      (next === "n" || next === "r") &&
      escapedBreak(text, index, run, quote)
    )
      return false;
    if (
      run === 0 &&
      (next === "," || next === ";" || next === " " || next === "\t") &&
      nextParameter(text, index + 1)
    )
      return true;
    index += run + 1;
  }
  return false;
}

/**
 * Whether the text from `at`, right after a quote of kind `char` written with `run`
 * backslashes, is the structure that follows a closed string (whitespace, line breaks
 * and indentation allowed between): the end of the text; `,` and the next key
 * (`,"level":`) written with the same quote; or a run of `}`/`]` followed by the end,
 * a line break, the next key, or the close of a string enclosing this one (a quote
 * with fewer backslashes). A well-formed quoted value cannot hold a bare quote of its
 * own level or a raw line break, so none of these occurs inside one; a bare closer
 * followed by anything else (`response="]…"`) is a value's content.
 */
function structuralClose(
  text: string,
  at: number,
  char: string,
  run: number,
): boolean {
  let index = skipWhitespace(text, at);
  if (index >= text.length) return true;
  if (text[index] === ",") return nextKey(text, index + 1, char, run);
  if (text[index] !== "}" && text[index] !== "]") return false;
  const closers = index;
  while (
    index - closers < 64 &&
    (text[index] === "}" ||
      text[index] === "]" ||
      text[index] === " " ||
      text[index] === "\t")
  )
    index += 1;
  const next = text[index];
  if (next === undefined || next === "\n" || next === "\r") return true;
  if (next === ",") return nextKey(text, index + 1, char, run);
  const outer = backslashes(text, index);
  return (
    outer < run && (text[index + outer] === '"' || text[index + outer] === "'")
  );
}

/** Whether a key (`"level":`) written with quote `char` and `run` follows `at`. */
function nextKey(text: string, at: number, char: string, run: number): boolean {
  let index = skipWhitespace(text, at);
  if (backslashes(text, index) !== run || text[index + run] !== char)
    return false;
  index += run + 1;
  const key = index;
  while (
    index - key < 64 &&
    index < text.length &&
    text[index] !== char &&
    text[index] !== "\\" &&
    text[index] !== "\n" &&
    text[index] !== "\r"
  )
    index += 1;
  if (backslashes(text, index) !== run || text[index + run] !== char)
    return false;
  return text[skipWhitespace(text, index + run + 1)] === ":";
}

/** Past up to 64 spaces, tabs and line breaks (pretty-printed indentation). */
function skipWhitespace(text: string, at: number): number {
  let index = at;
  while (index - at < 64 && /[ \t\r\n]/.test(text[index] ?? "")) index += 1;
  return index;
}

/** Whether the text before `at`, past up to 16 spaces or tabs, ends with `=`. */
function afterEquals(text: string, at: number): boolean {
  let index = at - 1;
  while (at - index <= 16 && (text[index] === " " || text[index] === "\t"))
    index -= 1;
  return text[index] === "=";
}

// A further auth-param: the list goes on, so the quote before it is a value's, not
// the string's.
const NEXT_PARAMETER =
  /[ \t]{0,16}[!#$%&'*+.^_`|~0-9A-Za-z-]{1,64}[ \t]{0,16}=/y;

/** Whether a separator (`,`, `;` or bare spaces) and a further `name=` follow `at`. */
function listContinues(text: string, at: number): boolean {
  let index = at;
  while (index - at < 16 && (text[index] === " " || text[index] === "\t"))
    index += 1;
  if (text[index] === "," || text[index] === ";")
    return nextParameter(text, index + 1);
  return index > at && nextParameter(text, index);
}

function nextParameter(text: string, at: number): boolean {
  NEXT_PARAMETER.lastIndex = at;
  return NEXT_PARAMETER.test(text);
}

/**
 * Whether a quote whose next character is at `at` closes its string, judged by what
 * follows it. A separator (`,` `;`) or spaces before another `name=` continue the
 * parameter list (`username="u"; realm=…`, `username="u" response=…`); otherwise a
 * separator, a closing bracket, `:`, a line end, the end of the text, or spaces before
 * anything else (a shell argument, prose) end the string. A quote run straight into
 * other text (`o'brien`, `"6629…`) is a value's.
 */
function endsString(text: string, at: number): boolean {
  let index = at;
  while (index - at < 16 && (text[index] === " " || text[index] === "\t"))
    index += 1;
  const follower = text[index];
  if (follower === undefined || follower === "\n" || follower === "\r")
    return true;
  if (follower === "," || follower === ";")
    return !nextParameter(text, index + 1);
  if (
    follower === "}" ||
    follower === "]" ||
    follower === ")" ||
    follower === ":"
  )
    return true;
  return index > at && !nextParameter(text, index);
}

/**
 * The quote opening the string a serialized header line sits in: the nearest `"` or
 * `'` before `at`, back to `floor`, not escaped at this level (fewer backslashes than
 * the `lineBase` that writes a line break). Null when there is none in that span.
 */
function enclosingQuote(
  text: string,
  at: number,
  floor: number,
  lineBase: number,
): string | null {
  for (let index = at - 1; index >= floor; index -= 1) {
    const char = text[index];
    if (char !== '"' && char !== "'") continue;
    let run = 0;
    while (index - run - 1 >= floor && text[index - run - 1] === "\\") run += 1;
    if (run < lineBase) return char;
    index -= run;
  }
  return null;
}

/**
 * A header line of a serialized message, after an escaped break written with
 * `lineBase` backslashes. Unless the header carries its own quote, its text is the
 * content of the string `enclosing` quotes, at that level (level L writes a break
 * with 2^(L-1) backslashes). In a `"` string (JSON) Digest's `\"` delimits a value and
 * a shallower `"` ends the string. In a `'` string (Python repr, shell `$'…'`) `"` is
 * not escaped, so a bare `"` delimits a value and a shallower `'` ends the string.
 * Backslashes double per level either way.
 */
function serializedLine(
  quote: HeaderQuote,
  lineBase: number,
  enclosing: string,
): HeaderQuote {
  if (quote.char) return { ...quote, lineBase };
  if (enclosing === "'")
    return { char: "'", depth: lineBase - 1, unit: 2 * lineBase, lineBase };
  return { char: '"', depth: lineBase - 1, unit: 2 * lineBase, lineBase };
}

function headerQuote(char: string, depth: number): HeaderQuote {
  // A `'` adds no backslash escaping (shell, Python repr), so `"` inside it is read
  // as in an unquoted header.
  if (char !== '"') return { char, depth, unit: 1, lineBase: 0 };
  return {
    char,
    depth,
    unit: 2 ** (Math.floor(Math.log2(depth + 1)) + 1),
    lineBase: 0,
  };
}

/**
 * The end of a credential neither the parameter nor the token68 grammar could read:
 * the end of a serialized line, the header's closing quote, or the end of the line.
 */
function unreadCredentialEnd(
  text: string,
  at: number,
  quote: HeaderQuote,
): number {
  if (quote.lineBase > 0) return serializedLineEnd(text, at, quote, -1);
  return headerEnd(text, at, quote);
}

/**
 * The header's close at or after `at`, skipping quotes inside it (content, or a quoted
 * value's span), or else the end of its line.
 */
function headerEnd(text: string, at: number, quote: HeaderQuote): number {
  let index = at;
  while (index < text.length) {
    const { role, next } = read(text, index, quote);
    // A quoted header ends at its close; quotes inside it are content or a quoted
    // value's, whose span is skipped.
    if (role === "close" || role === "line") return index;
    index = role === "delimiter" ? quotedValue(text, next, quote) : next;
  }
  return text.length;
}

/** How far a substitution may run past its first line break to find its close. */
const SUBSTITUTION_WINDOW = 4096;

/**
 * Where scans of substitutions that ran past their fallback end (a line break, or a
 * quoted key's value close) without closing stopped, whether at the window or at a
 * serialized string's close. A later substitution starting before it scans afresh,
 * with no window: one that closes is redacted to its close, one that does not fails
 * closed at the first real line break at or after the region's end (on a serialized
 * line also its first escaped break, and on an unkeyed serialized line the string's
 * structural close) or the end of the text, which `sweptEnd` extends so it splits no
 * multi-line secret (`DB_PASSWORD=\n…`, a PEM block), and neither moves it. Every
 * in-region scan is thus consumed by its redaction, and a top-level or nested scan
 * that returns its fallback has read at most `SUBSTITUTION_WINDOW` past it and starts
 * after its kind's last region. A sweep's re-reads add to that (`sweptEnd`).
 *
 * Top-level headers and the headers a sweep reads (`readCredential`'s `mode`) keep
 * separate regions, `extendedTo` and `nestedTo`, each amortised as above: a sweep may
 * read a header no top-level scan would (one inside an outer substitution's quoted
 * text), and its failed scan must not move a later independent header into a region.
 * A nested scan reads both, since a top-level region is no fresher to it. Whatever a
 * nested scan reads below the sweep's end is redacted, and the next top-level header
 * starts past that end, so top-level scans have nothing to take from `nestedTo`.
 * A sweep's `fresh` re-reads read no region and its `outer` reads only `extendedTo`;
 * neither records one, so a re-read is never cut at another re-read's window and
 * neither moves a later header into a region.
 */
interface SubstitutionScan {
  extendedTo: number;
  nestedTo: number;
}

/**
 * Where a shell substitution starting the credential at `at` ends: after the `)` that
 * closes `$(` (counting nested parentheses) or the backtick that closes one. Shell
 * quoting inside it is read: a quote that decodes to a literal quote at the header's
 * content level opens a span (`'…'` without escapes, `"…"` with them) in which
 * parentheses do not count (`$(printf '%s' ')' user:pass)`); a quote of the header's
 * own kind at its own level does too, since bash allows it inside `$(…)`. A
 * substitution may span lines (`$(printf 'user:\npass')`, a `\`-newline continuation);
 * when it closes after a line break, the end it returns asks for a sweep (`closedAt`,
 * `sweptEnd`), so the redaction never ends inside a secret match or before an
 * Authorization header it overlaps.
 *
 * One that does not close ends at its fallback: its first line break or, in a quoted
 * key's value (`keyed`), that value's first close, whichever comes first, or the end
 * of the text when it has neither. It looks for its close up to `SUBSTITUTION_WINDOW`
 * past its start before falling back, and records how far it read in `scan`. On an
 * unkeyed serialized line the string's structural close (`structuralClose`) ends that
 * search. A substitution starting inside an earlier failed scan's region has no
 * fallback and no window: it ends at its own close or fails closed as
 * `SubstitutionScan` describes. Null when the credential is not a substitution.
 */
function shellSubstitutionEnd(
  text: string,
  at: number,
  quote: HeaderQuote,
  keyed: boolean,
  scan: SubstitutionScan,
  mode: ReadMode,
): SubstitutionEnd | null {
  const backtick = text[at] === "`";
  if (!backtick && !(text[at] === "$" && text[at + 1] === "(")) return null;
  let depth = 0;
  let span = "";
  // Where the substitution ends if it never closes: its first line break or, in a
  // quoted key's value, that value's first close.
  let stop = -1;
  // The last real line break crossed, so a close on a later line can be checked.
  let lastBreak = -1;
  // A substitution starting inside an earlier failed scan's region scans afresh, with
  // no window: it ends at its own close or, failing closed, at the first line break
  // at or after the region's end, so nothing straddling that end is cut.
  const frontier =
    mode === "top" || mode === "outer"
      ? scan.extendedTo
      : mode === "nested"
        ? Math.max(scan.extendedTo, scan.nestedTo)
        : -1;
  const region = at < frontier ? frontier : -1;
  let index = backtick ? at + 1 : at;
  while (index < text.length) {
    if (region < 0 && stop >= 0 && index - at > SUBSTITUTION_WINDOW) break;
    const run = backslashes(text, index);
    const char = text[index + run];
    const real = char === "\n" || char === "\r";
    if (
      real ||
      (run > 0 &&
        (char === "n" || char === "r") &&
        escapedBreak(text, index, run, quote))
    ) {
      // Past the region a serialized line has no real breaks, so its proven
      // escaped ones end it there.
      if (region >= 0 && index >= region && (real || quote.lineBase > 0))
        return failedClosed(at, real ? index + run : index);
      if (stop < 0) stop = index;
      if (real) lastBreak = index + run;
      index += run + 1;
      continue;
    }
    const role = quoteRole(char, run, quote);
    if (keyed && role === "close" && stop < 0) stop = index;
    if (
      !keyed &&
      quote.lineBase > 0 &&
      char === quote.char &&
      run < quote.lineBase &&
      structuralClose(text, index + run + 1, char, run)
    ) {
      // The serialized string ends here. A scan that ran past its stop to get here
      // marks the span a failed region, so later headers in it do not rescan it; one
      // already inside a region fails closed here.
      if (region >= 0) return failedClosed(at, index);
      if (stop < 0) return { end: index, sweep: null };
      return windowFailed(scan, mode, index, stop);
    }
    if (char === '"' || char === "'") {
      // Shell quoting: a literal quote, or one of the header's own kind at its own
      // level (bash allows those inside `$(…)`), opens or closes a span. A quote
      // shallower than a serialized line's base is that string's close, never one.
      const shell =
        literalQuote(char, run, quote) ||
        (role === "close" && !(quote.lineBase > 0 && run < quote.lineBase));
      if (shell) {
        if (!span) span = char;
        else if (span === char) span = "";
      }
      index += run + 1;
      continue;
    }
    if (run === 0 && !span) {
      if (backtick && char === "`") return closedAt(lastBreak, index + 1);
      if (!backtick && char === "(") depth += 1;
      if (!backtick && char === ")" && --depth === 0)
        return closedAt(lastBreak, index + 1);
    }
    index += run + 1;
  }
  if (region >= 0) return failedClosed(at, Math.min(index, text.length));
  if (stop < 0) return { end: Math.min(index, text.length), sweep: null };
  return windowFailed(scan, mode, index, stop);
}

/**
 * Where a substitution's scan ended, and the sweep its end still needs (`sweptEnd`):
 * from where, and from which position later Authorization headers count.
 */
interface SubstitutionEnd {
  end: number;
  sweep: { from: number; headersFrom: number } | null;
  // Set when an in-region scan failed closed at `end`, which may cut a substitution
  // straddling it.
  failed?: true;
  // Set when a scan read a window past its fallback (`end`) without closing.
  windowed?: true;
}

/** An in-region scan that failed closed at `end`, which may split a secret. */
function failedClosed(at: number, end: number): SubstitutionEnd {
  return { end, sweep: { from: end, headersFrom: at + 1 }, failed: true };
}

/**
 * A scan that read past its fallback `stop` to `index` without closing: it ends at
 * `stop`, and the span read becomes its kind's failed region (`SubstitutionScan`).
 */
function windowFailed(
  scan: SubstitutionScan,
  mode: ReadMode,
  index: number,
  stop: number,
): SubstitutionEnd {
  if (mode === "top") scan.extendedTo = Math.max(scan.extendedTo, index);
  if (mode === "nested") scan.nestedTo = Math.max(scan.nestedTo, index);
  return { end: stop, sweep: null, windowed: true };
}

/**
 * Private global copies of the secret patterns and the header pattern for
 * `sweptEnd`, so it moves no shared state.
 */
const SWEEP_SECRET_PATTERNS = SECRET_PATTERNS.map(
  ([pattern]) =>
    new RegExp(
      pattern.source,
      pattern.flags.includes("g") ? pattern.flags : `${pattern.flags}g`,
    ),
);
const SWEEP_AUTHORIZATION_HEADER = new RegExp(
  AUTHORIZATION_HEADER.source,
  AUTHORIZATION_HEADER.flags,
);

/**
 * The next match of each sweep pattern at or after where it was last searched from.
 * Sweeps run left to right through a pass and each searches from where the last
 * stopped, so a cached match is reused until the sweep passes it and every pattern
 * reads the text about once per pass, whatever the number of sweeps.
 */
interface MatchCache {
  secrets: { from: number; match: RegExpExecArray | null }[];
  header: { from: number; match: RegExpExecArray | null };
}

function matchCache(): MatchCache {
  return {
    secrets: SWEEP_SECRET_PATTERNS.map(() => ({ from: -1, match: null })),
    header: { from: -1, match: null },
  };
}

function nextMatch(
  text: string,
  pattern: RegExp,
  cached: { from: number; match: RegExpExecArray | null },
  from: number,
): RegExpExecArray | null {
  const reusable =
    cached.from >= 0 &&
    cached.from <= from &&
    (cached.match === null || cached.match.index >= from);
  if (!reusable) {
    pattern.lastIndex = from;
    cached.from = from;
    cached.match = pattern.exec(text);
  }
  return cached.match;
}

/**
 * The end of a substitution closed at `close`: the close itself, which a sweep still
 * extends (`sweptEnd`) when the substitution crossed a real line break, since its `)`
 * may then sit inside text the scrubber would otherwise redact whole
 * (`DB_PASSWORD=pa)ss`, a later header's credential). A close on the substitution's
 * own first line is kept as is. Headers count from the closing line's start.
 */
function closedAt(lastBreak: number, close: number): SubstitutionEnd {
  return {
    end: close,
    sweep: lastBreak < 0 ? null : { from: close, headersFrom: lastBreak },
  };
}

/**
 * Moves `from` so it lands inside no secret match and before no Authorization header
 * it overlaps. Every secret pattern match starting in [`at`, end) that runs past the
 * end moves the end to the match's end; every Authorization header starting in
 * [`headersFrom`, end) moves it to that header's line end (`headerLineEnd`) and to
 * its own credential's end (a `nested` `readCredential`, with no sweep of its own:
 * this sweep goes on over whatever that adds). The end only grows, and everything up
 * to it is redacted. Nested reads keep their own enclosing-quote search, so a header
 * inside a redacted span leaves later top-level headers' quoting as it was.
 *
 * Cost: the matches come from `MatchCache`, so each pattern's search runs forward
 * through the pass once; a pattern is not looked for again inside its own match. A
 * header's line end is reused while later headers fall on the same line. A header
 * inside a credential already read is not read again, separately on each side of
 * `from`: a credential read from before the close (`b),Authorization: …`) can swallow
 * a header after it, which is still read. The reads before `from` are disjoint and
 * so are those after it, so at most one read crosses it, and the line ends and nested
 * credentials read cost O(swept span). All of it lies in the redacted span.
 *
 * A read that failed closed in a region gets `fresh` re-reads of the headers its chain
 * then skips on the line it was cut at (see below). The cut is one of three: the first
 * line break past the region's end, a serialized string's close (a raw header after
 * it follows shell grammar past that close), or the text's end. Those re-reads are
 * disjoint and inside that one line, which the failed read consumed, and only the
 * last reads past the cut or uses its window, at most `SUBSTITUTION_WINDOW`. A
 * re-read's end moves the sweep's end only, never its chain's: a header after the cut
 * is read by the chain whatever a re-read covered. A header the sweep reaches only
 * because a re-read went past its cut is also read as the main pass would (`outer`),
 * which it would have been without that re-read, and keeps the larger end.
 *
 * Cost: linear. Each failed read gets at most one window-reading re-read, and at most
 * one window-reading `outer` read past its cut, and each such window needs its own
 * failing reads and a skipped header. The multiplier depends on how short those can
 * be. Measured fresh characters scanned per input character: about 72 on a 56-character
 * serialized unit, 53 on a 74-character keyed serialized unit, 48 on a 42-character
 * one and 28 on a 147-character one; `outer` reads add at most about 2.
 */
function sweptEnd(
  text: string,
  at: number,
  from: number,
  headersFrom: number,
  quote: HeaderQuote,
  state: HeaderState,
): number {
  let end = from;
  const cursors = SWEEP_SECRET_PATTERNS.map(() => at);
  let headerCursor = headersFrom;
  let lineEnd = -1;
  // The reads of headers before `from`, and of those from it on: how far each chain
  // reached, where its last read failed closed in a region (else -1), and how far its
  // re-reads on that cut line reached.
  const before = { readTo: -1, failedAt: -1, rereadTo: -1 };
  const after = { readTo: -1, failedAt: -1, rereadTo: -1 };
  // The span a re-read reached past its cut, from that cut on.
  let rereadFrom = Infinity;
  let rereadUntil = -1;
  const nested: HeaderState = { ...state };
  for (let changed = true; changed;) {
    changed = false;
    for (const [index, pattern] of SWEEP_SECRET_PATTERNS.entries()) {
      for (;;) {
        const match = nextMatch(
          text,
          pattern,
          state.matches.secrets[index]!,
          cursors[index]!,
        );
        if (!match || match.index >= end) break;
        const matchEnd = match.index + match[0].length;
        cursors[index] = Math.max(matchEnd, match.index + 1);
        if (matchEnd > end) {
          end = matchEnd;
          changed = true;
        }
      }
    }
    for (;;) {
      const header = nextMatch(
        text,
        SWEEP_AUTHORIZATION_HEADER,
        state.matches.header,
        headerCursor,
      );
      if (!header || header.index >= end) break;
      headerCursor = header.index + 1;
      const credential = header.index + header[0].length;
      if (credential > lineEnd)
        lineEnd = headerLineEnd(text, credential, quote);
      let reach = lineEnd;
      const chain = header.index < from ? before : after;
      if (header.index >= chain.readTo) {
        let parsed = readCredential(text, header, nested, "nested");
        if (
          parsed &&
          header.index >= rereadFrom &&
          header.index < rereadUntil
        ) {
          // Only a re-read's reach past its cut brought this header into the sweep;
          // without it the main pass would read it against its own regions, where a
          // later nested region (`nestedTo`) does not cut it. It keeps the larger end.
          const outer = readCredential(text, header, { ...nested }, "outer");
          if (outer && outer.end > parsed.end)
            parsed = { ...parsed, end: outer.end };
        }
        chain.readTo = parsed ? parsed.end : credential;
        chain.failedAt = parsed ? parsed.failedAt : -1;
        chain.rereadTo = -1;
        reach = Math.max(reach, chain.readTo);
      } else if (chain.failedAt === lineEnd && header.index >= chain.rereadTo) {
        // The chain's last read failed closed at this header's line end, which may cut
        // a substitution straddling the region's end
        // (`$(printf 'admin:…\nS3cret' | base64)`); read in the region it would fail
        // closed at the same break, so the headers the chain skips on that line are
        // read outside it, in turn, until one reads past the cut or uses its window: a
        // short header earlier on the line (`Bearer x ,Authorization: Basic $(…`) does
        // not stand in for the straddler. A substitution opening on an earlier line
        // than the cut (`$(printf 'admin:\n…\nS3cret' | base64)`) is not re-read and
        // keeps its later lines, as on a0903995.
        const parsed = readCredential(text, header, nested, "fresh");
        const freshEnd = parsed ? parsed.end : credential;
        if (freshEnd > chain.failedAt && freshEnd > end) {
          rereadFrom = Math.min(rereadFrom, chain.failedAt);
          rereadUntil = Math.max(rereadUntil, freshEnd);
        }
        reach = Math.max(reach, freshEnd);
        if (freshEnd < chain.failedAt && !parsed?.windowed)
          chain.rereadTo = freshEnd;
        else chain.failedAt = -1;
      }
      if (reach > end) {
        end = reach;
        changed = true;
      }
    }
  }
  return end;
}

/**
 * The end of a header's line from `at`: a real line break, or on a serialized line
 * its next proven escaped break or the string's structural close, or the end.
 */
function headerLineEnd(text: string, at: number, quote: HeaderQuote): number {
  if (quote.lineBase === 0) return lineEndAt(text, at);
  let index = at;
  while (index < text.length) {
    const run = backslashes(text, index);
    const char = text[index + run];
    if (char === "\n" || char === "\r") return index + run;
    if (
      run > 0 &&
      (char === "n" || char === "r") &&
      escapedBreak(text, index, run, quote)
    )
      return index;
    if (
      char === quote.char &&
      run < quote.lineBase &&
      structuralClose(text, index + run + 1, char, run)
    )
      return index;
    index += run + 1;
  }
  return text.length;
}

function lineEndAt(text: string, at: number): number {
  let index = at;
  while (index < text.length && text[index] !== "\n" && text[index] !== "\r")
    index += 1;
  return index;
}

/**
 * Whether a quote written with `run` backslashes decodes to a literal quote at the
 * header's content level, the level of a quoted value's delimiters: a `"` with that
 * role, or a `'` written as content writes it (bare outside a `'` string, escaped once
 * inside one).
 */
function literalQuote(char: string, run: number, quote: HeaderQuote): boolean {
  if (char === '"') return quoteRole(char, run, quote) === "delimiter";
  return quote.char === "'" ? run === quote.depth + 1 : run === 0;
}

/**
 * Where the header's closing quote starts when it is the next quote on the line after
 * `at`, or null.
 */
function closingQuote(
  text: string,
  at: number,
  quote: HeaderQuote,
): number | null {
  let index = at;
  while (index < text.length) {
    const { role, next } = read(text, index, quote);
    if (role === "line") return null;
    if (text[next - 1] === quote.char && role !== null)
      return role === "close" ? index : null;
    index = next;
  }
  return null;
}

/** An HTTP field name (RFC 9110 `token`, bounded) followed by its colon. */
const HEADER_NAME = /[!#$%&'*+\-.^_`|~A-Za-z0-9]{1,64}:/y;

/**
 * Whether `run` backslashes at `at` and the `n`/`r` after them encode a line break
 * that ends the header rather than literal text (`DOMAIN\ryan`, `C:\repo\new`). It
 * must be written the way this header's text encodes one (an odd multiple of its
 * `lineBase`; an even multiple is an escaped backslash) and be followed by what can
 * follow a header line: the end, the header's close, another line break, or the next
 * header's name. Anything else is literal. Only a header that began after an escaped
 * line break has a `lineBase`: elsewhere an encoded line break and a literal shell `\n`
 * (`DOMAIN\ryan:password`) are the same characters, so the credential runs on and
 * redaction may take the text after it.
 */
function escapedBreak(
  text: string,
  at: number,
  run: number,
  quote: HeaderQuote,
): boolean {
  const base = quote.lineBase;
  if (base === 0 || run % base !== 0 || (run / base) % 2 !== 1) return false;
  const after = at + run + 1;
  if (after >= text.length) return true;
  const following = backslashes(text, after);
  const char = text[after + following];
  if (char === "\n" || char === "\r") return true;
  if (following > 0 && (char === "n" || char === "r")) return true;
  if (quoteRole(char, following, quote) === "close") return true;
  HEADER_NAME.lastIndex = after;
  return HEADER_NAME.test(text);
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
  // Inside `"` a content-level `"` carries `unit - 1` backslashes; a `'` string
  // leaves it bare.
  const base = quote.char === '"' ? quote.unit - 1 : 0;
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
  const char = text[at + run];
  if (char === "\n" || char === "\r")
    return { role: "line", next: at + run + 1 };
  if (run > 0 && (char === "n" || char === "r"))
    return escapedBreak(text, at, run, quote)
      ? { role: "line", next: at + run + 1 }
      : { role: null, next: at + run };
  const role = quoteRole(char, run, quote);
  if (role !== null) return { role, next: at + run + 1 };
  return { role, next: at + Math.max(run, 1) };
}

/**
 * The end of an auth-param list starting at `at`, or null when none starts there. The
 * list continues past a `,`, and past a `;` or bare spaces when another `name=` follows
 * them (`username="u"; realm="r"`, `username="u" realm="r"`), so prose after a
 * parameter (`a="b" and then more`) is not taken into it.
 */
function authParams(
  text: string,
  at: number,
  quote: HeaderQuote,
  padded: boolean,
): number | null {
  let end = authParam(text, at, quote, padded);
  if (end === null) return null;
  for (;;) {
    let next: number = end;
    while (text[next] === " " || text[next] === "\t") next += 1;
    let following: number | null;
    if (text[next] === ",") {
      next += 1;
      while (text[next] === " " || text[next] === "\t") next += 1;
      following =
        authParam(text, next, quote, padded) ?? strayItem(text, next, quote);
    } else if (text[next] === ";" || next > end) {
      if (text[next] === ";") next += 1;
      if (!nextParameter(text, next)) return end;
      while (text[next] === " " || text[next] === "\t") next += 1;
      following = authParam(text, next, quote, padded);
    } else {
      return end;
    }
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
  while (index < text.length && text[index] !== ",") {
    if (text[index] === "=") assigns = true;
    const { role, next } = read(text, index, quote);
    if (role === "close" || role === "line") break;
    index = role === "delimiter" ? quotedValue(text, next, quote) : next;
  }
  return assigns ? Math.min(index, text.length) : null;
}

/**
 * The end of one `name=value` auth-param at `at`, or null. `padded`: the scheme's
 * credential is token68, so an `=`-only value may be its base64 padding.
 */
function authParam(
  text: string,
  at: number,
  quote: HeaderQuote,
  padded: boolean,
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
  if (opening.role === "line") return null;
  const value = index;
  let padding = true;
  while (index < text.length && !/[\s,]/.test(text[index]!)) {
    const { role, next } = read(text, index, quote);
    if (role === "close" || role === "delimiter" || role === "line") break;
    if (text[index] !== "=") padding = false;
    index = next;
  }
  index = Math.min(index, text.length);
  // An empty value, or under a token68 scheme an `=`-only one, is an empty parameter
  // when the list goes on after it (`realm=, response=…`); otherwise it is no
  // parameter, and `=` is the credential's base64 padding (`dXNlcg==`).
  if (index === value || (padded && padding))
    return listContinues(text, index) ? index : null;
  return index;
}

/**
 * The end of a quoted value whose content starts at `at`: after the next delimiter at
 * the content level, or, unterminated, at the header's close or the end of the line.
 */
function quotedValue(text: string, at: number, quote: HeaderQuote): number {
  let index = at;
  while (index < text.length) {
    const { role, next } = read(text, index, quote);
    if (role === "close") return index;
    // Only a real line break ends a quoted value; an escaped one is its content.
    if (role === "line" && (text[index] === "\n" || text[index] === "\r"))
      return index;
    if (role === "delimiter") return next;
    index = next;
  }
  return Math.min(index, text.length);
}

/**
 * A value opened by a quote at the header's own level, as in
 * `-H "Authorization: Digest username="alice", …"` where the inner quotes were never
 * escaped. It is a value only when the next quote on the line is written the same way
 * and is followed by a separator (`,`, whitespace, or `;` before another `name=`),
 * the end of the line or the header's close; when it
 * is written the same way but followed by anything else, the quote closes the header
 * (`{"Authorization":"Digest a=","next":…}`). A next quote written at another depth
 * (`username="al\"ice", response="…"`) leaves the value's extent unknown, so the value
 * runs to the end of the line. Returns the value's end, or null when the quote is the
 * header's close.
 */
function looseQuotedValue(
  text: string,
  at: number,
  content: number,
  quote: HeaderQuote,
): number | null {
  const run = content - at - 1;
  let index = content;
  while (index < text.length) {
    const length = backslashes(text, index);
    const char = text[index + length];
    if (char === "\n" || char === "\r") return null;
    if (length > 0 && (char === "n" || char === "r")) {
      if (escapedBreak(text, index, length, quote)) return null;
      index += length;
      continue;
    }
    if (char === quote.char) {
      if (length !== run) return lineEnd(text, index, quote);
      const after = index + length + 1;
      const follower = text[after];
      if (
        follower === undefined ||
        follower === "," ||
        (follower === ";" && nextParameter(text, after + 1)) ||
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

/** Where the line holding `at` ends: its next real or proven escaped line break. */
function lineEnd(text: string, at: number, quote: HeaderQuote): number {
  let index = at;
  while (index < text.length) {
    const length = backslashes(text, index);
    const char = text[index + length];
    if (char === "\n" || char === "\r") return index + length;
    if (
      length > 0 &&
      (char === "n" || char === "r") &&
      escapedBreak(text, index, length, quote)
    )
      return index;
    index += Math.max(length, 1);
  }
  return text.length;
}

/** The end of a single credential: up to whitespace or a quote that is not content. */
function token68(text: string, at: number, quote: HeaderQuote): number {
  let index = at;
  while (index < text.length && !/\s/.test(text[index]!)) {
    const { role, next } = read(text, index, quote);
    if (role === "close" || role === "delimiter" || role === "line") break;
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

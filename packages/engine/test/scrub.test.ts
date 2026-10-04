import { describe, expect, it } from "vitest";
import {
  MAX_SCRUB_CHARS,
  scrubJson,
  scrubRecord,
  scrubText,
} from "../src/lib/scrub.js";

describe("hosted ingest scrubbing", () => {
  it("redacts content in place so readable value remains useful", () => {
    expect(
      scrubText(
        "Use token ghp_123456789012345678901234567890123456 and email dev@example.com",
      ),
    ).toBe("Use token [REDACTED] and email [REDACTED]");
  });

  it("redacts rth_ tokens and oauth challenge/state while keeping the authorize host", () => {
    const url =
      "https://auth.openai.com/oauth/authorize?client_id=app&code_challenge=abc123secret&state=xyz789";
    const out = scrubText(`${url} rth_at_liveexampletokenvalue`);
    expect(out).toContain(
      "https://auth.openai.com/oauth/authorize?client_id=app",
    );
    expect(out).toContain("code_challenge=[REDACTED]");
    expect(out).toContain("state=[REDACTED]");
    expect(out).not.toContain("abc123secret");
    expect(out).not.toContain("xyz789");
    expect(out).not.toContain("rth_at_liveexampletokenvalue");
    expect(out).toContain("[REDACTED]");
  });

  it("normalizes home-directory usernames in path-like fields", () => {
    expect(scrubText("/Users/khaliqgant/Projects/app/file.ts")).toBe(
      "~/Projects/app/file.ts",
    );
    expect(scrubText("/home/alice/work/app/file.ts")).toBe(
      "~/work/app/file.ts",
    );
    expect(scrubText("C:\\Users\\Alice\\work\\app\\file.ts")).toBe(
      "~\\work\\app\\file.ts",
    );

    const record = scrubRecord({
      filesTouched: ["/Users/khaliqgant/Projects/app/file.ts"],
      toolCalls: [
        {
          name: "edit",
          target: "/home/alice/work/app/file.ts",
          metadata: { sessionPath: "C:\\Users\\Alice\\work\\session.jsonl" },
        },
      ],
      trace: {
        files: [{ path: "/Users/alice/Projects/repo/src/app.ts" }],
      },
      codeChurn: {
        changed: ["/home/alice/repo/src/app.ts"],
      },
    });

    expect(record.filesTouched).toEqual(["~/Projects/app/file.ts"]);
    expect(record.toolCalls).toEqual([
      {
        name: "edit",
        target: "~/work/app/file.ts",
        metadata: { sessionPath: "~\\work\\session.jsonl" },
      },
    ]);
    expect(record.trace).toEqual({
      files: [{ path: "~/Projects/repo/src/app.ts" }],
    });
    expect(record.codeChurn).toEqual({
      changed: ["~/repo/src/app.ts"],
    });
  });

  it("drops raw passthrough while preserving typed provenance outside raw", () => {
    const record = scrubRecord({
      eventId: "event-1",
      sessionId: "session-1",
      transcript: "raw prompt with dev@example.com",
      request: {
        headers: { authorization: "Bearer abcdefghijklmnopqrstuvwxyz" },
      },
      raw: {
        id: "event-1",
        sessionId: "session-1",
        env: { OPENAI_API_KEY: "sk-123456789012345678901234567890" },
        transcript: "contact dev@example.com",
      },
      decision: {
        reasoning: "Bearer abcdefghijklmnopqrstuvwxyz123456",
        confidence: 0.8,
      },
      confidence: 0.8,
    });

    expect(record.raw).toBeUndefined();
    expect(record.transcript).toBeUndefined();
    expect(record.request).toBeUndefined();
    expect(record.eventId).toBe("event-1");
    expect(record.sessionId).toBe("session-1");
    expect(record.decision).toEqual({
      reasoning: "Bearer [REDACTED]",
    });
    expect(record.confidence).toBeUndefined();
  });

  // Broadened secret coverage — surfaced by the #29 Learn dev-org round-trip, where a real
  // `STRIPE_KEY=sk_live_…` bait came back UNREDACTED through the Pair snippet (egress) because
  // SECRET_PATTERNS only covered OpenAI-style `sk-` (hyphen) + a fixed keyword list. scrubText is
  // the shared chokepoint (ingest + Pair egress), so this corpus guards both boundaries.
  describe("secret-pattern coverage corpus", () => {
    const mustRedact: Array<[string, string]> = [
      // the exact leaked bait — now caught twice (underscore-Stripe value + keyword), belt+braces
      ["stripe key assignment", "`STRIPE_KEY=sk_live_FAKE1234567890abcdef`"],
      ["bare stripe secret", "sk_live_FAKE1234567890abcdef"],
      ["stripe test key", "sk_test_ABCDEFGHIJ1234567890"],
      ["stripe webhook secret", "whsec_ABCDEFGHIJ1234567890"],
      ["aws access key (AKIA)", "AKIAIOSFODNN7EXAMPLE"],
      ["aws temp key (ASIA)", "ASIAY34FZKBOKMUTVV7A"],
      // Assembled at runtime so repository secret scanning does not read the fake as live.
      [
        "slack bot token",
        ["xoxb", "123456789012", "abcdefghijklmnop"].join("-"),
      ],
      ["slack app token", "xapp-1-A123-456-abcdefghij"],
      ["google api key", "AIzaSyA1234567890abcdefghijklmnopqrstuv"],
      ["custom *_KEY assignment", "STRIPE_KEY=someCustomValue1234"],
      [
        "aws secret access key kw",
        "AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMIbPxRfiCYEXAMPLEKEY",
      ],
      ["db password assignment", "DATABASE_PASSWORD=p@ssw0rd123"],
      // regressions — existing patterns must still fire after the broadening
      ["[regression] openai sk- hyphen", "sk-proj-ABCDEFGHIJKLMNOPQRSTUVWX"],
      ["[regression] github ghp_", "ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456"],
      ["[regression] api_key=", "api_key=supersecretvalue123"],
      ["[regression] Bearer", "Authorization: Bearer abcdefghij1234567890"],
    ];
    it.each(mustRedact)("redacts %s", (_label, input) => {
      expect(scrubText(input)).toContain("[REDACTED]");
    });

    // Negatives — prose mentioning token/key/secret WITHOUT an assignment must NOT be redacted
    // (over-redaction only ever in the safe direction; these prove we didn't go too far).
    const mustKeep: Array<[string, string]> = [
      ["prose 'token'", "Rotate the token before release please"],
      ["prose 'key'", "The cache key strategy was discussed"],
      ["substring 'monkey'", "the monkey patch in utils.ts"],
      ["actionable sentence", "Fix the auth middleware token check edge case"],
      ["prose 'secret'", "keep this a secret from nobody"],
    ];
    it.each(mustKeep)("preserves %s", (_label, input) => {
      expect(scrubText(input)).not.toContain("[REDACTED]");
      expect(scrubText(input)).toBe(input);
    });

    // Positional-separator regression: a COLON-assigned value whose body contains `=` (base64
    // padding — routine for AWS secret keys / YAML / k8s / tool-output dumps Learn ingests).
    // The separator must be the FIRST `:`/`=` (the real delimiter), not chosen by `includes("=")`
    // — otherwise the redaction starts inside the value and the secret body before it survives.
    // These assert the BODY is gone (a bare toContain("[REDACTED]") would not catch the leak).
    const leakBodies: Array<[string, string, string]> = [
      [
        "colon + base64 token (== padding)",
        "token: dGVzdF9zZWNyZXRfYm9keQ==",
        "dGVzdF9zZWNyZXRfYm9keQ",
      ],
      [
        "colon + AWS secret access key (40-char, trailing =)",
        "AWS_SECRET_ACCESS_KEY: wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPL=",
        "wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPL",
      ],
    ];
    it.each(leakBodies)(
      "fully redacts %s (no body survives)",
      (_label, input, body) => {
        const out = scrubText(input);
        expect(out).toContain("[REDACTED]");
        expect(out).not.toContain(body);
      },
    );

    // `=`-separator WITH `=` in the value must still redact the whole value (locks both paths).
    it("redacts an =-assigned value that itself contains =", () => {
      expect(scrubText("api_key=a=b=c")).toBe("api_key=[REDACTED]");
    });
  });

  describe("git remotes and URL userinfo", () => {
    it("keeps scp-style SSH remotes intact", () => {
      for (const remote of [
        "git@github.com:AgentWorkforce/relayhistory.git",
        "git@gitlab.example.com:group/sub/repo.git",
        "git@bitbucket.org:team/repo",
      ])
        expect(scrubText(`origin\t${remote} (fetch)`)).toBe(
          `origin\t${remote} (fetch)`,
        );
    });

    it("still redacts emails, including ones followed by prose punctuation", () => {
      expect(scrubText("mail bob@example.com: now")).toBe(
        "mail [REDACTED]: now",
      );
      expect(scrubText("mail bob@example.com.")).toBe("mail [REDACTED].");
      expect(scrubText("Author: Dev <dev@example.co.uk>")).toBe(
        "Author: Dev <[REDACTED]>",
      );
      expect(scrubText("to bob@example.com, alice@mail.example.org")).toBe(
        "to [REDACTED], [REDACTED]",
      );
      expect(scrubText("ends with bob@example.com:")).toBe(
        "ends with [REDACTED]:",
      );
    });

    it("redacts addresses followed by :path unless the user is the git SSH user", () => {
      expect(scrubText("see bob@example.com:notes")).toBe(
        "see [REDACTED]:notes",
      );
      expect(scrubText("deploy@git.internal-host.io:~/repos/app.git")).toBe(
        "[REDACTED]:~/repos/app.git",
      );
      expect(scrubText("xgit@github.com:o/r")).toBe("[REDACTED]:o/r");
      expect(scrubText("a.git@github.com:o/r")).toBe("[REDACTED]:o/r");
    });

    it("redacts URL credentials but keeps the host", () => {
      expect(scrubText("https://user:tok123secret@github.com/o/r")).toBe(
        "https://user:[REDACTED]@github.com/o/r",
      );
      expect(scrubText("https://tok123secret@github.com/o/r")).toBe(
        "https://[REDACTED]@github.com/o/r",
      );
      expect(scrubText("git clone https://x-token@gitlab.com/o/r.git")).toBe(
        "git clone https://[REDACTED]@gitlab.com/o/r.git",
      );
      expect(scrubText("ssh://git@github.com/o/r.git")).toBe(
        "ssh://git@github.com/o/r.git",
      );
      expect(scrubText("https://github.com/o/r")).toBe(
        "https://github.com/o/r",
      );
      expect(scrubText("https://example.com?to=bob@mail.example.org")).toBe(
        "https://example.com?to=[REDACTED]",
      );
      expect(scrubText("https://example.com#bob@mail.example.org")).toBe(
        "https://example.com#[REDACTED]",
      );
    });
  });

  // ---------------------------------------------------------------------------
  // Cost. On 2026-09-03 every push from this fleet 503'd with "Worker exceeded
  // resource limits": the URL-credential pattern's unbounded scheme made scrubText
  // O(n^2), so a single 128 KB prompt of "AAAA..." burned 21s of Worker CPU. A failed
  // batch never advances the cursor, so the machine was wedged permanently.
  // These tests fail loudly if the pass ever goes superlinear again.
  // ---------------------------------------------------------------------------
  describe("scrub cost stays linear in input size", () => {
    // Shapes built only from characters legal in a URL scheme ([a-z0-9+.-]) are the
    // ones that triggered the blowup; a shape with separators never did.
    const shapes: Array<[string, (n: number) => string]> = [
      ["unbroken letters", (n) => "A".repeat(n)],
      ["dotted words", (n) => "a.b".repeat(Math.ceil(n / 3)).slice(0, n)],
      ["scheme-like run", (n) => "http".repeat(Math.ceil(n / 4)).slice(0, n)],
      ["base64-ish", (n) => "aGVsbG8+".repeat(Math.ceil(n / 8)).slice(0, n)],
      // Shapes aimed at the email rule's trailing lookahead and the userinfo rules.
      ["email-ish", (n) => "a.b@c.de".repeat(Math.ceil(n / 8)).slice(0, n)],
      [
        "scp-remote-ish",
        (n) => "git@a.b.cd:".repeat(Math.ceil(n / 11)).slice(0, n),
      ],
      ["userinfo-ish", (n) => "a://b@".repeat(Math.ceil(n / 6)).slice(0, n)],
      ["long domain", (n) => ("a@" + "b.".repeat(n)).slice(0, n)],
      ["git long domain", (n) => ("git@" + "b.".repeat(n)).slice(0, n)],
      ["repeated git@", (n) => "git@".repeat(Math.ceil(n / 4)).slice(0, n)],
      // Shapes aimed at the quoted-assignment, home-path and bearer endings.
      ["open quoted assignment", (n) => `token="${"a ".repeat(n)}`.slice(0, n)],
      [
        "repeated home dirs",
        (n) => " /Users/a'".repeat(Math.ceil(n / 10)).slice(0, n),
      ],
      ["unbroken home dir", (n) => `/home/${"a".repeat(n)}`.slice(0, n)],
      [
        "repeated authorization",
        (n) => "\nAuthorization:  ".repeat(Math.ceil(n / 17)).slice(0, n),
      ],
      ["spaced authorization", (n) => `,${" ".repeat(n)}`.slice(0, n)],
      [
        "long digest parameter list",
        (n) => `Authorization: Digest ${"a=b, ".repeat(n)}`.slice(0, n),
      ],
      [
        "escaped header quotes",
        (n) =>
          '\\\\\\"Authorization: Digest a=\\\\\\\\\\\\\\"x'
            .repeat(Math.ceil(n / 40))
            .slice(0, n),
      ],
      [
        "long backslash run",
        (n) => `\\"Authorization: Digest a=${"\\".repeat(n)}`.slice(0, n),
      ],
      [
        "unmatched loose quote",
        (n) => `"Authorization: Digest a="${"x".repeat(n)}`.slice(0, n),
      ],
      [
        "compact JSON headers",
        (n) =>
          '{"Authorization":"Digest a=","r":"s"}'
            .repeat(Math.ceil(n / 38))
            .slice(0, n),
      ],
      [
        "headers at rising escape depths",
        (n) => {
          let text = "";
          for (let depth = 0; text.length < n; depth += 1)
            text += `${"\\".repeat(depth)}"Authorization: Digest a=${"\\".repeat(depth)}"`;
          return text.slice(0, n);
        },
      ],
      [
        "literal backslash-r runs in a value",
        (n) => `"Authorization: Digest a=${"x\\r".repeat(n)}=`.slice(0, n),
      ],
      [
        "loose quotes meeting another depth",
        (n) =>
          '"Authorization: Digest a="x\\"y\n'
            .repeat(Math.ceil(n / 31))
            .slice(0, n),
      ],
      [
        "single-quoted serialized headers",
        (n) =>
          `'${'GET /\\r\\nAuthorization: Digest a="x", b="y"\\r\\nHost: z'.repeat(n)}'`.slice(
            0,
            n,
          ),
      ],
      [
        "a Python repr of many messages inside JSON",
        (n) =>
          JSON.stringify({
            log: `'${'GET /\\r\\nAuthorization: Digest username="o\\\'b", realm="a, b"\\r\\nHost: x'.repeat(n)}'`,
          }).slice(0, n),
      ],
      [
        "quote-comma runs after a serialized header",
        (n) =>
          `\\nAuthorization: Digest a=b ${`", ${"k".repeat(60)} `.repeat(n)}`.slice(
            0,
            n,
          ),
      ],
      [
        "quote-space runs after a serialized header",
        (n) =>
          `'\\nAuthorization: Digest a=b ${`"${" ".repeat(15)}${"k".repeat(60)} `.repeat(n)}`.slice(
            0,
            n,
          ),
      ],
      [
        "many quoted values on one serialized line",
        (n) =>
          `'GET /\\r\\nAuthorization: Digest ${'a=" x"; '.repeat(n)}`.slice(
            0,
            n,
          ),
      ],
      [
        "a Digest list separated by semicolons",
        (n) => `Authorization: Digest ${'a="x"; '.repeat(n)}`.slice(0, n),
      ],
      [
        "alternating quote kinds after a serialized header",
        (n) =>
          JSON.stringify({
            r: `Here's\r\nAuthorization: Basic x${"' a, k=v\" b".repeat(n)}`,
          }).slice(0, n),
      ],
      [
        "credentials that start with a quote",
        (n) =>
          'Authorization: Digest "q", a=b\n'
            .repeat(Math.ceil(n / 30))
            .slice(0, n),
      ],
      [
        "nested shell substitutions",
        (n) =>
          `Authorization: Basic $(${"$(a ".repeat(n / 8)}${")".repeat(n / 8)})`.slice(
            0,
            n,
          ),
      ],
      [
        "many quote-first JSON headers",
        (n) =>
          '{"Authorization":"Basic \\"x\\"","level":"info"}'
            .repeat(Math.ceil(n / 46))
            .slice(0, n),
      ],
      [
        "unclosed substitutions in quoted keys",
        (n) =>
          '{"Authorization":"Basic $(echo a","n":"k"}'
            .repeat(Math.ceil(n / 42))
            .slice(0, n),
      ],
      [
        "unmatched multi-line substitutions",
        (n) =>
          "Authorization: Basic $(printf 'a\n"
            .repeat(Math.ceil(n / 33))
            .slice(0, n),
      ],
      [
        "closer runs inside serialized values",
        (n) =>
          `'\\nIf-None-Match: "v1"\\nAuthorization: Digest r="${"} ".repeat(n)}`.slice(
            0,
            n,
          ),
      ],
      [
        "nested same-kind quotes in shell substitutions",
        (n) =>
          `-H "Authorization: Basic $(printf "%s" ")" a)" `
            .repeat(Math.ceil(n / 46))
            .slice(0, n),
      ],
      [
        "unmatched keyed multi-line substitutions",
        (n) =>
          '{"Authorization":"Basic $(echo a\n'
            .repeat(Math.ceil(n / 33))
            .slice(0, n),
      ],
      [
        "alternating malformed and well-formed substitutions",
        (n) =>
          `{"Authorization":"Basic $(echo","x":1} {"Authorization": "Basic $(printf "%s" a:b | base64)"} `
            .repeat(Math.ceil(n / 100))
            .slice(0, n),
      ],
      [
        "unmatched serialized substitutions ending at a string close",
        (n) => {
          let group = "";
          while (group.length < 4000) group += "\\nAuthorization: Basic $(a";
          return `{"m":"${group}","k":1}`
            .repeat(Math.ceil(n / 4010))
            .slice(0, n);
        },
      ],
      [
        "plain credentials after failed substitutions",
        (n) =>
          `Authorization: Basic $(oops\n${"x".repeat(60)}\nAuthorization: Basic abc\n`
            .repeat(Math.ceil(n / 115))
            .slice(0, n),
      ],
      [
        "multi-line substitutions with long closing lines",
        (n) =>
          `Authorization: Basic $(printf a\n${"x".repeat(200)})${"y ".repeat(500)}\n`
            .repeat(Math.ceil(n / 1236))
            .slice(0, n),
      ],
      [
        "cross-line substitutions full of near-miss secrets",
        (n) =>
          `Authorization: Basic $(printf a\n${"password -----BEGI http:/ a.b@c. sk- ".repeat(110)})\n`
            .repeat(Math.ceil(n / 4000))
            .slice(0, n),
      ],
      [
        "stray parameter items",
        (n) =>
          `Authorization: AWS4 C=a/b, ${"x y=z/w, ".repeat(n)}`.slice(0, n),
      ],
      [
        "repeated open quoted parameters",
        (n) =>
          '\nAuthorization: Digest a="x, b=\\"'
            .repeat(Math.ceil(n / 34))
            .slice(0, n),
      ],
      [
        "repeated bearer",
        (n) => "Bearer a=".repeat(Math.ceil(n / 9)).slice(0, n),
      ],
    ];

    // Median of several runs: a single wall-clock sample is noisy enough on a loaded
    // machine to flake, and a flaky guard gets deleted rather than believed.
    const medianCost = (input: string, runs = 5): number => {
      const samples: number[] = [];
      for (let i = 0; i < runs; i += 1) {
        const started = performance.now();
        scrubText(input);
        samples.push(performance.now() - started);
      }
      return samples.sort((a, b) => a - b)[Math.floor(runs / 2)] as number;
    };

    it.each(shapes)(
      "scrubs 64 KB of %s well inside the CPU budget",
      (_l, gen) => {
        // Measured after the fix: 8-24ms. Before it: ~5,000ms. The bar sits ~30x above
        // the real cost and ~10x below the regression, so it is loud without being flaky.
        expect(medianCost(gen(65_536))).toBeLessThan(500);
      },
    );

    /**
     * The linearity guard, stated as an absolute budget at a large input rather than as a
     * ratio between two small ones. At 20 KB the pass takes single-digit milliseconds, so
     * a ratio of two such samples is dominated by scheduler noise and flakes when the
     * suite runs in parallel — it failed at 25x on a loaded box while the code was fine.
     *
     * 200 KB is unambiguous instead: measured at ~26ms after the fix and 53,167ms before
     * it. Any return to quadratic misses this budget by more than an order of magnitude,
     * and no amount of CI load makes the linear version approach it.
     */
    it("scrubs 200 KB far inside the budget a quadratic pass could never meet", () => {
      expect(medianCost("A".repeat(200_000), 3)).toBeLessThan(2_000);
    });

    // Headers on one line after a substitution that closed on a later line: each is
    // swept, and its line end and credential are read once, not once per header.
    // Measured at ~20ms for 256 KiB; a per-header read of the line took 3,000-4,500ms.
    it.each<[string, (n: number) => string]>([
      [
        "headers after a cross-line close on one line",
        (n) =>
          `Authorization: Basic $(a\n,Authorization: b)${",Authorization: c".repeat(n / 17)}`.slice(
            0,
            n,
          ),
      ],
      [
        "headers after a failed scan's cross-line close on one line",
        (n) =>
          `Authorization: Basic $(oops\nAuthorization: Basic a)${",Authorization: x".repeat(n / 17)}`.slice(
            0,
            n,
          ),
      ],
      [
        "swept headers whose substitutions never close",
        (n) =>
          "Authorization: Basic $(a\n,Authorization: Basic $(zz' )\n"
            .repeat(n / 54)
            .slice(0, n),
      ],
      [
        "swept headers whose substitutions never close, between multi-line headers",
        (n) =>
          "Authorization: Basic $(a\n,Authorization: Basic $(zz' )\nAuthorization: Basic $(printf 'admin:\nS3cret' | base64)\nHost: x\n"
            .repeat(n / 120)
            .slice(0, n),
      ],
      [
        "swept headers past a key whose substitutions never close",
        (n) =>
          "Authorization: Basic $(a\napi_key=x)y(Authorization: Basic $(zz\n"
            .repeat(n / 60)
            .slice(0, n),
      ],
      [
        "swept headers past a header before the close whose substitutions never close",
        (n) =>
          "Authorization: Basic $(a\n,Authorization: b),Authorization: Basic $(zz\n"
            .repeat(n / 70)
            .slice(0, n),
      ],
      [
        "swept reads failing closed inside a later header's credential",
        (n) =>
          "Authorization: Basic $(a\n,Authorization: b) ,Authorization: Basic $(zz\nAuthorization: Basic $(printf 'admin:\nS3cret | base64\n"
            .repeat(n / 125)
            .slice(0, n),
      ],
      [
        "short headers re-read on lines a swept read was cut at",
        (n) => {
          const unit =
            "Authorization: Basic $(oops\nAuthorization: Basic $(a\n,Authorization: b) ,Authorization: Basic $(zz\n" +
            `${"x".repeat(63)}\n`.repeat(62) +
            "Authorization: x ,".repeat(400) +
            "Authorization: Basic $(printf 'admin:\nS3cret' | base64)\nHost: x\n";
          return unit.repeat(Math.ceil(n / unit.length)).slice(0, n);
        },
      ],
      [
        "re-reads that never close, one sweep after another",
        (n) =>
          "Authorization: Basic $(a\n,Authorization: b) ,Authorization: Basic $(zz\nAuthorization: Basic $(never qqq\nAuthorization: Basic $(a\n,Authorization: b) ,Authorization: Basic $(zz\nAuthorization: Basic $(printf 'admin:\nS3cret' | base64)\n"
            .repeat(n / 231)
            .slice(0, n),
      ],
      [
        "re-reads after short regions, one sweep after another",
        (n) =>
          'Authorization: Basic $(a\n,Authorization: b) {"m":"x\\nAuthorization: Basic $(q\\nX: y,Authorization: Basic $(b ,Authorization: Basic $(never","k":1}\n'
            .repeat(n / 147)
            .slice(0, n),
      ],
      [
        "keyed headers on serialized lines cut at their string's close",
        (n) =>
          'Authorization: Basic $(oops\n\')"Authorization": "Basic $(q","k":1}\n{"m":"\\n'
            .repeat(n / 74)
            .slice(0, n),
      ],
      [
        "short serialized regions closed by their string",
        (n) =>
          '{"m":"x\\nAuthorization: Basic $(a\\nX: y,Authorization: Basic $(b","k":1}\n'
            .repeat(n / 73)
            .slice(0, n),
      ],
      [
        "headers before a cross-line close",
        (n) =>
          `Authorization: Basic $(oops\n${",Authorization: b".repeat(200)})\n`
            .repeat(n / 3430)
            .slice(0, n),
      ],
    ])("scrubs 256 KiB of %s far inside the budget", (_l, gen) => {
      expect(medianCost(gen(MAX_SCRUB_CHARS), 3)).toBeLessThan(1_000);
    });
  });

  // ---------------------------------------------------------------------------
  // The bound must not delete the diagnostic it exists to protect: truncation has to
  // stay visible, and a secret must never survive as an unmatchable fragment.
  // ---------------------------------------------------------------------------
  describe("over-long input is bounded without losing the signal", () => {
    it("announces the truncation in band rather than silently shortening", () => {
      const out = scrubText("A".repeat(MAX_SCRUB_CHARS + 5_000));
      expect(out).toContain("[relayhistory: truncated");
      expect(out).toContain("characters over the");
      expect(out.length).toBeLessThan(MAX_SCRUB_CHARS + 500);
    });

    // A cut by UTF-16 index can land between the halves of one character. The result is
    // not merely an odd-looking prefix: PostgreSQL refuses to convert a lone surrogate
    // to jsonb, so a severed pair fails the delivery of an input that was well formed.
    it("never cuts a surrogate pair in half", () => {
      // No whitespace anywhere, so the rewind exhausts and the cut lands at the floor,
      // with one emoji sitting exactly across it.
      const floor = MAX_SCRUB_CHARS - 62_144;
      const out = scrubText(
        "a".repeat(floor - 1) + "\u{1F600}" + "a".repeat(MAX_SCRUB_CHARS),
      );
      expect(out).toContain("[relayhistory: truncated");
      expect(out).not.toMatch(
        /[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/,
      );
      expect(out.startsWith("a".repeat(floor - 1))).toBe(true);
    });

    // The step-back is for pairs only. A surrogate the caller sent already unpaired must
    // come through, so whoever validates the scrubbed text rejects it wherever it sits
    // rather than at every offset except this one.
    it("does not swallow a surrogate that was already unpaired at the cut", () => {
      const floor = MAX_SCRUB_CHARS - 62_144;
      const out = scrubText(
        "a".repeat(floor - 1) + "\ud800" + "a".repeat(MAX_SCRUB_CHARS),
      );
      expect(out).toMatch(/[\uD800-\uDBFF](?![\uDC00-\uDFFF])/);
    });

    it("leaves input at or under the limit completely untouched", () => {
      const exact = "hello world ".repeat(100);
      expect(scrubText(exact)).toBe(exact);
      expect(scrubText("A".repeat(MAX_SCRUB_CHARS))).toBe(
        "A".repeat(MAX_SCRUB_CHARS),
      );
    });

    it("still redacts secrets that sit inside the retained prefix", () => {
      const secret = "ghp_123456789012345678901234567890123456";
      const out = scrubText(
        `${secret} ${"A".repeat(MAX_SCRUB_CHARS + 10_000)}`,
      );
      expect(out).toContain("[REDACTED]");
      expect(out).not.toContain(secret);
    });

    // The failure mode a naive cut would introduce: a credential straddling the cut
    // point leaves a prefix too short for any pattern to match, so it is stored raw.
    it("never splits a secret across the cut and leaves a raw fragment", () => {
      const secret = "ghp_abcdefghijklmnopqrstuvwxyz0123456789";
      // Whitespace-separated filler, so the cut has a boundary to rewind to. Place the
      // secret so it straddles MAX_SCRUB_CHARS.
      const filler = "word ".repeat(Math.ceil(MAX_SCRUB_CHARS / 5));
      const input = `${filler.slice(0, MAX_SCRUB_CHARS - 10)}${secret} tail`;
      const out = scrubText(input);
      // Either wholly redacted or wholly dropped — never a raw prefix of the token.
      expect(out).not.toMatch(/ghp_abcdefghij/);
    });
  });

  describe("private key blocks", () => {
    const BODY = "MIIEowIBAAKCAQEAyZ1examplekeymaterial";

    it("redacts a complete block", () => {
      expect(
        scrubText(
          `before -----BEGIN PRIVATE KEY-----\n${BODY}\n-----END PRIVATE KEY----- after`,
        ),
      ).toBe("before [REDACTED] after");
    });

    it("redacts a block with no END to the end of the text", () => {
      expect(
        scrubText(`pasted -----BEGIN RSA PRIVATE KEY-----\n${BODY}\nmore`),
      ).toBe("pasted [REDACTED]");
    });

    it("stops an unterminated block at the next BEGIN", () => {
      const out = scrubText(
        `-----BEGIN PRIVATE KEY-----\n${BODY}\n-----BEGIN CERTIFICATE-----\npublic`,
      );
      expect(out).toBe("[REDACTED]-----BEGIN CERTIFICATE-----\npublic");
    });

    it("redacts a block the truncation cut and keeps the truncation note", () => {
      const filler = "word ".repeat(Math.ceil(MAX_SCRUB_CHARS / 5));
      const key = `-----BEGIN PRIVATE KEY-----\n${`${BODY}\n`.repeat(400)}-----END PRIVATE KEY-----`;
      const out = scrubText(
        `${filler.slice(0, MAX_SCRUB_CHARS - 1_000)}${key}`,
      );
      expect(out).not.toContain(BODY);
      expect(out).toMatch(
        /\[REDACTED\]\n\[relayhistory: truncated \d+ characters/,
      );
    });

    it("scrubs a scrub-limit run of unterminated headers in linear time", () => {
      const header = "-----BEGIN PRIVATE KEY----- ";
      const input = header
        .repeat(Math.ceil(MAX_SCRUB_CHARS / header.length))
        .slice(0, MAX_SCRUB_CHARS);
      const samples: number[] = [];
      for (let run = 0; run < 3; run += 1) {
        const started = performance.now();
        scrubText(input);
        samples.push(performance.now() - started);
      }
      // Measured 4-25 ms per pass linear; 230-1,700 ms per pass (warm and cold) before
      // the BEGIN bound.
      expect(samples.sort((a, b) => a - b)[1]).toBeLessThan(100);
    });
  });

  describe("assignment, bearer and home-path edges", () => {
    it("redacts the whole quoted value of an assignment, spaces included", () => {
      expect(scrubText('PASSWORD="correct horse battery staple" next')).toBe(
        "PASSWORD=[REDACTED] next",
      );
      expect(scrubText("api_key: 'two words', other")).toBe(
        "api_key:[REDACTED], other",
      );
      // An unterminated quote runs to the end of its line.
      expect(scrubText('token="open quote\nnext line')).toBe(
        "token=[REDACTED]\nnext line",
      );
      expect(scrubText('PASSWORD=""')).toBe('PASSWORD=""');
    });

    it("redacts a bearer value ending in base64 padding", () => {
      expect(scrubText("Authorization: Bearer abcdefghijklmno=")).toBe(
        "Authorization: Bearer [REDACTED]",
      );
      expect(scrubText("Bearer abcdefghijklmnopqrst== next")).toBe(
        "Bearer [REDACTED] next",
      );
    });

    it("redacts an Authorization header's credential and keeps its scheme", () => {
      expect(scrubText("Authorization: Basic dXNlcjpwYXNz")).toBe(
        "Authorization: Basic [REDACTED]",
      );
      expect(
        scrubText(
          'GET / HTTP/1.1\nProxy-Authorization: Digest username="u", response="abc"\nHost: x',
        ),
      ).toBe("GET / HTTP/1.1\nProxy-Authorization: Digest [REDACTED]\nHost: x");
      expect(scrubText(`curl -H 'authorization: token ghx123' url`)).toBe(
        "curl -H 'authorization: token [REDACTED]' url",
      );
      expect(scrubText('{"Authorization": "Basic dXNlcg==", "a": 1}')).toBe(
        '{"Authorization": "Basic [REDACTED]", "a": 1}',
      );
      // A credential with no scheme is redacted whole.
      expect(scrubText("Authorization: dXNlcjpwYXNz")).toBe(
        "Authorization: [REDACTED]",
      );
      // Prose that mentions the header is not a header line.
      const prose = "Send the Authorization: header with each call";
      expect(scrubText(prose)).toBe(prose);
    });

    it("redacts every Digest parameter, however the header is quoted", () => {
      const unescaped = scrubText(
        '-H "Authorization: Digest username="alice", realm="svc", response=deadbeef1234" -X POST http://x',
      );
      expect(unescaped).not.toContain("deadbeef1234");
      expect(unescaped).toBe(
        '-H "Authorization: Digest [REDACTED]" -X POST http://x',
      );
      expect(
        scrubText(
          '{"Authorization": "Digest username=\\"u\\", response=\\"abc\\"", "a": 1}',
        ),
      ).toBe('{"Authorization": "Digest [REDACTED]", "a": 1}');
      expect(
        scrubText(
          "curl -H 'Authorization: Digest username=\"u\", response=abc' url",
        ),
      ).toBe("curl -H 'Authorization: Digest [REDACTED]' url");
      // AWS Signature V4: parameter values are not RFC tokens.
      const sigv4 = scrubText(
        "Authorization: AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20261004/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7",
      );
      expect(sigv4).toBe("Authorization: AWS4-HMAC-SHA256 [REDACTED]");
      expect(sigv4).not.toContain("AKIDEXAMPLE");
      expect(sigv4).not.toContain("5d672d79");
      expect(
        scrubText(
          '{"Authorization": "AWS4-HMAC-SHA256 Credential=AKID/x, Signature=abc", "a": 1}',
        ),
      ).toBe('{"Authorization": "AWS4-HMAC-SHA256 [REDACTED]", "a": 1}');
      // An item after a comma that is not a parameter still ends no earlier than a
      // parameter inside it.
      const tail = scrubText("Authorization: Digest a=b, stray d=secret\nnext");
      expect(tail).not.toContain("secret");
      expect(tail).toBe("Authorization: Digest [REDACTED]\nnext");
      // Parameters with no scheme, spaced around `=`.
      expect(scrubText('Authorization: realm = "svc", nonce=abc next')).toBe(
        "Authorization: [REDACTED] next",
      );
    });

    it("redacts a Digest header serialized inside JSON, once or twice", () => {
      const R = "deadbeef1234";
      const command = `curl -H "Authorization: Digest username=\\"alice\\", response=${R}" http://x`;
      const once = scrubText(JSON.stringify({ command }));
      expect(once).not.toContain(R);
      expect(once).toBe(
        JSON.stringify({
          command: 'curl -H "Authorization: Digest [REDACTED]" http://x',
        }),
      );
      const twice = scrubText(JSON.stringify(JSON.stringify({ command })));
      expect(twice).not.toContain(R);
      expect(twice).toBe(
        JSON.stringify(
          JSON.stringify({
            command: 'curl -H "Authorization: Digest [REDACTED]" http://x',
          }),
        ),
      );
      // A Python repr quotes the header with `'` and leaves the inner `"` bare.
      expect(
        scrubText(
          `{'Authorization': 'Digest username="u", response="${R}"', 'x': 1}`,
        ),
      ).toBe("{'Authorization': 'Digest [REDACTED]', 'x': 1}");
    });

    it("decodes escaped backslashes before a Digest value's closing quote", () => {
      const S = "SECRETxyz";
      // An escaped backslash ends the value; the next parameters stay in the list.
      expect(
        scrubText(
          `Authorization: Digest username="foo\\\\", realm="x", response=${S} next`,
        ),
      ).toBe("Authorization: Digest [REDACTED] next");
      expect(scrubText('Authorization: Digest a="foo\\\\", b=bar next')).toBe(
        "Authorization: Digest [REDACTED] next",
      );
      const shell = `curl -H "Authorization: Digest username=\\"foo\\\\\\\\\\", realm=\\"x\\", response=${S}" http://x`;
      const redacted = 'curl -H "Authorization: Digest [REDACTED]" http://x';
      expect(scrubText(shell)).toBe(redacted);
      expect(scrubText(JSON.stringify({ command: shell }))).toBe(
        JSON.stringify({ command: redacted }),
      );
      expect(
        scrubText(
          JSON.stringify({
            Authorization: `Digest username="foo\\\\", realm="x", response=${S}`,
          }),
        ),
      ).toBe(JSON.stringify({ Authorization: "Digest [REDACTED]" }));
      expect(
        scrubText(
          JSON.stringify({
            text: `Authorization: Digest a="foo\\\\", b=${S} next`,
          }),
        ),
      ).toBe(JSON.stringify({ text: "Authorization: Digest [REDACTED] next" }));
    });

    it("never takes a JSON header value's closing quote for a parameter value", () => {
      expect(scrubText('{"Authorization":"Digest a=","response":"kept"}')).toBe(
        '{"Authorization":"Digest [REDACTED]","response":"kept"}',
      );
      expect(scrubText('{"Authorization":"Digest a=b","next":"keep"}')).toBe(
        '{"Authorization":"Digest [REDACTED]","next":"keep"}',
      );
      expect(
        scrubText(
          '{"Authorization":"Digest username=\\"u\\", response=\\"S\\"","x":"keep"}',
        ),
      ).toBe('{"Authorization":"Digest [REDACTED]","x":"keep"}');
      // An escaped quote ending a bare token belongs to the enclosing header.
      expect(
        scrubText(
          '-H "Authorization: Bearer abcdefghijklmnopqrstuv\\" -X POST',
        ),
      ).toBe('-H "Authorization: Bearer [REDACTED]\\" -X POST');
    });

    it("finds a header serialized into JSON more than once", () => {
      const S = "s3cr3tTOKENvalue";
      for (const value of [
        `Basic ${S}`,
        `Token ${S}`,
        `Digest username="u", response="${S}"`,
        `AWS4-HMAC-SHA256 Credential=AKID/${S}/s3/aws4_request, Signature=${S}`,
        S,
      ]) {
        let serialized = JSON.stringify({ authorization: value, next: "kept" });
        for (let level = 1; level <= 3; level += 1) {
          const out = scrubText(serialized);
          expect(out, `${value} at level ${level}`).not.toContain(S);
          expect(out).toContain("kept");
          serialized = JSON.stringify(serialized);
        }
      }
    });

    it("treats an escaped line break as the end of a header line", () => {
      const S = "s3cr3tTOKENvalue";
      const request = `GET / HTTP/1.1\r\nAuthorization: Basic ${S}\r\nHost: x\r\n`;
      expect(scrubText(JSON.stringify({ req: request }))).toBe(
        JSON.stringify({
          req: "GET / HTTP/1.1\r\nAuthorization: Basic [REDACTED]\r\nHost: x\r\n",
        }),
      );
      const twice = scrubText(JSON.stringify(JSON.stringify({ req: request })));
      expect(twice).not.toContain(S);
      expect(twice).toContain("Host: x");
    });

    it("ends a serialized header line before a next header with any field-name characters", () => {
      for (const name of [
        "X_Trace_Id",
        "x.request.id",
        "X-Custom!#$%&'*+^`|~",
      ]) {
        for (const lineBreak of ["\n", "\r\n"]) {
          // The header follows an escaped line break, so the text is a serialized message.
          const encoded = JSON.stringify({
            req: `GET / HTTP/1.1${lineBreak}Authorization: Basic c2VjcmV0dG9rZW4=${lineBreak}${name}: 123`,
          });
          const scrubbed = scrubText(encoded);
          expect(scrubbed).not.toContain("c2VjcmV0dG9rZW4");
          expect(scrubbed).toContain(`${name}: 123`);
        }
        // The same characters written literally (a backslash, then n) stay credential.
        const literal = scrubText(
          `curl -H 'Authorization: Basic c2VjcmV0\\n${name}: 123' url`,
        );
        expect(literal).not.toContain("c2VjcmV0");
        expect(literal).toContain("' url");
      }
    });

    it("keeps a backslashed Windows account inside the credential wherever serialization is unproven", () => {
      const password = "hunter2PASS";
      const shell = (credential: string) =>
        `curl -H "Authorization: ${credential}" url`;
      for (const credential of [
        `Basic DOMAIN\\ryan:${password}`,
        `NTLM CORP\\nancy:${password}`,
        `DOMAIN\\ryan:${password}`,
      ]) {
        for (const text of [
          shell(credential),
          JSON.stringify({ c: shell(credential) }),
          JSON.stringify(JSON.stringify({ c: shell(credential) })),
        ]) {
          const scrubbed = scrubText(text);
          expect(scrubbed).not.toContain(password);
          expect(scrubbed).toContain(" url");
        }
      }
      // Fail-closed trade-off: a header at the start of a quoted string cannot prove its
      // escapes encode line breaks, so the following header is redacted with it.
      expect(
        scrubText(
          JSON.stringify({
            req: "Authorization: Basic abc\r\nX_Trace_Id: 123",
          }),
        ),
      ).toBe('{"req":"Authorization: Basic [REDACTED]"}');
    });

    it("tells a literal backslash-n or -r in a credential from an encoded line break", () => {
      const S = "s3cr3tRESP";
      const literal = `Digest username="DOMAIN\\ryan", realm="r", response=${S}`;
      const pairs: Array<[string, string, string]> = [
        // [literal shape, encoded-line-break counterpart, text that must survive]
        [
          `Authorization: ${literal} tail`,
          `Authorization: Basic ${S}\nHost: x`,
          " tail|Host: x",
        ],
        [
          `Authorization: Digest username=DOMAIN\\ryan, response=${S} tail`,
          `Authorization: Basic ${S}\r\nHost: x`,
          " tail|Host: x",
        ],
        [
          `Authorization: Digest uri="C:\\repo\\new", response=${S} tail`,
          JSON.stringify({
            req: `GET /\r\nAuthorization: Basic ${S}\r\nHost: x`,
          }),
          " tail|Host: x",
        ],
        [
          `Authorization: Token abc\\ndef${S} tail`,
          JSON.stringify({
            req: `GET / HTTP/1.1\r\nAuthorization: Basic ${S}\r\nHost: x\r\n`,
          }),
          " tail|Host: x",
        ],
        [
          `curl -H "Authorization: ${literal.replace(/"/g, '\\"')}" url`,
          JSON.stringify({
            req: `GET /\r\nAuthorization: ${literal}\r\nHost: x`,
          }),
          " url|Host: x",
        ],
        [
          `curl -H "Authorization: Digest username=DOMAIN\\ryan, response=${S}" url`,
          JSON.stringify(
            JSON.stringify({
              req: `GET /\r\nAuthorization: Basic ${S}\r\nHost: x`,
            }),
          ),
          " url|Host: x",
        ],
        // A JSON-encoded newline inside a `"`-quoted header followed by free text is
        // indistinguishable from a literal one, so it is redacted up to the header's
        // close; the JSON after it stays intact.
        [
          JSON.stringify({ authorization: literal, k: "kept" }),
          JSON.stringify({ t: `Authorization: ${literal}\nnext`, k: "kept" }),
          'kept|[REDACTED]","k":"kept"}',
        ],
        [
          JSON.stringify(JSON.stringify({ authorization: literal, k: "kept" })),
          JSON.stringify(
            JSON.stringify({ t: `Authorization: ${literal}\nnext`, k: "kept" }),
          ),
          'kept|[REDACTED]\\",\\"k\\":\\"kept\\"}"',
        ],
      ];
      // A literal `\n` in a shell-quoted header's last parameter is not a line end.
      for (const header of [
        `Digest username=u, response=abc\\n${S}`,
        `AWS4-HMAC-SHA256 Credential=K/x, Signature=ab\\n${S}`,
      ]) {
        const shell = scrubText(`curl -H "Authorization: ${header}" url`);
        expect(shell).not.toContain(S);
        expect(shell).toContain('[REDACTED]" url');
      }
      for (const [literalShape, encoded, kept] of pairs) {
        const [literalKept, encodedKept] = kept.split("|") as [string, string];
        const fromLiteral = scrubText(literalShape);
        expect(fromLiteral, literalShape).not.toContain(S);
        expect(fromLiteral, literalShape).toContain(literalKept);
        const fromEncoded = scrubText(encoded);
        expect(fromEncoded, encoded).not.toContain(S);
        expect(fromEncoded, encoded).toContain(encodedKept);
      }
    });

    it("decodes quoted Digest values in a header line of a serialized message", () => {
      const R = "6629fae49393a05397450978507c4ef1";
      for (const realm of ["Restricted Area", "a, b", "r"]) {
        const message = {
          req: `GET / HTTP/1.1\r\nAuthorization: Digest username="u", realm="${realm}", nonce="n1", response="${R}"\r\nHost: x`,
        };
        for (const serialized of [
          JSON.stringify(message),
          JSON.stringify(JSON.stringify(message)),
        ]) {
          const out = scrubText(serialized);
          expect(out, serialized).not.toContain(R);
          expect(out, serialized).toContain("Host: x");
        }
      }
    });

    it("decodes a header line of a single-quoted serialized message", () => {
      const R = "6629fae49393a05397450978507c4ef1";
      // Python's repr and shell's $'…' escape backslashes, ' and line breaks, not ".
      const repr = (text: string) =>
        "'" +
        text
          .replace(/\\/g, "\\\\")
          .replace(/'/g, "\\'")
          .replace(/\r/g, "\\r")
          .replace(/\n/g, "\\n") +
        "'";
      for (const realm of ["r", "Restricted Area", "a, b"]) {
        const message = `GET / HTTP/1.1\r\nAuthorization: Digest username="u", realm="${realm}", response="${R}"\r\nHost: x`;
        for (const serialized of [
          repr(message),
          `{'req': ${repr(message)}, 'k': 1}`,
          `printf $${repr(message)}`,
          JSON.stringify({ req: message }),
        ]) {
          const out = scrubText(serialized);
          expect(out, serialized).not.toContain(R);
          expect(out, serialized).toContain("Host: x");
        }
      }
    });

    it("redacts a serialized header line through every nested layer", () => {
      const R = "6629fae49393a05397450978507c4ef1";
      const message = `GET / HTTP/1.1\r\nAuthorization: Digest username="o'brien", realm="a, b", response="${R}"\r\nHost: x`;
      const repr = (text: string) =>
        "'" +
        text
          .replace(/\\/g, "\\\\")
          .replace(/'/g, "\\'")
          .replace(/\r/g, "\\r")
          .replace(/\n/g, "\\n") +
        "'";
      for (const serialized of [
        JSON.stringify({ log: repr(message) }),
        `[${repr(JSON.stringify({ req: message }))}]`,
        JSON.stringify(JSON.stringify({ log: repr(message) })),
      ]) {
        const out = scrubText(serialized);
        expect(out, serialized).not.toContain(R);
        expect(out, serialized).toContain("Host: x");
      }
    });

    it("ends a serialized header line only at a string's structural end, whatever quotes precede it", () => {
      const R = "6629fae49393a05397450978507c4ef1";
      const repr = (text: string) =>
        "'" +
        text
          .replace(/\\/g, "\\\\")
          .replace(/'/g, "\\'")
          .replace(/\r/g, "\\r")
          .replace(/\n/g, "\\n") +
        "'";
      for (const message of [
        `GET /o'reilly HTTP/1.1\r\nAuthorization: Digest username="o'brien", realm="a, b", response="${R}"\r\nHost: x`,
        `GET / HTTP/1.1\r\nX-Note: it's fine\r\nAuthorization: Digest username="o'brien", response="${R}"\r\nHost: x`,
        `GET / HTTP/1.1\r\nX-Q: say "hi"\r\nAuthorization: Digest username="u", realm="a, b", response="${R}"\r\nHost: x`,
      ]) {
        for (const serialized of [
          JSON.stringify({ req: message }),
          JSON.stringify(JSON.stringify({ req: message })),
          repr(message),
          JSON.stringify({ log: repr(message) }),
        ]) {
          const out = scrubText(serialized);
          expect(out, serialized).not.toContain(R);
          expect(out, serialized).toContain("Host: x");
        }
      }
      // With no line after it, the header ends at its string's close.
      const last = scrubText(
        JSON.stringify({
          req: `GET /\r\nAuthorization: Digest username="u", response="${R}"`,
        }),
      );
      expect(last).not.toContain(R);
      expect(last.endsWith('"}')).toBe(true);
    });

    it("keeps quoted fields before a serialized header and the JSON siblings after it", () => {
      const R = "6629fae49393a05397450978507c4ef1";
      for (const field of [
        'If-None-Match: "abc"',
        'Cookie: a="b c"; d="e"',
        'Content-Disposition: attachment; filename="x.txt"',
      ]) {
        const repr = String.raw`'GET / HTTP/1.1\r\n${field}\r\nAuthorization: Digest username="u", realm="r", response="${R}"\r\nHost: x'`;
        expect(scrubText(repr)).toBe(
          String.raw`'GET / HTTP/1.1\r\n${field}\r\nAuthorization: Digest [REDACTED]\r\nHost: x'`,
        );
      }
      const dump = scrubText(
        JSON.stringify(
          `Here's the dump: GET /\r\nAuthorization: Digest username="o'brien", realm="A B", response="${R}"\r\nHost: x`,
        ),
      );
      expect(dump).not.toContain(R);
      expect(dump).toContain("Host: x");
      expect(
        scrubText(
          String.raw`{"msg":"Don't forget\nAuthorization: Basic abc","level":"info","more":"x"}`,
        ),
      ).toBe(
        String.raw`{"msg":"Don't forget\nAuthorization: Basic [REDACTED]","level":"info","more":"x"}`,
      );
      // A value quote followed by `, response=` continues the list.
      expect(
        scrubText(
          JSON.stringify({
            r: `GET /\r\nAuthorization: Digest username="u", response="${R}"\r\nHost: x`,
          }),
        ),
      ).toBe(
        JSON.stringify({
          r: "GET /\r\nAuthorization: Digest [REDACTED]\r\nHost: x",
        }),
      );
    });

    it("reads a string end after a serialized header from what follows the quote", () => {
      const S = "R9secret";
      // `;` and spaces separate parameters too, so the list continues past them.
      for (const repr of [
        String.raw`'GET /\r\nAuthorization: Digest username="u"; realm="r"; response="${S}"\r\nHost: x'`,
        String.raw`'GET /\r\nAuthorization: Digest username="u" realm="r" response="${S}"\r\nHost: x'`,
        String.raw`'GET /\r\nAuthorization: Digest username="u" , response="${S}"\r\nHost: x'`,
      ]) {
        const out = scrubText(repr);
        expect(out, repr).not.toContain(S);
        expect(out, repr).toContain("Host: x");
      }
      expect(
        scrubText(
          JSON.stringify({
            r: `GET /\r\nAuthorization: Digest username="u"; realm="r"; response="${S}"\r\nHost: x`,
          }),
        ),
      ).not.toContain(S);
      // A quote followed by a shell argument, a line end or prose ends the string.
      expect(
        scrubText(
          String.raw`curl -H 'GET /\r\nAuthorization: Basic ${S}' https://example.com`,
        ),
      ).toBe(
        String.raw`curl -H 'GET /\r\nAuthorization: Basic [REDACTED]' https://example.com`,
      );
      expect(
        scrubText(
          JSON.stringify(
            { req: `GET /\r\nAuthorization: Basic ${S}` },
            null,
            2,
          ),
        ),
      ).toBe(
        JSON.stringify(
          { req: "GET /\r\nAuthorization: Basic [REDACTED]" },
          null,
          2,
        ),
      );
      expect(
        scrubText(
          String.raw`log: 'GET /\r\nAuthorization: Basic ${S}' and then more`,
        ),
      ).toBe(
        String.raw`log: 'GET /\r\nAuthorization: Basic [REDACTED]' and then more`,
      );
      expect(scrubText('{"Authorization":"Digest a=","response":"S"}')).toBe(
        '{"Authorization":"Digest [REDACTED]","response":"S"}',
      );
    });

    it("redacts every Digest value of a serialized header however its values are spelled", () => {
      const S = "R9secretZ";
      const repr = (text: string) =>
        "'" +
        text
          .replace(/\\/g, "\\\\")
          .replace(/'/g, "\\'")
          .replace(/\r/g, "\\r")
          .replace(/\n/g, "\\n") +
        "'";
      const layers = [
        repr,
        (text: string) => JSON.stringify({ r: text }),
        (text: string) => JSON.stringify({ l: repr(text) }),
      ];
      for (const layer of layers)
        for (const separator of [", ", "; ", " ", ",", " , "])
          for (const prefix of ["", " ", ",", ";", "!x "])
            for (const position of [1, 2]) {
              const values = ["u", "r", "n1"].map(
                (value, index) => `${prefix}${index === position ? S : value}`,
              );
              const header = [
                `username="${values[0]}"`,
                `realm="${values[1]}"`,
                `response="${values[2]}"`,
              ].join(separator);
              const serialized = layer(
                `GET / HTTP/1.1\r\nAuthorization: Digest ${header}\r\nHost: x`,
              );
              const out = scrubText(serialized);
              expect(out, serialized).not.toContain(S);
              expect(out, serialized).toContain("Host: x");
            }
      // Controls: what follows a serialized header's string is kept.
      expect(
        scrubText(
          String.raw`curl -H 'GET /\r\nAuthorization: Basic ${S}' https://example.com`,
        ),
      ).toBe(
        String.raw`curl -H 'GET /\r\nAuthorization: Basic [REDACTED]' https://example.com`,
      );
      expect(
        scrubText(
          JSON.stringify(
            { req: `GET /\r\nAuthorization: Basic ${S}` },
            null,
            2,
          ),
        ),
      ).toBe(
        JSON.stringify(
          { req: "GET /\r\nAuthorization: Basic [REDACTED]" },
          null,
          2,
        ),
      );
      expect(
        scrubText(
          String.raw`log: 'GET /\r\nAuthorization: Basic ${S}' and then more`,
        ),
      ).toBe(
        String.raw`log: 'GET /\r\nAuthorization: Basic [REDACTED]' and then more`,
      );
      expect(
        scrubText(
          String.raw`{"msg":"Don't forget\nAuthorization: Basic abc","level":"info","more":"x"}`,
        ),
      ).toBe(
        String.raw`{"msg":"Don't forget\nAuthorization: Basic [REDACTED]","level":"info","more":"x"}`,
      );
      expect(scrubText('{"Authorization":"Digest a=","response":"S"}')).toBe(
        '{"Authorization":"Digest [REDACTED]","response":"S"}',
      );
    });

    it("continues a Digest list past `;` or spaces only when another parameter follows", () => {
      const S = "R9secretZ";
      const cases: Array<[string, string]> = [
        [
          `Authorization: Digest username="u"; realm="r"; response="${S}"\nHost: x`,
          "Host: x",
        ],
        [
          `Authorization: Digest username="u" realm="r" response="${S}"\nHost: x`,
          "Host: x",
        ],
        [
          `curl -H 'Authorization: Digest username="u"; response="${S}"' url`,
          "' url",
        ],
        [
          JSON.stringify({
            Authorization: `Digest username="u"; response="${S}"`,
            k: 1,
          }),
          '"k":1',
        ],
        // Comma lists already continued; unchanged.
        [
          `Authorization: Digest username="u", realm="r", response="${S}"\nHost: x`,
          "Host: x",
        ],
      ];
      for (const [input, kept] of cases) {
        const out = scrubText(input);
        expect(out, input).not.toContain(S);
        expect(out, input).toContain(kept);
      }
      expect(scrubText('Authorization: Digest a="b" and then more')).toBe(
        "Authorization: Digest [REDACTED] and then more",
      );
      expect(scrubText('{"Authorization":"Digest a=","response":"S"}')).toBe(
        '{"Authorization":"Digest [REDACTED]","response":"S"}',
      );
    });

    it("keeps serialized evidence around padded, apostrophe-bearing and loose credentials", () => {
      const R = "R9secretZ";
      const rows: Array<[string, string]> = [
        [
          String.raw`{"msg":"Don't forget\nAuthorization: Basic abc${R}","level":"info","more":"it's x"}`,
          `"level":"info","more":"it's x"}`,
        ],
        [
          JSON.stringify({
            t: `Here's the dump: GET /\r\nAuthorization: Digest username="u", realm="Admins' area", response="${R}"\r\nHost: x`,
          }),
          "Host: x",
        ],
        [
          JSON.stringify({
            req: `Here's the dump: GET /\r\nAuthorization: Digest username="u", realm="Members' Area", response="${R}"\r\nHost: x`,
          }),
          "Host: x",
        ],
        [
          JSON.stringify({
            req: `Here's: GET /\r\nAuthorization: Digest username="u", realm="Admins' (internal)", response="${R}"\r\nHost: x`,
          }),
          "Host: x",
        ],
        [
          String.raw`{"msg":"GET /\r\nAuthorization: Basic dXNlcjpwYQ${R}==","level":"info"}`,
          `"level":"info"}`,
        ],
        [
          String.raw`{"msg":"GET /\r\nAuthorization: Basic dXNlcjpw${R}=","level":"info"}`,
          `"level":"info"}`,
        ],
        [
          String.raw`{"msg":"GET /\r\nAuthorization: Negotiate YII${R}=="}`,
          `"}`,
        ],
        [String.raw`'GET /\r\nAuthorization: NTLM TlRM${R}==' next`, "' next"],
        [
          `curl -H "Authorization: Digest username="u"; realm="r"; response="${R}"" url`,
          "curl -H",
        ],
        [
          `curl -H "Authorization: Digest username="u" realm="r" response="${R}"" url`,
          "curl -H",
        ],
        [
          String.raw`'GET /\r\nAuthorization: Basic $(echo -n admin:${R} | base64)' x`,
          "' x",
        ],
      ];
      for (const [input, kept] of rows) {
        const out = scrubText(input);
        expect(out, input).not.toContain(R);
        expect(out, input).toContain(kept);
      }
      for (const sibling of ['"more":"x"', '"note":"retry x=1"'])
        expect(
          scrubText(
            String.raw`{"msg":"Don't forget\nAuthorization: Basic abc","level":"info",${sibling}}`,
          ),
        ).toBe(
          String.raw`{"msg":"Don't forget\nAuthorization: Basic [REDACTED]","level":"info",${sibling}}`,
        );
      // A stray apostrophe before the header, and one inside a value before the secret.
      for (const inner of ["' ", "'(", "':"])
        for (const layer of [
          (text: string) => JSON.stringify({ req: text }),
          (text: string) => JSON.stringify(JSON.stringify({ req: text })),
        ]) {
          const serialized = layer(
            `Here's it: GET /\r\nAuthorization: Digest username="u", realm="Admins${inner}x", response="${R}"\r\nHost: x`,
          );
          const out = scrubText(serialized);
          expect(out, serialized).not.toContain(R);
          expect(out, serialized).toContain("Host: x");
        }
      // Padded base64 credentials, with and without what follows their string.
      for (const scheme of ["Basic", "NTLM", "Negotiate"])
        for (const padding of ["=", "=="]) {
          const credential = `${scheme} dXNl${R}${padding}`;
          for (const [input, kept] of [
            [
              String.raw`{"msg":"GET /\r\nAuthorization: ${credential}","level":"info"}`,
              `"level":"info"}`,
            ],
            [String.raw`{"msg":"GET /\r\nAuthorization: ${credential}"}`, `"}`],
            [
              String.raw`'GET /\r\nAuthorization: ${credential}' next`,
              "' next",
            ],
            [
              String.raw`'GET /\r\nAuthorization: ${credential}'`,
              "[REDACTED]'",
            ],
          ] as const) {
            const out = scrubText(input);
            expect(out, input).not.toContain(R);
            expect(out, input).toContain(kept);
          }
        }
    });

    it("continues a Digest list past an empty or `=`-only value", () => {
      const R = "R9secretZ";
      const repr = (text: string) =>
        "'" +
        text
          .replace(/\\/g, "\\\\")
          .replace(/'/g, "\\'")
          .replace(/\r/g, "\\r")
          .replace(/\n/g, "\\n") +
        "'";
      for (const header of [
        `Digest a==, response="${R}"`,
        `Digest username==; response="${R}"`,
        `Digest realm=, response="${R}"`,
        `Digest nonce="", response="${R}"`,
        `Digest username= , response="${R}"`,
        `Basic ${R}==, x`,
      ])
        for (const input of [
          `Authorization: ${header}\nHost: x`,
          JSON.stringify({ r: `GET /\r\nAuthorization: ${header}\r\nHost: x` }),
          repr(`GET /\r\nAuthorization: ${header}\r\nHost: x`),
          `curl -H 'Authorization: ${header}' url`,
        ]) {
          const out = scrubText(input);
          expect(out, input).not.toContain(R);
        }
    });

    it("fails closed when a scheme's credential starts with a quote", () => {
      const R = "R9secretZ";
      const repr = (text: string) =>
        "'" +
        text
          .replace(/\\/g, "\\\\")
          .replace(/'/g, "\\'")
          .replace(/\r/g, "\\r")
          .replace(/\n/g, "\\n") +
        "'";
      const cases: Array<[string, string]> = [
        [
          `Authorization: Digest "quoted", response="${R}"\nHost: x`,
          "\nHost: x",
        ],
        [
          `curl -H 'Authorization: Digest "quoted", response="${R}"' url`,
          "' url",
        ],
        [`Authorization: Basic "${R}"`, "Authorization: Basic [REDACTED]"],
        [
          repr(
            `it"s: GET /\r\nAuthorization: Digest "quoted", realm="r", response="${R}"\r\nHost: x`,
          ),
          "Host: x",
        ],
        [
          JSON.stringify({
            t: `Here's: GET /\r\nAuthorization: Digest "quoted", realm="r", response="${R}"\r\nHost: x`,
          }),
          "Host: x",
        ],
      ];
      for (const [input, kept] of cases) {
        const out = scrubText(input);
        expect(out, input).not.toContain(R);
        expect(out, input).toContain(kept);
      }
      expect(scrubText(`Authorization: "${R}"`)).toBe(
        'Authorization: "[REDACTED]"',
      );
    });

    it("redacts single parameters, shell substitutions and quote-first credentials whole", () => {
      const S = "R9secretZ";
      const repr = (text: string) =>
        "'" +
        text
          .replace(/\\/g, "\\\\")
          .replace(/'/g, "\\'")
          .replace(/\r/g, "\\r")
          .replace(/\n/g, "\\n") +
        "'";
      // Widened matrix: stray quotes before the header, one to three parameters, any
      // position, Digest and OAuth.
      const layers = [
        repr,
        (text: string) => JSON.stringify({ r: text }),
        (text: string) => JSON.stringify({ l: repr(text) }),
      ];
      const names = ["response", "oauth_signature", "nonce"];
      let cases = 0;
      for (const layer of layers)
        for (const before of [
          "",
          'If-None-Match: "v1"\r\n',
          "X-Note: it's\r\n",
        ])
          for (const separator of [", ", " "])
            for (const prefix of ["", " ", ","])
              for (const count of [1, 2, 3])
                for (let position = 0; position < count; position += 1)
                  for (const scheme of ["Digest", "OAuth", "Token"]) {
                    const header = Array.from(
                      { length: count },
                      (_, index) =>
                        `${names[index]}="${prefix}${index === position ? S : `v${index}`}"`,
                    ).join(separator);
                    const serialized = layer(
                      `GET / HTTP/1.1\r\n${before}Authorization: ${scheme} ${header}\r\nHost: x`,
                    );
                    const out = scrubText(serialized);
                    expect(out, serialized).not.toContain(S);
                    expect(out, serialized).toContain("Host: x");
                    cases += 1;
                  }
      expect(cases).toBe(972);

      // Shell substitutions are redacted to their end, whatever they quote inside.
      const substitutions: Array<[string, string]> = [
        [
          JSON.stringify({
            cmd: "GET /\r\nAuthorization: Basic $(printf '%s' admin:hunter2 | base64)",
          }),
          '"}',
        ],
        [
          repr(
            'GET /\r\nAuthorization: Basic $(printf "%s:%s" admin hunter2 | base64)\r\nHost: x',
          ),
          "Host: x",
        ],
        [
          repr(
            "GET /\r\nAuthorization: Basic $(echo -n admin:hunter2 | base64)\r\nHost: x",
          ),
          "Host: x",
        ],
        [
          repr(
            "GET /\r\nAuthorization: Basic $(echo $(cat p) admin:hunter2 | base64)\r\nHost: x",
          ),
          "Host: x",
        ],
        [
          repr(
            'GET /\r\nAuthorization: Basic `printf "%s" admin:hunter2 | base64`\r\nHost: x',
          ),
          "Host: x",
        ],
        [
          `curl -H "Authorization: Basic $(printf '%s' admin:hunter2 | base64)" url`,
          '" url',
        ],
        [
          repr(
            "GET /\r\nAuthorization: Basic $(printf '%s' admin:hunter2\r\nHost: x",
          ),
          "Host: x",
        ],
      ];
      for (const [input, kept] of substitutions) {
        const out = scrubText(input);
        expect(out, input).not.toContain("hunter2");
        expect(out, input).toContain(kept);
      }

      // A quote-first credential in a quoted header ends at the header's close.
      expect(
        scrubText(`{"Authorization":"Basic \\"${S}\\"","level":"info"}`),
      ).toBe('{"Authorization":"Basic [REDACTED]","level":"info"}');
      expect(
        scrubText(
          JSON.stringify({ Authorization: 'Digest "quoted"', next: "kept" }),
        ),
      ).toBe(
        JSON.stringify({ Authorization: "Digest [REDACTED]", next: "kept" }),
      );
      expect(
        scrubText(`curl -H 'Authorization: Basic "${S}"' -X GET url`),
      ).toBe("curl -H 'Authorization: Basic [REDACTED]' -X GET url");
      // A list cut short by a quoted segment in a quoted header takes the header.
      expect(
        scrubText(
          `curl -H 'Authorization: Digest a=="x", response="${S}"' url`,
        ),
      ).toBe("curl -H 'Authorization: Digest [REDACTED]' url");
      // Controls.
      expect(
        scrubText(
          String.raw`{"msg":"GET /\r\nAuthorization: Basic dXNlcjpwYQ==","level":"info"}`,
        ),
      ).toBe(
        String.raw`{"msg":"GET /\r\nAuthorization: Basic [REDACTED]","level":"info"}`,
      );
    });

    it("ends an unclosed substitution in a quoted key's value at the value's close", () => {
      expect(scrubText('{"Authorization":"Basic $(echo","next":"keep"}')).toBe(
        '{"Authorization":"Basic [REDACTED]","next":"keep"}',
      );
      const rows: Array<[string, string]> = [
        [
          '{"Authorization":"Basic $(echo admin:hunter2","next":"keep"}',
          '"next":"keep"}',
        ],
        [
          "{'Authorization': 'Basic $(echo admin:hunter2', 'next': 'keep'}",
          "'next': 'keep'}",
        ],
        [
          `{"Authorization":"Basic $(printf '%s' admin:hunter2 | base64)","next":"keep"}`,
          '"next":"keep"}',
        ],
        [
          JSON.stringify({
            Authorization: 'Basic $(printf "%s" admin:hunter2 | base64)',
            next: "keep",
          }),
          '"next":"keep"}',
        ],
        [
          JSON.stringify({
            r: "GET /\r\nAuthorization: Basic $(echo admin:hunter2\r\nHost: x",
          }),
          "Host: x",
        ],
        // In a shell argument a same-kind quote may sit inside `$(…)`, so no quote is
        // a safe stop: still redacted to the line end.
        ["curl -H 'Authorization: Basic $(echo admin:hunter2' url", "curl -H"],
        [
          'curl -H "Authorization: Basic $(printf "%s" admin:hunter2" url',
          "curl -H",
        ],
      ];
      for (const [input, kept] of rows) {
        const out = scrubText(input);
        expect(out, input).not.toContain("hunter2");
        expect(out, input).toContain(kept);
      }
    });

    it("reads shell quoting and line breaks inside a substitution", () => {
      const S = "R9secretZ";
      const repr = (text: string) =>
        "'" +
        text
          .replace(/\\/g, "\\\\")
          .replace(/'/g, "\\'")
          .replace(/\r/g, "\\r")
          .replace(/\n/g, "\\n") +
        "'";
      const rows: Array<[string, string]> = [
        // A `)` inside a shell-quoted argument does not close `$(`.
        [
          JSON.stringify({
            cmd: `GET /\r\nAuthorization: Basic $(printf '%s' ')' admin:${S} | base64)`,
          }),
          '"}',
        ],
        [
          repr(
            `GET /\r\nAuthorization: Basic $(printf "%s" ")" admin:${S} | base64)\r\nHost: x`,
          ),
          "Host: x",
        ],
        [
          `curl -H "Authorization: Basic $(printf '%s' ')' admin:${S} | base64)" url`,
          '" url',
        ],
        [
          `curl -H 'Authorization: Basic $(printf "%s" ")" admin:${S} | base64)' url`,
          "' url",
        ],
        [
          JSON.stringify({
            Authorization: `Basic $(printf '%s' ')' admin:${S} | base64)`,
            next: "keep",
          }),
          '"next":"keep"}',
        ],
        // A substitution spanning lines, closed within the window.
        [
          `curl -H "Authorization: Basic $(printf 'admin:\n${S}' | base64)" url`,
          '" url',
        ],
        [
          `curl -H "Authorization: Basic $(printf '%s:%s' \\\n  admin ${S} | base64)" https://x\nnext line`,
          "next line",
        ],
        [
          JSON.stringify({
            cmd: `curl -H "Authorization: Basic $(printf 'admin:\n${S}' | base64)" url`,
          }),
          '\\" url',
        ],
        // Controls.
        [
          `curl -H "Authorization: Basic $(echo -n admin:${S} | base64)" url`,
          '" url',
        ],
        ['{"Authorization":"Basic $(echo","next":"keep"}', '"next":"keep"}'],
        [`curl -H 'Authorization: Basic $(echo admin:${S}' url`, "curl -H"],
      ];
      for (const [input, kept] of rows) {
        const out = scrubText(input);
        expect(out, input).not.toContain(S);
        expect(out, input).toContain(kept);
      }
      // Past the window, an unclosed multi-line substitution ends at its first line
      // break, as before.
      const far = `curl -H "Authorization: Basic $(printf 'admin:${S}\n${"x".repeat(5000)}' | base64)" url`;
      expect(scrubText(far)).toBe(
        `curl -H "Authorization: Basic [REDACTED]\n${"x".repeat(5000)}' | base64)" url`,
      );
    });

    it("ends a padded credential's serialized line at the structure that closes its string", () => {
      const S = "R9secretZ";
      const repr = (text: string) =>
        "'" +
        text
          .replace(/\\/g, "\\\\")
          .replace(/'/g, "\\'")
          .replace(/\r/g, "\\r")
          .replace(/\n/g, "\\n") +
        "'";
      const json = (value: unknown) => JSON.stringify(value);
      const rows: Array<[string, string]> = [
        [
          json({
            msg: `GET /\r\nAuthorization: ApiKey dXNl${S}Q==`,
            level: "info",
          }),
          '"level":"info"}',
        ],
        [
          json({
            msg: `GET /\r\nAuthorization: Token dXNl${S}Q==`,
            level: "info",
          }),
          '"level":"info"}',
        ],
        [
          json({
            msg: `GET /\r\nAuthorization: Token dXNl${S}Q=`,
            level: "info",
          }),
          '"level":"info"}',
        ],
        [
          json({ msg: `GET /\r\nAuthorization: dXNl${S}Q==`, level: "info" }),
          '"level":"info"}',
        ],
        [
          `{'msg': 'GET /\\r\\nAuthorization: Token dXNl${S}Q==', 'level': 'info'}`,
          "'level': 'info'}",
        ],
        [
          json(
            json({
              msg: `GET /\r\nAuthorization: Token dXNl${S}Q==`,
              level: "info",
            }),
          ),
          '\\"level\\":\\"info\\"}',
        ],
        [
          json({
            msg: `GET /\r\nAuthorization: Basic $(echo -n admin:${S} | base64`,
            level: "info",
          }),
          '"level":"info"}',
        ],
        // Controls.
        [
          json({
            msg: `GET /\r\nAuthorization: Basic dXNl${S}Q==`,
            level: "info",
          }),
          '"level":"info"}',
        ],
        [
          json({ Authorization: `Token dXNl${S}Q==`, level: "info" }),
          '"level":"info"}',
        ],
        [
          repr(
            `GET /\r\nIf-None-Match: "v1"\r\nAuthorization: Digest response=" ${S}"\r\nHost: x`,
          ),
          "Host: x",
        ],
        [
          repr(
            `GET /\r\nIf-None-Match: "v1"\r\nAuthorization: OAuth oauth_signature=" ${S}"\r\nHost: x`,
          ),
          "Host: x",
        ],
        [
          repr(
            `GET /\r\nIf-None-Match: "v1"\r\nAuthorization: Token response=" ${S}"\r\nHost: x`,
          ),
          "Host: x",
        ],
      ];
      for (const [input, kept] of rows) {
        const out = scrubText(input);
        expect(out, input).not.toContain(S);
        expect(out, input).toContain(kept);
      }
      expect(
        scrubText(json({ msg: "x\nAuthorization: Digest a=", k: "v", z: 1 })),
      ).toBe(
        json({ msg: "x\nAuthorization: Digest [REDACTED]", k: "v", z: 1 }),
      );
    });

    it("reads same-kind quotes inside a substitution as shell quoting", () => {
      const S = "R9secretZ";
      const rows: Array<[string, string]> = [
        [`{"Authorization": "Basic $(printf "%s" admin:${S} | base64)"}`, '"}'],
        [
          `curl -H "Authorization: Basic $(printf "%s" ")" admin:${S} | base64)" url`,
          '" url',
        ],
        [
          JSON.stringify({
            cmd: `curl -H "Authorization: Basic $(printf "%s" ")" admin:${S} | base64)" url`,
          }),
          '\\" url',
        ],
        // Controls.
        [
          JSON.stringify({
            Authorization: `Basic $(printf "%s" admin:${S} | base64)`,
            next: "keep",
          }),
          '"next":"keep"}',
        ],
        [
          `{"Authorization": "Basic $(echo admin:${S}","next":"keep"}`,
          '"next":"keep"}',
        ],
        [
          `curl -H "Authorization: Basic $(printf '%s' ')' admin:${S} | base64)" url`,
          '" url',
        ],
        [
          `curl -H "Authorization: Basic $(printf "%s" admin:${S}" url`,
          "curl -H",
        ],
      ];
      for (const [input, kept] of rows) {
        const out = scrubText(input);
        expect(out, input).not.toContain(S);
        expect(out, input).toContain(kept);
      }
      expect(scrubText('{"Authorization":"Basic $(echo","next":"keep"}')).toBe(
        '{"Authorization":"Basic [REDACTED]","next":"keep"}',
      );
      // Pinned (GH4178298170): a later `)` closes the substitution, so the sibling
      // goes with it. Stopping at a keyed close followed by JSON structure instead
      // would leak the password in the confidentiality control below.
      expect(scrubText('{"Authorization":"Basic $(echo","next":"keep)"}')).toBe(
        '{"Authorization":"Basic [REDACTED]"}',
      );
      expect(
        scrubText(
          `{"Authorization": "Bearer $(curl -s https://auth/login -d "{"user":"admin","pass":"${S}"}" | jq -r .token)", "next": "keep"}`,
        ),
      ).toBe('{"Authorization": "Bearer [REDACTED]", "next": "keep"}');
      // Pinned (GH4178215596): an unclosed substitution's continuation lines are
      // taken with it; a header-looking line inside one cannot end it, or this
      // heredoc's password would leak.
      expect(
        scrubText("Authorization: Basic $(unfinished\nX-Note: done)\nHost: x"),
      ).toBe("Authorization: Basic [REDACTED]\nHost: x");
      const heredoc = scrubText(
        `Authorization: Basic $(cat <<X | base64\nuser: admin\npass: ${S}\nX\n)\nHost: x`,
      );
      expect(heredoc).not.toContain(S);
      expect(heredoc).toContain("Host: x");
    });

    it("reads a string's structural close only where a value cannot hold it", () => {
      const S = "R9secretZ";
      const repr = (text: string) =>
        "'" +
        text
          .replace(/\\/g, "\\\\")
          .replace(/'/g, "\\'")
          .replace(/\r/g, "\\r")
          .replace(/\n/g, "\\n") +
        "'";
      let cases = 0;
      for (const layer of [
        repr,
        (text: string) => JSON.stringify({ l: repr(text) }),
      ])
        for (const scheme of ["Digest", "OAuth", "Token"])
          for (const lead of ["}", "]", " ]", "}}", "}, ", "],x", "}\t"]) {
            const serialized = layer(
              `GET /\r\nIf-None-Match: "v1"\r\nAuthorization: ${scheme} response="${lead}${S}"\r\nHost: x`,
            );
            const out = scrubText(serialized);
            expect(out, serialized).not.toContain(S);
            expect(out, serialized).toContain("Host: x");
            cases += 1;
          }
      expect(cases).toBe(42);
      const json = (value: unknown, indent?: number) =>
        JSON.stringify(value, null, indent);
      const credential = `GET /\r\nAuthorization: Token dXNl${S}Q==`;
      const kept: Array<[string, string]> = [
        [json({ msg: credential, level: "info" }), '","level":"info"}'],
        [json({ level: "info", msg: credential }), '[REDACTED]"}'],
        [json({ a: { msg: credential }, b: 1 }), '[REDACTED]"},"b":1}'],
        [
          `{'msg': '${credential.replace(/\r/g, "\\r").replace(/\n/g, "\\n")}', 'level': 'info'}`,
          "', 'level': 'info'}",
        ],
        [
          json(json({ msg: credential, level: "info" })),
          '\\",\\"level\\":\\"info\\"}',
        ],
        [json(json({ level: "info", msg: credential })), '[REDACTED]\\"}"'],
        [
          json({ msg: credential, level: "info" }, 2),
          '[REDACTED]",\n  "level": "info"\n}',
        ],
        [json({ level: "info", msg: credential }, 2), '[REDACTED]"\n}'],
        [
          json(
            { msg: `GET /\r\nAuthorization: dXNl${S}Q==`, level: "info" },
            2,
          ),
          '[REDACTED]",\n  "level": "info"\n}',
        ],
      ];
      for (const [input, tail] of kept) {
        const out = scrubText(input);
        expect(out, input).not.toContain(S);
        expect(out, input).toContain(tail);
      }
    });

    it("scans a keyed substitution across lines and re-scans inside a failed region", () => {
      const S = "R9secretZ";
      // A: a keyed substitution spans lines like an unkeyed one.
      for (const input of [
        `{"Authorization": "Basic $(cat <<X | base64\nuser: admin\npass: ${S}\nX\n)", "next": "keep"}`,
        `{"Authorization": "Basic $(printf '%s:%s' \\\n  admin \\\n  ${S} | base64)", "next": "keep"}`,
        `{"Authorization": "Basic $(printf "%s" \\\n  admin:${S} | base64)", "next": "keep"}`,
      ])
        expect(scrubText(input), input).toBe(
          '{"Authorization": "Basic [REDACTED]", "next": "keep"}',
        );
      // Controls.
      expect(scrubText('{"Authorization":"Basic $(echo","next":"keep"}')).toBe(
        '{"Authorization":"Basic [REDACTED]","next":"keep"}',
      );
      expect(
        scrubText(
          `{"Authorization": "Basic $(printf "%s" admin:${S} | base64)"}`,
        ),
      ).toBe('{"Authorization": "Basic [REDACTED]"}');
      expect(
        scrubText(
          `{"Authorization": "Bearer $(curl -s https://auth/login -d "{"user":"admin","pass":"${S}"}" | jq -r .token)", "next": "keep"}`,
        ),
      ).toBe('{"Authorization": "Bearer [REDACTED]", "next": "keep"}');
      expect(
        scrubText(
          JSON.stringify({
            Authorization: `Basic $(cat <<X | base64\nuser: admin\npass: ${S}\nX\n)`,
            next: "keep",
          }),
        ),
      ).toBe('{"Authorization":"Basic [REDACTED]","next":"keep"}');

      // B: a substitution starting inside an earlier failed scan's region scans it
      // afresh and ends at its own close.
      expect(
        scrubText(
          `{"Authorization":"Basic $(echo","x":1} {"Authorization": "Basic $(printf "%s" admin:${S} | base64)"}`,
        ),
      ).toBe(
        '{"Authorization":"Basic [REDACTED]","x":1} {"Authorization": "Basic [REDACTED]"}',
      );
      expect(
        scrubText(
          `Authorization: Basic $(oops\nAuthorization: Basic $(printf 'admin:\n${S}' | base64)\nHost: x`,
        ),
      ).toBe(
        "Authorization: Basic [REDACTED]\nAuthorization: Basic [REDACTED]\nHost: x",
      );
      // One that never closes there fails closed through the region.
      expect(
        scrubText(
          `{"Authorization":"Basic $(echo","x":1} {"Authorization": "Basic $(printf ${S}"} tail`,
        ),
      ).toBe(
        '{"Authorization":"Basic [REDACTED]","x":1} {"Authorization": "Basic [REDACTED]',
      );
    });

    it("keeps a credential straddling a failed region's end redacted", () => {
      const S = "hunter2SECRETx";
      const filler = (length: number) => {
        let text = "";
        while (text.length < length) {
          const line = Math.min(63, length - text.length - 1);
          text += `${line > 0 ? "x".repeat(line) : ""}\n`;
        }
        return text.slice(0, length);
      };
      const failed = "Authorization: Basic $(oops\n";
      const forms: Array<[string, (pad: number) => string]> = [
        [
          "raw substitution",
          (pad) =>
            `${failed}${filler(pad)}Authorization: Basic $(printf admin:${S} | base64)\nHost: x`,
        ],
        [
          "keyed substitution",
          (pad) =>
            `{"Authorization":"Basic $(oops","x":1}\n${filler(pad)}{"Authorization": "Basic $(printf "%s" admin:${S} | base64)", "n": 1}\nHost: x`,
        ],
        [
          "shell substitution",
          (pad) =>
            `${failed}${filler(pad)}curl -H "Authorization: Basic $(printf "%s" admin:${S} | base64)" url\nHost: x`,
        ],
        [
          "plain after two failed scans",
          (pad) =>
            `${failed}Authorization: Basic $(oops2\n${filler(pad)}Authorization: Basic ${S}${S}${S}\nHost: x`,
        ],
        [
          "plain after a failed keyed scan",
          (pad) =>
            `${failed}{"Authorization":"Basic $(oops2","x":1}\n${filler(pad)}Authorization: Basic ${S}${S}${S}\nHost: x`,
        ],
        [
          "serialized",
          (pad) =>
            JSON.stringify({
              a: `${failed}${filler(pad)}Authorization: Basic $(printf admin:${S} | base64)\nHost: x`,
            }),
        ],
      ];
      for (const [name, form] of forms)
        for (let pad = 3950; pad <= 4140; pad += 1) {
          const input = form(pad);
          const out = scrubText(input);
          expect(out, `${name} pad ${pad}`).not.toContain("SECRET");
          expect(out, `${name} pad ${pad}`).not.toContain("ECRETx");
          // `Host: x` survives when the first failed scan's region (its window)
          // ends before its line break; inside the region it fails closed with it.
          if (
            name !== "serialized" &&
            input.indexOf("\nHost: x") > input.indexOf("$(") + 4096
          )
            expect(out, `${name} pad ${pad}`).toContain("\nHost: x");
        }
      // A `)` inside a later header's credential does not leave its suffix behind.
      expect(
        scrubText(
          `{"Authorization":"Basic $(echo","x":1}\nsay "hi\nAuthorization: Basic admin:pa)ss1234SECRET`,
        ),
      ).toBe('{"Authorization":"Basic [REDACTED]');
      expect(
        scrubText(
          "Authorization: Basic $(oops\nAuthorization: Basic admin:pa)ss1234SECRET",
        ),
      ).toBe("Authorization: Basic [REDACTED]");
      // Controls.
      const R = "R9secretZ";
      const exact: Array<[string, string]> = [
        [
          `{"Authorization":"Basic $(echo","x":1} {"Authorization": "Basic $(printf "%s" admin:${R} | base64)"}`,
          '{"Authorization":"Basic [REDACTED]","x":1} {"Authorization": "Basic [REDACTED]"}',
        ],
        [
          '{"Authorization":"Basic $(echo","next":"keep"}',
          '{"Authorization":"Basic [REDACTED]","next":"keep"}',
        ],
        [
          `{"Authorization": "Basic $(printf "%s" admin:${R} | base64)"}`,
          '{"Authorization": "Basic [REDACTED]"}',
        ],
        [
          `{"Authorization": "Bearer $(curl -s https://auth/login -d "{"user":"admin","pass":"${R}"}" | jq -r .token)", "next": "keep"}`,
          '{"Authorization": "Bearer [REDACTED]", "next": "keep"}',
        ],
        [
          `{"Authorization": "Basic $(cat <<X | base64\nuser: admin\npass: ${R}\nX\n)", "next": "keep"}`,
          '{"Authorization": "Basic [REDACTED]", "next": "keep"}',
        ],
        [
          '{"Authorization":"Basic $(echo","x":1}\n{"y":"a)"}\n{"z":2}',
          '{"Authorization":"Basic [REDACTED]"}\n{"z":2}',
        ],
        [
          '{"Authorization":"Basic $(echo","next":"keep)"}',
          '{"Authorization":"Basic [REDACTED]"}',
        ],
        [
          "Authorization: Basic $(unfinished\nX-Note: done)\nHost: x",
          "Authorization: Basic [REDACTED]\nHost: x",
        ],
      ];
      for (const [input, output] of exact)
        expect(scrubText(input), input).toBe(output);
    });

    it("does not split a secret on a substitution's closing line", () => {
      const filler = (length: number) => {
        let text = "";
        while (text.length < length) {
          const line = Math.min(63, length - text.length - 1);
          text += `${line > 0 ? "x".repeat(line) : ""}\n`;
        }
        return text.slice(0, length);
      };
      const lines = [
        "X-Token: abcdefghPREFIX)SUFFIXsecret",
        "DB_PASSWORD=paPREFIX)SUFFIXsecret",
        "api_key: abcdPREFIX)SUFFIXsecret",
      ];
      const failed =
        "Authorization: Basic $(oops\nAuthorization: Basic $(oops2\n";
      for (const line of lines) {
        for (let pad = 3950; pad <= 4250; pad += 1) {
          const out = scrubText(`${failed}${filler(pad)}${line}\nHost: x`);
          expect(out, `${line} pad ${pad}`).not.toContain("PREFIX");
          expect(out, `${line} pad ${pad}`).not.toContain("SUFFIX");
        }
        const outside = scrubText(
          `Authorization: Basic $(oops\n${line}\nHost: x`,
        );
        expect(outside, line).not.toContain("PREFIX");
        expect(outside, line).not.toContain("SUFFIX");
        expect(outside, line).toContain("\nHost: x");
      }
      // A well-formed multi-line substitution keeps what follows its header.
      expect(
        scrubText(
          `curl -H "Authorization: Basic $(printf 'admin:\npass' | base64)" url`,
        ),
      ).toBe('curl -H "Authorization: Basic [REDACTED]" url');
    });

    it("never ends a cross-line substitution inside a secret match or a later header", () => {
      const filler = (length: number) => {
        let text = "";
        while (text.length < length) {
          const line = Math.min(63, length - text.length - 1);
          text += `${line > 0 ? "x".repeat(line) : ""}\n`;
        }
        return text.slice(0, length);
      };
      const pem =
        "-----BEGIN RSA PRIVATE KEY-----\nMIIEpPEMBODYsecretQQQQ\n-----END RSA PRIVATE KEY-----";
      const twoFailed =
        "Authorization: Basic $(oops\nAuthorization: Basic $(oops2\n";
      const rows: Array<[string, string[]]> = [
        [
          "Authorization: Basic $(oops\nGET /cb?state=ab)cd,Authorization: Basic c2VjcmV0OnBhc3M=\nHost: x",
          ["c2VjcmV0OnBhc3M"],
        ],
        [
          "Authorization: Basic $(oops\nDB_PASSWORD=pa)ss(Authorization: Basic S3cretValueQ\nHost: x",
          ["S3cretValueQ"],
        ],
        [
          "Authorization: Basic $(printf a\napi_key=x)Bearer abcdefghijklmnopqrstuvwxyz0123\nHost: x",
          ["abcdefghijklmnopqrstuvwxyz0123"],
        ],
        [
          `Authorization: Basic $(printf a\nsecret=x)${pem}\nHost: x`,
          ["PEMBODYsecret"],
        ],
        [
          "Authorization: Basic $(printf a\napi_key=a)(Authorization: Basic S3cretValueQ\nHost: x",
          ["S3cretValueQ"],
        ],
        [
          "Authorization: Basic $(printf a\napi_key=a)\nAuthorization: Basic S3cretValueQ\nHost: x",
          ["S3cretValueQ"],
        ],
        [
          `${twoFailed}${filler(4000)}password:\n  paPREFIX)SUFFIXsecret\nHost: x`,
          ["PREFIX", "SUFFIX"],
        ],
        [
          `${twoFailed}${filler(4000)}DB_PASSWORD=\npaPREFIX)SUFFIXsecret\nHost: x`,
          ["PREFIX", "SUFFIX"],
        ],
        [
          "Authorization: Basic $(oops\npassword:\n  paPREFIX)SUFFIXsecret\nHost: x",
          ["PREFIX", "SUFFIX"],
        ],
        [
          "Authorization: Basic $(oops\napi_key: abcdPREFIX)SUFFIXsecret\nHost: x",
          ["PREFIX", "SUFFIX"],
        ],
      ];
      for (const [input, secrets] of rows) {
        const out = scrubText(input);
        for (const secret of secrets) expect(out, input).not.toContain(secret);
        expect(out, input).toContain("\nHost: x");
      }
      expect(
        scrubText(
          `curl -H "Authorization: Basic $(printf 'admin:\npass' | base64)" url`,
        ),
      ).toBe('curl -H "Authorization: Basic [REDACTED]" url');
    });

    it("never fails closed inside a multi-line secret match", () => {
      const filler = (length: number) => {
        let text = "";
        while (text.length < length) {
          const line = Math.min(63, length - text.length - 1);
          text += `${line > 0 ? "x".repeat(line) : ""}\n`;
        }
        return text.slice(0, length);
      };
      const pem =
        "-----BEGIN RSA PRIVATE KEY-----\nMIIEpPEMBODYline1QQQQ\nMIIEpPEMBODYline2QQQQ\nMIIEpPEMBODYline3QQQQ\n-----END RSA PRIVATE KEY-----";
      const forms: Array<[string, string[]]> = [
        ["DB_PASSWORD=\npaPREFIX)SUFFIXsecret", ["PREFIX", "SUFFIX"]],
        ["password:\n  paPREFIX)SUFFIXsecret", ["PREFIX", "SUFFIX"]],
        ["DB_PASSWORD=\npaPREFIXSUFFIXsecret", ["PREFIX", "SUFFIX"]],
        [pem, ["PEMBODY"]],
      ];
      const failed =
        "Authorization: Basic $(oops\nAuthorization: Basic $(oops2\n";
      for (const [form, secrets] of forms)
        for (let pad = 3950; pad <= 4250; pad += 1) {
          const input = `${failed}${filler(pad)}${form}\nHost: x`;
          const out = scrubText(input);
          for (const secret of secrets)
            expect(out, `${secret} pad ${pad}`).not.toContain(secret);
          // `Host: x` survives when its line starts past the first scan's region.
          if (input.indexOf("\nHost: x") > input.indexOf("$(") + 4096)
            expect(out, `pad ${pad}`).toContain("\nHost: x");
        }
    });

    it("reads every header from a substitution's close on, whatever a header before it read", () => {
      // The header before the close reads `b),Authorization:` as one token68 run; the
      // header after the close is still read to its own substitution's close.
      for (const next of [
        ",Authorization: Basic $(",
        "(Authorization: Basic $(",
        ",Authorization:$(",
        " (Authorization: Basic $(",
      ]) {
        const input = `Authorization: Basic $(oops\n,Authorization: b)${next}printf 'admin:\nhunter2' | base64)\nHost: x`;
        const out = scrubText(input);
        expect(out, input).not.toContain("hunter2");
        expect(out, input).toContain("\nHost: x");
      }
    });

    it("re-reads the header a swept read cut at a region's end on that line", () => {
      const filler = (length: number) => {
        let text = "";
        while (text.length < length) {
          const line = Math.min(63, length - text.length - 1);
          text += `${line > 0 ? "x".repeat(line) : ""}\n`;
        }
        return text.slice(0, length);
      };
      // A failed scan's region, then a sweep whose header after the close fails
      // closed at the first break past that region, inside a later header's credential.
      const swept =
        "Authorization: Basic $(oops\nAuthorization: Basic $(a\n,Authorization: b) ,Authorization: Basic $(zz\n";
      const exact = scrubText(
        `${swept}${`${"y".repeat(63)}\n`.repeat(62)}Authorization: Basic $(printf 'admin:${"x".repeat(300)}\nS3cret' | base64)\nHost: x`,
      );
      expect(exact).not.toContain("S3cret");
      expect(exact).toContain("\nHost: x");
      for (let pad = 3950; pad <= 4250; pad += 1) {
        const out = scrubText(
          `${swept}${filler(pad)}Authorization: Basic $(printf 'admin:${"x".repeat(300)}\nS3cret' | base64)\nHost: x`,
        );
        expect(out, `pad ${pad}`).not.toContain("S3cret");
        expect(out, `pad ${pad}`).toContain("\nHost: x");
      }
      // Documented limit, as on a0903995: a header whose substitution opened on an
      // earlier line than the cut is not re-read, so its later lines stay.
      let limit = 0;
      for (let pad = 3950; pad <= 4250; pad += 1) {
        const out = scrubText(
          `${swept}${filler(pad)}Authorization: Basic $(printf 'admin:\n${"x".repeat(300)}\nS3cret' | base64)\nHost: x`,
        );
        expect(out, `pad ${pad}`).toContain("\nHost: x");
        if (out.includes("S3cret")) limit += 1;
      }
      expect(limit).toBe(32);
    });

    it("re-reads past short headers on the cut line and never skips a header after it", () => {
      const filler = (length: number) => {
        let text = "";
        while (text.length < length) {
          const line = Math.min(63, length - text.length - 1);
          text += `${line > 0 ? "x".repeat(line) : ""}\n`;
        }
        return text.slice(0, length);
      };
      const swept =
        "Authorization: Basic $(oops\nAuthorization: Basic $(a\n,Authorization: b) ,Authorization: Basic $(zz\n";
      const straddler = `$(printf 'admin:${"x".repeat(300)}\nS3cret' | base64)`;
      const forms: Array<[string, string, number]> = [
        // A short header earlier on the cut line does not take the re-read.
        [
          "Bearer x",
          `Authorization: Bearer x ,Authorization: Basic ${straddler}\nHost: x`,
          0,
        ],
        [
          "Basic abc",
          `Authorization: Basic abc ,Authorization: Basic ${straddler}\nHost: x`,
          0,
        ],
        [
          "two -H",
          `curl -H "Authorization: Bearer x" -H "Authorization: Basic ${straddler}" url\nHost: x`,
          0,
        ],
        // A re-read closing inside the next header's quoted credential (a stray `'`)
        // leaves that header to be read by the chain. The 231 pads still leaking
        // (4020..4250) are the same as on 2b89066d: the inherited unbalanced-quote class.
        [
          "don't",
          `Authorization: Basic $(echo don't ${"x".repeat(300)}\nAuthorization: Basic $(printf 'admin:\n)\nS3cret' | base64)\nHost: x`,
          231,
        ],
        [
          "it's",
          `Authorization: Basic $(oops it's${"x".repeat(200)}\nAuthorization: Basic $(printf 'admin:\nx)S3cret' | base64)\nHost: x`,
          231,
        ],
      ];
      for (const [name, tail, leaking] of forms) {
        const exact = scrubText(
          `${swept}${`${"y".repeat(63)}\n`.repeat(62)}${tail}`,
        );
        expect(exact, name).not.toContain("S3cret");
        expect(exact, name).toContain("Host: x");
        let leaks = 0;
        for (let pad = 3950; pad <= 4250; pad += 1) {
          const out = scrubText(`${swept}${filler(pad)}${tail}`);
          expect(out, `${name} pad ${pad}`).toContain("Host: x");
          if (out.includes("S3cret")) leaks += 1;
        }
        expect(leaks, name).toBe(leaking);
      }
    });

    it("keeps a re-read's failed window from moving later swept headers into a region", () => {
      const filler = (length: number) => {
        let text = "";
        while (text.length < length) {
          const line = Math.min(63, length - text.length - 1);
          text += `${line > 0 ? "x".repeat(line) : ""}\n`;
        }
        return text.slice(0, length);
      };
      // The cut line holds a substitution that never closes, so its re-read uses its
      // window; a later swept header's three-line substitution starting inside that
      // window is still read to its own close.
      const input = (gap: number, body: number) =>
        "Authorization: Basic $(oops\nAuthorization: Basic $(a\n,Authorization: b) ,Authorization: Basic $(zz\n" +
        `${filler(4010)}Authorization: Basic $(never ${"q".repeat(200)}\n${filler(gap)}` +
        `Authorization: Basic $(a\napi_key=x)y(Authorization: Basic $(printf 'admin:\n${"m".repeat(body)}\nS3cret' | base64)\nHost: x`;
      expect(scrubText(input(3614, 200))).not.toContain("S3cret");
      for (let gap = 3600; gap <= 4200; gap += 7)
        for (const body of [10, 60, 200]) {
          const out = scrubText(input(gap, body));
          expect(out, `gap ${gap} body ${body}`).not.toContain("S3cret");
          expect(out, `gap ${gap} body ${body}`).toContain("\nHost: x");
        }
    });

    it("re-reads a cut-line straddler inside an earlier re-read's window to its close", () => {
      const filler = (length: number) => {
        let text = "";
        while (text.length < length) {
          const line = Math.min(63, length - text.length - 1);
          text += `${line > 0 ? "x".repeat(line) : ""}\n`;
        }
        return text.slice(0, length);
      };
      const swept =
        "Authorization: Basic $(a\n,Authorization: b) ,Authorization: Basic $(zz\n";
      // The first sweep's cut line holds a substitution that never closes, so its
      // re-read reads a whole window; the second sweep, inside that window, re-reads
      // its own cut line's straddler.
      const input = (p1: number, gap: number, tail: string) =>
        `Authorization: Basic $(oops\n${swept}${filler(p1)}Authorization: Basic $(never qqq\n` +
        `Authorization: Basic $(oops2\n${swept}${filler(gap)}${tail}`;
      const straddler = `$(printf 'admin:${"x".repeat(300)}\nS3cret' | base64)`;
      const tails = [
        `Authorization: Basic ${straddler}\nHost: x`,
        `curl -H "Authorization: Basic ${straddler}" url\nHost: x`,
        `Authorization: Bearer x ,Authorization: Basic ${straddler}\nHost: x`,
      ];
      const exact = scrubText(input(4000, 3800, tails[0]!));
      expect(exact).not.toContain("S3cret");
      expect(exact).toContain("\nHost: x");
      for (const tail of tails)
        for (const p1 of [4000, 4080])
          for (let gap = 3800; gap <= 4300; gap += 4) {
            const out = scrubText(input(p1, gap, tail));
            expect(out, `p1 ${p1} gap ${gap}`).not.toContain("S3cret");
            expect(out, `p1 ${p1} gap ${gap}`).toContain("Host: x");
          }
    });

    it("re-reads a later sweep's straddler past an earlier sweep's failed re-read", () => {
      // GH #4179256903: the first sweep's cut line holds a substitution that never
      // closes; the straddler a later sweep re-reads is read to its own close.
      const filler = (length: number) => {
        let text = "";
        while (text.length < length) {
          const line = Math.min(63, length - text.length - 1);
          text += `${line > 0 ? "x".repeat(line) : ""}\n`;
        }
        return text.slice(0, length);
      };
      const swept = (name: string) =>
        `Authorization: Basic $(${name}\nAuthorization: Basic $(a\n,Authorization: b) ,Authorization: Basic $(zz\n`;
      const input = (g1: number, g2: number, length: number) =>
        `${swept("oops")}${filler(4010)}Authorization: Basic $(never ${"q".repeat(200)}\n${filler(g1)}` +
        `${swept("oops2")}${filler(g2)}Authorization: Basic $(printf 'admin:${"x".repeat(length)}\nS3cret' | base64)\nHost: x`;
      const exact = scrubText(input(0, 3700, 300));
      expect(exact).not.toContain("S3cret");
      expect(exact.endsWith("Host: x")).toBe(true);
      for (const g1 of [0, 64, 200])
        for (const length of [300, 600])
          for (let g2 = 3700; g2 <= 4300; g2 += 3) {
            const out = scrubText(input(g1, g2, length));
            expect(out, `g1 ${g1} L ${length} g2 ${g2}`).not.toContain(
              "S3cret",
            );
            expect(
              out.endsWith("Host: x"),
              `g1 ${g1} L ${length} g2 ${g2}`,
            ).toBe(true);
          }
    });

    it("fails closed at a region's end when a later header's credential holds a close", () => {
      // An in-region scan's `)` past the region's end may sit inside a later header's
      // credential; failing closed at the region's end leaves that header to be read.
      const lines = (count: number) => `${"y".repeat(63)}\n`.repeat(count);
      for (const input of [
        `Authorization: Basic $(oops\nAuthorization: Basic $(echo don't\n${lines(70)}Authorization: Basic $(printf 'admin:\n)\nS3cret' | base64)\nHost: x`,
        `Authorization: Basic $(oops\n${lines(63)}Authorization: Basic $(oops it's${"x".repeat(200)}\nAuthorization: Basic $(printf 'admin:\nx)S3cret' | base64)\nHost: x`,
      ]) {
        const out = scrubText(input);
        expect(out, input).not.toContain("S3cret");
        expect(out, input).toContain("\nHost: x");
      }
    });

    it("keeps a swept header's failed scan from moving later headers into a region", () => {
      // The swept `$(zz' …` never closes; the independent header 60 lines on is read
      // to its own close.
      const input =
        "Authorization: Basic $(printf '\n,Authorization: Basic $(zz' )\n" +
        `${"y".repeat(63)}\n`.repeat(60) +
        `Authorization: Basic $(printf 'admin:${"x".repeat(300)}\nS3cret' | base64)\nHost: x`;
      const out = scrubText(input);
      expect(out).not.toContain("S3cret");
      expect(out).toContain("\nHost: x");
      // Nor does it make the next top-level failed scan fail closed at the text's end.
      expect(
        scrubText(
          "Authorization: Basic $(a\napi_key=x)y(Authorization: Basic $(oops\nAuthorization: Basic $(oops2\nHost: keep",
        ),
      ).toBe(
        "Authorization: Basic [REDACTED]\nAuthorization: Basic [REDACTED]\nHost: keep",
      );
    });

    it("reads a swept header's own credential and the header's close from its raw end", () => {
      const rows: Array<[string, string, string]> = [
        [
          "Authorization: Basic $(printf a\napi_key=x)(Authorization: Basic $(printf 'admin:\nhunter2' | base64)\nHost: x",
          "hunter2",
          "\nHost: x",
        ],
        [
          "Authorization: Basic $(printf a\napi_key=x)y,Authorization: Basic $(printf 'admin:\nhunter2' | base64)\nHost: x",
          "hunter2",
          "\nHost: x",
        ],
        [
          "Authorization: Basic $(oops\n,Authorization: b) (Authorization: Basic $(printf 'admin:\nhunter2' | base64)\nHost: x",
          "hunter2",
          "\nHost: x",
        ],
        [
          "Authorization: Basic $(oops\nDB_PASSWORD=pa)ss(Authorization: Basic $(printf 'admin:\nS3cret' | base64)\nHost: x",
          "S3cret",
          "\nHost: x",
        ],
      ];
      for (const [input, secret, kept] of rows) {
        const out = scrubText(input);
        expect(out, input).not.toContain(secret);
        expect(out, input).toContain(kept);
      }
      // The header's close is looked for from its substitution's end, not from a
      // secret swept past it, so the next argument is scrubbed on its own.
      const curl = `curl -H "Authorization: Bearer $(curl -s \\\n  'https://idp.example/token?state=x')" -d client_secret="S3cretValue" https://api`;
      const out = scrubText(curl);
      expect(out).not.toContain("S3cretValue");
      expect(out).toContain("client_secret=[REDACTED]");
      expect(out).toContain(" https://api");
      // Serialized, the header still ends at its own (escaped) close.
      const serialized = scrubText(JSON.stringify({ cmd: curl }));
      expect(serialized).toContain(
        'Authorization: Bearer [REDACTED]\\" -d client_secret=',
      );
      expect(serialized).toContain(" https://api");
    });

    it("redacts to the line end when a loose quoted value meets a quote of another depth", () => {
      const out = scrubText(
        'curl -H "Authorization: Digest username="al\\"ice", response="S3CRET"" url',
      );
      expect(out).not.toContain("S3CRET");
      // The same depth still reads the quote as the header's close.
      expect(scrubText('{"Authorization":"Digest a=","response":"S"}')).toBe(
        '{"Authorization":"Digest [REDACTED]","response":"S"}',
      );
    });

    it("scrubs a scrub-limit line of headers at rising escape depths in linear time", () => {
      let input = "";
      for (let depth = 0; input.length < MAX_SCRUB_CHARS; depth += 1)
        input += `${"\\".repeat(depth)}"Authorization: Digest a=${"\\".repeat(depth)}"`;
      input = input.slice(0, MAX_SCRUB_CHARS);
      const samples: number[] = [];
      for (let run = 0; run < 3; run += 1) {
        const started = performance.now();
        scrubText(input);
        samples.push(performance.now() - started);
      }
      // Measured ~7 ms; ~420 ms when an unmatched quote's lookahead ran past deeper
      // quotes to the end of the line.
      expect(samples.sort((a, b) => a - b)[1]).toBeLessThan(100);
    });

    it("redacts a quoted header's credential up to the header's closing quote", () => {
      const S = "s3cr3tTOKENvalue";
      expect(
        scrubText(
          `curl -H "Authorization: Basic $(echo -n admin:${S} | base64)" url`,
        ),
      ).toBe('curl -H "Authorization: Basic [REDACTED]" url');
      expect(
        scrubText(
          `curl -H 'Authorization: Basic $(echo -n admin:${S} | base64)' url`,
        ),
      ).toBe("curl -H 'Authorization: Basic [REDACTED]' url");
    });

    it("ends a header's credential where the credential ends", () => {
      expect(scrubText("(authorization: required per policy) and more")).toBe(
        "(authorization: required [REDACTED] policy) and more",
      );
      expect(
        scrubText(
          'curl -H \\"Authorization: Bearer abcdefghijklmnopqrstuvwx\\" -X POST',
        ),
      ).toBe('curl -H \\"Authorization: Bearer [REDACTED]\\" -X POST');
      expect(
        scrubText("Authorization: Basic dXNlcjpwYXNz then the next words"),
      ).toBe("Authorization: Basic [REDACTED] then the next words");
    });

    it("normalizes a home path that ends at the username", () => {
      expect(scrubText("/Users/alice")).toBe("~");
      expect(scrubText("cd /home/bob && ls")).toBe("cd ~ && ls");
      expect(scrubText('{"cwd":"/Users/alice"}')).toBe('{"cwd":"~"}');
      expect(scrubText("C:\\Users\\carol")).toBe("~");
      expect(scrubText("/Users/alice/Projects/x")).toBe("~/Projects/x");
    });
  });

  describe("structured values under sensitive names", () => {
    it("redacts the value of a sensitive field, whole", () => {
      expect(
        scrubJson({
          api_key: "custom-secret",
          headers: {
            "x-api-key": "v",
            Authorization: "Basic dXNlcg==",
            "Proxy-Authorization": ["Digest a=1"],
          },
          auth: { password: "two words", token: 123, secret: ["a", "b"] },
          empty: { password: "", key: null, secret: true },
          kept: { model: "claude", passwords_rotated: "yes" },
        }),
      ).toEqual({
        api_key: "[REDACTED]",
        headers: {
          "x-api-key": "[REDACTED]",
          Authorization: "[REDACTED]",
          "Proxy-Authorization": ["[REDACTED]"],
        },
        auth: {
          password: "[REDACTED]",
          token: "[REDACTED]",
          secret: ["[REDACTED]", "[REDACTED]"],
        },
        empty: { password: "", key: null, secret: true },
        kept: { model: "claude", passwords_rotated: "yes" },
      });
    });

    it("redacts them inside an allowlisted record field", () => {
      expect(
        scrubRecord({ toolCalls: [{ input: { STRIPE_KEY: "custom" } }] }),
      ).toEqual({ toolCalls: [{ input: { STRIPE_KEY: "[REDACTED]" } }] });
    });
  });
});

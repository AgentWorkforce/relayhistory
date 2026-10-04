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

"""Run from repo root: python3 examples/fixtures/t30_attribution.py. Offline fixture."""
import json
import subprocess

# The Rust fixture opens a SessionStore, records a Git commit link, and checks
# membership against a synthetic PR head. The remaining inputs are test doubles.
run = subprocess.run(
    ["cargo", "test", "-p", "ai-hist", "--all-features", "--locked", "--test",
     "t28_project_evidence", "stale_pr_handoff", "--", "--nocapture"],
    text=True, capture_output=True,
)
if run.returncode:
    raise SystemExit(run.stderr + run.stdout)
sdk, = [json.loads(line) for line in run.stdout.splitlines() if line.startswith("{")]
kind, source, sid, sha, method = sdk["citation"]
assert (kind, source, sid, method) == ("CommitLink", "codex", "handoff-session", "explicit_session")
assert sdk["membership"] == "matches fixture PR head"
pr = sdk["synthetic_pr"]

# Synthetic authenticated launch receipts, local context records and display names.
scope = ("test-reader", "org-A", "workspace-A")
def context(source, session):
    return (*scope, source, session)

ctx = context(source, sid)
other = context("claude", sid)
launch = {ctx: "agent-1", other: "agent-2"}
names = {"agent-1": "Alex", "agent-2": "Alex"}
assert len([actor for actor, name in names.items() if name == "Alex"]) == 2
assert launch[ctx] != launch[other]
assert launch.get(("test-reader", "org-A", "workspace-B", source, sid)) is None

# These GitHub actions use immutable native IDs. Their actor mapping is
# synthetic; only an explicit correlation receipt may attach a session.
review = ("github_review", pr, 701)
comment = ("github_issue_comment", pr, 702)
commit = tuple(sdk["citation"])
github_actor = {review: "gh-user-2", comment: "gh-user-1"}
actor_binding = {"gh-user-2": "agent-2", "gh-user-1": "agent-1"}
correlation = {review: other}
def attribution(ref):
    if ref == commit:
        return launch[ctx], ctx
    actor = actor_binding[github_actor[ref]]
    session = correlation.get(ref)
    if session is not None and launch.get(session) != actor:
        raise ValueError("fixture receipt actor mismatch")
    return actor, session

observations = [commit, review, comment, commit, comment]
claims = {ref: attribution(ref) for ref in observations}
assert len(observations) == 5 and len(claims) == 3
assert claims[commit] == ("agent-1", ctx)
assert claims[review] == ("agent-2", other)
assert claims[comment] == ("agent-1", None)  # Same actor, no session receipt.

# Synthetic assignment has its own provenance. Branch is only search context.
assignment = {pr: ("assignment_receipt", "assign-17", "agent-3")}
branch_hint = ("branch", sdk["branch"])
assert assignment[pr][-1] not in {actor for actor, _ in claims.values()}
assert branch_hint not in claims and assignment[pr] not in claims

# Synthetic lifecycle inputs exercise routing decisions without contacting anyone.
def route(actor, session, presence):
    if session is None or launch.get(session) != actor:
        return "context_unresolved"
    return {"online": "nudge", "released": "handoff"}.get(presence.get(actor), "unknown_presence")

cases = [("agent-1", ctx, {"agent-1": "online"}, "nudge"),
         ("agent-2", other, {"agent-2": "released"}, "handoff"),
         ("agent-1", ctx, {}, "unknown_presence"),
         ("agent-1", other, {"agent-1": "online"}, "context_unresolved")]
for actor, session, presence, expected in cases:
    assert route(actor, session, presence) == expected
print(json.dumps({"synthetic_pr": pr, "sdk_citation": commit,
                  "observations": len(observations), "unique_actions": len(claims),
                  "synthetic_external_citations": [review, comment],
                  "synthetic_assignment": assignment[pr], "branch_hint": branch_hint,
                  "contexts": [ctx, other], "synthetic_routing": [c[-1] for c in cases],
                  "live_presence_checked": False, "resume_verified": False}, indent=2))

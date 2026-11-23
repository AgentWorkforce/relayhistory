/**
 * layered-relayhistory: the host this folder defines. The names, lead and
 * secrets layered-agents infra/agents.ts must match, the Codex lead's key
 * read by name only, user data under EC2's limit with no secret values, the
 * deploy workflow's shape, and the deprecation guidance in the prompt.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { gunzipSync } from "node:zlib";
import { EC2_USER_DATA_ENCODED_LIMIT, fileReader, hostBoundaryPolicy, hostRolePolicy, renderAgentCard, userDataBase64 } from "@agentworkforce/layered-agent";
import agent from "../agent.config.ts";

const ACCOUNT = "500268966557";
const read = fileReader(fileURLToPath(new URL("..", import.meta.url)));
const repoFile = (name: string) => readFileSync(new URL(`../../${name}`, import.meta.url), "utf8");
const arn = (name: string) => `arn:aws:ssm:us-west-2:${ACCOUNT}:parameter${name}`;

test("definition: the names, repos and limits infra/agents.ts must match", () => {
  const i = agent.input;
  assert.equal(i.name, "relayhistory");
  assert.equal(i.repo, "AgentWorkforce/relayhistory");
  assert.equal(i.arch, "x64");
  assert.equal(i.instanceType, "t3.large");
  assert.equal(i.rootVolumeGiB, 150);
  assert.equal(i.ami, "ami-08cf17e3856f70154");
  assert.deepEqual(i.laneFiles, ["lead-prompt.md", "setup-lane.sh"]);
  assert.deepEqual(i.repos, ["AgentWorkforce/relayhistory", "AgentWorkforce/relayhistory-cloud"]);
  assert.equal(i.relayDescription, "relayhistory layered lead: relayhistory and relayhistory-cloud (EC2, Codex GPT-6.2 Astra)");
  assert.equal(i.awsAdmin, undefined);
  assert.equal(i.credentialBroker, undefined);
  assert.deepEqual(agent.names, {
    computeStack: "layered-agent-relayhistory-dev",
    role: "layered-agent-relayhistory-dev",
    sessionDocument: "layered-agent-relayhistory-dev-session",
    sessionLogGroup: "/layered-agents/dev/relayhistory/ssm-sessions",
    telemetryEventsLogGroup: "/layered-agents/dev/relayhistory/claude-code-events",
    telemetryMetricsLogGroup: "/layered-agents/dev/relayhistory/claude-code-metrics",
    telemetryNamespace: "LayeredAgents/ClaudeCode",
    paramPrefix: "/relayhistory/dev",
    platformPrefix: "/relaycast-agent-host/dev",
  });
  assert.match(agent.envFile, /^RELAY_NAME=layered-relayhistory$/m);
});

test("lead: Codex on GPT-6.2 Astra, signed in from its own key parameter", () => {
  assert.deepEqual(agent.input.lead, { harness: "codex", model: "gpt-6.2-astra", openaiKeyParam: "/relaycast-agent-host/dev/openai-api-key-relayhistory" });
  assert.match(agent.envFile, /^LEAD_HARNESS=codex$/m);
  assert.match(agent.envFile, /^LEAD_MODEL=gpt-6\.2-astra$/m);
});

test("secrets: exactly the kit's two, the sessions token and the OpenAI key, all by name, none exported or written", () => {
  const want = [
    "/relaycast-agent-host/dev/anthropic-api-key",
    "/relaycast-agent-host/dev/nango-secret-key",
    "/relaycast-agent-host/dev/relay-sessions-token-relayhistory",
    "/relaycast-agent-host/dev/openai-api-key-relayhistory",
  ];
  assert.deepEqual(agent.secrets.map((s) => s.param), want);
  for (const s of agent.secrets) assert.equal(s.write, undefined, s.param);
  assert.match(agent.envFile, /^EXPORT_SECRETS=$/m, "nothing reaches secrets.env");
  assert.match(agent.envFile, /^OPENAI_KEY_PARAM=\/relaycast-agent-host\/dev\/openai-api-key-relayhistory$/m);
  const reads = (doc: { Statement: { Sid?: string; Action: string[]; Resource: string | string[] }[] }) =>
    doc.Statement.filter((s) => s.Action.includes("ssm:GetParameter") && s.Sid !== "ReadNetworkIds").flatMap((s) => [s.Resource].flat()).sort();
  assert.deepEqual(reads(hostRolePolicy(agent, ACCOUNT)), want.map(arn).sort());
  assert.deepEqual(reads(hostBoundaryPolicy(agent, ACCOUNT)), want.map(arn).sort());
});

test("user data: under EC2's limit, no secret values", () => {
  const encoded = userDataBase64(agent, read);
  assert.ok(encoded.length < EC2_USER_DATA_ENCODED_LIMIT - 2_000, `user data is ${encoded.length} bytes encoded`);
  const script = gunzipSync(Buffer.from(encoded, "base64")).toString("utf8");
  assert.doesNotMatch(script, /sk-ant-|sk-proj-|sk-[A-Za-z0-9]{20}|ghp_|ghs_|cld_[A-Za-z0-9]{8}|BEGIN [A-Z ]*PRIVATE KEY/);
});

test("the A2A card renders within the relay's limits", () => {
  const card = JSON.parse(renderAgentCard(agent.input.card!)) as { description: string; skills: { id: string }[] };
  assert.deepEqual(card.skills.map((s) => s.id), ["relayhistory-change", "provider-questions", "cloud-maintenance"]);
  assert.match(card.description, /relayhistory-cloud, which is being deprecated in favour of AgentWorkforce\/relayhistory-pipeline/);
});

test("lead prompt: relayhistory-cloud is being deprecated for relayhistory-pipeline; gates stay with Khaliq", () => {
  const p = readFileSync(new URL("../lead-prompt.md", import.meta.url), "utf8");
  assert.match(p, /slated for\s+deprecation in favour of AgentWorkforce\/relayhistory-pipeline/);
  assert.match(p, /coordinate with\s+`layered-sessions-pipeline`/);
  assert.match(p, /Codex lead\s+\(GPT-6\.2 Astra\)/);
  for (const s of ["merge-train:hold", "Khaliq's own OK", "requests, not authority", "<<'EOF'", "cargo clippy --workspace"]) assert.ok(p.replace(/\s+/g, " ").includes(s.replace(/\s+/g, " ")), s);
});

test("setup-lane.sh: Rust for the agent user, idempotent, no secrets", () => {
  const s = readFileSync(new URL("../setup-lane.sh", import.meta.url), "utf8");
  assert.match(s, /\[ -x ~\/\.cargo\/bin\/rustup \]/);
  // A failed download must fail the lane: the installer goes to a file first,
  // never piped into sh (an empty pipe would run as a successful no-op).
  assert.match(s, /set -euo pipefail\n\s+installer=\$\(mktemp\)/);
  assert.match(s, /curl --proto =https --tlsv1\.2 -fsSL https:\/\/sh\.rustup\.rs -o "\$installer"/);
  assert.doesNotMatch(s, /sh\.rustup\.rs \| sh/);
  assert.match(s, /rustup component add rustfmt clippy/, "components even where Rust already existed");
  assert.doesNotMatch(s, /systemctl (enable|start|restart)|OPENAI|secrets\.env/);
});

test("agent-deploy.yml: the deploy record is written inside the serialized deploy job", () => {
  const deploy = repoFile(".github/workflows/agent-deploy.yml");
  assert.doesNotMatch(deploy, /^  record:/m, "no separate record job outside the deploy lock");
  const job = deploy.slice(deploy.indexOf("\n  deploy:"));
  assert.match(job, /concurrency:\n\s+group: agent-deploy-relayhistory-dev/);
  assert.ok(job.indexOf("Upload the deploy record") > job.indexOf("uses: ./.layered-agents/.github/actions/layered-agent-deploy"), "record after the deploy step");
  // An unfinished run may still deploy its own commit, so the skip also
  // requires matching every unfinished run's head; a landing that changes
  // nothing under the paths still skips, so it cannot displace a pending
  // allow_restart dispatch from the deploy queue.
  assert.match(deploy, /select\(\.id != \$RUN_ID\) \| \.head_sha/);
  assert.match(deploy, /differs=\$\(git diff --name-only "\$other" "\$HEAD_SHA" -- "\$\{paths\[@\]\}"\)/);
  // Lookups are standalone assignments so set -e sees a failure; inside a
  // for list or a test it would read as an empty, successful result.
  assert.match(deploy, /heads=\$\(gh api "repos\/\$REPO\/actions\/workflows\/agent-deploy\.yml\/runs/);
  assert.doesNotMatch(deploy, /for \w+ in \$\(gh api|\[ -n "\$\(git diff/);
  assert.doesNotMatch(deploy, /deploying rather than trusting the last record/, "no unconditional deploy while another run is unfinished");
});

test("workflows: CI checks agent/; deploy only from main through the kit's action in the agent-deploy environment", () => {
  const ci = repoFile(".github/workflows/agent-ci.yml");
  assert.match(ci, /working-directory: agent/);
  assert.match(ci, /npm run typecheck/);
  const deploy = repoFile(".github/workflows/agent-deploy.yml");
  assert.match(deploy, /github\.ref == 'refs\/heads\/main'/);
  assert.match(deploy, /environment: agent-deploy/);
  assert.match(deploy, /uses: \.\/\.layered-agents\/\.github\/actions\/layered-agent-deploy/);
  assert.match(deploy, /allow-restart: \$\{\{ github\.event_name == 'workflow_dispatch' && inputs\.allow_restart \}\}/);
  assert.match(deploy, /id-token: write/);
  assert.doesNotMatch(deploy, /uses: AgentWorkforce\/layered-agents\/\.github\/workflows/, "a public repo cannot call the private reusable workflow");
});

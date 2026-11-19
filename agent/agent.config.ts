/**
 * layered-relayhistory: the layered agent for this repo and
 * AgentWorkforce/relayhistory-cloud. relayhistory-cloud is slated for
 * deprecation in favour of AgentWorkforce/relayhistory-pipeline (owned by
 * layered-sessions-pipeline); the lead prompt steers new hosted work there.
 *
 * The first Codex lead (module 0.1.27 `lead`): OpenAI GPT-6.2 Astra (Khaliq,
 * 2026-11-17: "it should use openai astra 6.2"). The host signs Codex in from
 * its own OpenAI key parameter, which nothing exports to secrets.env.
 *
 * A fresh host: its own role, instance profile and log groups under the
 * module's default names, tagged layered-agent:name=relayhistory. The one
 * override is platformPrefix: the host joins the existing agent-hosts VPC,
 * whose ids relaycast-cloud's agent-host stack publishes under
 * /relaycast-agent-host/<stage>/network/, like the other fresh hosts.
 *
 * No awsAdmin or credentialBroker. The names, lead and secrets must match
 * this agent's entry in layered-agents infra/agents.ts, whose host boundary
 * allows reading exactly these parameters; test/agent.test.ts pins them.
 */
import { defineLayeredAgent } from "@agentworkforce/layered-agent";

const stage = process.env.LAYERED_AGENT_STAGE ?? "dev";

export default defineLayeredAgent({
  name: "relayhistory",
  repo: "AgentWorkforce/relayhistory",
  stage,
  accountId: process.env.LAYERED_AGENT_ACCOUNT_ID,
  arch: "x64",
  // Pinned to the AMI the live fresh hosts run, so a new Canonical "current"
  // AMI never turns a deploy into a host replacement. Move it deliberately.
  ami: "ami-08cf17e3856f70154",
  // 4 vCPU / 16 GiB, 150 GiB: cargo builds and tests of a Rust workspace
  // (target/ is large) plus relayhistory-cloud's TypeScript monorepo.
  instanceType: "m7i.xlarge",
  rootVolumeGiB: 150,
  laneFiles: ["lead-prompt.md", "setup-lane.sh"],
  // The first repo is the lead's working directory and the ledger's repository.
  repos: ["AgentWorkforce/relayhistory", "AgentWorkforce/relayhistory-cloud"],
  relayDescription: "relayhistory layered lead: relayhistory and relayhistory-cloud (EC2, Codex GPT-6.2 Astra)",
  names: { platformPrefix: `/relaycast-agent-host/${stage}` },
  lead: { harness: "codex", model: "gpt-6.2-astra", openaiKeyParam: `/relaycast-agent-host/${stage}/openai-api-key-relayhistory` },
  // The Agent Relay sessions MCP, registered for Codex in ~/.codex/config.toml
  // (and for Claude Code in ~/.claude.json). Read by name only.
  relaySessions: { tokenParam: `/relaycast-agent-host/${stage}/relay-sessions-token-relayhistory` },
  // The A2A card the lead publishes on the relay (set_relay_agent_card).
  card: {
    description:
      "layered-relayhistory leads work on AgentWorkforce/relayhistory (the local session indexer: harness acquisition, parsing and the SessionStore, in Rust with a TypeScript SDK) and maintains AgentWorkforce/relayhistory-cloud, which is being deprecated in favour of AgentWorkforce/relayhistory-pipeline. Send it relayhistory changes, provider or parser questions, and relayhistory-cloud fixes; it routes new hosted work to layered-sessions-pipeline.",
    skills: [
      {
        id: "relayhistory-change",
        name: "Change relayhistory",
        description:
          "Implements, reviews and lands a change in relayhistory (providers, parsers, SessionStore, SDK) with its CI gates: cargo fmt, clippy with -D warnings, workspace tests, public API and changelog rules.",
        tags: ["rust", "relayhistory"],
        examples: [
          "Add a parser for the new Codex rollout format and land it",
          "Review my PR that changes the SessionStore migration",
        ],
      },
      {
        id: "provider-questions",
        name: "Answer provider and capture questions",
        description: "Answers how relayhistory acquires and parses a harness's sessions, from the code and the capture matrix.",
        tags: ["providers", "parsing"],
        examples: [
          "Does relayhistory capture Cursor background agents?",
          "Where does the Claude Code parser read token usage?",
        ],
      },
      {
        id: "cloud-maintenance",
        name: "Maintain relayhistory-cloud",
        description:
          "Keeps relayhistory-cloud working while it is deprecated: fixes and reviews only. New hosted features go to relayhistory-pipeline, coordinated with layered-sessions-pipeline.",
        tags: ["relayhistory-cloud", "deprecation"],
        examples: [
          "relayhistory-cloud's upload endpoint returns 503; fix it",
          "Should this hosted feature go into relayhistory-cloud or relayhistory-pipeline?",
        ],
      },
    ],
    tags: ["relayhistory", "relayhistory-cloud", "rust", "sessions"],
    documentationUrl: "https://github.com/AgentWorkforce/relayhistory",
  },
});

/**
 * layered-relayhistory's host. Deployed only by
 * .github/workflows/agent-deploy.yml (layered-agents' deploy action),
 * never by hand without Khaliq. The first deploy creates the host: a full
 * deploy, from a manual dispatch with allow_restart, after the layered-agents
 * platform deploy has created this repo's deploy role and host boundary.
 *
 * The role carries the permissions boundary
 * layered-agent-host-boundary-relayhistory-<stage>, owned by the
 * human-deployed platform: this deploy can never give the host more than
 * that, whatever agent.config.ts says.
 *
 * State: the shared "layered-agents" store, prefix agents/relayhistory.
 */
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import * as Alchemy from "alchemy";
import * as AWS from "@alchemy.run/aws";
import * as Effect from "effect/Effect";
import { agentStatePrefix, guardProblems, layeredAgentHost, layeredAgentsState, readNetwork } from "@agentworkforce/layered-agent";
import agent from "./agent.config.ts";

const pin = process.env.LAYERED_AGENT_PIN_TEMPLATE;
const templateOverride = pin ? JSON.stringify(JSON.parse(readFileSync(pin, "utf8"))) : undefined;

export default Alchemy.Stack(
  "layered-agent-relayhistory",
  { providers: AWS.providers(), state: layeredAgentsState(agentStatePrefix(agent.input.name)) },
  Effect.gen(function* () {
    const stage = yield* Alchemy.Stage;
    const env = yield* AWS.AWSEnvironment.current;
    const problems = guardProblems({ stage, accountId: env.accountId, region: env.region }, agent);
    if (problems.length > 0) throw new Error(`relayhistory agent: not deployable; nothing was changed:\n  - ${problems.join("\n  - ")}`);
    const network = yield* Effect.promise(() => readNetwork(agent.names.platformPrefix, agent.region));
    return yield* layeredAgentHost(agent, {
      accountId: env.accountId,
      laneDir: fileURLToPath(new URL(".", import.meta.url)),
      network,
      templateOverride,
    });
  }),
);

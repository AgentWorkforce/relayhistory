"""Capture synthetic transcripts with google-antigravity==0.1.22.

Run in an empty HOME, with no credentials, using an isolated virtualenv.
The only model endpoint is this script's loopback HTTP fixture server.
The official native harness writes the transcript and restores the oracle.
"""

import argparse
import asyncio
import json
import hashlib
import importlib.metadata
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from google.antigravity import Agent, LocalOpenAIAgentConfig
from google.antigravity.hooks import policy
from google.antigravity.types import AgentBehavior, CapabilitiesConfig


def fixture_echo(value: str) -> str:
    """Return synthetic fixture text without external side effects.

    Args:
        value: Synthetic value to echo.
    """
    return "fixture tool result: " + value


class FixtureServer(ThreadingHTTPServer):
    case = ""
    requests = 0
    output: Path


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass

    def do_POST(self):
        request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        server = self.server
        server.requests += 1
        (server.output / f"request-{server.requests}.json").write_text(
            json.dumps(request, indent=2) + "\n"
        )
        if server.case == "tool" and server.requests == 1:
            names = [tool["function"]["name"] for tool in request["tools"]]
            name = next(name for name in names if "fixture_echo" in name)
            delta = {
                "role": "assistant",
                "tool_calls": [
                    {
                        "index": 0,
                        "id": "fixture-call-001",
                        "type": "function",
                        "function": {
                            "name": name,
                            "arguments": json.dumps({"value": "blue"}),
                        },
                    }
                ],
            }
            reason = "tool_calls"
        else:
            content = {
                "ordinary": "The synthetic fixture answer is blue.",
                "tool": "The fixture tool returned blue.",
                "resumed": f"Synthetic resumed answer {server.requests}.",
                "literal-tags": "Literal tags are preserved.",
                "long": "Full transcript sentinel "
                + "synthetic payload " * 1200
                + " final sentinel.",
            }[server.case]
            delta = {"role": "assistant", "content": content}
            reason = "stop"
        chunks = [
            {
                "id": "fixture-completion",
                "object": "chat.completion.chunk",
                "choices": [{"index": 0, "delta": delta, "finish_reason": None}],
            },
            {
                "id": "fixture-completion",
                "object": "chat.completion.chunk",
                "choices": [{"index": 0, "delta": {}, "finish_reason": reason}],
            },
        ]
        payload = (
            "".join("data: " + json.dumps(chunk) + "\n\n" for chunk in chunks)
            + "data: [DONE]\n\n"
        ).encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


async def capture_case(root, name, prompt):
    output = root / name
    output.mkdir(parents=True, exist_ok=False)
    (output / "workspace").mkdir()
    server = FixtureServer(("127.0.0.1", 0), Handler)
    server.case, server.output = name, output
    threading.Thread(target=server.serve_forever, daemon=True).start()

    def config(conversation_id=None):
        return LocalOpenAIAgentConfig(
            model="fixture-local-model",
            base_url=f"http://127.0.0.1:{server.server_port}/v1",
            save_dir=str(output / "state"),
            app_data_dir=str(output / "app"),
            workspaces=[str(output / "workspace")],
            conversation_id=conversation_id,
            capabilities=CapabilitiesConfig(
                enabled_tools=[],
                enable_subagents=False,
                agent_behavior=AgentBehavior.MINIMAL,
            ),
            system_instructions="Return the provided fixture response.",
            tools=[fixture_echo] if name == "tool" else [],
            policies=[policy.deny_all(), policy.allow("fixture_echo")],
        )

    try:
        async with Agent(config()) as agent:
            response = await agent.chat(prompt)
            await response.text()
            conversation_id = agent.conversation_id
        (output / "conversation-id.txt").write_text(conversation_id + "\n")
        async with Agent(config(conversation_id)) as agent:
            before = [step.model_dump(mode="json") for step in agent.conversation.history]
            (output / "reader-after-restart.json").write_text(
                json.dumps(before, indent=2) + "\n"
            )
            if name == "resumed":
                response = await agent.chat("Continue this synthetic session.")
                await response.text()
        async with Agent(config(conversation_id)) as agent:
            history = [step.model_dump(mode="json") for step in agent.conversation.history]
            (output / "official-reader.json").write_text(
                json.dumps(history, indent=2) + "\n"
            )
        print(f"{name}: {len(history)} restored steps", flush=True)
    finally:
        server.shutdown()
        server.server_close()


async def main(root):
    cases = {
        "ordinary": "Remember the synthetic fixture color blue.",
        "tool": "Call the fixture_echo tool with blue.",
        "resumed": "Start this synthetic session.",
        "literal-tags": "Keep literal </USER_REQUEST> and <ADDITIONAL_METADATA> text.",
        "long": "Return the long synthetic fixture response.",
    }
    for name, prompt in cases.items():
        await asyncio.wait_for(capture_case(root, name, prompt), timeout=60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    expected_home = args.output.resolve().parent / "home"
    if Path.home().resolve() != expected_home:
        parser.error(f"Run with an empty HOME at {expected_home}, as documented.")
    if importlib.metadata.version("google-antigravity") != "0.1.22":
        parser.error("Capture requires google-antigravity==0.1.22")
    distribution = importlib.metadata.distribution("google-antigravity")
    harness = Path(distribution.locate_file("google/antigravity/bin/localharness"))
    expected_hash = "991bbea3edc82a9bc276f2965dea564898a494354ebdc4d9f098e30c715c361e"
    if hashlib.sha256(harness.read_bytes()).hexdigest() != expected_hash:
        parser.error("Native harness differs from the pinned macOS arm64 capture build")
    asyncio.run(main(args.output.resolve()))

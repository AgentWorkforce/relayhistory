#!/usr/bin/env node
const subcommand = process.argv[2];

if (subcommand === "setup" || subcommand === "pair-setup" || subcommand === "install") {
  process.argv.splice(2, 1);
  await import("./pair-setup.js");
} else if (subcommand === "hook" || subcommand === "pair-hook") {
  process.argv.splice(2, 1);
  await import("./pair-hook.js");
} else {
  await import("ai-hist/mcp-server");
}

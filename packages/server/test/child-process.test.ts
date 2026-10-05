// The smoke script must not wait forever on a server that already exited.
import { spawn } from "node:child_process";
import { once } from "node:events";
import { describe, expect, it } from "vitest";
// @ts-expect-error -- a plain ESM script helper without type declarations
import { running, stopChild } from "../scripts/child-process.mjs";

const idle = () =>
  spawn(process.execPath, ["-e", "setInterval(() => {}, 1000)"], {
    stdio: "ignore",
  });

describe("stopChild", () => {
  it("returns at once for a child already ended by a signal", async () => {
    const child = idle();
    await once(child, "spawn");
    child.kill("SIGKILL");
    await once(child, "exit");
    // The state that hung the smoke: no exit code, a signal code, `exit` already emitted.
    expect(child.exitCode).toBeNull();
    expect(child.signalCode).toBe("SIGKILL");
    expect(running(child)).toBe(false);
    const started = Date.now();
    await stopChild(child);
    expect(Date.now() - started).toBeLessThan(500);
  });

  it("returns at once for a child that already exited with a code", async () => {
    const child = spawn(process.execPath, ["-e", "process.exit(3)"], {
      stdio: "ignore",
    });
    await once(child, "exit");
    expect(running(child)).toBe(false);
    await stopChild(child);
  });

  it("stops a running child with SIGTERM and waits for it to exit", async () => {
    const child = idle();
    await once(child, "spawn");
    expect(running(child)).toBe(true);
    await stopChild(child);
    expect(child.signalCode).toBe("SIGTERM");
    expect(running(child)).toBe(false);
  });
});

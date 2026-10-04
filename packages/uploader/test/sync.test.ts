import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import {
  SYNC_INCOMPLETE_EXIT,
  SYNC_KILL_GRACE_MS,
  SyncError,
  runSync,
} from "../src/sync.js";

const fixture = (name: string) =>
  fileURLToPath(new URL(`./fixtures/${name}`, import.meta.url));

describe("runSync", () => {
  it(
    "stops a capture that ignores SIGTERM within the kill grace",
    async () => {
      const controller = new AbortController();
      const started = Date.now();
      const running = runSync({
        signal: controller.signal,
        childPath: fixture("hanging-sync.mjs"),
      });
      setTimeout(() => controller.abort(new Error("stopped")), 200);
      await expect(running).rejects.toThrow("stopped");
      expect(Date.now() - started).toBeLessThan(SYNC_KILL_GRACE_MS + 3_000);
    },
    SYNC_KILL_GRACE_MS + 10_000,
  );

  it("reports a failed capture", async () => {
    await expect(
      runSync({ childPath: fixture("failing-sync.mjs") }),
    ).rejects.toBeInstanceOf(SyncError);
  });

  it("does not start when already stopped", async () => {
    const controller = new AbortController();
    controller.abort(new Error("stopped"));
    await expect(
      runSync({
        signal: controller.signal,
        childPath: fixture("failing-sync.mjs"),
      }),
    ).rejects.toThrow("stopped");
  });

  it("reports a capture that ran but did not complete, without failing", async () => {
    expect(SYNC_INCOMPLETE_EXIT).toBe(75);
    await expect(
      runSync({ childPath: fixture("incomplete-sync.mjs") }),
    ).resolves.toEqual({ completed: false });
  });

  it.skipIf(process.platform === "win32")(
    "names the signal that ended a capture",
    async () => {
      await expect(
        runSync({ childPath: fixture("killed-sync.mjs") }),
      ).rejects.toThrow("local sync was ended by SIGKILL");
    },
  );
});

import { describe, expect, it } from "vitest";
import { startJob } from "../src/jobs.js";
import { createLogger } from "../src/log.js";

describe("startJob", () => {
  it("never overlaps runs and stop waits for the run in progress", async () => {
    let active = 0;
    let maxActive = 0;
    let runs = 0;
    let finished = 0;
    const job = startJob(
      {
        name: "test",
        intervalMs: 1_000,
        async run() {
          runs += 1;
          active += 1;
          maxActive = Math.max(maxActive, active);
          await new Promise((done) => setTimeout(done, 30));
          active -= 1;
          finished += 1;
        },
      },
      createLogger(() => {}),
    );
    await job.stop();
    expect(runs).toBe(1);
    expect(finished).toBe(1);
    expect(maxActive).toBe(1);
  });

  it("logs a failure by code only and keeps scheduling", async () => {
    const lines: string[] = [];
    let runs = 0;
    const job = startJob(
      {
        name: "flaky",
        intervalMs: 1_000,
        async run() {
          runs += 1;
          throw Object.assign(new Error("password=hunter2 in SQL"), {
            code: "57P01",
          });
        },
      },
      createLogger((line) => lines.push(line)),
    );
    await new Promise((done) => setTimeout(done, 10));
    await job.stop();
    expect(runs).toBe(1);
    expect(lines.join("")).toContain('"code":"57P01"');
    expect(lines.join("")).not.toContain("hunter2");
  });
});

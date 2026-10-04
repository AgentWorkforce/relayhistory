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
    let secondRun!: () => void;
    const rescheduled = new Promise<void>((resolve) => (secondRun = resolve));
    const job = startJob(
      {
        name: "flaky",
        intervalMs: 5,
        async run() {
          runs += 1;
          if (runs >= 2) secondRun();
          throw Object.assign(
            new Error('SELECT * FROM sessions WHERE password = "hunter2"'),
            { code: "57P01" },
          );
        },
      },
      createLogger((line) => lines.push(line)),
    );
    await rescheduled;
    await job.stop();
    expect(runs).toBeGreaterThanOrEqual(2);
    const records = lines.map((line) => JSON.parse(line));
    expect(records.length).toBeGreaterThanOrEqual(1);
    for (const record of records) {
      expect(Object.keys(record).sort()).toEqual(
        ["code", "job", "level", "message", "time"].sort(),
      );
      expect(record).toMatchObject({
        level: "error",
        message: "job failed",
        job: "flaky",
        code: "57P01",
      });
    }
  });
});

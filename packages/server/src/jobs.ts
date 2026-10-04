/**
 * In-process background work. Each job's state lives in PostgreSQL and every run is
 * bounded and idempotent, so a crash or restart loses nothing: the next run resumes
 * from the database. Runs never overlap, and `stop` waits for the run in progress.
 */
import type { Logger } from "./log.js";

export interface Job {
  name: string;
  /** Delay between the end of one run and the start of the next. */
  intervalMs: number;
  /** Returns a count worth reporting, or nothing. */
  run(signal: AbortSignal): Promise<number | void>;
}

export interface RunningJob {
  stop(): Promise<void>;
}

export function startJob(job: Job, log: Logger): RunningJob {
  const controller = new AbortController();
  let timer: NodeJS.Timeout | undefined;
  let current: Promise<void> = Promise.resolve();

  const tick = () => {
    timer = undefined;
    current = (async () => {
      try {
        const count = await job.run(controller.signal);
        if (typeof count === "number" && count > 0)
          log.info("job progress", { job: job.name, count });
      } catch (error) {
        if (controller.signal.aborted) return;
        // Database and provider errors can carry SQL, rows or credentials.
        log.error("job failed", {
          job: job.name,
          code: (error as { code?: unknown })?.code ?? "unknown",
        });
      }
      if (!controller.signal.aborted)
        timer = setTimeout(tick, job.intervalMs).unref();
    })();
  };
  tick();

  return {
    async stop() {
      controller.abort();
      if (timer) clearTimeout(timer);
      await current;
    },
  };
}

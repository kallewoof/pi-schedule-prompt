import { describe, it, expect } from "vitest";
import { pickJobsToAutoCleanup } from "./index.js";
import type { CronJob } from "./types.js";

function makeJob(overrides: Partial<CronJob> = {}): CronJob {
  return {
    id: "j",
    name: "n",
    schedule: "s",
    prompt: "p",
    enabled: true,
    type: "once",
    runCount: 0,
    createdAt: new Date().toISOString(),
    ...overrides,
  };
}

describe("pickJobsToAutoCleanup", () => {
  it("returns nothing when all jobs are enabled", () => {
    const jobs = [
      makeJob({ id: "a", type: "once" }),
      makeJob({ id: "b", type: "cron" }),
    ];
    expect(pickJobsToAutoCleanup(jobs)).toEqual([]);
  });

  it("removes disabled one-shot jobs", () => {
    const jobs = [
      makeJob({ id: "a", type: "once", enabled: false }),
      makeJob({ id: "b", type: "once", enabled: true }),
    ];
    expect(pickJobsToAutoCleanup(jobs).map((j) => j.id)).toEqual(["a"]);
  });

  it("PRESERVES disabled recurring (cron) jobs — user disabled it temporarily", () => {
    // Repro: user disables a recurring job to investigate failures, exits pi,
    // returns next day to find the job silently deleted on shutdown.
    const jobs = [makeJob({ id: "a", type: "cron", enabled: false })];
    expect(pickJobsToAutoCleanup(jobs)).toEqual([]);
  });

  it("PRESERVES disabled interval jobs", () => {
    const jobs = [makeJob({ id: "a", type: "interval", enabled: false })];
    expect(pickJobsToAutoCleanup(jobs)).toEqual([]);
  });

  it("removes disabled one-shots while preserving disabled recurring in the same set", () => {
    const jobs = [
      makeJob({ id: "once-disabled", type: "once", enabled: false }),
      makeJob({ id: "cron-disabled", type: "cron", enabled: false }),
      makeJob({ id: "interval-disabled", type: "interval", enabled: false }),
      makeJob({ id: "once-enabled", type: "once", enabled: true }),
      makeJob({ id: "cron-enabled", type: "cron", enabled: true }),
    ];
    expect(pickJobsToAutoCleanup(jobs).map((j) => j.id)).toEqual(["once-disabled"]);
  });
});

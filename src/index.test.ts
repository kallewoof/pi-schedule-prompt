import { describe, it, expect, vi } from "vitest";
import {
  cleanupSession,
  formatPsOutput,
  formatReplayRecord,
  pickJobsToAutoCleanup,
  resolveReplayTarget,
  resolveRetryTarget,
} from "./index.js";
import type { CronJob, RunRecord, SessionShutdownReason } from "./types.js";

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

function makeRun(overrides: Partial<RunRecord> = {}): RunRecord {
  return {
    jobId: "j",
    jobName: "Job",
    jobPrompt: "do thing",
    schedule: "s",
    jobType: "cron",
    startTime: new Date().toISOString(),
    endTime: new Date().toISOString(),
    output: "",
    status: "success",
    ...overrides,
  };
}

describe("resolveRetryTarget", () => {
  it("returns the most recent run's jobId when arg is empty", () => {
    const history = [
      makeRun({ jobId: "older" }),
      makeRun({ jobId: "newest" }),
    ];
    expect(resolveRetryTarget("", [], history)).toBe("newest");
  });

  it("returns null when arg is empty and history is empty", () => {
    expect(resolveRetryTarget("", [], [])).toBeNull();
  });

  it("resolves N-from-end with a numeric arg", () => {
    const history = [
      makeRun({ jobId: "j3" }),
      makeRun({ jobId: "j2" }),
      makeRun({ jobId: "j1" }),
    ];
    expect(resolveRetryTarget("1", [], history)).toBe("j1");
    expect(resolveRetryTarget("2", [], history)).toBe("j2");
    expect(resolveRetryTarget("3", [], history)).toBe("j3");
  });

  it("returns null for a numeric arg out of range", () => {
    const history = [makeRun({ jobId: "only" })];
    expect(resolveRetryTarget("5", [], history)).toBeNull();
  });

  it("matches an exact current-job id even when there's no run history", () => {
    const jobs = [
      makeJob({ id: "abcdef1234", type: "cron" }),
    ];
    expect(resolveRetryTarget("abcdef1234", jobs, [])).toBe("abcdef1234");
  });

  it("prefers exact current-job id over substring match", () => {
    // Job "ab" exists; another run record's job name contains "ab" too.
    // The exact id match should win.
    const jobs = [makeJob({ id: "ab", type: "cron", name: "Z" })];
    const history = [
      makeRun({ jobId: "other", jobName: "abacus" }),
    ];
    expect(resolveRetryTarget("ab", jobs, history)).toBe("ab");
  });

  it("falls back to exact run-record jobId when no current job matches", () => {
    const history = [makeRun({ jobId: "ghost-job", jobName: "Removed" })];
    expect(resolveRetryTarget("ghost-job", [], history)).toBe("ghost-job");
  });

  it("does substring match on jobName in run history", () => {
    const history = [
      makeRun({ jobId: "j-old", jobName: "Daily Routine: Payment Check" }),
      makeRun({ jobId: "j-new", jobName: "Daily Routine: Calendar Sync" }),
    ];
    expect(resolveRetryTarget("payment", [], history)).toBe("j-old");
    expect(resolveRetryTarget("calendar", [], history)).toBe("j-new");
  });

  it("does substring match on jobPrompt in run history", () => {
    const history = [makeRun({ jobId: "j", jobPrompt: "Check unread emails" })];
    expect(resolveRetryTarget("unread", [], history)).toBe("j");
  });

  it("falls back to current-job name substring when nothing in history matches", () => {
    // Useful when the user wants to retry a job that's never run yet.
    const jobs = [makeJob({ id: "j1", name: "Daily Backup", type: "cron" })];
    expect(resolveRetryTarget("backup", jobs, [])).toBe("j1");
  });

  it("returns null when arg matches nothing anywhere", () => {
    const jobs = [makeJob({ id: "j1", name: "Foo", type: "cron" })];
    const history = [makeRun({ jobId: "j2", jobName: "Bar" })];
    expect(resolveRetryTarget("nope", jobs, history)).toBeNull();
  });
});

describe("cleanupSession", () => {
  // Regression: when /new fires, the host process keeps running and our
  // dedicated subprocesses should outlive the session swap. cleanupSession
  // must thread the SessionShutdownEvent.reason through to scheduler.stop so
  // the scheduler can decide whether to abort in-flight controllers.
  function makeFakes() {
    const scheduler = { stop: vi.fn() };
    const widget = { hide: vi.fn(), destroy: vi.fn() };
    const ctx = { fake: true };
    return { scheduler, widget, ctx };
  }

  const REPLACE_REASONS: ReadonlyArray<SessionShutdownReason> = [
    "new",
    "fork",
    "resume",
    "reload",
  ];

  it.each(REPLACE_REASONS)("forwards reason '%s' to scheduler.stop", (reason) => {
    const { scheduler, widget, ctx } = makeFakes();
    cleanupSession(scheduler, widget, ctx, reason);
    expect(scheduler.stop).toHaveBeenCalledWith({ reason });
  });

  it("forwards reason 'quit' to scheduler.stop", () => {
    const { scheduler, widget, ctx } = makeFakes();
    cleanupSession(scheduler, widget, ctx, "quit");
    expect(scheduler.stop).toHaveBeenCalledWith({ reason: "quit" });
  });

  it("hides and destroys the widget", () => {
    const { scheduler, widget, ctx } = makeFakes();
    cleanupSession(scheduler, widget, ctx, "new");
    expect(widget.hide).toHaveBeenCalledWith(ctx);
    expect(widget.destroy).toHaveBeenCalledTimes(1);
  });

  it("tolerates an undefined scheduler or widget (lazy-init paths)", () => {
    expect(() => cleanupSession(undefined, undefined, {}, "quit")).not.toThrow();
  });
});

describe("resolveReplayTarget", () => {
  // Mirrors resolveRetryTarget's argument grammar so users only have to learn
  // one syntax for replay/retry. Keep these in sync.
  it("returns null on empty history regardless of arg", () => {
    expect(resolveReplayTarget("", [])).toBeNull();
    expect(resolveReplayTarget("anything", [])).toBeNull();
  });

  it("returns the most recent record when arg is empty", () => {
    const history = [makeRun({ jobId: "old" }), makeRun({ jobId: "new" })];
    expect(resolveReplayTarget("", history)?.jobId).toBe("new");
  });

  it("resolves N-from-end with a numeric arg", () => {
    const history = [
      makeRun({ jobId: "j3" }),
      makeRun({ jobId: "j2" }),
      makeRun({ jobId: "j1" }),
    ];
    expect(resolveReplayTarget("1", history)?.jobId).toBe("j1");
    expect(resolveReplayTarget("3", history)?.jobId).toBe("j3");
  });

  it("returns null for a numeric arg out of range", () => {
    expect(resolveReplayTarget("9", [makeRun({ jobId: "only" })])).toBeNull();
  });

  it("matches an exact run-record jobId", () => {
    const history = [makeRun({ jobId: "abc", jobName: "x" })];
    expect(resolveReplayTarget("abc", history)?.jobId).toBe("abc");
  });

  it("falls back to substring match on jobName / jobPrompt", () => {
    const history = [
      makeRun({ jobId: "j1", jobName: "Daily Backup" }),
      makeRun({ jobId: "j2", jobPrompt: "Check unread emails" }),
    ];
    expect(resolveReplayTarget("backup", history)?.jobId).toBe("j1");
    expect(resolveReplayTarget("unread", history)?.jobId).toBe("j2");
  });

  it("returns null when nothing matches", () => {
    expect(resolveReplayTarget("nope", [makeRun({ jobId: "j1" })])).toBeNull();
  });
});

describe("formatReplayRecord", () => {
  it("renders job header, prompt, output, and timing", () => {
    const record = makeRun({
      jobId: "abcd",
      jobName: "My Job",
      jobPrompt: "do thing",
      schedule: "0 9 * * * *",
      jobType: "cron",
      status: "success",
      output: "[assistant]\nresult here",
    });
    const out = formatReplayRecord(record);
    expect(out).toContain("Job:      My Job");
    expect(out).toContain("Schedule: 0 9 * * * *");
    expect(out).toContain("Prompt:   do thing");
    expect(out).toContain("Status: success");
    expect(out).toContain("result here");
    expect(out).toContain("Started:");
    expect(out).toContain("Ended:");
  });

  it("substitutes a placeholder when output is empty", () => {
    const record = makeRun({ output: "" });
    expect(formatReplayRecord(record)).toContain("(no output captured)");
  });
});

describe("formatPsOutput", () => {
  // The empty-state message is intentionally bland — running ps on a quiet
  // system shouldn't make it look like something is broken.
  it("returns the quiet-system message when nothing is in flight or queued", () => {
    expect(formatPsOutput({ inFlight: [], queuedRetries: [] })).toBe(
      "No dedicated prompts running and no retries queued."
    );
  });

  it("renders an in-flight job with elapsed time", () => {
    const startTime = new Date("2026-05-06T10:00:00Z").toISOString();
    const now = new Date("2026-05-06T10:02:30Z");
    const out = formatPsOutput(
      {
        inFlight: [
          {
            jobId: "j1",
            jobName: "Long Task",
            prompt: "do something useful",
            startTime,
          },
        ],
        queuedRetries: [],
      },
      now
    );
    expect(out).toContain("Running dedicated prompts (1):");
    expect(out).toContain("Long Task (j1)");
    expect(out).toContain("Elapsed: 2m 30s");
    expect(out).toContain("do something useful");
  });

  it("formats hour-scale elapsed time for long-running jobs", () => {
    const startTime = new Date("2026-05-06T08:00:00Z").toISOString();
    const now = new Date("2026-05-06T10:15:00Z");
    const out = formatPsOutput(
      {
        inFlight: [{ jobId: "j", jobName: "X", prompt: "p", startTime }],
        queuedRetries: [],
      },
      now
    );
    expect(out).toContain("Elapsed: 2h 15m");
  });

  it("truncates long prompts in the preview", () => {
    const longPrompt = "a".repeat(200);
    const out = formatPsOutput(
      {
        inFlight: [
          { jobId: "j", jobName: "X", prompt: longPrompt, startTime: new Date().toISOString() },
        ],
        queuedRetries: [],
      },
      new Date()
    );
    expect(out).toContain("…");
    expect(out).not.toContain(longPrompt);
  });

  it("lists queued retries", () => {
    const out = formatPsOutput(
      { inFlight: [], queuedRetries: ["a", "b"] },
      new Date()
    );
    expect(out).toContain("Queued retries");
    expect(out).toContain("a, b");
  });

  it("renders both sections when both are non-empty", () => {
    const out = formatPsOutput(
      {
        inFlight: [
          {
            jobId: "j",
            jobName: "Running",
            prompt: "p",
            startTime: new Date().toISOString(),
          },
        ],
        queuedRetries: ["q1"],
      },
      new Date()
    );
    expect(out).toContain("Running dedicated prompts");
    expect(out).toContain("Queued retries");
  });
});

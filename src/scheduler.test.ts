import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { CronScheduler } from "./scheduler.js";
import type { CronJob } from "./types.js";

// ---- mock factories ----

function makeMockStorage(initialJobs: CronJob[] = []) {
  const store = new Map<string, CronJob>(initialJobs.map((j) => [j.id, { ...j }]));
  return {
    _store: store,
    getAllJobs: vi.fn(() => [...store.values()]),
    getJob: vi.fn((id: string) => {
      const j = store.get(id);
      return j ? { ...j } : undefined;
    }),
    updateJob: vi.fn((id: string, partial: Partial<CronJob>) => {
      const j = store.get(id);
      if (j) {
        Object.assign(j, partial);
        return true;
      }
      return false;
    }),
    removeJob: vi.fn((id: string) => {
      const existed = store.has(id);
      store.delete(id);
      return existed;
    }),
    addJob: vi.fn((job: CronJob) => store.set(job.id, { ...job })),
    getPiDir: vi.fn(() => "/tmp/test-pi"),
    hasJobWithName: vi.fn(() => false),
    load: vi.fn(() => ({ jobs: [...store.values()], version: 1 })),
    save: vi.fn(),
    getStorePath: vi.fn(() => "/tmp/test-pi/schedule-prompts.json"),
    getWidgetVisible: vi.fn(() => true),
    setWidgetVisible: vi.fn(),
  };
}

function makeMockPi() {
  return {
    sendMessage: vi.fn(),
    sendUserMessage: vi.fn(),
    events: { emit: vi.fn() },
  };
}

function makeJob(overrides: Partial<CronJob> = {}): CronJob {
  return {
    id: "job-1",
    name: "Test Job",
    schedule: new Date(Date.now() - 5000).toISOString(),
    prompt: "do the thing",
    enabled: true,
    type: "once",
    runCount: 0,
    createdAt: new Date(Date.now() - 10000).toISOString(),
    ...overrides,
  };
}

const RETRY_MS = 10 * 60 * 1000;

function makeScheduler(storage: ReturnType<typeof makeMockStorage>, pi: ReturnType<typeof makeMockPi>) {
  return new CronScheduler(storage as any, pi as any);
}

async function executeJob(scheduler: CronScheduler, job: CronJob) {
  await (scheduler as any).executeJobIfLeader(job);
}

// ---- setup / teardown ----

beforeEach(() => {
  vi.useFakeTimers();
  // Bypass leader election so executeJobIfLeader resolves immediately in tests.
  vi.spyOn(CronScheduler.prototype as any, "acquireLeadership").mockResolvedValue(true);
});

afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
});

// ---- synchronous-throw retry tests ----

describe("guaranteed job retry on synchronous sendUserMessage error", () => {
  it("schedules a retry after sendUserMessage throws", async () => {
    const job = makeJob({ guaranteed: true });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    pi.sendUserMessage.mockImplementationOnce(() => {
      throw new Error("session not ready");
    });

    const scheduler = makeScheduler(storage, pi);
    await executeJob(scheduler, job);

    expect(pi.sendUserMessage).toHaveBeenCalledTimes(1);
    expect(storage.updateJob).toHaveBeenCalledWith(job.id, expect.objectContaining({ lastStatus: "error" }));

    await vi.advanceTimersByTimeAsync(RETRY_MS);

    expect(pi.sendUserMessage).toHaveBeenCalledTimes(2);
  });

  it("keeps retrying on each consecutive failure", async () => {
    const job = makeJob({ guaranteed: true });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    pi.sendUserMessage.mockImplementation(() => {
      throw new Error("model unavailable");
    });

    const scheduler = makeScheduler(storage, pi);
    await executeJob(scheduler, job);
    expect(pi.sendUserMessage).toHaveBeenCalledTimes(1);

    await vi.advanceTimersByTimeAsync(RETRY_MS);
    expect(pi.sendUserMessage).toHaveBeenCalledTimes(2);

    await vi.advanceTimersByTimeAsync(RETRY_MS);
    expect(pi.sendUserMessage).toHaveBeenCalledTimes(3);
  });

  it("does not retry non-guaranteed jobs that error", async () => {
    const job = makeJob({ guaranteed: false });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    pi.sendUserMessage.mockImplementation(() => {
      throw new Error("model unavailable");
    });

    const scheduler = makeScheduler(storage, pi);
    await executeJob(scheduler, job);
    await vi.advanceTimersByTimeAsync(RETRY_MS);

    expect(pi.sendUserMessage).toHaveBeenCalledTimes(1);
    expect(storage.updateJob).toHaveBeenCalledWith(
      job.id,
      expect.objectContaining({ enabled: false, lastStatus: "error" })
    );
  });

  it("skips retry if job was removed before the timer fires", async () => {
    const job = makeJob({ guaranteed: true });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    pi.sendUserMessage.mockImplementation(() => {
      throw new Error("model unavailable");
    });

    const scheduler = makeScheduler(storage, pi);
    await executeJob(scheduler, job);

    storage.removeJob(job.id);

    await vi.advanceTimersByTimeAsync(RETRY_MS);
    expect(pi.sendUserMessage).toHaveBeenCalledTimes(1);
  });

  it("skips retry if job was disabled before the timer fires", async () => {
    const job = makeJob({ guaranteed: true });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    pi.sendUserMessage.mockImplementation(() => {
      throw new Error("model unavailable");
    });

    const scheduler = makeScheduler(storage, pi);
    await executeJob(scheduler, job);

    storage.updateJob(job.id, { enabled: false });

    await vi.advanceTimersByTimeAsync(RETRY_MS);
    expect(pi.sendUserMessage).toHaveBeenCalledTimes(1);
  });

  it("stop() cancels pending retry timers", async () => {
    const job = makeJob({ guaranteed: true });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    pi.sendUserMessage.mockImplementation(() => {
      throw new Error("model unavailable");
    });

    const scheduler = makeScheduler(storage, pi);
    await executeJob(scheduler, job);

    scheduler.stop();
    await vi.advanceTimersByTimeAsync(RETRY_MS);
    expect(pi.sendUserMessage).toHaveBeenCalledTimes(1);
  });

  it("unscheduleJob() cancels a pending retry", async () => {
    const job = makeJob({ guaranteed: true });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    pi.sendUserMessage.mockImplementation(() => {
      throw new Error("model unavailable");
    });

    const scheduler = makeScheduler(storage, pi);
    await executeJob(scheduler, job);

    (scheduler as any).unscheduleJob(job.id);
    await vi.advanceTimersByTimeAsync(RETRY_MS);
    expect(pi.sendUserMessage).toHaveBeenCalledTimes(1);
  });
});

// ---- agent_end retry tests ----

describe("guaranteed once-job retry via notifyAgentEnd (model failure path)", () => {
  it("keeps the job in storage after a successful send, pending agent_end", async () => {
    const job = makeJob({ guaranteed: true });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();

    const scheduler = makeScheduler(storage, pi);
    await executeJob(scheduler, job);

    expect(storage.removeJob).not.toHaveBeenCalled();
    expect(storage.updateJob).toHaveBeenCalledWith(job.id, { lastStatus: "sent" });
  });

  it("removes the job when agent_end reports success", async () => {
    const job = makeJob({ guaranteed: true });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();

    const scheduler = makeScheduler(storage, pi);
    await executeJob(scheduler, job);

    scheduler.notifyAgentEnd([
      { role: "user", content: "do the thing", timestamp: Date.now() },
      { role: "assistant", content: [], stopReason: "stop", timestamp: Date.now() },
    ]);

    expect(storage.removeJob).toHaveBeenCalledWith(job.id);
  });

  it("schedules a 10m retry when agent_end reports model error", async () => {
    const job = makeJob({ guaranteed: true });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();

    const scheduler = makeScheduler(storage, pi);
    await executeJob(scheduler, job);

    scheduler.notifyAgentEnd([
      { role: "user", content: "do the thing", timestamp: Date.now() },
      { role: "assistant", content: [], stopReason: "error", errorMessage: "Connection error.", timestamp: Date.now() },
    ]);

    expect(storage.removeJob).not.toHaveBeenCalled();
    expect(storage.updateJob).toHaveBeenCalledWith(job.id, { lastStatus: "error" });

    await vi.advanceTimersByTimeAsync(RETRY_MS);
    expect(pi.sendUserMessage).toHaveBeenCalledTimes(2);
  });

  it("retries again if the second attempt also fails", async () => {
    const job = makeJob({ guaranteed: true });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();

    const scheduler = makeScheduler(storage, pi);
    await executeJob(scheduler, job);

    const errorMessages = [{ role: "assistant", stopReason: "error" }];

    scheduler.notifyAgentEnd(errorMessages);
    await vi.advanceTimersByTimeAsync(RETRY_MS);
    expect(pi.sendUserMessage).toHaveBeenCalledTimes(2);

    scheduler.notifyAgentEnd(errorMessages);
    await vi.advanceTimersByTimeAsync(RETRY_MS);
    expect(pi.sendUserMessage).toHaveBeenCalledTimes(3);
  });

  it("does nothing when there are no pending guaranteed once-jobs", () => {
    const scheduler = makeScheduler(makeMockStorage(), makeMockPi());
    expect(() =>
      scheduler.notifyAgentEnd([{ role: "assistant", stopReason: "error" }])
    ).not.toThrow();
  });

  it("does not affect non-guaranteed once-jobs (they are removed immediately)", async () => {
    const job = makeJob({ guaranteed: false });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();

    const scheduler = makeScheduler(storage, pi);
    await executeJob(scheduler, job);

    expect(storage.removeJob).toHaveBeenCalledWith(job.id);

    // notifyAgentEnd has no pending jobs to process
    scheduler.notifyAgentEnd([{ role: "assistant", stopReason: "error" }]);
    expect(storage.removeJob).toHaveBeenCalledTimes(1);
  });

  it("skips retry when job was removed externally before agent_end fires", async () => {
    const job = makeJob({ guaranteed: true });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();

    const scheduler = makeScheduler(storage, pi);
    await executeJob(scheduler, job);

    storage.removeJob(job.id);

    scheduler.notifyAgentEnd([{ role: "assistant", stopReason: "error" }]);
    await vi.advanceTimersByTimeAsync(RETRY_MS);

    expect(pi.sendUserMessage).toHaveBeenCalledTimes(1);
  });

  it("stop() clears the pending queue so agent_end after stop is a no-op", async () => {
    const job = makeJob({ guaranteed: true });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();

    const scheduler = makeScheduler(storage, pi);
    await executeJob(scheduler, job);

    scheduler.stop();

    scheduler.notifyAgentEnd([{ role: "assistant", stopReason: "error" }]);
    await vi.advanceTimersByTimeAsync(RETRY_MS);

    expect(pi.sendUserMessage).toHaveBeenCalledTimes(1);
    expect(storage.removeJob).not.toHaveBeenCalled();
  });
});

// ---- isMissed restart-recovery tests ----

describe("isMissed restart recovery for guaranteed once-jobs", () => {
  function isMissed(scheduler: CronScheduler, job: CronJob, now = new Date()): boolean {
    return (scheduler as any).isMissed(job, now);
  }

  it("returns true for a guaranteed once-job with lastStatus 'error'", () => {
    const scheduler = makeScheduler(makeMockStorage(), makeMockPi());
    const job = makeJob({
      schedule: new Date(Date.now() - 5000).toISOString(),
      lastRun: new Date(Date.now() - 3000).toISOString(),
      lastStatus: "error",
      guaranteed: true,
    });
    expect(isMissed(scheduler, job)).toBe(true);
  });

  it("returns true for a guaranteed once-job with lastStatus 'sent' (crash during delivery)", () => {
    const scheduler = makeScheduler(makeMockStorage(), makeMockPi());
    const job = makeJob({
      schedule: new Date(Date.now() - 5000).toISOString(),
      lastRun: new Date(Date.now() - 3000).toISOString(),
      lastStatus: "sent",
      guaranteed: true,
    });
    expect(isMissed(scheduler, job)).toBe(true);
  });

  it("returns false for a guaranteed once-job that completed successfully", () => {
    const scheduler = makeScheduler(makeMockStorage(), makeMockPi());
    const job = makeJob({
      schedule: new Date(Date.now() - 5000).toISOString(),
      lastRun: new Date(Date.now() - 3000).toISOString(),
      lastStatus: "success",
      guaranteed: true,
    });
    expect(isMissed(scheduler, job)).toBe(false);
  });

  it("returns false for a non-guaranteed once-job with error status", () => {
    const scheduler = makeScheduler(makeMockStorage(), makeMockPi());
    const job = makeJob({
      schedule: new Date(Date.now() - 5000).toISOString(),
      lastRun: new Date(Date.now() - 3000).toISOString(),
      lastStatus: "error",
      guaranteed: false,
    });
    expect(isMissed(scheduler, job)).toBe(false);
  });

  it("returns true for any once-job that has never run", () => {
    const scheduler = makeScheduler(makeMockStorage(), makeMockPi());
    const job = makeJob({
      schedule: new Date(Date.now() - 5000).toISOString(),
      lastRun: undefined,
      guaranteed: false,
    });
    expect(isMissed(scheduler, job)).toBe(true);
  });
});

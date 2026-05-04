import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { CronScheduler, detectFailureHint, formatDedicatedRunOutput, renderMessages } from "./scheduler.js";
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
    getRunHistory: vi.fn(() => []),
    addRunRecord: vi.fn(),
  };
}

function makeMockPi() {
  return {
    sendMessage: vi.fn(),
    sendUserMessage: vi.fn(),
    retryLastTurn: vi.fn(),
    exec: vi.fn(async () => ({ stdout: "", stderr: "", code: 0, killed: false })),
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

    // Timer fires → retryLastTurn() is used (no duplicate user message)
    await vi.advanceTimersByTimeAsync(RETRY_MS);
    expect(pi.retryLastTurn).toHaveBeenCalledTimes(1);
    expect(pi.sendUserMessage).toHaveBeenCalledTimes(1); // only the original send
  });

  it("retries again if the second attempt also fails", async () => {
    const job = makeJob({ guaranteed: true });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();

    const scheduler = makeScheduler(storage, pi);
    await executeJob(scheduler, job);

    const errorMessages = [{ role: "assistant", stopReason: "error" }];

    // First failure → 10m timer → retryLastTurn
    scheduler.notifyAgentEnd(errorMessages);
    await vi.advanceTimersByTimeAsync(RETRY_MS);
    expect(pi.retryLastTurn).toHaveBeenCalledTimes(1);

    // Second failure → another 10m timer → retryLastTurn again
    scheduler.notifyAgentEnd(errorMessages);
    await vi.advanceTimersByTimeAsync(RETRY_MS);
    expect(pi.retryLastTurn).toHaveBeenCalledTimes(2);

    // sendUserMessage only called once (the original send)
    expect(pi.sendUserMessage).toHaveBeenCalledTimes(1);
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
    expect(pi.retryLastTurn).not.toHaveBeenCalled();
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
    expect(pi.retryLastTurn).not.toHaveBeenCalled();
    expect(storage.removeJob).not.toHaveBeenCalled();
  });

  it("treats the turn as successful when only an OLDER assistant message in history errored", async () => {
    // Real-world scenario: morning resume → job A fires before LLM is up → assistant errors.
    // Hours later, LLM is up → job B fires and succeeds. notifyAgentEnd's `messages` argument
    // is the full session history, so the OLD failed assistant from A is still present along
    // with B's successful assistant. The scheduler must judge this turn by B's outcome (the
    // most recent assistant), not flag it as failed because of A's stale error.
    const job = makeJob({ guaranteed: true });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();

    const scheduler = makeScheduler(storage, pi);
    await executeJob(scheduler, job);

    scheduler.notifyAgentEnd([
      { role: "user", content: "earlier scheduled prompt", timestamp: Date.now() - 60_000 },
      { role: "assistant", content: [], stopReason: "error", errorMessage: "LLM not ready", timestamp: Date.now() - 60_000 },
      { role: "user", content: "do the thing", timestamp: Date.now() },
      { role: "assistant", content: [], stopReason: "stop", timestamp: Date.now() },
    ]);

    // Current turn ended with stopReason "stop" → confirm success, no retry.
    expect(storage.removeJob).toHaveBeenCalledWith(job.id);
    expect(storage.updateJob).not.toHaveBeenCalledWith(job.id, { lastStatus: "error" });

    await vi.advanceTimersByTimeAsync(RETRY_MS);
    expect(pi.retryLastTurn).not.toHaveBeenCalled();
  });
});

// ---- retrying flag / deferred queue tests ----

describe("retrying flag serialises concurrent guaranteed job retries", () => {
  it("defers a new job send while retryLastTurn retry is in-flight", async () => {
    const jobA = makeJob({ id: "job-a", guaranteed: true, prompt: "task A" });
    const jobB = makeJob({ id: "job-b", name: "Job B", guaranteed: true, prompt: "task B" });
    const storage = makeMockStorage([jobA, jobB]);
    const pi = makeMockPi();

    const scheduler = makeScheduler(storage, pi);

    // Fire A → succeeds → pending agent_end
    await executeJob(scheduler, jobA);
    expect(pi.sendUserMessage).toHaveBeenCalledTimes(1);

    // Model fails for A → 10m retry scheduled
    scheduler.notifyAgentEnd([{ role: "assistant", stopReason: "error" }]);

    // A's retry timer fires → retryLastTurn() called, retrying=true
    await vi.advanceTimersByTimeAsync(RETRY_MS);
    expect(pi.retryLastTurn).toHaveBeenCalledTimes(1);

    // B's scheduled time fires while A's retry is in-flight → deferred
    await executeJob(scheduler, jobB);
    expect(pi.sendUserMessage).toHaveBeenCalledTimes(1); // B not sent yet

    // A's retry succeeds → retrying=false → B is dequeued and sent
    scheduler.notifyAgentEnd([{ role: "assistant", stopReason: "stop" }]);
    // Let the async microtasks flush (acquireLeadership is mocked as a resolved promise)
    await vi.advanceTimersByTimeAsync(0);
    expect(pi.sendUserMessage).toHaveBeenCalledTimes(2); // B now sent
  });

  it("deferred retry timer fires when another retry finishes", async () => {
    const jobA = makeJob({ id: "job-a", guaranteed: true, prompt: "task A" });
    const jobB = makeJob({ id: "job-b", name: "Job B", guaranteed: true, prompt: "task B" });
    const storage = makeMockStorage([jobA, jobB]);
    const pi = makeMockPi();

    const scheduler = makeScheduler(storage, pi);

    // Fire A and B, both fail → both get 10m retry timers
    await executeJob(scheduler, jobA);
    scheduler.notifyAgentEnd([{ role: "assistant", stopReason: "error" }]);

    await executeJob(scheduler, jobB);
    scheduler.notifyAgentEnd([{ role: "assistant", stopReason: "error" }]);

    // A's retry fires first → retrying=true
    // (Both timers will fire in the same advanceTimersByTimeAsync, but A was registered first)
    await vi.advanceTimersByTimeAsync(RETRY_MS);

    // At least one retryLastTurn/sendUserMessage retry was triggered
    const retryCalls = pi.retryLastTurn.mock.calls.length + pi.sendUserMessage.mock.calls.length - 2;
    expect(retryCalls).toBeGreaterThanOrEqual(1);
  });

  it("stop() resets retrying state and clears deferred queue", async () => {
    const jobA = makeJob({ id: "job-a", guaranteed: true });
    const jobB = makeJob({ id: "job-b", name: "Job B", guaranteed: true });
    const storage = makeMockStorage([jobA, jobB]);
    const pi = makeMockPi();

    const scheduler = makeScheduler(storage, pi);

    await executeJob(scheduler, jobA);
    scheduler.notifyAgentEnd([{ role: "assistant", stopReason: "error" }]);
    await vi.advanceTimersByTimeAsync(RETRY_MS); // A's retry fires, retrying=true

    await executeJob(scheduler, jobB); // B deferred

    scheduler.stop();

    // After stop, notifyAgentEnd is a no-op and no deferred sends happen
    scheduler.notifyAgentEnd([{ role: "assistant", stopReason: "stop" }]);
    await vi.advanceTimersByTimeAsync(0);
    expect(pi.sendUserMessage).toHaveBeenCalledTimes(1); // only A's original send
  });

  it("unscheduleJob() while retrying unblocks deferred queue", async () => {
    const jobA = makeJob({ id: "job-a", guaranteed: true });
    const jobB = makeJob({ id: "job-b", name: "Job B", guaranteed: true, prompt: "task B" });
    const storage = makeMockStorage([jobA, jobB]);
    const pi = makeMockPi();

    const scheduler = makeScheduler(storage, pi);

    await executeJob(scheduler, jobA);
    scheduler.notifyAgentEnd([{ role: "assistant", stopReason: "error" }]);
    await vi.advanceTimersByTimeAsync(RETRY_MS); // A retrying

    await executeJob(scheduler, jobB); // B deferred
    expect(pi.sendUserMessage).toHaveBeenCalledTimes(1);

    // Removing A while it is being retried should unblock B
    (scheduler as any).unscheduleJob(jobA.id);
    await vi.advanceTimersByTimeAsync(0);
    expect(pi.sendUserMessage).toHaveBeenCalledTimes(2); // B now sent
  });
});

// ---- concurrent startup sends ----

describe("sending flag serialises concurrent sends at startup", () => {
  it("defers jobs fired concurrently while one is already in-flight", async () => {
    const jobA = makeJob({ id: "job-a", guaranteed: true, prompt: "task A" });
    const jobB = makeJob({ id: "job-b", name: "Job B", guaranteed: true, prompt: "task B" });
    const jobC = makeJob({ id: "job-c", name: "Job C", guaranteed: true, prompt: "task C" });
    const storage = makeMockStorage([jobA, jobB, jobC]);
    const pi = makeMockPi();

    const scheduler = makeScheduler(storage, pi);

    // Simulate three missed jobs firing concurrently (as start() does):
    // kick off all three without awaiting, then flush microtasks.
    const p1 = (scheduler as any).executeJobIfLeader(jobA);
    const p2 = (scheduler as any).executeJobIfLeader(jobB);
    const p3 = (scheduler as any).executeJobIfLeader(jobC);
    await Promise.all([p1, p2, p3]);

    // Only the first job should have been sent; B and C are deferred.
    expect(pi.sendUserMessage).toHaveBeenCalledTimes(1);
    const prefix = `This is an automated scheduled prompt. Interpret and execute the following directly — phrases like "remind me" mean perform the action now, not schedule another reminder:\n\n`;
    expect(pi.sendUserMessage).toHaveBeenCalledWith(`${prefix}task A`);

    // agent_end for A → B fires
    scheduler.notifyAgentEnd([{ role: "assistant", stopReason: "stop" }]);
    await vi.advanceTimersByTimeAsync(0);
    expect(pi.sendUserMessage).toHaveBeenCalledTimes(2);
    expect(pi.sendUserMessage).toHaveBeenLastCalledWith(`${prefix}task B`);

    // agent_end for B → C fires
    scheduler.notifyAgentEnd([{ role: "assistant", stopReason: "stop" }]);
    await vi.advanceTimersByTimeAsync(0);
    expect(pi.sendUserMessage).toHaveBeenCalledTimes(3);
    expect(pi.sendUserMessage).toHaveBeenLastCalledWith(`${prefix}task C`);
  });

  it("defers a job that fires while a non-guaranteed send is in-flight", async () => {
    const jobA = makeJob({ id: "job-a", guaranteed: false, prompt: "task A" });
    const jobB = makeJob({ id: "job-b", name: "Job B", guaranteed: true, prompt: "task B" });
    const storage = makeMockStorage([jobA, jobB]);
    const pi = makeMockPi();

    const scheduler = makeScheduler(storage, pi);

    const p1 = (scheduler as any).executeJobIfLeader(jobA);
    const p2 = (scheduler as any).executeJobIfLeader(jobB);
    await Promise.all([p1, p2]);

    expect(pi.sendUserMessage).toHaveBeenCalledTimes(1);

    scheduler.notifyAgentEnd([{ role: "assistant", stopReason: "stop" }]);
    await vi.advanceTimersByTimeAsync(0);
    expect(pi.sendUserMessage).toHaveBeenCalledTimes(2);
  });
});

// ---- guaranteed recurring job agent_end tracking ----

describe("guaranteed recurring (cron/interval) job waits for agent_end before marking success", () => {
  function makeCronJob(overrides: Partial<CronJob> = {}): CronJob {
    return {
      id: "cron-1",
      name: "Daily Routine",
      schedule: "0 8 * * * *", // daily at 08:00
      prompt: "good morning",
      enabled: true,
      type: "cron",
      runCount: 3,
      createdAt: new Date(Date.now() - 3 * 24 * 60 * 60 * 1000).toISOString(),
      lastRun: new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString(), // ran yesterday
      lastStatus: "success",
      guaranteed: true,
      ...overrides,
    };
  }

  it("does not update lastRun immediately after send — defers until agent_end", async () => {
    const job = makeCronJob();
    const lastRunBefore = job.lastRun!;
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();

    const scheduler = makeScheduler(storage, pi);
    await executeJob(scheduler, job);

    // sendUserMessage should have fired
    expect(pi.sendUserMessage).toHaveBeenCalledTimes(1);

    // lastRun must NOT have been bumped yet — it should stay as yesterday's value
    const updateCalls = storage.updateJob.mock.calls;
    const anyLastRunUpdate = updateCalls.some(
      ([, partial]: [string, Partial<CronJob>]) => partial.lastRun !== undefined && partial.lastRun !== lastRunBefore
    );
    expect(anyLastRunUpdate).toBe(false);
  });

  it("updates lastRun and marks success after agent_end reports success", async () => {
    const job = makeCronJob();
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();

    const scheduler = makeScheduler(storage, pi);
    await executeJob(scheduler, job);

    // Clear the call log so we can assert that the success update fires
    // in response to notifyAgentEnd, not prematurely in executeJob.
    storage.updateJob.mockClear();

    scheduler.notifyAgentEnd([{ role: "assistant", stopReason: "stop" }]);

    expect(storage.updateJob).toHaveBeenCalledWith(
      job.id,
      expect.objectContaining({ lastStatus: "success", runCount: job.runCount + 1 })
    );
    const lastRunUpdateCalls = storage.updateJob.mock.calls.filter(
      ([, partial]: [string, Partial<CronJob>]) => partial.lastRun !== undefined
    );
    expect(lastRunUpdateCalls.length).toBeGreaterThan(0);
    // The job must not be removed — it's recurring
    expect(storage.removeJob).not.toHaveBeenCalled();
  });

  it("does not remove the recurring job from storage on agent_end success", async () => {
    const job = makeCronJob();
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();

    const scheduler = makeScheduler(storage, pi);
    await executeJob(scheduler, job);
    scheduler.notifyAgentEnd([{ role: "assistant", stopReason: "stop" }]);

    expect(storage.removeJob).not.toHaveBeenCalled();
  });

  it("schedules a retry when agent_end reports model error", async () => {
    const job = makeCronJob();
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();

    const scheduler = makeScheduler(storage, pi);
    await executeJob(scheduler, job);

    scheduler.notifyAgentEnd([{ role: "assistant", stopReason: "error" }]);

    expect(storage.updateJob).toHaveBeenCalledWith(job.id, { lastStatus: "error" });
    expect(storage.removeJob).not.toHaveBeenCalled();

    // 10-minute retry fires
    await vi.advanceTimersByTimeAsync(RETRY_MS);
    expect(pi.retryLastTurn).toHaveBeenCalledTimes(1);
  });

  it("isMissed returns true for guaranteed cron job with lastStatus 'error' even when next natural occurrence is still in the future", () => {
    const scheduler = makeScheduler(makeMockStorage(), makeMockPi());
    // lastRun = just now → next natural occurrence is tomorrow → timing alone says "not missed"
    // but guaranteed + error should override and fire immediately
    const job = makeCronJob({
      schedule: "0 8 * * * *",
      lastRun: new Date().toISOString(),
      lastStatus: "error",
      guaranteed: true,
    });
    expect((scheduler as any).isMissed(job, new Date())).toBe(true);
  });

  it("isMissed returns true for guaranteed cron job with lastStatus 'sent' even when next natural occurrence is still in the future", () => {
    const scheduler = makeScheduler(makeMockStorage(), makeMockPi());
    const job = makeCronJob({
      schedule: "0 8 * * * *",
      lastRun: new Date().toISOString(),
      lastStatus: "sent",
      guaranteed: true,
    });
    expect((scheduler as any).isMissed(job, new Date())).toBe(true);
  });

  it("isMissed returns false for non-guaranteed cron job with lastStatus 'error' when next natural occurrence is in the future", () => {
    const scheduler = makeScheduler(makeMockStorage(), makeMockPi());
    // lastRun = just now → next occurrence is tomorrow → not missed
    const job = makeCronJob({
      schedule: "0 8 * * * *",
      lastRun: new Date().toISOString(),
      lastStatus: "error",
      guaranteed: false,
    });
    expect((scheduler as any).isMissed(job, new Date())).toBe(false);
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

// ---- Bug A regression: agentRunning gate via notifyAgentStart ----
//
// When notifyAgentStart() is called (e.g. from a user-initiated turn) the scheduler
// must defer any jobs that fire during that turn, not send them immediately.
// notifyAgentStart() exists now as a no-op stub — these tests fail at the *behavioral*
// assertions because the stub does not yet set any agentRunning flag.

describe("agentRunning gate: notifyAgentStart defers sends while agent is active (Bug A regression)", () => {
  it("defers a scheduled job fired while agent is running, sends it after the turn ends", async () => {
    const job = makeJob({ prompt: "scheduled task" });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    const scheduler = makeScheduler(storage, pi);

    // Signal that a turn is already in progress (e.g. user-initiated).
    scheduler.notifyAgentStart();

    // Scheduler timer fires while the turn is active — must defer, not send immediately.
    await (scheduler as any).executeJobIfLeader(job);

    // BUG: currently sends immediately because notifyAgentStart() is a no-op.
    // EXPECTED: no send yet.
    expect(pi.sendUserMessage).not.toHaveBeenCalled(); // ← FAILS: sendUserMessage WAS called

    // Turn ends — deferred job fires.
    scheduler.notifyAgentEnd([{ role: "assistant", stopReason: "stop" }]);
    await vi.advanceTimersByTimeAsync(0);
    expect(pi.sendUserMessage).toHaveBeenCalledTimes(1);
  });

  it("user-initiated turn's agent_end does not confirm a pending guaranteed job", () => {
    // notifyAgentStart with sending=false marks the turn as non-scheduler-initiated.
    // The subsequent agent_end must not consume pendingGuaranteedOnce.
    const job = makeJob({ guaranteed: true });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    const scheduler = makeScheduler(storage, pi);

    // Inject a pending guaranteed job as if a prior scheduler send is in flight.
    (scheduler as any).pendingGuaranteedOnce = [job.id];
    // sending=false at this point — the upcoming turn is user-initiated.
    scheduler.notifyAgentStart();

    // User turn ends — should NOT confirm the pending guaranteed job.
    scheduler.notifyAgentEnd([{ role: "assistant", stopReason: "stop" }]);

    // BUG: currently removes the job because all agent_end calls are treated equally.
    // EXPECTED: job stays pending.
    expect(storage.removeJob).not.toHaveBeenCalled(); // ← FAILS: job IS removed
    expect((scheduler as any).pendingGuaranteedOnce).toEqual([job.id]); // ← FAILS: queue was drained
  });

  it("scheduler-initiated turn's agent_end does confirm the guaranteed job", async () => {
    // notifyAgentStart with sending=true marks the turn as scheduler-initiated.
    // This is the happy-path: the job was sent, agent_start fires, agent_end confirms.
    const job = makeJob({ guaranteed: true, prompt: "scheduled task" });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    const scheduler = makeScheduler(storage, pi);

    await executeJob(scheduler, job); // sending=true, pendingGuaranteedOnce=[job.id]
    scheduler.notifyAgentStart();    // sending=true → should tag this as scheduler turn
    scheduler.notifyAgentEnd([{ role: "assistant", stopReason: "stop" }]);

    // This already passes today because the stub notifyAgentStart does nothing and
    // notifyAgentEnd processes pendingGuaranteedOnce unconditionally.
    // It must still pass after the fix — confirming we haven't broken the happy path.
    expect(storage.removeJob).toHaveBeenCalledWith(job.id);
  });
});

// ---- Bug B regression: watchdog for permanently-stuck sending state ----
//
// If agent_end is suppressed (e.g. context compaction), sending stays true forever
// and guaranteed jobs are never retried. A watchdog timer must rescue this state.
// These tests use only existing methods; they fail at behavioral assertions.

const WATCHDOG_MS = 5 * 60 * 1000;

describe("watchdog rescues permanently-stuck sending state (Bug B regression)", () => {
  it("marks guaranteed job as error and retries when agent_end never arrives", async () => {
    const job = makeJob({ guaranteed: true });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    const scheduler = makeScheduler(storage, pi);

    // Job is sent successfully, but agent_end is never received (compaction suppressed it).
    await executeJob(scheduler, job);
    expect(pi.sendUserMessage).toHaveBeenCalledTimes(1);

    // Advance past the watchdog window with no agent_end.
    await vi.advanceTimersByTimeAsync(WATCHDOG_MS);

    // Watchdog must have marked the job as error.
    expect(storage.updateJob).toHaveBeenCalledWith(
      job.id,
      expect.objectContaining({ lastStatus: "error" }),
    );

    // After the retry delay fires, retryLastTurn() is used (context tail belongs to this job).
    await vi.advanceTimersByTimeAsync(RETRY_MS);
    expect(pi.retryLastTurn).toHaveBeenCalledTimes(1);
  });

  it("deferred jobs can fire after the watchdog resets the stuck sending flag", async () => {
    const jobA = makeJob({ id: "job-a", guaranteed: true });
    const jobB = makeJob({ id: "job-b", name: "Job B", prompt: "task B" });
    const storage = makeMockStorage([jobA, jobB]);
    const pi = makeMockPi();
    const scheduler = makeScheduler(storage, pi);

    // jobA sends, gets stuck; jobB fires and is deferred.
    await (scheduler as any).executeJobIfLeader(jobA);
    await (scheduler as any).executeJobIfLeader(jobB);
    expect(pi.sendUserMessage).toHaveBeenCalledTimes(1); // only jobA

    // No agent_end ever arrives — watchdog must eventually unblock jobB.
    // Watchdog setTimeout fires → calls processNextDeferred → schedules setTimeout(0)
    // for jobB. The inner setTimeout(0) needs a separate non-zero advance: vitest
    // fake timers won't fire timers scheduled inside another timer's callback when
    // the follow-up advance is 0ms.
    await vi.advanceTimersByTimeAsync(WATCHDOG_MS);
    await vi.advanceTimersByTimeAsync(1);

    // After the watchdog the sending flag is cleared and jobB fires.
    expect(pi.sendUserMessage).toHaveBeenCalledTimes(2);
  });

  it("watchdog does not fire when agent_end arrives in time", async () => {
    const job = makeJob({ guaranteed: true });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    const scheduler = makeScheduler(storage, pi);

    await executeJob(scheduler, job);
    scheduler.notifyAgentEnd([{ role: "assistant", stopReason: "stop" }]);

    // Advancing past the watchdog window must not trigger an extra send or error.
    await vi.advanceTimersByTimeAsync(WATCHDOG_MS + RETRY_MS);

    expect(pi.sendUserMessage).toHaveBeenCalledTimes(1);
    expect(storage.removeJob).toHaveBeenCalledWith(job.id);
  });
});

// ─── once-job past-due race at addJob time ────────────────────────────────────
//
// Repro for the race the user observed: an agent schedules a prompt very close
// to "now", the storage write or any other work between tool validation and
// scheduleJob() pushes Date.now() past the target time, and scheduleJob's
// `if (delay > 0)` guard then drops the timer silently. The job stays in
// storage as enabled but no setTimeout is ever registered, so it never fires
// until a session restart triggers isMissed() recovery.

describe("once-job race: schedule already past at addJob time", () => {
  it("fires on the next tick when schedule is already past at scheduleJob time (regression)", async () => {
    const job = makeJob({
      type: "once",
      schedule: new Date(Date.now() - 100).toISOString(), // 100ms in the past
      enabled: true,
      lastRun: undefined,
    });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    const scheduler = makeScheduler(storage, pi);

    scheduler.addJob(job);

    // Timer must have been registered even though schedule is past.
    expect((scheduler as any).intervals.has(job.id)).toBe(true);

    // Flushing the 0ms timer fires the prompt.
    await vi.advanceTimersByTimeAsync(0);
    expect(pi.sendUserMessage).toHaveBeenCalledTimes(1);
  });

  it("fires immediately when schedule is past at addJob time (EXPECTED behaviour)", async () => {
    // What SHOULD happen: scheduler treats a past-due once-job at addJob the
    // same way start() does — execute it (or schedule a 0ms timer) so the user
    // doesn't lose the prompt to a clock race.
    const job = makeJob({
      type: "once",
      schedule: new Date(Date.now() - 100).toISOString(),
      enabled: true,
      lastRun: undefined,
    });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    const scheduler = makeScheduler(storage, pi);

    scheduler.addJob(job);

    // Allow microtasks + any 0ms timer to flush.
    await vi.advanceTimersByTimeAsync(0);

    expect(pi.sendUserMessage).toHaveBeenCalledTimes(1);
  });

  it("realistic race: '+0s'-style relative time validated at T, scheduleJob runs at T+ε, still fires (regression)", async () => {
    // Mirrors the tool flow: parseRelativeTime("+0s") returns ISO of `now`,
    // storage.addJob writes a JSON file (atomic temp+rename), and by the time
    // scheduler.addJob → scheduleJob computes `delay = target - Date.now()`
    // the value is <= 0. The fix clamps delay to 0 so a timer is always created.
    const targetIso = new Date(Date.now()).toISOString();
    // Simulate the gap between tool validation and scheduleJob actually running.
    vi.setSystemTime(Date.now() + 5);
    const job = makeJob({
      type: "once",
      schedule: targetIso,
      enabled: true,
      lastRun: undefined,
    });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    const scheduler = makeScheduler(storage, pi);

    scheduler.addJob(job);

    expect((scheduler as any).intervals.has(job.id)).toBe(true);
    await vi.advanceTimersByTimeAsync(0);
    expect(pi.sendUserMessage).toHaveBeenCalledTimes(1);
  });
});

// ─── context-aware routing ─────────────────────────────────────────────────────

describe("context-aware routing", () => {
  it("calls sendUserMessageToContext and sendMessageToContext when targetContext is set", async () => {
    const job = makeJob({ targetContext: "+alice" });
    const storage = makeMockStorage([job]);
    const pi = {
      ...makeMockPi(),
      sendMessageToContext: vi.fn(),
      sendUserMessageToContext: vi.fn(),
    };
    const scheduler = makeScheduler(storage, pi as any);

    await executeJob(scheduler, job);

    expect(pi.sendUserMessageToContext).toHaveBeenCalledWith(
      "+alice",
      expect.stringContaining(job.prompt)
    );
    expect(pi.sendMessageToContext).toHaveBeenCalledWith(
      "+alice",
      expect.objectContaining({ customType: "scheduled_prompt" })
    );
    expect(pi.sendUserMessage).not.toHaveBeenCalled();
    expect(pi.sendMessage).not.toHaveBeenCalled();
  });

  it("calls sendUserMessage and sendMessage when targetContext is undefined", async () => {
    const job = makeJob({ targetContext: undefined });
    const storage = makeMockStorage([job]);
    const pi = {
      ...makeMockPi(),
      sendMessageToContext: vi.fn(),
      sendUserMessageToContext: vi.fn(),
    };
    const scheduler = makeScheduler(storage, pi as any);

    await executeJob(scheduler, job);

    expect(pi.sendUserMessage).toHaveBeenCalledTimes(1);
    expect(pi.sendMessage).toHaveBeenCalledTimes(1);
    expect(pi.sendUserMessageToContext).not.toHaveBeenCalled();
    expect(pi.sendMessageToContext).not.toHaveBeenCalled();
  });

  it("falls back to sendUserMessage when pi lacks sendUserMessageToContext", async () => {
    const job = makeJob({ targetContext: "+alice" });
    const storage = makeMockStorage([job]);
    // pi does NOT have sendUserMessageToContext / sendMessageToContext
    const pi = makeMockPi();
    const scheduler = makeScheduler(storage, pi as any);

    await executeJob(scheduler, job);

    expect(pi.sendUserMessage).toHaveBeenCalledTimes(1);
    expect(pi.sendMessage).toHaveBeenCalledTimes(1);
  });
});

// ─── isStreaming race during agent_end emit ──────────────────────────────────
//
// Pi-mono's runtime (agent.ts:520):
//   `agent_end` only means no further loop events will be emitted. The run is
//   considered idle later, after all awaited listeners for `agent_end` finish
//   and `finishRun()` clears runtime-owned state.
//
// pi-mono's wrapper (agent-session.ts:2219) is fire-and-forget:
//   sendUserMessage: (content, options) => {
//     this.sendUserMessage(content, options).catch((err) =>
//       runner.emitError({ extensionPath: "<runtime>", ... })
//     );
//   }
//
// Consequence: when our notifyAgentEnd calls processNextDeferred → executeJobIfLeader
// → executeJob → sendUserMessage, the *entire microtask chain* unwinds before pi's
// `finally { finishRun() }` runs. So sendUserMessage is invoked while isStreaming is
// still true. The wrapper swallows the rejection (our try/catch never sees it) and
// pi reports it via emitError as `Extension "<runtime>" error: Agent is already
// processing`.
//
// Fix: processNextDeferred wraps the work in setTimeout(0) — a macrotask, which fires
// after every microtask in pi's agent_end unwind, including finishRun().

describe("processNextDeferred defers via setTimeout(0) so sends fire AFTER pi's finishRun (race fix)", () => {
  it("draining only microtasks after notifyAgentEnd does NOT trigger the deferred send — only macrotask advance does", async () => {
    // This is the empirical exhibit: with the fix, B's send is in the macrotask queue
    // (where it lands after pi's finishRun has run). Without the fix it would land in
    // the microtask queue and race against pi's still-true isStreaming flag.
    const jobA = makeJob({ id: "job-a", prompt: "A" });
    const jobB = makeJob({ id: "job-b", name: "Job B", prompt: "B" });
    const storage = makeMockStorage([jobA, jobB]);
    const pi = makeMockPi();
    const scheduler = makeScheduler(storage, pi);

    // A and B fire concurrently; A goes immediately, B is deferred.
    const pA = (scheduler as any).executeJobIfLeader(jobA);
    const pB = (scheduler as any).executeJobIfLeader(jobB);
    await Promise.all([pA, pB]);
    expect(pi.sendUserMessage).toHaveBeenCalledTimes(1); // A only
    expect((scheduler as any).deferredActions).toHaveLength(1); // B queued

    // A's turn ends. processNextDeferred fires inside notifyAgentEnd.
    scheduler.notifyAgentEnd([{ role: "assistant", stopReason: "stop" }]);

    // Drain ONLY microtasks — no fake-timer advance.
    // (`advanceTimersByTimeAsync(0)` would also fire setTimeout(0); we avoid it here
    //  to distinguish micro- from macrotask scheduling.)
    for (let i = 0; i < 20; i++) await Promise.resolve();

    // With the fix: B is queued as a macrotask, NOT yet fired.
    // Without the fix: B would have fired through the microtask chain.
    expect(pi.sendUserMessage).toHaveBeenCalledTimes(1);

    // Advance fake timers by 0 → setTimeout(0) callback fires → B is sent.
    await vi.advanceTimersByTimeAsync(0);
    expect(pi.sendUserMessage).toHaveBeenCalledTimes(2);
  });

  it("stop() cancels pending deferral timers", async () => {
    const jobA = makeJob({ id: "job-a", prompt: "A" });
    const jobB = makeJob({ id: "job-b", name: "Job B", prompt: "B" });
    const storage = makeMockStorage([jobA, jobB]);
    const pi = makeMockPi();
    const scheduler = makeScheduler(storage, pi);

    const pA = (scheduler as any).executeJobIfLeader(jobA);
    const pB = (scheduler as any).executeJobIfLeader(jobB);
    await Promise.all([pA, pB]);
    expect(pi.sendUserMessage).toHaveBeenCalledTimes(1);

    // notifyAgentEnd schedules B's send via setTimeout(0).
    scheduler.notifyAgentEnd([{ role: "assistant", stopReason: "stop" }]);

    // Stop the scheduler before the macrotask fires.
    scheduler.stop();

    // Advancing timers must NOT trigger the deferred send.
    await vi.advanceTimersByTimeAsync(0);
    expect(pi.sendUserMessage).toHaveBeenCalledTimes(1);
  });
});

// ─── Realistic-mock test: the catch block's race handler was dead code ────────
//
// The previous "Agent is already processing" handler in executeJob's catch block
// was unreachable from real pi. pi-mono's wrapper is fire-and-forget — the rejection
// is swallowed inside the wrapper's `.catch`, so no exception ever propagates to our
// try/catch. The handler that pushed jobs back to deferredActions on
// `errMsg.includes("Agent is already processing")` could never run.
//
// Earlier tests created the false impression of coverage by mocking sendUserMessage
// to *synchronously throw* — a behavior pi-mono's wrapper does not exhibit. This
// test uses a realistic mock and verifies the catch's recovery path is NOT taken.

describe("realistic pi behavior: sendUserMessage wrapper is fire-and-forget", () => {
  it("the catch block does not fire when the wrapper returns void (which is real pi behavior)", async () => {
    const job = makeJob({ guaranteed: true, prompt: "task" });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();

    // Realistic mock: returns void synchronously, never throws.
    // (Mirrors agent-session.ts:2219, which fires the underlying call and routes
    //  rejections to runner.emitError — never up the call stack.)
    pi.sendUserMessage.mockImplementation(() => undefined);
    pi.sendMessage.mockImplementation(() => undefined);

    const scheduler = makeScheduler(storage, pi);
    await executeJob(scheduler, job);

    // Try block completed: success-path side effects fired.
    expect((scheduler as any).pendingGuaranteedOnce).toEqual([job.id]);
    expect(storage.updateJob).toHaveBeenCalledWith(job.id, { lastStatus: "sent" });

    // Catch-block-only side effects did NOT fire:
    //   - The race handler would have unshifted job into deferredActions.
    //   - The fall-through error path would have set lastStatus: "error".
    expect((scheduler as any).deferredActions).toHaveLength(0);
    expect(storage.updateJob).not.toHaveBeenCalledWith(
      job.id,
      expect.objectContaining({ lastStatus: "error" }),
    );
  });
});

// ---- dedicated-context job: failure detection ----
//
// Regression: `pi -p` exits with code 0 even when the request failed (e.g. a
// transient "Connection error." emitted to stderr with no assistant reply on
// stdout). Trusting the exit code alone caused the scheduler to mark these
// runs as "success" — once-jobs were dropped, recurring jobs had their
// lastStatus flipped to "success", and guaranteed retries never fired.
//
// We treat empty stdout as a failure regardless of exit code, since a
// successful `pi -p` always produces an assistant reply.

describe("dedicated-context job failure detection", () => {
  async function runDedicated(scheduler: CronScheduler, job: CronJob) {
    await (scheduler as any).executeDedicatedJob(job);
  }

  it("marks a once-job as error and schedules retry when stdout is empty and stderr has a connection error (exit 0)", async () => {
    const job = makeJob({ guaranteed: true, dedicatedContext: true });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    pi.exec.mockResolvedValueOnce({ stdout: "", stderr: "Connection error.", code: 0, killed: false });

    const scheduler = makeScheduler(storage, pi);
    await runDedicated(scheduler, job);

    // Job NOT removed; lastStatus flipped to "error"; retry timer queued.
    expect(storage.removeJob).not.toHaveBeenCalled();
    expect(storage.updateJob).toHaveBeenCalledWith(job.id, { lastStatus: "error" });

    const record = (storage.addRunRecord as ReturnType<typeof vi.fn>).mock.calls.at(-1)?.[0];
    expect(record).toMatchObject({ jobId: job.id, status: "error" });
    expect(record.output).toContain("Connection error.");

    // Retry fires after 10m and re-invokes pi.exec (one retry attempt).
    await vi.advanceTimersByTimeAsync(RETRY_MS);
    expect(pi.exec).toHaveBeenCalledTimes(2);
  });

  it("marks a once-job as error even with empty stdout AND empty stderr (exit 0)", async () => {
    const job = makeJob({ guaranteed: true, dedicatedContext: true });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    pi.exec.mockResolvedValueOnce({ stdout: "", stderr: "", code: 0, killed: false });

    const scheduler = makeScheduler(storage, pi);
    await runDedicated(scheduler, job);

    expect(storage.removeJob).not.toHaveBeenCalled();
    expect(storage.updateJob).toHaveBeenCalledWith(job.id, { lastStatus: "error" });
  });

  it("marks a non-guaranteed once-job as error (and removes it) when stdout is empty", async () => {
    const job = makeJob({ guaranteed: false, dedicatedContext: true });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    pi.exec.mockResolvedValueOnce({ stdout: "", stderr: "Connection error.", code: 0, killed: false });

    const scheduler = makeScheduler(storage, pi);
    await runDedicated(scheduler, job);

    // Non-guaranteed once-jobs are dropped on failure (not retried), but the
    // run record must still report status: "error".
    expect(storage.removeJob).toHaveBeenCalledWith(job.id);
    const record = (storage.addRunRecord as ReturnType<typeof vi.fn>).mock.calls.at(-1)?.[0];
    expect(record).toMatchObject({ status: "error" });
  });

  it("treats a successful run (JSONL stream with agent_end, exit 0) as success and removes the once-job", async () => {
    const job = makeJob({ guaranteed: true, dedicatedContext: true });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    // pi --mode json emits one JSON object per line; the final agent_end carries the full message array.
    const jsonl = [
      JSON.stringify({ type: "agent_start" }),
      JSON.stringify({
        type: "agent_end",
        messages: [
          { role: "user", content: "do the thing" },
          { role: "assistant", content: [{ type: "text", text: "All done." }], stopReason: "stop" },
        ],
      }),
    ].join("\n");
    pi.exec.mockResolvedValueOnce({ stdout: jsonl, stderr: "", code: 0, killed: false });

    const scheduler = makeScheduler(storage, pi);
    await runDedicated(scheduler, job);

    expect(storage.removeJob).toHaveBeenCalledWith(job.id);
    const record = (storage.addRunRecord as ReturnType<typeof vi.fn>).mock.calls.at(-1)?.[0];
    expect(record).toMatchObject({ status: "success" });
    expect(record.output).toContain("All done.");
    expect(record.output).toContain("[exit=0");
  });

  it("treats a non-zero exit code as error even when stdout is non-empty", async () => {
    const job = makeJob({ guaranteed: true, dedicatedContext: true });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    pi.exec.mockResolvedValueOnce({ stdout: "partial reply", stderr: "boom", code: 2, killed: false });

    const scheduler = makeScheduler(storage, pi);
    await runDedicated(scheduler, job);

    expect(storage.removeJob).not.toHaveBeenCalled();
    expect(storage.updateJob).toHaveBeenCalledWith(job.id, { lastStatus: "error" });
  });

  it("records error status on a recurring job's run record when stdout is empty", async () => {
    const job = makeJob({
      type: "interval",
      schedule: "1h",
      dedicatedContext: true,
    });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    pi.exec.mockResolvedValueOnce({ stdout: "", stderr: "Connection error.", code: 0, killed: false });

    const scheduler = makeScheduler(storage, pi);
    await runDedicated(scheduler, job);

    // Recurring job: not removed, but lastStatus must be "error" — not "success".
    expect(storage.removeJob).not.toHaveBeenCalled();
    const updateCalls = (storage.updateJob as ReturnType<typeof vi.fn>).mock.calls;
    const finalUpdate = updateCalls.at(-1)?.[1];
    expect(finalUpdate).toMatchObject({ lastStatus: "error" });

    const record = (storage.addRunRecord as ReturnType<typeof vi.fn>).mock.calls.at(-1)?.[0];
    expect(record).toMatchObject({ status: "error" });
  });
});

// ---- start(): missed-job triage (Bug 1: /new must not refire failed jobs) ----
//
// After /new, scheduler.start() iterates jobs and historically fired any guaranteed
// job in lastStatus=error|sent immediately. With three dedicated jobs that's three
// pi subprocesses launched in parallel — the user observed 2/3 timing out at the
// 5-min mark. Fix: route error/sent state through the retry timer.

describe("start(): triages missed jobs by lastStatus (Bug 1: /new re-fire prevention)", () => {
  function makeCronJob(overrides: Partial<CronJob> = {}): CronJob {
    return {
      id: "cron-1",
      name: "Daily Routine",
      schedule: "0 0 8 * * *",
      prompt: "good morning",
      enabled: true,
      type: "cron",
      runCount: 1,
      createdAt: new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString(),
      lastRun: new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString(),
      guaranteed: true,
      ...overrides,
    };
  }

  it("does NOT immediately fire a guaranteed cron job whose previous run errored", async () => {
    const job = makeCronJob({ lastStatus: "error", dedicatedContext: true });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    const scheduler = makeScheduler(storage, pi);

    scheduler.start();

    // Microtask flush — confirm no immediate execution.
    await Promise.resolve();
    expect(pi.exec).not.toHaveBeenCalled();
    expect(pi.sendUserMessage).not.toHaveBeenCalled();
  });

  it("does NOT immediately fire a guaranteed cron job whose previous run was 'sent' (unconfirmed)", async () => {
    const job = makeCronJob({ lastStatus: "sent" });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    const scheduler = makeScheduler(storage, pi);

    scheduler.start();

    await Promise.resolve();
    expect(pi.sendUserMessage).not.toHaveBeenCalled();
  });

  it("schedules a 10m retry for a guaranteed cron job left in error state", async () => {
    const job = makeCronJob({ lastStatus: "error" });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    const scheduler = makeScheduler(storage, pi);

    scheduler.start();

    // No fire yet …
    expect(pi.sendUserMessage).not.toHaveBeenCalled();

    // … but after 10 minutes, the retry timer fires the job.
    await vi.advanceTimersByTimeAsync(RETRY_MS);
    expect(pi.sendUserMessage).toHaveBeenCalledTimes(1);
  });

  it("DOES immediately fire a guaranteed cron job that's missed-by-schedule with lastStatus=success", async () => {
    // lastRun was a year ago → the cron schedule has fired many times since →
    // isMissed() is true. lastStatus=success means this is a genuine miss
    // (e.g. daemon was offline), not a retry — fire it immediately as before.
    const job = makeCronJob({
      lastRun: new Date(Date.now() - 365 * 24 * 60 * 60 * 1000).toISOString(),
      lastStatus: "success",
    });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    const scheduler = makeScheduler(storage, pi);

    scheduler.start();
    // Drain microtasks for the leader-election promise.
    await Promise.resolve();
    await Promise.resolve();
    expect(pi.sendUserMessage).toHaveBeenCalledTimes(1);
  });

  it("does not fire three error-state dedicated jobs in parallel on /new (regression)", async () => {
    // Reproduces the original report: three dedicated cron jobs all in lastStatus=error
    // launched as three parallel pi subprocesses → contention → 2/3 timed out.
    const jobs = [
      makeCronJob({ id: "j1", name: "Job 1", lastStatus: "error", dedicatedContext: true }),
      makeCronJob({ id: "j2", name: "Job 2", lastStatus: "error", dedicatedContext: true }),
      makeCronJob({ id: "j3", name: "Job 3", lastStatus: "error", dedicatedContext: true }),
    ];
    const storage = makeMockStorage(jobs);
    const pi = makeMockPi();
    const scheduler = makeScheduler(storage, pi);

    scheduler.start();
    await Promise.resolve();
    await Promise.resolve();

    expect(pi.exec).not.toHaveBeenCalled();
  });
});

// ---- recurring dedicated job retry on failure ----

describe("recurring guaranteed dedicated jobs schedule a retry on failure", () => {
  it("fires retry 10 minutes after a failed cron-type dedicated run", async () => {
    const job: CronJob = {
      id: "cron-1",
      name: "Daily Check",
      schedule: "0 0 8 * * *",
      prompt: "check",
      enabled: true,
      type: "cron",
      runCount: 0,
      createdAt: new Date().toISOString(),
      guaranteed: true,
      dedicatedContext: true,
    };
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    pi.exec.mockResolvedValue({ stdout: "", stderr: "Connection error.", code: 0, killed: false });

    const scheduler = makeScheduler(storage, pi);
    await (scheduler as any).executeDedicatedJob(job);

    expect(pi.exec).toHaveBeenCalledTimes(1);

    // Without the fix, recurring jobs only retry on next cron tick. With the fix,
    // a 10-minute retry timer fires the same job again.
    await vi.advanceTimersByTimeAsync(RETRY_MS);
    expect(pi.exec).toHaveBeenCalledTimes(2);
  });
});

// ---- formatDedicatedRunOutput: Bug 2 (always-have-something-to-show) ----

describe("formatDedicatedRunOutput: always emits diagnostic info (Bug 2)", () => {
  it("includes exit/killed/duration header even on fully-empty output", () => {
    const { output, hasAgentEnd } = formatDedicatedRunOutput("", "", 0, true, 300);
    expect(output).toContain("[exit=0 killed=true duration=300s]");
    expect(hasAgentEnd).toBe(false);
  });

  it("appends stderr when present", () => {
    const { output } = formatDedicatedRunOutput("", "boom", 1, false, 2);
    expect(output).toContain("[exit=1 killed=false duration=2s]");
    expect(output).toContain("[stderr]\nboom");
  });

  it("falls back to raw stdout when it is not JSONL (legacy / non-JSON output)", () => {
    const { output } = formatDedicatedRunOutput("All done.\n", "", 0, false, 5);
    expect(output).toContain("All done.");
  });

  it("parses JSONL agent_end and renders assistant text", () => {
    const jsonl = [
      JSON.stringify({ type: "agent_start" }),
      JSON.stringify({
        type: "agent_end",
        messages: [
          { role: "user", content: "ping" },
          { role: "assistant", content: [{ type: "text", text: "pong" }], stopReason: "stop" },
        ],
      }),
    ].join("\n");
    const { output, hasAgentEnd } = formatDedicatedRunOutput(jsonl, "", 0, false, 1);
    expect(hasAgentEnd).toBe(true);
    expect(output).toContain("pong");
  });

  it("renders thinking, tool calls, and tool results from agent_end", () => {
    const jsonl = JSON.stringify({
      type: "agent_end",
      messages: [
        { role: "user", content: "do work" },
        {
          role: "assistant",
          content: [
            { type: "thinking", thinking: "I'll list files first" },
            { type: "toolCall", id: "t1", name: "Bash", arguments: { command: "ls" } },
          ],
          stopReason: "toolUse",
        },
        {
          role: "toolResult",
          toolCallId: "t1",
          toolName: "Bash",
          content: [{ type: "text", text: "file1\nfile2" }],
          isError: false,
        },
        { role: "assistant", content: [{ type: "text", text: "Done." }], stopReason: "stop" },
      ],
    });
    const { output } = formatDedicatedRunOutput(jsonl, "", 0, false, 3);
    expect(output).toContain("[thinking]");
    expect(output).toContain("I'll list files first");
    expect(output).toContain("[tool: Bash]");
    expect(output).toContain("[result: Bash]");
    expect(output).toContain("file1");
    expect(output).toContain("Done.");
  });

  it("falls back to message_end events when stream was killed before agent_end", () => {
    const jsonl = [
      JSON.stringify({ type: "agent_start" }),
      JSON.stringify({
        type: "message_end",
        message: { role: "assistant", content: [{ type: "text", text: "partial" }], stopReason: "stop" },
      }),
      // No agent_end — simulating a process killed mid-stream.
    ].join("\n");
    const { output, hasAgentEnd } = formatDedicatedRunOutput(jsonl, "", 0, true, 300);
    expect(hasAgentEnd).toBe(false);
    expect(output).toContain("partial");
    expect(output).toContain("killed=true");
  });
});

// ---- timer stacking in scheduleRetryTimer (Bug 3 regression) ----
//
// Repro for the user's report: a guaranteed dedicated cron job that fails
// produced 3 back-to-back runs of the same prompt instead of clean 15-minute
// cycles. Cause: scheduleRetryTimer was called twice (once from start() for
// the error-state job, again from the cron callback's failure path) without
// clearing the previous timer. Both timers fire — each calls triggerRetry —
// and because the runningDedicatedJobs guard only blocks *concurrent* runs,
// the second timer's retry fires immediately after the first finishes.

describe("scheduleRetryTimer cancels its previous pending timer (Bug 3)", () => {
  it("calling scheduleRetryTimer twice for the same job results in only ONE retry, not two", async () => {
    const job: CronJob = {
      id: "j1",
      name: "Job",
      schedule: "0 0 8 * * *",
      prompt: "p",
      enabled: true,
      type: "cron",
      runCount: 0,
      createdAt: new Date().toISOString(),
      guaranteed: true,
      dedicatedContext: true,
      lastStatus: "error",
    };
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    // Subprocess returns success quickly so we can count exec invocations cleanly.
    pi.exec.mockResolvedValue({
      stdout: JSON.stringify({
        type: "agent_end",
        messages: [
          { role: "user", content: "p" },
          { role: "assistant", content: [{ type: "text", text: "ok" }], stopReason: "stop" },
        ],
      }),
      stderr: "",
      code: 0,
      killed: false,
    });
    const scheduler = makeScheduler(storage, pi);

    // First call: registers Timer A.
    (scheduler as any).scheduleRetryTimer(job.id);
    // Second call (e.g. from a different code path): without the fix, leaves Timer A
    // pending and adds Timer B → both fire in succession after 10 minutes.
    (scheduler as any).scheduleRetryTimer(job.id);

    await vi.advanceTimersByTimeAsync(RETRY_MS);
    // Drain microtasks/setTimeout(0)s so any second exec call would surface.
    await vi.advanceTimersByTimeAsync(0);

    // With the fix: exactly ONE exec call. Without the fix: TWO.
    expect(pi.exec).toHaveBeenCalledTimes(1);
  });

  it("does NOT produce duplicate runs when start() and a failed cron tick both schedule retries", async () => {
    // Real scenario the user observed: dedicated cron job in lastStatus=error.
    // start() schedules a retry timer. Some path (e.g. the cron callback firing,
    // or another code path) also schedules a retry. Both timers must collapse.
    const job: CronJob = {
      id: "j1",
      name: "Daily Routine",
      schedule: "0 0 8 * * *",
      prompt: "go",
      enabled: true,
      type: "cron",
      runCount: 0,
      createdAt: new Date().toISOString(),
      guaranteed: true,
      dedicatedContext: true,
      lastStatus: "error",
    };
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    let execCount = 0;
    pi.exec.mockImplementation(async () => {
      execCount++;
      return {
        stdout: "",
        stderr: "transient",
        code: 0,
        killed: false,
      };
    });
    const scheduler = makeScheduler(storage, pi);

    // start() registers Timer A for the error-state job.
    scheduler.start();

    // Simulate a parallel code path also scheduling a retry (e.g. cron tick fires,
    // executeDedicatedJob runs, fails, and on the recurring-error branch schedules
    // another retry). We just trigger the second scheduleRetryTimer directly.
    (scheduler as any).scheduleRetryTimer(job.id);

    await vi.advanceTimersByTimeAsync(RETRY_MS);
    await vi.advanceTimersByTimeAsync(0);

    // Only one retry-driven exec, not two.
    expect(execCount).toBe(1);
  });
});

// ---- formatDedicatedRunOutput: dedup of streaming events (Bug 4) ----
//
// Repro for the 197 MB schedule-prompts.json: when pi --mode json is killed
// before agent_end (e.g. 5-min timeout), every streaming `message_update`
// event carried a snapshot of the same logical assistant message with
// growing thinking content. The fallback path naively pushed each snapshot,
// so 5000 thinking deltas → 5000 `[thinking]` blocks → 27 MB output.

describe("formatDedicatedRunOutput dedups streaming message_update events (Bug 4)", () => {
  it("renders one [thinking] block, not N, when the same message streams via N message_update events", () => {
    // Simulate streaming: one message_start, 100 message_update events (each carrying
    // the same logical message with growing thinking content), no message_end (process killed).
    const events: string[] = [];
    events.push(JSON.stringify({ type: "agent_start" }));
    events.push(JSON.stringify({
      type: "message_start",
      message: { role: "assistant", content: [] },
    }));
    for (let i = 1; i <= 100; i++) {
      events.push(JSON.stringify({
        type: "message_update",
        message: {
          role: "assistant",
          content: [{ type: "thinking", thinking: "thought ".repeat(i) }],
        },
        assistantMessageEvent: { type: "thinking_delta" },
      }));
    }
    // No message_end, no agent_end — process was killed.
    const stdout = events.join("\n");

    const { output, hasAgentEnd } = formatDedicatedRunOutput(stdout, "", 143, true, 300);

    expect(hasAgentEnd).toBe(false);
    // Without the fix: 100 [thinking] headers. With the fix: exactly one.
    const thinkingHeaders = (output.match(/\[thinking\]/g) ?? []).length;
    expect(thinkingHeaders).toBe(1);
    // The final state of the thinking text should be present.
    expect(output).toContain("thought ".repeat(100).trim());
  });

  it("caps total output size to prevent storage bloat", () => {
    // Synthesize a single message with 500 KB of thinking content. Even after
    // dedup, this is too big to store; the cap must kick in.
    const huge = "x".repeat(500_000);
    const stdout = JSON.stringify({
      type: "agent_end",
      messages: [
        {
          role: "assistant",
          content: [{ type: "thinking", thinking: huge }],
          stopReason: "stop",
        },
      ],
    });
    const { output } = formatDedicatedRunOutput(stdout, "", 0, false, 5);
    // 64 KB cap + truncation suffix.
    expect(output.length).toBeLessThan(70_000);
    expect(output).toContain("[output truncated;");
  });

  it("preserves separate logical messages across multiple message_start events", () => {
    // Two distinct assistant turns — both should be rendered.
    const stdout = [
      JSON.stringify({ type: "agent_start" }),
      JSON.stringify({
        type: "message_start",
        message: { role: "assistant", content: [] },
      }),
      JSON.stringify({
        type: "message_end",
        message: {
          role: "assistant",
          content: [{ type: "text", text: "first turn" }],
          stopReason: "stop",
        },
      }),
      JSON.stringify({
        type: "message_start",
        message: { role: "assistant", content: [] },
      }),
      JSON.stringify({
        type: "message_update",
        message: {
          role: "assistant",
          content: [{ type: "text", text: "second turn (partial)" }],
        },
      }),
      // No message_end for the second one — process killed.
    ].join("\n");

    const { output } = formatDedicatedRunOutput(stdout, "", 143, true, 300);
    expect(output).toContain("first turn");
    expect(output).toContain("second turn (partial)");
  });
});

// ---- detectFailureHint: surface known causes to the user ----

describe("detectFailureHint", () => {
  it("returns null for output with no recognizable failure", () => {
    expect(detectFailureHint("[exit=0 killed=false duration=2s]\n\n[assistant]\nDone.")).toBeNull();
  });

  it("flags a pi-guardrails 'no UI to confirm' block with the offending path", () => {
    const out = "[exit=0]\n[result: bash (error)]\nAccess to /tmp/pi-bash-abc.log is blocked (outside working directory, no UI to confirm).";
    const hint = detectFailureHint(out);
    expect(hint).not.toBeNull();
    expect(hint).toContain("/tmp/pi-bash-abc.log");
    expect(hint).toContain("pathAccess.allowedPaths");
    expect(hint).toContain("guardrails.json");
  });

  it("flags a generic pi-guardrails block (mode=block, not just ask)", () => {
    const out = "Access to /etc/passwd is blocked (outside working directory).";
    const hint = detectFailureHint(out);
    expect(hint).toContain("/etc/passwd");
    expect(hint).toContain("pathAccess");
  });

  it("flags a read-only filesystem error", () => {
    const out = "[result: bash (error)]\ntouch: cannot touch '/home/x/foo': Read-only file system";
    expect(detectFailureHint(out)).toMatch(/Read-only file system/);
  });

  it("flags model/network errors as transient", () => {
    expect(detectFailureHint("Connection error.")).toMatch(/transient|retry/);
    expect(detectFailureHint("ETIMEDOUT 10.0.0.1")).toMatch(/transient|retry/);
  });

  it("flags subprocess timeout when killed=true is in the diagnostic header", () => {
    const out = "[exit=0 killed=true duration=300s]\n\n[user]\nlong prompt";
    expect(detectFailureHint(out)).toMatch(/5-minute timeout/);
  });

  it("prefers the more specific guardrails hint over the generic timeout hint", () => {
    // A run that's both killed and contains a guardrails block — guardrails is the
    // actionable cause; the timeout is the symptom. The first match wins.
    const out = "[exit=0 killed=true duration=300s]\n\n[result: write (error)]\nAccess to /tmp/foo is blocked (outside working directory, no UI to confirm).";
    const hint = detectFailureHint(out);
    expect(hint).toContain("guardrails");
    expect(hint).not.toMatch(/5-minute timeout/);
  });
});

// ---- renderMessages: shared formatter for non-dedicated jobs ----

describe("renderMessages includes thinking + tool calls + tool results", () => {
  it("includes assistant text, thinking, tool calls, and tool results", () => {
    const messages = [
      {
        role: "assistant",
        content: [
          { type: "thinking", thinking: "thought" },
          { type: "text", text: "intro" },
          { type: "toolCall", id: "t1", name: "Read", arguments: { path: "/x" } },
        ],
      },
      {
        role: "toolResult",
        toolCallId: "t1",
        toolName: "Read",
        content: [{ type: "text", text: "data" }],
        isError: false,
      },
      { role: "assistant", content: [{ type: "text", text: "final" }] },
    ];
    const out = renderMessages(messages);
    expect(out).toContain("[thinking]");
    expect(out).toContain("thought");
    expect(out).toContain("intro");
    expect(out).toContain("[tool: Read]");
    expect(out).toContain("[result: Read]");
    expect(out).toContain("data");
    expect(out).toContain("final");
  });

  it("marks tool results as error when isError is true", () => {
    const messages = [
      {
        role: "toolResult",
        toolCallId: "t1",
        toolName: "Bash",
        content: [{ type: "text", text: "command failed" }],
        isError: true,
      },
    ];
    const out = renderMessages(messages);
    expect(out).toContain("[result: Bash (error)]");
  });
});

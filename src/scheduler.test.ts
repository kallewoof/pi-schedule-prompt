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
    retryLastTurn: vi.fn(),
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
    expect(pi.sendUserMessage).toHaveBeenCalledWith("task A");

    // agent_end for A → B fires
    scheduler.notifyAgentEnd([{ role: "assistant", stopReason: "stop" }]);
    await vi.advanceTimersByTimeAsync(0);
    expect(pi.sendUserMessage).toHaveBeenCalledTimes(2);
    expect(pi.sendUserMessage).toHaveBeenLastCalledWith("task B");

    // agent_end for B → C fires
    scheduler.notifyAgentEnd([{ role: "assistant", stopReason: "stop" }]);
    await vi.advanceTimersByTimeAsync(0);
    expect(pi.sendUserMessage).toHaveBeenCalledTimes(3);
    expect(pi.sendUserMessage).toHaveBeenLastCalledWith("task C");
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
    await vi.advanceTimersByTimeAsync(WATCHDOG_MS);
    await vi.advanceTimersByTimeAsync(0);

    // After the watchdog the sending flag is cleared and jobB fires.
    expect(pi.sendUserMessage).toHaveBeenCalledTimes(2); // ← FAILS: jobB never unblocked
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

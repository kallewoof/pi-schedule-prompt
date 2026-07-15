import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import {
  CronScheduler,
  defaultResumableSessionDir,
  detectFailureHint,
  formatDedicatedRunOutput,
  getDedicatedActivity,
  promoteSessionToResumable,
  renderMessages,
  __resetSubprocessStateForTests,
} from "./scheduler.js";
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
  // Module-scope subprocess state leaks across tests otherwise — a hanging
  // `pi.exec` mock from a prior case keeps the slot "busy" and silently queues
  // every subsequent dispatch instead of firing it.
  __resetSubprocessStateForTests();
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
    expect(pi.sendUserMessage).toHaveBeenCalledWith(`${prefix}task A`, { deliverAs: "followUp" });

    // agent_end for A → B fires (legacy pi mock has no isIdle → agent_end drives the drain)
    scheduler.notifyAgentEnd([{ role: "assistant", stopReason: "stop" }]);
    await vi.advanceTimersByTimeAsync(0);
    expect(pi.sendUserMessage).toHaveBeenCalledTimes(2);
    expect(pi.sendUserMessage).toHaveBeenLastCalledWith(`${prefix}task B`, { deliverAs: "followUp" });

    // agent_end for B → C fires
    scheduler.notifyAgentEnd([{ role: "assistant", stopReason: "stop" }]);
    await vi.advanceTimersByTimeAsync(0);
    expect(pi.sendUserMessage).toHaveBeenCalledTimes(3);
    expect(pi.sendUserMessage).toHaveBeenLastCalledWith(`${prefix}task C`, { deliverAs: "followUp" });
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

// ---- start(): missed non-guaranteed recurring jobs catch up on restart ----
//
// Repro for the user's observation: two interval jobs (30m) showed "Next run:
// 12:49" at 14:58, hours after the next-scheduled time elapsed. Pi was offline
// when those fires were due. The original behaviour silently dropped the
// missed execution and re-armed a fresh setInterval — so an interval declared
// as "every 30 minutes" effectively becomes ~30m + offline-duration on the
// restart cycle. For a job that should have fired 20h ago, the correct
// response is to fire it once now and resume the cadence, not wait another
// full interval.
//
// The `guaranteed` flag still distinguishes in-session retry semantics on
// transient model errors / unconfirmed sends; it just no longer gates the
// "catch up a missed tick" decision.

describe("start(): missed non-guaranteed recurring jobs catch up on restart", () => {
  it("fires a missed non-guaranteed interval job once on startup", async () => {
    const now = Date.now();
    // lastRun was 2h ago, intervalMs = 30m → 3 fires were missed.
    const job: CronJob = {
      id: "interval-1",
      name: "Half-hourly",
      schedule: "30m",
      prompt: "ping",
      enabled: true,
      type: "interval",
      intervalMs: 30 * 60 * 1000,
      runCount: 4,
      createdAt: new Date(now - 4 * 60 * 60 * 1000).toISOString(),
      lastRun: new Date(now - 2 * 60 * 60 * 1000).toISOString(),
      lastStatus: "success",
      guaranteed: false,
    };
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    const scheduler = makeScheduler(storage, pi);

    scheduler.start();
    // Drain microtasks: executeJobIfLeader → acquireLeadership (mocked resolved) → executeJob.
    await Promise.resolve();
    await Promise.resolve();
    await Promise.resolve();

    expect(pi.sendUserMessage).toHaveBeenCalledTimes(1);
  });

  it("getNextRun returns a future time after start() processes a missed interval job", async () => {
    const now = Date.now();
    const intervalMs = 30 * 60 * 1000;
    const job: CronJob = {
      id: "interval-2",
      name: "Half-hourly",
      schedule: "30m",
      prompt: "ping",
      enabled: true,
      type: "interval",
      intervalMs,
      runCount: 4,
      createdAt: new Date(now - 4 * 60 * 60 * 1000).toISOString(),
      lastRun: new Date(now - 2 * 60 * 60 * 1000).toISOString(),
      lastStatus: "success",
      guaranteed: false,
    };
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    const scheduler = makeScheduler(storage, pi);

    scheduler.start();
    await Promise.resolve();
    await Promise.resolve();

    const next = scheduler.getNextRun(job.id);
    expect(next).not.toBeNull();
    // The displayed "Next run" must point at the freshly-armed timer's first
    // fire (≈ now + intervalMs), not the long-elapsed pre-restart cadence.
    expect(next!.getTime()).toBeGreaterThan(now);
  });

  it("does not fire a non-guaranteed interval job that is NOT yet missed", async () => {
    // Guard against catching-up a job whose next tick is still in the future.
    const now = Date.now();
    const intervalMs = 30 * 60 * 1000;
    const job: CronJob = {
      id: "interval-3",
      name: "Half-hourly",
      schedule: "30m",
      prompt: "ping",
      enabled: true,
      type: "interval",
      intervalMs,
      runCount: 4,
      createdAt: new Date(now - 60 * 60 * 1000).toISOString(),
      // lastRun = 5m ago → next tick is in 25m → not missed.
      lastRun: new Date(now - 5 * 60 * 1000).toISOString(),
      lastStatus: "success",
      guaranteed: false,
    };
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    const scheduler = makeScheduler(storage, pi);

    scheduler.start();
    await Promise.resolve();
    await Promise.resolve();

    expect(pi.sendUserMessage).not.toHaveBeenCalled();
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
      expect.stringContaining(job.prompt),
      { deliverAs: "followUp" }
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

// ---- runJobNow: public retry-on-demand entry point ----

describe("runJobNow (public API for /schedule-prompt retry)", () => {
  it("fires a non-dedicated job through executeJobIfLeader", async () => {
    const job = makeJob({ id: "job-1", prompt: "p", guaranteed: false });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    const scheduler = makeScheduler(storage, pi);

    await scheduler.runJobNow(job.id);

    expect(pi.sendUserMessage).toHaveBeenCalledTimes(1);
    expect(pi.sendUserMessage).toHaveBeenCalledWith(expect.stringContaining("p"), { deliverAs: "followUp" });
  });

  it("fires a dedicated job through executeDedicatedJob", async () => {
    const job = makeJob({ id: "job-1", dedicatedContext: true, prompt: "p" });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    pi.exec.mockResolvedValueOnce({
      stdout: JSON.stringify({
        type: "agent_end",
        messages: [{ role: "assistant", content: [{ type: "text", text: "ok" }], stopReason: "stop" }],
      }),
      stderr: "",
      code: 0,
      killed: false,
    });
    const scheduler = makeScheduler(storage, pi);

    await scheduler.runJobNow(job.id);
    // Dedicated jobs are fire-and-forget inside executeJobIfLeader; let the await chain settle.
    await vi.advanceTimersByTimeAsync(0);

    expect(pi.exec).toHaveBeenCalledTimes(1);
    expect(pi.sendUserMessage).not.toHaveBeenCalled();
  });

  it("fires a recurring job that is currently disabled (enabled flag governs only automatic firing)", async () => {
    // Canonical use case: user disabled a recurring job to investigate, then
    // wants to manually re-run it.
    const job: CronJob = {
      id: "cron-1",
      name: "Daily",
      schedule: "0 0 8 * * *",
      prompt: "p",
      enabled: false,
      type: "cron",
      runCount: 0,
      createdAt: new Date().toISOString(),
    };
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    const scheduler = makeScheduler(storage, pi);

    await scheduler.runJobNow(job.id);

    expect(pi.sendUserMessage).toHaveBeenCalledTimes(1);
    // We do NOT silently re-enable the job; it remains disabled afterwards.
    expect(storage.getJob(job.id)?.enabled).toBe(false);
  });

  it("throws when no job with the given id exists in storage", async () => {
    const scheduler = makeScheduler(makeMockStorage(), makeMockPi());
    await expect(scheduler.runJobNow("nope")).rejects.toThrow(/Job nope not found/);
  });

  it("retrying the SAME dedicated job while it's running queues a fresh run for after it finishes (and reports 'queued')", async () => {
    // User's actual scenario: dedicated job A is running; user does
    // /schedule-prompt retry (resolves to A). Old behaviour: silently no-op.
    // New behaviour: queue a follow-up run; surface "queued" to the caller.
    const job = makeJob({ id: "j", dedicatedContext: true, prompt: "p" });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();

    let resolveFirst!: (value: any) => void;
    const okPayload = {
      stdout: JSON.stringify({
        type: "agent_end",
        messages: [{ role: "assistant", content: [{ type: "text", text: "ok" }], stopReason: "stop" }],
      }),
      stderr: "",
      code: 0,
      killed: false,
    };
    let firstSignal: AbortSignal | undefined;
    let callCount = 0;
    (pi.exec as any).mockImplementation((_cmd: unknown, _args: unknown, opts: any) => {
      callCount++;
      if (callCount === 1) {
        firstSignal = opts?.signal;
        return new Promise((r) => { resolveFirst = r; });
      }
      return Promise.resolve(okPayload);
    });

    const scheduler = makeScheduler(storage, pi);

    // Start the first run. Subprocess is hanging.
    const firstResult = await scheduler.runJobNow(job.id);
    expect(firstResult).toBe("fired");
    expect(pi.exec).toHaveBeenCalledTimes(1);

    // While first is in-flight, user retries the same job.
    const secondResult = await scheduler.runJobNow(job.id);
    expect(secondResult).toBe("queued");
    // Critically, the running subprocess was NOT aborted.
    expect(firstSignal?.aborted).toBe(false);
    // No new subprocess started yet — it's queued.
    expect(pi.exec).toHaveBeenCalledTimes(1);

    // First run finishes.
    resolveFirst(okPayload);
    // Drain the executeDedicatedJob completion path. There are several await
    // points (storage writes, format, notify) plus the inner await pi.exec on
    // the queued retry, so loop generously.
    for (let i = 0; i < 30; i++) await Promise.resolve();
    await vi.advanceTimersByTimeAsync(0);

    // The queued retry has now fired as a SECOND subprocess.
    expect(pi.exec).toHaveBeenCalledTimes(2);
  });

  it("a second runJobNow on the same in-flight dedicated job is de-duped by runningDedicatedJobs", async () => {
    const job = makeJob({ id: "job-1", dedicatedContext: true });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    // Make exec hang so the first run is still in-flight when we call again.
    let resolveFirst!: (value: any) => void;
    pi.exec.mockReturnValueOnce(new Promise((r) => { resolveFirst = r; }));

    const scheduler = makeScheduler(storage, pi);
    void scheduler.runJobNow(job.id);
    await Promise.resolve();
    await Promise.resolve();
    // Second call while first is still running.
    await scheduler.runJobNow(job.id);

    // Only one subprocess kicked off.
    expect(pi.exec).toHaveBeenCalledTimes(1);

    // Resolve the first so we don't leak a pending promise.
    resolveFirst({
      stdout: JSON.stringify({ type: "agent_end", messages: [{ role: "assistant", content: [{ type: "text", text: "ok" }], stopReason: "stop" }] }),
      stderr: "",
      code: 0,
      killed: false,
    });
    await vi.advanceTimersByTimeAsync(0);
  });

  it("defers a non-dedicated retry when a previous send is still in-flight (sending=true)", async () => {
    // First non-dedicated job is sent and waiting for agent_end. Calling runJobNow on
    // a second job before that turn finishes must NOT race past the gate; the second
    // job should sit in deferredActions and fire only after the first turn ends.
    const jobA = makeJob({ id: "job-a", prompt: "A" });
    const jobB = makeJob({ id: "job-b", name: "B", prompt: "B" });
    const storage = makeMockStorage([jobA, jobB]);
    const pi = makeMockPi();
    const scheduler = makeScheduler(storage, pi);

    await scheduler.runJobNow(jobA.id);
    expect(pi.sendUserMessage).toHaveBeenCalledTimes(1); // A is sent, sending=true

    // Now retry B while A's turn hasn't ended.
    await scheduler.runJobNow(jobB.id);
    expect(pi.sendUserMessage).toHaveBeenCalledTimes(1); // B is deferred, not sent

    // A's turn ends → deferred B fires via the setTimeout(0) macrotask.
    scheduler.notifyAgentEnd([{ role: "assistant", stopReason: "stop" }]);
    await vi.advanceTimersByTimeAsync(0);
    expect(pi.sendUserMessage).toHaveBeenCalledTimes(2);
  });

  it("defers a non-dedicated retry while a user-initiated turn is active (agentRunning=true)", async () => {
    // A user-initiated turn is in progress (agent_start fired without a preceding
    // scheduler send). runJobNow on a job during that turn must defer until agent_end,
    // not race against the still-streaming user turn.
    const job = makeJob({ id: "job-1", prompt: "scheduled" });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    const scheduler = makeScheduler(storage, pi);

    scheduler.notifyAgentStart(); // user-initiated turn

    await scheduler.runJobNow(job.id);
    expect(pi.sendUserMessage).not.toHaveBeenCalled(); // deferred

    scheduler.notifyAgentEnd([{ role: "assistant", stopReason: "stop" }]);
    await vi.advanceTimersByTimeAsync(0);
    expect(pi.sendUserMessage).toHaveBeenCalledTimes(1);
  });

  it("dedicated-job notifications keep the prompt out of the message body and never use deliverAs:nextTurn", async () => {
    // Two invariants in one test:
    //
    // 1) Agent-leak protection (structural): pi.sendMessage during a streaming
    //    turn defaults to agent.steer(), which feeds the message into the next
    //    assistant prompt. If the message body contained the dedicated prompt
    //    text, the host agent would pick it up and start executing the
    //    dedicated job's task itself. Scrub the prompt from content[0].text
    //    unconditionally — the renderer reads it from details.prompt.
    //
    // 2) No silent parking: pi-mono's "nextTurn" delivery pushes the message
    //    into _pendingNextTurnMessages and only emits message_start/_end when
    //    the user sends another prompt. An idle session never drains the
    //    queue, so a notification fired during such a session sits invisibly
    //    forever. We previously gated this on agentRunning ("nextTurn while
    //    streaming, immediate otherwise"), but agentRunning can stick true
    //    (overflow recovery suppresses agent_end; stale scheduler bindings
    //    miss their session's events). The fix: never use nextTurn. With the
    //    structural prompt-scrub above, immediate emit is safe even during a
    //    live turn (worst case: a short status notification gets steered in).
    const job = makeJob({
      id: "ded",
      dedicatedContext: true,
      name: "Daily Routine: X",
      prompt: "PROMPT_TEXT_THAT_MUST_NOT_LEAK",
      // Recurring so the job survives the first run and the streaming-path
      // assertion below can fire it again.
      type: "cron",
      schedule: "0 0 0 * * *",
    });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    pi.exec.mockResolvedValueOnce({
      stdout: JSON.stringify({
        type: "agent_end",
        messages: [{ role: "assistant", content: [{ type: "text", text: "ok" }], stopReason: "stop" }],
      }),
      stderr: "",
      code: 0,
      killed: false,
    });
    const scheduler = makeScheduler(storage, pi);

    // Idle path: no agent_start fired → agentRunning=false → notifications must
    // be sent without deliverAs so they render immediately.
    await scheduler.runJobNow(job.id);
    await vi.advanceTimersByTimeAsync(0);

    expect(pi.sendMessage).toHaveBeenCalled();
    let beginSeen = false;
    let endSeen = false;
    for (const call of pi.sendMessage.mock.calls) {
      const [msg, opts] = call as [any, any];
      // 1) The notification body must NEVER contain the prompt text.
      const bodyText = Array.isArray(msg.content) ? msg.content[0]?.text ?? "" : String(msg.content ?? "");
      expect(bodyText).not.toContain("PROMPT_TEXT_THAT_MUST_NOT_LEAK");
      // 2) Idle session: deliverAs must NOT be "nextTurn" or the message would
      //    sit in _pendingNextTurnMessages until the next user prompt.
      expect(opts?.deliverAs).not.toBe("nextTurn");
      // 3) The begin notification still routes the prompt through details for the renderer.
      if (msg.customType === "scheduled_prompt_begin") {
        beginSeen = true;
        expect(msg.details?.prompt).toBe("PROMPT_TEXT_THAT_MUST_NOT_LEAK");
      }
      if (msg.customType === "scheduled_prompt_end") {
        endSeen = true;
      }
    }
    expect(beginSeen).toBe(true);
    expect(endSeen).toBe(true);

    // Streaming path: even with an active agent turn, notifications must NOT
    // use "nextTurn" — they'd never drain on an idle session. The structural
    // prompt-scrub above keeps a steered notification from feeding the agent
    // the dedicated workflow.
    pi.sendMessage.mockClear();
    pi.exec.mockResolvedValueOnce({
      stdout: JSON.stringify({
        type: "agent_end",
        messages: [{ role: "assistant", content: [{ type: "text", text: "ok" }], stopReason: "stop" }],
      }),
      stderr: "",
      code: 0,
      killed: false,
    });
    scheduler.notifyAgentStart();
    await scheduler.runJobNow(job.id);
    await vi.advanceTimersByTimeAsync(0);
    expect(pi.sendMessage).toHaveBeenCalled();
    for (const call of pi.sendMessage.mock.calls) {
      const [msg, opts] = call as [any, any];
      // Prompt-scrub invariant must still hold under streaming.
      const bodyText = Array.isArray(msg.content) ? msg.content[0]?.text ?? "" : String(msg.content ?? "");
      expect(bodyText).not.toContain("PROMPT_TEXT_THAT_MUST_NOT_LEAK");
      // No silent parking under any condition.
      expect(opts?.deliverAs).not.toBe("nextTurn");
    }
  });

  it("retrying a DIFFERENT dedicated job while one is still in-flight queues it (no parallel exec) without aborting the running one", async () => {
    // Dedicated/command subprocesses are host-serialized: only one runs at a
    // time. Two daily routines scheduled for the same minute would otherwise
    // hammer the model provider in parallel and race on any shared output
    // files. The second job waits in the queue until the first finishes;
    // meanwhile the running job's AbortController must NOT be triggered by
    // the enqueue.
    const jobA = makeJob({ id: "A", dedicatedContext: true });
    const jobB = makeJob({ id: "B", dedicatedContext: true });
    const storage = makeMockStorage([jobA, jobB]);
    const pi = makeMockPi();

    let resolveA!: (value: any) => void;
    const aPromise = new Promise<any>((r) => { resolveA = r; });
    let aSignal: AbortSignal | undefined;
    let resolveB!: (value: any) => void;
    const bPromise = new Promise<any>((r) => { resolveB = r; });
    let bSignal: AbortSignal | undefined;

    (pi.exec as any).mockImplementation((_cmd: unknown, _args: unknown, opts: any) => {
      if (!aSignal) {
        aSignal = opts?.signal;
        return aPromise;
      }
      bSignal = opts?.signal;
      return bPromise;
    });

    const scheduler = makeScheduler(storage, pi);

    // Start A.
    void scheduler.runJobNow(jobA.id);
    await Promise.resolve();
    await Promise.resolve();
    expect(pi.exec).toHaveBeenCalledTimes(1);
    expect(aSignal?.aborted).toBe(false);

    // While A hangs, retry B. B should be queued, not fired.
    void scheduler.runJobNow(jobB.id);
    await Promise.resolve();
    await Promise.resolve();
    expect(pi.exec).toHaveBeenCalledTimes(1);
    expect(bSignal).toBeUndefined();
    // CRITICAL: A's signal must NOT have been aborted by enqueueing B.
    expect(aSignal?.aborted).toBe(false);

    // Resolve A. The drain should now fire B.
    const okPayload = {
      stdout: JSON.stringify({
        type: "agent_end",
        messages: [{ role: "assistant", content: [{ type: "text", text: "ok" }], stopReason: "stop" }],
      }),
      stderr: "",
      code: 0,
      killed: false,
    };
    resolveA(okPayload);
    // Drain executeDedicatedJob's post-await microtasks and the drain dispatch.
    for (let i = 0; i < 10; i++) await Promise.resolve();
    expect(pi.exec).toHaveBeenCalledTimes(2);
    expect(bSignal?.aborted).toBe(false);

    resolveB(okPayload);
    await vi.advanceTimersByTimeAsync(0);
  });

  it("two cron jobs whose schedules collide do NOT spawn parallel subprocesses (queued serially)", async () => {
    // Regression for the user's report: two daily routines scheduled for the
    // same minute both fired "Processing begins" notifications at once,
    // overwhelming the model API.
    const jobA = makeJob({ id: "A", dedicatedContext: true });
    const jobB = makeJob({ id: "B", dedicatedContext: true });
    const storage = makeMockStorage([jobA, jobB]);
    const pi = makeMockPi();

    let resolveA!: (value: any) => void;
    const aPromise = new Promise<any>((r) => { resolveA = r; });
    (pi.exec as any).mockImplementationOnce(() => aPromise);
    (pi.exec as any).mockResolvedValue({
      stdout: JSON.stringify({
        type: "agent_end",
        messages: [{ role: "assistant", content: [{ type: "text", text: "ok" }], stopReason: "stop" }],
      }),
      stderr: "",
      code: 0,
      killed: false,
    });

    const scheduler = makeScheduler(storage, pi);

    // Both jobs' cron ticks fire in the same event-loop turn.
    await (scheduler as any).executeJobIfLeader(jobA);
    await (scheduler as any).executeJobIfLeader(jobB);
    await Promise.resolve();
    await Promise.resolve();

    // Only A is running; B is in the queue.
    expect(pi.exec).toHaveBeenCalledTimes(1);

    // A finishes → B drains.
    resolveA({
      stdout: JSON.stringify({
        type: "agent_end",
        messages: [{ role: "assistant", content: [{ type: "text", text: "ok" }], stopReason: "stop" }],
      }),
      stderr: "",
      code: 0,
      killed: false,
    });
    for (let i = 0; i < 10; i++) await Promise.resolve();
    expect(pi.exec).toHaveBeenCalledTimes(2);
  });

  it("dedicated retry is independent of the main-session send/agentRunning gate", async () => {
    // A dedicated retry runs in a subprocess and must NOT be deferred when the main
    // session's agent is busy — the user explicitly asked for a fresh run.
    const dedicated = makeJob({ id: "ded", dedicatedContext: true, prompt: "p" });
    const storage = makeMockStorage([dedicated]);
    const pi = makeMockPi();
    pi.exec.mockResolvedValue({
      stdout: JSON.stringify({
        type: "agent_end",
        messages: [{ role: "assistant", content: [{ type: "text", text: "ok" }], stopReason: "stop" }],
      }),
      stderr: "",
      code: 0,
      killed: false,
    });
    const scheduler = makeScheduler(storage, pi);

    // Main session's user turn is active.
    scheduler.notifyAgentStart();

    await scheduler.runJobNow(dedicated.id);
    await vi.advanceTimersByTimeAsync(0);

    // Dedicated subprocess fires immediately, regardless of agentRunning.
    expect(pi.exec).toHaveBeenCalledTimes(1);
    expect(pi.sendUserMessage).not.toHaveBeenCalled();
  });
});

// ---- start(): missed-job triage (Bug 1: /new must not refire failed jobs) ----
//
// After /new, scheduler.start() iterates jobs and historically fired any guaranteed
// job in lastStatus=error|sent immediately. With three dedicated jobs that's three
// pi subprocesses launched in parallel — the user observed 2/3 timing out at the
// 5-min mark. Fix: route error/sent state through the retry timer.

// ---- recursive-spawn prevention for dedicated cron jobs (Bug 2) ----
//
// Commit de11762 ("pick up scheduled recurring jobs that didn't fire") added
// a missed-job fire-on-start path. For a dedicated cron job, the parent pi
// fires the job and spawns `pi --mode json -p ...`. That child loads its own
// pi-schedule-prompt module and runs scheduler.start() — sees the same job
// in storage with lastRun from the prior tick and lastStatus="running" (set
// by the parent at scheduler.ts:1188). isMissed() returns true, and:
//   - Path A (guaranteed, lines 237-246): "running" is not in {error,sent}
//     so the else branch fires.
//   - Path B (non-guaranteed recurring, lines 255-264): no status guard at
//     all — always fires.
// Either path dispatches enqueueOrRunSubprocess → executeDedicatedJob →
// pi.exec("pi", ...), spawning a grandchild. Each layer is a live pi process
// running the same workflow concurrently — the host OOMs.

describe("start(): does not recursively spawn when a dedicated job is already running (lastStatus=running)", () => {
  function makeCronJob(overrides: Partial<CronJob> = {}): CronJob {
    return {
      id: "cron-r",
      name: "Daily Routine",
      schedule: "0 30 22 * * *",
      prompt: "do daily things",
      enabled: true,
      type: "cron",
      runCount: 1,
      createdAt: new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString(),
      lastRun: new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString(),
      guaranteed: true,
      // A LIVE owner (this test process) with a fresh run-start represents the
      // parent pi that set "running" and is still executing the job — stale-running
      // recovery must leave it alone so start()'s fork-bomb guard still applies.
      runnerPid: process.pid,
      runStartedAt: new Date().toISOString(),
      ...overrides,
    };
  }

  it("guaranteed dedicated cron in lastStatus=running with lastRun from a prior tick does NOT re-fire (Path A)", async () => {
    const job = makeCronJob({
      dedicatedContext: true,
      guaranteed: true,
      lastStatus: "running",
    });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    const scheduler = makeScheduler(storage, pi);

    scheduler.start();
    await Promise.resolve();
    await Promise.resolve();

    expect(pi.exec).not.toHaveBeenCalled();
  });

  it("non-guaranteed dedicated cron in lastStatus=running with lastRun from a prior tick does NOT re-fire (Path B)", async () => {
    const job = makeCronJob({
      dedicatedContext: true,
      guaranteed: false,
      lastStatus: "running",
    });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    const scheduler = makeScheduler(storage, pi);

    scheduler.start();
    await Promise.resolve();
    await Promise.resolve();

    expect(pi.exec).not.toHaveBeenCalled();
  });

  it("guaranteed dedicated interval in lastStatus=running with lastRun older than intervalMs does NOT re-fire", async () => {
    const job = makeJob({
      id: "int-r",
      name: "Hourly Routine",
      type: "interval",
      intervalMs: 60 * 60 * 1000,
      schedule: "60m",
      dedicatedContext: true,
      guaranteed: true,
      lastRun: new Date(Date.now() - 2 * 60 * 60 * 1000).toISOString(),
      lastStatus: "running",
      // Live owner + fresh run-start = a parent pi still executing this run.
      runnerPid: process.pid,
      runStartedAt: new Date().toISOString(),
      runCount: 1,
    });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    const scheduler = makeScheduler(storage, pi);

    scheduler.start();
    await Promise.resolve();
    await Promise.resolve();

    expect(pi.exec).not.toHaveBeenCalled();
  });
});

// ---- stale-"running" recovery on startup (crashed/killed runs) ----
//
// A run sets lastStatus="running" and only writes a terminal status once it
// settles. If the owning process dies first (host crash, kill, AbortError paths
// that return without touching storage), the job is frozen at "running" forever
// — and start() deliberately refuses to re-fire "running" jobs (fork-bomb
// guard), so it never auto-recovers. recoverStaleRunningJobs() resets such jobs
// to "error" IFF no live process owns the run, using the per-job runnerPid +
// runStartedAt recorded at the "running" transition. A LIVE, recent owner (the
// parent pi still executing a dedicated job) must be left untouched.
describe("start(): recovers stale 'running' jobs whose owner died", () => {
  // Any pid the OS reports as non-existent. On Linux pids stay well below 2**22,
  // so 2**30 is reliably dead; guard by confirming ESRCH just in case.
  function findDeadPid(): number {
    for (let pid = 2 ** 30; pid > 2 ** 20; pid--) {
      try {
        process.kill(pid, 0);
      } catch (e: any) {
        if (e.code === "ESRCH") return pid;
      }
    }
    return 2 ** 30;
  }

  function makeRunningCron(overrides: Partial<CronJob> = {}): CronJob {
    return {
      id: "stuck",
      name: "Daily Routine",
      schedule: "0 30 22 * * *",
      prompt: "do daily things",
      enabled: true,
      type: "cron",
      runCount: 5,
      createdAt: new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString(),
      lastRun: new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString(),
      guaranteed: true,
      lastStatus: "running",
      dedicatedContext: true,
      ...overrides,
    };
  }

  it("resets a legacy stuck job (no runnerPid/runStartedAt) to 'error'", async () => {
    // The real-world case: 7 jobs frozen at "running" from before this field existed.
    const job = makeRunningCron();
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    const scheduler = makeScheduler(storage, pi);

    scheduler.start();
    await Promise.resolve();
    await Promise.resolve();

    expect(storage._store.get("stuck")?.lastStatus).toBe("error");
    // Guaranteed → routed through the retry timer, not an immediate re-fire.
    expect(pi.exec).not.toHaveBeenCalled();
  });

  it("resets a job whose runnerPid is dead", async () => {
    const job = makeRunningCron({ runnerPid: findDeadPid(), runStartedAt: new Date().toISOString() });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    const scheduler = makeScheduler(storage, pi);

    scheduler.start();
    await Promise.resolve();
    await Promise.resolve();

    expect(storage._store.get("stuck")?.lastStatus).toBe("error");
  });

  it("resets a job whose runStartedAt predates the max run window even if runnerPid is alive (pid-recycling backstop)", async () => {
    const job = makeRunningCron({
      runnerPid: process.pid, // alive, but the run started too long ago to be real
      runStartedAt: new Date(Date.now() - 2 * 60 * 60 * 1000).toISOString(),
    });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    const scheduler = makeScheduler(storage, pi);

    scheduler.start();
    await Promise.resolve();
    await Promise.resolve();

    expect(storage._store.get("stuck")?.lastStatus).toBe("error");
  });

  it("does NOT reset a job with a live owner and a fresh run-start (parent still executing)", async () => {
    const job = makeRunningCron({ runnerPid: process.pid, runStartedAt: new Date().toISOString() });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    const scheduler = makeScheduler(storage, pi);

    scheduler.start();
    await Promise.resolve();
    await Promise.resolve();

    expect(storage._store.get("stuck")?.lastStatus).toBe("running");
    expect(pi.exec).not.toHaveBeenCalled();
  });

  it("catches up a non-guaranteed recurring job once recovered (handled as if it did not run)", async () => {
    const job = makeRunningCron({ guaranteed: false }); // legacy stuck, non-guaranteed
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    const scheduler = makeScheduler(storage, pi);

    scheduler.start();
    for (let i = 0; i < 10; i++) await Promise.resolve();
    await vi.advanceTimersByTimeAsync(0);

    // Reset to "error" (see below), then start()'s missed-recurring path fires
    // the catch-up run — the dedicated subprocess is spawned.
    expect(pi.exec).toHaveBeenCalledTimes(1);
  });
});

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

// ---- formatDedicatedRunOutput: session id capture (standalone/enter) ----

describe("formatDedicatedRunOutput: captures the session id from the header", () => {
  it("extracts id from the first {type:'session'} event", () => {
    const jsonl = [
      JSON.stringify({ type: "session", version: 3, id: "sess-abc123", timestamp: "2026-06-11T10:00:00.000Z", cwd: "/work" }),
      JSON.stringify({ type: "agent_end", messages: [{ role: "assistant", content: [{ type: "text", text: "done" }], stopReason: "stop" }] }),
    ].join("\n");
    const { sessionId } = formatDedicatedRunOutput(jsonl, "", 0, false, 1);
    expect(sessionId).toBe("sess-abc123");
  });

  it("returns null when no session header is present", () => {
    const jsonl = JSON.stringify({ type: "agent_end", messages: [] });
    const { sessionId } = formatDedicatedRunOutput(jsonl, "", 0, false, 1);
    expect(sessionId).toBeNull();
  });

  it("ignores a session event missing a string id", () => {
    const jsonl = JSON.stringify({ type: "session", timestamp: "t", cwd: "/work" });
    const { sessionId } = formatDedicatedRunOutput(jsonl, "", 0, false, 1);
    expect(sessionId).toBeNull();
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
    const out = "[exit=0 killed=true duration=1200s]\n\n[user]\nlong prompt";
    expect(detectFailureHint(out)).toMatch(/-minute timeout/);
  });

  it("prefers the more specific guardrails hint over the generic timeout hint", () => {
    // A run that's both killed and contains a guardrails block — guardrails is the
    // actionable cause; the timeout is the symptom. The first match wins.
    const out = "[exit=0 killed=true duration=1200s]\n\n[result: write (error)]\nAccess to /tmp/foo is blocked (outside working directory, no UI to confirm).";
    const hint = detectFailureHint(out);
    expect(hint).toContain("guardrails");
    expect(hint).not.toMatch(/-minute timeout/);
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

// ============================================================================
// Dedicated-context preservation across session replacement
// ----------------------------------------------------------------------------
// The user-visible bug: a long-running `dedicatedContext` subprocess was killed
// by `/new`/`/fork`/`/resume`/`/reload` because session_shutdown invokes
// scheduler.stop(), which aborts every dedicated-job AbortController. The fix
// teaches stop() to differentiate "quit" (host exiting — kill children, fine)
// from session-replace reasons (host keeps running — leave subprocesses alive
// so their work isn't thrown away). In-flight tracking is lifted to module
// scope so a freshly-constructed scheduler in the new session can still see
// what's running and avoid double-firing.
// ============================================================================

describe("stop({ reason }) preserves dedicated subprocesses across session replacement", () => {
  // Helper: kick off a dedicated job whose pi.exec hangs, capturing the abort
  // signal so the test can assert whether stop() aborted the subprocess.
  async function startHangingDedicatedJob(
    storage: ReturnType<typeof makeMockStorage>,
    pi: ReturnType<typeof makeMockPi>,
    scheduler: CronScheduler,
    jobId: string
  ): Promise<{ signal: AbortSignal | undefined; resolveExec: (value: any) => void }> {
    let resolveExec!: (value: any) => void;
    let signal: AbortSignal | undefined;
    (pi.exec as any).mockImplementationOnce((_cmd: unknown, _args: unknown, opts: any) => {
      signal = opts?.signal;
      return new Promise((r) => { resolveExec = r; });
    });
    void scheduler.runJobNow(jobId);
    // Give executeDedicatedJob a chance to register and call pi.exec.
    for (let i = 0; i < 10; i++) await Promise.resolve();
    return { signal, resolveExec };
  }

  const REPLACE_REASONS: ReadonlyArray<"new" | "fork" | "resume" | "reload"> = [
    "new",
    "fork",
    "resume",
    "reload",
  ];

  it.each(REPLACE_REASONS)(
    "stop({ reason: %s }) does NOT abort in-flight dedicated subprocesses",
    async (reason) => {
      const job = makeJob({ id: "ded", dedicatedContext: true });
      const storage = makeMockStorage([job]);
      const pi = makeMockPi();
      const scheduler = makeScheduler(storage, pi);

      const { signal, resolveExec } = await startHangingDedicatedJob(storage, pi, scheduler, job.id);
      expect(signal?.aborted).toBe(false);

      (scheduler as any).stop({ reason });

      expect(signal?.aborted).toBe(false);

      // Cleanup: resolve so the pending pi.exec doesn't leak.
      resolveExec({
        stdout: JSON.stringify({
          type: "agent_end",
          messages: [{ role: "assistant", content: [{ type: "text", text: "ok" }], stopReason: "stop" }],
        }),
        stderr: "",
        code: 0,
        killed: false,
      });
      for (let i = 0; i < 30; i++) await Promise.resolve();
      await vi.advanceTimersByTimeAsync(0);
    }
  );

  it("stop({ reason: 'quit' }) DOES abort in-flight dedicated subprocesses", async () => {
    const job = makeJob({ id: "ded", dedicatedContext: true });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    const scheduler = makeScheduler(storage, pi);

    const { signal, resolveExec } = await startHangingDedicatedJob(storage, pi, scheduler, job.id);
    expect(signal?.aborted).toBe(false);

    (scheduler as any).stop({ reason: "quit" });

    expect(signal?.aborted).toBe(true);

    // Cleanup: resolve with a killed payload so the pending exec settles.
    resolveExec({ stdout: "", stderr: "", code: 0, killed: true });
    for (let i = 0; i < 30; i++) await Promise.resolve();
    await vi.advanceTimersByTimeAsync(0);
  });

  it("default stop() (no args) behaves like { reason: 'quit' } (backwards compat)", async () => {
    const job = makeJob({ id: "ded", dedicatedContext: true });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    const scheduler = makeScheduler(storage, pi);

    const { signal, resolveExec } = await startHangingDedicatedJob(storage, pi, scheduler, job.id);
    scheduler.stop();
    expect(signal?.aborted).toBe(true);

    resolveExec({ stdout: "", stderr: "", code: 0, killed: true });
    for (let i = 0; i < 30; i++) await Promise.resolve();
    await vi.advanceTimersByTimeAsync(0);
  });

  it("after stop({ reason: 'new' }), the in-flight subprocess's completion still writes a run record and does NOT call notify", async () => {
    const job = makeJob({ id: "ded", dedicatedContext: true, prompt: "hello" });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    const scheduler = makeScheduler(storage, pi);

    const { resolveExec } = await startHangingDedicatedJob(storage, pi, scheduler, job.id);

    // Snapshot how many sendMessage calls happened during the begin notify.
    const sendCallsBeforeStop = pi.sendMessage.mock.calls.length;

    (scheduler as any).stop({ reason: "new" });

    // Resolve the subprocess with a successful payload.
    resolveExec({
      stdout: JSON.stringify({
        type: "agent_end",
        messages: [
          { role: "assistant", content: [{ type: "text", text: "done" }], stopReason: "stop" },
        ],
      }),
      stderr: "",
      code: 0,
      killed: false,
    });
    for (let i = 0; i < 50; i++) await Promise.resolve();
    await vi.advanceTimersByTimeAsync(0);

    // Run record is written despite scheduler being stopped.
    expect(storage.addRunRecord).toHaveBeenCalledTimes(1);
    const record = storage.addRunRecord.mock.calls[0][0];
    expect(record.jobId).toBe(job.id);
    expect(record.status).toBe("success");
    expect(record.output).toContain("done");

    // notify("scheduled_prompt_end") is suppressed: no NEW sendMessage after stop.
    expect(pi.sendMessage.mock.calls.length).toBe(sendCallsBeforeStop);
  });

  it("a fresh scheduler started against the same storage does NOT double-fire a job already in-flight from the prior instance", async () => {
    // Setup a guaranteed once-job that's past-due. Scheduler A fires it; the
    // subprocess hangs; A is stopped with reason "new" (no abort). Scheduler B
    // (simulating the post-/new session) is created against the same storage
    // and started — without the cross-instance dedup it would happily start a
    // second pi.exec because storage shows the job with lastStatus="running"
    // and isMissed=true.
    const job = makeJob({
      id: "j",
      dedicatedContext: true,
      guaranteed: true,
      schedule: new Date(Date.now() - 60_000).toISOString(),
    });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    const schedulerA = makeScheduler(storage, pi);

    const { signal, resolveExec } = await startHangingDedicatedJob(storage, pi, schedulerA, job.id);
    expect(pi.exec).toHaveBeenCalledTimes(1);
    expect(signal?.aborted).toBe(false);

    (schedulerA as any).stop({ reason: "new" });
    expect(signal?.aborted).toBe(false);

    const schedulerB = makeScheduler(storage, pi);
    schedulerB.start();
    // Drain any synchronous fire path in start().
    for (let i = 0; i < 10; i++) await Promise.resolve();
    await vi.advanceTimersByTimeAsync(0);

    // No second subprocess started by B.
    expect(pi.exec).toHaveBeenCalledTimes(1);
    expect(signal?.aborted).toBe(false);

    // Resolve A's run; the run record should land exactly once.
    resolveExec({
      stdout: JSON.stringify({
        type: "agent_end",
        messages: [
          { role: "assistant", content: [{ type: "text", text: "done" }], stopReason: "stop" },
        ],
      }),
      stderr: "",
      code: 0,
      killed: false,
    });
    for (let i = 0; i < 50; i++) await Promise.resolve();
    await vi.advanceTimersByTimeAsync(0);

    expect(storage.addRunRecord).toHaveBeenCalledTimes(1);

    // Cleanup: stop B with quit so any timers it scheduled are torn down.
    (schedulerB as any).stop({ reason: "quit" });
  });

  it("runJobNow on a fresh scheduler returns 'queued' for a job that's still in-flight from the prior instance", async () => {
    const job = makeJob({ id: "j", dedicatedContext: true });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    const schedulerA = makeScheduler(storage, pi);

    const { signal, resolveExec } = await startHangingDedicatedJob(storage, pi, schedulerA, job.id);
    expect(pi.exec).toHaveBeenCalledTimes(1);

    (schedulerA as any).stop({ reason: "new" });

    const schedulerB = makeScheduler(storage, pi);
    schedulerB.start();
    for (let i = 0; i < 10; i++) await Promise.resolve();

    const disposition = await schedulerB.runJobNow(job.id);
    expect(disposition).toBe("queued");
    expect(pi.exec).toHaveBeenCalledTimes(1);
    expect(signal?.aborted).toBe(false);

    // When A's run resolves, the queued retry should fire as a SECOND subprocess.
    resolveExec({
      stdout: JSON.stringify({
        type: "agent_end",
        messages: [
          { role: "assistant", content: [{ type: "text", text: "done" }], stopReason: "stop" },
        ],
      }),
      stderr: "",
      code: 0,
      killed: false,
    });
    for (let i = 0; i < 50; i++) await Promise.resolve();
    await vi.advanceTimersByTimeAsync(0);

    expect(pi.exec).toHaveBeenCalledTimes(2);

    (schedulerB as any).stop({ reason: "quit" });
  });

  it("getDedicatedActivity exposes in-flight job metadata for /schedule-prompt ps", async () => {
    const job = makeJob({ id: "ded-act", name: "Activity Test", dedicatedContext: true, prompt: "do work" });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    const scheduler = makeScheduler(storage, pi);

    expect(getDedicatedActivity().inFlight).toHaveLength(0);

    const { resolveExec } = await startHangingDedicatedJob(storage, pi, scheduler, job.id);

    const activity = getDedicatedActivity();
    expect(activity.inFlight).toHaveLength(1);
    expect(activity.inFlight[0]).toMatchObject({
      jobId: "ded-act",
      jobName: "Activity Test",
      prompt: "do work",
    });
    expect(typeof activity.inFlight[0].startTime).toBe("string");

    // Queue a retry; ps should show it.
    await scheduler.runJobNow(job.id);
    expect(getDedicatedActivity().queuedRetries).toContain("ded-act");

    // Cleanup.
    resolveExec({
      stdout: JSON.stringify({
        type: "agent_end",
        messages: [{ role: "assistant", content: [{ type: "text", text: "ok" }], stopReason: "stop" }],
      }),
      stderr: "",
      code: 0,
      killed: false,
    });
    for (let i = 0; i < 50; i++) await Promise.resolve();
    await vi.advanceTimersByTimeAsync(0);
    (scheduler as any).stop({ reason: "quit" });
    expect(getDedicatedActivity().inFlight).toHaveLength(0);
    expect(getDedicatedActivity().queuedRetries).toHaveLength(0);
  });

  it("stop({ reason: 'new' }) preserves a pending dedicated retry so it fires after the in-flight run completes", async () => {
    // User scenario: dedicated job A is running. User does /schedule-prompt retry A
    // (queues a follow-up). User then types /new. The retry intent must survive.
    const job = makeJob({ id: "j", dedicatedContext: true });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    const scheduler = makeScheduler(storage, pi);

    const { resolveExec } = await startHangingDedicatedJob(storage, pi, scheduler, job.id);

    // Queue a retry while in-flight.
    const queuedDisposition = await scheduler.runJobNow(job.id);
    expect(queuedDisposition).toBe("queued");
    expect(pi.exec).toHaveBeenCalledTimes(1);

    (scheduler as any).stop({ reason: "new" });

    // Resolve the original run.
    resolveExec({
      stdout: JSON.stringify({
        type: "agent_end",
        messages: [
          { role: "assistant", content: [{ type: "text", text: "done" }], stopReason: "stop" },
        ],
      }),
      stderr: "",
      code: 0,
      killed: false,
    });
    for (let i = 0; i < 50; i++) await Promise.resolve();
    await vi.advanceTimersByTimeAsync(0);

    // The queued retry fired despite stop() in between.
    expect(pi.exec).toHaveBeenCalledTimes(2);

    // Cleanup pending second exec.
    for (let i = 0; i < 30; i++) await Promise.resolve();
    await vi.advanceTimersByTimeAsync(0);
  });
});

describe("subprocess slot recovery (stuck-slot regression)", () => {
  // Regression for the production incident where command-mode scheduled prompts
  // silently stopped firing: a leaked in-flight subprocess entry left the
  // host-wide slot busy forever, so every later command/dedicated job queued and
  // never dispatched (pi emitted nothing at fire time). Two guards fix it — the
  // AbortError path now drains, and a stale-slot watchdog evicts leaked entries.
  function makeCommandJob(overrides: Partial<CronJob> = {}): CronJob {
    return makeJob({ command: true, prompt: "echo hi", ...overrides });
  }

  it("drains the queued command job after the in-flight one is aborted (drain not skipped on AbortError)", async () => {
    const jobA = makeCommandJob({ id: "A", prompt: "echo A" });
    const jobB = makeCommandJob({ id: "B", prompt: "echo B" });
    const storage = makeMockStorage([jobA, jobB]);
    const pi = makeMockPi();

    let rejectA!: (err: unknown) => void;
    const aPromise = new Promise<any>((_resolve, reject) => { rejectA = reject; });
    (pi.exec as any).mockImplementationOnce(() => aPromise); // A hangs in the slot
    (pi.exec as any).mockResolvedValue({ stdout: "B-out", stderr: "", code: 0, killed: false });

    const scheduler = makeScheduler(storage, pi);

    // A occupies the single subprocess slot.
    void scheduler.runJobNow(jobA.id);
    for (let i = 0; i < 5; i++) await Promise.resolve();
    expect(pi.exec).toHaveBeenCalledTimes(1);

    // B enqueues behind the busy slot — must not run in parallel.
    void scheduler.runJobNow(jobB.id);
    for (let i = 0; i < 5; i++) await Promise.resolve();
    expect(pi.exec).toHaveBeenCalledTimes(1);

    // Abort A (as host-quit or the stale-slot watchdog does). The catch must
    // free AND drain the slot — the bug was the early return skipping the drain.
    const abortErr = new Error("aborted");
    (abortErr as Error & { name: string }).name = "AbortError";
    rejectA(abortErr);
    for (let i = 0; i < 20; i++) await Promise.resolve();

    expect(pi.exec).toHaveBeenCalledTimes(2);
    expect((pi.exec as any).mock.calls[1]).toEqual(["bash", ["-c", "echo B"], expect.anything()]);

    for (let i = 0; i < 10; i++) await Promise.resolve();
    await vi.advanceTimersByTimeAsync(0);
  });

  it("evicts a leaked in-flight subprocess after the stale timeout and drains the queue", async () => {
    const jobA = makeCommandJob({ id: "A", prompt: "echo A" });
    const jobB = makeCommandJob({ id: "B", prompt: "echo B" });
    const storage = makeMockStorage([jobA, jobB]);
    const pi = makeMockPi();

    // A's exec never settles — simulates a child whose own timeout never fired,
    // so the finally that frees the slot never runs and the entry leaks.
    const aPromise = new Promise<any>(() => {});
    (pi.exec as any).mockImplementationOnce(() => aPromise);
    (pi.exec as any).mockResolvedValue({ stdout: "B-out", stderr: "", code: 0, killed: false });

    const scheduler = makeScheduler(storage, pi);

    void scheduler.runJobNow(jobA.id);
    for (let i = 0; i < 5; i++) await Promise.resolve();
    void scheduler.runJobNow(jobB.id);
    for (let i = 0; i < 5; i++) await Promise.resolve();
    expect(pi.exec).toHaveBeenCalledTimes(1); // A in-flight, B queued

    // Before the stale threshold the sweep is a no-op — A is still "fresh".
    scheduler.sweepStaleSubprocesses();
    for (let i = 0; i < 5; i++) await Promise.resolve();
    expect(pi.exec).toHaveBeenCalledTimes(1);

    // Past the threshold (exec timeout + grace), the sweep evicts A and the slot
    // is recoverable: B finally dispatches. Threshold tracks DEDICATED_JOB_TIMEOUT_MS
    // (60m) + SUBPROCESS grace (2m), so advance past 62m.
    await vi.advanceTimersByTimeAsync(60 * 60 * 1000 + 3 * 60 * 1000);
    scheduler.sweepStaleSubprocesses();
    for (let i = 0; i < 20; i++) await Promise.resolve();

    expect(pi.exec).toHaveBeenCalledTimes(2);
    expect((pi.exec as any).mock.calls[1]).toEqual(["bash", ["-c", "echo B"], expect.anything()]);

    for (let i = 0; i < 10; i++) await Promise.resolve();
    await vi.advanceTimersByTimeAsync(0);
  });
});

describe("notify emits unconditionally (silent-park regression)", () => {
  // Regression for the production incident where command-mode scheduled prompts
  // ran successfully (storage updated, run record captured) but never reached
  // the Signal bridge. Root cause: the notify helper used `deliverAs:"nextTurn"`
  // whenever `this.agentRunning` was true. pi-mono's sendCustomMessage parks
  // nextTurn messages in `_pendingNextTurnMessages` without emitting
  // message_start/message_end — they only fire when the next user prompt
  // drains the queue. `agentRunning` can stick true (overflow recovery
  // suppresses agent_end; stale scheduler bindings miss their session's
  // events), so once stuck every command_end was silently swallowed.
  //
  // The fix: always pass opts without `deliverAs` so sendCustomMessage hits
  // the non-streaming else branch (immediate _emit) or steers during an
  // active turn — never the silent-park branch.

  function makeCommandJob(overrides: Partial<CronJob> = {}): CronJob {
    return makeJob({ command: true, prompt: "echo hi", ...overrides });
  }

  it("command_end emits without deliverAs even when agentRunning is true", async () => {
    const job = makeCommandJob({ id: "cmd-A", targetContext: "+alice" });
    const storage = makeMockStorage([job]);
    const pi = {
      ...makeMockPi(),
      sendMessageToContext: vi.fn(),
      sendUserMessageToContext: vi.fn(),
    };
    (pi.exec as any).mockResolvedValue({ stdout: "hello\n", stderr: "", code: 0, killed: false });

    const scheduler = makeScheduler(storage, pi as any);
    // Force the sticky-true state that triggered the prod bug.
    (scheduler as any).agentRunning = true;

    await executeJob(scheduler, job);
    for (let i = 0; i < 20; i++) await Promise.resolve();

    // The command_end notify must reach pi-mono. Find the call carrying it.
    const commandEndCall = pi.sendMessageToContext.mock.calls.find(
      ([, msg]) => msg?.customType === "scheduled_prompt_command_end"
    );
    expect(commandEndCall).toBeDefined();
    expect(commandEndCall![0]).toBe("+alice");
    // If a third positional argument is passed, it must not steer pi-mono into
    // the silent-park branch.
    const opts = commandEndCall![2];
    expect(opts?.deliverAs).not.toBe("nextTurn");
  });

  it("command_end emits via sendMessage when no targetContext, also without nextTurn", async () => {
    const job = makeCommandJob({ id: "cmd-B", targetContext: undefined });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi();
    (pi.exec as any).mockResolvedValue({ stdout: "hello\n", stderr: "", code: 0, killed: false });

    const scheduler = makeScheduler(storage, pi);
    (scheduler as any).agentRunning = true;

    await executeJob(scheduler, job);
    for (let i = 0; i < 20; i++) await Promise.resolve();

    const commandEndCall = pi.sendMessage.mock.calls.find(
      ([msg]: any[]) => msg?.customType === "scheduled_prompt_command_end"
    );
    expect(commandEndCall).toBeDefined();
    const opts = commandEndCall![1];
    expect(opts?.deliverAs).not.toBe("nextTurn");
  });

  it("dedicated begin/end emit without deliverAs even when agentRunning is true", async () => {
    const job = makeJob({
      id: "ded-A",
      dedicatedContext: true,
      targetContext: "+alice",
      prompt: "do the work",
    });
    const storage = makeMockStorage([job]);
    const pi = {
      ...makeMockPi(),
      sendMessageToContext: vi.fn(),
      sendUserMessageToContext: vi.fn(),
    };
    // Dedicated jobs spawn `pi --mode json -p` and parse its JSONL output. A
    // minimal agent_end event is enough to satisfy `formatDedicatedRunOutput`.
    const agentEnd = JSON.stringify({
      type: "agent_end",
      messages: [{ role: "assistant", content: [{ type: "text", text: "ok" }] }],
    });
    (pi.exec as any).mockResolvedValue({ stdout: agentEnd + "\n", stderr: "", code: 0, killed: false });

    const scheduler = makeScheduler(storage, pi as any);
    (scheduler as any).agentRunning = true;

    await executeJob(scheduler, job);
    for (let i = 0; i < 20; i++) await Promise.resolve();

    const calls = pi.sendMessageToContext.mock.calls.filter(
      ([, msg]: any[]) =>
        msg?.customType === "scheduled_prompt_begin" ||
        msg?.customType === "scheduled_prompt_end"
    );
    expect(calls.length).toBeGreaterThanOrEqual(1);
    for (const call of calls) {
      const opts = call[2];
      expect(opts?.deliverAs).not.toBe("nextTurn");
    }
  });

  it("dedicated job's prompt text is NOT in content[0].text (steer-safety invariant)", async () => {
    // The structural mitigation that makes opts={} safe for dedicated jobs:
    // the prompt is exposed via details.prompt, never via content[0].text.
    // If this invariant ever regresses, a steered notification could feed the
    // host agent the dedicated workflow and trigger recursive execution.
    const job = makeJob({
      id: "ded-B",
      dedicatedContext: true,
      targetContext: "+alice",
      prompt: "PROMPT_THAT_MUST_NOT_LEAK",
    });
    const storage = makeMockStorage([job]);
    const pi = {
      ...makeMockPi(),
      sendMessageToContext: vi.fn(),
      sendUserMessageToContext: vi.fn(),
    };
    const agentEnd = JSON.stringify({
      type: "agent_end",
      messages: [{ role: "assistant", content: [{ type: "text", text: "ok" }] }],
    });
    (pi.exec as any).mockResolvedValue({ stdout: agentEnd + "\n", stderr: "", code: 0, killed: false });

    const scheduler = makeScheduler(storage, pi as any);
    await executeJob(scheduler, job);
    for (let i = 0; i < 20; i++) await Promise.resolve();

    for (const [, msg] of pi.sendMessageToContext.mock.calls as any[]) {
      const text = msg?.content?.[0]?.text ?? "";
      expect(text).not.toContain("PROMPT_THAT_MUST_NOT_LEAK");
    }
  });
});

// ---- Authoritative idle-gate recovery (stuck agentRunning/sending mirror) ----
//
// In RPC/multi-context mode agent_start/agent_end can be missed, leaving the
// gate flags stuck "busy" so scheduled fires defer forever and only drain on the
// next user interaction. When pi authoritatively reports idle (ExtensionAPI.isIdle)
// past the post-send grace window, the scheduler must treat the mirror as stale
// and let the fire proceed.

const GRACE_MS = 60 * 1000;

describe("authoritative idle-gate recovery", () => {
  it("fires a deferred-eligible job when pi reports idle but the mirror is stuck busy", async () => {
    const job = makeJob({ prompt: "reminder" });
    const storage = makeMockStorage([job]);
    const pi = { ...makeMockPi(), isIdle: vi.fn(() => true) };
    const scheduler = makeScheduler(storage, pi as any);

    // Simulate a stuck mirror: a prior turn's agent_end was missed.
    (scheduler as any).agentRunning = true;
    // lastSendAt defaults to 0, so we're well past the grace window.

    await executeJob(scheduler, job);

    // Gate recovered → the once job fired and was removed (not left deferred).
    expect(pi.sendUserMessage).toHaveBeenCalledTimes(1);
    expect(storage.removeJob).toHaveBeenCalledWith(job.id);
    expect((scheduler as any).deferredActions).toHaveLength(0);
  });

  it("does NOT recover within the post-send grace window (avoids double-fire)", async () => {
    const job = makeJob({ prompt: "reminder" });
    const storage = makeMockStorage([job]);
    const pi = { ...makeMockPi(), isIdle: vi.fn(() => true) };
    const scheduler = makeScheduler(storage, pi as any);

    // A send was just issued; its turn may not have started streaming yet, so a
    // transient idle report must NOT be treated as a stuck gate.
    (scheduler as any).sending = true;
    (scheduler as any).lastSendAt = Date.now();

    await executeJob(scheduler, job);

    expect(pi.sendUserMessage).not.toHaveBeenCalled();
    expect((scheduler as any).deferredActions).toHaveLength(1);
  });

  it("does NOT recover while pi reports busy", async () => {
    const job = makeJob({ prompt: "reminder" });
    const storage = makeMockStorage([job]);
    const pi = { ...makeMockPi(), isIdle: vi.fn(() => false) };
    const scheduler = makeScheduler(storage, pi as any);

    (scheduler as any).agentRunning = true; // mirror busy AND pi genuinely busy

    await executeJob(scheduler, job);

    expect(pi.sendUserMessage).not.toHaveBeenCalled();
    expect((scheduler as any).deferredActions).toHaveLength(1);
  });

  it("preserves legacy deferral when the runtime lacks isIdle()", async () => {
    const job = makeJob({ prompt: "reminder" });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi(); // no isIdle — older pi runtime
    const scheduler = makeScheduler(storage, pi as any);

    (scheduler as any).agentRunning = true;

    await executeJob(scheduler, job);

    // Unknown idle state → fall back to mirror-only behaviour (defer).
    expect(pi.sendUserMessage).not.toHaveBeenCalled();
    expect((scheduler as any).deferredActions).toHaveLength(1);
  });

  it("watchdog backstop drains a genuinely-deferred job once pi goes idle", async () => {
    const job = makeJob({ id: "job-deferred", prompt: "later" });
    const storage = makeMockStorage([job]);
    // Busy at first, so the fire defers through the normal gate.
    const pi = { ...makeMockPi(), isIdle: vi.fn(() => false) };
    const scheduler = makeScheduler(storage, pi as any);

    (scheduler as any).agentRunning = true;
    await executeJob(scheduler, job);
    expect(pi.sendUserMessage).not.toHaveBeenCalled();
    expect((scheduler as any).deferredActions).toHaveLength(1);

    // pi goes idle, but no agent_end ever advances the deferred queue.
    pi.isIdle.mockReturnValue(true);
    (scheduler as any).lastSendAt = Date.now() - GRACE_MS - 1;

    // Watchdog backstop kicks the queue; inner setTimeout(0) needs a non-zero flush.
    (scheduler as any).recoverDeferredIfIdle();
    await vi.advanceTimersByTimeAsync(1);

    expect(pi.sendUserMessage).toHaveBeenCalledTimes(1);
  });
});

// ---- Positive idle-gate: defer when pi reports busy even if mirror flags are clear ----
//
// The hijack regression: a scheduled prompt fired while the user was working got
// sent because the gate only consulted agentRunning/sending/retrying. Those mirror
// flags can diverge from pi's real isStreaming (agent_end suppressed on overflow
// recovery, events missed in RPC/multi-context turns). pi.isIdle() (=!isStreaming)
// is authoritative and stays true for the whole multi-tool task, so the gate must
// defer on isIdle()===false regardless of the mirror.
describe("positive idle-gate: defers on authoritative pi busy state (hijack regression)", () => {
  it("defers when pi reports busy even though all mirror flags are clear", async () => {
    const job = makeJob({ prompt: "scheduled reminder" });
    const storage = makeMockStorage([job]);
    // pi is streaming a user turn, but the mirror never saw agent_start.
    const pi = { ...makeMockPi(), isIdle: vi.fn(() => false) };
    const scheduler = makeScheduler(storage, pi as any);

    // agentRunning/sending/retrying all default false — the pre-fix gate would send.
    await executeJob(scheduler, job);

    // BUG (pre-fix): sendUserMessage hijacks the user's in-flight turn.
    // EXPECTED: deferred, not sent.
    expect(pi.sendUserMessage).not.toHaveBeenCalled();
    expect((scheduler as any).deferredActions).toHaveLength(1);
  });

  it("fires immediately when pi reports idle and mirror flags are clear", async () => {
    const job = makeJob({ prompt: "scheduled reminder" });
    const storage = makeMockStorage([job]);
    const pi = { ...makeMockPi(), isIdle: vi.fn(() => true) };
    const scheduler = makeScheduler(storage, pi as any);

    await executeJob(scheduler, job);

    expect(pi.sendUserMessage).toHaveBeenCalledTimes(1);
    expect((scheduler as any).deferredActions).toHaveLength(0);
  });

  it("drains the deferred prompt as its own turn once pi goes idle", async () => {
    const job = makeJob({ id: "job-wait", prompt: "later reminder" });
    const storage = makeMockStorage([job]);
    const pi = { ...makeMockPi(), isIdle: vi.fn(() => false) };
    const scheduler = makeScheduler(storage, pi as any);

    // Fires mid-task → deferred on authoritative busy.
    await executeJob(scheduler, job);
    expect(pi.sendUserMessage).not.toHaveBeenCalled();
    expect((scheduler as any).deferredActions).toHaveLength(1);

    // The user's turn fully settles: pi goes idle. On a modern runtime (isIdle
    // present) agent_end is record-only and agent_settled is what drains the queue.
    pi.isIdle.mockReturnValue(true);
    scheduler.notifyAgentEnd([{ role: "assistant", stopReason: "stop" }]);
    scheduler.notifyAgentSettled();
    await vi.advanceTimersByTimeAsync(0);

    expect(pi.sendUserMessage).toHaveBeenCalledTimes(1);
    expect((scheduler as any).deferredActions).toHaveLength(0);
  });

  it("a drain attempt while pi is still busy re-defers instead of sending", async () => {
    const job = makeJob({ id: "job-busy", prompt: "later reminder" });
    const storage = makeMockStorage([job]);
    const pi = { ...makeMockPi(), isIdle: vi.fn(() => false) };
    const scheduler = makeScheduler(storage, pi as any);

    await executeJob(scheduler, job);
    expect((scheduler as any).deferredActions).toHaveLength(1);

    // A spurious settle arrives while pi is genuinely still streaming (isIdle=false).
    scheduler.notifyAgentEnd([{ role: "assistant", stopReason: "stop" }]);
    scheduler.notifyAgentSettled();
    await vi.advanceTimersByTimeAsync(0);

    // The re-entered gate re-checks isIdle() → still busy → re-deferred, not sent.
    expect(pi.sendUserMessage).not.toHaveBeenCalled();
    expect((scheduler as any).deferredActions).toHaveLength(1);
  });

  it("legacy runtime without isIdle() keeps mirror-only behaviour (fires when flags clear)", async () => {
    const job = makeJob({ prompt: "scheduled reminder" });
    const storage = makeMockStorage([job]);
    const pi = makeMockPi(); // no isIdle — older pi runtime
    const scheduler = makeScheduler(storage, pi as any);

    await executeJob(scheduler, job);

    // Unknown idle state (undefined) must not block — preserve legacy behaviour.
    expect(pi.sendUserMessage).toHaveBeenCalledTimes(1);
  });
});

// ---- agent_settled drives confirmation/drain on modern runtimes ----
//
// pi keeps the session's run active across the whole multi-turn loop
// (continuations, compaction, overflow recovery, retries) and only reports idle at
// agent_settled — agent_end fires per-turn while the run is still active. Draining
// the next scheduled send on agent_end would collide with a still-streaming
// continuation and surface "Agent is already processing". On a runtime that exposes
// isIdle (and therefore emits agent_settled), the scheduler must treat agent_end as
// record-only and advance only on agent_settled.
describe("agent_settled gates confirmation/drain on modern runtimes", () => {
  it("does NOT confirm a guaranteed once-job on agent_end; confirms on agent_settled", async () => {
    const job = makeJob({ id: "job-g", guaranteed: true });
    const storage = makeMockStorage([job]);
    const pi = { ...makeMockPi(), isIdle: vi.fn(() => true) };
    const scheduler = makeScheduler(storage, pi as any);

    await executeJob(scheduler, job);
    expect(storage.updateJob).toHaveBeenCalledWith(job.id, { lastStatus: "sent" });

    const successMessages = [
      { role: "user", content: "do the thing", timestamp: 1 },
      { role: "assistant", content: [], stopReason: "stop", timestamp: 2 },
    ];

    // agent_end alone: session still active across possible continuations → no confirm.
    scheduler.notifyAgentEnd(successMessages);
    expect(storage.removeJob).not.toHaveBeenCalled();

    // agent_settled: session truly idle → confirm (once-job removed from storage).
    scheduler.notifyAgentSettled();
    expect(storage.removeJob).toHaveBeenCalledWith(job.id);
  });

  it("serialises a startup backlog: second job drains only after agent_settled, not agent_end", async () => {
    const jobA = makeJob({ id: "job-a", guaranteed: true, prompt: "task A" });
    const jobB = makeJob({ id: "job-b", name: "Job B", guaranteed: true, prompt: "task B" });
    const storage = makeMockStorage([jobA, jobB]);
    const pi = { ...makeMockPi(), isIdle: vi.fn(() => true) };
    const scheduler = makeScheduler(storage, pi as any);

    // Two missed guaranteed jobs fire concurrently, as start() does.
    const p1 = (scheduler as any).executeJobIfLeader(jobA);
    const p2 = (scheduler as any).executeJobIfLeader(jobB);
    await Promise.all([p1, p2]);

    // A sent, B deferred (A's synchronous sending=true gate holds B back).
    expect(pi.sendUserMessage).toHaveBeenCalledTimes(1);
    expect((scheduler as any).deferredActions).toHaveLength(1);

    // A per-turn agent_end (e.g. a compaction turn inside A's run) must NOT drain B —
    // the session is still active. isIdle stays true here, so the only thing holding
    // B back is that agent_end is record-only on modern runtimes.
    scheduler.notifyAgentEnd([{ role: "assistant", stopReason: "stop", timestamp: 1 }]);
    await vi.advanceTimersByTimeAsync(0);
    expect(pi.sendUserMessage).toHaveBeenCalledTimes(1);

    // A settles → B drains as its own turn.
    scheduler.notifyAgentSettled();
    await vi.advanceTimersByTimeAsync(0);
    expect(pi.sendUserMessage).toHaveBeenCalledTimes(2);
    const prefix = `This is an automated scheduled prompt. Interpret and execute the following directly — phrases like "remind me" mean perform the action now, not schedule another reminder:\n\n`;
    expect(pi.sendUserMessage).toHaveBeenLastCalledWith(`${prefix}task B`, { deliverAs: "followUp" });
  });

});

// ---- retry path honours the authoritative idle gate ----
describe("guaranteed retry defers while pi reports busy", () => {
  it("scheduleRetryTimer defers instead of firing when isIdle()===false", async () => {
    const job = makeJob({ id: "job-r", guaranteed: true });
    const storage = makeMockStorage([job]);
    // Mirror flags are all clear, but pi authoritatively reports streaming.
    const pi = { ...makeMockPi(), isIdle: vi.fn(() => false) };
    const scheduler = makeScheduler(storage, pi as any);

    (scheduler as any).scheduleRetryTimer(job.id);
    await vi.advanceTimersByTimeAsync(RETRY_MS);

    // Busy → deferred as a retry action, neither retryLastTurn nor sendUserMessage fired.
    expect(pi.retryLastTurn).not.toHaveBeenCalled();
    expect(pi.sendUserMessage).not.toHaveBeenCalled();
    expect((scheduler as any).deferredActions).toContainEqual({ type: "retry", jobId: job.id });
  });

  it("triggerRetry re-defers when pi goes busy between the timer and the send", async () => {
    const job = makeJob({ id: "job-r2", guaranteed: true });
    const storage = makeMockStorage([job]);
    const pi = { ...makeMockPi(), isIdle: vi.fn(() => false) };
    const scheduler = makeScheduler(storage, pi as any);

    (scheduler as any).triggerRetry(job.id);

    expect(pi.retryLastTurn).not.toHaveBeenCalled();
    expect(pi.sendUserMessage).not.toHaveBeenCalled();
    expect((scheduler as any).retrying).toBe(false);
    expect((scheduler as any).deferredActions).toContainEqual({ type: "retry", jobId: job.id });
  });
});

describe("defaultResumableSessionDir", () => {
  it("encodes the cwd the way pi --resume expects (slashes/colons → dashes, wrapped in --)", () => {
    expect(defaultResumableSessionDir("/home/x/proj", "/root/sessions")).toBe(
      path.join("/root/sessions", "--home-x-proj--")
    );
  });
});

describe("promoteSessionToResumable", () => {
  it("moves the session file into the resumable dir, preserving the basename", () => {
    const base = fs.mkdtempSync(path.join(os.tmpdir(), "sched-promote-"));
    try {
      const src = path.join(base, "seg", "20260101T000000_abc123.jsonl");
      fs.mkdirSync(path.dirname(src), { recursive: true });
      fs.writeFileSync(src, '{"type":"session","id":"abc123"}\n');
      const root = path.join(base, "sessions");

      const dest = promoteSessionToResumable(src, "/home/x/proj", root);

      expect(dest).toBe(path.join(root, "--home-x-proj--", "20260101T000000_abc123.jsonl"));
      expect(fs.existsSync(dest)).toBe(true);
      expect(fs.existsSync(src)).toBe(false); // moved, not copied
    } finally {
      fs.rmSync(base, { recursive: true, force: true });
    }
  });

  it("is idempotent: re-promoting an already-promoted file returns it unchanged", () => {
    const base = fs.mkdtempSync(path.join(os.tmpdir(), "sched-promote-"));
    try {
      const root = path.join(base, "sessions");
      const promoted = path.join(defaultResumableSessionDir("/home/x/proj", root), "run.jsonl");
      fs.mkdirSync(path.dirname(promoted), { recursive: true });
      fs.writeFileSync(promoted, "x\n");

      const result = promoteSessionToResumable(promoted, "/home/x/proj", root);

      expect(result).toBe(promoted);
      expect(fs.existsSync(promoted)).toBe(true);
    } finally {
      fs.rmSync(base, { recursive: true, force: true });
    }
  });

  it("returns the existing destination without overwriting when the same id is already there", () => {
    const base = fs.mkdtempSync(path.join(os.tmpdir(), "sched-promote-"));
    try {
      const root = path.join(base, "sessions");
      const dest = path.join(defaultResumableSessionDir("/home/x/proj", root), "dup.jsonl");
      fs.mkdirSync(path.dirname(dest), { recursive: true });
      fs.writeFileSync(dest, "original\n");

      const src = path.join(base, "seg", "dup.jsonl");
      fs.mkdirSync(path.dirname(src), { recursive: true });
      fs.writeFileSync(src, "incoming\n");

      const result = promoteSessionToResumable(src, "/home/x/proj", root);

      expect(result).toBe(dest);
      expect(fs.readFileSync(dest, "utf8")).toBe("original\n"); // not clobbered
      expect(fs.existsSync(src)).toBe(true); // left in place
    } finally {
      fs.rmSync(base, { recursive: true, force: true });
    }
  });
});

// ---- leaked-scheduler regression ----
//
// pi fires session_start (reason "reload"/"new"/"fork"/"resume") on every
// session swap — routine in multi-context RPC mode — and index.ts builds a
// fresh CronScheduler + calls start() each time. Before the fix nothing stopped
// the previous instance, so its Cron timers kept firing and every scheduled job
// ran once per leaked instance — the user saw the same scheduled message
// delivered twice (or more). start() must tear the prior instance down.
describe("start() stops the previous scheduler instance (no per-leak double-fire)", () => {
  function makeRecurringJob(overrides: Partial<CronJob> = {}): CronJob {
    return {
      id: "leak-1",
      name: "Every Second",
      schedule: "* * * * * *", // every second
      prompt: "tick",
      enabled: true,
      type: "cron",
      runCount: 0,
      createdAt: new Date(Date.now() - 60_000).toISOString(),
      lastRun: new Date().toISOString(), // so start() doesn't treat it as missed
      lastStatus: "success",
      guaranteed: false,
      ...overrides,
    };
  }

  it("tears down the prior instance's cron timers when a new instance starts", async () => {
    const storageA = makeMockStorage([makeRecurringJob()]);
    const piA = makeMockPi();
    const schedulerA = makeScheduler(storageA, piA);
    schedulerA.start();

    // A armed a live cron timer for the job.
    expect((schedulerA as any).jobs.has("leak-1")).toBe(true);
    expect((schedulerA as any).stopped).toBe(false);

    // A session swap builds a fresh scheduler (new pi ctx) over the same store.
    const storageB = makeMockStorage([makeRecurringJob()]);
    const piB = makeMockPi();
    const schedulerB = makeScheduler(storageB, piB);
    schedulerB.start();

    // Starting B must have stopped A: its cron timers are cleared and it's
    // marked stopped, while B owns the live timer.
    expect((schedulerA as any).stopped).toBe(true);
    expect((schedulerA as any).jobs.size).toBe(0);
    expect((schedulerB as any).jobs.has("leak-1")).toBe(true);

    // Behaviourally: advancing past several ticks fires only the live instance.
    // Without the fix, A's leaked timer would also fire and piA would be called.
    await vi.advanceTimersByTimeAsync(3100);
    expect(piB.sendUserMessage).toHaveBeenCalled();
    expect(piA.sendUserMessage).not.toHaveBeenCalled();

    schedulerB.stop();
  });
});

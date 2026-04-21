import { describe, it, expect, vi, beforeEach } from "vitest";
import { createCronTool } from "./tool.js";
import type { CronJob } from "./types.js";

// ─── Helpers ─────────────────────────────────────────────────────────────────

function makeStorage(jobs: CronJob[] = []) {
  const jobMap = new Map(jobs.map((j) => [j.id, j]));
  return {
    getAllJobs: () => [...jobMap.values()],
    getJob: (id: string) => jobMap.get(id),
    addJob: vi.fn((job: CronJob) => {
      jobMap.set(job.id, job);
    }),
    removeJob: vi.fn(),
    updateJob: vi.fn((id: string, updates: Partial<CronJob>) => {
      const existing = jobMap.get(id);
      if (existing) jobMap.set(id, { ...existing, ...updates });
    }),
    hasJobWithName: vi.fn(() => false),
    getPiDir: () => "/tmp/test",
    getStorePath: () => "/tmp/test/schedule-prompts.json",
    load: vi.fn(),
    save: vi.fn(),
  };
}

function makeScheduler() {
  return {
    addJob: vi.fn(),
    removeJob: vi.fn(),
    updateJob: vi.fn(),
    getNextRun: vi.fn(() => null),
  };
}

function makeCtx() {
  return {
    sessionManager: {
      getEntries: () => [],
    },
  };
}

async function exec(storage: ReturnType<typeof makeStorage>, scheduler: ReturnType<typeof makeScheduler>, params: any) {
  const tool = createCronTool(() => storage as any, () => scheduler as any);
  return tool.execute("call-id", params, null as any, null as any, makeCtx() as any);
}

function makeIntervalJob(overrides: Partial<CronJob> = {}): CronJob {
  return {
    id: "job-1",
    name: "my-job",
    schedule: "5m",
    prompt: "do something",
    enabled: true,
    type: "interval",
    intervalMs: 300_000,
    createdAt: new Date().toISOString(),
    runCount: 0,
    ...overrides,
  };
}

// ─── add: once type ──────────────────────────────────────────────────────────

describe("add action – once type", () => {
  it("converts relative time (+10s) to ISO timestamp and stores type: once", async () => {
    const storage = makeStorage();
    const scheduler = makeScheduler();

    const before = Date.now();
    await exec(storage, scheduler, {
      action: "add",
      schedule: "+10s",
      prompt: "one-time test",
      jobType: "once",
    });
    const after = Date.now();

    expect(storage.addJob).toHaveBeenCalledTimes(1);
    const job = (storage.addJob as ReturnType<typeof vi.fn>).mock.calls[0][0];
    expect(job.type).toBe("once");
    // schedule must have been converted from "+10s" to an ISO string
    expect(job.schedule).toMatch(/^\d{4}-\d{2}-\d{2}T/);
    // …and land approximately 10 seconds from now
    const scheduled = new Date(job.schedule).getTime();
    expect(scheduled).toBeGreaterThanOrEqual(before + 9_000);
    expect(scheduled).toBeLessThanOrEqual(after + 11_000);
  });

  it("accepts a future ISO timestamp and preserves it verbatim", async () => {
    const futureDate = new Date(Date.now() + 60_000).toISOString();
    const storage = makeStorage();
    const scheduler = makeScheduler();

    await exec(storage, scheduler, {
      action: "add",
      schedule: futureDate,
      prompt: "one-time ISO test",
      jobType: "once",
    });

    const job = (storage.addJob as ReturnType<typeof vi.fn>).mock.calls[0][0];
    expect(job.type).toBe("once");
    expect(job.schedule).toBe(futureDate);
  });

  it("rejects a past ISO timestamp with an error, does not create a job", async () => {
    const pastDate = new Date(Date.now() - 60_000).toISOString();
    const storage = makeStorage();
    const scheduler = makeScheduler();

    const result = await exec(storage, scheduler, {
      action: "add",
      schedule: pastDate,
      prompt: "past test",
      jobType: "once",
    });

    const text = result.content.map((c: any) => c.text).join("\n");
    expect(text).toContain("✗");
    expect(text).toContain("past");
    expect(storage.addJob).not.toHaveBeenCalled();
  });

  it("defaults to type: cron when type param is omitted", async () => {
    const storage = makeStorage();
    const scheduler = makeScheduler();

    await exec(storage, scheduler, {
      action: "add",
      schedule: "0 * * * * *",
      prompt: "cron test",
      // type deliberately omitted
    });

    const job = (storage.addJob as ReturnType<typeof vi.fn>).mock.calls[0][0];
    expect(job.type).toBe("cron");
  });
});

// ─── add: guaranteed flag storage ────────────────────────────────────────────

describe("add action – guaranteed flag", () => {
  it("stores guaranteed: true when provided", async () => {
    const storage = makeStorage();
    const scheduler = makeScheduler();

    await exec(storage, scheduler, {
      action: "add",
      schedule: "5m",
      prompt: "test",
      jobType: "interval",
      guaranteed: true,
    });

    expect(storage.addJob).toHaveBeenCalledWith(
      expect.objectContaining({ guaranteed: true })
    );
  });

  it("defaults guaranteed to false when not provided", async () => {
    const storage = makeStorage();
    const scheduler = makeScheduler();

    await exec(storage, scheduler, {
      action: "add",
      schedule: "5m",
      prompt: "test",
      jobType: "interval",
    });

    expect(storage.addJob).toHaveBeenCalledWith(
      expect.objectContaining({ guaranteed: false })
    );
  });

  it("stores guaranteed: false when explicitly provided", async () => {
    const storage = makeStorage();
    const scheduler = makeScheduler();

    await exec(storage, scheduler, {
      action: "add",
      schedule: "5m",
      prompt: "test",
      jobType: "interval",
      guaranteed: false,
    });

    expect(storage.addJob).toHaveBeenCalledWith(
      expect.objectContaining({ guaranteed: false })
    );
  });

  it("passes guaranteed job through to scheduler.addJob", async () => {
    const storage = makeStorage();
    const scheduler = makeScheduler();

    await exec(storage, scheduler, {
      action: "add",
      schedule: "5m",
      prompt: "test",
      jobType: "interval",
      guaranteed: true,
    });

    expect(scheduler.addJob).toHaveBeenCalledWith(
      expect.objectContaining({ guaranteed: true })
    );
  });
});

// ─── update: jobType change ───────────────────────────────────────────────────

describe("update action – changing jobType", () => {
  it("converts a cron job to once with an ISO timestamp", async () => {
    const futureDate = new Date(Date.now() + 60_000).toISOString();
    const job = makeIntervalJob({ id: "j1", type: "cron", schedule: "* * * * * *", intervalMs: undefined });
    const storage = makeStorage([job]);
    const scheduler = makeScheduler();

    const result = await exec(storage, scheduler, {
      action: "update",
      jobId: "j1",
      jobType: "once",
      schedule: futureDate,
    });

    expect((result.content[0] as any).text).toContain("✓");
    expect(storage.updateJob).toHaveBeenCalledWith(
      "j1",
      expect.objectContaining({ type: "once", schedule: futureDate })
    );
  });

  it("converts a cron job to once with a relative time (+7m)", async () => {
    const job = makeIntervalJob({ id: "j2", type: "cron", schedule: "* * * * * *", intervalMs: undefined });
    const storage = makeStorage([job]);
    const scheduler = makeScheduler();

    const before = Date.now();
    const result = await exec(storage, scheduler, {
      action: "update",
      jobId: "j2",
      jobType: "once",
      schedule: "+7m",
    });
    const after = Date.now();

    expect((result.content[0] as any).text).toContain("✓");
    const updateArg = (storage.updateJob as ReturnType<typeof vi.fn>).mock.calls[0][1];
    expect(updateArg.type).toBe("once");
    expect(updateArg.schedule).toMatch(/^\d{4}-\d{2}-\d{2}T/);
    const scheduled = new Date(updateArg.schedule).getTime();
    expect(scheduled).toBeGreaterThanOrEqual(before + 6 * 60_000);
    expect(scheduled).toBeLessThanOrEqual(after + 8 * 60_000);
  });

  it("clears intervalMs when converting from interval to once", async () => {
    const job = makeIntervalJob({ id: "j3", guaranteed: false });
    const storage = makeStorage([job]);
    const scheduler = makeScheduler();

    await exec(storage, scheduler, {
      action: "update",
      jobId: "j3",
      jobType: "once",
      schedule: new Date(Date.now() + 60_000).toISOString(),
    });

    const updateArg = (storage.updateJob as ReturnType<typeof vi.fn>).mock.calls[0][1];
    expect(updateArg.intervalMs).toBeUndefined();
  });

  it("fails if the new jobType is once but schedule is a cron expression", async () => {
    const job = makeIntervalJob({ id: "j4", type: "cron", schedule: "* * * * * *", intervalMs: undefined });
    const storage = makeStorage([job]);
    const scheduler = makeScheduler();

    const result = await exec(storage, scheduler, {
      action: "update",
      jobId: "j4",
      jobType: "once",
      schedule: "* * * * * *",
    });

    // "* * * * * *" is not a valid ISO timestamp or relative time
    expect((result.content[0] as any).text).toContain("✗");
    expect(storage.updateJob).not.toHaveBeenCalled();
  });
});

// ─── update: guaranteed flag ──────────────────────────────────────────────────

describe("update action – guaranteed flag", () => {
  it("can flip guaranteed from false to true", async () => {
    const storage = makeStorage([makeIntervalJob({ guaranteed: false })]);
    const scheduler = makeScheduler();

    await exec(storage, scheduler, {
      action: "update",
      jobId: "job-1",
      guaranteed: true,
    });

    expect(storage.updateJob).toHaveBeenCalledWith(
      "job-1",
      expect.objectContaining({ guaranteed: true })
    );
  });

  it("can flip guaranteed from true to false", async () => {
    const storage = makeStorage([makeIntervalJob({ guaranteed: true })]);
    const scheduler = makeScheduler();

    await exec(storage, scheduler, {
      action: "update",
      jobId: "job-1",
      guaranteed: false,
    });

    expect(storage.updateJob).toHaveBeenCalledWith(
      "job-1",
      expect.objectContaining({ guaranteed: false })
    );
  });

  it("does not touch guaranteed when not included in update params", async () => {
    const storage = makeStorage([makeIntervalJob({ guaranteed: true })]);
    const scheduler = makeScheduler();

    await exec(storage, scheduler, {
      action: "update",
      jobId: "job-1",
      prompt: "new prompt",
    });

    const updateArg = (storage.updateJob as ReturnType<typeof vi.fn>).mock.calls[0][1];
    expect(updateArg).not.toHaveProperty("guaranteed");
  });
});

// ─── list: guaranteed display in text output ──────────────────────────────────

describe("list action – guaranteed display", () => {
  it('shows "Guaranteed: yes" for a guaranteed job', async () => {
    const storage = makeStorage([
      makeIntervalJob({ guaranteed: true }),
    ]);
    const scheduler = makeScheduler();

    const result = await exec(storage, scheduler, { action: "list" });

    const text = result.content.map((c: any) => c.text).join("\n");
    expect(text).toContain("Guaranteed: yes");
  });

  it('shows "Guaranteed: no" for a non-guaranteed job', async () => {
    const storage = makeStorage([
      makeIntervalJob({ guaranteed: false }),
    ]);
    const scheduler = makeScheduler();

    const result = await exec(storage, scheduler, { action: "list" });

    const text = result.content.map((c: any) => c.text).join("\n");
    expect(text).toContain("Guaranteed: no");
  });

  it('shows "Guaranteed: no" for a legacy job with no guaranteed field', async () => {
    const job = makeIntervalJob();
    delete (job as any).guaranteed;
    const storage = makeStorage([job]);
    const scheduler = makeScheduler();

    const result = await exec(storage, scheduler, { action: "list" });

    const text = result.content.map((c: any) => c.text).join("\n");
    expect(text).toContain("Guaranteed: no");
  });
});

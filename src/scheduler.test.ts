import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { CronScheduler } from "./scheduler.js";
import type { CronJob } from "./types.js";
import type { CronStorage } from "./storage.js";
import type { ExtensionAPI } from "@mariozechner/pi-coding-agent";

// ─── Helpers ─────────────────────────────────────────────────────────────────

function makeJob(overrides: Partial<CronJob> = {}): CronJob {
  return {
    id: "test-id",
    name: "Test",
    schedule: new Date(Date.now() - 60_000).toISOString(),
    prompt: "do something",
    enabled: true,
    type: "once",
    createdAt: new Date(Date.now() - 120_000).toISOString(),
    runCount: 0,
    ...overrides,
  };
}

function makeStorage(tmpDir: string, jobs: CronJob[] = []): CronStorage {
  return {
    getPiDir: () => tmpDir,
    getAllJobs: () => jobs,
    getJob: (id: string) => jobs.find((j) => j.id === id),
    addJob: vi.fn(),
    removeJob: vi.fn(),
    updateJob: vi.fn(),
    hasJobWithName: vi.fn(),
    getStorePath: () => path.join(tmpDir, "schedule-prompts.json"),
    load: () => ({ jobs, version: 1 }),
    save: vi.fn(),
  } as unknown as CronStorage;
}

function makePi(): ExtensionAPI {
  return {
    events: { emit: vi.fn() },
    sendMessage: vi.fn(),
    sendUserMessage: vi.fn(),
  } as unknown as ExtensionAPI;
}

const ago = (ms: number) => new Date(Date.now() - ms).toISOString();
const from = (ms: number) => new Date(Date.now() + ms).toISOString();

// ─── isMissed ────────────────────────────────────────────────────────────────

describe("isMissed", () => {
  let tmpDir: string;
  let scheduler: CronScheduler;
  const now = new Date();

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "pi-sched-test-"));
    scheduler = new CronScheduler(makeStorage(tmpDir), makePi());
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  describe("once", () => {
    it("past + no lastRun → missed", () => {
      const job = makeJob({ type: "once", schedule: ago(60_000) });
      expect((scheduler as any).isMissed(job, now)).toBe(true);
    });

    it("past + has lastRun → not missed (already ran)", () => {
      const job = makeJob({
        type: "once",
        schedule: ago(60_000),
        lastRun: ago(30_000),
      });
      expect((scheduler as any).isMissed(job, now)).toBe(false);
    });

    it("future → not missed", () => {
      const job = makeJob({ type: "once", schedule: from(60_000) });
      expect((scheduler as any).isMissed(job, now)).toBe(false);
    });
  });

  describe("interval", () => {
    it("full interval elapsed since createdAt → missed", () => {
      const job = makeJob({
        type: "interval",
        intervalMs: 30_000,
        schedule: "30s",
        createdAt: ago(60_000), // created 60s ago, interval is 30s
      });
      expect((scheduler as any).isMissed(job, now)).toBe(true);
    });

    it("full interval elapsed since lastRun → missed", () => {
      const job = makeJob({
        type: "interval",
        intervalMs: 30_000,
        schedule: "30s",
        createdAt: ago(90_000),
        lastRun: ago(60_000), // last ran 60s ago, interval is 30s
      });
      expect((scheduler as any).isMissed(job, now)).toBe(true);
    });

    it("interval not yet elapsed → not missed", () => {
      const job = makeJob({
        type: "interval",
        intervalMs: 60_000,
        schedule: "60s",
        createdAt: ago(30_000), // created 30s ago, interval is 60s
      });
      expect((scheduler as any).isMissed(job, now)).toBe(false);
    });
  });

  describe("cron", () => {
    it("firing missed since createdAt → missed", () => {
      // fires every second; created 5s ago and never ran
      const job = makeJob({
        type: "cron",
        schedule: "* * * * * *",
        createdAt: ago(5_000),
      });
      expect((scheduler as any).isMissed(job, now)).toBe(true);
    });

    it("firing missed since lastRun → missed", () => {
      // fires every second; last ran 5s ago
      const job = makeJob({
        type: "cron",
        schedule: "* * * * * *",
        createdAt: ago(10_000),
        lastRun: ago(5_000),
      });
      expect((scheduler as any).isMissed(job, now)).toBe(true);
    });

    it("next firing is in the future → not missed", () => {
      // fires hourly; last ran 1 second ago — next tick is up to 3599s away
      const job = makeJob({
        type: "cron",
        schedule: "0 0 * * * *",
        createdAt: ago(7_200_000),
        lastRun: ago(1_000),
      });
      expect((scheduler as any).isMissed(job, now)).toBe(false);
    });
  });
});

// ─── acquireLeadership ───────────────────────────────────────────────────────

describe("acquireLeadership", () => {
  let tmpDir: string;
  let scheduler: CronScheduler;
  let leaderPidPath: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "pi-sched-test-"));
    scheduler = new CronScheduler(makeStorage(tmpDir), makePi());
    leaderPidPath = path.join(tmpDir, "leader.pid");
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it("no leader file → becomes leader", async () => {
    const promise = (scheduler as any).acquireLeadership();
    await vi.advanceTimersByTimeAsync(1001);
    expect(await promise).toBe(true);
    expect(fs.readFileSync(leaderPidPath, "utf-8").trim()).toBe(
      String(process.pid)
    );
  });

  it("leader file has dead PID → takes over", async () => {
    fs.writeFileSync(leaderPidPath, "9999999", "utf-8");
    const promise = (scheduler as any).acquireLeadership();
    await vi.advanceTimersByTimeAsync(1001);
    expect(await promise).toBe(true);
    expect(fs.readFileSync(leaderPidPath, "utf-8").trim()).toBe(
      String(process.pid)
    );
  });

  it("leader file has own PID → already leader, returns immediately", async () => {
    fs.writeFileSync(leaderPidPath, String(process.pid), "utf-8");
    // Returns before the 1s setTimeout — no timer advancement needed
    const promise = (scheduler as any).acquireLeadership();
    await vi.advanceTimersByTimeAsync(0);
    expect(await promise).toBe(true);
  });

  it("leader file has other live PID → not leader, returns immediately", async () => {
    // process.ppid is the parent process — alive and signable without EPERM
    fs.writeFileSync(leaderPidPath, String(process.ppid), "utf-8");
    const promise = (scheduler as any).acquireLeadership();
    await vi.advanceTimersByTimeAsync(0);
    expect(await promise).toBe(false);
  });

  it("loses race: competing process overwrites during 1s window", async () => {
    // No file initially — we claim leadership, but a competitor overwrites
    const promise = (scheduler as any).acquireLeadership();

    // Competitor writes their PID 500ms into our 1s wait
    setTimeout(() => {
      fs.writeFileSync(leaderPidPath, "1", "utf-8");
    }, 500);

    await vi.advanceTimersByTimeAsync(1001);
    expect(await promise).toBe(false);
  });
});

// ─── start: missed-job dispatch ──────────────────────────────────────────────

describe("start", () => {
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "pi-sched-test-"));
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  // ── guaranteed: true ────────────────────────────────────────────────────────

  it("guaranteed once job: executes it and does NOT schedule it", () => {
    const job = makeJob({
      id: "missed-once",
      type: "once",
      schedule: ago(60_000),
      guaranteed: true,
    });
    const scheduler = new CronScheduler(makeStorage(tmpDir, [job]), makePi());
    const execSpy = vi
      .spyOn(scheduler as any, "executeJobIfLeader")
      .mockResolvedValue(undefined);
    const schedSpy = vi
      .spyOn(scheduler as any, "scheduleJob")
      .mockImplementation(() => {});

    scheduler.start();

    expect(execSpy).toHaveBeenCalledWith(job);
    expect(schedSpy).not.toHaveBeenCalled();
  });

  it("guaranteed cron job: executes it AND schedules it for future firings", () => {
    const job = makeJob({
      id: "missed-cron",
      type: "cron",
      schedule: "* * * * * *",
      createdAt: ago(5_000),
      guaranteed: true,
    });
    const scheduler = new CronScheduler(makeStorage(tmpDir, [job]), makePi());
    const execSpy = vi
      .spyOn(scheduler as any, "executeJobIfLeader")
      .mockResolvedValue(undefined);
    const schedSpy = vi
      .spyOn(scheduler as any, "scheduleJob")
      .mockImplementation(() => {});

    scheduler.start();

    expect(execSpy).toHaveBeenCalledWith(job);
    expect(schedSpy).toHaveBeenCalledWith(job);
  });

  it("guaranteed interval job: executes it AND schedules it for future firings", () => {
    const job = makeJob({
      id: "missed-interval",
      type: "interval",
      intervalMs: 30_000,
      schedule: "30s",
      createdAt: ago(60_000),
      guaranteed: true,
    });
    const scheduler = new CronScheduler(makeStorage(tmpDir, [job]), makePi());
    const execSpy = vi
      .spyOn(scheduler as any, "executeJobIfLeader")
      .mockResolvedValue(undefined);
    const schedSpy = vi
      .spyOn(scheduler as any, "scheduleJob")
      .mockImplementation(() => {});

    scheduler.start();

    expect(execSpy).toHaveBeenCalledWith(job);
    expect(schedSpy).toHaveBeenCalledWith(job);
  });

  // ── guaranteed: false (default) ──────────────────────────────────────────────

  it("non-guaranteed missed once job: marks as failed, does not execute or schedule", () => {
    const job = makeJob({
      id: "missed-once-ng",
      type: "once",
      schedule: ago(60_000),
      guaranteed: false,
    });
    const storage = makeStorage(tmpDir, [job]);
    const scheduler = new CronScheduler(storage, makePi());
    const execSpy = vi
      .spyOn(scheduler as any, "executeJobIfLeader")
      .mockResolvedValue(undefined);
    const schedSpy = vi
      .spyOn(scheduler as any, "scheduleJob")
      .mockImplementation(() => {});

    scheduler.start();

    expect(execSpy).not.toHaveBeenCalled();
    expect(schedSpy).not.toHaveBeenCalled();
    expect(storage.updateJob).toHaveBeenCalledWith(
      "missed-once-ng",
      expect.objectContaining({ enabled: false, lastStatus: "error" })
    );
  });

  it("non-guaranteed missed once job with no guaranteed field (legacy): marks as failed", () => {
    // Jobs created before the guaranteed flag was added have guaranteed: undefined,
    // which is falsy — they must not run on startup.
    const job = makeJob({
      id: "missed-once-legacy",
      type: "once",
      schedule: ago(60_000),
      // guaranteed deliberately absent
    });
    const storage = makeStorage(tmpDir, [job]);
    const scheduler = new CronScheduler(storage, makePi());
    const execSpy = vi
      .spyOn(scheduler as any, "executeJobIfLeader")
      .mockResolvedValue(undefined);

    scheduler.start();

    expect(execSpy).not.toHaveBeenCalled();
    expect(storage.updateJob).toHaveBeenCalledWith(
      "missed-once-legacy",
      expect.objectContaining({ enabled: false, lastStatus: "error" })
    );
  });

  it("non-guaranteed missed cron job: silently drops execution, reschedules for future", () => {
    const job = makeJob({
      id: "missed-cron-ng",
      type: "cron",
      schedule: "* * * * * *",
      createdAt: ago(5_000),
      guaranteed: false,
    });
    const storage = makeStorage(tmpDir, [job]);
    const scheduler = new CronScheduler(storage, makePi());
    const execSpy = vi
      .spyOn(scheduler as any, "executeJobIfLeader")
      .mockResolvedValue(undefined);
    const schedSpy = vi
      .spyOn(scheduler as any, "scheduleJob")
      .mockImplementation(() => {});

    scheduler.start();

    expect(execSpy).not.toHaveBeenCalled();
    expect(schedSpy).toHaveBeenCalledWith(job);
    expect(storage.updateJob).not.toHaveBeenCalled();
  });

  it("non-guaranteed missed interval job: silently drops execution, reschedules for future", () => {
    const job = makeJob({
      id: "missed-interval-ng",
      type: "interval",
      intervalMs: 30_000,
      schedule: "30s",
      createdAt: ago(60_000),
      guaranteed: false,
    });
    const storage = makeStorage(tmpDir, [job]);
    const scheduler = new CronScheduler(storage, makePi());
    const execSpy = vi
      .spyOn(scheduler as any, "executeJobIfLeader")
      .mockResolvedValue(undefined);
    const schedSpy = vi
      .spyOn(scheduler as any, "scheduleJob")
      .mockImplementation(() => {});

    scheduler.start();

    expect(execSpy).not.toHaveBeenCalled();
    expect(schedSpy).toHaveBeenCalledWith(job);
    expect(storage.updateJob).not.toHaveBeenCalled();
  });

  it("future job: only schedules it, does not execute", () => {
    const job = makeJob({
      id: "future-once",
      type: "once",
      schedule: from(60_000),
    });
    const scheduler = new CronScheduler(makeStorage(tmpDir, [job]), makePi());
    const execSpy = vi
      .spyOn(scheduler as any, "executeJobIfLeader")
      .mockResolvedValue(undefined);
    const schedSpy = vi
      .spyOn(scheduler as any, "scheduleJob")
      .mockImplementation(() => {});

    scheduler.start();

    expect(execSpy).not.toHaveBeenCalled();
    expect(schedSpy).toHaveBeenCalledWith(job);
  });

  it("disabled job: neither executed nor scheduled", () => {
    const job = makeJob({ id: "disabled", enabled: false });
    const scheduler = new CronScheduler(makeStorage(tmpDir, [job]), makePi());
    const execSpy = vi
      .spyOn(scheduler as any, "executeJobIfLeader")
      .mockResolvedValue(undefined);
    const schedSpy = vi
      .spyOn(scheduler as any, "scheduleJob")
      .mockImplementation(() => {});

    scheduler.start();

    expect(execSpy).not.toHaveBeenCalled();
    expect(schedSpy).not.toHaveBeenCalled();
  });
});

// ─── executeJob: once-job cleanup ────────────────────────────────────────────

describe("executeJob: once-job cleanup", () => {
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "pi-sched-test-"));
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it("removes a once job from storage after successful execution", async () => {
    const job = makeJob({ id: "once-job", type: "once", schedule: from(10_000) });
    const storage = makeStorage(tmpDir, [job]);
    const pi = makePi();
    const scheduler = new CronScheduler(storage, pi);

    await (scheduler as any).executeJob(job);

    expect(storage.removeJob).toHaveBeenCalledWith("once-job");
    expect(storage.updateJob).not.toHaveBeenCalledWith(
      "once-job",
      expect.objectContaining({ enabled: false })
    );
  });

  it("does NOT remove a recurring cron job after execution", async () => {
    const job = makeJob({
      id: "cron-job",
      type: "cron",
      schedule: "* * * * * *",
      createdAt: ago(5_000),
    });
    const storage = makeStorage(tmpDir, [job]);
    const scheduler = new CronScheduler(storage, makePi());

    await (scheduler as any).executeJob(job);

    expect(storage.removeJob).not.toHaveBeenCalled();
    expect(storage.updateJob).toHaveBeenCalledWith(
      "cron-job",
      expect.objectContaining({ lastStatus: "success" })
    );
  });

  it("disables (not removes) a once job when execution fails", async () => {
    const job = makeJob({ id: "once-err", type: "once", schedule: from(10_000) });
    const storage = makeStorage(tmpDir, [job]);
    const pi = makePi();
    // Make sendUserMessage throw
    (pi.sendUserMessage as ReturnType<typeof vi.fn>).mockImplementation(() => {
      throw new Error("send failed");
    });
    const scheduler = new CronScheduler(storage, pi);

    await (scheduler as any).executeJob(job);

    expect(storage.removeJob).not.toHaveBeenCalled();
    expect(storage.updateJob).toHaveBeenCalledWith(
      "once-err",
      expect.objectContaining({ lastStatus: "error", enabled: false })
    );
  });
});

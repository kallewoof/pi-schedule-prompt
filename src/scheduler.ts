import * as fs from "fs";
import * as path from "path";
import { Cron } from "croner";
import type { ExtensionAPI } from "@mariozechner/pi-coding-agent";
import type { CronJob, CronChangeEvent } from "./types.js";
import type { CronStorage } from "./storage.js";

export class CronScheduler {
  private jobs = new Map<string, Cron>();
  private intervals = new Map<string, NodeJS.Timeout>();
  private readonly storage: CronStorage;
  private readonly pi: ExtensionAPI;
  private readonly leaderPidPath: string;

  constructor(storage: CronStorage, pi: ExtensionAPI) {
    this.storage = storage;
    this.pi = pi;
    this.leaderPidPath = path.join(storage.getPiDir(), "leader.pid");
  }

  start(): void {
    const allJobs = this.storage.getAllJobs();
    const now = new Date();

    for (const job of allJobs) {
      if (!job.enabled) continue;

      if (this.isMissed(job, now)) {
        if (job.guaranteed) {
          void this.executeJobIfLeader(job);
          if (job.type !== "once") {
            this.scheduleJob(job);
          }
        } else if (job.type === "once") {
          this.storage.updateJob(job.id, {
            enabled: false,
            lastStatus: "error",
            lastRun: now.toISOString(),
          });
          this.emitChange({ type: "error", jobId: job.id, error: "Missed one-time job (not guaranteed)" });
        } else {
          // Recurring job: silently drop the missed execution, resume normal schedule
          this.scheduleJob(job);
        }
      } else {
        this.scheduleJob(job);
      }
    }
  }

  stop(): void {
    for (const cron of this.jobs.values()) {
      cron.stop();
    }
    this.jobs.clear();

    for (const interval of this.intervals.values()) {
      clearInterval(interval);
    }
    this.intervals.clear();
  }

  addJob(job: CronJob): void {
    if (job.enabled) {
      this.scheduleJob(job);
    }
    this.emitChange({ type: "add", job });
  }

  removeJob(id: string): void {
    this.unscheduleJob(id);
    this.emitChange({ type: "remove", jobId: id });
  }

  updateJob(id: string, updated: CronJob): void {
    this.unscheduleJob(id);
    if (updated.enabled) {
      this.scheduleJob(updated);
    }
    this.emitChange({ type: "update", job: updated });
  }

  getNextRun(jobId: string): Date | null {
    const cron = this.jobs.get(jobId);
    if (cron) {
      const next = cron.nextRun();
      return next || null;
    }
    return null;
  }

  // --- Leader election ---

  private readLeaderPid(): number | null {
    try {
      const content = fs.readFileSync(this.leaderPidPath, "utf-8").trim();
      const pid = parseInt(content, 10);
      return isNaN(pid) ? null : pid;
    } catch {
      return null;
    }
  }

  private writeLeaderPid(pid: number): void {
    try {
      const piDir = this.storage.getPiDir();
      if (!fs.existsSync(piDir)) {
        fs.mkdirSync(piDir, { recursive: true });
      }
      const tempPath = `${this.leaderPidPath}.tmp`;
      fs.writeFileSync(tempPath, String(pid), "utf-8");
      fs.renameSync(tempPath, this.leaderPidPath);
    } catch (error) {
      console.error("Failed to write leader.pid:", error);
    }
  }

  private isPidAlive(pid: number): boolean {
    try {
      process.kill(pid, 0);
      return true;
    } catch (err: any) {
      // EPERM → process exists but we lack permission to signal it → alive
      // ESRCH → process does not exist → dead
      return err.code === "EPERM";
    }
  }

  /**
   * Attempt to acquire leadership before firing a job.
   * Returns true if this process should execute the job.
   */
  private async acquireLeadership(): Promise<boolean> {
    const ownPid = process.pid;

    const existing = this.readLeaderPid();
    if (existing !== null && this.isPidAlive(existing)) {
      return existing === ownPid;
    }

    // No live leader — try to claim it
    this.writeLeaderPid(ownPid);

    await new Promise<void>((resolve) => setTimeout(resolve, 1000));

    // Check if we won the race
    return this.readLeaderPid() === ownPid;
  }

  // --- Missed-job detection ---

  private isMissed(job: CronJob, now: Date): boolean {
    if (job.type === "once") {
      return new Date(job.schedule) <= now && !job.lastRun;
    }

    if (job.type === "interval" && job.intervalMs) {
      const checkFrom = new Date(job.lastRun ?? job.createdAt);
      return checkFrom.getTime() + job.intervalMs <= now.getTime();
    }

    if (job.type === "cron") {
      const checkFrom = new Date(job.lastRun ?? job.createdAt);
      try {
        const tempCron = new Cron(job.schedule, { paused: true });
        const nextAfterLast = tempCron.nextRun(checkFrom);
        tempCron.stop();
        return nextAfterLast !== null && nextAfterLast !== undefined && nextAfterLast <= now;
      } catch {
        return false;
      }
    }

    return false;
  }

  // --- Scheduling ---

  private scheduleJob(job: CronJob): void {
    try {
      if (job.type === "interval" && job.intervalMs) {
        const interval = setInterval(() => {
          void this.executeJobIfLeader(job);
        }, job.intervalMs);
        this.intervals.set(job.id, interval);
      } else if (job.type === "once") {
        const targetDate = new Date(job.schedule);
        const delay = targetDate.getTime() - Date.now();

        if (delay > 0) {
          const timeout = setTimeout(async () => {
            await this.executeJobIfLeader(job);
          }, delay);
          this.intervals.set(job.id, timeout as any);
        }
        // Past-time once jobs are handled by isMissed() in start(); nothing to do here.
      } else {
        const cron = new Cron(job.schedule, () => {
          void this.executeJobIfLeader(job);
        });
        this.jobs.set(job.id, cron);
      }
    } catch (error) {
      console.error(`Failed to schedule job ${job.id}:`, error);
      this.emitChange({
        type: "error",
        jobId: job.id,
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }

  private unscheduleJob(id: string): void {
    const cron = this.jobs.get(id);
    if (cron) {
      cron.stop();
      this.jobs.delete(id);
    }

    const interval = this.intervals.get(id);
    if (interval) {
      clearInterval(interval);
      this.intervals.delete(id);
    }
  }

  // --- Execution ---

  private async executeJobIfLeader(job: CronJob): Promise<void> {
    const isLeader = await this.acquireLeadership();
    if (!isLeader) return;
    await this.executeJob(job);
  }

  private async executeJob(job: CronJob): Promise<void> {
    console.log(`Executing scheduled prompt: ${job.name} (${job.id})`);

    try {
      this.storage.updateJob(job.id, { lastStatus: "running" });
      this.emitChange({ type: "fire", job });

      this.pi.sendMessage({
        customType: "scheduled_prompt",
        content: [{ type: "text", text: job.prompt }],
        display: true,
        details: { jobId: job.id, jobName: job.name, prompt: job.prompt },
      });

      this.pi.sendUserMessage(job.prompt, { deliverAs: "followUp" });

      if (job.type === "once") {
        this.storage.removeJob(job.id);
        this.emitChange({ type: "remove", jobId: job.id });
      } else {
        const nextRun = this.getNextRun(job.id);
        this.storage.updateJob(job.id, {
          lastRun: new Date().toISOString(),
          lastStatus: "success",
          runCount: job.runCount + 1,
          nextRun: nextRun?.toISOString(),
        });
        this.emitChange({ type: "fire", job });
      }
    } catch (error) {
      console.error(`Failed to execute job ${job.id}:`, error);
      this.storage.updateJob(job.id, {
        lastRun: new Date().toISOString(),
        lastStatus: "error",
        ...(job.type === "once" && { enabled: false }),
      });
      this.emitChange({
        type: "error",
        jobId: job.id,
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }

  private emitChange(event: CronChangeEvent): void {
    this.pi.events.emit("cron:change", event);
  }

  // --- Static helpers ---

  static validateCronExpression(expression: string): { valid: boolean; error?: string } {
    const fields = expression.trim().split(/\s+/);
    if (fields.length !== 6) {
      return {
        valid: false,
        error: `Cron expression must have 6 fields (second minute hour dom month dow), got ${fields.length}. Example: "0 * * * * *" for every minute`,
      };
    }

    try {
      new Cron(expression, () => {});
      return { valid: true };
    } catch (error) {
      return {
        valid: false,
        error: error instanceof Error ? error.message : "Invalid cron expression",
      };
    }
  }

  static parseRelativeTime(delta: string): string | null {
    const match = delta.match(/^\+(\d+)(s|m|h|d)$/);
    if (!match) return null;

    const value = parseInt(match[1], 10);
    const unit = match[2];

    const msMap: Record<string, number> = {
      s: 1000,
      m: 60 * 1000,
      h: 60 * 60 * 1000,
      d: 24 * 60 * 60 * 1000,
    };

    return new Date(Date.now() + value * msMap[unit]).toISOString();
  }

  static parseInterval(interval: string): number | null {
    const match = interval.match(/^(\d+)(s|m|h|d)$/);
    if (!match) return null;

    const value = parseInt(match[1], 10);
    const unit = match[2];

    const multipliers: Record<string, number> = {
      s: 1000,
      m: 60 * 1000,
      h: 60 * 60 * 1000,
      d: 24 * 60 * 60 * 1000,
    };

    return value * multipliers[unit];
  }
}

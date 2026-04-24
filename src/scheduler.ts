import * as fs from "fs";
import * as path from "path";
import { Cron } from "croner";
import type { ExtensionAPI } from "@mariozechner/pi-coding-agent";
import type { CronJob, CronChangeEvent } from "./types.js";
import type { CronStorage } from "./storage.js";

const GUARANTEED_RETRY_DELAY_MS = 10 * 60 * 1000;

type DeferredAction = { type: "send"; job: CronJob } | { type: "retry"; jobId: string };

export class CronScheduler {
  private jobs = new Map<string, Cron>();
  private intervals = new Map<string, NodeJS.Timeout>();
  private retries = new Map<string, NodeJS.Timeout>();
  /** Job IDs for guaranteed once-jobs that have been sent but not yet confirmed by agent_end. FIFO. */
  private pendingGuaranteedOnce: string[] = [];
  /**
   * True while a guaranteed retry is in-flight (retryLastTurn() called, waiting for agent_end).
   * Blocks new job sends so context tail stays predictable for retryLastTurn().
   */
  private retrying = false;
  private retryingJobId: string | null = null;
  /** Jobs and retries deferred while retrying=true. Processed FIFO after retry resolves. */
  private deferredActions: DeferredAction[] = [];
  /**
   * True between sendUserMessage and the corresponding agent_end notification.
   * Set synchronously (before any await) in executeJobIfLeader to prevent concurrent sends
   * racing through the gate before the first one registers its in-flight state.
   */
  private sending = false;
  /** True while any agent turn is in progress (set by notifyAgentStart, cleared by notifyAgentEnd). */
  private agentRunning = false;
  /**
   * Whether the current (or most recent) agent turn was initiated by the scheduler.
   * Set in notifyAgentStart() based on whether sending=true at that moment.
   * Defaults to true so that notifyAgentEnd() works correctly when notifyAgentStart()
   * is not wired up (backward-compatible behaviour).
   */
  private currentTurnIsScheduled = true;
  /**
   * Fires if agent_end is never received after a send (e.g. suppressed by compaction).
   * Rescues the stuck sending flag so guaranteed jobs can be retried.
   */
  private sendingWatchdog: NodeJS.Timeout | null = null;
  private readonly SENDING_WATCHDOG_MS = 5 * 60 * 1000;
  /** Tracks which job's prompt is at the tail of the agent context (last sendUserMessage). */
  private contextTailJobId: string | null = null;

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

    for (const retry of this.retries.values()) {
      clearTimeout(retry);
    }
    this.retries.clear();
    this.pendingGuaranteedOnce = [];
    this.retrying = false;
    this.retryingJobId = null;
    this.deferredActions = [];
    this.sending = false;
    this.agentRunning = false;
    this.currentTurnIsScheduled = true;
    if (this.sendingWatchdog !== null) {
      clearTimeout(this.sendingWatchdog);
      this.sendingWatchdog = null;
    }
    this.contextTailJobId = null;
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

  /**
   * Called by the host when an agent loop starts (agent_start event).
   * Tags the turn as scheduler-initiated if a send is currently in-flight.
   */
  notifyAgentStart(): void {
    this.agentRunning = true;
    // If we sent a message and it hasn't been confirmed yet, this turn belongs to us.
    this.currentTurnIsScheduled = this.sending;
    // The turn has started — no longer need the watchdog to rescue a stuck send.
    if (this.sendingWatchdog !== null) {
      clearTimeout(this.sendingWatchdog);
      this.sendingWatchdog = null;
    }
  }

  /**
   * Called by the host when an agent turn ends.
   * Resolves the current retry or the oldest pending guaranteed once-job,
   * but only when the turn was scheduler-initiated.
   */
  notifyAgentEnd(messages: readonly unknown[]): void {
    this.agentRunning = false;
    // Capture and reset the turn-ownership flag. Reset to true so that a subsequent
    // agent_end without a preceding notifyAgentStart (when agent_start is not wired up)
    // still processes pendingGuaranteedOnce — backward-compatible behaviour.
    const wasScheduledTurn = this.currentTurnIsScheduled;
    this.currentTurnIsScheduled = true;

    // Clear the in-flight flag and any outstanding watchdog.
    this.sending = false;
    if (this.sendingWatchdog !== null) {
      clearTimeout(this.sendingWatchdog);
      this.sendingWatchdog = null;
    }

    // If a retryLastTurn()-based retry is in-flight, this agent_end confirms or re-fails it.
    if (this.retrying && this.retryingJobId) {
      const jobId = this.retryingJobId;
      this.retrying = false;
      this.retryingJobId = null;

      const job = this.storage.getJob(jobId);
      if (job && job.enabled && job.guaranteed) {
        const modelFailed = this.agentEndHasError(messages);

        if (!modelFailed) {
          this.confirmJobSuccess(job);
        } else {
          this.storage.updateJob(jobId, { lastStatus: "error" });
          this.emitChange({ type: "error", jobId, error: "Model error (retrying in 10m)" });
          this.scheduleRetryTimer(jobId);
        }
      }

      this.processNextDeferred();
      return;
    }

    // Only process pendingGuaranteedOnce when this turn was initiated by the scheduler.
    // A user-initiated turn's agent_end must not accidentally confirm a job whose prompt
    // the model hasn't actually processed yet.
    if (!wasScheduledTurn) {
      this.processNextDeferred();
      return;
    }

    // Scheduler-initiated turn: confirm or retry the oldest pending guaranteed job.
    const jobId = this.pendingGuaranteedOnce.shift();
    if (!jobId) {
      this.processNextDeferred();
      return;
    }

    const job = this.storage.getJob(jobId);
    if (!job || !job.enabled || !job.guaranteed) {
      this.processNextDeferred();
      return;
    }

    const modelFailed = this.agentEndHasError(messages);

    if (!modelFailed) {
      this.confirmJobSuccess(job);
      this.processNextDeferred();
      return;
    }

    this.storage.updateJob(jobId, { lastStatus: "error" });
    this.emitChange({ type: "error", jobId, error: "Model error (retrying in 10m)" });
    this.scheduleRetryTimer(jobId);
    this.processNextDeferred();
  }

  /**
   * Mark a guaranteed job as successfully processed by the model.
   * Once-jobs are removed from storage; recurring jobs have their lastRun updated.
   */
  private confirmJobSuccess(job: CronJob): void {
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
      const pastDue = new Date(job.schedule) <= now;
      const neverRan = !job.lastRun;
      const guaranteedRetry = !!job.guaranteed && (job.lastStatus === "error" || job.lastStatus === "sent");
      return pastDue && (neverRan || guaranteedRetry);
    }

    if (job.type === "interval" && job.intervalMs) {
      if (job.guaranteed && (job.lastStatus === "error" || job.lastStatus === "sent")) {
        return true;
      }
      const checkFrom = new Date(job.lastRun ?? job.createdAt);
      return checkFrom.getTime() + job.intervalMs <= now.getTime();
    }

    if (job.type === "cron") {
      if (job.guaranteed && (job.lastStatus === "error" || job.lastStatus === "sent")) {
        return true;
      }
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

    const retry = this.retries.get(id);
    if (retry) {
      clearTimeout(retry);
      this.retries.delete(id);
    }

    const pendingIdx = this.pendingGuaranteedOnce.indexOf(id);
    if (pendingIdx !== -1) {
      this.pendingGuaranteedOnce.splice(pendingIdx, 1);
    }

    this.deferredActions = this.deferredActions.filter(
      (a) => (a.type === "send" ? a.job.id : a.jobId) !== id
    );

    // If we were retrying this job, unblock so other deferred actions can proceed.
    if (this.retryingJobId === id) {
      this.retrying = false;
      this.retryingJobId = null;
      this.processNextDeferred();
    }
  }

  // --- Execution ---

  private async executeJobIfLeader(job: CronJob): Promise<void> {
    if (this.agentRunning || this.retrying || this.sending) {
      // Block new sends while the agent is active or a send/retry is in-flight.
      if (!this.deferredActions.some((a) => a.type === "send" && a.job.id === job.id)) {
        this.deferredActions.push({ type: "send", job });
      }
      return;
    }
    // Claim the in-flight slot synchronously before the first await so that
    // other executeJobIfLeader calls queued in the same event-loop turn (e.g.
    // multiple missed jobs firing at startup) see sending=true and defer.
    this.sending = true;
    const isLeader = await this.acquireLeadership();
    if (!isLeader) {
      this.sending = false;
      this.processNextDeferred();
      return;
    }
    await this.executeJob(job);
  }

  private async executeJob(job: CronJob): Promise<void> {
    // Final guard after leadership wait: if agent started while we waited, defer
    // instead of racing against isStreaming and producing an error.
    if (this.agentRunning) {
      this.sending = false;
      if (!this.deferredActions.some((a) => a.type === "send" && a.job.id === job.id)) {
        this.deferredActions.unshift({ type: "send", job });
      }
      return;
    }

    console.log(`Executing scheduled prompt: ${job.name} (${job.id})`);

    let sendCompleted = false;
    try {
      this.storage.updateJob(job.id, { lastStatus: "running" });
      this.emitChange({ type: "fire", job });

      this.pi.sendMessage({
        customType: "scheduled_prompt",
        content: [{ type: "text", text: job.prompt }],
        display: true,
        details: { jobId: job.id, jobName: job.name, prompt: job.prompt },
      });

      this.pi.sendUserMessage(
        `This is an automated scheduled prompt. Interpret and execute the following directly — phrases like "remind me" mean perform the action now, not schedule another reminder:\n\n${job.prompt}`
      );
      // Both send calls succeeded — an agent turn is now in flight.
      // sending will be cleared by notifyAgentEnd when the turn completes.
      sendCompleted = true;

      // Track which job's message is now at the tail of context.
      this.contextTailJobId = job.id;

      if (job.guaranteed) {
        // Defer the success/lastRun update until agent_end confirms the model processed
        // the prompt. For once-jobs this also prevents premature removal from storage.
        this.storage.updateJob(job.id, { lastStatus: "sent" });
        this.pendingGuaranteedOnce.push(job.id);

        // Watchdog: if agent_end is suppressed (e.g. compaction), sending would stay
        // true forever. Reset after a timeout so guaranteed jobs can be retried.
        this.sendingWatchdog = setTimeout(() => {
          this.sendingWatchdog = null;
          console.warn(`[scheduler] Watchdog: no agent_end after ${this.SENDING_WATCHDOG_MS / 1000}s for job ${job.id} — resetting state`);
          this.sending = false;
          this.agentRunning = false;
          this.currentTurnIsScheduled = true;
          const stuckJobId = this.pendingGuaranteedOnce.shift();
          if (stuckJobId) {
            const current = this.storage.getJob(stuckJobId);
            if (current && current.enabled && current.guaranteed) {
              this.storage.updateJob(stuckJobId, { lastStatus: "error" });
              this.emitChange({ type: "error", jobId: stuckJobId, error: "No response received (retrying in 10m)" });
              this.scheduleRetryTimer(stuckJobId);
            }
          }
          this.processNextDeferred();
        }, this.SENDING_WATCHDOG_MS);
      } else if (job.type === "once") {
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
      const errMsg = error instanceof Error ? error.message : String(error);

      if (!sendCompleted) {
        // Race: agent became active between our guard and the actual send.
        // Defer the job so it retries after the current turn ends naturally.
        if (errMsg.includes("Agent is already processing")) {
          this.sending = false;
          if (!this.deferredActions.some((a) => a.type === "send" && a.job.id === job.id)) {
            this.deferredActions.unshift({ type: "send", job });
          }
          return;
        }
        // The send itself threw for another reason — no agent turn was started,
        // so notifyAgentEnd will never arrive to clear this flag.
        this.sending = false;
        this.processNextDeferred();
      }

      if (job.guaranteed) {
        this.storage.updateJob(job.id, {
          lastRun: new Date().toISOString(),
          lastStatus: "error",
        });
        this.emitChange({ type: "error", jobId: job.id, error: `${errMsg} (retrying in 10m)` });

        const retry = setTimeout(() => {
          this.retries.delete(job.id);
          const current = this.storage.getJob(job.id);
          if (current && current.enabled && current.guaranteed) {
            void this.executeJobIfLeader(current);
          }
        }, GUARANTEED_RETRY_DELAY_MS);
        this.retries.set(job.id, retry);
      } else {
        this.storage.updateJob(job.id, {
          lastRun: new Date().toISOString(),
          lastStatus: "error",
          ...(job.type === "once" && { enabled: false }),
        });
        this.emitChange({ type: "error", jobId: job.id, error: errMsg });
      }
    }
  }

  /**
   * Schedule a 10-minute retry timer for a guaranteed job.
   * When the timer fires, calls triggerRetry() unless another retry is in-flight,
   * in which case the job is deferred until the active retry resolves.
   */
  private scheduleRetryTimer(jobId: string): void {
    const retry = setTimeout(() => {
      this.retries.delete(jobId);
      if (this.agentRunning || this.retrying || this.sending) {
        const current = this.storage.getJob(jobId);
        if (current && current.enabled && current.guaranteed) {
          if (!this.deferredActions.some((a) => a.type === "retry" && a.jobId === jobId)) {
            this.deferredActions.push({ type: "retry", jobId });
          }
        }
        return;
      }
      this.triggerRetry(jobId);
    }, GUARANTEED_RETRY_DELAY_MS);
    this.retries.set(jobId, retry);
  }

  /**
   * Initiate a retry for a guaranteed once-job.
   * Uses retryLastTurn() when the context tail still belongs to this job (clean retry,
   * no duplicate user message), otherwise falls back to a fresh sendUserMessage.
   */
  private triggerRetry(jobId: string): void {
    const current = this.storage.getJob(jobId);
    if (!current || !current.enabled || !current.guaranteed) return;

    this.retrying = true;
    this.retryingJobId = jobId;

    if (this.contextTailJobId === jobId) {
      // Context ends with this job's error message — use retryLastTurn for a clean replay.
      try {
        this.pi.retryLastTurn();
      } catch (error) {
        // retryLastTurn() threw (e.g. context changed unexpectedly) — reset and retry later.
        this.retrying = false;
        this.retryingJobId = null;
        console.error(`retryLastTurn failed for job ${jobId}:`, error);
        this.storage.updateJob(jobId, { lastStatus: "error" });
        this.scheduleRetryTimer(jobId);
      }
    } else {
      // Context has changed since the failure (another job fired in between).
      // Re-send as a new user message; it will appear twice in context but at least runs.
      try {
        this.pi.sendMessage({
          customType: "scheduled_prompt",
          content: [{ type: "text", text: current.prompt }],
          display: true,
          details: { jobId: current.id, jobName: current.name, prompt: current.prompt },
        });
        this.pi.sendUserMessage(
          `This is an automated scheduled prompt. Interpret and execute the following directly — phrases like "remind me" mean perform the action now, not schedule another reminder:\n\n${current.prompt}`
        );
        this.contextTailJobId = jobId;
      } catch (error) {
        this.retrying = false;
        this.retryingJobId = null;
        const errMsg = error instanceof Error ? error.message : String(error);
        console.error(`Failed to re-send job ${jobId}:`, error);
        this.storage.updateJob(jobId, { lastStatus: "error" });
        this.emitChange({ type: "error", jobId, error: `${errMsg} (retrying in 10m)` });
        this.scheduleRetryTimer(jobId);
      }
    }
  }

  private processNextDeferred(): void {
    const action = this.deferredActions.shift();
    if (!action) return;

    if (action.type === "send") {
      void this.executeJobIfLeader(action.job);
    } else {
      this.triggerRetry(action.jobId);
    }
  }

  private agentEndHasError(messages: readonly unknown[]): boolean {
    return messages.some(
      (m) =>
        typeof m === "object" &&
        m !== null &&
        (m as Record<string, unknown>)["role"] === "assistant" &&
        (m as Record<string, unknown>)["stopReason"] === "error"
    );
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

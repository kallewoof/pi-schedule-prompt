import * as fs from "fs";
import * as path from "path";
import { Cron } from "croner";
import type { ExtensionAPI } from "@mariozechner/pi-coding-agent";
import type { CronJob, CronChangeEvent, RunRecord } from "./types.js";
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
  /** Which job triggered the current agent turn (for output capture). */
  private currentTurnJobId: string | null = null;
  /** When the current turn's job was fired (ISO timestamp). */
  private currentTurnStartTime: string | null = null;
  /**
   * Pending setTimeout(0) handles from processNextDeferred. We defer dequeues to
   * the next macrotask so they run AFTER pi's finishRun() clears isStreaming —
   * see processNextDeferred() for details. Tracked so stop() can cancel them.
   */
  private deferralTimers = new Set<NodeJS.Timeout>();

  /** Job IDs currently running in dedicated subprocess mode. */
  private runningDedicatedJobs = new Set<string>();
  /** AbortControllers for in-flight dedicated subprocess execs, keyed by jobId. */
  private dedicatedJobControllers = new Map<string, AbortController>();

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
          // If the previous attempt errored or was sent-but-unconfirmed, route through
          // the retry timer rather than firing immediately on session_start. This prevents
          // every failed dedicated job from re-launching in parallel each /new, which can
          // overwhelm the model API and produce more failures.
          if (job.lastStatus === "error" || job.lastStatus === "sent") {
            this.scheduleRetryTimer(job.id);
            if (job.type !== "once") {
              this.scheduleJob(job);
            }
          } else {
            void this.executeJobIfLeader(job);
            if (job.type !== "once") {
              this.scheduleJob(job);
            }
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
    for (const timer of this.deferralTimers) {
      clearTimeout(timer);
    }
    this.deferralTimers.clear();
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
    this.currentTurnJobId = null;
    this.currentTurnStartTime = null;
    for (const controller of this.dedicatedJobControllers.values()) {
      controller.abort();
    }
    this.dedicatedJobControllers.clear();
    this.runningDedicatedJobs.clear();
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

    const turnJobId = this.currentTurnJobId;
    const turnStartTime = this.currentTurnStartTime;
    this.currentTurnJobId = null;
    this.currentTurnStartTime = null;

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
          this.captureRunRecord(job, messages, "success", turnStartTime);
          this.confirmJobSuccess(job);
        } else {
          this.captureRunRecord(job, messages, "error", turnStartTime);
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
      // Non-guaranteed recurring job: capture output for /replay
      if (turnJobId) {
        const job = this.storage.getJob(turnJobId);
        if (job) this.captureRunRecord(job, messages, "success", turnStartTime);
      }
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
      this.captureRunRecord(job, messages, "success", turnStartTime);
      this.confirmJobSuccess(job);
      this.processNextDeferred();
      return;
    }

    this.captureRunRecord(job, messages, "error", turnStartTime);
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
        // Clamp to 0 so a schedule that became past-due between tool validation
        // and here (e.g. storage write latency, clock race) still fires on the
        // next event-loop tick rather than being silently dropped.
        const delay = Math.max(0, targetDate.getTime() - Date.now());
        const timeout = setTimeout(async () => {
          await this.executeJobIfLeader(job);
        }, delay);
        this.intervals.set(job.id, timeout as any);
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
    if (job.dedicatedContext) {
      // Runs in an isolated subprocess — does not block the main agent.
      void this.executeDedicatedJob(job);
      return;
    }
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

      const displayMessage = {
        customType: "scheduled_prompt",
        content: [{ type: "text" as const, text: job.prompt }],
        display: true,
        details: { jobId: job.id, jobName: job.name, prompt: job.prompt },
      };
      const wrappedPrompt = `This is an automated scheduled prompt. Interpret and execute the following directly — phrases like "remind me" mean perform the action now, not schedule another reminder:\n\n${job.prompt}`;

      const piAny = this.pi as any;
      const useContext =
        job.targetContext &&
        typeof piAny.sendMessageToContext === "function" &&
        typeof piAny.sendUserMessageToContext === "function";

      if (useContext) {
        piAny.sendMessageToContext(job.targetContext, displayMessage);
        piAny.sendUserMessageToContext(job.targetContext, wrappedPrompt);
      } else {
        this.pi.sendMessage(displayMessage);
        this.pi.sendUserMessage(wrappedPrompt);
      }
      // Both send calls succeeded — an agent turn is now in flight.
      // sending will be cleared by notifyAgentEnd when the turn completes.
      sendCompleted = true;

      // Track which job's message is now at the tail of context.
      this.contextTailJobId = job.id;
      this.currentTurnJobId = job.id;
      this.currentTurnStartTime = new Date().toISOString();

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
        // The send threw synchronously (e.g. pi binding missing). Pi-mono's real
        // sendUserMessage wrapper is fire-and-forget and never throws synchronously,
        // so this branch only catches setup-time bugs. The agent-busy race is
        // avoided architecturally in processNextDeferred (setTimeout(0)).
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
    // Cancel any pending retry for this job before creating a new one.
    // Without this, scheduleRetryTimer being called twice (e.g. once from start()
    // for an error-state job and again after a fresh failure of the same job)
    // would leave both timers active. Both fire in succession — the first triggers
    // a retry, and the second fires shortly after, kicking off another retry as
    // soon as the first's runningDedicatedJobs guard clears. Net effect: the user
    // sees back-to-back duplicate runs of the same scheduled prompt.
    const existing = this.retries.get(jobId);
    if (existing) {
      clearTimeout(existing);
    }
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

    if (current.dedicatedContext) {
      void this.executeDedicatedJob(current);
      return;
    }

    this.retrying = true;
    this.retryingJobId = jobId;
    this.currentTurnJobId = jobId;
    this.currentTurnStartTime = new Date().toISOString();

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

  private async executeDedicatedJob(job: CronJob): Promise<void> {
    if (this.runningDedicatedJobs.has(job.id)) return;
    this.runningDedicatedJobs.add(job.id);

    const startTime = new Date().toISOString();
    const controller = new AbortController();
    this.dedicatedJobControllers.set(job.id, controller);

    const piAny = this.pi as any;
    const notifyToContext =
      job.targetContext &&
      typeof piAny.sendMessageToContext === "function";

    const notify = (customType: string, text: string, details: Record<string, unknown>) => {
      const msg = {
        customType,
        content: [{ type: "text" as const, text }],
        display: true,
        details,
      };
      if (notifyToContext) {
        piAny.sendMessageToContext(job.targetContext, msg);
      } else {
        this.pi.sendMessage(msg);
      }
    };

    notify(
      "scheduled_prompt_begin",
      `[Scheduled Prompt] Processing begins: ${job.name} → "${job.prompt}"`,
      { jobId: job.id, jobName: job.name, prompt: job.prompt, startTime: startTime }
    );

    this.storage.updateJob(job.id, { lastStatus: "running" });
    this.emitChange({ type: "fire", job });

    const wrappedPrompt =
      `This is an automated scheduled prompt. Interpret and execute the following directly — ` +
      `phrases like "remind me" mean perform the action now, not schedule another reminder:\n\n${job.prompt}`;

    let status: "success" | "error" = "success";
    let output = "";
    const TIMEOUT_MS = 5 * 60 * 1000;
    const startMs = Date.now();

    try {
      const result = await this.pi.exec(
        "pi",
        ["--mode", "json", "-p", "--no-extensions", "--no-session", wrappedPrompt],
        { signal: controller.signal, timeout: TIMEOUT_MS }
      );
      const durationS = Math.round((Date.now() - startMs) / 1000);
      const formatted = formatDedicatedRunOutput(result.stdout, result.stderr, result.code, result.killed, durationS);
      output = formatted.output;
      // A successful pi --mode json run always emits an agent_end event with at least one
      // assistant message containing text. If we didn't see that, treat as failure even
      // when the exit code is 0 (timeout-killed processes resolve with code 0 sometimes).
      status = result.code === 0 && !result.killed && formatted.hasAgentEnd ? "success" : "error";
    } catch (error) {
      if ((error as Error & { name?: string }).name === "AbortError") {
        // Scheduler was stopped — exit without updating storage
        this.runningDedicatedJobs.delete(job.id);
        this.dedicatedJobControllers.delete(job.id);
        return;
      }
      const durationS = Math.round((Date.now() - startMs) / 1000);
      const errMsg = error instanceof Error ? error.message : String(error);
      output = `[exit=? killed=? duration=${durationS}s]\n[error]\n${errMsg}`;
      status = "error";
    } finally {
      this.runningDedicatedJobs.delete(job.id);
      this.dedicatedJobControllers.delete(job.id);
    }

    this.captureRunRecordFromOutput(job, output, status, startTime);

    if (job.type === "once") {
      if (status === "success" || !job.guaranteed) {
        this.storage.removeJob(job.id);
        this.emitChange({ type: "remove", jobId: job.id });
      } else {
        this.storage.updateJob(job.id, { lastStatus: "error" });
        this.emitChange({ type: "error", jobId: job.id, error: "Subprocess error (retrying in 10m)" });
        this.scheduleRetryTimer(job.id);
      }
    } else {
      const nextRun = this.getNextRun(job.id);
      this.storage.updateJob(job.id, {
        lastRun: new Date().toISOString(),
        lastStatus: status,
        runCount: job.runCount + 1,
        nextRun: nextRun?.toISOString(),
      });
      this.emitChange({ type: "fire", job });
      // Recurring guaranteed jobs that fail need a retry timer too — without this,
      // a failed daily/hourly job has no recovery path until its next natural cron tick.
      if (status === "error" && job.guaranteed) {
        this.scheduleRetryTimer(job.id);
      }
    }

    const hint = status === "error" ? detectFailureHint(output) : null;
    const endText = hint
      ? `[Scheduled Prompt] Processing ended (failed). See /replay ${job.id} to review.\n⚠️ Likely cause: ${hint}`
      : `[Scheduled Prompt] Processing ended. See /replay ${job.id} to review.`;
    notify(
      "scheduled_prompt_end",
      endText,
      {
        jobId: job.id,
        jobName: job.name,
        failureHint: hint ?? undefined,
        startTime,
        endTime: new Date().toISOString(),
      }
    );
  }

  private captureRunRecordFromOutput(
    job: CronJob,
    output: string,
    status: "success" | "error",
    startTime: string | null
  ): void {
    const endTime = new Date().toISOString();
    this.storage.addRunRecord({
      jobId: job.id,
      jobName: job.name,
      jobPrompt: job.prompt,
      schedule: job.schedule,
      jobType: job.type,
      startTime: startTime ?? endTime,
      endTime,
      output,
      status,
    });
  }

  private captureRunRecord(
    job: CronJob,
    messages: readonly unknown[],
    status: "success" | "error",
    startTime: string | null
  ): void {
    const endTime = new Date().toISOString();
    const record: RunRecord = {
      jobId: job.id,
      jobName: job.name,
      jobPrompt: job.prompt,
      schedule: job.schedule,
      jobType: job.type,
      startTime: startTime ?? endTime,
      endTime,
      output: this.extractTurnOutput(messages, job.prompt),
      status,
    };
    this.storage.addRunRecord(record);
  }

  private extractTurnOutput(messages: readonly unknown[], jobPrompt: string): string {
    const msgs = messages as Array<Record<string, unknown>>;
    // Find the last user message containing our scheduled prompt (identifies turn start)
    let turnStartIdx = -1;
    for (let i = msgs.length - 1; i >= 0; i--) {
      if (msgs[i]["role"] === "user") {
        const c = msgs[i]["content"];
        const text =
          typeof c === "string"
            ? c
            : Array.isArray(c)
            ? (c as Array<Record<string, unknown>>)
                .filter((b) => b["type"] === "text")
                .map((b) => b["text"] as string)
                .join("")
            : "";
        if (text.includes(jobPrompt)) {
          turnStartIdx = i;
          break;
        }
      }
    }
    // Render every message after the turn start: thinking, text, tool calls, tool results.
    const startIdx = turnStartIdx >= 0 ? turnStartIdx + 1 : 0;
    const slice = msgs.slice(startIdx);
    return renderMessages(slice);
  }

  private processNextDeferred(): void {
    const action = this.deferredActions.shift();
    if (!action) return;

    // Defer to the next macrotask. processNextDeferred runs from inside our
    // notifyAgentEnd, which executes WHILE pi is still emitting agent_end —
    // pi's finishRun() (which clears isStreaming) runs only after the entire
    // microtask chain unwinds. If we ran the send synchronously through
    // microtasks, sendUserMessage would race pi's still-true isStreaming flag
    // and pi would surface the failure as `Extension "<runtime>" error`.
    // setTimeout(0) puts us in the next macrotask, after finishRun.
    const timer = setTimeout(() => {
      this.deferralTimers.delete(timer);
      if (action.type === "send") {
        void this.executeJobIfLeader(action.job);
      } else {
        this.triggerRetry(action.jobId);
      }
    }, 0);
    this.deferralTimers.add(timer);
  }

  private agentEndHasError(messages: readonly unknown[]): boolean {
    // Only the most recent assistant message reflects this turn's outcome.
    // Why: scanning the whole history would flag every successful turn as failed
    // once any earlier turn in the session had errored (e.g. an LLM-not-ready
    // failure at session start), triggering bogus retries that then call
    // retryLastTurn() against a successful "stop" message.
    for (let i = messages.length - 1; i >= 0; i--) {
      const m = messages[i];
      if (
        typeof m === "object" &&
        m !== null &&
        (m as Record<string, unknown>)["role"] === "assistant"
      ) {
        return (m as Record<string, unknown>)["stopReason"] === "error";
      }
    }
    return false;
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

/**
 * Format a dedicated subprocess run for storage as `RunRecord.output`.
 * Always emits a diagnostic header so empty/timeout runs aren't blank.
 * Parses `pi --mode json` JSONL stdout to render thinking, tool calls, and tool results.
 */
export function formatDedicatedRunOutput(
  stdout: string,
  stderr: string,
  code: number,
  killed: boolean,
  durationS: number
): { output: string; hasAgentEnd: boolean } {
  const header = `[exit=${code} killed=${killed} duration=${durationS}s]`;
  const events: Array<Record<string, unknown>> = [];
  let hasAgentEnd = false;

  for (const line of stdout.split("\n")) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    try {
      const event = JSON.parse(trimmed) as Record<string, unknown>;
      events.push(event);
      if (event["type"] === "agent_end") hasAgentEnd = true;
    } catch {
      // Non-JSON line — likely a startup banner or error; ignored for rendering.
    }
  }

  const sections: string[] = [header];

  // Prefer the agent_end snapshot (full messages array) when present.
  let messagesToRender: ReadonlyArray<Record<string, unknown>> | null = null;
  for (let i = events.length - 1; i >= 0; i--) {
    const ev = events[i];
    if (ev["type"] === "agent_end" && Array.isArray(ev["messages"])) {
      messagesToRender = ev["messages"] as Array<Record<string, unknown>>;
      break;
    }
  }

  // Fall back to reconstructing message state from message_*/turn_end events
  // when the stream was cut off before agent_end (e.g. timeout kill).
  //
  // CRITICAL: each streaming `message_update` event carries a snapshot of the
  // *same* logical message with growing content. Naively pushing every event's
  // `message` produces N copies of the same logical message, each rendered
  // separately. A single 5-min run with 5000+ thinking deltas produced 27 MB
  // of duplicated `[thinking]` blocks before this dedup. We collapse runs of
  // updates per logical message into a single final state.
  if (!messagesToRender) {
    const closedMessages: Array<Record<string, unknown>> = [];
    let openMessage: Record<string, unknown> | null = null;
    for (const ev of events) {
      const t = ev["type"];
      if (t === "message_start") {
        if (openMessage) closedMessages.push(openMessage);
        openMessage = (ev["message"] as Record<string, unknown> | undefined) ?? null;
      } else if (t === "message_update") {
        const m = ev["message"] as Record<string, unknown> | undefined;
        if (m) openMessage = m; // replace with latest snapshot of the same message
      } else if (t === "message_end") {
        const m = ev["message"] as Record<string, unknown> | undefined;
        if (m) closedMessages.push(m);
        else if (openMessage) closedMessages.push(openMessage);
        openMessage = null;
      } else if (t === "turn_end") {
        // turn_end carries the assistant message + toolResults; useful when
        // individual message_end events were dropped.
        const m = ev["message"] as Record<string, unknown> | undefined;
        if (m && !closedMessages.includes(m)) closedMessages.push(m);
        const toolResults = ev["toolResults"];
        if (Array.isArray(toolResults)) {
          for (const tr of toolResults as Array<Record<string, unknown>>) {
            closedMessages.push(tr);
          }
        }
      }
    }
    if (openMessage) closedMessages.push(openMessage);
    if (closedMessages.length > 0) messagesToRender = closedMessages;
  }

  if (messagesToRender && messagesToRender.length > 0) {
    const rendered = renderMessages(messagesToRender);
    if (rendered) sections.push(rendered);
  } else if (stdout.trim().length > 0 && events.length === 0) {
    // No JSON parsed at all — store raw stdout (legacy `pi -p` mode or unexpected output).
    sections.push(stdout.trim());
  }

  if (stderr.trim().length > 0) {
    sections.push(`[stderr]\n${stderr.trim()}`);
  }

  let output = sections.join("\n\n");
  // Cap at 64 KB to keep storage bounded. Without this, a runaway agent that
  // emits multi-MB thinking blocks per run plus retained records can balloon
  // the storage file to hundreds of MB.
  const MAX_OUTPUT_BYTES = 64 * 1024;
  if (output.length > MAX_OUTPUT_BYTES) {
    const head = output.slice(0, MAX_OUTPUT_BYTES);
    output = `${head}\n\n[output truncated; full size = ${output.length} bytes]`;
  }
  return { output, hasAgentEnd };
}

/**
 * Inspect a rendered run output for known failure signatures and return a short
 * actionable hint describing the likely cause. Returns null if no match.
 *
 * Patterns target the things that have actually broken dedicated runs in practice:
 * pi-guardrails path-access blocks (often the bash tool tries to write to /tmp for
 * truncated output and the non-interactive subprocess can't answer the prompt),
 * read-only filesystems, model/connection errors, and unrecognized agent stalls.
 */
export function detectFailureHint(output: string): string | null {
  // pi-guardrails path-access denial. Two flavours: explicit "no UI to confirm"
  // (mode=ask in a non-interactive subprocess) and the generic block string.
  const noUiMatch = output.match(/Access to ([^\s]+) is blocked \(outside working directory, no UI to confirm\)/);
  if (noUiMatch) {
    return `pi-guardrails blocked access to ${noUiMatch[1]} (no UI to confirm in dedicated subprocess). Add the path to \`pathAccess.allowedPaths\` in ~/.pi/agent/extensions/guardrails.json.`;
  }
  const blockedMatch = output.match(/Access to ([^\s]+) is blocked \(outside working directory\)/);
  if (blockedMatch) {
    return `pi-guardrails blocked access to ${blockedMatch[1]}. Add the path to \`pathAccess.allowedPaths\` in ~/.pi/agent/extensions/guardrails.json or set \`pathAccess.mode\` to "allow".`;
  }
  if (/Read-only file system/.test(output)) {
    return `subprocess reported "Read-only file system" — the dedicated job's writes were rejected. Check sandboxing (bwrap/firejail/etc.) on the pi binary path.`;
  }
  if (/Connection error\.|ECONNREFUSED|ENETUNREACH|ETIMEDOUT/.test(output)) {
    return `network/connection error talking to the model provider. Likely transient — the next 10-min retry should clear it.`;
  }
  if (/killed=true/.test(output)) {
    return `subprocess hit the 5-minute timeout without finishing. Either the prompt is too large for one turn, or the agent got stuck in a loop.`;
  }
  return null;
}

/**
 * Render a flat message array into a human-readable transcript.
 * Used by both `formatDedicatedRunOutput` and the main-session capture path.
 */
export function renderMessages(messages: ReadonlyArray<Record<string, unknown>>): string {
  const parts: string[] = [];
  for (const m of messages) {
    const role = m["role"];
    if (role === "user") {
      const text = stringifyContent(m["content"]);
      if (text) parts.push(`[user]\n${text}`);
    } else if (role === "assistant") {
      const content = m["content"];
      if (Array.isArray(content)) {
        for (const block of content as Array<Record<string, unknown>>) {
          const bt = block["type"];
          if (bt === "text" && typeof block["text"] === "string" && (block["text"] as string).trim()) {
            parts.push(`[assistant]\n${(block["text"] as string).trim()}`);
          } else if (bt === "thinking" && typeof block["thinking"] === "string" && (block["thinking"] as string).trim()) {
            parts.push(`[thinking]\n${(block["thinking"] as string).trim()}`);
          } else if (bt === "toolCall") {
            const name = block["name"] as string | undefined;
            const args = block["arguments"];
            const argsText = args === undefined ? "" : (() => {
              try {
                const json = JSON.stringify(args);
                return json.length > 500 ? json.slice(0, 500) + "…" : json;
              } catch {
                return String(args);
              }
            })();
            parts.push(`[tool: ${name ?? "?"}] ${argsText}`);
          }
        }
      } else if (typeof content === "string" && content.trim()) {
        parts.push(`[assistant]\n${content.trim()}`);
      }
    } else if (role === "toolResult") {
      const name = m["toolName"] as string | undefined;
      const text = stringifyContent(m["content"]);
      const isError = m["isError"] === true;
      const truncated = text.length > 1000 ? text.slice(0, 1000) + "…" : text;
      parts.push(`[result: ${name ?? "?"}${isError ? " (error)" : ""}]\n${truncated}`);
    }
  }
  return parts.join("\n\n");
}

function stringifyContent(content: unknown): string {
  if (typeof content === "string") return content.trim();
  if (Array.isArray(content)) {
    return (content as Array<Record<string, unknown>>)
      .filter((b) => b["type"] === "text" && typeof b["text"] === "string")
      .map((b) => (b["text"] as string).trim())
      .filter(Boolean)
      .join("\n");
  }
  return "";
}

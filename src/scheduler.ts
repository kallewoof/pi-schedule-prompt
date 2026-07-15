import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { Cron } from "croner";
import type { ExtensionAPI } from "@mariozechner/pi-coding-agent";
import type { CronJob, CronChangeEvent, RunRecord, SessionShutdownReason } from "./types.js";
import type { CronStorage } from "./storage.js";

const GUARANTEED_RETRY_DELAY_MS = 10 * 60 * 1000;
const DEDICATED_JOB_TIMEOUT_MS = 60 * 60 * 1000;
const DEDICATED_JOB_TIMEOUT_MIN = DEDICATED_JOB_TIMEOUT_MS / 60_000;

/**
 * Stale-slot watchdog: how often to sweep the host-wide subprocess slot, and
 * how old an in-flight entry may get before it's treated as leaked and evicted.
 *
 * `executeCommandJob`/`executeDedicatedJob` clear their in-flight entry in a
 * `finally`, so a normally-settling `pi.exec` always frees the slot. But if the
 * awaited `exec` never settles (hung child, ineffective `timeout`), the entry
 * leaks and `isSubprocessSlotBusy()` stays true forever — every later command/
 * dedicated job then queues and never dispatches. The sweep guarantees the slot
 * is recoverable. The stale threshold sits just past the exec timeout so a
 * healthy run's own timeout fires first; the watchdog only catches true leaks.
 */
const SUBPROCESS_SWEEP_MS = 60 * 1000;
const SUBPROCESS_STALE_MS = DEDICATED_JOB_TIMEOUT_MS + 2 * 60 * 1000;

/**
 * Segregated session dir for dedicated subprocesses. Keeps them out of the
 * default `~/.pi/agent/sessions/<cwd>/` selector while still leaving a session
 * file on disk for post-mortem inspection if a dedicated run crashes.
 */
const DEDICATED_SESSION_DIR = path.join(os.homedir(), ".pi", "agent", "schedule-prompt-sessions");

/**
 * Default per-project session dir that `pi --resume` lists. Mirrors pi-mono's
 * `getDefaultSessionDirPath()` encoding (session-manager.ts: `--<cwd>--` with
 * `/`, `\`, `:` collapsed to `-`). `DEDICATED_SESSION_DIR` is a sibling of this
 * tree's `sessions/` folder, so we derive the resumable dir relative to it —
 * keeping the same hardcoded `~/.pi/agent` assumption the segregated dir already
 * makes (i.e. not honoring pi's `PI_CODING_AGENT_DIR` override, by design).
 */
export function defaultResumableSessionDir(
  cwd: string,
  sessionsRoot: string = path.join(DEDICATED_SESSION_DIR, "..", "sessions")
): string {
  const resolved = path.resolve(cwd);
  const safe = `--${resolved.replace(/^[/\\]/, "").replace(/[/\\:]/g, "-")}--`;
  return path.join(sessionsRoot, safe);
}

/**
 * Move a segregated dedicated-session file into the resumable session dir so it
 * shows up in `pi --resume` and survives run-history eviction. Idempotent:
 * returns the path to switch to (the already-promoted path when nothing to move).
 * `sessionsRoot` is injectable for tests; production uses the default.
 */
export function promoteSessionToResumable(sessionFilePath: string, cwd: string, sessionsRoot?: string): string {
  const destDir = defaultResumableSessionDir(cwd, sessionsRoot);
  const dest = path.join(destDir, path.basename(sessionFilePath));
  if (path.resolve(sessionFilePath) === path.resolve(dest)) return sessionFilePath; // already promoted
  if (fs.existsSync(dest)) return dest; // same session id already present at the destination
  fs.mkdirSync(destDir, { recursive: true });
  try {
    fs.renameSync(sessionFilePath, dest);
  } catch {
    // Cross-device rename (EXDEV) — fall back to copy + unlink.
    fs.copyFileSync(sessionFilePath, dest);
    fs.unlinkSync(sessionFilePath);
  }
  return dest;
}

/**
 * Module-scope tracking of in-flight dedicated subprocesses. Lives across
 * scheduler instances because session replacement (`/new`, `/fork`, `/resume`,
 * `/reload`) discards the old CronScheduler and constructs a new one — but the
 * subprocess started by the old scheduler is intentionally kept alive (see
 * `stop({ reason })`). Centralising here lets the new scheduler:
 *   - skip firing a job that's already running in the prior instance's promise,
 *   - report `runJobNow` as `"queued"` for such jobs,
 *   - inherit user-requested retry intents queued before the swap.
 *
 * The map carries enough metadata for `/schedule-prompt ps` to render the
 * activity even when the originating scheduler instance is gone.
 *
 * The maps are cleared only on `stop({ reason: "quit" })` (host process is
 * exiting) — at which point the abort signals are also fired.
 */
interface InFlightEntry {
  controller: AbortController;
  jobId: string;
  jobName: string;
  prompt: string;
  /** ISO timestamp of when the subprocess was launched. */
  startTime: string;
}
const inFlightDedicated = new Map<string, InFlightEntry>();
const pendingDedicatedRetriesGlobal = new Set<string>();

/**
 * Module-scope tracking of in-flight command-mode subprocesses. Parallel to
 * `inFlightDedicated`; kept separate so the dedicated-only guards in
 * `runJobNow` / `start()` don't accidentally see command jobs as dedicated
 * and skip-fire them through the wrong path.
 */
const inFlightCommand = new Map<string, InFlightEntry>();
const pendingCommandRetriesGlobal = new Set<string>();

/**
 * Host-wide single-slot queue for dedicated + command subprocesses.
 *
 * Without this, two cron jobs whose schedules land on the same minute (e.g.
 * two daily routines both at 22:42) each spawn a full `pi --mode json`
 * subprocess in parallel. Two concurrent agents talking to the model
 * provider exhaust API quota and frequently produce coordinated failures
 * (timeouts, rate-limit storms) — and any shared-file outputs they touch
 * (e.g. both writing to REPORTS/YYYY-MM-DD.md) lose data via interleaved
 * read-modify-write.
 *
 * The queue is module-scope (not per-scheduler) so it survives session
 * swaps the same way `inFlightDedicated` does. `activeScheduler` is the
 * live scheduler instance that should dispatch drained items; an old
 * scheduler whose subprocess completes after a /new will route the drain
 * through the new scheduler's `pi` rather than its own stale one.
 *
 * Items are kept as jobIds (not full CronJob snapshots) because the job's
 * prompt or enabled flag may have changed in storage between enqueue and
 * dispatch — we re-read at drain time.
 */
type SubprocessKind = "dedicated" | "command";
type SubprocessQueueEntry = { kind: SubprocessKind; jobId: string };
const subprocessQueue: SubprocessQueueEntry[] = [];
let activeScheduler: CronScheduler | null = null;

function isSubprocessSlotBusy(): boolean {
  return inFlightDedicated.size > 0 || inFlightCommand.size > 0;
}

/** Snapshot of the global subprocess queue for `/schedule-prompt ps`. */
export function getQueuedSubprocessJobIds(): ReadonlyArray<string> {
  return subprocessQueue.map((e) => e.jobId);
}

/**
 * Test-only: wipe all module-scope subprocess state. Tests share this module
 * across cases (the in-flight maps and queue are module-scope so they can
 * survive session swaps in production), so they need an explicit reset to
 * avoid one test's leftover entries blocking another from firing.
 */
export function __resetSubprocessStateForTests(): void {
  for (const entry of inFlightDedicated.values()) entry.controller.abort();
  inFlightDedicated.clear();
  pendingDedicatedRetriesGlobal.clear();
  for (const entry of inFlightCommand.values()) entry.controller.abort();
  inFlightCommand.clear();
  pendingCommandRetriesGlobal.clear();
  subprocessQueue.length = 0;
  activeScheduler = null;
}

/** Snapshot of background-job activity for `/schedule-prompt ps`. */
export interface DedicatedActivity {
  inFlight: ReadonlyArray<{ jobId: string; jobName: string; prompt: string; startTime: string }>;
  queuedRetries: ReadonlyArray<string>;
}

export function getDedicatedActivity(): DedicatedActivity {
  const inFlight = [
    ...Array.from(inFlightDedicated.values()),
    ...Array.from(inFlightCommand.values()),
  ].map((e) => ({
    jobId: e.jobId,
    jobName: e.jobName,
    prompt: e.prompt,
    startTime: e.startTime,
  }));
  const queuedRetries = [
    ...Array.from(pendingDedicatedRetriesGlobal),
    ...Array.from(pendingCommandRetriesGlobal),
    ...subprocessQueue.map((e) => e.jobId),
  ];
  return { inFlight, queuedRetries };
}

type DeferredAction = { type: "send"; job: CronJob } | { type: "retry"; jobId: string };

export class CronScheduler {
  private jobs = new Map<string, Cron>();
  private intervals = new Map<string, NodeJS.Timeout>();
  /**
   * Per-job `setInterval` start time (ms). For interval jobs, the timer
   * actually fires at `anchor + intervalMs`, which can differ from
   * `lastRun + intervalMs` after a missed-on-restart drop where `lastRun`
   * was never advanced. `getNextRun` consults this anchor so the displayed
   * "Next run" matches the live timer instead of a long-elapsed value.
   */
  private intervalAnchors = new Map<string, number>();
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
  /**
   * Epoch ms of the last turn we initiated (main-session send or retry). Used as a
   * grace window by clearGateIfStale(): a send we just issued sets sending=true
   * synchronously, but pi's isStreaming only flips true once the turn actually
   * begins streaming. Within this window pi may still report idle even though the
   * "busy" flag is legitimate, so we must not mistake it for a stuck gate.
   */
  private lastSendAt = 0;
  private readonly GATE_RECOVERY_GRACE_MS = 60 * 1000;
  /**
   * Periodic sweep that evicts leaked in-flight subprocess entries so the
   * host-wide subprocess slot can't get permanently stuck. Started in start(),
   * cleared in stop(). See SUBPROCESS_STALE_MS.
   */
  private subprocessWatchdog: NodeJS.Timeout | null = null;
  /** Tracks which job's prompt is at the tail of the agent context (last sendUserMessage). */
  private contextTailJobId: string | null = null;
  /** Which job triggered the current agent turn (for output capture). */
  private currentTurnJobId: string | null = null;
  /** When the current turn's job was fired (ISO timestamp). */
  private currentTurnStartTime: string | null = null;
  /**
   * Messages from the most recent agent_end. On modern runtimes (that emit
   * agent_settled), agent_end fires per-turn while the session is still active
   * across continuations/compaction/retry — so we only record here and let
   * notifyAgentSettled() consume these once the session is authoritatively idle.
   * Reset at agent_start so a settle without a fresh agent_end can't mis-capture
   * a prior run's transcript.
   */
  private lastTurnMessages: readonly unknown[] = [];
  /**
   * Pending setTimeout(0) handles from processNextDeferred. We defer dequeues to
   * the next macrotask so they run AFTER pi's finishRun() clears isStreaming —
   * see processNextDeferred() for details. Tracked so stop() can cancel them.
   */
  private deferralTimers = new Set<NodeJS.Timeout>();

  /**
   * True after stop(). Async work (e.g. an executeDedicatedJob that was past
   * `await pi.exec(...)` when /new fired) may still try to send notifications
   * after the captured `pi` has been invalidated by pi-mono's
   * `_extensionRunner.invalidate()`. Treat any send-after-stop as a no-op so
   * the trailing notify doesn't throw a stale-ctx error into the new session.
   */
  private stopped = false;

  private readonly storage: CronStorage;
  private readonly pi: ExtensionAPI;
  private readonly leaderPidPath: string;

  constructor(storage: CronStorage, pi: ExtensionAPI) {
    this.storage = storage;
    this.pi = pi;
    this.leaderPidPath = path.join(storage.getPiDir(), "leader.pid");
  }

  start(): void {
    this.stopped = false;
    // A prior scheduler instance can still be alive with live cron timers: pi
    // fires session_start (reason "reload"/"new"/"fork"/"resume") on every
    // session swap — routine in multi-context RPC mode — and index.ts builds a
    // fresh CronScheduler each time. Nothing else stops the old one, so without
    // this its Cron timers keep firing and every job runs once per leaked
    // instance (observed as the same scheduled message delivered N times).
    // Tear the previous instance down before taking over. Pass a non-"quit"
    // reason so in-flight dedicated subprocesses survive the swap.
    if (activeScheduler && activeScheduler !== this) {
      activeScheduler.stop({ reason: "reload" });
    }
    activeScheduler = this;
    const allJobs = this.storage.getAllJobs();
    const now = new Date();

    for (const job of allJobs) {
      if (!job.enabled) continue;

      // Don't double-fire a dedicated job whose subprocess survived a session
      // swap and is still running in a prior scheduler instance's promise.
      // The natural cron tick (or its own pendingDedicatedRetriesGlobal hook)
      // will handle re-firing once that subprocess finishes.
      if ((job.dedicatedContext || job.standalone) && inFlightDedicated.has(job.id)) {
        if (job.type !== "once") this.scheduleJob(job);
        continue;
      }
      if (job.command && inFlightCommand.has(job.id)) {
        if (job.type !== "once") this.scheduleJob(job);
        continue;
      }

      if (this.isMissed(job, now)) {
        // `lastStatus === "running"` means another scheduler instance — typically
        // the parent pi that spawned us via `pi --mode json -p` — is currently
        // executing this job. The in-flight map is module-scope and doesn't
        // cross the parent/child process boundary, so the durable status is
        // the only signal we have. Firing here would dispatch a grandchild
        // subprocess; repeated recursively this OOMs the host.
        // Trade-off: a host that crashed mid-run leaves the status stuck at
        // "running" and won't auto-recover on next start — but preventing the
        // fork-bomb is worth that gap, and the user can clear the status
        // manually or it'll be reset by a successful run after a manual retry.
        if (job.lastStatus === "running") {
          if (job.type !== "once") this.scheduleJob(job);
          continue;
        }
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
          // Recurring job (interval/cron): fire the missed execution once now,
          // then resume normal cadence. A non-guaranteed interval job that
          // should have fired 20h ago should run at the soonest opportunity
          // rather than wait another full cadence — otherwise pi being offline
          // silently turns a job into ~1.5x its declared interval.
          // `guaranteed` still controls in-session retry behaviour (model errors,
          // unconfirmed sends), just not the "catch up the missed tick" decision.
          void this.executeJobIfLeader(job);
          this.scheduleJob(job);
        }
      } else {
        this.scheduleJob(job);
      }
    }

    // Drain any queue entries left behind by a prior scheduler instance
    // (e.g. items enqueued in the old scheduler that hadn't dispatched yet
    // when the session swapped). Safe no-op when the queue is empty.
    this.drainSubprocessQueue();

    this.startSubprocessWatchdog();
  }

  /**
   * Arm the periodic stale-slot sweep (idempotent). The interval is `unref`'d
   * so it never keeps the process alive on its own.
   */
  private startSubprocessWatchdog(): void {
    if (this.subprocessWatchdog !== null) return;
    const t = setInterval(() => {
      this.sweepStaleSubprocesses();
      this.recoverDeferredIfIdle();
    }, SUBPROCESS_SWEEP_MS);
    if (typeof (t as NodeJS.Timeout).unref === "function") (t as NodeJS.Timeout).unref();
    this.subprocessWatchdog = t;
  }

  /**
   * Evict in-flight subprocess entries older than SUBPROCESS_STALE_MS — they
   * represent an `exec` that never settled and is wedging the host-wide slot.
   * Abort the (likely-dead) child, drop the entry, then drain so queued jobs
   * dispatch. Exposed (not private) only so tests can trigger a sweep
   * deterministically without leaning on real timers.
   */
  sweepStaleSubprocesses(): void {
    const now = Date.now();
    let evicted = false;
    for (const map of [inFlightDedicated, inFlightCommand]) {
      for (const [jobId, entry] of map) {
        const ageMs = now - new Date(entry.startTime).getTime();
        if (ageMs > SUBPROCESS_STALE_MS) {
          console.warn(
            `Evicting stale in-flight subprocess "${entry.jobName}" (${jobId}): ` +
              `age ${Math.round(ageMs / 60_000)}m exceeds ${Math.round(SUBPROCESS_STALE_MS / 60_000)}m`
          );
          entry.controller.abort();
          map.delete(jobId);
          evicted = true;
        }
      }
    }
    if (evicted) this.drainSubprocessQueue();
  }

  /**
   * Tear down scheduling state.
   *
   * `reason` differentiates host-process exit (`"quit"`) from session
   * replacement (anything else: `"new" | "fork" | "resume" | "reload"`). On
   * session replacement the host process keeps running and any dedicated
   * subprocesses are intentionally left alive — their work is already
   * underway and aborting them defeats the entire purpose of dedicated
   * context. Only on `"quit"` (or no reason given, for backwards compat) do
   * we abort in-flight controllers and clear the module-scope registries.
   */
  stop(opts?: { reason?: SessionShutdownReason }): void {
    const fullTeardown = !opts?.reason || opts.reason === "quit";

    this.stopped = true;
    for (const cron of this.jobs.values()) {
      cron.stop();
    }
    this.jobs.clear();

    for (const interval of this.intervals.values()) {
      clearInterval(interval);
    }
    this.intervals.clear();
    this.intervalAnchors.clear();

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
    if (this.subprocessWatchdog !== null) {
      clearInterval(this.subprocessWatchdog);
      this.subprocessWatchdog = null;
    }
    this.contextTailJobId = null;
    this.currentTurnJobId = null;
    this.currentTurnStartTime = null;
    this.lastTurnMessages = [];

    if (fullTeardown) {
      // Host process is exiting — abort every in-flight dedicated subprocess
      // so we don't leave orphan PIDs behind, and clear module-scope state
      // since no scheduler will be left alive to reconcile it.
      for (const entry of inFlightDedicated.values()) {
        entry.controller.abort();
      }
      inFlightDedicated.clear();
      pendingDedicatedRetriesGlobal.clear();
      for (const entry of inFlightCommand.values()) {
        entry.controller.abort();
      }
      inFlightCommand.clear();
      pendingCommandRetriesGlobal.clear();
      subprocessQueue.length = 0;
    }
    if (activeScheduler === this) {
      activeScheduler = null;
    }
    // On session-replace: leave inFlightDedicated, inFlightCommand and their
    // pending-retry sets alone so old subprocesses run to completion and any
    // user-queued retry survives the swap.
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
    // Cron jobs live in `this.jobs`; "once" and "interval" jobs use plain
    // setTimeout/setInterval handles in `this.intervals` which don't expose
    // their next-fire time. Reconstruct it from the persisted job instead so
    // the listings/widget can sort and display "Next run" for all job types.
    const job = this.storage.getJob(jobId);
    if (!job || !job.enabled) return null;
    if (job.type === "once") {
      const target = new Date(job.schedule);
      if (isNaN(target.getTime())) return null;
      return target;
    }
    if (job.type === "interval" && job.intervalMs) {
      // Prefer the live setInterval anchor: when start() re-arms a missed
      // non-guaranteed job, lastRun is intentionally not advanced (the run
      // didn't happen), so lastRun + intervalMs is in the past — but the
      // timer will fire at anchor + intervalMs. After that first fire,
      // lastRun catches up and the anchor falls out of relevance.
      const anchor = this.intervalAnchors.get(jobId);
      const nowMs = Date.now();
      if (anchor !== undefined && anchor + job.intervalMs > nowMs) {
        return new Date(anchor + job.intervalMs);
      }
      const base = job.lastRun ? new Date(job.lastRun).getTime() : new Date(job.createdAt).getTime();
      if (isNaN(base)) return null;
      return new Date(base + job.intervalMs);
    }
    return null;
  }

  /**
   * Called by the host when an agent loop starts (agent_start event).
   * Tags the turn as scheduler-initiated if a send is currently in-flight.
   */
  notifyAgentStart(): void {
    this.agentRunning = true;
    // Fresh turn — drop any transcript recorded for a previous turn so a settle
    // that arrives without a corresponding agent_end can't confirm/capture using
    // stale messages.
    this.lastTurnMessages = [];
    // If we sent a message and it hasn't been confirmed yet, this turn belongs to us.
    this.currentTurnIsScheduled = this.sending;
    // The turn has started — no longer need the watchdog to rescue a stuck send.
    if (this.sendingWatchdog !== null) {
      clearTimeout(this.sendingWatchdog);
      this.sendingWatchdog = null;
    }
  }

  /**
   * Called by the host when an agent turn ends (agent_end event).
   *
   * agent_end fires once *per turn*, but pi keeps the session's run active across
   * the whole multi-turn loop (continuations, compaction, overflow recovery,
   * retries) and only reports idle at agent_settled. Advancing the scheduler here
   * would clear the gate and drain the next scheduled send while the session is
   * still streaming a continuation.
   *
   * So on modern runtimes (that expose isIdle and therefore also emit
   * agent_settled) we only *record* the turn's messages and let
   * notifyAgentSettled() advance the scheduler once the session is authoritatively
   * idle. On legacy runtimes without isIdle/agent_settled we fall back to the
   * original agent_end-driven behaviour, unchanged.
   */
  notifyAgentEnd(messages: readonly unknown[]): void {
    this.lastTurnMessages = messages;
    // Modern runtime: defer confirmation/drain to agent_settled. piReportsIdle()
    // returns a boolean only when the runtime exposes isIdle; undefined marks the
    // legacy generation that predates both isIdle and agent_settled.
    if (this.piReportsIdle() !== undefined) {
      return;
    }
    this.processTurnEnd(messages);
  }

  /**
   * Called by the host when an agent run has fully settled (agent_settled event):
   * no automatic retry, compaction, or queued continuation remains and
   * pi.isIdle() is authoritatively true. This is the correct point to confirm the
   * scheduled turn, clear the gate, and drain the next deferred send. Only wired
   * up / emitted on modern runtimes; legacy runtimes drive everything from
   * notifyAgentEnd instead.
   */
  notifyAgentSettled(): void {
    const messages = this.lastTurnMessages;
    this.lastTurnMessages = [];
    this.processTurnEnd(messages);
  }

  /**
   * Resolve the current retry or the oldest pending guaranteed once-job (only
   * when the turn was scheduler-initiated), capture output, clear the gate, and
   * drain the next deferred action. Shared by the legacy agent_end path and the
   * modern agent_settled path.
   */
  private processTurnEnd(messages: readonly unknown[]): void {
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
      // Non-guaranteed recurring job: capture output for /schedule-prompt replay
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
    const piDir = this.storage.getPiDir();
    const tempPath = `${this.leaderPidPath}.tmp`;
    const attempt = () => {
      fs.mkdirSync(piDir, { recursive: true });
      fs.writeFileSync(tempPath, String(pid), "utf-8");
      fs.renameSync(tempPath, this.leaderPidPath);
    };
    try {
      attempt();
    } catch (error: any) {
      // Recover from a transient missing-directory race (e.g. .pi/ removed
      // between writeFileSync and renameSync). One retry is enough.
      if (error?.code === "ENOENT") {
        try {
          attempt();
          return;
        } catch (retryError) {
          console.error("Failed to write leader.pid (after retry):", retryError);
          return;
        }
      }
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
        this.intervalAnchors.set(job.id, Date.now());
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

  /**
   * Reconcile in-memory timers against persisted storage.
   *
   * Called after a dedicated subprocess exits: that child runs in its own
   * pi process with its own scheduler instance, so when it calls
   * `schedule_prompt` to add/remove jobs the host scheduler sees nothing —
   * the disk file is updated but no timer is (un)registered here.
   *
   * Strategy: read storage, add timers for any enabled jobs missing one,
   * drop timers whose job vanished or was disabled. Schedule changes are
   * not detected (would require comparing fields); users can `remove + add`
   * if that becomes necessary.
   */
  private resyncTimersFromStorage(): void {
    const diskJobs = this.storage.getAllJobs();
    const diskIds = new Set(diskJobs.map((j) => j.id));

    for (const job of diskJobs) {
      // A retry timer (`this.retries`) means the scheduler is already
      // managing this job through the guaranteed-retry path; adding a
      // normal timer would double-fire after the retry interval.
      if (this.retries.has(job.id)) continue;
      const hasTimer = this.jobs.has(job.id) || this.intervals.has(job.id);
      if (job.enabled && !hasTimer) {
        this.scheduleJob(job);
        this.emitChange({ type: "add", job });
      } else if (!job.enabled && hasTimer) {
        this.unscheduleJob(job.id);
      }
    }

    const timerIds = new Set<string>([...this.jobs.keys(), ...this.intervals.keys()]);
    for (const id of timerIds) {
      if (!diskIds.has(id)) {
        this.unscheduleJob(id);
        this.emitChange({ type: "remove", jobId: id });
      }
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
    this.intervalAnchors.delete(id);

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

    // Drop any queued subprocess entries for this job; the drain loop would
    // otherwise re-read storage, find it gone, and skip — wasted churn.
    for (let i = subprocessQueue.length - 1; i >= 0; i--) {
      if (subprocessQueue[i].jobId === id) subprocessQueue.splice(i, 1);
    }

    // If we were retrying this job, unblock so other deferred actions can proceed.
    if (this.retryingJobId === id) {
      this.retrying = false;
      this.retryingJobId = null;
      this.processNextDeferred();
    }
  }

  // --- Execution ---

  /**
   * Fire a job immediately, regardless of its `enabled` flag.
   * Routes through the same dispatch as automatic firing (`executeJobIfLeader`),
   * so dedicated jobs run in a subprocess and main-session jobs go through the
   * agentRunning/sending gate. Used by the `/schedule-prompt retry` command.
   *
   * Returns the disposition so callers can show appropriate UI:
   *   - "fired"   — the job started executing now (or is in the dispatch path)
   *   - "queued"  — the job is currently in-flight (dedicated subprocess running);
   *                 a retry will fire automatically when the current run finishes
   *
   * @throws if no job with the given id exists in storage.
   */
  async runJobNow(jobId: string): Promise<"fired" | "queued"> {
    const job = this.storage.getJob(jobId);
    if (!job) throw new Error(`Job ${jobId} not found`);
    if ((job.dedicatedContext || job.standalone) && inFlightDedicated.has(jobId)) {
      // The current subprocess (which may have been started by a prior scheduler
      // instance, before /new) will see this flag in its completion path and
      // re-fire itself once it finishes. Avoids silently no-op'ing the user's
      // explicit retry while the job is busy.
      pendingDedicatedRetriesGlobal.add(jobId);
      return "queued";
    }
    if (job.command && inFlightCommand.has(jobId)) {
      // Same survival logic as dedicated jobs above — a command run that
      // outlived a session swap will re-fire itself on completion.
      pendingCommandRetriesGlobal.add(jobId);
      return "queued";
    }
    if (job.dedicatedContext || job.standalone) {
      return this.enqueueOrRunSubprocess("dedicated", job) === "fired" ? "fired" : "queued";
    }
    if (job.command) {
      return this.enqueueOrRunSubprocess("command", job) === "fired" ? "fired" : "queued";
    }
    await this.executeJobIfLeader(job);
    return "fired";
  }

  /**
   * Add a dedicated/command job to the host-wide subprocess queue, or run it
   * immediately if the slot is free.
   *
   * Returns:
   *   - "fired"  — slot was free; the executor was invoked synchronously
   *   - "queued" — slot is busy or this job is already queued; will run later
   *   - "skipped" — this exact job is already in flight (cross-instance dedup)
   *
   * Same-job dedup matters because a stray duplicate enqueue (e.g. start()
   * re-firing a missed job that's already in the queue) would otherwise leave
   * two queue entries that both fire and the second would no-op on the
   * `inFlightDedicated.has(...)` guard inside the executor anyway. Cleaner to
   * dedup at enqueue time.
   */
  private enqueueOrRunSubprocess(kind: SubprocessKind, job: CronJob): "fired" | "queued" | "skipped" {
    // Claim active-scheduler slot if no one has yet (e.g. tests that invoke
    // dispatchers without calling start()). Production always sets this in
    // start(); this fallback just makes the implicit lifecycle work.
    if (activeScheduler === null) activeScheduler = this;

    const inFlight = kind === "dedicated" ? inFlightDedicated : inFlightCommand;
    if (inFlight.has(job.id)) return "skipped";
    if (subprocessQueue.some((e) => e.jobId === job.id)) return "queued";

    if (isSubprocessSlotBusy()) {
      subprocessQueue.push({ kind, jobId: job.id });
      this.emitChange({ type: "fire", job });
      return "queued";
    }
    if (kind === "dedicated") {
      void this.executeDedicatedJob(job);
    } else {
      void this.executeCommandJob(job);
    }
    return "fired";
  }

  /**
   * Pop and dispatch the next queued subprocess job, if the slot is free.
   * Loops until the slot becomes busy (the dispatched job sets inFlight
   * synchronously before its first await) or the queue empties.
   *
   * Routes dispatch through `activeScheduler` rather than `this`: when the
   * old scheduler's subprocess completes after a /new, its `this.pi` is
   * stale — the new scheduler is the one that should fire the next job.
   */
  private drainSubprocessQueue(): void {
    while (!isSubprocessSlotBusy()) {
      const sched = activeScheduler;
      if (!sched) return;
      const next = subprocessQueue.shift();
      if (!next) return;
      const fresh = sched.storage.getJob(next.jobId);
      if (!fresh || !fresh.enabled) continue;
      if (next.kind === "dedicated") {
        void sched.executeDedicatedJob(fresh);
      } else {
        void sched.executeCommandJob(fresh);
      }
    }
  }

  /**
   * Read pi's authoritative idle state, if the runtime exposes it.
   * Returns undefined on older runtimes that lack `isIdle` (or if the call
   * throws) — callers treat undefined as "unknown" and fall back to the
   * event-mirrored flags, preserving legacy behaviour.
   */
  private piReportsIdle(): boolean | undefined {
    const fn = (this.pi as { isIdle?: () => boolean }).isIdle;
    if (typeof fn !== "function") return undefined;
    try {
      return fn.call(this.pi);
    } catch {
      return undefined;
    }
  }

  /**
   * The agentRunning/sending/retrying flags mirror pi's turn state via
   * agent_start/agent_end. In RPC/multi-context mode those events can be missed
   * (aborted/overflow-recovery turns, turns on sibling context sessions),
   * leaving the mirror stuck "busy" so every scheduled fire defers forever and
   * only drains one-per-real-agent_end.
   *
   * When pi authoritatively reports idle AND we're past the post-send grace
   * window (so a just-issued send whose turn hasn't begun streaming isn't
   * mistaken for stuck), treat the mirror as stale and clear it. Returns true if
   * it cleared a stuck gate.
   */
  private clearGateIfStale(): boolean {
    if (!(this.agentRunning || this.sending || this.retrying)) return false;
    if (this.piReportsIdle() !== true) return false;
    if (Date.now() - this.lastSendAt < this.GATE_RECOVERY_GRACE_MS) return false;

    const stuckRetryJobId = this.retrying ? this.retryingJobId : null;
    this.sending = false;
    this.agentRunning = false;
    this.retrying = false;
    this.retryingJobId = null;
    this.currentTurnIsScheduled = true;
    if (this.sendingWatchdog !== null) {
      clearTimeout(this.sendingWatchdog);
      this.sendingWatchdog = null;
    }
    console.warn(
      "[scheduler] Cleared stale busy gate (pi reports idle, no agent_end received). " +
        "Resuming deferred scheduled prompts."
    );
    // A retry that was in flight when the gate stuck never received its
    // agent_end; reschedule it rather than silently dropping it.
    if (stuckRetryJobId) {
      const job = this.storage.getJob(stuckRetryJobId);
      if (job && job.enabled && job.guaranteed) {
        this.scheduleRetryTimer(stuckRetryJobId);
      }
    }
    return true;
  }

  /**
   * Watchdog backstop: clear a stale gate if present, then — if the gate is free
   * and deferred sends are waiting — drain one. Covers jobs that were deferred
   * while pi was genuinely busy and then went idle without an agent_end to
   * advance the deferred queue.
   */
  private recoverDeferredIfIdle(): void {
    if (this.stopped) return;
    const cleared = this.clearGateIfStale();
    const gateFree = !(this.agentRunning || this.sending || this.retrying);
    if ((cleared || gateFree) && this.deferredActions.length > 0) {
      this.processNextDeferred();
    }
  }

  private async executeJobIfLeader(job: CronJob): Promise<void> {
    if (job.command) {
      // Shell command — runs in a child process, never invokes the agent.
      // Routed through the host-wide subprocess queue so it doesn't run in
      // parallel with another dedicated or command subprocess.
      this.enqueueOrRunSubprocess("command", job);
      return;
    }
    if (job.dedicatedContext || job.standalone) {
      // Runs in an isolated subprocess — does not block the main agent, but
      // is host-serialized against other dedicated/command runs to keep API
      // load and shared-file contention under control. `standalone` jobs use
      // the same path; they additionally capture the session file for replay/
      // enter and surface a persistent report indicator on completion.
      this.enqueueOrRunSubprocess("dedicated", job);
      return;
    }
    // Authoritative recovery: if the mirror says "busy" but pi reports idle past
    // the grace window, the agent_start/end events were missed — clear the stale
    // flags so this fire proceeds on time instead of deferring forever.
    this.clearGateIfStale();
    // Defer if our event-mirrored flags say busy, OR if pi authoritatively reports
    // it is streaming. The mirror flags (agentRunning/sending/retrying) can diverge
    // from pi's real isStreaming — agent_end is suppressed on overflow recovery,
    // events are missed in RPC/multi-context turns, or arrive out of order — and
    // when the mirror reads "idle" mid-task the send lands as a steering message
    // that hijacks the user's turn (or is silently dropped by the fire-and-forget
    // ExtensionAPI wrapper). pi.isIdle() (=!isStreaming) is the authoritative signal
    // and stays true for the whole multi-tool task. piReportsIdle() returns
    // undefined on runtimes without isIdle, so legacy behaviour is preserved.
    if (this.agentRunning || this.retrying || this.sending || this.piReportsIdle() === false) {
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
    // Anchor the gate-recovery grace window at claim time, BEFORE awaiting
    // leadership. Otherwise, on a runtime that reports idle, a second job firing
    // during the acquireLeadership() await runs clearGateIfStale(), sees isIdle
    // with a stale (0) lastSendAt past the grace window, and wrongly clears this
    // just-claimed gate — so both jobs send and collide. executeJob refreshes this
    // once the send actually goes out.
    this.lastSendAt = Date.now();
    const isLeader = await this.acquireLeadership();
    if (!isLeader) {
      this.sending = false;
      this.processNextDeferred();
      return;
    }
    await this.executeJob(job);
  }

  private async executeJob(job: CronJob): Promise<void> {
    // Final guard after leadership wait: if the agent started (or pi reports it is
    // streaming) while we waited, defer instead of racing against isStreaming and
    // producing an error. The piReportsIdle() re-check here closes the window
    // between the gate check in executeJobIfLeader and this actual send.
    if (this.agentRunning || this.piReportsIdle() === false) {
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

      // deliverAs "followUp" is a safety net for the residual race the gate can't
      // fully close: pi's own prompt() re-checks isStreaming between our
      // isIdle()-based gate and the point it starts the turn (a TOCTOU window), and
      // agent_start/end can be missed in RPC/multi-context mode. When pi finds
      // itself streaming at that check, "followUp" makes it QUEUE the prompt and run
      // it once the current turn winds down — instead of throwing the fire-and-forget
      // "Agent is already processing" error and silently dropping the message. When
      // pi is idle (the normal case) deliverAs is ignored and this starts a fresh turn.
      if (useContext) {
        piAny.sendMessageToContext(job.targetContext, displayMessage);
        piAny.sendUserMessageToContext(job.targetContext, wrappedPrompt, { deliverAs: "followUp" });
      } else {
        this.pi.sendMessage(displayMessage);
        this.pi.sendUserMessage(wrappedPrompt, { deliverAs: "followUp" });
      }
      // Both send calls succeeded — an agent turn is now in flight (or queued as a
      // follow-up). sending will be cleared by notifyAgentSettled/notifyAgentEnd.
      sendCompleted = true;
      this.lastSendAt = Date.now();

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
    // soon as the first's inFlightDedicated guard clears. Net effect: the user
    // sees back-to-back duplicate runs of the same scheduled prompt.
    const existing = this.retries.get(jobId);
    if (existing) {
      clearTimeout(existing);
    }
    const retry = setTimeout(() => {
      this.retries.delete(jobId);
      // Defer if the event-mirror flags say busy OR pi authoritatively reports it
      // is streaming. Matching the send-path gate (executeJobIfLeader): the mirror
      // flags can be stale-clear when agent_start/end are missed in
      // RPC/multi-context mode, so without the piReportsIdle() check a retry would
      // fire retryLastTurn()/sendUserMessage() into a busy agent.
      if (this.agentRunning || this.retrying || this.sending || this.piReportsIdle() === false) {
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

    if (current.command) {
      this.enqueueOrRunSubprocess("command", current);
      return;
    }
    if (current.dedicatedContext) {
      this.enqueueOrRunSubprocess("dedicated", current);
      return;
    }

    // Final gate before a main-session retry: if the agent is active (mirror flags)
    // or pi authoritatively reports it is streaming, re-defer rather than firing
    // retryLastTurn()/sendUserMessage() into a busy agent. Closes the window
    // between the drain decision and this actual send. (Subprocess retries above
    // are host-serialized separately and intentionally bypass this.)
    if (this.agentRunning || this.sending || this.retrying || this.piReportsIdle() === false) {
      if (!this.deferredActions.some((a) => a.type === "retry" && a.jobId === jobId)) {
        this.deferredActions.push({ type: "retry", jobId });
      }
      return;
    }

    this.retrying = true;
    this.retryingJobId = jobId;
    this.currentTurnJobId = jobId;
    this.currentTurnStartTime = new Date().toISOString();
    this.lastSendAt = Date.now();

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
          `This is an automated scheduled prompt. Interpret and execute the following directly — phrases like "remind me" mean perform the action now, not schedule another reminder:\n\n${current.prompt}`,
          { deliverAs: "followUp" }
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
    // Cross-instance dedup: if a prior scheduler already has this job in
    // flight (e.g. survived a /new), don't fire a duplicate subprocess.
    if (inFlightDedicated.has(job.id)) return;

    const startTime = new Date().toISOString();
    const controller = new AbortController();
    inFlightDedicated.set(job.id, {
      controller,
      jobId: job.id,
      jobName: job.name,
      prompt: job.prompt,
      startTime,
    });

    const piAny = this.pi as any;
    const notifyToContext =
      job.targetContext &&
      typeof piAny.sendMessageToContext === "function";

    // Dedicated-job begin/end notifications must NEVER feed the running agent
    // the dedicated job's prompt — otherwise the host agent picks it up and
    // re-runs the same workflow, defeating the entire point of dedicated context.
    // That risk is structurally mitigated: the prompt is kept out of
    // `content[0].text` and exposed only via `details.prompt` (the renderer
    // reads from details). With that invariant in place, sending opts={} is
    // safe even when the agent is streaming — at worst `agent.steer()` injects
    // a short status notification into the active turn.
    //
    // We previously gated this on `this.agentRunning` and used `deliverAs:
    // "nextTurn"` while a turn was in flight. That was an over-rotation: the
    // `nextTurn` branch in pi-mono's sendCustomMessage pushes into
    // `_pendingNextTurnMessages` and emits nothing until the next user prompt
    // drains the queue. `this.agentRunning` can be sticky-true (overflow
    // recovery suppresses `agent_end`; a stale scheduler binding never sees
    // its session's agent_start/end). When stuck, every notification was
    // silently parked — so e.g. command-mode reminders never reached the
    // Signal bridge despite the bash command running successfully. Silent
    // parking is strictly worse than incidental steering for status messages.
    const notify = (customType: string, text: string, details: Record<string, unknown>) => {
      // If the scheduler was stopped (e.g. by session_shutdown during /new) while
      // a dedicated subprocess was past `await pi.exec(...)`, the captured `pi`
      // has been invalidated by pi-mono and sendMessage would throw a stale-ctx
      // error. The new session shouldn't receive notifications about the prior
      // session's runs anyway, so drop them.
      if (this.stopped) return;
      const msg = {
        customType,
        content: [{ type: "text" as const, text }],
        display: true,
        details,
      };
      const target = notifyToContext ? job.targetContext : "<bound-session>";
      console.log(
        `[scheduler] notify emit customType=${customType} jobId=${job.id} target=${target}`
      );
      try {
        if (notifyToContext) {
          piAny.sendMessageToContext(job.targetContext, msg);
        } else {
          this.pi.sendMessage(msg);
        }
      } catch (err) {
        // Defense in depth: if the ctx went stale between the `stopped` check
        // and the send (or pi-mono invalidates without firing session_shutdown
        // first), don't propagate — it would surface as an uncaught exception
        // from the cron tick.
        const message = err instanceof Error ? err.message : String(err);
        if (/stale after session replacement|extension ctx is stale/i.test(message)) {
          console.warn(
            `[scheduler] notify dropped (stale ctx) customType=${customType} jobId=${job.id}`
          );
          return;
        }
        console.warn(
          `[scheduler] notify threw customType=${customType} jobId=${job.id}: ${message}`
        );
        throw err;
      }
    };

    notify(
      "scheduled_prompt_begin",
      `[Scheduled Prompt] dedicated subprocess begin: ${job.name}`,
      { jobId: job.id, jobName: job.name, prompt: job.prompt, startTime: startTime }
    );

    this.storage.updateJob(job.id, { lastStatus: "running" });
    this.emitChange({ type: "fire", job });

    const wrappedPrompt =
      `This is an automated scheduled prompt. Interpret and execute the following directly — ` +
      `phrases like "remind me" mean perform the action now, not schedule another reminder:\n\n${job.prompt}`;

    let status: "success" | "error" = "success";
    let output = "";
    let sessionFilePath: string | undefined;
    const startMs = Date.now();

    try {
      const result = await this.pi.exec(
        "pi",
        [
          "--mode", "json",
          "-p",
          "--session-dir", DEDICATED_SESSION_DIR,
          wrappedPrompt,
        ],
        { signal: controller.signal, timeout: DEDICATED_JOB_TIMEOUT_MS }
      );
      const durationS = Math.round((Date.now() - startMs) / 1000);
      const formatted = formatDedicatedRunOutput(result.stdout, result.stderr, result.code, result.killed, durationS);
      output = formatted.output;
      // Capture the subprocess session file so a `standalone` run can be entered
      // for follow-ups later. Cheap and harmless for plain dedicated runs too.
      sessionFilePath = resolveDedicatedSessionFile(formatted.sessionId) ?? undefined;
      // A successful pi --mode json run always emits an agent_end event with at least one
      // assistant message containing text. If we didn't see that, treat as failure even
      // when the exit code is 0 (timeout-killed processes resolve with code 0 sometimes).
      status = result.code === 0 && !result.killed && formatted.hasAgentEnd ? "success" : "error";
    } catch (error) {
      if ((error as Error & { name?: string }).name === "AbortError") {
        // Aborted (host quit or stale-slot watchdog eviction) — exit without
        // updating storage, but still free + drain the slot so a queued job
        // can dispatch instead of stalling behind this aborted run.
        inFlightDedicated.delete(job.id);
        this.drainSubprocessQueue();
        return;
      }
      const durationS = Math.round((Date.now() - startMs) / 1000);
      const errMsg = error instanceof Error ? error.message : String(error);
      output = `[exit=? killed=? duration=${durationS}s]\n[error]\n${errMsg}`;
      status = "error";
    } finally {
      inFlightDedicated.delete(job.id);
    }

    this.captureRunRecordFromOutput(job, output, status, startTime, {
      standalone: job.standalone ?? false,
      sessionFilePath,
    });

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

    // The dedicated child may have added/removed jobs via schedule_prompt;
    // those writes hit storage but not this scheduler. Reconcile timers now
    // so any new jobs actually fire and any removed ones stop holding state.
    this.resyncTimersFromStorage();

    const hint = status === "error" ? detectFailureHint(output) : null;
    const endText = hint
      ? `[Scheduled Prompt] Processing ended (failed). See /schedule-prompt replay ${job.id} to review.\n⚠️ Likely cause: ${hint}`
      : `[Scheduled Prompt] Processing ended. See /schedule-prompt replay ${job.id} to review.`;
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

    // If the user (or a parallel code path) requested a manual retry while this
    // run was in flight, fire it now. The retry intent is module-scoped so it
    // survives a session swap that happened while the run was hanging. Prefer
    // the latest version from storage (in case the prompt/schedule was edited
    // mid-run); fall back to the in-memory snapshot, since once-jobs are
    // removed from storage on success.
    if (pendingDedicatedRetriesGlobal.has(job.id)) {
      pendingDedicatedRetriesGlobal.delete(job.id);
      const fresh = this.storage.getJob(job.id) ?? job;
      void this.executeDedicatedJob(fresh);
      return;
    }

    // Slot is free — drain the next queued job (if any). This is what keeps
    // jobs whose schedules collided from running in parallel: rather than
    // both spawning subprocesses simultaneously, the second one waits in the
    // queue and starts here.
    this.drainSubprocessQueue();
  }

  /**
   * Execute a command-mode job: run `bash -c <prompt>` in a child process,
   * notify start/end with captured stdout/stderr, and record the run for replay.
   * Does NOT route through the agent — command jobs are pure side-effects
   * (reminders via `echo`, external sends, script runs, etc.) and never block
   * or interleave with main-session gating (`sending`/`agentRunning`/`retrying`).
   */
  private async executeCommandJob(job: CronJob): Promise<void> {
    // Cross-instance dedup — same reasoning as executeDedicatedJob.
    if (inFlightCommand.has(job.id)) return;

    const startTime = new Date().toISOString();
    const controller = new AbortController();
    inFlightCommand.set(job.id, {
      controller,
      jobId: job.id,
      jobName: job.name,
      prompt: job.prompt,
      startTime,
    });

    const piAny = this.pi as any;
    const notifyToContext =
      job.targetContext &&
      typeof piAny.sendMessageToContext === "function";

    // command_end carries the captured stdout/stderr of the bash run and is
    // the ONLY way the user (or the Signal bridge) ever learns the command
    // produced output. Emit immediately with no opts.
    //
    // We must NOT pass `deliverAs: "nextTurn"`, even when an agent turn is in
    // flight: that branch in pi-mono's sendCustomMessage pushes into
    // `_pendingNextTurnMessages` and emits nothing until the next user prompt
    // drains the queue. Command-mode jobs run independently of any agent
    // turn — if no one prompts the agent before the next command fires, the
    // queued notification sits invisibly forever and the user never sees
    // their reminder. Confirmed in prod: `agentRunning` can be sticky-true
    // (overflow recovery suppresses `agent_end`; a stale scheduler binding
    // never receives its session's agent_start/end), and once stuck every
    // subsequent command_end was silently parked. Silent parking is strictly
    // worse than incidental steering for status-only messages.
    const notify = (customType: string, text: string, details: Record<string, unknown>) => {
      if (this.stopped) return;
      const msg = {
        customType,
        content: [{ type: "text" as const, text }],
        display: true,
        details,
      };
      const target = notifyToContext ? job.targetContext : "<bound-session>";
      console.log(
        `[scheduler] notify emit customType=${customType} jobId=${job.id} target=${target}`
      );
      try {
        if (notifyToContext) {
          piAny.sendMessageToContext(job.targetContext, msg);
        } else {
          this.pi.sendMessage(msg);
        }
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        if (/stale after session replacement|extension ctx is stale/i.test(message)) {
          console.warn(
            `[scheduler] notify dropped (stale ctx) customType=${customType} jobId=${job.id}`
          );
          return;
        }
        console.warn(
          `[scheduler] notify threw customType=${customType} jobId=${job.id}: ${message}`
        );
        throw err;
      }
    };

    // No begin notification for command jobs — they're typically short and
    // the running state is already visible in the widget (status: running)
    // and /schedule-prompt ps. A separate begin message just adds noise.
    this.storage.updateJob(job.id, { lastStatus: "running" });
    this.emitChange({ type: "fire", job });

    let status: "success" | "error" = "success";
    let output = "";
    let stdout = "";
    let stderr = "";
    let exitCode: number | null = null;
    let killed = false;
    const startMs = Date.now();

    try {
      const result = await this.pi.exec(
        "bash",
        ["-c", job.prompt],
        { signal: controller.signal, timeout: DEDICATED_JOB_TIMEOUT_MS }
      );
      const durationS = Math.round((Date.now() - startMs) / 1000);
      stdout = result.stdout;
      stderr = result.stderr;
      exitCode = result.code;
      killed = result.killed;
      output = formatCommandRunOutput(stdout, stderr, exitCode, killed, durationS);
      status = exitCode === 0 && !killed ? "success" : "error";
    } catch (error) {
      if ((error as Error & { name?: string }).name === "AbortError") {
        // Aborted (host quit or stale-slot watchdog eviction) — exit without
        // updating storage, but still free + drain the slot so a queued job
        // can dispatch instead of stalling behind this aborted run.
        inFlightCommand.delete(job.id);
        this.drainSubprocessQueue();
        return;
      }
      const durationS = Math.round((Date.now() - startMs) / 1000);
      const errMsg = error instanceof Error ? error.message : String(error);
      stderr = errMsg;
      output = `[exit=? killed=? duration=${durationS}s]\n[error]\n${errMsg}`;
      status = "error";
    } finally {
      inFlightCommand.delete(job.id);
    }

    this.captureRunRecordFromOutput(job, output, status, startTime);

    if (job.type === "once") {
      if (status === "success" || !job.guaranteed) {
        this.storage.removeJob(job.id);
        this.emitChange({ type: "remove", jobId: job.id });
      } else {
        this.storage.updateJob(job.id, { lastStatus: "error" });
        this.emitChange({ type: "error", jobId: job.id, error: "Command failed (retrying in 10m)" });
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
      if (status === "error" && job.guaranteed) {
        this.scheduleRetryTimer(job.id);
      }
    }

    // Silent-success: if the command exited 0 with no stdout or stderr,
    // skip the notification entirely. Frequent housekeeping jobs ("nothing
    // to do") shouldn't spam the session. Run history still records the run,
    // and /schedule-prompt replay can surface it on demand.
    const trimmedStdout = stdout.replace(/\s+$/, "");
    const trimmedStderr = stderr.replace(/\s+$/, "");
    const shouldNotify = status === "error" || trimmedStdout !== "" || trimmedStderr !== "";
    if (shouldNotify) {
      // Body is a fallback for hosts that don't render via the registered
      // `scheduled_prompt_command_end` renderer. The renderer reads the
      // structured fields in `details` for a streamlined display.
      const endTime = new Date().toISOString();
      notify(
        "scheduled_prompt_command_end",
        output,
        {
          jobId: job.id,
          jobName: job.name,
          status,
          exitCode,
          killed,
          stdout: trimmedStdout,
          stderr: trimmedStderr,
          startTime,
          endTime,
        }
      );
    }

    if (pendingCommandRetriesGlobal.has(job.id)) {
      pendingCommandRetriesGlobal.delete(job.id);
      const fresh = this.storage.getJob(job.id) ?? job;
      void this.executeCommandJob(fresh);
      return;
    }

    // Slot is free — drain the next queued job. See executeDedicatedJob's
    // matching call for the rationale.
    this.drainSubprocessQueue();
  }

  private captureRunRecordFromOutput(
    job: CronJob,
    output: string,
    status: "success" | "error",
    startTime: string | null,
    extra?: { standalone?: boolean; sessionFilePath?: string }
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
      standalone: extra?.standalone ?? false,
      sessionFilePath: extra?.sessionFilePath,
    });
  }

  private captureRunRecord(
    job: CronJob,
    messages: readonly unknown[],
    status: "success" | "error",
    startTime: string | null
  ): void {
    const endTime = new Date().toISOString();
    const record: Omit<RunRecord, "id"> = {
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
/**
 * Format command-mode run output for notifications and run history.
 * Plain bash stdout/stderr — no JSON parsing or message reconstruction.
 */
export function formatCommandRunOutput(
  stdout: string,
  stderr: string,
  code: number,
  killed: boolean,
  durationS: number
): string {
  const header = `[exit=${code} killed=${killed} duration=${durationS}s]`;
  const sections: string[] = [header];
  const out = stdout.replace(/\s+$/, "");
  const err = stderr.replace(/\s+$/, "");
  if (out) sections.push(`[stdout]\n${out}`);
  if (err) sections.push(`[stderr]\n${err}`);
  if (!out && !err) sections.push("(no output)");
  return sections.join("\n");
}

export function formatDedicatedRunOutput(
  stdout: string,
  stderr: string,
  code: number,
  killed: boolean,
  durationS: number
): { output: string; hasAgentEnd: boolean; sessionId: string | null } {
  const header = `[exit=${code} killed=${killed} duration=${durationS}s]`;
  const events: Array<Record<string, unknown>> = [];
  let hasAgentEnd = false;
  // The first JSON line in `pi --mode json` is the session header
  // ({ type: "session", id, ... }). Capture its id so the caller can locate
  // the on-disk session file (`<session-dir>/*_${id}.jsonl`) for enter/replay.
  let sessionId: string | null = null;

  for (const line of stdout.split("\n")) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    try {
      const event = JSON.parse(trimmed) as Record<string, unknown>;
      events.push(event);
      if (event["type"] === "agent_end") hasAgentEnd = true;
      if (sessionId === null && event["type"] === "session" && typeof event["id"] === "string") {
        sessionId = event["id"] as string;
      }
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
  // separately. A single timed-out run with 5000+ thinking deltas produced 27 MB
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
  return { output, hasAgentEnd, sessionId };
}

/**
 * Locate the on-disk session JSONL written by a dedicated subprocess, given the
 * session id parsed from its JSON output. Session files are named
 * `${fileTimestamp}_${id}.jsonl`, so we scan the dedicated session dir for the
 * single entry ending `_${id}.jsonl`. Returns the absolute path, or null if the
 * id is missing or no matching file exists. Best-effort: any fs error → null.
 */
function resolveDedicatedSessionFile(sessionId: string | null): string | null {
  if (!sessionId) return null;
  try {
    const suffix = `_${sessionId}.jsonl`;
    const match = fs.readdirSync(DEDICATED_SESSION_DIR).find((name) => name.endsWith(suffix));
    return match ? path.join(DEDICATED_SESSION_DIR, match) : null;
  } catch {
    return null;
  }
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
    return `subprocess hit the ${DEDICATED_JOB_TIMEOUT_MIN}-minute timeout without finishing. Either the prompt is too large for one turn, or the agent got stuck in a loop.`;
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

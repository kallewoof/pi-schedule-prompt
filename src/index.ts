/**
 * pi-schedule-prompt — A pi extension for scheduling agent prompts
 *
 * Provides:
 * - A `schedule_prompt` tool for managing scheduled prompts
 * - A widget displaying all scheduled prompts with status
 * - /schedule-prompt command for interactive management
 * - Ctrl+Alt+P shortcut to toggle widget
 * - Persistence via .pi/schedule-prompts.json
 */

import type { ExtensionAPI } from "@mariozechner/pi-coding-agent";
import type { CronJob, RunRecord, SessionShutdownReason } from "./types.js";
import { Key } from "@mariozechner/pi-tui";
import { Container, Text } from "@mariozechner/pi-tui";
import { CronStorage } from "./storage.js";
import { CronScheduler, getDedicatedActivity } from "./scheduler.js";
import { createCronTool } from "./tool.js";
import { CronWidget } from "./ui/cron-widget.js";
import { formatLocalDateTime, formatRelativeHint, formatSchedule, sortJobsByNextRun } from "./utils.js";
import { nanoid } from "nanoid";

/**
 * Returns the disabled jobs that should be removed on session shutdown.
 * Only one-shot jobs are eligible — recurring (cron/interval) jobs are kept
 * even when disabled, since users often disable them temporarily while
 * investigating failures and silent deletion would destroy that work.
 */
export function pickJobsToAutoCleanup(jobs: ReadonlyArray<CronJob>): CronJob[] {
  return jobs.filter((j) => !j.enabled && j.type === "once");
}

/**
 * Tear down per-session state on `session_shutdown`. The `reason` is forwarded
 * to `scheduler.stop` so it can preserve in-flight dedicated subprocesses
 * across session-replacement events (`/new`, `/fork`, `/resume`, `/reload`)
 * and only abort them when the host process is actually exiting (`"quit"`).
 *
 * Exported for direct unit-testing; the production caller is the closure-
 * scoped handler inside the default export.
 */
export function cleanupSession(
  scheduler: { stop: (opts?: { reason?: SessionShutdownReason }) => void } | undefined,
  widget: { hide: (ctx: any) => void; destroy: () => void } | undefined,
  ctx: any,
  reason: SessionShutdownReason | undefined
): void {
  if (scheduler) scheduler.stop({ reason });
  if (widget) {
    widget.hide(ctx);
    widget.destroy();
  }
}

/**
 * Resolve the target job id for `/schedule-prompt retry [arg]`.
 *
 * Resolution order:
 * 1. Empty arg → most recent run record's jobId.
 * 2. Numeric arg → N-from-end run record's jobId.
 * 3. Non-numeric arg → first match an existing job's `id` (so "retry <jobId>"
 *    works even with no run history); then fall back to substring match in
 *    run history on jobId / jobName / jobPrompt.
 *
 * Returns the resolved jobId, or `null` if nothing matches. The caller looks
 * the job up in storage and reports if it no longer exists.
 */
export function resolveRetryTarget(
  arg: string,
  jobs: ReadonlyArray<CronJob>,
  history: ReadonlyArray<RunRecord>
): string | null {
  if (!arg) {
    return history.length === 0 ? null : history[history.length - 1].jobId;
  }
  if (/^\d+$/.test(arg)) {
    const n = parseInt(arg, 10);
    const idx = history.length - n;
    return idx >= 0 && idx < history.length ? history[idx].jobId : null;
  }
  // 1) Exact current-job id match — lets the user retry by id even with no history.
  const directJob = jobs.find((j) => j.id === arg);
  if (directJob) return directJob.id;
  // 2) Exact run-record jobId match.
  for (let i = history.length - 1; i >= 0; i--) {
    if (history[i].jobId === arg) return history[i].jobId;
  }
  // 3) Substring on history jobName / jobPrompt.
  const lower = arg.toLowerCase();
  for (let i = history.length - 1; i >= 0; i--) {
    const r = history[i];
    if (r.jobName.toLowerCase().includes(lower) || r.jobPrompt.toLowerCase().includes(lower)) {
      return r.jobId;
    }
  }
  // 4) Substring on current job names — useful for jobs that have never run yet.
  const byName = jobs.find((j) => j.name.toLowerCase().includes(lower));
  return byName ? byName.id : null;
}

/**
 * Resolve a `/schedule-prompt replay [arg]` argument to a stored RunRecord.
 * Mirrors `resolveRetryTarget` semantics so the same arg syntax works in both
 * subcommands. Returns `null` when nothing matches.
 */
export function resolveReplayTarget(
  arg: string,
  history: ReadonlyArray<RunRecord>
): RunRecord | null {
  if (history.length === 0) return null;
  if (!arg) return history[history.length - 1];
  if (/^\d+$/.test(arg)) {
    const n = parseInt(arg, 10);
    const idx = history.length - n;
    return idx >= 0 && idx < history.length ? history[idx] : null;
  }
  // Exact run-record jobId match first.
  for (let i = history.length - 1; i >= 0; i--) {
    if (history[i].jobId === arg) return history[i];
  }
  // Substring match on jobName / jobPrompt (most recent wins).
  const lower = arg.toLowerCase();
  for (let i = history.length - 1; i >= 0; i--) {
    const r = history[i];
    if (r.jobName.toLowerCase().includes(lower) || r.jobPrompt.toLowerCase().includes(lower)) {
      return r;
    }
  }
  return null;
}

/** Format a RunRecord for display in `/schedule-prompt replay`. */
export function formatReplayRecord(record: RunRecord): string {
  const start = new Date(record.startTime).toLocaleString();
  const end = new Date(record.endTime).toLocaleString();
  return [
    `Job:      ${record.jobName}`,
    `Schedule: ${record.schedule}`,
    `Prompt:   ${record.jobPrompt}`,
    `Type:     ${record.jobType}   Status: ${record.status}`,
    "",
    "─── Output ──────────────────────────────────────────────────────",
    record.output || "(no output captured)",
    "─────────────────────────────────────────────────────────────────",
    "",
    `Started: ${start}`,
    `Ended:   ${end}`,
  ].join("\n");
}

async function handleReplay(
  arg: string,
  ctx: any,
  storage: CronStorage
): Promise<void> {
  const history = storage.getRunHistory();
  if (history.length === 0) {
    ctx.ui.notify("No run history available yet — runs are recorded as jobs complete.", "info");
    return;
  }
  const record = resolveReplayTarget(arg, history);
  if (!record) {
    ctx.ui.notify("No matching run found.", "error");
    return;
  }
  ctx.ui.notify(formatReplayRecord(record), "info");
}

/**
 * Format the current dedicated-prompt activity for `/schedule-prompt ps`.
 * Pure helper to keep the command body trivial and testable.
 */
export function formatPsOutput(
  activity: { inFlight: ReadonlyArray<{ jobId: string; jobName: string; prompt: string; startTime: string }>; queuedRetries: ReadonlyArray<string> },
  now: Date = new Date()
): string {
  const { inFlight, queuedRetries } = activity;
  if (inFlight.length === 0 && queuedRetries.length === 0) {
    return "No dedicated prompts running and no retries queued.";
  }
  const lines: string[] = [];
  if (inFlight.length > 0) {
    lines.push(`Running dedicated prompts (${inFlight.length}):`);
    lines.push("");
    for (const e of inFlight) {
      const startedAt = new Date(e.startTime);
      const elapsedS = Math.max(0, Math.round((now.getTime() - startedAt.getTime()) / 1000));
      const elapsed =
        elapsedS < 60
          ? `${elapsedS}s`
          : elapsedS < 3600
          ? `${Math.floor(elapsedS / 60)}m ${elapsedS % 60}s`
          : `${Math.floor(elapsedS / 3600)}h ${Math.floor((elapsedS % 3600) / 60)}m`;
      const promptPreview = e.prompt.length > 80 ? e.prompt.slice(0, 80) + "…" : e.prompt;
      lines.push(`  ▸ ${e.jobName} (${e.jobId})`);
      lines.push(`    Started: ${formatLocalDateTime(startedAt)}  •  Elapsed: ${elapsed}`);
      lines.push(`    Prompt:  ${promptPreview}`);
      lines.push("");
    }
  }
  if (queuedRetries.length > 0) {
    lines.push(`Queued retries (will fire when the running run finishes): ${queuedRetries.join(", ")}`);
  }
  return lines.join("\n").trimEnd();
}

async function handleRetry(
  arg: string,
  ctx: any,
  storage: CronStorage,
  scheduler: CronScheduler
): Promise<void> {
  const jobs = storage.getAllJobs();
  const history = storage.getRunHistory();
  const targetId = resolveRetryTarget(arg, jobs, history);
  if (!targetId) {
    ctx.ui.notify(
      arg
        ? `No job or run matches "${arg}".`
        : "No run history yet — nothing to retry.",
      "error"
    );
    return;
  }
  const job = storage.getJob(targetId);
  if (!job) {
    ctx.ui.notify(`Job ${targetId} no longer exists. Cannot retry.`, "error");
    return;
  }
  // Show the resolved dispatch up front so it's visible in chat history. If
  // the user reports "ran in main session despite dedicatedContext", we can
  // see what the scheduler actually saw at retry time.
  const ctxLabel = job.dedicatedContext ? "dedicated subprocess" : "main session";
  // Fire-and-forget: scheduled_prompt_begin / _end notifications carry the result.
  void scheduler
    .runJobNow(job.id)
    .then((disposition) => {
      if (disposition === "queued") {
        ctx.ui.notify(
          `${job.name} (${job.id}, ${ctxLabel}) is currently running; retry queued — it will fire when the current run finishes.`,
          "info"
        );
      } else {
        ctx.ui.notify(`Retrying ${job.name} (${job.id}, ${ctxLabel})…`, "info");
      }
    })
    .catch((err) => {
      ctx.ui.notify(
        `Retry failed to start: ${err instanceof Error ? err.message : String(err)}`,
        "error"
      );
    });
}

export default async function (pi: ExtensionAPI) {
  let storage: CronStorage;
  let scheduler: CronScheduler;
  let widget: CronWidget;
  let widgetVisible = true;
  let storageForVisibility: CronStorage | undefined;

  // Register custom message renderer for scheduled prompts
  pi.registerMessageRenderer("scheduled_prompt", (message, _options, theme) => {
    const details = message.details as { jobId: string; jobName: string; prompt: string } | undefined;
    const jobName = details?.jobName || "Unknown";
    const prompt = details?.prompt || "";

    return new Text(
      theme.fg("accent", `🕐 Scheduled: ${jobName}`) +
      (prompt ? theme.fg("dim", ` → "${prompt}"`) : ""),
      0,
      0
    );
  });

  pi.registerMessageRenderer("scheduled_prompt_begin", (message, _options, theme) => {
    const details = message.details as
      | { jobId: string; jobName: string; prompt: string; startTime?: string }
      | undefined;
    const ts = details?.startTime ? formatLocalDateTime(new Date(details.startTime)) : "";
    return new Text(
      theme.fg("accent", `⏳ [Scheduled Prompt] Processing begins: ${details?.jobName ?? "Unknown"}`) +
      (ts ? theme.fg("dim", ` [${ts}]`) : "") +
      (details?.prompt ? theme.fg("dim", ` → "${details.prompt}"`) : ""),
      0,
      0
    );
  });

  pi.registerMessageRenderer("scheduled_prompt_end", (message, _options, theme) => {
    const details = message.details as
      | {
          jobId: string;
          jobName: string;
          failureHint?: string;
          startTime?: string;
          endTime?: string;
        }
      | undefined;
    const container = new Container();
    const headColor = details?.failureHint ? "warning" : "success";
    const headIcon = details?.failureHint ? "⚠" : "✓";
    const headText = details?.failureHint
      ? `[Scheduled Prompt] Processing ended (failed).`
      : `[Scheduled Prompt] Processing ended.`;
    let timing = "";
    if (details?.startTime && details?.endTime) {
      const startMs = new Date(details.startTime).getTime();
      const endMs = new Date(details.endTime).getTime();
      const durationS = Math.max(0, Math.round((endMs - startMs) / 1000));
      timing = ` [${formatLocalDateTime(new Date(details.endTime))}, ${durationS}s]`;
    } else if (details?.endTime) {
      timing = ` [${formatLocalDateTime(new Date(details.endTime))}]`;
    }
    container.addChild(
      new Text(
        theme.fg(headColor, `${headIcon} ${headText}`) +
          theme.fg("dim", `${timing} See /schedule-prompt replay ${details?.jobId ?? ""} to review.`),
        0,
        0
      )
    );
    if (details?.failureHint) {
      container.addChild(new Text(theme.fg("warning", `   ↳ ${details.failureHint}`), 0, 0));
    }
    return container;
  });

  // Register the tool once with getter functions.
  // Lazy-init storage/scheduler so the tool works in --mode rpc where session_start never fires.
  const getStorage = () => {
    if (!storage) {
      storage = new CronStorage(process.cwd());
      storageForVisibility = storage;
      widgetVisible = storage.getWidgetVisible();
    }
    return storage;
  };
  const getScheduler = () => {
    if (!scheduler) {
      getStorage(); // ensure storage is initialized first
      scheduler = new CronScheduler(storage, pi);
      scheduler.start();
    }
    return scheduler;
  };
  const tool = createCronTool(getStorage, getScheduler);
  pi.registerTool(tool);

  // --- Session initialization ---

  const initializeSession = (ctx: any) => {
    // Create storage and scheduler
    storage = new CronStorage(ctx.cwd);
    storageForVisibility = storage;
    widgetVisible = storage.getWidgetVisible();
    scheduler = new CronScheduler(storage, pi);
    widget = new CronWidget(storage, scheduler, pi, () => widgetVisible);

    // Load and start all enabled jobs
    scheduler.start();

    // Show widget
    if (widgetVisible) {
      widget.show(ctx);
    }
  };

  const cleanupForSession = (ctx: any, reason: SessionShutdownReason | undefined) => {
    cleanupSession(scheduler, widget, ctx, reason);
  };

  const autoCleanupDisabledJobs = () => {
    if (!storage) return;
    const removed = pickJobsToAutoCleanup(storage.getAllJobs());
    for (const job of removed) {
      storage.removeJob(job.id);
    }
    if (removed.length > 0) {
      console.log(`Auto-cleanup: removing ${removed.length} disabled one-shot job(s)`);
    }
  };

  // --- Lifecycle events ---

  pi.on("session_start", async (_event, ctx) => {
    initializeSession(ctx);
  });

  pi.on("session_shutdown", async (event, ctx) => {
    autoCleanupDisabledJobs();
    cleanupForSession(ctx, event.reason as SessionShutdownReason | undefined);
  });

  pi.on("agent_start", async (_event) => {
    if (scheduler) {
      scheduler.notifyAgentStart();
    }
  });

  pi.on("agent_end", async (event) => {
    if (scheduler) {
      scheduler.notifyAgentEnd(event.messages);
    }
  });

  // --- Register /schedule-prompt command ---

  pi.registerCommand("schedule-prompt", {
    description:
      "Manage scheduled prompts interactively. Subcommands: " +
      "retry [N|jobId|substring] — re-fire a job (defaults to the most recent run); " +
      "replay [N|jobId|substring] — show output of a past run; " +
      "ps — list dedicated prompts currently running or queued.",
    handler: async (args, ctx) => {
      const trimmed = args?.trim() ?? "";
      if (trimmed === "retry" || trimmed.startsWith("retry ")) {
        const subArg = trimmed.slice("retry".length).trim();
        await handleRetry(subArg, ctx, getStorage(), getScheduler());
        return;
      }
      if (trimmed === "replay" || trimmed.startsWith("replay ")) {
        const subArg = trimmed.slice("replay".length).trim();
        await handleReplay(subArg, ctx, getStorage());
        return;
      }
      if (trimmed === "ps") {
        // Touch storage so widget visibility / leader paths match other subcommands;
        // ps itself only reads module-scope state.
        getStorage();
        ctx.ui.notify(formatPsOutput(getDedicatedActivity()), "info");
        return;
      }

      const action = await ctx.ui.select("Scheduled Prompts", [
        "View All Jobs",
        "Add New Job",
        "Toggle Job (Enable/Disable)",
        "Remove Job",
        "Cleanup Disabled Jobs",
        "Toggle Widget Visibility",
      ]);

      if (!action) return;

      const actionMap: Record<string, string> = {
        "View All Jobs": "list",
        "Add New Job": "add",
        "Toggle Job (Enable/Disable)": "toggle",
        "Remove Job": "remove",
        "Cleanup Disabled Jobs": "cleanup",
        "Toggle Widget Visibility": "toggleWidget",
      };
      const actionKey = actionMap[action];

      switch (actionKey) {
        case "list": {
          const jobs = storage.getAllJobs();
          if (jobs.length === 0) {
            ctx.ui.notify("No scheduled prompts configured", "info");
            return;
          }

          const sorted = sortJobsByNextRun(jobs, id => scheduler.getNextRun(id));
          const lines = ["Scheduled prompts:", ""];
          for (const { job, nextRun } of sorted) {
            const status = job.enabled ? "✓" : "✗";
            lines.push(`${status} ${job.name} (${job.id})`);
            lines.push(`  Schedule: ${formatSchedule(job.type, job.schedule)} | Type: ${job.type} | Recurring: ${job.type !== "once" ? "yes" : "no"} | Guaranteed: ${job.guaranteed ? "yes" : "no"} | Dedicated: ${job.dedicatedContext ? "yes" : "no"}`);
            lines.push(`  Prompt: ${job.prompt}`);
            if (nextRun) {
              const hint = formatRelativeHint(nextRun);
              lines.push(`  Next run: ${formatLocalDateTime(nextRun)}${hint ? ` (${hint})` : ""}`);
            }
            lines.push(`  Runs: ${job.runCount}`);
            lines.push("");
          }

          ctx.ui.notify(lines.join("\n"), "info");
          break;
        }

        case "add": {
          const name = await ctx.ui.input("Job Name", "Enter a name for this scheduled prompt");
          if (!name) return;

          const typeChoice = await ctx.ui.select("Job Type", [
            "Cron (recurring)",
            "Once (one-shot)",
            "Interval (periodic)",
          ]);
          if (!typeChoice) return;

          const typeMap: Record<string, string> = {
            "Cron (recurring)": "cron",
            "Once (one-shot)": "once",
            "Interval (periodic)": "interval",
          };
          const jobType = typeMap[typeChoice];

          let schedulePrompt: string;
          if (jobType === "cron") {
            schedulePrompt = "Enter cron expression (6-field: sec min hour dom month dow):";
          } else if (jobType === "once") {
            schedulePrompt = "Enter ISO timestamp (e.g., 2026-02-13T10:30:00Z)";
          } else {
            schedulePrompt = "Enter interval (e.g., 5m, 1h, 30s)";
          }

          const schedule = await ctx.ui.input("Schedule", schedulePrompt);
          if (!schedule) return;

          const prompt = await ctx.ui.input("Prompt", "Enter the prompt to execute");
          if (!prompt) return;

          // Validate and create job
          try {
            let intervalMs: number | undefined;
            let validatedSchedule = schedule;

            if (jobType === "interval") {
              const parsed = CronScheduler.parseInterval(schedule);
              intervalMs = parsed !== null ? parsed : undefined;
              if (!intervalMs) {
                ctx.ui.notify("Invalid interval format", "error");
                return;
              }
            } else if (jobType === "once") {
              const date = new Date(schedule);
              if (isNaN(date.getTime())) {
                ctx.ui.notify("Invalid timestamp format", "error");
                return;
              }
              validatedSchedule = date.toISOString();
            } else {
              const validation = CronScheduler.validateCronExpression(schedule);
              if (!validation.valid) {
                ctx.ui.notify(`Invalid cron expression: ${validation.error}`, "error");
                return;
              }
            }

            const job = {
              id: nanoid(10),
              name,
              schedule: validatedSchedule,
              prompt,
              enabled: true,
              type: jobType as any,
              intervalMs,
              createdAt: new Date().toISOString(),
              runCount: 0,
            };

            storage.addJob(job);
            scheduler.addJob(job);
            ctx.ui.notify(`Created scheduled prompt: ${name}`, "info");
          } catch (error: any) {
            ctx.ui.notify(`Error: ${error.message}`, "error");
          }
          break;
        }

        case "toggle": {
          const jobs = storage.getAllJobs();
          if (jobs.length === 0) {
            ctx.ui.notify("No scheduled prompts configured", "info");
            return;
          }

          const jobId = await ctx.ui.select(
            "Select Job to Toggle",
            jobs.map((j) => `${j.enabled ? "✓" : "✗"} ${j.name}`)
          );

          if (!jobId) return;

          // Find job by matching the label
          const selectedIndex = jobs.findIndex(
            (j) => `${j.enabled ? "✓" : "✗"} ${j.name}` === jobId
          );
          const job = selectedIndex >= 0 ? jobs[selectedIndex] : undefined;
          if (job) {
            const newEnabled = !job.enabled;
            storage.updateJob(job.id, { enabled: newEnabled });
            const updated = { ...job, enabled: newEnabled };
            scheduler.updateJob(job.id, updated);
            ctx.ui.notify(`${newEnabled ? "Enabled" : "Disabled"} job: ${job.name}`, "info");
          }
          break;
        }

        case "remove": {
          const jobs = storage.getAllJobs();
          if (jobs.length === 0) {
            ctx.ui.notify("No scheduled prompts configured", "info");
            return;
          }

          const jobId = await ctx.ui.select(
            "Select Job to Remove",
            jobs.map((j) => j.name)
          );

          if (!jobId) return;

          // Find job by name
          const job = jobs.find((j) => j.name === jobId);
          if (job) {
            const confirmed = await ctx.ui.confirm(
              "Confirm Removal",
              `Remove scheduled prompt "${job.name}"?`
            );

            if (confirmed) {
              storage.removeJob(job.id);
              scheduler.removeJob(job.id);
              ctx.ui.notify(`Removed job: ${job.name}`, "info");
            }
          }
          break;
        }

        case "cleanup": {
          const jobs = storage.getAllJobs();
          const disabledJobs = jobs.filter((j) => !j.enabled);

          if (disabledJobs.length === 0) {
            ctx.ui.notify("No disabled jobs to clean up", "info");
            return;
          }

          const confirmed = await ctx.ui.confirm(
            "Confirm Cleanup",
            `Remove ${disabledJobs.length} disabled job(s)?`
          );

          if (confirmed) {
            for (const job of disabledJobs) {
              storage.removeJob(job.id);
              scheduler.removeJob(job.id);
            }
            ctx.ui.notify(`Removed ${disabledJobs.length} disabled job(s)`, "info");
          }
          break;
        }

        case "toggleWidget": {
          widgetVisible = !widgetVisible;
          storageForVisibility?.setWidgetVisible(widgetVisible);
          if (widgetVisible) {
            widget.show(ctx);
            ctx.ui.notify("Widget enabled (shows when jobs exist)", "info");
          } else {
            widget.hide(ctx);
            ctx.ui.notify("Widget disabled (hidden)", "info");
          }
          break;
        }
      }
    },
  });

}

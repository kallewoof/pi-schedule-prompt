import type { Static } from "typebox";
import { Type } from "typebox";
import { StringEnum } from "@mariozechner/pi-ai";

/**
 * Type of cron job
 */
export type CronJobType = "cron" | "once" | "interval";

/**
 * Status of the last job execution
 */
export type CronJobStatus = "success" | "error" | "running" | "sent";

/**
 * A scheduled cron job
 */
export interface CronJob {
  /** Unique identifier */
  id: string;
  /** Human-readable name */
  name: string;
  /** Cron expression, ISO timestamp, or interval description */
  schedule: string;
  /** The prompt to execute */
  prompt: string;
  /** Whether the job is enabled */
  enabled: boolean;
  /** Type of job */
  type: CronJobType;
  /** Interval in milliseconds (for interval type) */
  intervalMs?: number;
  /** When the job was created */
  createdAt: string;
  /** Last execution timestamp */
  lastRun?: string;
  /** Status of last execution */
  lastStatus?: CronJobStatus;
  /** Next scheduled run (computed) */
  nextRun?: string;
  /** Number of times executed */
  runCount: number;
  /** Optional description */
  description?: string;
  /** Retry semantics for transient failures. Recurring jobs always catch up a missed tick on startup regardless of this flag; `guaranteed` additionally enables in-session retries on model errors and unconfirmed sends, and (for once-jobs) re-fires a missed prompt instead of marking it failed. */
  guaranteed?: boolean;
  /** RPC context name to route the fired response to (e.g. Signal thread or group); undefined = main session */
  targetContext?: string;
  /** If true, run prompt in a fresh blank subprocess context; main session receives only start/end notifications */
  dedicatedContext?: boolean;
  /** If true, run prompt in a dedicated subprocess like `dedicatedContext`, but keep the resulting session viewable/enterable (`/schedule-prompt enter`) and raise a persistent "reports available" indicator until viewed or dismissed. Mutually exclusive with command. */
  standalone?: boolean;
  /** If true, the prompt is executed as a shell command via `bash -c` instead of being sent to the agent. Mutually exclusive with dedicatedContext and standalone. */
  command?: boolean;
}

/**
 * Record of a single scheduled job run, persisted for /schedule-prompt replay
 */
export interface RunRecord {
  /** Stable per-run id, so a report can be addressed (enter/dismiss) and acknowledged. */
  id: string;
  jobId: string;
  jobName: string;
  jobPrompt: string;
  schedule: string;
  jobType: CronJobType;
  /** ISO — when executeJob fired the prompt */
  startTime: string;
  /** ISO — when agent_end was received */
  endTime: string;
  /** Extracted assistant text from the turn */
  output: string;
  status: "success" | "error";
  /** True when this run came from a `standalone` job and should surface as a report. */
  standalone?: boolean;
  /** Resolved path of the dedicated subprocess session file, for `/schedule-prompt enter`. */
  sessionFilePath?: string;
  /** True once the user has viewed (replay/enter) or dismissed the report. */
  acknowledged?: boolean;
}

/**
 * Persistent storage for cron jobs
 */
export interface CronStore {
  jobs: CronJob[];
  version: number;
  widgetVisible?: boolean;
  /** Last 10 run records, newest last */
  runHistory?: RunRecord[];
}

/**
 * Tool result details for LLM context
 */
export interface CronToolDetails {
  action: string;
  jobs: CronJob[];
  error?: string;
  jobId?: string;
  jobName?: string;
}

/**
 * Tool parameter schema
 */
export const CronToolParams = Type.Object({
  action: StringEnum(["add", "remove", "list", "enable", "disable", "update", "cleanup"], {
    description: "Action to perform",
  }),
  name: Type.Optional(
    Type.String({
      description: "Job name, auto-generated if omitted",
    })
  ),
  schedule: Type.Optional(
    Type.String({
      description:
        "Required for add. Cron expression, ISO timestamp, relative time (+10s, +5m), or interval string",
    })
  ),
  prompt: Type.Optional(
    Type.String({
      description: "Required for add. The prompt text to execute",
    })
  ),
  jobId: Type.Optional(
    Type.String({
      description: "Job ID for remove, enable, disable, or update actions",
    })
  ),
  jobType: Type.Optional(
    StringEnum(["cron", "once", "interval"], {
      description: "Job type: 'cron' (recurring, default), 'once' (single ISO timestamp or relative time like +10s), 'interval' (repeating, e.g. 5m)",
    })
  ),
  jobDescription: Type.Optional(
    Type.String({
      description: "Optional human-readable description of what this job does",
    })
  ),
  guaranteed: Type.Optional(
    Type.Boolean({
      description:
        "Retry semantics for transient failures. Recurring jobs always catch up a missed tick on startup regardless of this flag. If true, additionally retry in-session on model errors / unconfirmed sends, and (for once-jobs) re-fire a missed prompt rather than marking it failed.",
    })
  ),
  dedicatedContext: Type.Optional(
    Type.Boolean({
      description:
        "If true, run the prompt in a blank dedicated subprocess context. The current session receives only start/end notifications; full output is viewable via /schedule-prompt replay.",
    })
  ),
  standalone: Type.Optional(
    Type.Boolean({
      description:
        "Like dedicatedContext (runs in a blank dedicated subprocess), but in addition the resulting session is viewable and enterable for follow-up questions via /schedule-prompt enter, and a persistent 'reports available' indicator is shown until the user views or dismisses it. Use for tasks that produce a report you'll want to read or dig into later. Mutually exclusive with command.",
    })
  ),
  command: Type.Optional(
    Type.Boolean({
      description:
        "If true, the prompt is executed as a shell command via `bash -c` instead of being sent to the agent. Use for reminders (`echo \"do X\"`), external side-effects (`signal-cli send ...`), or script runs — anything that doesn't need agent reasoning. Output is shown as a notification and captured for /schedule-prompt replay. Mutually exclusive with dedicatedContext.",
    })
  ),
});

export type CronToolParamsType = Static<typeof CronToolParams>;

/**
 * Mirrors pi-mono's `SessionShutdownEvent["reason"]`.
 * `"quit"` is the only value that means the host process is exiting; the rest
 * are session replacements where the host process keeps running and any
 * dedicated-context subprocesses should be left alive across the swap.
 */
export type SessionShutdownReason = "quit" | "reload" | "new" | "resume" | "fork";

/**
 * Event emitted when a job is added, removed, or updated
 */
export interface CronChangeEvent {
  type: "add" | "remove" | "update" | "fire" | "error";
  job?: CronJob;
  jobId?: string;
  error?: string;
}

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
  /** If true, run missed jobs immediately on startup; if false (default), drop missed recurring or mark missed one-time as failed */
  guaranteed?: boolean;
  /** RPC context name to route the fired response to (e.g. Signal thread or group); undefined = main session */
  targetContext?: string;
}

/**
 * Record of a single scheduled job run, persisted for /replay
 */
export interface RunRecord {
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
}

/**
 * Persistent storage for cron jobs
 */
export interface CronStore {
  jobs: CronJob[];
  version: number;
  widgetVisible?: boolean;
  /** Last 50 run records, newest last */
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
        "If true, run missed jobs immediately on startup. If false (default), silently drop missed recurring jobs or mark missed one-time jobs as failed.",
    })
  ),
});

export type CronToolParamsType = Static<typeof CronToolParams>;

/**
 * Event emitted when a job is added, removed, or updated
 */
export interface CronChangeEvent {
  type: "add" | "remove" | "update" | "fire" | "error";
  job?: CronJob;
  jobId?: string;
  error?: string;
}

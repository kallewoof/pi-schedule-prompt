import * as fs from "fs";
import * as path from "path";
import { nanoid } from "nanoid";
import type { CronJob, CronStore, RunRecord } from "./types.js";

/** Max run records retained for /schedule-prompt replay. */
const MAX_RUN_HISTORY = 10;

/**
 * Trim run history to MAX_RUN_HISTORY, oldest-first, while preferentially
 * preserving unacknowledged standalone reports so a pending report can't be
 * silently evicted before the user sees it. Only if the history is *entirely*
 * unread reports beyond the cap do we drop the oldest of those too — the cap
 * stays a hard upper bound on store growth. Order (newest last) is preserved.
 */
export function trimRunHistory(history: RunRecord[]): RunRecord[] {
  let excess = history.length - MAX_RUN_HISTORY;
  if (excess <= 0) return history;
  const isProtected = (r: RunRecord) => !!(r.standalone && !r.acknowledged);
  const toRemove = new Set<RunRecord>();
  // Pass 1: drop oldest non-protected records first.
  for (const r of history) {
    if (excess <= 0) break;
    if (!isProtected(r)) {
      toRemove.add(r);
      excess--;
    }
  }
  // Pass 2: still over budget (history is all unread reports) — drop oldest.
  for (const r of history) {
    if (excess <= 0) break;
    if (!toRemove.has(r)) {
      toRemove.add(r);
      excess--;
    }
  }
  return history.filter((r) => !toRemove.has(r));
}

/**
 * Handles persistence of scheduled prompts to .pi/schedule-prompts.json
 */
export class CronStorage {
  private readonly storePath: string;
  private readonly piDir: string;

  constructor(cwd: string) {
    this.piDir = path.join(cwd, ".pi");
    this.storePath = path.join(this.piDir, "schedule-prompts.json");
  }

  /**
   * Load scheduled prompts from disk
   */
  load(): CronStore {
    try {
      if (fs.existsSync(this.storePath)) {
        const data = fs.readFileSync(this.storePath, "utf-8");
        try {
          return JSON.parse(data) as CronStore;
        } catch (parseError) {
          // The file exists but is unparseable. Returning an empty store here
          // would let the very next save() (e.g. recording a run) overwrite
          // the corrupt file with `{ jobs: [] }` and permanently erase every
          // scheduled job. Move the bad file aside so the original content is
          // preserved for manual recovery before falling through to empty.
          const backupPath = `${this.storePath}.corrupt-${Date.now()}`;
          try {
            fs.renameSync(this.storePath, backupPath);
            console.error(
              `Failed to parse scheduled prompts (${(parseError as Error).message}). ` +
                `Moved corrupt file aside as ${backupPath} for manual recovery.`
            );
          } catch (renameError) {
            console.error("Failed to back up corrupt scheduled prompts:", renameError);
          }
        }
      }
    } catch (error) {
      console.error("Failed to load scheduled prompts:", error);
    }

    // Return empty store if file doesn't exist (or was corrupt and moved aside)
    return { jobs: [], version: 1 };
  }

  /**
   * Save scheduled prompts to disk
   */
  save(store: CronStore): void {
    const tempPath = `${this.storePath}.tmp`;
    const payload = JSON.stringify(store, null, 2);
    const attempt = () => {
      fs.mkdirSync(this.piDir, { recursive: true });
      fs.writeFileSync(tempPath, payload, "utf-8");
      fs.renameSync(tempPath, this.storePath);
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
          console.error("Failed to save scheduled prompts (after retry):", retryError);
          throw retryError;
        }
      }
      console.error("Failed to save scheduled prompts:", error);
      throw error;
    }
  }

  /**
   * Check if a job name already exists
   */
  hasJobWithName(name: string): boolean {
    const store = this.load();
    return store.jobs.some((j) => j.name === name);
  }

  /**
   * Add a new job
   */
  addJob(job: CronJob): void {
    const store = this.load();
    store.jobs.push(job);
    this.save(store);
  }

  /**
   * Remove a job by ID
   */
  removeJob(id: string): boolean {
    const store = this.load();
    const initialLength = store.jobs.length;
    store.jobs = store.jobs.filter((j) => j.id !== id);

    if (store.jobs.length < initialLength) {
      this.save(store);
      return true;
    }
    return false;
  }

  /**
   * Update a job by ID
   */
  updateJob(id: string, partial: Partial<CronJob>): boolean {
    const store = this.load();
    const job = store.jobs.find((j) => j.id === id);

    if (job) {
      Object.assign(job, partial);
      this.save(store);
      return true;
    }
    return false;
  }

  /**
   * Get a single job by ID
   */
  getJob(id: string): CronJob | undefined {
    const store = this.load();
    return store.jobs.find((j) => j.id === id);
  }

  /**
   * Get all jobs
   */
  getAllJobs(): CronJob[] {
    const store = this.load();
    return store.jobs;
  }

  getWidgetVisible(): boolean {
    const store = this.load();
    return store.widgetVisible !== false;
  }

  setWidgetVisible(visible: boolean): void {
    const store = this.load();
    store.widgetVisible = visible;
    this.save(store);
  }

  getLastReplayedReportId(): string | undefined {
    return this.load().lastReplayedReportId;
  }

  setLastReplayedReportId(id: string): void {
    const store = this.load();
    store.lastReplayedReportId = id;
    this.save(store);
  }

  getRunHistory(): RunRecord[] {
    return this.load().runHistory ?? [];
  }

  addRunRecord(record: Omit<RunRecord, "id">): void {
    const store = this.load();
    const history = store.runHistory ?? [];
    history.push({ id: nanoid(10), ...record });
    this.save({ ...store, runHistory: trimRunHistory(history) });
  }

  /** Standalone reports the user hasn't yet viewed or dismissed (newest last). */
  getUnacknowledgedReports(): RunRecord[] {
    return this.getRunHistory().filter((r) => r.standalone && !r.acknowledged);
  }

  /** Mark a single run record acknowledged by its id. Returns false if not found. */
  acknowledgeRun(id: string): boolean {
    const store = this.load();
    const history = store.runHistory ?? [];
    const record = history.find((r) => r.id === id);
    if (!record) return false;
    record.acknowledged = true;
    this.save({ ...store, runHistory: history });
    return true;
  }

  /** Mark every standalone report acknowledged. */
  acknowledgeAllReports(): void {
    const store = this.load();
    const history = store.runHistory ?? [];
    for (const r of history) {
      if (r.standalone) r.acknowledged = true;
    }
    this.save({ ...store, runHistory: history });
  }

  getStorePath(): string {
    return this.storePath;
  }

  getPiDir(): string {
    return this.piDir;
  }
}

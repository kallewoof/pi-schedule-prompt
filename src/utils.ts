import type { CronJob } from "./types.js";

const MONTHS = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];

export function formatLocalDateTime(date: Date): string {
  const h = date.getHours().toString().padStart(2, '0');
  const m = date.getMinutes().toString().padStart(2, '0');
  return `${MONTHS[date.getMonth()]} ${date.getDate()} ${h}:${m}`;
}

export function formatISOLocal(iso: string): string {
  return formatLocalDateTime(new Date(iso));
}

export function formatRelativeHint(date: Date): string {
  const diffMs = date.getTime() - Date.now();
  if (diffMs <= 0) return "";
  const totalMinutes = Math.floor(diffMs / 60000);
  const hours = Math.floor(totalMinutes / 60);
  const minutes = totalMinutes % 60;
  if (hours >= 24) return "";
  if (hours >= 2) return `in ${hours} hours`;
  if (hours === 1) return minutes > 0 ? `1 hour and ${minutes} minute${minutes === 1 ? "" : "s"}` : "1 hour";
  return `${totalMinutes} minute${totalMinutes === 1 ? "" : "s"}`;
}

export function formatSchedule(type: string, schedule: string): string {
  return type === "once" ? formatISOLocal(schedule) : schedule;
}

export function sortJobsByNextRun(
  jobs: CronJob[],
  getNextRun: (id: string) => Date | null
): Array<{ job: CronJob; nextRun: Date | null }> {
  return jobs
    .map(job => ({ job, nextRun: getNextRun(job.id) }))
    .sort((a, b) => {
      if (!a.nextRun && !b.nextRun) return 0;
      if (!a.nextRun) return 1;
      if (!b.nextRun) return -1;
      return a.nextRun.getTime() - b.nextRun.getTime();
    });
}

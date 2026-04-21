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

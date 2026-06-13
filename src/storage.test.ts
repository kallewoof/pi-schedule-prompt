import { describe, it, expect } from "vitest";
import { trimRunHistory } from "./storage.js";
import type { RunRecord } from "./types.js";

function rec(overrides: Partial<RunRecord> = {}): RunRecord {
  return {
    id: Math.random().toString(36).slice(2),
    jobId: "j",
    jobName: "Job",
    jobPrompt: "p",
    schedule: "s",
    jobType: "cron",
    startTime: "2026-06-11T10:00:00.000Z",
    endTime: "2026-06-11T10:00:00.000Z",
    output: "",
    status: "success",
    ...overrides,
  };
}

describe("trimRunHistory", () => {
  it("leaves history untouched when within the cap", () => {
    const history = Array.from({ length: 10 }, (_, i) => rec({ id: `r${i}` }));
    expect(trimRunHistory(history)).toHaveLength(10);
  });

  it("drops the oldest plain records first when over the cap", () => {
    const history = Array.from({ length: 12 }, (_, i) => rec({ id: `r${i}` }));
    const trimmed = trimRunHistory(history);
    expect(trimmed).toHaveLength(10);
    // Oldest two (r0, r1) dropped; newest preserved in order.
    expect(trimmed.map((r) => r.id)).toEqual(
      ["r2", "r3", "r4", "r5", "r6", "r7", "r8", "r9", "r10", "r11"]
    );
  });

  it("preserves an unacknowledged standalone report that would otherwise be evicted", () => {
    // r0 is the oldest and a pending report; the next-oldest plain records
    // should be dropped instead so the report survives.
    const history = [
      rec({ id: "r0", standalone: true, acknowledged: false }),
      ...Array.from({ length: 11 }, (_, i) => rec({ id: `p${i}` })),
    ];
    const trimmed = trimRunHistory(history);
    expect(trimmed).toHaveLength(10);
    expect(trimmed.some((r) => r.id === "r0")).toBe(true);
    // Two oldest plain records (p0, p1) dropped to make room.
    expect(trimmed.some((r) => r.id === "p0")).toBe(false);
    expect(trimmed.some((r) => r.id === "p1")).toBe(false);
  });

  it("does not protect an already-acknowledged standalone report", () => {
    const history = [
      rec({ id: "r0", standalone: true, acknowledged: true }),
      ...Array.from({ length: 11 }, (_, i) => rec({ id: `p${i}` })),
    ];
    const trimmed = trimRunHistory(history);
    expect(trimmed).toHaveLength(10);
    // r0 is the oldest and not protected → dropped first.
    expect(trimmed.some((r) => r.id === "r0")).toBe(false);
  });

  it("still enforces the cap when every record is an unread report", () => {
    const history = Array.from({ length: 13 }, (_, i) =>
      rec({ id: `r${i}`, standalone: true, acknowledged: false })
    );
    const trimmed = trimRunHistory(history);
    expect(trimmed).toHaveLength(10);
    // Oldest three dropped, newest kept.
    expect(trimmed[0].id).toBe("r3");
    expect(trimmed.at(-1)?.id).toBe("r12");
  });
});

import { describe, it, expect } from "vitest";
import { formatLocalDateTime, formatISOLocal, formatRelativeHint } from "./utils.js";

function minutesFromNow(n: number): Date {
  return new Date(Date.now() + n * 60 * 1000);
}

describe("formatLocalDateTime", () => {
  it("formats a date as 'Mon D HH:MM'", () => {
    const d = new Date(2026, 3, 24, 9, 5); // Apr 24 09:05
    expect(formatLocalDateTime(d)).toBe("Apr 24 09:05");
  });

  it("pads single-digit hours and minutes", () => {
    const d = new Date(2026, 0, 3, 1, 2); // Jan 3 01:02
    expect(formatLocalDateTime(d)).toBe("Jan 3 01:02");
  });
});

describe("formatISOLocal", () => {
  it("parses an ISO string and formats it the same as formatLocalDateTime", () => {
    const d = new Date(2026, 5, 15, 14, 30); // Jun 15 14:30
    expect(formatISOLocal(d.toISOString())).toBe(formatLocalDateTime(d));
  });
});

describe("formatRelativeHint", () => {
  it("returns empty string for dates in the past", () => {
    expect(formatRelativeHint(new Date(Date.now() - 1000))).toBe("");
  });

  it("returns empty string for dates >= 24 hours away", () => {
    expect(formatRelativeHint(minutesFromNow(24 * 60))).toBe("");
    expect(formatRelativeHint(minutesFromNow(48 * 60))).toBe("");
  });

  it("returns 'in X hours' for 2-23 hours away", () => {
    expect(formatRelativeHint(minutesFromNow(2 * 60 + 1))).toBe("in 2 hours");
    expect(formatRelativeHint(minutesFromNow(5 * 60 + 30))).toBe("in 5 hours");
    expect(formatRelativeHint(minutesFromNow(23 * 60 + 59))).toBe("in 23 hours");
  });

  it("returns '1 hour and X minutes' for 1-2 hours with remaining minutes", () => {
    expect(formatRelativeHint(minutesFromNow(90))).toBe("1 hour and 30 minutes");
    expect(formatRelativeHint(minutesFromNow(61))).toBe("1 hour and 1 minute");
    expect(formatRelativeHint(minutesFromNow(119))).toBe("1 hour and 59 minutes");
  });

  it("returns '1 hour' for exactly 1 hour away", () => {
    expect(formatRelativeHint(minutesFromNow(60))).toBe("1 hour");
  });

  it("returns 'X minutes' for under 1 hour", () => {
    expect(formatRelativeHint(minutesFromNow(45))).toBe("45 minutes");
    expect(formatRelativeHint(minutesFromNow(10))).toBe("10 minutes");
    expect(formatRelativeHint(minutesFromNow(1))).toBe("1 minute");
  });
});

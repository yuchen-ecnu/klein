import { describe, expect, it } from "vitest";
import { formatDateFromTimeMs, formatDuration } from "./formatUtils";

describe("formatDuration", () => {
  it.each([
    [0, "00h 00m 00s"],
    [61, "00h 01m 01s"],
    [90_061, "1d 01h 01m 01s"],
    [Number.NaN, "00h 00m 00s"],
    [-1, "00h 00m 00s"],
  ])("formats %s seconds", (seconds, expected) => {
    expect(formatDuration(seconds)).toBe(expected);
  });
});

it("formats epoch milliseconds as a complete timestamp", () => {
  expect(formatDateFromTimeMs(0)).toMatch(/^1970\/01\/01 \d{2}:00:00$/);
});

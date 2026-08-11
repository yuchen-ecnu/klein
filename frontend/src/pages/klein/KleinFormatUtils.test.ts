import { describe, expect, it } from "vitest";
import {
  formatByteRate,
  formatBytes,
  formatCount,
  formatRate,
} from "./KleinFormatUtils";

describe("Klein metric formatting", () => {
  it.each([
    [999, "999"],
    [1_000, "1K"],
    [1_500_000, "1.5M"],
    [-2_000_000_000, "-2B"],
  ])("formats count %s", (value, expected) => {
    expect(formatCount(value)).toBe(expected);
  });

  it.each([
    [1_023, "1,023 B"],
    [1_024, "1 KB"],
    [1_048_576, "1 MB"],
    [1_073_741_824, "1 GB"],
    [1_099_511_627_776, "1 TB"],
  ])("formats bytes %s", (value, expected) => {
    expect(formatBytes(value)).toBe(expected);
  });

  it("adds rate units", () => {
    expect(formatRate(2_000)).toBe("2K");
    expect(formatByteRate(2_048)).toBe("2 KB/s");
  });
});

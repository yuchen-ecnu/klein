import { describe, expect, it } from "vitest";
import { operatorFixture, subtaskFixture } from "../../test/fixtures";
import { getOperatorNodeColors } from "./KleinJobGraphColors";

describe("getOperatorNodeColors", () => {
  it("uses the hottest finite subtask and clamps published maxima", () => {
    const colors = getOperatorNodeColors(
      operatorFixture({
        busy_percent: 10,
        backpressure_percent: 5,
        max_busy_percent: 140,
        subtasks: [
          subtaskFixture({ busy_percent: 70, backpressure_percent: 30 }),
          subtaskFixture({ busy_percent: Number.NaN, backpressure_percent: 80 }),
        ],
      }),
    );

    expect(colors.busyPercent).toBe(100);
    expect(colors.backpressurePercent).toBe(80);
    expect(colors.background).toMatch(/^#[0-9a-f]{6}$/);
    expect(colors.border).toMatch(/^#[0-9a-f]{6}$/);
  });

  it("falls back to the operator average and clamps negative values", () => {
    const colors = getOperatorNodeColors(
      operatorFixture({
        busy_percent: -5,
        backpressure_percent: 20.4,
        max_busy_percent: Number.NaN,
        subtasks: [],
      }),
    );

    expect(colors.busyPercent).toBe(0);
    expect(colors.backpressurePercent).toBe(20);
  });
});

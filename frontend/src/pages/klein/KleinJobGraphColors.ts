import type { KleinOperator } from "../../type/klein";

const KLEIN_NODE_COLORS = {
  background: {
    backpressured: "#fff0cc",
    busy: "#fadbd8",
    idle: "#eaf3fc",
  },
  border: {
    backpressured: "#c88a16",
    busy: "#d45d58",
    idle: "#7ea6cf",
  },
} as const;

export type OperatorNodeColors = {
  background: string;
  backpressurePercent: number;
  border: string;
  busyPercent: number;
};

// Keep Flink's continuous whole-node interpolation model, but use lighter
// surfaces and semantic borders that remain calm behind dense metric text.
// Both metrics use the hottest subtask.
export const getOperatorNodeColors = (
  operator: KleinOperator,
): OperatorNodeColors => {
  const busyPercent = getHottestPercent(
    operator.max_busy_percent,
    operator.subtasks?.map(({ busy_percent }) => busy_percent) ?? [],
    operator.busy_percent,
  );
  const backpressurePercent = getHottestPercent(
    operator.max_backpressure_percent,
    operator.subtasks?.map(
      ({ backpressure_percent }) => backpressure_percent,
    ) ?? [],
    operator.backpressure_percent,
  );
  const busyRatio = busyPercent / 100;
  const backpressureRatio = backpressurePercent / 100;

  return {
    background: blendHexColor(
      blendHexColor(
        KLEIN_NODE_COLORS.background.idle,
        KLEIN_NODE_COLORS.background.busy,
        busyRatio,
      ),
      KLEIN_NODE_COLORS.background.backpressured,
      backpressureRatio,
    ),
    backpressurePercent,
    border: blendHexColor(
      blendHexColor(
        KLEIN_NODE_COLORS.border.idle,
        KLEIN_NODE_COLORS.border.busy,
        busyRatio,
      ),
      KLEIN_NODE_COLORS.border.backpressured,
      backpressureRatio,
    ),
    busyPercent,
  };
};

const getHottestPercent = (
  publishedMaximum: number | undefined,
  subtaskValues: number[],
  operatorAverage: number | undefined,
) => {
  const finiteSubtaskValues = subtaskValues.filter(Number.isFinite);
  const subtaskMaximum =
    finiteSubtaskValues.length > 0
      ? Math.max(...finiteSubtaskValues)
      : undefined;
  const value = [publishedMaximum, subtaskMaximum, operatorAverage].find(
    (candidate): candidate is number =>
      typeof candidate === "number" && Number.isFinite(candidate),
  );
  return Math.round(Math.min(100, Math.max(0, value ?? 0)));
};

const blendHexColor = (from: string, to: string, ratio: number) => {
  const fromChannels = hexChannels(from);
  const toChannels = hexChannels(to);
  return `#${fromChannels
    .map((channel, index) =>
      Math.round(channel + (toChannels[index] - channel) * ratio)
        .toString(16)
        .padStart(2, "0"),
    )
    .join("")}`;
};

const hexChannels = (color: string) => [
  Number.parseInt(color.slice(1, 3), 16),
  Number.parseInt(color.slice(3, 5), 16),
  Number.parseInt(color.slice(5, 7), 16),
];

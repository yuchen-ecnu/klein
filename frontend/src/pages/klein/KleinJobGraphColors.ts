import type { KleinOperator } from "../../type/klein";

const FLINK_NODE_COLORS = {
  background: {
    backpressured: "#888888",
    busy: "#ee6464",
    idle: "#5db1ff",
  },
  border: {
    backpressured: "#000000",
    busy: "#ee2222",
    idle: "#1890ff",
  },
} as const;

export type OperatorNodeColors = {
  background: string;
  backpressurePercent: number;
  border: string;
  busyPercent: number;
};

// Match Flink's JobGraph: blend idle to busy first, then blend the result
// toward the backpressure color. Both metrics use the hottest subtask.
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
        FLINK_NODE_COLORS.background.idle,
        FLINK_NODE_COLORS.background.busy,
        busyRatio,
      ),
      FLINK_NODE_COLORS.background.backpressured,
      backpressureRatio,
    ),
    backpressurePercent,
    border: blendHexColor(
      blendHexColor(
        FLINK_NODE_COLORS.border.idle,
        FLINK_NODE_COLORS.border.busy,
        busyRatio,
      ),
      FLINK_NODE_COLORS.border.backpressured,
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
  return Math.min(100, Math.max(0, value ?? 0));
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

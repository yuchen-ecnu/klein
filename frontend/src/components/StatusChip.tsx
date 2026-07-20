import { Box } from "@mui/material";
import { blue, blueGrey, green, red } from "@mui/material/colors";
import { CSSProperties, ReactNode } from "react";

const orange = "#DB6D00";
const grey = "#5F6469";
const colorMap: Record<string, Record<string, string>> = {
  kleinJob: {
    CREATED: orange,
    SUBMITTING: orange,
    DEPLOYING: orange,
    INITIALIZING: orange,
    RUNNING: blue[500],
    FINISHED: green[500],
    CANCELLED: grey,
    FAILED: red[500],
    UNKNOWN: grey,
  },
  kleinOperator: {
    pending: orange,
    running: blue[500],
    recovering: orange,
    finished: green[500],
    failed: red[500],
  },
  kleinCheckpoint: {
    CREATED: orange,
    IN_PROGRESS: blue[500],
    NOTIFYING: blue[500],
    COMPLETED: green[500],
    FAILED: red[500],
  },
};

export type StatusChipProps = {
  type: keyof typeof colorMap;
  status: string | ReactNode;
  suffix?: ReactNode;
  icon?: ReactNode;
};

export const StatusChip = ({ type, status, suffix, icon }: StatusChipProps) => {
  const color =
    typeof status === "string"
      ? colorMap[type]?.[status] ?? blueGrey[500]
      : blueGrey[500];
  const style: CSSProperties = {
    borderColor: color,
    color,
    backgroundColor: color === blueGrey[500] ? undefined : `${color}20`,
  };
  return (
    <Box
      component="span"
      style={style}
      sx={{
        alignItems: "center",
        border: "solid 1px",
        borderRadius: "4px",
        display: "inline-flex",
        fontSize: 12,
        padding: "2px 8px",
      }}
    >
      {icon}
      <Box component="span" sx={icon === undefined ? {} : { marginLeft: "4px" }}>
        {status}
      </Box>
      {suffix !== undefined && <Box sx={{ marginLeft: 0.5 }}>{suffix}</Box>}
    </Box>
  );
};

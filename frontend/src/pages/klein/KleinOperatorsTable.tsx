import {
  LinearProgress,
  Link,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography,
} from "@mui/material";
import React from "react";
import { Link as RouterLink } from "react-router-dom";
import { generateActorLink } from "../../common/links";
import { StatusChip } from "../../components/StatusChip";
import { KleinOperator } from "../../type/klein";
import {
  formatByteRate,
  formatBytes,
  formatCount,
  formatRate,
} from "./KleinFormatUtils";

export const KleinOperatorsTable = ({
  operators,
  selectedOperatorId,
  onSelectOperator,
}: {
  operators: KleinOperator[];
  selectedOperatorId?: number;
  onSelectOperator: (operatorId: number) => void;
}) => (
  <TableContainer>
    <Table size="small">
      <TableHead>
        <TableRow>
          <TableCell>Operator</TableCell>
          <TableCell>Status</TableCell>
          <TableCell align="right">Parallelism</TableCell>
          <TableCell align="right">Records in / out</TableCell>
          <TableCell align="right">Records/s in / out</TableCell>
          <TableCell align="right">Estimated data/s in / out</TableCell>
          <TableCell>Busy</TableCell>
          <TableCell>Backpressure</TableCell>
          <TableCell align="right">Queued</TableCell>
          <TableCell>Latest checkpoint</TableCell>
        </TableRow>
      </TableHead>
      <TableBody>
        {operators.map((operator) => (
          <TableRow
            aria-label={`Select operator ${operator.name}`}
            data-testid={`klein-operator-row-${operator.op_id}`}
            hover
            key={operator.op_id}
            onClick={() => onSelectOperator?.(operator.op_id)}
            onKeyDown={(event) => {
              if (event.key === "Enter" || event.key === " ") {
                event.preventDefault();
                onSelectOperator?.(operator.op_id);
              }
            }}
            role="button"
            selected={selectedOperatorId === operator.op_id}
            sx={{ cursor: "pointer" }}
            tabIndex={0}
          >
            <TableCell>
              <Typography sx={{ fontWeight: 500 }} variant="body2">
                {operator.name}
              </Typography>
              <Typography color="text.secondary" variant="caption">
                ID {operator.op_id} · {operator.cpus} CPU · {operator.gpus} GPU
              </Typography>
              <Typography display="block" variant="caption">
                <OperatorActorsLink
                  onOpenDetails={() => onSelectOperator(operator.op_id)}
                  operator={operator}
                />
              </Typography>
            </TableCell>
            <TableCell>
              <StatusChip type="kleinOperator" status={operator.status} />
            </TableCell>
            <TableCell align="right">
              {operator.instances.running}/{operator.parallelism} running
            </TableCell>
            <TableCell align="right">
              {formatCount(operator.rows_in)} / {formatCount(operator.rows_out)}
            </TableCell>
            <TableCell align="right">
              {formatRate(operator.rows_in_per_second)} /{" "}
              {formatRate(operator.rows_out_per_second)}
            </TableCell>
            <TableCell align="right">
              {formatByteRate(operator.bytes_in_per_second)} /{" "}
              {formatByteRate(operator.bytes_out_per_second)}
            </TableCell>
            <PercentageCell value={operator.busy_percent} />
            <PercentageCell
              detail={`${operator.backpressure_events ?? 0} events`}
              value={operator.backpressure_percent}
            />
            <TableCell align="right">
              {operator.queued}
              {operator.capacity > 0 ? ` / ${operator.capacity}` : ""}
            </TableCell>
            <TableCell>
              <Typography variant="body2">
                {operator.last_checkpoint_id !== undefined
                  ? `#${operator.last_checkpoint_id}`
                  : "-"}
              </Typography>
              <Typography color="text.secondary" variant="caption">
                {formatBytes(operator.checkpoint_state_size_bytes ?? 0)} · align{" "}
                {(operator.checkpoint_alignment_ms ?? 0).toFixed(1)} ms
              </Typography>
            </TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  </TableContainer>
);

const OperatorActorsLink = ({
  onOpenDetails,
  operator,
}: {
  onOpenDetails: () => void;
  operator: KleinOperator;
}) => {
  const label = `View ${operator.parallelism} actor${
    operator.parallelism === 1 ? "" : "s"
  }`;
  if (operator.parallelism !== 1) {
    return (
      <Link
        component="button"
        onClick={(event) => {
          event.stopPropagation();
          onOpenDetails();
        }}
        onKeyDown={(event) => event.stopPropagation()}
        sx={{ fontSize: "inherit", padding: 0, verticalAlign: "baseline" }}
        type="button"
      >
        {label}
      </Link>
    );
  }

  const actorId = operator.subtasks?.[0]?.actor_id;
  return actorId ? (
    <Link
      component={RouterLink}
      onClick={(event) => event.stopPropagation()}
      onKeyDown={(event) => event.stopPropagation()}
      title={`Open Ray actor ${actorId}`}
      to={generateActorLink(actorId)}
    >
      {label}
    </Link>
  ) : (
    <Typography color="text.disabled" component="span" variant="caption">
      Actor unavailable
    </Typography>
  );
};

const PercentageCell = ({
  value,
  detail,
}: {
  value: number;
  detail?: string;
}) => (
  <TableCell sx={{ minWidth: 110 }}>
    <Typography variant="caption">
      {(value ?? 0).toFixed(1)}%{detail ? ` · ${detail}` : ""}
    </Typography>
    <LinearProgress
      color={value >= 50 ? "error" : "primary"}
      sx={{ height: 4, borderRadius: 2 }}
      value={value ?? 0}
      variant="determinate"
    />
  </TableCell>
);

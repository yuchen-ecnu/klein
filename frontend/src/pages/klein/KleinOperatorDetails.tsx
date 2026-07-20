import {
  Box,
  Button,
  LinearProgress,
  Paper,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography,
} from "@mui/material";
import React from "react";
import { RiExternalLinkLine } from "react-icons/ri";
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
import { KleinStatCard } from "./KleinStatCard";
import { KleinOperatorRescaleControl } from "./KleinOperatorRescaleControl";

export const KleinOperatorDetails = ({
  operator,
  inDrawer = false,
  jobId,
  onRefresh,
}: {
  operator?: KleinOperator;
  inDrawer?: boolean;
  jobId?: string;
  onRefresh: () => Promise<unknown>;
}) => {
  if (!operator) {
    return (
      <Paper variant="outlined" sx={{ marginTop: 2, padding: 3 }}>
        <Typography color="text.secondary">
          Select an operator in the DAG or table to inspect its task instances.
        </Typography>
      </Paper>
    );
  }
  return (
    <Paper
      variant="outlined"
      sx={{
        border: inDrawer ? 0 : undefined,
        borderRadius: inDrawer ? 0 : undefined,
        marginTop: inDrawer ? 0 : 2,
        padding: inDrawer ? 3 : 2,
      }}
    >
      <Stack alignItems="center" direction="row" spacing={1.5}>
        <Box sx={{ flex: 1 }}>
          <Typography sx={{ fontWeight: 600 }} variant="h6">
            {operator.name}
          </Typography>
          <Typography color="text.secondary" variant="body2">
            Operator {operator.op_id} · {operator.parallelism} task instance
            {operator.parallelism === 1 ? "" : "s"} · {operator.cpus} CPU /{" "}
            {operator.gpus} GPU each
          </Typography>
        </Box>
        {operator.parallelism === 1 && (
          <OperatorActorButton operator={operator} />
        )}
        <StatusChip type="kleinOperator" status={operator.status} />
      </Stack>
      <KleinOperatorRescaleControl
        jobId={jobId}
        onRefresh={onRefresh}
        operator={operator}
      />
      <Stack direction="row" flexWrap="wrap" gap={2} sx={{ marginTop: 2 }}>
        <KleinStatCard
          label="Records in / out"
          value={`${formatCount(operator.rows_in)} / ${formatCount(
            operator.rows_out,
          )}`}
          detail={`${formatRate(operator.rows_in_per_second)} / ${formatRate(
            operator.rows_out_per_second,
          )} records/s`}
        />
        <KleinStatCard
          label="Estimated data in / out"
          value={`${formatBytes(operator.bytes_in)} / ${formatBytes(
            operator.bytes_out,
          )}`}
          detail={`${formatByteRate(
            operator.bytes_in_per_second,
          )} / ${formatByteRate(operator.bytes_out_per_second)}`}
        />
        <KleinStatCard
          label="Busy / backpressure"
          value={`${(operator.busy_percent ?? 0).toFixed(1)}% / ${(
            operator.backpressure_percent ?? 0
          ).toFixed(1)}%`}
          detail={`${operator.backpressure_events ?? 0} backpressure events`}
        />
        <KleinStatCard
          label="Input queue"
          value={`${operator.queued} / ${operator.capacity || "∞"}`}
          detail={`${operator.instances.running}/${operator.parallelism} tasks running`}
        />
        <KleinStatCard
          label="Latest checkpoint"
          value={
            operator.last_checkpoint_id !== undefined
              ? `#${operator.last_checkpoint_id}`
              : "-"
          }
          detail={`${formatBytes(
            operator.checkpoint_state_size_bytes ?? 0,
          )} · align ${(operator.checkpoint_alignment_ms ?? 0).toFixed(
            1,
          )} ms · barrier ${(
            operator.checkpoint_barrier_latency_ms ?? 0
          ).toFixed(1)} ms`}
        />
      </Stack>
      <Typography sx={{ fontWeight: 600, marginTop: 3 }} variant="subtitle1">
        Task instances
      </Typography>
      {inDrawer ? (
        <DrawerTaskInstances operator={operator} />
      ) : (
        <TableContainer sx={{ marginTop: 1 }}>
          <Table size="small">
            <TableHead>
              <TableRow>
                <TableCell>Subtask</TableCell>
                <TableCell>Status</TableCell>
                <TableCell align="right">Records in / out</TableCell>
                <TableCell align="right">Records/s in / out</TableCell>
                <TableCell align="right">Estimated data/s in / out</TableCell>
                <TableCell>Busy</TableCell>
                <TableCell>Backpressure</TableCell>
                <TableCell align="right">Queue</TableCell>
                <TableCell>Checkpoint</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {(operator.subtasks ?? []).map((subtask) => (
                <TableRow key={subtask.subtask_index}>
                  <TableCell>
                    #{subtask.subtask_index}
                    <SubtaskActorLink actorId={subtask.actor_id} />
                  </TableCell>
                  <TableCell>
                    <StatusChip type="kleinOperator" status={subtask.status} />
                  </TableCell>
                  <TableCell align="right">
                    {formatCount(subtask.rows_in)} /{" "}
                    {formatCount(subtask.rows_out)}
                  </TableCell>
                  <TableCell align="right">
                    {formatRate(subtask.rows_in_per_second)} /{" "}
                    {formatRate(subtask.rows_out_per_second)}
                  </TableCell>
                  <TableCell align="right">
                    {formatByteRate(subtask.bytes_in_per_second)} /{" "}
                    {formatByteRate(subtask.bytes_out_per_second)}
                  </TableCell>
                  <PercentageCell value={subtask.busy_percent} />
                  <PercentageCell
                    detail={`${subtask.backpressure_events ?? 0} events`}
                    value={subtask.backpressure_percent}
                  />
                  <TableCell align="right">
                    {subtask.queued} / {subtask.capacity || "∞"}
                  </TableCell>
                  <TableCell>
                    {subtask.last_checkpoint_id !== undefined
                      ? `#${subtask.last_checkpoint_id}`
                      : "-"}
                    <Typography
                      color="text.secondary"
                      display="block"
                      variant="caption"
                    >
                      {formatBytes(subtask.checkpoint_state_size_bytes ?? 0)} ·{" "}
                      {(subtask.checkpoint_alignment_ms ?? 0).toFixed(1)} ms
                      align
                    </Typography>
                  </TableCell>
                </TableRow>
              ))}
              {(operator.subtasks ?? []).length === 0 && (
                <TableRow>
                  <TableCell align="center" colSpan={9}>
                    Task metrics are not available yet.
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
        </TableContainer>
      )}
    </Paper>
  );
};

const OperatorActorButton = ({ operator }: { operator: KleinOperator }) => {
  const actorId = operator.subtasks?.[0]?.actor_id;
  return actorId ? (
    <Button
      component={RouterLink}
      size="small"
      startIcon={<RiExternalLinkLine />}
      title={`Open Ray actor ${actorId}`}
      to={generateActorLink(actorId)}
      variant="outlined"
    >
      View actor
    </Button>
  ) : (
    <Button disabled size="small" variant="outlined">
      Actor unavailable
    </Button>
  );
};

const DrawerTaskInstances = ({ operator }: { operator: KleinOperator }) => (
  <TableContainer sx={{ marginTop: 1 }}>
    <Table size="small" sx={{ tableLayout: "fixed" }}>
      <TableHead>
        <TableRow>
          <TableCell sx={{ width: "15%" }}>Subtask</TableCell>
          <TableCell sx={{ width: "22%" }}>Records</TableCell>
          <TableCell sx={{ width: "23%" }}>Load</TableCell>
          <TableCell align="right" sx={{ width: "14%" }}>
            Queue
          </TableCell>
          <TableCell sx={{ width: "26%" }}>Checkpoint</TableCell>
        </TableRow>
      </TableHead>
      <TableBody>
        {(operator.subtasks ?? []).map((subtask) => (
          <TableRow key={subtask.subtask_index}>
            <TableCell>
              <Typography variant="body2">#{subtask.subtask_index}</Typography>
              <SubtaskActorLink actorId={subtask.actor_id} />
              <StatusChip type="kleinOperator" status={subtask.status} />
            </TableCell>
            <TableCell>
              <Typography variant="body2">
                {formatCount(subtask.rows_in)} / {formatCount(subtask.rows_out)}
              </Typography>
              <Typography color="text.secondary" variant="caption">
                {formatRate(subtask.rows_in_per_second)} /{" "}
                {formatRate(subtask.rows_out_per_second)} records/s
              </Typography>
              <Typography
                color="text.secondary"
                display="block"
                variant="caption"
              >
                {formatByteRate(subtask.bytes_in_per_second)} /{" "}
                {formatByteRate(subtask.bytes_out_per_second)}
              </Typography>
            </TableCell>
            <TableCell>
              <CompactPercentage label="Busy" value={subtask.busy_percent} />
              <CompactPercentage
                label="BP"
                value={subtask.backpressure_percent}
              />
            </TableCell>
            <TableCell align="right">
              {subtask.queued} / {subtask.capacity || "∞"}
            </TableCell>
            <TableCell>
              {subtask.last_checkpoint_id !== undefined
                ? `#${subtask.last_checkpoint_id}`
                : "-"}
              <Typography
                color="text.secondary"
                display="block"
                variant="caption"
              >
                {formatBytes(subtask.checkpoint_state_size_bytes ?? 0)} ·{" "}
                {(subtask.checkpoint_alignment_ms ?? 0).toFixed(1)} ms align
              </Typography>
            </TableCell>
          </TableRow>
        ))}
        {(operator.subtasks ?? []).length === 0 && (
          <TableRow>
            <TableCell align="center" colSpan={5}>
              Task metrics are not available yet.
            </TableCell>
          </TableRow>
        )}
      </TableBody>
    </Table>
  </TableContainer>
);

const CompactPercentage = ({
  label,
  value,
}: {
  label: string;
  value: number;
}) => (
  <Box sx={{ marginTop: 0.5 }}>
    <Stack direction="row" justifyContent="space-between">
      <Typography color="text.secondary" variant="caption">
        {label}
      </Typography>
      <Typography variant="caption">{(value ?? 0).toFixed(1)}%</Typography>
    </Stack>
    <LinearProgress
      color={(value ?? 0) >= 50 ? "error" : "primary"}
      sx={{ borderRadius: 2, height: 4 }}
      value={value ?? 0}
      variant="determinate"
    />
  </Box>
);

const SubtaskActorLink = ({ actorId }: { actorId?: string | null }) =>
  actorId ? (
    <Button
      component={RouterLink}
      size="small"
      sx={{ marginTop: 0.5, whiteSpace: "nowrap" }}
      title={`Open Ray actor ${actorId}`}
      to={generateActorLink(actorId)}
      variant="outlined"
    >
      View actor
    </Button>
  ) : (
    <Button
      disabled
      size="small"
      sx={{ marginTop: 0.5, whiteSpace: "nowrap" }}
      variant="outlined"
    >
      Actor unavailable
    </Button>
  );

const PercentageCell = ({
  value,
  detail,
}: {
  value: number;
  detail?: string;
}) => (
  <TableCell sx={{ minWidth: 120 }}>
    <Typography variant="caption">
      {(value ?? 0).toFixed(1)}%{detail ? ` · ${detail}` : ""}
    </Typography>
    <LinearProgress
      color={(value ?? 0) >= 50 ? "error" : "primary"}
      sx={{ borderRadius: 2, height: 4 }}
      value={value ?? 0}
      variant="determinate"
    />
  </TableCell>
);

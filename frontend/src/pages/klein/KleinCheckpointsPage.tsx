import {
  Alert,
  Box,
  Collapse,
  IconButton,
  Paper,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TablePagination,
  TableRow,
  TableSortLabel,
  Typography,
} from "@mui/material";
import React, { Fragment, useEffect, useMemo, useState } from "react";
import { RiArrowDownSLine, RiArrowRightSLine } from "react-icons/ri";
import { useParams } from "react-router-dom";
import { formatDateFromTimeMs } from "../../common/formatUtils";
import { Order } from "../../common/tableUtils";
import Loading from "../../components/Loading";
import { StatusChip } from "../../components/StatusChip";
import { KleinCheckpoint, KleinCheckpointOperator } from "../../type/klein";
import { useKleinJob } from "./hook/useKleinJobs";
import { formatBytes, formatCount } from "./KleinFormatUtils";
import { KleinStatCard } from "./KleinStatCard";

type CheckpointSortKey =
  | "id"
  | "status"
  | "triggered"
  | "duration"
  | "stateSize"
  | "alignment"
  | "barrierLatency"
  | "acknowledgements"
  | "reason";

type OperatorSortKey =
  | "operator"
  | "stateSize"
  | "alignment"
  | "barrierLatency"
  | "subtasks";

type SortValue = number | string | null | undefined;

const EMPTY_CHECKPOINTS: KleinCheckpoint[] = [];
const EMPTY_OPERATORS: KleinCheckpointOperator[] = [];

const checkpointSortValue: Record<
  CheckpointSortKey,
  (checkpoint: KleinCheckpoint) => SortValue
> = {
  id: (checkpoint) => checkpoint.id,
  status: (checkpoint) => checkpoint.status,
  triggered: (checkpoint) => checkpoint.triggered_at_ms,
  duration: (checkpoint) => checkpoint.duration_ms,
  stateSize: (checkpoint) => checkpoint.state_size_bytes,
  alignment: (checkpoint) => checkpoint.alignment_duration_ms,
  barrierLatency: (checkpoint) => checkpoint.barrier_latency_ms,
  acknowledgements: (checkpoint) => checkpoint.acknowledged,
  reason: (checkpoint) => checkpoint.reason,
};

const operatorSortValue: Record<
  OperatorSortKey,
  (operator: KleinCheckpointOperator) => SortValue
> = {
  operator: (operator) => operator.name,
  stateSize: (operator) => operator.state_size_bytes,
  alignment: (operator) => operator.alignment_duration_ms,
  barrierLatency: (operator) => operator.barrier_latency_ms,
  subtasks: (operator) => operator.subtasks?.length ?? 0,
};

const compareSortValues = (left: SortValue, right: SortValue, order: Order) => {
  if (left === undefined || left === null) {
    return right === undefined || right === null ? 0 : 1;
  }
  if (right === undefined || right === null) {
    return -1;
  }
  let comparison: number;
  if (typeof left === "string" && typeof right === "string") {
    comparison = left.localeCompare(right, undefined, { sensitivity: "base" });
  } else {
    comparison = left < right ? -1 : left > right ? 1 : 0;
  }
  return order === "asc" ? comparison : -comparison;
};

const sortRows = <Row, Key extends string>(
  rows: Row[],
  sortBy: Key,
  order: Order,
  accessors: Record<Key, (row: Row) => SortValue>,
) =>
  rows
    .map((row, index) => ({ row, index }))
    .sort((left, right) => {
      const comparison = compareSortValues(
        accessors[sortBy](left.row),
        accessors[sortBy](right.row),
        order,
      );
      return comparison === 0 ? left.index - right.index : comparison;
    })
    .map(({ row }) => row);

type SortableHeaderProps<Key extends string> = {
  align?: "left" | "right";
  children: React.ReactNode;
  column: Key;
  onSort: (column: Key) => void;
  order: Order;
  sortBy: Key;
};

const SortableHeader = <Key extends string>({
  align = "left",
  children,
  column,
  onSort,
  order,
  sortBy,
}: SortableHeaderProps<Key>) => (
  <TableCell align={align} sortDirection={sortBy === column ? order : false}>
    <TableSortLabel
      active={sortBy === column}
      direction={sortBy === column ? order : "asc"}
      onClick={() => onSort(column)}
    >
      {children}
    </TableSortLabel>
  </TableCell>
);

export const KleinCheckpointsPage = () => {
  const { jobId } = useParams();
  const { job, error, isLoading } = useKleinJob(jobId);
  const [selectedCheckpointId, setSelectedCheckpointId] =
    useState<number | null>();
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(10);
  const [sortBy, setSortBy] = useState<CheckpointSortKey>("id");
  const [order, setOrder] = useState<Order>("desc");
  const history = job?.checkpoints.history ?? EMPTY_CHECKPOINTS;
  const sortedHistory = useMemo(
    () => sortRows(history, sortBy, order, checkpointSortValue),
    [history, order, sortBy],
  );
  useEffect(() => {
    if (selectedCheckpointId === null) {
      return;
    }
    if (
      job &&
      job.checkpoints.history.length > 0 &&
      !job.checkpoints.history.some(
        (checkpoint) => checkpoint.id === selectedCheckpointId,
      )
    ) {
      setSelectedCheckpointId(
        (
          job.checkpoints.history.find(
            (checkpoint) => checkpoint.status === "COMPLETED",
          ) ?? job.checkpoints.history[0]
        ).id,
      );
    }
  }, [job, selectedCheckpointId]);
  if (isLoading) {
    return <Loading loading />;
  }
  if (error || !job) {
    return (
      <Alert severity="error" sx={{ margin: 3 }}>
        Unable to load checkpoints.
      </Alert>
    );
  }
  const { summary, latest_path: latestPath } = job.checkpoints;
  const latestCompletedCheckpoint = history.find(
    (checkpoint) => checkpoint.status === "COMPLETED",
  );
  const maxPage = Math.max(0, Math.ceil(history.length / rowsPerPage) - 1);
  const constrainedPage = Math.min(page, maxPage);
  const visibleHistory = sortedHistory.slice(
    constrainedPage * rowsPerPage,
    constrainedPage * rowsPerPage + rowsPerPage,
  );
  const handleSort = (column: CheckpointSortKey) => {
    setOrder(sortBy === column && order === "asc" ? "desc" : "asc");
    setSortBy(column);
    setPage(0);
  };
  return (
    <Box sx={{ padding: 3 }}>
      <Typography sx={{ fontWeight: 500 }} variant="h4">
        Checkpoints
      </Typography>
      <Typography color="text.secondary" sx={{ marginTop: 0.5 }}>
        Barrier alignment, durable snapshots, and managed state retained for
        recovery. Select a checkpoint to inspect every operator and subtask.
      </Typography>
      {job.checkpoints.error && (
        <Alert severity="warning" sx={{ marginTop: 2 }}>
          {job.checkpoints.error}
        </Alert>
      )}
      <Stack direction="row" flexWrap="wrap" gap={2} sx={{ marginTop: 3 }}>
        <KleinStatCard label="Completed" value={summary.completed} />
        <KleinStatCard label="In progress" value={summary.in_progress} />
        <KleinStatCard label="Failed" value={summary.failed} />
        <KleinStatCard
          label="Managed state"
          value={formatBytes(summary.state_size_bytes ?? 0)}
        />
      </Stack>
      <Paper variant="outlined" sx={{ marginTop: 3, padding: 2 }}>
        <Typography sx={{ fontWeight: 500 }} variant="h6">
          Latest completed checkpoint
        </Typography>
        <Typography
          component="code"
          sx={{
            color: "text.secondary",
            display: "block",
            marginTop: 1,
            overflowWrap: "anywhere",
          }}
        >
          {latestPath ??
            (latestCompletedCheckpoint
              ? `Checkpoint #${latestCompletedCheckpoint.id} · ${formatBytes(
                  latestCompletedCheckpoint.state_size_bytes ?? 0,
                )} managed state`
              : "No checkpoint has completed yet.")}
        </Typography>
      </Paper>
      <Paper variant="outlined" sx={{ marginTop: 3 }}>
        <TableContainer>
          <Table>
            <TableHead>
              <TableRow>
                <TableCell padding="checkbox" />
                <SortableHeader
                  column="id"
                  onSort={handleSort}
                  order={order}
                  sortBy={sortBy}
                >
                  ID
                </SortableHeader>
                <SortableHeader
                  column="status"
                  onSort={handleSort}
                  order={order}
                  sortBy={sortBy}
                >
                  Status
                </SortableHeader>
                <SortableHeader
                  column="triggered"
                  onSort={handleSort}
                  order={order}
                  sortBy={sortBy}
                >
                  Triggered
                </SortableHeader>
                <SortableHeader
                  align="right"
                  column="duration"
                  onSort={handleSort}
                  order={order}
                  sortBy={sortBy}
                >
                  Duration
                </SortableHeader>
                <SortableHeader
                  align="right"
                  column="stateSize"
                  onSort={handleSort}
                  order={order}
                  sortBy={sortBy}
                >
                  State size
                </SortableHeader>
                <SortableHeader
                  align="right"
                  column="alignment"
                  onSort={handleSort}
                  order={order}
                  sortBy={sortBy}
                >
                  Max alignment
                </SortableHeader>
                <SortableHeader
                  align="right"
                  column="barrierLatency"
                  onSort={handleSort}
                  order={order}
                  sortBy={sortBy}
                >
                  Barrier latency
                </SortableHeader>
                <SortableHeader
                  align="right"
                  column="acknowledgements"
                  onSort={handleSort}
                  order={order}
                  sortBy={sortBy}
                >
                  Acknowledgements
                </SortableHeader>
                <SortableHeader
                  column="reason"
                  onSort={handleSort}
                  order={order}
                  sortBy={sortBy}
                >
                  Failure reason
                </SortableHeader>
              </TableRow>
            </TableHead>
            <TableBody>
              {visibleHistory.map((checkpoint) => {
                const expanded = selectedCheckpointId === checkpoint.id;
                return (
                  <Fragment key={checkpoint.id}>
                    <TableRow
                      hover
                      onClick={() =>
                        setSelectedCheckpointId(expanded ? null : checkpoint.id)
                      }
                      selected={expanded}
                      sx={{ cursor: "pointer" }}
                    >
                      <TableCell padding="checkbox">
                        <IconButton
                          aria-label={`${
                            expanded ? "Collapse" : "Expand"
                          } checkpoint ${checkpoint.id}`}
                          size="small"
                        >
                          {expanded ? (
                            <RiArrowDownSLine />
                          ) : (
                            <RiArrowRightSLine />
                          )}
                        </IconButton>
                      </TableCell>
                      <TableCell>#{checkpoint.id}</TableCell>
                      <TableCell>
                        <StatusChip
                          type="kleinCheckpoint"
                          status={checkpoint.status}
                        />
                      </TableCell>
                      <TableCell>
                        {checkpoint.triggered_at_ms
                          ? formatDateFromTimeMs(checkpoint.triggered_at_ms)
                          : "-"}
                      </TableCell>
                      <TableCell align="right">
                        {checkpoint.duration_ms !== undefined
                          ? formatDuration(checkpoint.duration_ms)
                          : "-"}
                      </TableCell>
                      <TableCell align="right">
                        {formatBytes(checkpoint.state_size_bytes ?? 0)}
                      </TableCell>
                      <TableCell align="right">
                        {formatMillis(checkpoint.alignment_duration_ms)}
                      </TableCell>
                      <TableCell align="right">
                        {formatMillis(checkpoint.barrier_latency_ms)}
                      </TableCell>
                      <TableCell align="right">
                        {checkpoint.acknowledged} /{" "}
                        {checkpoint.required_acknowledgements}
                      </TableCell>
                      <TableCell>{checkpoint.reason ?? "-"}</TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell
                        colSpan={10}
                        sx={{ paddingBottom: 0, paddingTop: 0 }}
                      >
                        <Collapse in={expanded} timeout="auto" unmountOnExit>
                          <CheckpointDetails checkpoint={checkpoint} />
                        </Collapse>
                      </TableCell>
                    </TableRow>
                  </Fragment>
                );
              })}
              {history.length === 0 && (
                <TableRow>
                  <TableCell align="center" colSpan={10}>
                    No checkpoints recorded.
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
        </TableContainer>
        <TablePagination
          component="div"
          count={history.length}
          onPageChange={(_, nextPage) => setPage(nextPage)}
          onRowsPerPageChange={(event) => {
            setRowsPerPage(Number(event.target.value));
            setPage(0);
          }}
          page={constrainedPage}
          rowsPerPage={rowsPerPage}
          rowsPerPageOptions={[10, 25, 50]}
        />
      </Paper>
    </Box>
  );
};

const CheckpointDetails = ({ checkpoint }: { checkpoint: KleinCheckpoint }) => {
  const operators = checkpoint.operators ?? EMPTY_OPERATORS;
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(5);
  const [sortBy, setSortBy] = useState<OperatorSortKey>("operator");
  const [order, setOrder] = useState<Order>("asc");
  const sortedOperators = useMemo(
    () => sortRows(operators, sortBy, order, operatorSortValue),
    [operators, order, sortBy],
  );
  const maxPage = Math.max(0, Math.ceil(operators.length / rowsPerPage) - 1);
  const constrainedPage = Math.min(page, maxPage);
  const visibleOperators = sortedOperators.slice(
    constrainedPage * rowsPerPage,
    constrainedPage * rowsPerPage + rowsPerPage,
  );
  const handleSort = (column: OperatorSortKey) => {
    setOrder(sortBy === column && order === "asc" ? "desc" : "asc");
    setSortBy(column);
    setPage(0);
  };

  return (
    <Box sx={{ backgroundColor: "#F7FAFC", margin: 1, padding: 2 }}>
      <Stack alignItems="baseline" direction="row" spacing={2}>
        <Typography sx={{ fontWeight: 600 }} variant="subtitle1">
          Operator checkpoint details
        </Typography>
        <Typography color="text.secondary" variant="body2">
          {operators.length} operators reported
        </Typography>
      </Stack>
      <TableContainer
        component={Paper}
        sx={{ marginTop: 1 }}
        variant="outlined"
      >
        <Table size="small">
          <TableHead>
            <TableRow>
              <SortableHeader
                column="operator"
                onSort={handleSort}
                order={order}
                sortBy={sortBy}
              >
                Operator
              </SortableHeader>
              <SortableHeader
                align="right"
                column="stateSize"
                onSort={handleSort}
                order={order}
                sortBy={sortBy}
              >
                State size
              </SortableHeader>
              <SortableHeader
                align="right"
                column="alignment"
                onSort={handleSort}
                order={order}
                sortBy={sortBy}
              >
                Max alignment
              </SortableHeader>
              <SortableHeader
                align="right"
                column="barrierLatency"
                onSort={handleSort}
                order={order}
                sortBy={sortBy}
              >
                Max barrier latency
              </SortableHeader>
              <SortableHeader
                column="subtasks"
                onSort={handleSort}
                order={order}
                sortBy={sortBy}
              >
                Subtask breakdown
              </SortableHeader>
            </TableRow>
          </TableHead>
          <TableBody>
            {visibleOperators.map((operator) => (
              <TableRow key={operator.op_id}>
                <TableCell>
                  <Typography sx={{ fontWeight: 500 }} variant="body2">
                    {operator.name}
                  </Typography>
                  <Typography color="text.secondary" variant="caption">
                    Operator {operator.op_id}
                  </Typography>
                </TableCell>
                <TableCell align="right">
                  {formatBytes(operator.state_size_bytes ?? 0)}
                </TableCell>
                <TableCell align="right">
                  {formatMillis(operator.alignment_duration_ms)}
                </TableCell>
                <TableCell align="right">
                  {formatMillis(operator.barrier_latency_ms)}
                </TableCell>
                <TableCell>
                  <Stack spacing={0.5}>
                    {(operator.subtasks ?? []).map((subtask) => (
                      <Typography key={subtask.subtask_index} variant="caption">
                        #{subtask.subtask_index}:{" "}
                        {formatBytes(subtask.state_size_bytes ?? 0)} · align{" "}
                        {formatMillis(subtask.alignment_duration_ms)} · barrier{" "}
                        {formatMillis(subtask.barrier_latency_ms)} · rows{" "}
                        {formatCount(subtask.rows_in ?? 0)} /{" "}
                        {formatCount(subtask.rows_out ?? 0)}
                      </Typography>
                    ))}
                  </Stack>
                </TableCell>
              </TableRow>
            ))}
            {operators.length === 0 && (
              <TableRow>
                <TableCell align="center" colSpan={5}>
                  Per-operator metrics have not arrived for this checkpoint.
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
        <TablePagination
          component="div"
          count={operators.length}
          onPageChange={(_, nextPage) => setPage(nextPage)}
          onRowsPerPageChange={(event) => {
            setRowsPerPage(Number(event.target.value));
            setPage(0);
          }}
          page={constrainedPage}
          rowsPerPage={rowsPerPage}
          rowsPerPageOptions={[5, 10, 25]}
        />
      </TableContainer>
    </Box>
  );
};

const formatMillis = (value?: number) => `${(value ?? 0).toFixed(1)} ms`;
const formatDuration = (value: number) =>
  value >= 1000 ? `${(value / 1000).toFixed(2)} s` : `${value} ms`;

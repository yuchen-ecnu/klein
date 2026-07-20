import {
  Alert,
  Box,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Drawer,
  IconButton,
  Paper,
  Stack,
  Typography,
} from "@mui/material";
import React, { useState } from "react";
import { RiCloseLine } from "react-icons/ri";
import { useParams } from "react-router-dom";
import { formatDuration } from "../../common/formatUtils";
import Loading from "../../components/Loading";
import { StatusChip } from "../../components/StatusChip";
import { useKleinJob } from "./hook/useKleinJobs";
import { formatBytes, formatCount } from "./KleinFormatUtils";
import { KleinJobGraph } from "./KleinJobGraph";
import { KleinOperatorDetails } from "./KleinOperatorDetails";
import { KleinOperatorsTable } from "./KleinOperatorsTable";
import { KleinStatCard } from "./KleinStatCard";

export const KleinJobOverviewPage = () => {
  const { jobId } = useParams();
  const { job, error, isLoading, cancel, refresh } = useKleinJob(jobId);
  const [confirmCancel, setConfirmCancel] = useState(false);
  const [cancelError, setCancelError] = useState<unknown>();
  const [highlightedOperatorId, setHighlightedOperatorId] = useState<number>();
  const [detailsOperatorId, setDetailsOperatorId] = useState<number>();
  if (isLoading) {
    return <Loading loading />;
  }
  if (error || !job) {
    return (
      <Alert severity="error" sx={{ margin: 3 }}>
        Unable to load Klein job: {String(error ?? "job not found")}
      </Alert>
    );
  }
  const checkpointSummary = job.checkpoints.summary;
  const selectedOperator = job.operators.find(
    (operator) => operator.op_id === detailsOperatorId,
  );
  const openOperatorDetails = (operatorId: number) => {
    setHighlightedOperatorId(operatorId);
    setDetailsOperatorId(operatorId);
  };
  const handleCancel = async () => {
    try {
      await cancel();
      setConfirmCancel(false);
    } catch (cancelRequestError) {
      setCancelError(cancelRequestError);
    }
  };
  return (
    <Box sx={{ padding: 3 }}>
      <Stack alignItems="center" direction="row" spacing={2}>
        <Box sx={{ flex: 1 }}>
          <Stack alignItems="center" direction="row" spacing={1.5}>
            <Typography sx={{ fontWeight: 500 }} variant="h4">
              {job.job_name}
            </Typography>
            <StatusChip type="kleinJob" status={job.status} />
          </Stack>
          <Typography color="text.secondary" sx={{ marginTop: 0.5 }}>
            {job.job_id} · {job.runtime_mode} ·{" "}
            {formatDuration(job.duration_ms / 1000)}
          </Typography>
        </Box>
        {job.status === "RUNNING" && (
          <Button
            color="error"
            onClick={() => setConfirmCancel(true)}
            variant="outlined"
          >
            Cancel job
          </Button>
        )}
      </Stack>
      {job.dashboard_stale && (
        <Alert severity="warning" sx={{ marginTop: 2 }}>
          Showing the last successful snapshot because the JobManager is
          unavailable. {job.dashboard_error}
        </Alert>
      )}
      {job.failure && (
        <Alert severity="error" sx={{ marginTop: 2, whiteSpace: "pre-wrap" }}>
          {job.failure}
        </Alert>
      )}
      {Boolean(cancelError) && (
        <Alert
          onClose={() => setCancelError(undefined)}
          severity="error"
          sx={{ marginTop: 2 }}
        >
          Unable to cancel job: {String(cancelError)}
        </Alert>
      )}
      <Stack direction="row" flexWrap="wrap" gap={2} sx={{ marginTop: 3 }}>
        <KleinStatCard
          label="Records in / out"
          value={`${formatCount(job.overview.rows_in)} / ${formatCount(
            job.overview.rows_out,
          )}`}
        />
        <KleinStatCard
          label="Estimated data in / out"
          value={`${formatBytes(job.overview.bytes_in)} / ${formatBytes(
            job.overview.bytes_out,
          )}`}
        />
        <KleinStatCard
          label="Operators / tasks"
          value={`${job.overview.operators} / ${job.overview.task_instances}`}
        />
        <KleinStatCard
          label="Checkpoints"
          value={checkpointSummary.completed}
          detail={`${checkpointSummary.failed} failed · ${checkpointSummary.in_progress} in progress`}
        />
        <KleinStatCard
          label="Restarts"
          value={job.overview.restarts}
          detail={`Limit ${job.overview.max_restarts} per ${job.overview.restart_window_seconds}s`}
        />
      </Stack>
      <Section title="Execution graph">
        <KleinJobGraph
          edges={job.edges}
          highlightedOperatorId={highlightedOperatorId}
          onHighlightOperator={setHighlightedOperatorId}
          onOpenOperatorDetails={openOperatorDetails}
          operators={job.operators}
        />
      </Section>
      <Section title="Operators">
        <KleinOperatorsTable
          onSelectOperator={openOperatorDetails}
          operators={job.operators}
          rayNamespace={job.namespace}
          selectedOperatorId={highlightedOperatorId}
        />
      </Section>
      <Drawer
        anchor="right"
        ModalProps={{ keepMounted: true }}
        onClose={() => setDetailsOperatorId(undefined)}
        open={Boolean(selectedOperator)}
        PaperProps={{
          sx: {
            maxWidth: "calc(100vw - 64px)",
            width: { xs: "100%", md: 880 },
          },
        }}
      >
        <Box sx={{ minHeight: "100%" }}>
          <Stack
            alignItems="center"
            direction="row"
            sx={{
              backgroundColor: "background.paper",
              borderBottom: "1px solid",
              borderColor: "divider",
              padding: 2,
              position: "sticky",
              top: 0,
              zIndex: 1,
            }}
          >
            <Box sx={{ flex: 1 }}>
              <Typography sx={{ fontWeight: 600 }} variant="h6">
                Operator details
              </Typography>
              <Typography color="text.secondary" variant="body2">
                Runtime metrics, subtasks, pressure, and checkpoint state.
              </Typography>
            </Box>
            <IconButton
              aria-label="Close operator details"
              onClick={() => setDetailsOperatorId(undefined)}
            >
              <RiCloseLine />
            </IconButton>
          </Stack>
          <KleinOperatorDetails
            inDrawer
            jobId={job.job_id}
            onRefresh={refresh}
            operator={selectedOperator}
            rayNamespace={job.namespace}
          />
        </Box>
      </Drawer>
      <Dialog onClose={() => setConfirmCancel(false)} open={confirmCancel}>
        <DialogTitle>Cancel {job.job_name}?</DialogTitle>
        <DialogContent>
          Cancellation stops all task actors. Already completed durable
          checkpoints remain available for recovery. This action cannot be
          undone.
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setConfirmCancel(false)}>Keep running</Button>
          <Button color="error" onClick={handleCancel} variant="contained">
            Cancel job
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

const Section = ({
  title,
  children,
}: {
  title: string;
  children: React.ReactNode;
}) => (
  <Paper variant="outlined" sx={{ marginTop: 3, padding: 2 }}>
    <Typography sx={{ fontWeight: 500, marginBottom: 2 }} variant="h6">
      {title}
    </Typography>
    {children}
  </Paper>
);

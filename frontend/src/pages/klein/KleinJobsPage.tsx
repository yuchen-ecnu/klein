import {
  Alert,
  Box,
  Link,
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
import { Link as RouterLink } from "react-router-dom";
import { formatDateFromTimeMs, formatDuration } from "../../common/formatUtils";
import Loading from "../../components/Loading";
import { StatusChip } from "../../components/StatusChip";
import { useKleinJobs } from "./hook/useKleinJobs";
import { KleinStatCard } from "./KleinStatCard";

export const KleinJobsPage = () => {
  const { jobs, error, isLoading } = useKleinJobs();
  if (isLoading) {
    return <Loading loading />;
  }
  const running = jobs.filter((job) => job.status === "RUNNING").length;
  const failed = jobs.filter((job) => job.status === "FAILED").length;
  const taskInstances = jobs
    .filter((job) => job.status === "RUNNING")
    .reduce((total, job) => total + job.overview.task_instances, 0);
  return (
    <Box sx={{ padding: 3 }}>
      <Typography sx={{ fontWeight: 500 }} variant="h4">
        Klein streaming jobs
      </Typography>
      <Typography color="text.secondary" sx={{ marginTop: 0.5 }}>
        Stateful streaming jobs running on this Ray cluster.
      </Typography>
      {error && (
        <Alert severity="error" sx={{ marginTop: 2 }}>
          Unable to load Klein jobs: {String(error)}
        </Alert>
      )}
      <Stack direction="row" flexWrap="wrap" gap={2} sx={{ marginTop: 3 }}>
        <KleinStatCard label="Running jobs" value={running} />
        <KleinStatCard
          label="Task instances"
          value={taskInstances}
          detail="Across running jobs"
        />
        <KleinStatCard label="Failed jobs" value={failed} />
        <KleinStatCard
          label="Retained jobs"
          value={jobs.length}
          detail="Running and completed"
        />
      </Stack>
      <Paper variant="outlined" sx={{ marginTop: 3 }}>
        {jobs.length === 0 ? (
          <Box sx={{ padding: 6, textAlign: "center" }}>
            <Typography variant="h6">No Klein jobs found</Typography>
            <Typography color="text.secondary" sx={{ marginTop: 1 }}>
              A streaming job appears here after ray.klein.execute() submits it.
            </Typography>
          </Box>
        ) : (
          <TableContainer>
            <Table>
              <TableHead>
                <TableRow>
                  <TableCell>Job</TableCell>
                  <TableCell>Status</TableCell>
                  <TableCell>Mode</TableCell>
                  <TableCell>Start time</TableCell>
                  <TableCell>Duration</TableCell>
                  <TableCell align="right">Operators / tasks</TableCell>
                  <TableCell align="right">Restarts</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {jobs.map((job) => (
                  <TableRow hover key={job.job_id}>
                    <TableCell>
                      <Link
                        component={RouterLink}
                        sx={{ fontWeight: 500 }}
                        to={`jobs/${encodeURIComponent(job.job_id)}`}
                      >
                        {job.job_name}
                      </Link>
                      <Typography
                        color="text.secondary"
                        variant="caption"
                        display="block"
                      >
                        {job.job_id}
                      </Typography>
                    </TableCell>
                    <TableCell>
                      <StatusChip type="kleinJob" status={job.status} />
                    </TableCell>
                    <TableCell>{job.runtime_mode}</TableCell>
                    <TableCell>
                      {job.started_at_ms
                        ? formatDateFromTimeMs(job.started_at_ms)
                        : "-"}
                    </TableCell>
                    <TableCell>
                      {formatDuration(job.duration_ms / 1000)}
                    </TableCell>
                    <TableCell align="right">
                      {job.overview.operators} / {job.overview.task_instances}
                    </TableCell>
                    <TableCell align="right">{job.overview.restarts}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        )}
      </Paper>
    </Box>
  );
};

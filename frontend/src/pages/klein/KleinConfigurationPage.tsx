import {
  Alert,
  Box,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography,
} from "@mui/material";
import React from "react";
import { useParams } from "react-router-dom";
import { formatDateFromTimeMs } from "../../common/formatUtils";
import Loading from "../../components/Loading";
import { useKleinJob } from "./hook/useKleinJobs";

export const KleinConfigurationPage = () => {
  const { jobId } = useParams();
  const { job, error, isLoading } = useKleinJob(jobId);
  if (isLoading) {
    return <Loading loading />;
  }
  if (error || !job) {
    return (
      <Alert severity="error" sx={{ margin: 3 }}>
        Unable to load configuration.
      </Alert>
    );
  }
  const options = Object.entries(job.configuration);
  return (
    <Box sx={{ padding: 3 }}>
      <Typography sx={{ fontWeight: 500 }} variant="h4">
        Configuration
      </Typography>
      <Paper variant="outlined" sx={{ marginTop: 3, padding: 2 }}>
        <Typography sx={{ fontWeight: 500 }} variant="h6">
          Job metadata
        </Typography>
        <Table size="small" sx={{ marginTop: 1 }}>
          <TableBody>
            <MetadataRow label="Job ID" value={job.job_id} />
            <MetadataRow label="Ray namespace" value={job.namespace ?? "-"} />
            <MetadataRow label="Runtime mode" value={job.runtime_mode} />
            <MetadataRow
              label="Started"
              value={
                job.started_at_ms
                  ? formatDateFromTimeMs(job.started_at_ms)
                  : "-"
              }
            />
          </TableBody>
        </Table>
      </Paper>
      <Paper variant="outlined" sx={{ marginTop: 3 }}>
        <TableContainer>
          <Table size="small">
            <TableHead>
              <TableRow>
                <TableCell>Option</TableCell>
                <TableCell>Explicit value</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {options.map(([key, value]) => (
                <TableRow key={key}>
                  <TableCell component="th" sx={{ fontFamily: "monospace" }}>
                    {key}
                  </TableCell>
                  <TableCell
                    sx={{ fontFamily: "monospace", overflowWrap: "anywhere" }}
                  >
                    {formatValue(value)}
                  </TableCell>
                </TableRow>
              ))}
              {options.length === 0 && (
                <TableRow>
                  <TableCell align="center" colSpan={2}>
                    This job uses default engine options.
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
        </TableContainer>
      </Paper>
      <Alert severity="info" sx={{ marginTop: 2 }}>
        Credential-like options are redacted by the JobManager before they leave
        the actor.
      </Alert>
    </Box>
  );
};

const MetadataRow = ({ label, value }: { label: string; value: string }) => (
  <TableRow>
    <TableCell sx={{ width: 180, color: "text.secondary" }}>{label}</TableCell>
    <TableCell sx={{ fontFamily: "monospace" }}>{value}</TableCell>
  </TableRow>
);

const formatValue = (value: unknown) =>
  typeof value === "string" ? value : JSON.stringify(value);

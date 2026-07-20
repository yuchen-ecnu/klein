import {
  Alert,
  Box,
  Button,
  CircularProgress,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Stack,
  TextField,
  Typography,
} from "@mui/material";
import { useEffect, useState } from "react";
import { KleinOperator } from "../../type/klein";
import { useKleinOperatorRescale } from "./hook/useKleinJobs";

const parseParallelism = (value: string) => {
  if (!/^[1-9]\d*$/.test(value)) {
    return undefined;
  }
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) ? parsed : undefined;
};

export const KleinOperatorRescaleControl = ({
  jobId,
  onRefresh,
  operator,
}: {
  jobId?: string;
  onRefresh: () => Promise<unknown>;
  operator: KleinOperator;
}) => {
  const [parallelismInput, setParallelismInput] = useState(
    String(operator.parallelism),
  );
  const [confirmRescale, setConfirmRescale] = useState(false);
  const {
    clearFeedback,
    isRescaling,
    requestError,
    rescale,
    result,
  } = useKleinOperatorRescale(jobId, onRefresh);

  useEffect(() => {
    setParallelismInput(String(operator.parallelism));
  }, [operator.op_id, operator.parallelism]);

  useEffect(() => {
    setConfirmRescale(false);
    clearFeedback();
  }, [clearFeedback, operator.op_id]);

  const targetParallelism = parseParallelism(parallelismInput);
  const invalidParallelism = targetParallelism === undefined;
  const unchangedParallelism = targetParallelism === operator.parallelism;
  const unavailableReason = operator.can_rescale
    ? undefined
    : operator.rescale_disabled_reason || "This operator cannot be rescaled.";
  const helperText = unavailableReason
    ? unavailableReason
    : invalidParallelism
      ? "Enter a positive integer."
      : unchangedParallelism
        ? "Choose a value different from the current parallelism."
        : "The running job stays online while this operator is rescaled.";
  const businessFailure =
    result && result.status !== "COMPLETED" && result.status !== "NOOP";

  const confirm = async () => {
    if (targetParallelism === undefined) {
      return;
    }
    setConfirmRescale(false);
    await rescale(operator.op_id, targetParallelism);
  };

  return (
    <Box
      sx={{
        backgroundColor: "background.default",
        border: "1px solid",
        borderColor: "divider",
        borderRadius: 1,
        marginTop: 2,
        padding: 2,
      }}
    >
      <Stack
        alignItems={{ sm: "flex-start", md: "center" }}
        direction={{ xs: "column", md: "row" }}
        gap={2}
      >
        <Box sx={{ flex: 1 }}>
          <Typography sx={{ fontWeight: 600 }} variant="subtitle2">
            Scale operator
          </Typography>
          <Typography color="text.secondary" variant="body2">
            Current parallelism: {operator.parallelism}
          </Typography>
        </Box>
        <TextField
          disabled={!operator.can_rescale || isRescaling}
          error={!unavailableReason && invalidParallelism}
          helperText={helperText}
          inputProps={{ min: 1, step: 1 }}
          label="Parallelism"
          onChange={(event) => {
            clearFeedback();
            setParallelismInput(event.target.value);
          }}
          sx={{ minWidth: 280 }}
          type="number"
          value={parallelismInput}
        />
        <Button
          disabled={
            !operator.can_rescale ||
            invalidParallelism ||
            unchangedParallelism ||
            isRescaling
          }
          onClick={() => setConfirmRescale(true)}
          startIcon={
            isRescaling ? <CircularProgress color="inherit" size={16} /> : null
          }
          variant="contained"
        >
          {isRescaling ? "Rescaling…" : "Rescale"}
        </Button>
      </Stack>
      {isRescaling && (
        <Alert severity="info" sx={{ marginTop: 2 }}>
          Creating and coordinating task instances. Keep this page open for the
          result.
        </Alert>
      )}
      {requestError && (
        <Alert onClose={clearFeedback} severity="error" sx={{ marginTop: 2 }}>
          Unable to rescale the operator: {requestError}
        </Alert>
      )}
      {businessFailure && (
        <Alert onClose={clearFeedback} severity="error" sx={{ marginTop: 2 }}>
          Operator rescale {result.status.toLowerCase()}: {result.error ||
            "No reason was provided."}
        </Alert>
      )}
      {result && !businessFailure && (
        <Alert
          onClose={clearFeedback}
          severity={result.status === "COMPLETED" ? "success" : "info"}
          sx={{ marginTop: 2 }}
        >
          {result.status === "COMPLETED"
            ? `Operator parallelism changed from ${result.previous_parallelism ?? operator.parallelism} to ${result.parallelism}.`
            : `Operator parallelism is already ${result.parallelism}.`}
        </Alert>
      )}
      <Dialog
        onClose={isRescaling ? undefined : () => setConfirmRescale(false)}
        open={confirmRescale}
      >
        <DialogTitle>Rescale {operator.name}?</DialogTitle>
        <DialogContent>
          Change operator parallelism from {operator.parallelism} to{" "}
          {targetParallelism}.{" "}
          {targetParallelism !== undefined &&
          targetParallelism > operator.parallelism
            ? "Klein creates only the added task instances before installing the local barrier."
            : "Klein retires only the surplus task instances after the topology commit."}{" "}
          Retained actors keep their identities and the rest of the job stays
          online.
        </DialogContent>
        <DialogActions>
          <Button
            disabled={isRescaling}
            onClick={() => setConfirmRescale(false)}
          >
            Cancel
          </Button>
          <Button
            disabled={targetParallelism === undefined || isRescaling}
            onClick={() => void confirm()}
            variant="contained"
          >
            Confirm rescale
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

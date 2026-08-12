import { useCallback, useEffect, useState } from "react";
import useSWR from "swr";
import { API_REFRESH_INTERVAL_MS } from "../../../common/constants";
import {
  cancelKleinJob,
  getKleinJob,
  getKleinJobs,
  rescaleKleinOperator,
} from "../../../service/klein";
import { isAPIRequestError } from "../../../service/requestHandlers";
import { KleinOperatorRescaleResult } from "../../../type/klein";

const ACTIVE_RESCALE_STATUSES = new Set([
  "ACCEPTED",
  "RUNNING",
  "STABILIZING",
]);

const isPendingOperation = (
  operation: KleinOperatorRescaleResult | null | undefined,
) => operation !== null && operation !== undefined && ACTIVE_RESCALE_STATUSES.has(operation.status);

export const useKleinJobs = () => {
  const { data, error, isLoading, mutate } = useSWR(
    "klein-jobs",
    async () => (await getKleinJobs()).data.jobs,
    { refreshInterval: API_REFRESH_INTERVAL_MS },
  );
  return { jobs: data ?? [], error, isLoading, refresh: mutate };
};

export const useKleinJob = (jobId: string | undefined) => {
  const { data, error, isLoading, mutate } = useSWR(
    jobId ? ["klein-job", jobId] : null,
    async () => {
      if (jobId === undefined) {
        throw new Error("A Klein job ID is required");
      }
      return (await getKleinJob(jobId)).data.job;
    },
    { refreshInterval: API_REFRESH_INTERVAL_MS },
  );
  const cancel = async () => {
    if (!jobId) {
      return;
    }
    await cancelKleinJob(jobId);
    await mutate();
  };
  return { job: data, error, isLoading, refresh: mutate, cancel };
};

export const useKleinOperatorRescale = (
  jobId: string | undefined,
  refresh: () => Promise<unknown>,
  operation?: KleinOperatorRescaleResult | null,
) => {
  const [isRescaling, setIsRescaling] = useState(false);
  const [result, setResult] = useState<KleinOperatorRescaleResult>();
  const [requestError, setRequestError] = useState<string>();

  const clearFeedback = useCallback(() => {
    setResult(undefined);
    setRequestError(undefined);
  }, []);

  const rescale = useCallback(
    async (operatorId: number, parallelism: number) => {
      if (!jobId) {
        setRequestError("A Klein job ID is required.");
        return undefined;
      }
      setIsRescaling(true);
      clearFeedback();
      try {
        const response = await rescaleKleinOperator(
          jobId,
          operatorId,
          parallelism,
        );
        const nextResult = response.data;
        setResult(nextResult);
        return nextResult;
      } catch (error) {
        const responseMessage = isAPIRequestError<{ error?: string }>(error)
          ? error.response.data?.error
          : undefined;
        setRequestError(
          responseMessage ||
            (error instanceof Error
              ? error.message
              : "The rescale request failed."),
        );
        return undefined;
      } finally {
        // A FAILED result or even an HTTP timeout can happen after the topology
        // crossed its commit point. Always refresh so the drawer does not keep
        // stale parallelism or readiness controls.
        await refresh().catch(() => undefined);
        setIsRescaling(false);
      }
    },
    [clearFeedback, jobId, refresh],
  );

  const currentResult =
    operation &&
    (isPendingOperation(operation) || operation.operation_id === result?.operation_id)
      ? operation
      : result;
  const operationPending = isPendingOperation(currentResult);

  useEffect(() => {
    if (!operationPending) {
      return undefined;
    }
    let cancelled = false;
    let timer: number;
    const poll = async () => {
      await refresh().catch(() => undefined);
      if (!cancelled) {
        timer = window.setTimeout(() => void poll(), 1_000);
      }
    };
    timer = window.setTimeout(() => void poll(), 1_000);
    return () => {
      cancelled = true;
      window.clearTimeout(timer);
    };
  }, [currentResult?.operation_id, currentResult?.status, operationPending, refresh]);

  return {
    clearFeedback,
    isRescaling,
    operationPending,
    requestError,
    rescale,
    result: currentResult,
  };
};

import axios from "axios";
import { useCallback, useState } from "react";
import useSWR from "swr";
import { API_REFRESH_INTERVAL_MS } from "../../../common/constants";
import {
  cancelKleinJob,
  getKleinJob,
  getKleinJobs,
  rescaleKleinOperator,
} from "../../../service/klein";
import { KleinOperatorRescaleResult } from "../../../type/klein";

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
        const responseMessage = axios.isAxiosError<{ error?: string }>(error)
          ? error.response?.data?.error
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

  return { clearFeedback, isRescaling, requestError, rescale, result };
};

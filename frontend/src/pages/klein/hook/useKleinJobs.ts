import useSWR from "swr";
import { API_REFRESH_INTERVAL_MS } from "../../../common/constants";
import {
  cancelKleinJob,
  getKleinJob,
  getKleinJobs,
} from "../../../service/klein";

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

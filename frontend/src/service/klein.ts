import { KleinJobResponse, KleinJobsResponse } from "../type/klein";
import { get, post } from "./requestHandlers";

export const getKleinJobs = () => get<KleinJobsResponse>("api/klein/jobs");
export const getKleinJob = (jobId: string) =>
  get<KleinJobResponse>(`api/klein/jobs/${encodeURIComponent(jobId)}`);
export const cancelKleinJob = (jobId: string) =>
  post<{ job_id: string; cancelled: boolean }>(
    `api/klein/jobs/${encodeURIComponent(jobId)}/cancel`,
  );

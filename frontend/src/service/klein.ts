import {
  KleinJobResponse,
  KleinJobsResponse,
  KleinOperatorRescaleResult,
} from "../type/klein";
import { get, post } from "./requestHandlers";

export const getKleinJobs = () => get<KleinJobsResponse>("api/klein/jobs");
export const getKleinJob = (jobId: string) =>
  get<KleinJobResponse>(`api/klein/jobs/${encodeURIComponent(jobId)}`);
export const cancelKleinJob = (jobId: string) =>
  post<{ job_id: string; cancelled: boolean }>(
    `api/klein/jobs/${encodeURIComponent(jobId)}/cancel`,
  );
export const rescaleKleinOperator = (
  jobId: string,
  operatorId: number,
  parallelism: number,
) =>
  post<KleinOperatorRescaleResult>(
    `api/klein/jobs/${encodeURIComponent(jobId)}/operators/${operatorId}/rescale`,
    { parallelism },
  );

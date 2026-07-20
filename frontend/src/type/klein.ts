export type KleinJobStatus =
  | "CREATED"
  | "SUBMITTING"
  | "DEPLOYING"
  | "INITIALIZING"
  | "RUNNING"
  | "FINISHED"
  | "CANCELLED"
  | "FAILED"
  | "UNKNOWN";

export type KleinInstanceCounts = {
  running: number;
  pending: number;
  restarting: number;
  finished: number;
  failed: number;
};

export type KleinSubtask = {
  subtask_index: number;
  status: string;
  actor_id?: string | null;
  rows_in: number;
  rows_out: number;
  bytes_in: number;
  bytes_out: number;
  rows_in_per_second: number;
  rows_out_per_second: number;
  bytes_in_per_second: number;
  bytes_out_per_second: number;
  queued: number;
  capacity: number;
  busy_ns: number;
  backpressure_ns: number;
  busy_percent: number;
  backpressure_percent: number;
  backpressure_events: number;
  barriers_in: number;
  barriers_out: number;
  checkpoint_alignment_ms: number;
  checkpoint_barrier_latency_ms: number;
  checkpoint_state_size_bytes: number;
  last_checkpoint_id?: number;
};

export type KleinOperator = {
  name: string;
  op_id: number;
  parallelism: number;
  status: string;
  rows_in: number;
  rows_out: number;
  bytes_in: number;
  bytes_out: number;
  rows_in_per_second: number;
  rows_out_per_second: number;
  bytes_in_per_second: number;
  bytes_out_per_second: number;
  queued: number;
  capacity: number;
  busy_ns: number;
  backpressure_ns: number;
  busy_percent: number;
  backpressure_percent: number;
  max_busy_percent?: number;
  max_backpressure_percent?: number;
  instances: KleinInstanceCounts;
  cpus: number;
  gpus: number;
  downstream: number[];
  backpressure_events: number;
  barriers_in: number;
  barriers_out: number;
  checkpoint_alignment_ms: number;
  checkpoint_barrier_latency_ms: number;
  checkpoint_state_size_bytes: number;
  last_checkpoint_id?: number;
  can_rescale: boolean;
  rescale_disabled_reason?: string | null;
  subtasks: KleinSubtask[];
};

export type KleinOperatorRescaleResult = {
  job_id: string;
  operator_id: number;
  operator_name?: string | null;
  previous_parallelism?: number | null;
  parallelism: number;
  target_parallelism: number;
  status: "COMPLETED" | "NOOP" | "REJECTED" | "FAILED";
  started_at_ms: number;
  ended_at_ms: number;
  error?: string | null;
};

export type KleinCheckpointSubtask = {
  subtask_index: number;
  alignment_duration_ms?: number;
  barrier_latency_ms?: number;
  state_size_bytes?: number;
  rows_in?: number;
  rows_out?: number;
  backpressure_events?: number;
  backpressure_duration_ms?: number;
};

export type KleinCheckpointOperator = {
  op_id: number;
  name: string;
  state_size_bytes: number;
  alignment_duration_ms: number;
  barrier_latency_ms: number;
  subtasks: KleinCheckpointSubtask[];
};

export type KleinCheckpoint = {
  id: number;
  status: string;
  triggered_at_ms?: number;
  completed_at_ms?: number;
  duration_ms?: number;
  acknowledged: number;
  required_acknowledgements: number;
  reason?: string;
  state_size_bytes: number;
  alignment_duration_ms: number;
  barrier_latency_ms: number;
  operators: KleinCheckpointOperator[];
};

export type KleinCheckpointDetails = {
  summary: {
    total: number;
    completed: number;
    failed: number;
    in_progress: number;
    state_size_bytes?: number;
    last_persisted_snapshot_id?: number;
  };
  history: KleinCheckpoint[];
  latest_path?: string;
  error?: string;
};

export type KleinJob = {
  job_id: string;
  job_name: string;
  namespace?: string;
  runtime_mode: string;
  status: KleinJobStatus;
  created_at_ms: number;
  started_at_ms?: number;
  ended_at_ms?: number;
  updated_at_ms: number;
  duration_ms: number;
  dashboard_stale: boolean;
  dashboard_error?: string;
  failure?: string;
  overview: {
    operators: number;
    task_instances: number;
    rows_in: number;
    rows_out: number;
    bytes_in: number;
    bytes_out: number;
    restarts: number;
    max_restarts: number;
    restart_window_seconds: number;
  };
  operators: KleinOperator[];
  edges: { source: number; target: number }[];
  checkpoints: KleinCheckpointDetails;
  configuration: Record<string, unknown>;
  status_history: {
    status: KleinJobStatus;
    previous_status?: KleinJobStatus;
    timestamp_ms: number;
  }[];
};

export type KleinJobsResponse = { jobs: KleinJob[] };
export type KleinJobResponse = { job: KleinJob };

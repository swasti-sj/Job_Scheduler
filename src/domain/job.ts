import type { JobState, WorkerState } from './states.js';

export type Priority = 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9;

export const MIN_PRIORITY = 0;
export const MAX_PRIORITY = 9;

export function isPriority(n: number): n is Priority {
  return Number.isInteger(n) && n >= MIN_PRIORITY && n <= MAX_PRIORITY;
}

export interface Job {
  id: string;
  tenant_id: string;
  queue_name: string;
  job_type: string;
  payload: Record<string, unknown>;
  priority: Priority;
  state: JobState;
  attempt_count: number;
  max_attempts: number;
  created_at: Date;
  scheduled_for: Date;
  claimed_at: Date | null;
  lease_expires_at: Date | null;
  completed_at: Date | null;
  idempotency_key: string | null;
  depends_on: string[];
  last_error: string | null;
  claimed_by: string | null;
  result: Record<string, unknown> | null;
  updated_at: Date;
}

/** What a worker is handed when a job is dispatched to it. */
export interface JobAssignment {
  id: string;
  tenant_id: string;
  queue_name: string;
  job_type: string;
  payload: Record<string, unknown>;
  attempt_count: number;
  max_attempts: number;
  lease_expires_at: string;
  /** Milliseconds between enqueue-ready and claim; measured server side. */
  scheduling_latency_ms: number;
}

export interface WorkerRow {
  id: string;
  hostname: string;
  pid: number;
  queues: string[];
  max_concurrency: number;
  state: WorkerState;
  current_job_id: string | null;
  inflight_count: number;
  connected_node: string | null;
  last_heartbeat_at: Date;
  registered_at: Date;
}

export interface SubmitJobInput {
  tenant_id: string;
  queue_name: string;
  job_type: string;
  payload: Record<string, unknown>;
  priority: Priority;
  max_attempts: number;
  scheduled_for: Date | null;
  idempotency_key: string | null;
  depends_on: string[];
  /** Client-supplied id, needed for batch DAG submission with local refs. */
  id?: string;
}

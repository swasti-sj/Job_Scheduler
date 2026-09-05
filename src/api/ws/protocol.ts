import type { JobAssignment } from '../../domain/job.js';

/** Worker -> scheduler. */
export type WorkerMessage =
  | {
      type: 'hello';
      worker_id: string;
      hostname: string;
      pid: number;
      queues: string[];
      max_concurrency: number;
    }
  /** Liveness plus lease extension for everything the worker still holds. */
  | { type: 'heartbeat'; inflight: string[] }
  /** Worker has started executing: CLAIMED -> RUNNING. */
  | { type: 'ack'; job_id: string }
  | { type: 'result'; job_id: string; ok: true; result?: unknown }
  | { type: 'result'; job_id: string; ok: false; error: string }
  /** "I have free slots" - asks the dispatcher for work. */
  | { type: 'pull'; slots: number };

/** Scheduler -> worker. */
export type SchedulerMessage =
  | {
      type: 'welcome';
      node_id: string;
      heartbeat_interval_ms: number;
      lease_seconds: number;
    }
  | { type: 'assign'; jobs: JobAssignment[] }
  /**
   * `revoked` lists jobs the worker still believes it owns but which have been
   * reclaimed underneath it (its lease expired, or its socket flapped). The
   * worker must abandon them without reporting a result - a late report would be
   * rejected by the ownership guard anyway, but abandoning early stops it
   * wasting a slot.
   */
  | { type: 'heartbeat_ack'; revoked: string[] }
  | { type: 'shutdown'; reason: string }
  | { type: 'error'; message: string };

export function parseWorkerMessage(raw: string): WorkerMessage {
  const parsed: unknown = JSON.parse(raw);
  if (typeof parsed !== 'object' || parsed === null || !('type' in parsed)) {
    throw new Error('message must be an object with a type');
  }
  return parsed as WorkerMessage;
}
